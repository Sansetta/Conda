"""Постоянные Apify ID акторов (17 символов, например «Ba6hYg3ZpFzUrCsyy»).

username/name меняются при переименовании актора, ID - нет. Поэтому обе итоговые таблицы
(actor_price_monitor и actor_catalog) ключуются по ID: цена обновляется у той же строки, рерайт не повторяется.

Источники ID, по убыванию надёжности:
  1. поле `id` в ответе Store API (если API его отдаёт - дополнительных запросов нет);
  2. уже известное соответствие actor_key -> apify_id из actor_price_monitor (после первого прогона почти всё здесь);
  3. код страниц сайта https://apify.com/store/categories и страниц категорий: JSON внутри <script>
     (__NEXT_DATA__, application/json, ld+json) обходится целиком, берутся объекты с id + username + name;
  4. GET /v2/acts/{username}~{name} -> id (точно, но по одному запросу на актора).
Страницы сайта - дополнительный источник: они отдают лишь часть Store, полное покрытие даёт API.
"""
from __future__ import annotations

import html
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from .apify_api import ACTORS_URL, get_json

log = logging.getLogger("apify-tracker")

SITE_URL = "https://apify.com"
CATEGORIES_URL = f"{SITE_URL}/store/categories"
ID_RE = re.compile(r"^[A-Za-z0-9]{17}$")
NEXT_RE = re.compile(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)
JSON_SCRIPT_RE = re.compile(r'<script[^>]*type="application/(?:ld\+)?json"[^>]*>(.*?)</script>', re.S)
CAT_LINK_RE = re.compile(r'href="(/store/categories/[a-z0-9_-]+)"', re.I)
UA = "Mozilla/5.0 (compatible; apify-store-tracker/2.0)"


def valid_id(v) -> str | None:
    return v if isinstance(v, str) and ID_RE.match(v) else None


def _walk(o):
    stack = [o]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            yield cur
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)


def ids_from_json(obj) -> dict[str, str]:
    """Любая JSON-структура -> {"username/name": id}: берутся только объекты, где есть и id, и username, и name."""
    out: dict[str, str] = {}
    for d in _walk(obj):
        aid, user, name = valid_id(d.get("id")), d.get("username"), d.get("name")
        if aid and isinstance(user, str) and isinstance(name, str) and user and name:
            out[f"{user}/{name}"] = aid
    return out


def ids_from_html(page: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for rx in (NEXT_RE, JSON_SCRIPT_RE):
        for m in rx.finditer(page or ""):
            try:
                out.update(ids_from_json(json.loads(html.unescape(m.group(1)))))
            except ValueError:
                continue
    return out


def category_links(page: str) -> list[str]:
    seen: dict[str, None] = {}
    for m in CAT_LINK_RE.finditer(page or ""):
        seen[SITE_URL + m.group(1)] = None
    return list(seen)


def _get_html(session: requests.Session, url: str) -> str:
    for attempt in range(3):
        try:
            r = session.get(url, timeout=60, headers={"User-Agent": UA, "Accept": "text/html"})
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 404):
                return ""
        except requests.RequestException as exc:
            log.warning("Сайт %s: %s", url, exc)
        time.sleep(2 ** attempt * 2)
    return ""


def site_id_map(session: requests.Session, delay: float = 0.5, max_pages: int = 300) -> dict[str, str]:
    """ID со страницы https://apify.com/store/categories и страниц категорий (какие есть в HTML этих страниц)."""
    page = _get_html(session, CATEGORIES_URL)
    found = ids_from_html(page)
    links = category_links(page)[:max_pages]
    for url in links:
        time.sleep(delay)
        found.update(ids_from_html(_get_html(session, url)))
    log.info("Сайт: страниц категорий %s, ID актёров в HTML: %s", len(links), len(found))
    return found


def lookup_id(session: requests.Session, username: str, name: str, delay: float = 0.2) -> str | None:
    data = get_json(session, f"{ACTORS_URL}/{username}~{name}", {})
    time.sleep(delay)
    return valid_id((data or {}).get("id"))


def known_ids(conn) -> dict[str, str]:
    cur = conn.cursor()
    cur.execute("SELECT actor_key, apify_id FROM actor_price_monitor")
    out = {k: v for k, v in cur.fetchall()}
    cur.close()
    conn.commit()
    return out


def resolve_ids(conn, rows: list[dict], workers: int = 6, delay: float = 0.2, use_site: bool = True) -> dict:
    """Проставляет row["apify_id"] всем строкам, где его ещё нет. -> статистика по источникам."""
    stats = {"api_list": 0, "db": 0, "site": 0, "lookup": 0, "unresolved": 0}
    known = known_ids(conn)
    missing: list[dict] = []
    for r in rows:
        if valid_id(r.get("apify_id")):
            stats["api_list"] += 1
        elif r["actor_key"] in known:
            r["apify_id"] = known[r["actor_key"]]
            stats["db"] += 1
        else:
            missing.append(r)

    if missing and use_site:
        s = requests.Session()
        try:
            smap = site_id_map(s)
        except Exception as exc:
            log.warning("Обход сайта не удался: %s", exc)
            smap = {}
        rest = []
        for r in missing:
            aid = smap.get(r["actor_key"])
            if aid:
                r["apify_id"] = aid
                stats["site"] += 1
            else:
                rest.append(r)
        missing = rest

    if missing:
        log.info("ID: через API запрашиваю %s акторов без известного ID", len(missing))
        local = threading.local()

        def work(r: dict) -> str | None:
            s = getattr(local, "s", None)
            if s is None:
                s = local.s = requests.Session()
                s.headers["User-Agent"] = "apify-store-tracker/2.0"
            return lookup_id(s, r["username"], r["name"], delay)

        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            futs = {ex.submit(work, r): r for r in missing}
            for i, fut in enumerate(as_completed(futs), 1):
                r = futs[fut]
                try:
                    aid = fut.result()
                except Exception as exc:
                    log.error("ID %s: %s", r["actor_key"], exc)
                    aid = None
                if aid:
                    r["apify_id"] = aid
                    stats["lookup"] += 1
                else:
                    stats["unresolved"] += 1
                if i % 1000 == 0:
                    log.info("ID: %s/%s", i, len(missing))
    log.info("ID акторов: %s", stats)
    return stats
