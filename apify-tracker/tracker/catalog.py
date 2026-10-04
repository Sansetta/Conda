"""Таблица 2: каталог уникальных типов (actor_catalog).

Рерайт делается ОДИН раз на Apify ID. Актор с новым ID (его ещё нет в actor_catalog) получает новое название и
карточку в структуре Apify Store, актор с уже известным ID не трогается: его цена живёт в actor_price_monitor и
подтягивается представлением v_actor_catalog_latest. Цен в тексте карточки нет, поэтому менять цену можно без рерайта.

Одна итерация = окно (по умолчанию 3 часа). Готовые карточки копятся в памяти и пишутся в таблицу пачкой раз в
flush_s секунд (и в конце окна/при сбое). INSERT IGNORE гарантирует, что существующая запись никогда не перезаписывается.
Область (scope): top - акторы из топов услуг (дёшево, по умолчанию), eligible - все с ценой и min_users, all - все.
Классификация ниши/услуги берётся из последнего посчитанного снимка, поэтому совсем новый актор попадает в каталог
после ближайшего ежедневного прогона (когда он получил нишу и услугу).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import threading
import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass

import requests

from .content import (PROMPT_VERSION, ContentStore, RewriteConfig, ServiceCtx, Source, content_similarity,
                      details_hash, fetch_details, make_provider, normalize_content, source_text)

log = logging.getLogger("apify-tracker")

CATALOG_COLS = ["apify_id", "actor_key", "niche_slug", "niche_name", "service_key", "service_display_name", "title",
                "tagline", "content", "source_title", "source_hash", "provider", "model", "lang", "similarity",
                "created_at"]


@dataclass
class CatalogJob:
    apify_id: str
    actor_pk: int
    actor_key: str
    source_title: str
    niche_slug: str
    ctx: ServiceCtx


@dataclass
class CatalogResult:
    apify_id: str
    status: str                 # rewritten | no_sources | too_similar | dup_title | failed
    new_details: dict
    row: dict | None = None
    note: str = ""


def _norm_title(t: str) -> str:
    return " ".join(t.lower().split())


def process_actor(job: CatalogJob, cfg: RewriteConfig, provider, session, used_titles: set[str],
                  lock: threading.Lock) -> CatalogResult:
    ctx, src = job.ctx, job.ctx.sources[0]
    now = dt.datetime.now()
    new_details: dict = {}
    d = src.cached
    stale = d is None or (now - d["fetched_at"]).total_seconds() > cfg.details_ttl_days * 86400
    if stale and cfg.fetch_details:
        try:
            fresh = fetch_details(session, src.username, src.name, cfg)
        except Exception as exc:
            log.warning("Описание %s не получено: %s", job.actor_key, exc)
            fresh = None
        if fresh:
            new_details[src.actor_pk] = {**fresh, "text_hash": details_hash(fresh)}
            d = fresh
    if not d or not (d.get("description") or d.get("seo_description") or d.get("readme_excerpt")):
        return CatalogResult(job.apify_id, "no_sources", new_details, note="нет описания у актора")

    text = source_text(src, d, cfg)
    corpus = f"{text} {job.source_title}"
    hint, content, sim, last_problem = "", None, 1.0, "failed"
    for _ in range(2):
        try:
            content = normalize_content(provider.rewrite(ctx, [text], cfg, hint), ctx)
        except ValueError as exc:
            hint, content, last_problem = f"The previous answer was invalid ({exc}). Follow the JSON schema exactly.", None, "failed"
            continue
        key = _norm_title(content["title"])
        with lock:
            title_taken = key in used_titles or key == _norm_title(job.source_title)
        sim = content_similarity(content, corpus)
        if title_taken:
            hint, last_problem = "The product name is already taken or equals the original. Invent a different one.", "dup_title"
            content = None
            continue
        if sim > cfg.max_similarity:
            hint = (f"The previous draft was too close to the notes (overlap {sim:.0%}). "
                    f"Rewrite from scratch with different wording, order and examples.")
            last_problem, content = "too_similar", None
            continue
        with lock:
            if key in used_titles:     # другой поток успел занять это название
                last_problem, content = "dup_title", None
                continue
            used_titles.add(key)
        break
    if content is None:
        return CatalogResult(job.apify_id, last_problem, new_details, note=last_problem)

    row = {"apify_id": job.apify_id, "actor_key": job.actor_key, "niche_slug": job.niche_slug,
           "niche_name": ctx.niche_name, "service_key": ctx.service_key,
           "service_display_name": ctx.display_name[:255], "title": content["title"][:255],
           "tagline": content["tagline"][:512], "content": json.dumps(content, ensure_ascii=False),
           "source_title": job.source_title[:512],
           "source_hash": hashlib.sha256(f"{PROMPT_VERSION}|{text}".encode()).hexdigest(),
           "provider": provider.name, "model": cfg.model if provider.name == "llm" else None, "lang": cfg.lang,
           "similarity": round(min(sim, 9.999), 3), "created_at": dt.datetime.now().replace(microsecond=0)}
    return CatalogResult(job.apify_id, "rewritten", new_details, row)


class CatalogStore:
    def __init__(self, conn):
        self.conn = conn
        self.details = ContentStore(conn)

    def _ping(self) -> None:
        if hasattr(self.conn, "ping"):
            self.conn.ping(reconnect=True)

    def used_titles(self) -> set[str]:
        self._ping()
        cur = self.conn.cursor()
        cur.execute("SELECT title FROM actor_catalog")
        out = {_norm_title(t) for (t,) in cur.fetchall()}
        cur.close()
        self.conn.commit()
        return out

    def load_jobs(self, cfg: RewriteConfig) -> list[CatalogJob]:
        """Акторы мониторинга, у которых ещё НЕТ записи в каталоге (новые ID), в области cfg.scope."""
        self._ping()
        q = ("SELECT m.apify_id, a.actor_pk, m.actor_key, m.username, m.name, m.title "
             "FROM actor_price_monitor m JOIN actors a ON a.actor_key = m.actor_key "
             "LEFT JOIN actor_catalog c ON c.apify_id = m.apify_id "
             "WHERE c.apify_id IS NULL AND m.is_active = 1 AND m.total_users >= %s")
        if cfg.scope == "top":
            q += (" AND a.actor_pk IN (SELECT t.actor_pk FROM service_top t "
                  "JOIN v_latest_run lr ON lr.run_id = t.run_id)")
        elif cfg.scope == "eligible":
            q += " AND m.cost_1000 IS NOT NULL"
        elif cfg.scope == "all":
            q = q.replace("m.total_users >= %s", "%s = %s")   # без порога
        else:
            raise ValueError(f"неизвестная область каталога: {cfg.scope}")
        q += " ORDER BY m.total_users DESC"
        cur = self.conn.cursor()
        cur.execute(q, (1, 1) if cfg.scope == "all" else (cfg.min_users,))
        base = cur.fetchall()
        if cfg.limit:
            base = base[:cfg.limit]
        pks = [r[1] for r in base]
        cls: dict[int, tuple] = {}
        for i in range(0, len(pks), 1000):
            part = pks[i:i + 1000]
            cur.execute("SELECT actor_pk, niche_slug, niche_name, service_key, service_display_name "
                        f"FROM v_actor_services WHERE actor_pk IN ({','.join(['%s'] * len(part))})", part)
            for pk, ns, nn, sk, sd in cur.fetchall():
                if pk not in cls or (cls[pk][2] == "general" and sk != "general"):   # конкретная услуга лучше general
                    cls[pk] = (ns, nn, sk, sd)
        cur.close()
        self.conn.commit()
        cache = self.details._load_details(set(pks))
        jobs = []
        for aid, pk, key, user, name, title in base:
            if cfg.niche and (pk not in cls or cls[pk][0] != cfg.niche):
                continue
            if pk not in cls:
                continue
            ns, nn, sk, sd = cls[pk]
            svc_name = sd[len(nn):].strip() if sd.startswith(nn) else sd
            src = Source(pk, user, name, title, 0, cache.get(pk))
            ctx = ServiceCtx(0, ns, nn, sk, svc_name, sd, "-", {"uid": aid[-5:], "source_title": title, "actors": 1},
                             [src], mode="actor")
            jobs.append(CatalogJob(aid, pk, key, title, ns, ctx))
        return jobs

    def insert(self, rows: list[dict]) -> int:
        """Пачка новых записей. INSERT IGNORE: уже существующий apify_id никогда не перезаписывается."""
        if not rows:
            return 0
        self._ping()
        cur = self.conn.cursor()
        try:
            cur.executemany(f"INSERT IGNORE INTO actor_catalog ({','.join(CATALOG_COLS)}) "
                            f"VALUES ({','.join(['%s'] * len(CATALOG_COLS))})",
                            [tuple(r[c] for c in CATALOG_COLS) for r in rows])
            n = cur.rowcount
            self.conn.commit()
            return int(n)
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()


def run_catalog(conn, cfg: RewriteConfig, store=None, provider=None, clock=time.monotonic) -> Counter:
    store = store or CatalogStore(conn)
    provider = provider or make_provider(cfg)
    jobs = store.load_jobs(cfg)
    used = store.used_titles()
    lock = threading.Lock()
    stats: Counter = Counter()
    log.info("Каталог: новых ID в очереди %s (область %s), окно %.1f ч, запись пачкой раз в %.0f мин",
             len(jobs), cfg.scope, cfg.window_s / 3600, cfg.flush_s / 60)
    tls = threading.local()

    def work(job: CatalogJob) -> CatalogResult:
        s = getattr(tls, "s", None)
        if s is None:
            s = tls.s = requests.Session()
            s.headers["User-Agent"] = "apify-store-tracker/2.0"
        return process_actor(job, cfg, provider, s, used, lock)

    buf: list[dict] = []

    def flush() -> None:
        nonlocal buf, last_flush
        if buf:
            stats["inserted"] += store.insert(buf)
            log.info("Каталог: записано %s карточек (в очереди окна осталось %s)", len(buf), len(pending))
        buf, last_flush = [], clock()

    started = last_flush = clock()
    deadline = started + cfg.window_s
    pending, inflight = deque(jobs), {}
    try:
        with ThreadPoolExecutor(max_workers=max(1, cfg.workers)) as ex:
            while pending or inflight:
                if clock() < deadline:
                    while pending and len(inflight) < cfg.workers * 2:
                        job = pending.popleft()
                        inflight[ex.submit(work, job)] = job
                elif pending:
                    stats["deferred"] = len(pending)
                    log.info("Каталог: окно закончилось, отложено до следующего прогона: %s", len(pending))
                    pending.clear()
                if not inflight:
                    break
                done, _ = wait(list(inflight), timeout=30, return_when=FIRST_COMPLETED)
                for fut in done:
                    job = inflight.pop(fut)
                    try:
                        res = fut.result()
                    except Exception as exc:
                        stats["failed"] += 1
                        log.error("Каталог %s: %s", job.actor_key, exc)
                        continue
                    store.details.save_details(res.new_details)
                    stats[res.status] += 1
                    if res.status == "rewritten":
                        buf.append(res.row)
                    elif res.note:
                        log.warning("Каталог %s: %s", job.actor_key, res.note)
                if buf and (clock() - last_flush >= cfg.flush_s or len(buf) >= 200):
                    flush()
    finally:
        flush()
    log.info("Каталог завершён за %.0f мин: %s", (clock() - started) / 60, dict(stats))
    return stats
