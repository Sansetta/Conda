"""Сбор данных из публичного API Apify Store: HTTP, парсинг цен, обход Store (Collector), нормализация.

Логика сбора перенесена из исходного apify_store_tracker.py без изменений (кроме normalize).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

API_URL = "https://api.apify.com/v2/store"
ACTORS_URL = "https://api.apify.com/v2/acts"
PAGE_LIMIT = 1000  # API не отдаёт больше 1000 записей на один набор фильтров
DEFAULT_MAX_ACTORS = 200_000

# Стартовый список категорий. Новые категории добавляются автоматически:
# скрипт читает поле `categories` у найденных Actor'ов и обходит их тоже.
SEED_CATEGORIES = [
    "AI", "AGENTS", "AUTOMATION", "DEVELOPER_TOOLS", "ECOMMERCE", "JOBS",
    "LEAD_GENERATION", "NEWS", "SEO_TOOLS", "SOCIAL_MEDIA", "TRAVEL",
    "VIDEOS", "REAL_ESTATE", "INTEGRATIONS", "OPEN_SOURCE", "MCP_SERVERS",
    "MARKETING", "OTHER",
]
PRICING_MODELS = ["FREE", "FLAT_PRICE_PER_MONTH", "PRICE_PER_DATASET_ITEM", "PAY_PER_EVENT"]
SORTS = ["popularity", "newest", "lastUpdate", "relevance"]

# Слова, по которым не имеет смысла искать
STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "your", "you", "are", "can", "all",
    "any", "not", "use", "using", "get", "has", "have", "will", "also", "more", "into", "such",
    "their", "they", "its", "our", "out", "one", "new", "per", "via", "etc", "how", "what",
    "when", "which", "than", "then", "them", "these", "those", "was", "were", "been", "being",
}
TOKEN_RE = re.compile(r"[a-z0-9]{3,}")

# Если очередная пачка поисковых запросов дала меньше стольких новых Actor'ов, поиск останавливается
MIN_NEW_PER_SEARCH_BATCH = 20

log = logging.getLogger("apify-tracker")



# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------
def get_json(session: requests.Session, url: str, params: dict, retries: int = 5):
    """GET с ретраями. Возвращает data-объект или None, если фильтр невалиден (400) / нет объекта (404)."""
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=90)
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()["data"]
        except (requests.RequestException, ValueError, KeyError) as exc:
            wait = 2 ** attempt * 2
            log.warning("Ошибка запроса %s %s (%s), повтор через %ss", url, params, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Не удалось получить данные после {retries} попыток: {url} {params}")


# --------------------------------------------------------------------------
# Pricing
# --------------------------------------------------------------------------
def _num(v) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _tier_value(tiers) -> float | None:
    """Достаёт цену из tiered-структуры вида {"FREE": {"tieredPricePerUnitUsd": 0.005}, "BRONZE": {...}}.
    Берём тариф FREE (базовая цена на странице Actor'а), иначе первый попавшийся."""
    if not isinstance(tiers, dict) or not tiers:
        return None
    tier = tiers.get("FREE") or next(iter(tiers.values()))
    if isinstance(tier, dict):
        for v in tier.values():
            if _num(v) is not None:
                return float(v)
        return None
    return _num(tier)


def _fmt_money(x: float) -> str:
    return f"${round(x, 6):g}"


def _fmt_trial(minutes) -> str:
    m = _num(minutes)
    if not m:
        return ""
    if m % 1440 == 0:
        return f"{int(m // 1440)} d trial"
    if m % 60 == 0:
        return f"{int(m // 60)} h trial"
    return f"{int(m)} min trial"


def parse_pricing(pi: dict | None) -> dict:
    """currentPricingInfo -> плоский набор полей для БД/отчётов."""
    pi = pi or {}
    model = pi.get("pricingModel")
    unit = pi.get("unitName")
    trial = pi.get("trialMinutes")
    price = _num(pi.get("pricePerUnitUsd"))
    if price is None:
        price = _tier_value(pi.get("tieredPricing"))

    events: dict[str, dict] = {}
    for ev_key, ev in ((pi.get("pricingPerEvent") or {}).get("actorChargeEvents") or {}).items():
        ev = ev or {}
        p = _num(ev.get("eventPriceUsd"))
        if p is None:
            p = _tier_value(ev.get("eventTieredPricingUsd"))
        events[ev_key] = {"title": ev.get("eventTitle") or ev_key, "price_usd": p}

    if model is None:
        summary = ""
    elif model == "FREE":
        summary = "Free"
    elif model == "FLAT_PRICE_PER_MONTH":
        summary = f"{_fmt_money(price)}/month" if price is not None else "Flat price per month"
        t = _fmt_trial(trial)
        summary += f" + {t}" if t else ""
    elif model == "PRICE_PER_DATASET_ITEM":
        u = (unit or "result").rstrip("s") + "s"
        summary = f"{_fmt_money(price * 1000)} / 1,000 {u}" if price is not None else "Price per dataset item"
    elif model == "PAY_PER_EVENT":
        parts = [
            f"{e['title']} {_fmt_money(e['price_usd'])}" if e["price_usd"] is not None else e["title"]
            for e in list(events.values())[:8]
        ]
        if len(events) > 8:
            parts.append(f"+{len(events) - 8} more")
        summary = "Pay per event: " + "; ".join(parts) if parts else "Pay per event"
    else:
        summary = str(model)

    return {
        "pricing_model": model,
        "pricing_price_usd": price,
        "pricing_unit": unit,
        "pricing_trial_min": int(trial) if _num(trial) is not None else None,
        "pricing_summary": summary,
        "pricing_events": json.dumps(events, ensure_ascii=False, separators=(",", ":")) if events else None,
        "pricing_json": json.dumps(pi, ensure_ascii=False, separators=(",", ":")) if pi else None,
    }


def needs_pricing_detail(pi: dict | None) -> bool:
    """Платный Actor, у которого в ответе списка нет деталей цены."""
    pi = pi or {}
    model = pi.get("pricingModel")
    if model in (None, "FREE"):
        return False
    if model == "PAY_PER_EVENT":
        return not (pi.get("pricingPerEvent") or {}).get("actorChargeEvents")
    return pi.get("pricePerUnitUsd") is None and not pi.get("tieredPricing")


def pick_current_pricing(actor: dict) -> dict | None:
    """Из объекта Actor (GET /v2/acts/..) выбирает действующую запись pricingInfos."""
    infos = sorted(actor.get("pricingInfos") or [], key=lambda i: i.get("startedAt") or "")
    now = dt.datetime.now(dt.timezone.utc)
    cur = None
    for info in infos:
        started = info.get("startedAt")
        try:
            ts = dt.datetime.fromisoformat(started.replace("Z", "+00:00")) if started else None
        except ValueError:
            ts = None
        if ts is None or ts <= now:
            cur = info
    return cur or actor.get("currentPricingInfo")


# --------------------------------------------------------------------------
# Сбор данных
# --------------------------------------------------------------------------
class Collector:
    def __init__(self, url: str, delay: float, workers: int = 6,
                 max_actors: int = DEFAULT_MAX_ACTORS, include_unrunnable: bool = True,
                 max_terms: int = 3000):
        self.url = url
        self.delay = delay
        self.workers = max(1, workers)
        self.max_actors = max_actors  # 0 = без ограничения
        self.include_unrunnable = include_unrunnable
        self.max_terms = max_terms
        self.items: dict[str, dict] = {}
        self.seen_categories: set[str] = set()
        self.users_done: set[str] = set()
        self.terms_done: set[str] = set()
        self.requests_made = 0
        self.failed = 0
        self.lock = threading.Lock()
        self._local = threading.local()

    # -- инфраструктура ------------------------------------------------------
    def _session(self) -> requests.Session:
        s = getattr(self._local, "session", None)
        if s is None:
            s = self._local.session = requests.Session()
            s.headers["User-Agent"] = "apify-store-tracker/2.0"
        return s

    def full(self) -> bool:
        return bool(self.max_actors) and len(self.items) >= self.max_actors

    def sweep(self, **filters) -> tuple[int, int]:
        """Один запрос с фильтрами. Возвращает (получено записей, из них новых)."""
        params = {k: v for k, v in filters.items() if v}
        if self.include_unrunnable:
            params["includeUnrunnableActors"] = "true"
        params.update(limit=PAGE_LIMIT, offset=0)
        data = get_json(self._session(), self.url, params)
        time.sleep(self.delay)
        with self.lock:
            self.requests_made += 1
        if not data:
            return 0, 0
        batch = data.get("items", [])
        new = 0
        with self.lock:
            for it in batch:
                key = f'{it.get("username")}/{it.get("name")}'
                if key not in self.items:
                    self.items[key] = it
                    new += 1
                for c in it.get("categories") or []:
                    self.seen_categories.add(c)
        return len(batch), new

    def _sweep_guarded(self, filters: dict) -> tuple[int, int]:
        if self.full():
            return 0, 0
        try:
            return self.sweep(**filters)
        except Exception as exc:  # один упавший запрос не должен ронять весь прогон
            with self.lock:
                self.failed += 1
            log.error("Запрос %s не удался: %s", filters, exc)
            return 0, 0

    def run(self, jobs: list[dict], label: str = "") -> list[tuple[dict, int, int]]:
        """Параллельно выполняет список наборов фильтров. -> [(filters, n, new)]"""
        results: list[tuple[dict, int, int]] = []
        if not jobs:
            return results
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futs = {ex.submit(self._sweep_guarded, f): f for f in jobs}
            for i, fut in enumerate(as_completed(futs), 1):
                n, new = fut.result()
                results.append((futs[fut], n, new))
                if label and i % 500 == 0:
                    log.info("%s: %s/%s запросов, уникальных Actor'ов: %s", label, i, len(jobs), len(self.items))
        return results

    def probe(self) -> int:
        """Проверяет доступность API и флага includeUnrunnableActors, возвращает total."""
        params = {"limit": 1, "offset": 0}
        if self.include_unrunnable:
            params["includeUnrunnableActors"] = "true"
        data = get_json(self._session(), self.url, params)
        if data is None and self.include_unrunnable:
            log.warning("API отклонил includeUnrunnableActors, продолжаем без него")
            self.include_unrunnable = False
            data = get_json(self._session(), self.url, {"limit": 1, "offset": 0})
        return int((data or {}).get("total", 0))

    # -- фазы обхода -----------------------------------------------------------
    def phase_slices(self, sorts: list[str], deep: bool) -> None:
        """Глобальные срезы + категории (с дроблением, если упёрлись в 1000)."""
        jobs = [dict(sortBy="popularity")]
        if deep:
            jobs += [dict(pricingModel=pm, sortBy=s) for pm in PRICING_MODELS for s in sorts]
        self.run(jobs)

        done: set[str] = set()
        while not self.full():
            todo = sorted((set(SEED_CATEGORIES) | self.seen_categories) - done)
            if not todo:
                break
            done.update(todo)
            res = self.run([dict(category=c, sortBy="popularity") for c in todo])
            for f, n, _ in res:
                log.info("Категория %-18s первый срез: %4s, уникальных всего: %s", f["category"], n, len(self.items))
            if deep:
                saturated = [f["category"] for f, n, _ in res if n >= PAGE_LIMIT]
                self.run([dict(category=c, pricingModel=pm, sortBy=s)
                          for c in saturated for pm in PRICING_MODELS for s in sorts])

    def phase_usernames(self) -> None:
        """По каждому известному разработчику вытягиваем все его Actor'ы."""
        while not self.full():
            with self.lock:
                todo = sorted({it["username"] for it in self.items.values() if it.get("username")} - self.users_done)
            if not todo:
                break
            self.users_done.update(todo)
            before = len(self.items)
            log.info("Разработчики: запрашиваю %s, Actor'ов сейчас %s", len(todo), before)
            res = self.run([dict(username=u, sortBy="popularity") for u in todo], label="Разработчики")
            # у разработчика >= 1000 Actor'ов -> дробим по модели цены и сортировке
            big = [f["username"] for f, n, _ in res if n >= PAGE_LIMIT]
            self.run([dict(username=u, pricingModel=pm, sortBy=s)
                      for u in big for pm in PRICING_MODELS for s in ("popularity", "newest")])
            log.info("Разработчики: +%s новых, всего %s", len(self.items) - before, len(self.items))

    def candidate_terms(self, limit: int) -> list[str]:
        """Слова из названий/описаний найденных Actor'ов, по которым ещё не искали (частые первыми)."""
        cnt: Counter = Counter()
        with self.lock:
            items = list(self.items.values())
        for it in items:
            text = f'{it.get("title") or ""} {it.get("name") or ""} {it.get("description") or ""}'.lower()
            cnt.update(set(TOKEN_RE.findall(text)))
        return [t for t, _ in cnt.most_common()
                if t not in self.terms_done and t not in STOPWORDS][:limit]

    def phase_search(self, batch: int = 300) -> None:
        while not self.full() and len(self.terms_done) < self.max_terms:
            terms = self.candidate_terms(min(batch, self.max_terms - len(self.terms_done)))
            if not terms:
                break
            self.terms_done.update(terms)
            before = len(self.items)
            res = self.run([dict(search=t, sortBy="relevance") for t in terms], label="Поиск")
            saturated = [f["search"] for f, n, _ in res if n >= PAGE_LIMIT]
            self.run([dict(search=t, pricingModel=pm, sortBy=s)
                      for t in saturated for pm in PRICING_MODELS for s in ("popularity", "newest")])
            gained = len(self.items) - before
            log.info("Поиск: %s слов (всего %s), +%s новых, уникальных всего: %s",
                     len(terms), len(self.terms_done), gained, len(self.items))
            if gained < MIN_NEW_PER_SEARCH_BATCH:
                break

    def collect(self, mode: str = "full") -> tuple[list[dict], int]:
        total = self.probe()
        log.info("Всего Actor'ов в Store по данным API: %s (цель сбора: %s)",
                 total, self.max_actors or "без ограничения")

        deep = mode == "full"
        self.phase_slices(SORTS if deep else ["popularity"], deep)
        log.info("После срезов и категорий: %s Actor'ов", len(self.items))

        if deep:
            for rnd in range(1, 6):
                if self.full():
                    break
                before = len(self.items)
                self.phase_usernames()
                self.phase_search()
                if len(self.items) == before:
                    break
                log.info("Раунд %s завершён: %s Actor'ов", rnd, len(self.items))

        items = list(self.items.values())
        if self.max_actors:
            items = items[: self.max_actors]
        return items, total

    # -- Pricing: докачка деталей ---------------------------------------------
    def enrich_pricing(self, items: list[dict], mode: str = "auto") -> None:
        if mode == "never":
            return
        targets = []
        for it in items:
            pi = it.get("currentPricingInfo") or {}
            if pi.get("pricingModel") in (None, "FREE"):
                continue
            if mode == "always" or needs_pricing_detail(pi):
                targets.append(it)
        if not targets:
            log.info("Pricing: деталей докачивать не нужно")
            return
        log.info("Pricing: докачиваю детали цены для %s платных Actor'ов", len(targets))

        def work(it: dict) -> bool:
            data = get_json(self._session(), f'{ACTORS_URL}/{it["username"]}~{it["name"]}', {})
            time.sleep(self.delay)
            cur = pick_current_pricing(data) if data else None
            if cur:
                it["currentPricingInfo"] = {**(it.get("currentPricingInfo") or {}), **cur}
                return True
            return False

        ok = fail = 0
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futs = [ex.submit(work, it) for it in targets]
            for i, fut in enumerate(as_completed(futs), 1):
                try:
                    ok += bool(fut.result())
                except Exception as exc:
                    fail += 1
                    log.error("Pricing: %s", exc)
                if i % 500 == 0:
                    log.info("Pricing: %s/%s", i, len(targets))
        log.info("Pricing: обновлено %s, не удалось %s, без данных %s", ok, fail, len(targets) - ok - fail)



# --------------------------------------------------------------------------
# Нормализация
# --------------------------------------------------------------------------
def normalize(item: dict) -> tuple[dict, list[str]]:
    """Сырой Actor из API -> (плоская строка для actor_snapshots, список категорий)."""
    stats = item.get("stats") or {}
    count = item.get("actorReviewCount", stats.get("actorReviewCount")) or 0
    rating = item.get("actorReviewRating", stats.get("actorReviewRating"))
    # API отдаёт 0 для Actor'ов без отзывов: это "нет оценки", а не плохая оценка
    rating = round(float(rating), 3) if (count and rating) else None
    username, name = (item.get("username") or "")[:128], (item.get("name") or "")[:255]
    row = {
        "actor_key": f"{username}/{name}",
        "apify_id": item.get("id") if isinstance(item.get("id"), str) and re.fullmatch(r"[A-Za-z0-9]{17}", item["id"]) else None,
        "username": username,
        "name": name,
        "title": (item.get("title") or name)[:512],
        "url": (item.get("url") or f"https://apify.com/{username}/{name}")[:512],
        "rating": rating,
        "review_count": int(count),
        "total_users": int(stats.get("totalUsers") or 0),
        "users_30d": int(stats.get("totalUsers30Days") or 0),
        "total_runs": int(stats.get("totalRuns") or 0),
    }
    row.update(parse_pricing(item.get("currentPricingInfo")))
    row.pop("pricing_json", None)  # сырой JSON цены в MySQL не храним (раздувает историю)
    # строгий режим MySQL роняет всю транзакцию на слишком длинной строке - режем заранее под размеры колонок
    row["pricing_unit"] = (row["pricing_unit"] or None) and str(row["pricing_unit"])[:64]
    row["pricing_summary"] = (row["pricing_summary"] or None) and row["pricing_summary"][:1000]
    return row, [c for c in (item.get("categories") or []) if c]
