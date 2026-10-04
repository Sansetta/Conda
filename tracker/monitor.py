"""Таблица 1: мониторинг цен всех акторов Store (actor_price_monitor).

Одна строка на Apify ID. Каждый проход (после ежедневного сбора или отдельный --monitor-prices) обновляет:
текущую цену, оценку $/1000, оценку и число пользователей, и фиксирует изменение цены:
prev_cost_1000 / price_changed_at / change_pct (минус = скидка) / peak_cost_1000 (максимум за всё время).
Фильтров нет: в таблицу попадают и дорогие, и бесплатные, и без рейтинга - скидка может появиться у любого.
Рерайт к этой таблице не относится: названия и описания здесь оригинальные, их не трогают.
"""
from __future__ import annotations

import datetime as dt
import logging

from .config import ScoreConfig
from .ids import valid_id
from .scoring import estimate_cost

log = logging.getLogger("apify-tracker")

COLS = ["apify_id", "actor_key", "username", "name", "title", "url", "pricing_model", "pricing_unit", "price_usd",
        "pricing_summary", "cost_1000", "cost_basis", "prev_cost_1000", "prev_pricing_summary", "price_changed_at",
        "change_pct", "peak_cost_1000", "rating", "review_count", "total_users", "users_30d", "is_active",
        "first_seen_at", "last_seen_at"]
KEEP_ON_UPDATE = {"apify_id", "first_seen_at"}
CHUNK = 2000
BIG_DROP_PCT = -20.0     # изменения цены сильнее этого пишутся в лог как «скидка»


def _f(x) -> float | None:
    return None if x is None or x != x else float(x)


def _sig(model, price, cost, summary) -> tuple:
    return (model, None if price is None else round(float(price), 8),
            None if cost is None else round(float(cost), 4), summary or None)


def compute_rows(existing: dict[str, dict], rows: list[dict], cfg: ScoreConfig, now: dt.datetime
                 ) -> tuple[list[tuple], dict, list[dict]]:
    """Чистая функция (без БД): -> (кортежи для upsert, статистика, заметные изменения цены)."""
    out: list[tuple] = []
    stats = {"new": 0, "changed": 0, "unchanged": 0, "no_id": 0}
    notable: list[dict] = []
    seen: set[str] = set()
    for r in rows:
        aid = valid_id(r.get("apify_id"))
        if not aid:
            stats["no_id"] += 1
            continue
        if aid in seen:
            continue
        seen.add(aid)
        cost, basis = estimate_cost(r, cfg)
        cost, price = _f(cost), _f(r.get("pricing_price_usd"))
        old = existing.get(aid)
        prev_cost = prev_summary = changed_at = change_pct = None
        peak = cost
        first_seen = now
        if old:
            first_seen = old["first_seen_at"]
            peak = max([v for v in (_f(old.get("peak_cost_1000")), cost) if v is not None], default=None)
            prev_cost, prev_summary = _f(old.get("prev_cost_1000")), old.get("prev_pricing_summary")
            changed_at, change_pct = old.get("price_changed_at"), _f(old.get("change_pct"))
            if _sig(old.get("pricing_model"), old.get("price_usd"), old.get("cost_1000"), old.get("pricing_summary")) != \
                    _sig(r.get("pricing_model"), price, cost, r.get("pricing_summary")):
                stats["changed"] += 1
                old_cost = _f(old.get("cost_1000"))
                prev_cost, prev_summary, changed_at = old_cost, old.get("pricing_summary"), now
                change_pct = round((cost - old_cost) / old_cost * 100, 2) if cost is not None and old_cost else None
                if change_pct is not None and change_pct <= BIG_DROP_PCT:
                    notable.append({"apify_id": aid, "actor": r["actor_key"], "from": old_cost, "to": cost, "pct": change_pct})
            else:
                stats["unchanged"] += 1
        else:
            stats["new"] += 1
        vals = {
            "apify_id": aid, "actor_key": r["actor_key"], "username": r["username"], "name": r["name"],
            "title": r["title"], "url": r["url"], "pricing_model": r.get("pricing_model"),
            "pricing_unit": r.get("pricing_unit"), "price_usd": price,
            "pricing_summary": r.get("pricing_summary"), "cost_1000": cost, "cost_basis": basis,
            "prev_cost_1000": prev_cost, "prev_pricing_summary": prev_summary, "price_changed_at": changed_at,
            "change_pct": change_pct, "peak_cost_1000": peak, "rating": _f(r.get("rating")),
            "review_count": int(r.get("review_count") or 0), "total_users": int(r.get("total_users") or 0),
            "users_30d": int(r.get("users_30d") or 0), "is_active": 1, "first_seen_at": first_seen, "last_seen_at": now,
        }
        out.append(tuple(vals[c] for c in COLS))
    return out, stats, notable


def load_existing(conn) -> dict[str, dict]:
    cur = conn.cursor()
    cur.execute("SELECT apify_id, pricing_model, price_usd, pricing_summary, cost_1000, prev_cost_1000, "
                "prev_pricing_summary, price_changed_at, change_pct, peak_cost_1000, first_seen_at "
                "FROM actor_price_monitor")
    names = ["pricing_model", "price_usd", "pricing_summary", "cost_1000", "prev_cost_1000",
             "prev_pricing_summary", "price_changed_at", "change_pct", "peak_cost_1000", "first_seen_at"]
    out = {r[0]: dict(zip(names, r[1:])) for r in cur.fetchall()}
    cur.close()
    conn.commit()
    return out


def sync(conn, rows: list[dict], cfg: ScoreConfig, mark_inactive: bool = True) -> dict:
    """Обновляет actor_price_monitor по свежему обходу Store. mark_inactive=True ставит is_active=0 актёрам,
    которых не было в обходе (передавайте True только при достаточном покрытии Store)."""
    now = dt.datetime.now().replace(microsecond=0)
    existing = load_existing(conn)
    data, stats, notable = compute_rows(existing, rows, cfg, now)
    sql = (f"INSERT INTO actor_price_monitor ({','.join(COLS)}) VALUES ({','.join(['%s'] * len(COLS))}) "
           "ON DUPLICATE KEY UPDATE " + ", ".join(f"{c}=VALUES({c})" for c in COLS if c not in KEEP_ON_UPDATE))
    cur = conn.cursor()
    try:
        for i in range(0, len(data), CHUNK):   # партиями: частичный прогресс для монитора безопасен
            cur.executemany(sql, data[i:i + CHUNK])
            conn.commit()
        stats["deactivated"] = 0
        if mark_inactive and data:
            cur.execute("UPDATE actor_price_monitor SET is_active=0 WHERE is_active=1 AND last_seen_at < %s", (now,))
            stats["deactivated"] = cur.rowcount
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    for n in notable[:50]:
        log.info("СКИДКА %s: %.4f -> %.4f $/1000 (%.1f%%)", n["actor"], n["from"], n["to"], n["pct"])
    log.info("Мониторинг цен: %s (заметных скидок: %s)", stats, len(notable))
    return stats
