"""Слой MySQL: подключение, схема, запись снимка и рейтингов, чтение снимка для пересчёта.

Все записи идут в транзакциях: фронт (представления v_*) видит либо прошлый полный снимок, либо новый
полный, но никогда - половину. Новый снимок «публикуется» последней командой (runs.scored_at).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Sequence
from urllib.parse import unquote, urlparse

import numpy as np
import pandas as pd
import pymysql

from .config import ScoreConfig
from .scoring import Rankings

log = logging.getLogger("apify-tracker")

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "schema.sql"
DEFAULT_DSN = "mysql://apify:apify@127.0.0.1:3306/apify_tracker"
CHUNK = 2000

SNAPSHOT_COLS = ["rating", "review_count", "total_users", "users_30d", "total_runs", "pricing_model",
                 "pricing_price_usd", "pricing_unit", "pricing_trial_min", "pricing_summary", "pricing_events"]


# --------------------------------------------------------------------------
# Подключение
# --------------------------------------------------------------------------
def dsn_from_env(cli_value: str | None = None) -> str:
    return cli_value or os.environ.get("MYSQL_DSN") or DEFAULT_DSN


def connect(dsn: str, **kw) -> pymysql.connections.Connection:
    """DSN вида mysql://user:password@host:3306/dbname"""
    u = urlparse(dsn)
    if u.scheme not in ("mysql", "mysql+pymysql"):
        raise ValueError(f"Ожидается DSN вида mysql://user:pass@host:3306/db, получено: {u.scheme}://...")
    return pymysql.connect(
        host=u.hostname or "127.0.0.1", port=u.port or 3306, user=unquote(u.username or ""),
        password=unquote(u.password or ""), database=(u.path or "/").lstrip("/"),
        charset="utf8mb4", autocommit=False, **kw)


@contextmanager
def transaction(conn) -> Iterator[pymysql.cursors.Cursor]:
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def init_schema(conn) -> None:
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    with conn.cursor() as cur:
        for stmt in (s.strip() for s in sql.split(";")):
            if stmt:
                cur.execute(stmt)
    conn.commit()
    log.info("Схема MySQL применена")


# --------------------------------------------------------------------------
# Вспомогательное
# --------------------------------------------------------------------------
def _py(x):
    """numpy/NaN -> типы, понятные PyMySQL (None для пропусков)."""
    if x is None or x is pd.NA or x is pd.NaT:
        return None
    if isinstance(x, np.generic):
        x = x.item()
    if isinstance(x, float) and x != x:
        return None
    return x


def _rows(df: pd.DataFrame, cols: Sequence[str]) -> list[tuple]:
    return [tuple(_py(v) for v in r) for r in df[list(cols)].itertuples(index=False, name=None)]


def _exec_many(cur, sql: str, rows: list[tuple]) -> None:
    for i in range(0, len(rows), CHUNK):
        cur.executemany(sql, rows[i:i + CHUNK])


def _placeholders(n: int) -> str:
    return ",".join(["%s"] * n)


# --------------------------------------------------------------------------
# Запись сырого снимка
# --------------------------------------------------------------------------
def persist_snapshot(conn, snapshot_date: str, rows: list[dict], categories: dict[str, list[str]],
                     store_total: int) -> int:
    """Сохраняет Actor'ов, их дневной снимок и категории. Возвращает run_id.
    Снимок НЕ виден фронту, пока не вызван persist_rankings (scored_at = NULL)."""
    with transaction(conn) as cur:
        cur.execute(
            "INSERT INTO runs (snapshot_date, collected, store_total, finished_at, scored_at) "
            "VALUES (%s,%s,%s,%s,NULL) ON DUPLICATE KEY UPDATE collected=VALUES(collected), "
            "store_total=VALUES(store_total), finished_at=VALUES(finished_at), scored_at=NULL",
            (snapshot_date, len(rows), store_total, dt.datetime.now().replace(microsecond=0)))
        cur.execute("SELECT run_id FROM runs WHERE snapshot_date=%s", (snapshot_date,))
        run_id = cur.fetchone()[0]

        _exec_many(cur,
                   "INSERT INTO actors (actor_key, username, name, title, url, first_seen, last_seen) "
                   "VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE username=VALUES(username), "
                   "name=VALUES(name), title=VALUES(title), url=VALUES(url), "
                   "last_seen=GREATEST(last_seen, VALUES(last_seen))",
                   [(r["actor_key"], r["username"], r["name"], r["title"], r["url"], snapshot_date, snapshot_date)
                    for r in rows])
        cur.execute("SELECT actor_key, actor_pk FROM actors")
        pk = dict(cur.fetchall())

        cur.execute("DELETE FROM actor_snapshots WHERE run_id=%s", (run_id,))
        _exec_many(cur,
                   f"INSERT INTO actor_snapshots (run_id, actor_pk, {','.join(SNAPSHOT_COLS)}) "
                   f"VALUES (%s,%s,{_placeholders(len(SNAPSHOT_COLS))})",
                   [(run_id, pk[r["actor_key"]], *(_py(r.get(c)) for c in SNAPSHOT_COLS)) for r in rows])

        # категории: текущее состояние (не история)
        codes = sorted({c for cs in categories.values() for c in cs})
        if codes:
            cur.executemany("INSERT IGNORE INTO categories (code) VALUES (%s)", [(c,) for c in codes])
        cur.execute("SELECT code, category_id FROM categories")
        cat_id = dict(cur.fetchall())
        pks = [pk[r["actor_key"]] for r in rows]
        for i in range(0, len(pks), 1000):
            part = pks[i:i + 1000]
            cur.execute(f"DELETE FROM actor_categories WHERE actor_pk IN ({_placeholders(len(part))})", part)
        _exec_many(cur, "INSERT IGNORE INTO actor_categories (actor_pk, category_id) VALUES (%s,%s)",
                   [(pk[k], cat_id[c]) for k, cs in categories.items() for c in cs])
    log.info("MySQL: сохранён снимок %s (run_id=%s), Actor'ов: %s", snapshot_date, run_id, len(rows))
    return run_id


# --------------------------------------------------------------------------
# Чтение снимка (для --rescore)
# --------------------------------------------------------------------------
def latest_run(conn) -> tuple[int, str] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT run_id, snapshot_date FROM runs ORDER BY snapshot_date DESC LIMIT 1")
        r = cur.fetchone()
    conn.commit()
    return (r[0], r[1].isoformat()) if r else None


def load_snapshot(conn, run_id: int) -> pd.DataFrame:
    sql = ("SELECT a.actor_key, a.username, a.name, a.title, a.url, s.rating, s.review_count, s.total_users, "
           "s.users_30d, s.total_runs, s.pricing_model, s.pricing_price_usd, s.pricing_unit, "
           "s.pricing_trial_min, s.pricing_summary, s.pricing_events "
           "FROM actor_snapshots s JOIN actors a ON a.actor_pk = s.actor_pk WHERE s.run_id=%s")
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(sql, (run_id,))
        df = pd.DataFrame(cur.fetchall())
    conn.commit()
    for c in ("rating", "pricing_price_usd"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# --------------------------------------------------------------------------
# Запись рейтингов
# --------------------------------------------------------------------------
def persist_rankings(conn, run_id: int, rk: Rankings, cfg: ScoreConfig) -> None:
    """Пересобирает все производные таблицы для снимка и публикует его (scored_at)."""
    with transaction(conn) as cur:
        cur.execute("SELECT a.actor_key, a.actor_pk FROM actor_snapshots s "
                    "JOIN actors a ON a.actor_pk = s.actor_pk WHERE s.run_id=%s", (run_id,))
        pk = dict(cur.fetchall())
        for t in ("actor_metrics", "service_top", "niche_stats", "service_stats"):
            cur.execute(f"DELETE FROM {t} WHERE run_id=%s", (run_id,))
        cur.execute("DELETE FROM actor_services")

        m = rk.metrics.assign(actor_pk=rk.metrics["actor_key"].map(pk), run_id=run_id)
        _exec_many(cur, "INSERT INTO actor_metrics (run_id, actor_pk, cost_1000, cost_basis, bayes_rating) "
                        "VALUES (%s,%s,%s,%s,%s)",
                   _rows(m, ["run_id", "actor_pk", "cost_1000", "cost_basis", "bayes_rating"]))

        if not rk.niches.empty:
            _exec_many(cur, "INSERT INTO niches (slug, name, kind) VALUES (%s,%s,%s) "
                            "ON DUPLICATE KEY UPDATE name=VALUES(name), kind=VALUES(kind)",
                       _rows(rk.niches, ["slug", "name", "kind"]))
            cur.execute("SELECT slug, niche_id FROM niches")
            niche_id = dict(cur.fetchall())
            nk2id = {r.niche_key: niche_id[r.slug] for r in rk.niches.itertuples()}

            sv = rk.services.assign(niche_id=rk.services["niche_key"].map(nk2id))
            _exec_many(cur, "INSERT INTO services (niche_id, service_key, name, display_name, kind) "
                            "VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE name=VALUES(name), "
                            "display_name=VALUES(display_name), kind=VALUES(kind)",
                       _rows(sv, ["niche_id", "service_key", "name", "display_name", "kind"]))
            cur.execute("SELECT niche_id, service_key, service_id FROM services")
            sid = {(n, k): i for n, k, i in cur.fetchall()}

            def svc(df: pd.DataFrame) -> pd.Series:
                return pd.Series([sid[(nk2id[n], s)] for n, s in zip(df["niche_key"], df["service_key"])], index=df.index)

            mem = rk.membership.assign(actor_pk=rk.membership["actor_key"].map(pk), service_id=svc(rk.membership))
            _exec_many(cur, "INSERT IGNORE INTO actor_services (actor_pk, service_id) VALUES (%s,%s)",
                       _rows(mem, ["actor_pk", "service_id"]))

            if not rk.top.empty:
                top = rk.top.assign(run_id=run_id, actor_pk=rk.top["actor_key"].map(pk), service_id=svc(rk.top))
                cols = ["run_id", "service_id", "rank_no", "actor_pk", "value_score", "s_price", "s_users",
                        "s_rating", "s_reviews", "score_scope", "pool_size"]
                _exec_many(cur, f"INSERT INTO service_top ({','.join(cols)}) VALUES ({_placeholders(len(cols))})",
                           _rows(top, cols))
            ns = rk.niche_stats.assign(run_id=run_id, niche_id=rk.niche_stats["niche_key"].map(nk2id))
            cols = ["run_id", "niche_id", "actors", "priced", "eligible", "services", "total_users",
                    "median_cost_1000", "min_cost_1000", "median_rating"]
            _exec_many(cur, f"INSERT INTO niche_stats ({','.join(cols)}) VALUES ({_placeholders(len(cols))})",
                       _rows(ns, cols))
            ss = rk.service_stats.assign(run_id=run_id, service_id=svc(rk.service_stats))
            cols = ["run_id", "service_id", "actors", "eligible", "total_users", "median_cost_1000", "min_cost_1000"]
            _exec_many(cur, f"INSERT INTO service_stats ({','.join(cols)}) VALUES ({_placeholders(len(cols))})",
                       _rows(ss, cols))

        cur.execute("UPDATE runs SET scored_at=%s, config=%s WHERE run_id=%s",
                    (dt.datetime.now().replace(microsecond=0), json.dumps(cfg.to_dict(), ensure_ascii=False), run_id))
    log.info("MySQL: рейтинги опубликованы (run_id=%s)", run_id)


# --------------------------------------------------------------------------
# Обслуживание
# --------------------------------------------------------------------------
def prune(conn, keep_days: int) -> None:
    """Удаляет снимки старше keep_days дней (дочерние строки уходят каскадом)."""
    if keep_days <= 0:
        return
    with transaction(conn) as cur:
        cur.execute("DELETE FROM runs WHERE snapshot_date < CURDATE() - INTERVAL %s DAY", (keep_days,))
        removed = cur.rowcount
        cur.execute("DELETE FROM actors WHERE last_seen < CURDATE() - INTERVAL %s DAY", (keep_days,))
    if removed:
        log.info("Удалено старых снимков: %s", removed)


def fetch_df(conn, sql: str, params: Sequence | None = None) -> pd.DataFrame:
    """SELECT -> DataFrame (Decimal приводятся к float)."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
    conn.commit()
    df = pd.DataFrame(rows)
    for c in df.columns:
        if df[c].map(lambda v: hasattr(v, "as_tuple")).any():
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df
