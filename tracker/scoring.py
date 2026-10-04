"""Оценка стоимости и «условной выгоды» (Value score), топ-N Actor'ов внутри каждой услуги каждой ниши.

Value score (0-100) = 100 x (45% цена + 20% пользователи + 20% оценка + 15% число оценок).
Компоненты нормируются (0..1) внутри «пула сравнения»:
  * пул = подходящие Actor'ы самой услуги, если их >= min_service_pool;
  * иначе пул = подходящие Actor'ы всей ниши (иначе шкала на 1-3 Actor'ах вырождается).
Какой пул использован, сохраняется в service_top.score_scope ('service' | 'niche').
"""
from __future__ import annotations

import json
import re
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .config import ScoreConfig
from .niches import assign_niches, niche_name, niche_slug
from .services import assign_services

log = logging.getLogger("apify-tracker")

# Событие «за запуск» (а не «за результат») в pay-per-event
START_RE = re.compile(r"(^|[\W_])(start|init|initiali[sz]e|launch|setup|run)([\W_]|$)")


def estimate_cost(row, cfg: ScoreConfig) -> tuple[float | None, str]:
    """Приводит любую модель цены к оценке «$ за 1000 результатов» (None, если оценить нельзя).

    PRICE_PER_DATASET_ITEM  цена за результат x 1000 (compute уже включён в цену)
    PAY_PER_EVENT           цена «за запуск» (1 раз) + 1000 x медиана цен остальных событий
    FLAT_PRICE_PER_MONTH    аренда / (monthly_results/1000) + compute  (compute платит пользователь)
    FREE                    только compute (допущение compute_per_1000)
    """
    model, price = row["pricing_model"], row["pricing_price_usd"]
    if model == "FREE":
        return cfg.compute_per_1000, "free + compute"
    if model == "PRICE_PER_DATASET_ITEM" and pd.notna(price):
        return float(price) * 1000, "pay per result"
    if model == "FLAT_PRICE_PER_MONTH" and pd.notna(price):
        return (float(price) / (cfg.monthly_results / 1000) + cfg.compute_per_1000,
                f"rental / {cfg.monthly_results:,} results + compute")
    if model == "PAY_PER_EVENT":
        try:
            events = json.loads(row["pricing_events"]) if row["pricing_events"] else {}
        except (TypeError, ValueError):
            events = {}
        per_run, unit = 0.0, []
        for key, ev in events.items():
            p = (ev or {}).get("price_usd")
            if p is None:
                continue
            if START_RE.search(f"{key} {(ev or {}).get('title') or ''}".lower()):
                per_run += p
            else:
                unit.append(p)
        if unit:
            return per_run + 1000 * float(np.median(unit)), f"pay per event (median of {len(unit)})"
        return None, "no per-result event"
    return None, "no price data"



def _norm_ref(v: pd.Series, ref: pd.Series, log_scale: bool) -> pd.Series:
    """Нормализация v в 0..1 по шкале пула ref: (лог-)шкала, обрезка по 5-му и 95-му перцентилям."""
    f = (lambda s: np.log1p(s.astype(float))) if log_scale else (lambda s: s.astype(float))
    lo, hi = f(ref).quantile(0.05), f(ref).quantile(0.95)
    if not np.isfinite(lo) or not np.isfinite(hi) or hi - lo < 1e-9:
        return pd.Series(0.5, index=v.index)
    return (f(v).clip(lo, hi) - lo) / (hi - lo)


def score_group(g: pd.DataFrame, pool: pd.DataFrame, cfg: ScoreConfig) -> pd.DataFrame:
    g = g.copy()
    g["s_price"] = 1 - _norm_ref(g["cost_1000"], pool["cost_1000"], True)
    g["s_users"] = (0.5 * _norm_ref(g["total_users"], pool["total_users"], True)
                    + 0.5 * _norm_ref(g["users_30d"], pool["users_30d"], True))
    g["s_rating"] = _norm_ref(g["bayes_rating"], pool["bayes_rating"], False)
    g["s_reviews"] = _norm_ref(g["review_count"], pool["review_count"], True)
    g["value_score"] = 100 * (cfg.w_price * g["s_price"] + cfg.w_users * g["s_users"]
                              + cfg.w_rating * g["s_rating"] + cfg.w_reviews * g["s_reviews"])
    return g


@dataclass
class Rankings:
    metrics: pd.DataFrame        # actor_key, cost_1000, cost_basis, bayes_rating (по всем Actor'ам снимка)
    niches: pd.DataFrame         # niche_key, slug, name, kind
    services: pd.DataFrame       # niche_key, service_key, name, display_name, kind
    membership: pd.DataFrame     # actor_key, niche_key, service_key
    top: pd.DataFrame            # niche_key, service_key, rank_no, actor_key, value_score, s_*, score_scope, pool_size
    niche_stats: pd.DataFrame
    service_stats: pd.DataFrame


def build_rankings(cur: pd.DataFrame, cfg: ScoreConfig) -> Rankings:
    """cur - последний снимок: по строке на Actor'а (колонки как в actor_snapshots + title/name/username)."""
    cur = cur.copy().reset_index(drop=True)
    est = cur.apply(lambda r: estimate_cost(r, cfg), axis=1)
    cur["cost_1000"] = [c for c, _ in est]
    cur["cost_basis"] = [b for _, b in est]

    # байесовская оценка: мало отзывов -> оценка подтягивается к среднему по рынку
    rated = cur[cur["review_count"] > 0]
    global_mean = float((rated["rating"] * rated["review_count"]).sum() / rated["review_count"].sum()) \
        if len(rated) else 4.0
    v = cur["review_count"].astype(float)
    cur["bayes_rating"] = (v * cur["rating"].fillna(0) + cfg.bayes_prior * global_mean) / (v + cfg.bayes_prior)
    metrics = cur[["actor_key", "cost_1000", "cost_basis", "bayes_rating"]]

    members, niche_meta = assign_niches(cur, cfg)
    empty = pd.DataFrame()
    if members.empty:
        log.warning("Ниши не найдены (слишком мало Actor'ов?)")
        return Rankings(metrics, empty, empty, empty, empty, empty, empty)
    membership, services = assign_services(cur, members, niche_meta, cfg)

    niches = niche_meta[["niche_key", "kind"]].assign(
        slug=lambda d: d["niche_key"].map(niche_slug), name=lambda d: d["niche_key"].map(niche_name))

    cur["eligible"] = cur["cost_1000"].notna() & (cur["total_users"] >= cfg.min_users)
    by_key = cur.set_index("actor_key")
    top_parts, svc_stats, niche_stats = [], [], []

    for niche, nm in membership.groupby("niche_key"):
        n_actors = by_key.loc[nm["actor_key"].unique()].reset_index()
        n_pool = n_actors[n_actors["eligible"]]
        priced = n_actors[n_actors["cost_1000"].notna()]
        niche_stats.append({
            "niche_key": niche, "actors": len(n_actors), "priced": len(priced), "eligible": len(n_pool),
            "services": nm["service_key"].nunique(), "total_users": int(n_actors["total_users"].sum()),
            "median_cost_1000": priced["cost_1000"].median() if len(priced) else None,
            "min_cost_1000": priced["cost_1000"].min() if len(priced) else None,
            "median_rating": n_actors["rating"].median() if n_actors["rating"].notna().any() else None,
        })
        for service, sm in nm.groupby("service_key"):
            s_actors = by_key.loc[sm["actor_key"]].reset_index()
            s_elig = s_actors[s_actors["eligible"]]
            s_priced = s_actors[s_actors["cost_1000"].notna()]
            scope = "service" if len(s_elig) >= cfg.min_service_pool else "niche"
            pool = s_elig if scope == "service" else n_pool
            svc_stats.append({
                "niche_key": niche, "service_key": service, "actors": len(s_actors), "eligible": len(s_elig),
                "total_users": int(s_actors["total_users"].sum()),
                "median_cost_1000": s_priced["cost_1000"].median() if len(s_priced) else None,
                "min_cost_1000": s_priced["cost_1000"].min() if len(s_priced) else None,
            })
            if s_elig.empty:
                continue
            scored = score_group(s_elig, pool, cfg).sort_values(
                ["value_score", "cost_1000", "total_users"], ascending=[False, True, False]).head(cfg.per_service)
            scored["rank_no"] = range(1, len(scored) + 1)
            scored["niche_key"], scored["service_key"] = niche, service
            scored["score_scope"], scored["pool_size"] = scope, len(pool)
            top_parts.append(scored)

    cols = ["niche_key", "service_key", "rank_no", "actor_key", "value_score",
            "s_price", "s_users", "s_rating", "s_reviews", "score_scope", "pool_size"]
    top = pd.concat(top_parts, ignore_index=True)[cols] if top_parts else pd.DataFrame(columns=cols)
    log.info("Ниш: %s, услуг: %s, позиций в топе: %s", len(niches), len(services), len(top))
    return Rankings(metrics, niches, services, membership, top,
                    pd.DataFrame(niche_stats), pd.DataFrame(svc_stats))


def formula_text(cfg: ScoreConfig) -> list[tuple[str, str]]:
    """Человекочитаемое описание формулы (для листа «Value formula» и для /api/meta)."""
    return [
        ("Value score (0-100)",
         f"100 x ({cfg.w_price:.0%} Price + {cfg.w_users:.0%} Users + {cfg.w_rating:.0%} Rating + "
         f"{cfg.w_reviews:.0%} Reviews). Компоненты нормируются в 0..1 внутри пула сравнения."),
        ("Пул сравнения", f"Подходящие Actor'ы услуги, если их >= {cfg.min_service_pool}; иначе подходящие Actor'ы всей ниши."),
        ("Price score", "1 - норма(log(1 + стоимость за 1000 результатов)). Чем дешевле, тем ближе к 1."),
        ("Users score", "0.5 x норма(log(1 + всего пользователей)) + 0.5 x норма(log(1 + пользователи за 30 дней))."),
        ("Rating score", f"норма(байесовская оценка), вес среднего по рынку = {cfg.bayes_prior:g} отзывов."),
        ("Reviews score", "норма(log(1 + число оценок))."),
        ("норма()", "Min-max по пулу, значения обрезаются по 5-му и 95-му перцентилям."),
        ("Est. cost / 1000", "USD за 1000 результатов: pay-per-result = цена x 1000; pay-per-event = события «за запуск» "
                             "+ 1000 x медиана остальных; аренда = цена / (объём/1000) + compute; free = compute."),
        ("Допущения", f"compute = ${cfg.compute_per_1000:g}/1000 для FREE и аренды; объём аренды = {cfg.monthly_results:,} результатов/мес."),
        ("Фильтры", f"В рейтинг попадают Actor'ы с известной ценой и >= {cfg.min_users} пользователей. "
                    f"В каждой услуге показывается максимум {cfg.per_service}."),
    ]
