"""Параметры расчёта «выгодности». Всё, что помечено «допущение», меняется через CLI."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class ScoreConfig:
    per_service: int = 3               # сколько лучших Actor'ов показывать в каждой услуге (по условию задачи - 3)
    min_niche_actors: int = 5          # минимум Actor'ов, чтобы считать нишей
    min_service_actors: int = 3        # минимум Actor'ов, чтобы считать услугу самостоятельной (иначе -> "general")
    min_service_pool: int = 5          # если в услуге подходящих Actor'ов меньше, шкалы берутся по всей нише
    min_discovered_service: int = 8    # сколько Actor'ов в нише должно упоминать слово, чтобы оно стало услугой
    auto_services: bool = True         # автоподбор услуг из названий (кроме встроенного справочника)
    min_users: int = 10                # отсекаем «мёртвых» Actor'ов (мало пользователей)
    compute_per_1000: float = 0.5      # допущение: $ за compute на 1000 результатов (FREE и аренда)
    monthly_results: int = 10_000      # допущение: объём в месяц для пересчёта аренды в $/1000
    bayes_prior: float = 5.0           # «вес» среднего по рынку в байесовской оценке (в отзывах)
    w_price: float = 0.45
    w_users: float = 0.20
    w_rating: float = 0.20
    w_reviews: float = 0.15
    niches_file: Path | None = None    # JSON с ручными правками ниш и услуг (см. README)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["niches_file"] = str(self.niches_file) if self.niches_file else None
        return d


def parse_weights(text: str) -> tuple[float, float, float, float]:
    parts = [float(x) for x in text.replace(";", ",").split(",")]
    if len(parts) != 4 or min(parts) < 0 or sum(parts) <= 0:
        raise ValueError("--weights: нужно 4 неотрицательных числа: цена,пользователи,оценка,число оценок")
    s = sum(parts)
    return tuple(p / s for p in parts)  # type: ignore[return-value]
