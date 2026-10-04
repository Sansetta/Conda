"""Услуги внутри ниши: «Instagram Post Scraper», «Instagram Followers Scraper», «Google Maps Reviews Scraper»...

Как определяется услуга Actor'а
-------------------------------
1. Из текста «title + slug» вырезается сама ниша (иначе «google search» в нише «google search» стал бы услугой).
2. Текст проверяется по справочнику SERVICE_TYPES (общий для всех ниш: посты, подписчики, отзывы, ...).
   Actor может попасть в несколько услуг («Profile & Post Scraper» -> profiles + posts).
3. Автоподбор: слова, которые часто встречаются в названиях Actor'ов именно этой ниши и не покрыты
   справочником («Instagram Location Scraper» -> услуга «Location»). Порог: --min-discovered-service.
4. Услуга, в которой меньше min_service_actors Actor'ов, не считается самостоятельной.
5. Всё, что не попало ни в одну услугу, - «general» (универсальный «<Ниша> Scraper»).
"""
from __future__ import annotations

import json
import math
import re
import warnings
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .apify_api import STOPWORDS
from .config import ScoreConfig
from .niches import GENERIC_WORDS, TASK_WORDS, niche_name, niche_texts


@dataclass(frozen=True)
class ServiceType:
    key: str
    label: str                  # «Post Scraper»; витринное имя = «<Ниша> <label>»
    patterns: tuple[str, ...]   # regex по нормализованному тексту (a-z0-9 и пробелы)


GENERAL = ServiceType("general", "Scraper (all-in-one)", ())

SERVICE_TYPES: list[ServiceType] = [
    ServiceType("followers", "Followers Scraper",
                (r"\bfollowers?\b", r"\bfollowing\b", r"\bsubscribers?\b", r"\bfans\b", r"\bfriends\b", r"\bmembers\b")),
    ServiceType("comments", "Comments Scraper", (r"\bcomments?\b", r"\breplies\b")),
    ServiceType("reviews", "Reviews Scraper", (r"\breviews?\b", r"\bratings?\b", r"\btestimonials?\b")),
    ServiceType("likes", "Likes & Reactions Scraper", (r"\blikes?\b", r"\breactions?\b", r"\blikers\b")),
    ServiceType("posts", "Post Scraper", (r"\bposts?\b", r"\btweets?\b", r"\bfeeds?\b", r"\btimeline\b", r"\barticles?\b")),
    ServiceType("videos", "Video / Reels Scraper", (r"\breels?\b", r"\bvideos?\b", r"\bshorts\b", r"\bclips?\b")),
    ServiceType("stories", "Stories Scraper", (r"\bstor(?:y|ies)\b",)),
    ServiceType("hashtags", "Hashtag & Trends Scraper", (r"\bhashtags?\b", r"\btrend(?:s|ing)?\b")),
    ServiceType("profiles", "Profile Scraper",
                (r"\bprofiles?\b", r"\busers?\b", r"\baccounts?\b", r"\bchannels?\b", r"\bcreators?\b", r"\binfluencers?\b")),
    ServiceType("search", "Search / SERP Scraper", (r"\bsearch\b", r"\bserp\b", r"\bkeywords?\b", r"\bautocomplete\b")),
    ServiceType("places", "Places & Business Scraper",
                (r"\bplaces?\b", r"\bbusiness(?:es)?\b", r"\brestaurants?\b", r"\bhotels?\b", r"\bleads?\b", r"\bcompan(?:y|ies)\b")),
    ServiceType("products", "Product & Listing Scraper",
                (r"\bproducts?\b", r"\blistings?\b", r"\bcatalog\b", r"\bsellers?\b", r"\bshops?\b", r"\bproperties\b")),
    ServiceType("prices", "Price & Deals Monitor",
                (r"\bpric(?:e|es|ing)\b", r"\bdeals?\b", r"\bdiscounts?\b", r"\bprice tracker\b")),
    ServiceType("jobs", "Jobs Scraper", (r"\bjobs?\b", r"\bvacanc\w*", r"\bcareers?\b", r"\bhiring\b")),
    ServiceType("contacts", "Email & Contact Finder",
                (r"\bemails?\b", r"\bcontacts?\b", r"\bphones?\b", r"\bcontact info\b")),
    ServiceType("ads", "Ads Scraper", (r"\bads?\b", r"\badvertis\w*")),
    ServiceType("transcripts", "Transcript Scraper", (r"\btranscripts?\b", r"\bsubtitles?\b", r"\bcaptions?\b")),
    ServiceType("downloader", "Media Downloader", (r"\bdownload\w*", r"\bsaver\b")),
    ServiceType("images", "Images Scraper", (r"\bimages?\b", r"\bphotos?\b", r"\bpictures?\b")),
]

# Слова, которые не могут быть названием услуги при автоподборе
NOISE_WORDS = {
    "tool", "tools", "actor", "actors", "apify", "unlimited", "premium", "export", "extract", "extraction",
    "scraped", "online", "public", "batch", "automated", "automation", "complete", "full", "real", "time",
    "realtime", "latest", "top", "lists", "dataset", "json", "csv", "excel", "code", "trial", "pay",
    "enrich", "enrichment", "data", "extractor", "scraper", "scrapers", "crawler", "spider", "with",
    "from", "for", "and", "the", "com", "www", "http", "https",
}


def _compile(patterns) -> re.Pattern:
    return re.compile("|".join(f"(?:{p})" for p in patterns))


def load_service_overrides(path: Path | None) -> tuple[list[ServiceType], set[str]]:
    """Справочник услуг с учётом JSON-файла: {"services": {"key": {"label": "...", "patterns": [...]}},
    "exclude_services": ["key"]}. Одинаковый key заменяет встроенную услугу."""
    types = {t.key: t for t in SERVICE_TYPES}
    excluded: set[str] = set()
    if path:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        for key, spec in (data.get("services") or {}).items():
            if isinstance(spec, list):
                spec = {"patterns": spec}
            old = types.get(key)
            types[key] = ServiceType(key, spec.get("label") or (old.label if old else f"{key.title()} Scraper"),
                                     tuple(spec.get("patterns") or (old.patterns if old else ())))
        excluded = set(data.get("exclude_services") or [])
    return [t for k, t in types.items() if k not in excluded], excluded


def _contains(text: pd.Series, pattern: re.Pattern) -> pd.Series:
    with warnings.catch_warnings():  # пользовательские regex могут содержать группы
        warnings.simplefilter("ignore", UserWarning)
        return text.str.contains(pattern, regex=True)


def discover_services(text: pd.Series, known: re.Pattern, cfg: ScoreConfig, limit: int = 6) -> list[str]:
    """Частые «содержательные» слова в названиях ниши, не покрытые справочником."""
    threshold = max(cfg.min_discovered_service, math.ceil(0.02 * len(text)))
    skip = TASK_WORDS | GENERIC_WORDS | STOPWORDS | NOISE_WORDS
    cnt: Counter = Counter()
    for t in text:
        for tok in set(t.split()):
            if len(tok) > 2 and not tok.isdigit() and tok not in skip and not known.search(tok):
                cnt[tok] += 1
    return [tok for tok, n in cnt.most_common() if n >= threshold][:limit]


def assign_services(cur: pd.DataFrame, members: pd.DataFrame, niche_meta: pd.DataFrame,
                    cfg: ScoreConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """-> (membership[actor_key, niche_key, service_key], services[niche_key, service_key, name, display_name, kind])."""
    types, _ = load_service_overrides(cfg.niches_file)
    known = _compile([p for t in types for p in t.patterns]) if types else re.compile(r"(?!x)x")
    compiled = [(t, _compile(t.patterns)) for t in types if t.patterns]

    texts = niche_texts(cur)
    text_by_actor = pd.Series(texts.values, index=cur["actor_key"].values)
    text_by_actor = text_by_actor[~text_by_actor.index.duplicated()]

    mem_parts, svc_rows = [], []
    for niche in niche_meta.itertuples():
        keys = members.loc[members["niche_key"] == niche.niche_key, "actor_key"].tolist()
        text = text_by_actor.reindex(keys).fillna("")
        text = text.str.replace(re.compile(niche.pattern), " ", regex=True)  # вырезаем саму нишу
        pairs: list[tuple[str, str]] = []   # (actor_key, service_key)
        labels: dict[str, tuple[str, str]] = {}   # service_key -> (label, kind)

        for t, rx in compiled:
            hit = text.index[_contains(text, rx).values]
            pairs += [(a, t.key) for a in hit]
            labels[t.key] = (t.label, "taxonomy")
        if cfg.auto_services:
            for tok in discover_services(text, known, cfg):
                hit = text.index[_contains(text, re.compile(rf"\b{re.escape(tok)}\b")).values]
                key = f"kw-{tok}"
                pairs += [(a, key) for a in hit]
                labels[key] = (f"{tok.title()} Scraper", "discovered")

        df = pd.DataFrame(pairs, columns=["actor_key", "service_key"]).drop_duplicates()
        sizes = df["service_key"].value_counts()
        keep = set(sizes[sizes >= cfg.min_service_actors].index)       # п.4: слишком мелкие услуги отбрасываем
        df = df[df["service_key"].isin(keep)]
        orphans = sorted(set(keys) - set(df["actor_key"]))             # п.5: остальные -> general
        if orphans:
            df = pd.concat([df, pd.DataFrame({"actor_key": orphans, "service_key": "general"})], ignore_index=True)
            labels["general"] = (GENERAL.label, "general")
        df["niche_key"] = niche.niche_key
        mem_parts.append(df)

        nname = niche_name(niche.niche_key)
        for skey in df["service_key"].unique():
            label, kind = labels[skey]
            svc_rows.append({"niche_key": niche.niche_key, "service_key": skey, "name": label,
                             "display_name": f"{nname} {label}", "kind": kind})

    cols = ["actor_key", "niche_key", "service_key"]
    membership = pd.concat(mem_parts, ignore_index=True)[cols] if mem_parts else pd.DataFrame(columns=cols)
    services = pd.DataFrame(svc_rows, columns=["niche_key", "service_key", "name", "display_name", "kind"])
    return membership, services
