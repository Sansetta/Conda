"""Ниши: заданный список (SEED_NICHES) + автоподбор из названий вида «<Ниша> Scraper».

Ниша = платформа или предметная область (instagram, google maps, amazon...).
Услуги внутри ниши (posts, followers, reviews...) определяет модуль services.py.
"""
from __future__ import annotations

import json
import re
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd

from .apify_api import STOPWORDS
from .config import ScoreConfig

# Ниши, известные заранее (платформы и популярные задачи). Регулярки применяются к
# нормализованному тексту «title + slug» (нижний регистр, всё кроме a-z0-9 -> пробел).
SEED_NICHES: dict[str, list[str]] = {
    "instagram": [r"\binstagram\b", r"\binsta\b"],
    "tiktok": [r"\btik ?tok\b"],
    "facebook": [r"\bfacebook\b"],
    "twitter / x": [r"\btwitter\b", r"\btweets?\b", r"\bx com\b"],
    "linkedin": [r"\blinked ?in\b"],
    "youtube": [r"\byou ?tube\b"],
    "pinterest": [r"\bpinterest\b"],
    "reddit": [r"\breddit\b"],
    "telegram": [r"\btelegram\b"],
    "whatsapp": [r"\bwhats ?app\b"],
    "discord": [r"\bdiscord\b"],
    "twitch": [r"\btwitch\b"],
    "snapchat": [r"\bsnapchat\b"],
    "threads": [r"\bthreads\b"],
    "spotify": [r"\bspotify\b"],
    "github": [r"\bgithub\b"],
    "quora": [r"\bquora\b"],
    "medium": [r"\bmedium\b"],
    "google maps": [r"\bgoogle ?maps?\b", r"\bgmaps\b"],
    "google search (SERP)": [r"\bgoogle ?search\b", r"\bserp\b"],
    "google trends": [r"\bgoogle ?trends?\b"],
    "google shopping": [r"\bgoogle ?shopping\b"],
    "google news": [r"\bgoogle ?news\b"],
    "google play": [r"\bgoogle ?play\b", r"\bplay ?store\b"],
    "app store (apple)": [r"\bapp ?store\b", r"\bapple\b"],
    "amazon": [r"\bamazon\b"],
    "ebay": [r"\bebay\b"],
    "walmart": [r"\bwalmart\b"],
    "aliexpress": [r"\bali ?express\b"],
    "alibaba": [r"\balibaba\b"],
    "etsy": [r"\betsy\b"],
    "shopify": [r"\bshopify\b"],
    "temu": [r"\btemu\b"],
    "shein": [r"\bshein\b"],
    "zalando": [r"\bzalando\b"],
    "home depot": [r"\bhome ?depot\b"],
    "airbnb": [r"\bairbnb\b"],
    "booking.com": [r"\bbooking\b"],
    "tripadvisor": [r"\btrip ?advisor\b"],
    "expedia": [r"\bexpedia\b"],
    "yelp": [r"\byelp\b"],
    "trustpilot": [r"\btrustpilot\b"],
    "yellow pages": [r"\byellow ?pages\b"],
    "zillow": [r"\bzillow\b"],
    "realtor / redfin": [r"\brealtor\b", r"\bredfin\b"],
    "indeed": [r"\bindeed\b"],
    "glassdoor": [r"\bglassdoor\b"],
    "crunchbase": [r"\bcrunchbase\b"],
    "email / contact finder": [r"\bemails?\b", r"\bcontact (info|details)\b"],
    "website crawler": [r"\bwebsite (content )?crawler\b", r"\bweb crawler\b", r"\bweb ?scraper\b"],
    "ai / llm": [r"\bchatgpt\b", r"\bopenai\b", r"\bllm\b", r"\bgemini\b", r"\bclaude\b"],
}

# Слова-«задачи»: всё, что левее них в названии, считается кандидатом в нишу («Instagram Scraper»)
TASK_WORDS = {
    "scraper", "scrapers", "crawler", "extractor", "downloader", "finder", "checker", "monitor",
    "tracker", "parser", "collector", "api", "bot", "spider", "scraping", "lookup", "enricher",
    "generator", "analyzer", "exporter",
}
# Слова-уточнения: не часть ниши (тип контента, маркетинговые прилагательные)
GENERIC_WORDS = {
    "profile", "profiles", "post", "posts", "reel", "reels", "story", "stories", "hashtag",
    "hashtags", "comment", "comments", "video", "videos", "image", "images", "photo", "photos",
    "review", "reviews", "product", "products", "listing", "listings", "price", "prices",
    "search", "results", "result", "data", "info", "details", "user", "users", "follower",
    "followers", "following", "email", "emails", "contact", "contacts", "ads", "ad", "page",
    "pages", "fast", "best", "free", "cheap", "pro", "lite", "ultimate", "advanced", "simple",
    "easy", "no", "cookies", "cookie", "unofficial", "official", "all", "in", "one", "bulk",
    "mass", "smart", "web", "ai", "and", "of", "by", "to", "on", "or", "a", "an", "fastest",
    "powerful", "universal", "mini", "plus", "v2", "v3", "scrape", "extract", "get", "find",
    "url", "urls", "link", "links", "text", "content", "list", "feed", "feeds", "job", "jobs",
}


def norm_text(*parts: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", " ".join(p or "" for p in parts).lower()).strip()


def _phrase_regex(phrase: str) -> str:
    # "google maps" -> \bgoogle ?maps\b (ловит и googlemaps)
    return r"\b" + " ?".join(re.escape(t) for t in phrase.split()) + r"\b"


def load_niche_overrides(path: Path | None) -> tuple[dict[str, list[str]], set[str]]:
    if not path:
        return {}, set()
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    include = {k: list(v) for k, v in (data.get("include") or {}).items()}
    exclude = {norm_text(x) for x in (data.get("exclude") or [])}
    return include, exclude


def discover_niches(titles: pd.Series, seeds: dict[str, list[str]], exclude: set[str],
                    min_actors: int) -> dict[str, list[str]]:
    """Автоподбор ниш из названий вида «<Ниша> ... Scraper»."""
    cnt: Counter = Counter()
    for t in titles:
        toks = t.split()
        idx = next((i for i, w in enumerate(toks) if w in TASK_WORDS), 0)
        if idx == 0:
            continue
        left = [w for w in toks[:idx] if w not in GENERIC_WORDS and w not in STOPWORDS
                and len(w) > 2 and not w.isdigit()]
        if left:
            cnt[" ".join(left[-2:])] += 1

    seed_tokens = [set(re.findall(r"[a-z0-9]+", k.lower())) for k in seeds]
    seed_res = [re.compile(a) for aliases in seeds.values() for a in aliases]
    accepted: list[tuple[str, set[str]]] = []
    for phrase, n in cnt.most_common():
        if n < min_actors:
            break
        toks = set(phrase.split())
        if phrase in exclude or toks & exclude:
            continue
        if any(toks <= s for s in seed_tokens) or any(r.search(phrase) for r in seed_res):
            continue  # уже покрыто «семенной» нишей
        if any(toks <= a or a <= toks for _, a in accepted):
            continue  # вложенная/объемлющая ниша: оставляем более частую
        accepted.append((phrase, toks))
    return {p: [_phrase_regex(p)] for p, _ in accepted}



# Красивые названия для витрины (всё остальное - key.title())
NICHE_DISPLAY = {
    "twitter / x": "Twitter / X", "linkedin": "LinkedIn", "youtube": "YouTube", "tiktok": "TikTok",
    "whatsapp": "WhatsApp", "github": "GitHub", "ebay": "eBay", "aliexpress": "AliExpress",
    "ai / llm": "AI / LLM", "google serp": "Google SERP", "google search (SERP)": "Google Search (SERP)",
    "app store (apple)": "App Store (Apple)", "booking.com": "Booking.com", "tripadvisor": "Tripadvisor",
    "realtor / redfin": "Realtor / Redfin", "email / contact finder": "Email / Contact Finder",
}


def niche_slug(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", key.lower()).strip("-") or "niche"


def niche_name(key: str) -> str:
    return NICHE_DISPLAY.get(key, key.title())


def assign_niches(cur: pd.DataFrame, cfg: ScoreConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """-> (membership[actor_key, niche_key], meta[niche_key, kind, pattern]).
    Actor может попасть в несколько ниш. kind: seed | auto | custom.
    pattern - объединённая regex ниши (services.py вырезает её из текста, чтобы «instagram» не считался услугой)."""
    include, exclude = load_niche_overrides(cfg.niches_file)
    seeds = {k: v for k, v in SEED_NICHES.items() if norm_text(k) not in exclude}
    seeds.update(include)

    titles = cur["title"].fillna("").map(norm_text)
    discovered = discover_niches(titles, seeds, exclude, cfg.min_niche_actors)
    niches = {**seeds, **discovered}
    kinds = {k: ("custom" if k in include else "seed") for k in seeds} | {k: "auto" for k in discovered}

    texts = niche_texts(cur)
    parts, meta = [], []
    for niche, aliases in niches.items():
        with warnings.catch_warnings():  # пользовательские regex могут содержать группы
            warnings.simplefilter("ignore", UserWarning)
            mask = texts.str.contains("|".join(f"(?:{a})" for a in aliases), regex=True)
        if mask.sum() >= cfg.min_niche_actors:
            parts.append(pd.DataFrame({"actor_key": cur.loc[mask, "actor_key"].values, "niche_key": niche}))
            meta.append({"niche_key": niche, "kind": kinds[niche],
                         "pattern": "|".join(f"(?:{a})" for a in aliases)})
    if not parts:
        return pd.DataFrame(columns=["actor_key", "niche_key"]), pd.DataFrame(columns=["niche_key", "kind", "pattern"])
    return pd.concat(parts, ignore_index=True).drop_duplicates(), pd.DataFrame(meta)


def niche_texts(cur: pd.DataFrame) -> pd.Series:
    """Нормализованный текст «title + slug» для каждого Actor'а (по нему ищутся ниши и услуги)."""
    return pd.Series([norm_text(t, n) for t, n in zip(cur["title"], cur["name"])], index=cur.index)
