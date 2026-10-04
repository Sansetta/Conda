"""Рерайт услуг для продажи: описания Actor'ов с Apify -> уникальная карточка услуги в структуре страницы Apify Store.

Единица рерайта - услуга (ниша x service_key), например «Instagram Post Scraper». Для неё:
  1. берутся до N самых популярных Actor'ов услуги (последний полностью посчитанный снимок);
  2. по каждому подтягиваются описание / SEO-описание / фрагмент README (кэш в actor_details);
  3. по исходникам считается source_hash: не изменились исходники (или рерайту меньше min_age_days) - услугу пропускаем;
  4. провайдер (LLM или шаблон) пишет карточку: tagline, описание, возможности, сценарии, шаги, вход/выход, FAQ, SEO;
  5. проверка уникальности: доля 4-грамм, совпавших с исходниками, не выше max_similarity (иначе одна повторная попытка);
  6. результат СРАЗУ кладётся в service_content_staging (переживёт падение), а в живую service_content
     переезжает пачкой раз в flush_s секунд (одна транзакция) и в конце окна.

Окно (window_s, по умолчанию 3 часа) - одна итерация: новые услуги не запускаются после дедлайна,
необработанный хвост остаётся на следующий прогон (очередь пересобирается по хэшам, ничего не теряется).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import logging
import os
import re
import threading
import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field

import requests

from .apify_api import ACTORS_URL, get_json

log = logging.getLogger("apify-tracker")

PROMPT_VERSION = "v1"   # меняете промпт/структуру - поднимите версию, и все карточки пересоберутся
BUILDS_URL = "https://api.apify.com/v2/actor-builds"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


# --------------------------------------------------------------------------
# Конфиг
# --------------------------------------------------------------------------
@dataclass
class RewriteConfig:
    provider: str = "llm"                 # llm | template (template - без API-ключа, для проверки конвейера)
    model: str = field(default_factory=lambda: os.environ.get("REWRITE_MODEL", "claude-sonnet-5-5"))
    lang: str = "en"                      # язык карточки (Apify Store - английский)
    sources_per_service: int = 5          # сколько Actor'ов услуги идёт в исходники
    details_ttl_days: int = 14            # как долго кэш описаний считается свежим
    fetch_details: bool = True            # False = работать только по кэшу actor_details
    readme_chars: int = 2500              # сколько символов README брать из каждого Actor'а (0 = не брать)
    workers: int = 3                      # параллельных рерайтов
    window_s: float = 3 * 3600            # длина одной итерации
    flush_s: float = 15 * 60              # как часто публиковать накопленное
    min_age_days: float = 7               # не переписывать услугу чаще, чем раз в N дней (если не --force)
    max_similarity: float = 0.20          # порог совпадения 4-грамм с исходниками
    niche: str | None = None              # только одна ниша (slug)
    scope: str = "top"                    # каталог актёров: top (в топах услуг) | eligible (с ценой и min_users) | all
    min_users: int = 10                   # порог пользователей для каталога (scope eligible/top)
    limit: int = 0                        # максимум услуг за итерацию (0 = без лимита)
    force: bool = False                   # переписать всё, игнорируя хэши и возраст
    delay: float = 0.2                    # пауза после запроса к Apify, сек
    llm_timeout: float = 120.0


# --------------------------------------------------------------------------
# Текстовые утилиты
# --------------------------------------------------------------------------
TAG_RE = re.compile(r"<[^>]+>")
MD_RE = re.compile(r"[`*_>#]+|!?\[([^\]]*)\]\([^)]*\)")
WORD_RE = re.compile(r"[a-z0-9а-яё]+")


def clean_text(s: str | None) -> str:
    """HTML/Markdown -> простой текст в одну строку."""
    if not s:
        return ""
    s = html.unescape(TAG_RE.sub(" ", str(s)))
    s = MD_RE.sub(lambda m: m.group(1) or " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _shingles(text: str, n: int = 4) -> set[tuple[str, ...]]:
    w = WORD_RE.findall(text.lower())
    return {tuple(w[i:i + n]) for i in range(len(w) - n + 1)}


def similarity(output_text: str, source_text: str) -> float:
    """Доля 4-грамм результата, встречающихся в исходниках (0 = ничего общего, 1 = копия)."""
    out = _shingles(output_text)
    return len(out & _shingles(source_text)) / len(out) if out else 0.0


def flatten_content(c: dict) -> str:
    """Весь «человеческий» текст карточки одной строкой (для проверки уникальности)."""
    parts = [c.get("tagline", ""), c.get("short_description", ""), c.get("what_it_does", "")]
    for k in ("features", "use_cases", "how_to_use", "input", "output"):
        parts += [str(x) for x in c.get(k) or []]
    for qa in c.get("faq") or []:
        parts += [str(qa.get("q", "")), str(qa.get("a", ""))]
    return " ".join(parts)


def content_blocks(c: dict) -> list[str]:
    """Смысловые блоки карточки: каждый проверяется отдельно, чтобы скопированный абзац не «растворился» в остальном тексте."""
    blocks = [c.get("tagline", ""), c.get("short_description", "")] + (c.get("what_it_does") or "").split("\n")
    for k in ("features", "use_cases", "how_to_use"):
        blocks += [str(x) for x in c.get(k) or []]
    blocks += [str(qa.get("a", "")) for qa in c.get("faq") or []]
    return [b for b in blocks if len(WORD_RE.findall(b)) >= 6]


def content_similarity(c: dict, corpus: str) -> float:
    """max(по всей карточке, по худшему блоку): ловит и общее заимствование, и один скопированный абзац."""
    src = _shingles(corpus)
    def sim(text: str) -> float:
        out = _shingles(text)
        return len(out & src) / len(out) if out else 0.0
    return max([sim(flatten_content(c))] + [sim(b) for b in content_blocks(c)])


# --------------------------------------------------------------------------
# Данные и контекст
# --------------------------------------------------------------------------
@dataclass
class Source:
    actor_pk: int
    username: str
    name: str
    title: str
    total_users: int = 0
    cached: dict | None = None            # строка actor_details (+ fetched_at)


@dataclass
class ServiceCtx:
    service_id: int
    niche_slug: str
    niche_name: str
    service_key: str
    service_name: str                     # «Post Scraper»
    display_name: str                     # «Instagram Post Scraper»
    kind: str
    stats: dict
    sources: list[Source]
    mode: str = "service"                 # service: карточка услуги | actor: карточка одного актора


@dataclass
class Result:
    service_id: int
    status: str                           # rewritten | skipped | no_sources | too_similar | failed
    new_details: dict[int, dict] = field(default_factory=dict)
    row: dict | None = None
    note: str = ""


def fetch_details(session: requests.Session, username: str, name: str, cfg: RewriteConfig) -> dict | None:
    """Описание Actor'а из публичного API Apify: description, seoTitle, seoDescription + фрагмент README.
    Поля README зависят от версии API: если его нет - остаются только описания (этого достаточно для рерайта)."""
    data = get_json(session, f"{ACTORS_URL}/{username}~{name}", {})
    time.sleep(cfg.delay)
    if not data:
        return None
    readme = data.get("readmeSummary") or ""
    build_id = ((data.get("taggedBuilds") or {}).get("latest") or {}).get("buildId")
    if cfg.readme_chars > 0 and build_id:
        try:
            build = get_json(session, f"{BUILDS_URL}/{build_id}", {})
            time.sleep(cfg.delay)
            readme = ((build or {}).get("actorDefinition") or {}).get("readme") or readme
        except Exception as exc:  # README - бонус, без него обходимся
            log.debug("README %s/%s недоступен: %s", username, name, exc)
    return {
        "description": clean_text(data.get("description"))[:2000] or None,
        "seo_title": clean_text(data.get("seoTitle"))[:512] or None,
        "seo_description": clean_text(data.get("seoDescription"))[:2000] or None,
        "readme_excerpt": clean_text(readme)[:max(cfg.readme_chars, 0)] or None,
    }


def details_hash(d: dict) -> str:
    raw = "|".join(str(d.get(k) or "") for k in ("description", "seo_title", "seo_description", "readme_excerpt"))
    return hashlib.sha256(raw.encode()).hexdigest()


def source_text(src: Source, d: dict | None, cfg: RewriteConfig) -> str:
    """Исходный текст одного Actor'а (title + описания + README)."""
    d = d or {}
    parts = [src.title, d.get("description"), d.get("seo_description"), (d.get("readme_excerpt") or "")[:cfg.readme_chars]]
    return " ".join(p for p in parts if p)


def source_hash(ctx: ServiceCtx, texts: list[str], cfg: RewriteConfig) -> str:
    head = f"{PROMPT_VERSION}|{cfg.provider}|{cfg.model if cfg.provider == 'llm' else '-'}|{cfg.lang}|" \
           f"{ctx.niche_slug}|{ctx.service_key}|{ctx.display_name}"
    return hashlib.sha256((head + "\n" + "\n".join(sorted(texts))).encode()).hexdigest()


# --------------------------------------------------------------------------
# Провайдеры рерайта
# --------------------------------------------------------------------------
# Справочник для шаблонного режима: (что собирает, поля результата, сценарии, типичные входы)
KB: dict[str, tuple[str, list[str], list[str], list[str]]] = {
    "followers": ("followers and following lists", ["username", "full name", "bio", "verified flag", "follower count"],
                  ["Audience research", "Competitor benchmarking", "Influencer vetting", "Giveaway audits"],
                  ["profile URLs or usernames", "maximum accounts per profile"]),
    "comments": ("comments and replies", ["comment text", "author", "timestamp", "like count", "reply thread"],
                 ["Sentiment analysis", "Community moderation research", "Customer feedback mining", "Campaign monitoring"],
                 ["post or video URLs", "maximum comments per item"]),
    "reviews": ("reviews and ratings", ["review text", "rating", "author", "date", "owner response"],
                ["Reputation monitoring", "Competitor comparison", "Product feedback analysis", "Local SEO audits"],
                ["listing URLs or IDs", "maximum reviews per listing", "sort order"]),
    "likes": ("likes and reactions", ["reacting user", "reaction type", "post reference", "timestamp"],
              ["Engagement analysis", "Lead discovery", "Campaign reporting"], ["post URLs", "maximum users per post"]),
    "posts": ("posts and feed items", ["post text", "media links", "publish date", "engagement counts", "author"],
              ["Content research", "Brand monitoring", "Trend tracking", "Competitor content analysis"],
              ["profile or page URLs", "date range", "maximum posts"]),
    "videos": ("videos and short clips", ["video URL", "caption", "view count", "duration", "publish date"],
               ["Viral content research", "Creator analysis", "Content calendars", "Competitor tracking"],
               ["account or channel URLs", "maximum videos"]),
    "stories": ("stories and highlights", ["media link", "posted time", "expiry time", "author"],
                ["Brand and competitor monitoring", "Campaign archiving"], ["usernames", "story type"]),
    "hashtags": ("hashtags and trending topics", ["hashtag", "post count", "top posts", "related tags"],
                 ["Trend discovery", "Campaign planning", "Topic research"], ["hashtags or topics", "maximum results"]),
    "profiles": ("public profile details", ["name", "handle", "bio", "follower count", "links", "location"],
                 ["Influencer discovery", "Lead enrichment", "Competitor research", "Creator databases"],
                 ["profile URLs or usernames"]),
    "search": ("search results", ["rank", "title", "URL", "snippet", "result type"],
               ["Keyword research", "Rank tracking", "Market monitoring", "Lead sourcing"],
               ["search queries", "country and language", "maximum results per query"]),
    "places": ("places and local businesses", ["name", "address", "phone", "website", "category", "rating", "coordinates"],
               ["Local lead generation", "Market mapping", "Competitor analysis", "Territory planning"],
               ["search terms", "location or area", "maximum places"]),
    "products": ("products and listings", ["title", "price", "currency", "availability", "seller", "images", "URL"],
                 ["Catalog monitoring", "Market research", "Assortment benchmarking", "Dropshipping research"],
                 ["category, search or product URLs", "maximum items"]),
    "prices": ("prices and deals", ["current price", "previous price", "discount", "seller", "timestamp"],
               ["Price monitoring", "Dynamic pricing inputs", "Deal alerts", "MAP compliance checks"],
               ["product URLs or queries", "schedule"]),
    "jobs": ("job listings", ["job title", "company", "location", "salary", "posted date", "description"],
             ["Job aggregation", "Hiring-trend analysis", "Recruiting lead lists", "Salary benchmarking"],
             ["search keywords", "location", "maximum jobs"]),
    "contacts": ("public contact details", ["email", "phone", "social links", "source page"],
                 ["Outreach list building", "CRM enrichment", "Sales prospecting"], ["website URLs or company lists"]),
    "ads": ("advertisements and creatives", ["ad text", "creative link", "advertiser", "start date", "platform"],
            ["Competitive ad research", "Creative swipe files", "Spend-trend monitoring"], ["advertiser or keyword", "country"]),
    "transcripts": ("transcripts and captions", ["full text", "timestamps", "language", "video reference"],
                    ["Content repurposing", "Keyword extraction", "Accessibility", "LLM training data preparation"],
                    ["video URLs", "language"]),
    "downloader": ("media files", ["file link", "format", "resolution", "source URL"],
                   ["Archiving", "Offline review", "Creative asset collection"], ["media URLs", "quality or format"]),
    "images": ("images and photos", ["image URL", "dimensions", "alt text", "source page"],
               ["Visual research", "Dataset building", "Moodboards"], ["page or profile URLs", "maximum images"]),
    "general": ("public data", ["structured records in JSON, CSV or Excel"],
                ["Market research", "Monitoring", "Lead generation", "Data enrichment"],
                ["start URLs or search terms", "maximum items"]),
}


def _kb(ctx: ServiceCtx) -> tuple[str, list[str], list[str], list[str]]:
    if ctx.service_key in KB:
        return KB[ctx.service_key]
    word = ctx.service_key.removeprefix("kw-").replace("-", " ")
    return (f"{word} data", [f"{word} records", "source URL", "timestamp"],
            [f"{word.title()} research", "Monitoring", "Data enrichment"], ["start URLs or search terms", "maximum items"])


class TemplateProvider:
    """Без LLM: собирает карточку из справочника KB. Нужен для проверки конвейера и как запасной вариант.
    Тексты однотипны для одной услуги в разных нишах - для настоящей уникальности используйте LLM-провайдер."""

    name = "template"

    def rewrite(self, ctx: ServiceCtx, source_texts: list[str], cfg: RewriteConfig, hint: str = "") -> dict:
        noun, fields, uses, inputs = _kb(ctx)
        niche, title = ctx.niche_name, ctx.display_name
        uid = ctx.stats.get("uid") if ctx.mode == "actor" else None
        return {
            "title": f"{title} ({uid})" if uid else title,
            "tagline": f"Collect {niche} {noun} as clean, structured data - no code required.",
            "short_description": f"{title} turns {niche} {noun} into structured records you can export to JSON, CSV "
                                 f"or Excel. Choose your inputs, run it, and get the results in minutes.",
            "what_it_does": f"{title} gathers publicly available {noun} from {niche} and returns them in a consistent "
                            f"format. It handles pagination and retries for you, so you can focus on using the data.\n\n"
                            f"Run it on demand from the web console, on a schedule, or through the API and plug the "
                            f"output into your spreadsheets, dashboards or automations.",
            "features": [f"Extracts {', '.join(fields[:4])} and more", "Export to JSON, CSV, Excel or via API",
                         "Scheduling and webhooks for hands-off runs", "Proxy support for stable large runs",
                         "Easy integration with no-code tools and your own code"],
            "use_cases": uses,
            "how_to_use": ["Open the tool and create a new task.", f"Enter {' and '.join(inputs[:2])}.",
                           "Start the run and wait for it to finish.", "Download the dataset or fetch it via API."],
            "input": inputs,
            "output": fields,
            "faq": [{"q": "Does it need an account or login?",
                     "a": "It works with publicly available pages, so no account of yours is required."},
                    {"q": "Is scraping legal?",
                     "a": f"The tool collects public data only. You are responsible for following {niche}'s terms "
                          f"and the laws that apply to you, such as GDPR."},
                    {"q": "How do I get the results?",
                     "a": "Download them as JSON, CSV or Excel, or read them through the API."}],
            "seo": {"title": f"{title} - structured {niche} data", "description":
                    f"{title}: extract {noun} from {niche} and export to JSON, CSV or Excel.",
                    "keywords": [title.lower(), f"{niche.lower()} {noun}", f"{niche.lower()} scraper"]},
        }


SYSTEM_PROMPT = """You write product pages for a marketplace of data-extraction tools, in the style of Apify Store listings: \
clear, practical, benefit-led, no hype. You receive notes about several existing tools that solve the same task. \
Write ONE original listing for a new tool of this type.
Rules:
- Use your own wording and structure. Never copy sentences or distinctive phrases from the notes.
- Use only facts supported by the notes. If input or output details are unclear, stay generic. Invent no numbers, \
speed, accuracy, integrations or customer claims.
- Do not name the source tools, their authors, Apify, or competitors.
- Do not promise legal compliance. Mention that only publicly available data is collected and the user must follow the platform's terms.
- Language: {lang}. Return ONLY valid JSON, no markdown fences, with exactly these keys:
{{"tagline": str (<=110 chars), "short_description": str (<=300 chars), "what_it_does": str (2 short paragraphs), \
"features": [5-7 str], "use_cases": [4-6 str], "how_to_use": [4-5 str steps], "input": [3-6 str typical inputs], \
"output": [5-10 str typical output fields], "faq": [{{"q": str, "a": str}} x4], \
"seo": {{"title": str (<=60), "description": str (<=155), "keywords": [5-8 str]}}}}"""


ACTOR_SYSTEM_PROMPT = SYSTEM_PROMPT.replace(
    "You receive notes about several existing tools that solve the same task. Write ONE original listing for a new tool of this type.",
    "You receive notes about ONE existing tool. Write an original listing for a comparable tool that will be sold under its own name.",
).replace(
    '{{"tagline"',
    '{{"title": str (<=60 chars, a NEW product name that contains the platform and the task, different from the original '
    'name and free of any author or brand names), "tagline"', 1)
assert "ONE existing tool" in ACTOR_SYSTEM_PROMPT and '"title": str' in ACTOR_SYSTEM_PROMPT


def build_user_prompt(ctx: ServiceCtx, source_texts: list[str], hint: str = "") -> str:
    if ctx.mode == "actor":
        return (f"Platform / niche: {ctx.niche_name}\nTool type: {ctx.service_name}\n"
                f"Original name (do not reuse): {ctx.stats.get('source_title', '')}\n\n"
                f"Notes about the existing tool:\n{source_texts[0][:6000]}\n" + (f"\n{hint}\n" if hint else ""))
    notes = "\n\n".join(f"[{i}] {t[:3500]}" for i, t in enumerate(source_texts, 1))
    return (f"Platform / niche: {ctx.niche_name}\nTool type: {ctx.service_name}\nListing title: {ctx.display_name}\n"
            f"Actors in this category: {ctx.stats.get('actors')}\n\nNotes about existing tools:\n{notes}\n"
            + (f"\n{hint}\n" if hint else ""))


class AnthropicProvider:
    name = "llm"

    def __init__(self, cfg: RewriteConfig):
        self.key = os.environ.get("ANTHROPIC_API_KEY")
        if not self.key:
            raise RuntimeError("Для --rewrite-provider llm нужен ANTHROPIC_API_KEY (или используйте --rewrite-provider template)")
        self.local = threading.local()

    def _session(self) -> requests.Session:
        s = getattr(self.local, "s", None)
        if s is None:
            s = self.local.s = requests.Session()
            s.headers.update({"x-api-key": self.key, "anthropic-version": "2023-06-01", "content-type": "application/json"})
        return s

    def rewrite(self, ctx: ServiceCtx, source_texts: list[str], cfg: RewriteConfig, hint: str = "") -> dict:
        body = {"model": cfg.model, "max_tokens": 3000, "temperature": 0.7,
                "system": (ACTOR_SYSTEM_PROMPT if ctx.mode == "actor" else SYSTEM_PROMPT).format(lang=cfg.lang),
                "messages": [{"role": "user", "content": build_user_prompt(ctx, source_texts, hint)}]}
        last: Exception | None = None
        for attempt in range(4):
            try:
                r = self._session().post(ANTHROPIC_URL, json=body, timeout=cfg.llm_timeout)
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                r.raise_for_status()
                text = "".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text")
                return parse_json_object(text)
            except (requests.RequestException, ValueError) as exc:
                last = exc
                time.sleep(2 ** attempt * 3)
        raise RuntimeError(f"LLM не ответил корректно: {last}")


def parse_json_object(text: str) -> dict:
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    a, b = text.find("{"), text.rfind("}")
    if a < 0 or b <= a:
        raise ValueError("в ответе нет JSON-объекта")
    return json.loads(text[a:b + 1])


def make_provider(cfg: RewriteConfig):
    if cfg.provider == "template":
        return TemplateProvider()
    if cfg.provider == "llm":
        return AnthropicProvider(cfg)
    raise ValueError(f"неизвестный провайдер: {cfg.provider}")


# --------------------------------------------------------------------------
# Проверка и нормализация результата
# --------------------------------------------------------------------------
def _strs(v, lo: int, hi: int, name: str) -> list[str]:
    items = [str(x).strip() for x in (v or []) if str(x).strip()]
    if len(items) < lo:
        raise ValueError(f"поле {name}: нужно минимум {lo} элементов")
    return items[:hi]


def normalize_content(raw: dict, ctx: ServiceCtx) -> dict:
    """Приводит ответ провайдера к фиксированной структуре, обрезает длину. Бросает ValueError при дырах."""
    if not isinstance(raw, dict):
        raise ValueError("ответ не объект")
    seo = raw.get("seo") or {}
    faq = [{"q": str(x.get("q", "")).strip(), "a": str(x.get("a", "")).strip()}
           for x in raw.get("faq") or [] if isinstance(x, dict) and x.get("q") and x.get("a")]
    if len(faq) < 2:
        raise ValueError("поле faq: нужно минимум 2 вопроса")
    title = ctx.display_name
    if ctx.mode == "actor":
        title = str(raw.get("title", "")).strip()[:100]
        if not title:
            raise ValueError("пустой title")
    out = {
        "title": title,
        "tagline": str(raw.get("tagline", "")).strip()[:120],
        "short_description": str(raw.get("short_description", "")).strip()[:320],
        "what_it_does": str(raw.get("what_it_does", "")).strip(),
        "features": _strs(raw.get("features"), 3, 8, "features"),
        "use_cases": _strs(raw.get("use_cases"), 3, 8, "use_cases"),
        "how_to_use": _strs(raw.get("how_to_use"), 3, 6, "how_to_use"),
        "input": _strs(raw.get("input"), 1, 8, "input"),
        "output": _strs(raw.get("output"), 2, 12, "output"),
        "faq": faq[:6],
        "seo": {"title": str(seo.get("title") or ctx.display_name).strip()[:70],
                "description": str(seo.get("description") or raw.get("short_description", "")).strip()[:160],
                "keywords": _strs(seo.get("keywords"), 1, 10, "seo.keywords")},
    }
    if not out["tagline"] or not out["short_description"] or not out["what_it_does"]:
        raise ValueError("пустые tagline / short_description / what_it_does")
    return out


def market_block(ctx: ServiceCtx) -> dict:
    """Рыночные цифры считаются кодом из снимка, а не пишутся моделью (нельзя выдумать цену)."""
    s = ctx.stats
    return {"actors_in_service": s.get("actors"), "eligible_actors": s.get("eligible"),
            "total_users": s.get("total_users"), "median_cost_1000_usd": s.get("median_cost_1000"),
            "min_cost_1000_usd": s.get("min_cost_1000"),
            "note": "estimated USD per 1,000 results across comparable Actors on the market"}


# --------------------------------------------------------------------------
# Обработка одной услуги (выполняется в потоке: только сеть и CPU, БД не трогает)
# --------------------------------------------------------------------------
def process_service(ctx: ServiceCtx, cfg: RewriteConfig, provider, known: tuple[str, dt.datetime] | None,
                    session: requests.Session) -> Result:
    now = dt.datetime.now()
    new_details: dict[int, dict] = {}
    texts: list[str] = []
    for src in ctx.sources:
        d = src.cached
        stale = d is None or (now - d["fetched_at"]).total_seconds() > cfg.details_ttl_days * 86400
        if stale and cfg.fetch_details:
            try:
                fresh = fetch_details(session, src.username, src.name, cfg)
            except Exception as exc:
                log.warning("Описание %s/%s не получено: %s", src.username, src.name, exc)
                fresh = None
            if fresh:
                new_details[src.actor_pk] = {**fresh, "text_hash": details_hash(fresh)}
                d = fresh
        t = source_text(src, d, cfg)
        if d and (d.get("description") or d.get("seo_description") or d.get("readme_excerpt")):
            texts.append(t)
    if not texts:
        return Result(ctx.service_id, "no_sources", new_details, note="нет ни одного описания")

    h = source_hash(ctx, texts, cfg)
    if known and not cfg.force:
        old_hash, old_at = known
        if old_hash == h or (now - old_at).total_seconds() < cfg.min_age_days * 86400:
            return Result(ctx.service_id, "skipped", new_details)

    corpus = " ".join(texts)
    hint, content, sim = "", None, 1.0
    for attempt in range(2):
        try:
            content = normalize_content(provider.rewrite(ctx, texts, cfg, hint), ctx)
        except ValueError as exc:
            hint = f"The previous answer was invalid ({exc}). Follow the JSON schema exactly."
            content = None
            continue
        sim = content_similarity(content, corpus)
        if sim <= cfg.max_similarity:
            break
        hint = (f"The previous draft was too close to the notes (overlap {sim:.0%}). "
                f"Rewrite from scratch with different wording, order and examples.")
    if content is None:
        return Result(ctx.service_id, "failed", new_details, note="не удалось получить корректную карточку")
    if sim > cfg.max_similarity:
        return Result(ctx.service_id, "too_similar", new_details, note=f"совпадение {sim:.0%}")

    content["market"] = market_block(ctx)
    content["sources_count"] = len(texts)
    row = {"service_id": ctx.service_id, "source_hash": h, "provider": provider.name,
           "model": cfg.model if provider.name == "llm" else None, "lang": cfg.lang,
           "headline": ctx.display_name[:255], "tagline": content["tagline"][:512],
           "content": json.dumps(content, ensure_ascii=False), "similarity": round(min(sim, 9.999), 3),
           "rewritten_at": dt.datetime.now().replace(microsecond=0)}
    return Result(ctx.service_id, "rewritten", new_details, row)


# --------------------------------------------------------------------------
# Хранилище (все обращения к MySQL - только из главного потока)
# --------------------------------------------------------------------------
STAGE_COLS = ["service_id", "source_hash", "provider", "model", "lang", "headline", "tagline", "content",
              "similarity", "rewritten_at"]


class ContentStore:
    def __init__(self, conn):
        self.conn = conn

    def _ping(self) -> None:
        if hasattr(self.conn, "ping"):
            self.conn.ping(reconnect=True)   # окно длится часами: соединение могло закрыться по таймауту

    def load_jobs(self, cfg: RewriteConfig) -> list[ServiceCtx]:
        """Услуги последнего полностью посчитанного снимка + их Actor'ы-источники."""
        self._ping()
        cur = self.conn.cursor()
        q = ("SELECT o.service_id, o.niche_slug, o.niche_name, o.service_key, o.service_name, o.service_display_name, "
             "o.service_kind, o.actors, o.eligible, o.total_users, o.median_cost_1000, o.min_cost_1000 "
             "FROM v_service_overview_latest o")
        args: list = []
        if cfg.niche:
            q += " WHERE o.niche_slug = %s"
            args.append(cfg.niche)
        q += " ORDER BY o.total_users DESC"
        cur.execute(q, args)
        svc = cur.fetchall()
        if cfg.limit:
            svc = svc[:cfg.limit]
        ids = [r[0] for r in svc]
        by_service: dict[int, list[Source]] = {i: [] for i in ids}
        if ids:
            cur.execute("SELECT asv.service_id, a.actor_pk, a.username, a.name, a.title, sn.total_users "
                        "FROM v_latest_run lr "
                        "JOIN actor_snapshots sn ON sn.run_id = lr.run_id "
                        "JOIN actors a ON a.actor_pk = sn.actor_pk "
                        "JOIN actor_services asv ON asv.actor_pk = a.actor_pk")
            wanted = set(ids)
            for sid, pk, user, name, title, users in cur.fetchall():
                if sid in wanted:
                    by_service[sid].append(Source(pk, user, name, title, int(users or 0)))
            cache = self._load_details({s.actor_pk for v in by_service.values() for s in v})
        else:
            cache = {}
        jobs = []
        for r in svc:
            srcs = sorted(by_service[r[0]], key=lambda s: (-s.total_users, s.actor_pk))[:cfg.sources_per_service]
            for s in srcs:
                s.cached = cache.get(s.actor_pk)
            jobs.append(ServiceCtx(
                service_id=r[0], niche_slug=r[1], niche_name=r[2], service_key=r[3], service_name=r[4],
                display_name=r[5], kind=r[6],
                stats={"actors": r[7], "eligible": r[8], "total_users": int(r[9] or 0),
                       "median_cost_1000": float(r[10]) if r[10] is not None else None,
                       "min_cost_1000": float(r[11]) if r[11] is not None else None},
                sources=srcs))
        cur.close()
        self.conn.commit()
        return jobs

    def _load_details(self, pks: set[int]) -> dict[int, dict]:
        out: dict[int, dict] = {}
        cur = self.conn.cursor()
        pk_list = sorted(pks)
        for i in range(0, len(pk_list), 1000):
            part = pk_list[i:i + 1000]
            cur.execute("SELECT actor_pk, description, seo_title, seo_description, readme_excerpt, fetched_at "
                        f"FROM actor_details WHERE actor_pk IN ({','.join(['%s'] * len(part))})", part)
            for pk, d, st, sd, rd, at in cur.fetchall():
                out[pk] = {"description": d, "seo_title": st, "seo_description": sd, "readme_excerpt": rd, "fetched_at": at}
        cur.close()
        return out

    def known_hashes(self) -> dict[int, tuple[str, dt.datetime]]:
        """service_id -> (hash, rewritten_at): живая таблица, поверх неё очередь (она новее)."""
        self._ping()
        cur = self.conn.cursor()
        known: dict[int, tuple[str, dt.datetime]] = {}
        for table in ("service_content", "service_content_staging"):
            cur.execute(f"SELECT service_id, source_hash, rewritten_at FROM {table}")
            for sid, h, at in cur.fetchall():
                known[sid] = (h, at)
        cur.close()
        self.conn.commit()
        return known

    def save_details(self, details: dict[int, dict]) -> None:
        if not details:
            return
        self._ping()
        now = dt.datetime.now().replace(microsecond=0)
        cur = self.conn.cursor()
        try:
            cur.executemany(
                "INSERT INTO actor_details (actor_pk, description, seo_title, seo_description, readme_excerpt, "
                "text_hash, fetched_at) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
                "description=VALUES(description), seo_title=VALUES(seo_title), seo_description=VALUES(seo_description), "
                "readme_excerpt=VALUES(readme_excerpt), text_hash=VALUES(text_hash), fetched_at=VALUES(fetched_at)",
                [(pk, d.get("description"), d.get("seo_title"), d.get("seo_description"),
                  d.get("readme_excerpt"), d["text_hash"], now) for pk, d in details.items()])
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def stage(self, row: dict) -> None:
        """Накопление: запись переживает падение процесса, но фронт её ещё не видит."""
        self._ping()
        cur = self.conn.cursor()
        try:
            cur.execute(f"REPLACE INTO service_content_staging ({','.join(STAGE_COLS)}) "
                        f"VALUES ({','.join(['%s'] * len(STAGE_COLS))})", [row[c] for c in STAGE_COLS])
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def flush(self) -> int:
        """Публикация накопленного: staging -> service_content одной транзакцией. -> число опубликованных."""
        self._ping()
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM service_content_staging")
            n = cur.fetchone()[0]
            if n:
                cols = ",".join(STAGE_COLS)
                cur.execute(
                    f"INSERT INTO service_content ({cols}, version, published_at) "
                    f"SELECT {cols}, 1, %s FROM service_content_staging "
                    "ON DUPLICATE KEY UPDATE source_hash=VALUES(source_hash), provider=VALUES(provider), "
                    "model=VALUES(model), lang=VALUES(lang), headline=VALUES(headline), tagline=VALUES(tagline), "
                    "content=VALUES(content), similarity=VALUES(similarity), rewritten_at=VALUES(rewritten_at), "
                    "version=service_content.version + 1, published_at=VALUES(published_at)",
                    (dt.datetime.now().replace(microsecond=0),))
                cur.execute("DELETE FROM service_content_staging")
            self.conn.commit()
            return int(n)
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()


# --------------------------------------------------------------------------
# Одна итерация: накопление в течение окна + периодическая публикация
# --------------------------------------------------------------------------
def run_rewrite(conn, cfg: RewriteConfig, store: ContentStore | None = None, provider=None,
                clock=time.monotonic) -> Counter:
    store = store or ContentStore(conn)
    provider = provider or make_provider(cfg)
    jobs = store.load_jobs(cfg)
    known = {} if cfg.force else store.known_hashes()
    stats: Counter = Counter()
    log.info("Рерайт: услуг в очереди %s, окно %.1f ч, публикация раз в %.0f мин, провайдер %s",
             len(jobs), cfg.window_s / 3600, cfg.flush_s / 60, provider.name)

    tls = threading.local()

    def work(ctx: ServiceCtx) -> Result:
        s = getattr(tls, "s", None)
        if s is None:
            s = tls.s = requests.Session()
            s.headers["User-Agent"] = "apify-store-tracker/2.0"
        return process_service(ctx, cfg, provider, known.get(ctx.service_id), s)

    started = last_flush = clock()
    deadline = started + cfg.window_s
    pending, inflight = deque(jobs), {}
    try:
        with ThreadPoolExecutor(max_workers=max(1, cfg.workers)) as ex:
            while pending or inflight:
                if clock() < deadline:
                    while pending and len(inflight) < cfg.workers * 2:
                        ctx = pending.popleft()
                        inflight[ex.submit(work, ctx)] = ctx
                elif pending:
                    stats["deferred"] = len(pending)
                    log.info("Рерайт: окно закончилось, отложено до следующего прогона: %s услуг", len(pending))
                    pending.clear()
                if not inflight:
                    break
                done, _ = wait(list(inflight), timeout=30, return_when=FIRST_COMPLETED)
                for fut in done:
                    ctx = inflight.pop(fut)
                    try:
                        res: Result = fut.result()
                    except Exception as exc:
                        stats["failed"] += 1
                        log.error("Рерайт %s: %s", ctx.display_name, exc)
                        continue
                    store.save_details(res.new_details)
                    stats[res.status] += 1
                    if res.status == "rewritten":
                        store.stage(res.row)
                    elif res.note:
                        log.warning("Рерайт %s: %s (%s)", ctx.display_name, res.status, res.note)
                if clock() - last_flush >= cfg.flush_s:
                    n = store.flush()
                    stats["published"] += n
                    last_flush = clock()
                    log.info("Публикация накопленного: %s карточек (обработано %s из %s)",
                             n, sum(v for k, v in stats.items() if k != "deferred"), len(jobs))
    finally:
        n = store.flush()   # хвост окна публикуем всегда, в т.ч. при падении/Ctrl+C
        stats["published"] += n
    log.info("Рерайт завершён за %.0f мин: %s", (clock() - started) / 60, dict(stats))
    return stats
