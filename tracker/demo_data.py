"""Синтетические Actor'ы в формате Apify Store API: чтобы проверить MySQL-схему, рейтинги и фронт без обхода Store."""
from __future__ import annotations

import random
import string

PLATFORMS = ["Instagram", "TikTok", "YouTube", "LinkedIn", "Twitter", "Facebook", "Reddit",
             "Google Maps", "Amazon", "Airbnb", "Zillow", "Indeed", "Booking", "eBay"]
# (слово в названии, как часто встречается)
SERVICE_WORDS = [("", 6), ("Post", 5), ("Profile", 5), ("Followers", 4), ("Comments", 4), ("Reviews", 4),
                 ("Hashtag", 3), ("Search", 3), ("Email", 3), ("Product", 3), ("Location", 9), ("Reels", 3)]
DEVS = ["apify", "clockworks", "curious_coder", "lukaskrivka", "data_hawk", "scrapeking", "api_ninja",
        "webharvest", "pixel_miner", "bright_bits", "fastcrawl", "nexus_data", "tidy_scraper", "orbit_labs"]


def _pricing(rnd: random.Random) -> dict:
    kind = rnd.choices(["PRICE_PER_DATASET_ITEM", "PAY_PER_EVENT", "FLAT_PRICE_PER_MONTH", "FREE"], [5, 3, 2, 1])[0]
    if kind == "PRICE_PER_DATASET_ITEM":
        return {"pricingModel": kind, "pricePerUnitUsd": round(rnd.uniform(0.5, 8) / 1000, 6), "unitName": "result"}
    if kind == "PAY_PER_EVENT":
        return {"pricingModel": kind, "pricingPerEvent": {"actorChargeEvents": {
            "actor-start": {"eventTitle": "Actor start", "eventPriceUsd": 0.005},
            "item": {"eventTitle": "Result", "eventPriceUsd": round(rnd.uniform(0.5, 6) / 1000, 6)}}}}
    if kind == "FLAT_PRICE_PER_MONTH":
        return {"pricingModel": kind, "pricePerUnitUsd": rnd.choice([9, 19, 29, 49, 99]), "trialMinutes": 4320}
    return {"pricingModel": "FREE"}


def make_items(n: int = 3000, seed: int = 7) -> list[dict]:
    rnd = random.Random(seed)
    items, seen = [], set()
    words = [w for w, _ in SERVICE_WORDS]
    weights = [x for _, x in SERVICE_WORDS]
    while len(items) < n:
        platform = rnd.choice(PLATFORMS)
        word = rnd.choices(words, weights)[0]
        dev = rnd.choice(DEVS) + str(rnd.randint(1, 40))
        title = " ".join(x for x in (platform, word, rnd.choice(["Scraper", "Scraper", "Extractor", "API"])) if x)
        name = title.lower().replace(" ", "-") + (f"-{rnd.randint(2, 9)}" if rnd.random() < 0.3 else "")
        if (dev, name) in seen:
            continue
        seen.add((dev, name))
        users = int(rnd.paretovariate(1.1) * 3)
        reviews = int(users * rnd.uniform(0.0, 0.08))
        items.append({
            "id": "".join(rnd.choices(string.ascii_letters + string.digits, k=17)),
            "username": dev, "name": name, "title": title, "url": f"https://apify.com/{dev}/{name}",
            "stats": {"totalUsers": users, "totalUsers30Days": int(users * rnd.uniform(0, 0.4)),
                      "totalRuns": users * rnd.randint(5, 400)},
            "actorReviewRating": round(rnd.uniform(3.2, 5.0), 2) if reviews else 0, "actorReviewCount": reviews,
            "categories": rnd.choice([["SOCIAL_MEDIA"], ["LEAD_GENERATION", "SOCIAL_MEDIA"], ["ECOMMERCE"], ["TRAVEL"]]),
            "currentPricingInfo": _pricing(rnd),
        })
    return items
