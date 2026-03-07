"""
Centris.ca Property Scraper + MongoDB Storage
===============================================
Scrapes property listings from Centris.ca and stores them in MongoDB.

SETUP:
    pip install requests beautifulsoup4 lxml pandas pymongo python-dotenv
    # For search pages (dynamic):
    pip install playwright && playwright install chromium

.env FILE:
    MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
    MONGO_DB=centris
    MONGO_COLLECTION=listings

USAGE:
    python centris_scraper.py
"""

import json
import os
import re
import time
import random
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# CONFIG — all from environment variables
# ──────────────────────────────────────────────

MONGO_URI        = os.environ["MONGO_URI"]
MONGO_DB         = os.getenv("MONGO_DB", "centris")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "locations")
CYCLE_SLEEP      = int(os.getenv("CYCLE_SLEEP", "86400"))   # seconds between full runs
MAX_PAGES        = int(os.getenv("MAX_PAGES", "9999"))      # 9999 = no limit
MAX_DETAILS      = int(os.getenv("MAX_DETAILS", "999999"))  # 999999 = no limit
SCRAPE_24H_SKIP  = os.getenv("SCRAPE_24H_SKIP", "true").lower() == "true"

CENTRIS_SEARCH_URL = os.getenv(
    "CENTRIS_SEARCH_URL",
    (
        "https://www.centris.ca/fr/propriete~a-louer"
        "?q=H4sIAAAAAAAACo2QMQ-CQAyF_8vNDKy6GRON0RgjhoU4VO4BFw8Oe6AhhP9uiQ6oi536Xr_XJu1V"
        "ab2aq1AF6sLuCl46DTFEuywzKbboXrL1WMPlTHXRRQXVkFwYKD-2scFDZHIWDeK02FP53pIZ24Dfw8z"
        "Aah-Tbcd00r-MjRZ0SQ1yx51E7uNcrCO80agaQ1YNwRSOYK2p8lNX44Ovmi9wZ-7CLRg04aJbS4wV8E"
        "NTpf9lx2MHlgdN4PAPZjYpNZyHJz4eXrmCAQAA"
        "&sort=None&pageSize=20"
    ),
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY_RANGE = (1.5, 3.0)


def delay():
    time.sleep(random.uniform(*DELAY_RANGE))


# ──────────────────────────────────────────────
# MONGODB CONNECTION
# ──────────────────────────────────────────────

class MongoDB:
    """MongoDB wrapper for Centris listings."""

    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]

        # Create indexes for fast lookups
        self.collection.create_index("centris_no", unique=True, sparse=True)
        self.collection.create_index("listing_id", sparse=True)
        self.collection.create_index("price")
        self.collection.create_index("address")
        self.collection.create_index([("latitude", 1), ("longitude", 1)])
        self.collection.create_index("scraped_at")

        print(f"  ✅ Connected to MongoDB: {MONGO_DB}.{MONGO_COLLECTION}")

    def upsert(self, data: dict) -> str:
        """
        Insert or update a listing by centris_no.
        Returns 'inserted' or 'updated'.
        """
        centris_no = data.get("centris_no")
        if not centris_no:
            result = self.collection.insert_one(data)
            return f"inserted ({result.inserted_id})"

        data["updated_at"] = datetime.utcnow()

        result = self.collection.update_one(
            {"centris_no": centris_no},
            {
                "$set": data,
                "$setOnInsert": {"first_seen": datetime.utcnow()}
            },
            upsert=True
        )

        if result.upserted_id:
            return "inserted"
        elif result.modified_count > 0:
            return "updated"
        return "unchanged"

    def bulk_upsert(self, items: list[dict]) -> dict:
        """Bulk upsert multiple listings. Returns stats."""
        if not items:
            return {"inserted": 0, "updated": 0, "errors": 0}

        ops = []
        for item in items:
            centris_no = item.get("centris_no")
            if not centris_no:
                continue
            item["updated_at"] = datetime.utcnow()
            ops.append(
                UpdateOne(
                    {"centris_no": centris_no},
                    {
                        "$set": item,
                        "$setOnInsert": {"first_seen": datetime.utcnow()}
                    },
                    upsert=True
                )
            )

        if not ops:
            return {"inserted": 0, "updated": 0, "errors": 0}

        result = self.collection.bulk_write(ops, ordered=False)
        return {
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "matched": result.matched_count,
        }

    def find_by_centris_no(self, centris_no: str) -> dict | None:
        return self.collection.find_one({"centris_no": centris_no}, {"_id": 0})

    def find_by_price_range(self, min_price: int, max_price: int) -> list[dict]:
        return list(self.collection.find(
            {"price": {"$gte": str(min_price), "$lte": str(max_price)}},
            {"_id": 0}
        ))

    def find_by_city(self, city_keyword: str) -> list[dict]:
        return list(self.collection.find(
            {"address": {"$regex": city_keyword, "$options": "i"}},
            {"_id": 0}
        ))

    def count(self) -> int:
        return self.collection.count_documents({})

    def get_stats(self) -> dict:
        pipeline = [
            {"$group": {
                "_id": None,
                "total": {"$sum": 1},
                "with_price": {"$sum": {"$cond": [{"$ne": ["$price", ""]}, 1, 0]}},
                "with_photos": {"$sum": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$photos", []]}}, 0]}, 1, 0]}},
                "categories": {"$addToSet": "$category"},
            }}
        ]
        result = list(self.collection.aggregate(pipeline))
        return result[0] if result else {}

    def close(self):
        self.client.close()


# ──────────────────────────────────────────────
# SCRAPE A SINGLE DETAIL PAGE
# ──────────────────────────────────────────────

def scrape_detail_page(url: str, session: requests.Session = None) -> dict:
    """
    Scrape a Centris property detail page using requests + BS4.
    No headless browser needed — pages are SSR.
    """
    s = session or requests.Session()
    s.headers.update(HEADERS)

    resp = s.get(url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    data = {"url": url, "scraped_at": datetime.utcnow().isoformat()}

    # ── Centris No ──
    el = soup.select_one("#ListingDisplayId")
    data["centris_no"] = el.get_text(strip=True) if el else ""

    # ── Listing ID ──
    el = soup.select_one("#ListingId")
    data["listing_id"] = el.get_text(strip=True) if el else ""

    # ── Category ──
    el = soup.select_one("span[data-id='PageTitle']")
    data["category"] = el.get_text(strip=True) if el else ""

    # ── Price ──
    el = soup.select_one("meta[itemprop='price']")
    data["price"] = el["content"] if el and el.get("content") else ""

    el = soup.select_one("meta[itemprop='priceCurrency']")
    data["currency"] = el["content"] if el and el.get("content") else "CAD"

    price_div = soup.select_one(".price-container .price, .house-info .price")
    data["price_display"] = price_div.get_text(strip=True) if price_div else ""

    # ── Address ──
    el = soup.select_one("h2[itemprop='address']")
    data["address"] = el.get_text(strip=True) if el else ""

    # ── GPS ──
    lat_el = soup.select_one("meta[itemprop='latitude']")
    lng_el = soup.select_one("meta[itemprop='longitude']")
    data["latitude"] = float(lat_el["content"]) if lat_el and lat_el.get("content") else None
    data["longitude"] = float(lng_el["content"]) if lng_el and lng_el.get("content") else None

    # ── GeoJSON (for MongoDB geo queries) ──
    if data["latitude"] and data["longitude"]:
        data["location"] = {
            "type": "Point",
            "coordinates": [data["longitude"], data["latitude"]]
        }

    # ── Teaser stats ──
    el = soup.select_one(".teaser .piece")
    data["rooms"] = el.get_text(strip=True) if el else ""

    el = soup.select_one(".teaser .cac")
    data["bedrooms"] = el.get_text(strip=True) if el else ""

    el = soup.select_one(".teaser .sdb")
    data["bathrooms"] = el.get_text(strip=True) if el else ""

    # ── Characteristics ──
    characteristics = {}
    for carac in soup.select(".carac-container"):
        title_el = carac.select_one(".carac-title")
        value_el = carac.select_one(".carac-value span")
        if title_el and value_el:
            key = title_el.get_text(strip=True)
            val = value_el.get_text(strip=True)
            characteristics[key] = val
    data["characteristics"] = characteristics

    data["building_style"] = characteristics.get("Style de bâtiment", "")
    data["year_built"] = characteristics.get("Année de construction", "")
    data["living_area"] = characteristics.get("Superficie habitable", "")
    data["lot_size"] = characteristics.get("Superficie du terrain", "")
    data["parking"] = characteristics.get("Stationnement total", "")
    data["move_in_date"] = characteristics.get("Date d'emménagement", "")

    # ── WalkScore ──
    ws_el = soup.select_one(".walkscore a span")
    data["walkscore"] = ws_el.get_text(strip=True) if ws_el else ""

    # ── Description ──
    el = soup.select_one("div[itemprop='description']")
    data["description"] = el.get_text(strip=True) if el else ""

    # ── Brokers ──
    brokers = []
    for broker_div in soup.select(".broker-info"):
        broker = {}
        name_el = broker_div.select_one(".broker-info__broker-title")
        broker["name"] = name_el.get_text(strip=True) if name_el else ""

        job_el = broker_div.select_one("[itemprop='jobTitle']")
        broker["title"] = job_el.get_text(strip=True) if job_el else ""

        agency_el = broker_div.select_one(".broker-info__agency-name")
        broker["agency"] = agency_el.get_text(strip=True) if agency_el else ""

        phone_el = broker_div.select_one("a[itemprop='telephone']")
        broker["phone"] = phone_el.get_text(strip=True) if phone_el else ""

        img_el = broker_div.select_one(".broker-info-broker-image")
        broker["photo"] = img_el["src"] if img_el and img_el.get("src") else ""

        if broker.get("name"):
            brokers.append(broker)

    seen = set()
    unique_brokers = []
    for b in brokers:
        if b["name"] not in seen:
            seen.add(b["name"])
            unique_brokers.append(b)
    data["brokers"] = unique_brokers

    # ── Photos ──
    photos = []
    for script in soup.find_all("script"):
        txt = script.string or ""
        if "MosaicPhotoUrls" in txt:
            match = re.search(r'MosaicPhotoUrls\s*=\s*\[(.*?)\]', txt, re.DOTALL)
            if match:
                photos = re.findall(r'"(https?://[^"]+)"', match.group(1))
            break
    data["photos"] = photos
    data["photo_count"] = len(photos)

    og_img = soup.select_one("meta[property='og:image']")
    data["main_photo"] = og_img["content"] if og_img and og_img.get("content") else ""

    # ── Municipality ID ──
    el = soup.select_one("#municipalityId")
    data["municipality_id"] = el.get_text(strip=True) if el else ""

    # ── URLs ──
    el = soup.select_one("link[rel='canonical']")
    data["canonical_url"] = el["href"] if el and el.get("href") else ""

    el = soup.select_one("link[hreflang='en']")
    data["url_en"] = el["href"] if el and el.get("href") else ""

    return data


# ──────────────────────────────────────────────
# SCRAPE + STORE PIPELINE
# ──────────────────────────────────────────────

def scrape_and_store(urls: list[str], batch_size: int = 10):
    """
    Scrape detail pages and store in MongoDB.
    Processes in batches for efficient bulk upserts.
    """
    db = MongoDB()
    session = requests.Session()
    session.headers.update(HEADERS)

    total = len(urls)
    all_results = []
    batch = []
    stats = {"inserted": 0, "updated": 0, "errors": 0, "skipped": 0}

    print(f"\n🏠 Scraping {total} properties → MongoDB\n")

    for i, url in enumerate(urls, 1):
        print(f"  [{i}/{total}] {url}")

        # Check if already scraped recently (within 24h)
        if SCRAPE_24H_SKIP:
            centris_no_match = re.search(r'/(\d+)$', url)
            if centris_no_match:
                existing = db.find_by_centris_no(centris_no_match.group(1))
                if existing and existing.get("updated_at"):
                    from datetime import timedelta
                    last_update = existing["updated_at"]
                    if isinstance(last_update, str):
                        last_update = datetime.fromisoformat(last_update)
                    if datetime.utcnow() - last_update < timedelta(hours=24):
                        print(f"    ⏭️  Skipped (scraped < 24h ago)")
                        stats["skipped"] += 1
                        continue

        try:
            data = scrape_detail_page(url, session)
            batch.append(data)
            all_results.append(data)
            print(f"    ✅ {data.get('centris_no')} — {data.get('price')}$ — {data.get('address', '')[:50]}")
        except Exception as e:
            print(f"    ❌ Error: {e}")
            stats["errors"] += 1

        # Bulk upsert when batch is full
        if len(batch) >= batch_size:
            result = db.bulk_upsert(batch)
            stats["inserted"] += result["inserted"]
            stats["updated"] += result["updated"]
            print(f"\n    💾 Batch saved: +{result['inserted']} new, ~{result['updated']} updated\n")
            batch = []

        if i < total:
            delay()

    # Save remaining batch
    if batch:
        result = db.bulk_upsert(batch)
        stats["inserted"] += result["inserted"]
        stats["updated"] += result["updated"]

    # Print summary
    print("\n" + "=" * 50)
    print("📊 SCRAPING SUMMARY")
    print("=" * 50)
    print(f"  Total URLs:    {total}")
    print(f"  Inserted:      {stats['inserted']}")
    print(f"  Updated:       {stats['updated']}")
    print(f"  Skipped:       {stats['skipped']}")
    print(f"  Errors:        {stats['errors']}")
    print(f"  DB total:      {db.count()} listings")
    print("=" * 50)

    db.close()
    return all_results


# ──────────────────────────────────────────────
# SEARCH → SCRAPE → STORE (full pipeline)
# ──────────────────────────────────────────────

async def full_pipeline(search_url: str, max_pages: int = 2, max_details: int = 20):
    """
    1. Scrape search results (Playwright) → get URLs
    2. Scrape each detail page (requests) → extract data
    3. Store in MongoDB with upsert
    """
    from playwright.async_api import async_playwright

    print("=" * 60)
    print("  CENTRIS FULL PIPELINE → MongoDB")
    print("=" * 60)

    # ── Step 1: Get URLs from search ──
    detail_urls = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=HEADERS["User-Agent"],
            locale="fr-CA",
        )
        page = await ctx.new_page()

        print(f"\n🔍 Loading search: {search_url[:80]}...")
        await page.goto(search_url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)

        # Dismiss Didomi cookie consent popup if present
        try:
            consent_btn = await page.query_selector(
                "#didomi-notice-agree-button, "
                "button[aria-label*='accepter'], "
                "button[aria-label*='Accepter'], "
                ".didomi-continue-without-agreeing, "
                "#didomi-notice-disagree-button"
            )
            if consent_btn:
                await consent_btn.click()
                await page.wait_for_timeout(1000)
                print("  🍪 Cookie popup dismissed")
        except Exception:
            pass

        for pg in range(1, max_pages + 1):
            print(f"\n📄 Search page {pg}/{max_pages}")

            links = await page.eval_on_selector_all(
                "a[href*='~']",
                """els => els.map(el => el.href).filter(h =>
                    h && (h.includes('~a-vendre') || h.includes('~a-louer')
                       || h.includes('~for-sale') || h.includes('~for-rent'))
                    && /\\/\\d+$/.test(h)
                )"""
            )

            new_links = list(set(links) - set(detail_urls))
            detail_urls.extend(new_links)
            print(f"  Found {len(new_links)} new URLs (total: {len(detail_urls)})")

            if pg < max_pages:
                next_btn = await page.query_selector(
                    "li.next a, a.next, "
                    "[data-action='next'], "
                    "a[aria-label='Suivant'], "
                    "a[aria-label='Next'], "
                    ".pager-next a, "
                    "li.pagination-next a"
                )
                if next_btn:
                    # Scroll into view and use JS click to bypass any overlay
                    await next_btn.scroll_into_view_if_needed()
                    await page.evaluate("el => el.click()", next_btn)
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    await page.wait_for_timeout(2000)
                else:
                    print("  No next page.")
                    break

        await browser.close()

    if not detail_urls:
        print("\n❌ No URLs found.")
        return

    # ── Step 2 + 3: Scrape details → MongoDB ──
    urls_to_scrape = detail_urls[:max_details]
    results = scrape_and_store(urls_to_scrape)

    # Also save local JSON backup
    Path("centris_backup.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8"
    )
    print(f"\n📁 Local backup: centris_backup.json")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import asyncio

    run = 1
    while True:
        print(f"\n{'=' * 60}")
        print(f"  RUN #{run} — {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"{'=' * 60}")
        try:
            asyncio.run(full_pipeline(
                search_url=CENTRIS_SEARCH_URL,
                max_pages=MAX_PAGES,
                max_details=MAX_DETAILS,
            ))
        except Exception as e:
            print(f"\n❌ Pipeline failed: {e}")

        print(f"\n💤 Sleeping {CYCLE_SLEEP}s until next run...")
        time.sleep(CYCLE_SLEEP)
        run += 1