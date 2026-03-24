#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Centris Data Cleaner — Locations (Incremental)
Normalise les données brutes MongoDB → MongoDB clean collection.

Usage:
    python cleaner.py              # Incremental (only new docs)
    python cleaner.py --dry-run    # Preview sans écriture
    python cleaner.py --sample 5   # Afficher N exemples après nettoyage
    python cleaner.py --full       # Force re-clean de tout (drop + recreate)
"""

import os
import re
import html
import unicodedata
import argparse
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv
from storage import upload_images, check_b2

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGO_URI        = os.environ.get("MONGODB_URI", "")
if not MONGO_URI:
    raise RuntimeError("MONGODB_URI is not set or empty")
MONGO_DB         = os.getenv("MONGO_DB", "centris")
SOURCE_COLLECTION = os.getenv("MONGO_COLLECTION", "locations")
CLEAN_COLLECTION  = os.getenv("MONGO_COLLECTION_CLEAN", "locations_clean")
BATCH_SIZE = 500

MIN_SURFACE_SQFT = 100
MAX_SURFACE_SQFT = 20_000
MAX_BEDROOMS = 20

SQFT_TO_M2 = 0.092903

# ============================================================
# CATEGORY → PROPERTY TYPE MAP
# ============================================================

# Residential types to keep
# Commercial types to keep
CATEGORY_MAP = {
    # ── Residential ──
    "unifamiliale":    "house",
    "maison":          "house",
    "mobile":          "mobile_home",
    "intergénération": "multigenerational",
    "jumelé":          "house",
    "bungalow":        "house",
    "villa":           "house",
    "cottage":         "chalet",
    "chalet":          "chalet",
    "plex":            "plex",
    "condo":           "condo",
    "appartement":     "apartment",
    "studio":          "apartment",
    "loft":            "loft",
    # ── Commercial ──
    "bureau":          "office",
    "industriel":      "industrial",
    "commercial":      "commercial",
    "agricole":        "agricultural",
    "entreprise":      "business",
}


def map_property_type(category: str) -> str:
    if not category:
        return "other"
    cat_lower = category.lower()
    for keyword, ptype in CATEGORY_MAP.items():
        if keyword in cat_lower:
            return ptype
    return "other"


# ============================================================
# PARSING HELPERS
# ============================================================

def parse_int(value) -> int | None:
    """Extract first integer from a value (string or number)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    m = re.search(r'\d+', str(value))
    return int(m.group()) if m else None


def parse_price(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    cleaned = re.sub(r'[^\d]', '', str(value))
    return int(cleaned) if cleaned else None


def normalize_price_monthly(raw_price, raw_period=None) -> int | None:
    """Normalize price to monthly.
    If period contains 'an' / 'year' / 'annual' → divide by 12.
    """
    price = parse_price(raw_price)
    if price is None:
        return None
    if raw_period:
        period = str(raw_period).lower()
        if any(w in period for w in ("an", "year", "annual", "annuel", "/an", "par an")):
            return round(price / 12)
    return price


def parse_surface_sqft(raw: str) -> int | None:
    """Parse '1 310 pc' or '850 sq.ft' → integer sqft."""
    if not raw:
        return None
    m = re.search(r'[\d\s,]+', raw)
    if not m:
        return None
    num_str = re.sub(r'[\s,]', '', m.group())
    try:
        return int(num_str)
    except ValueError:
        return None


def parse_bedrooms(raw: str) -> int | None:
    """'2 chambres' → 2, 'Studio' → 0."""
    if not raw:
        return None
    if "studio" in raw.lower():
        return 0
    return parse_int(raw)


def parse_bathrooms(raw: str) -> dict | None:
    """'1 salle de bain et 1 salle d'eau' → {full: 1, half: 1, total: 2}."""
    if not raw:
        return None
    result = {}
    m = re.search(r'(\d+)\s+salle[s]?\s+de\s+bain', raw, re.IGNORECASE)
    result["full"] = int(m.group(1)) if m else 0

    m = re.search(r'(\d+)\s+salle[s]?\s+d.eau', raw, re.IGNORECASE)
    result["half"] = int(m.group(1)) if m else 0

    result["total"] = result["full"] + result["half"]
    return result if result["total"] > 0 else None


def parse_city(address: str) -> str | None:
    """Extract city from Centris address format.
    e.g. '1955, Rue du X, app. F, Trois-Rivières' → 'Trois-Rivières'
    """
    if not address:
        return None
    parts = [p.strip() for p in address.split(",")]
    # Last non-empty part that is not a unit/app indicator
    for part in reversed(parts):
        if part and not re.match(r'^(app|apt|unit|suite|bureau|local)\.?\s*\w*$', part, re.IGNORECASE):
            return part
    return parts[-1].strip() if parts else None


def parse_features(raw: str) -> list[str]:
    """'Animaux non acceptés, Fumeurs non acceptés, Ascenseur' → list."""
    if not raw:
        return []
    return [f.strip() for f in raw.split(",") if f.strip()]


def parse_move_in_date(raw: str) -> str | None:
    """Normalize date: '2024-07-01' → '2024-07-01', 'Disponible' → None."""
    if not raw:
        return None
    m = re.search(r'\d{4}-\d{2}-\d{2}', raw)
    return m.group() if m else None


def parse_year_built(raw: str) -> str | None:
    """'À construire, Neuve' → 'new', '1985' → '1985'."""
    if not raw:
        return None
    if re.search(r'constru|neuve|neuf', raw, re.IGNORECASE):
        return "new"
    m = re.search(r'\d{4}', raw)
    return m.group() if m else raw.strip() or None


def clean_description(raw: str) -> str | None:
    """Strip HTML, decode entities, normalize accents."""
    if not raw:
        return None
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html.unescape(text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    return text if len(text) > 10 else None


# ============================================================
# MAIN CLEAN FUNCTION
# ============================================================

def clean_document(doc: dict) -> dict:
    c = {}

    c["source_id"]        = doc.get("centris_no") or doc.get("listing_id")
    c["source"]           = "centris"
    c["country"]          = "CA"
    c["transaction_type"] = "rent"

    c["url"] = doc.get("canonical_url") or doc.get("url")

    c["property_type"] = map_property_type(doc.get("category", ""))

    # ── Address & location ──
    address = doc.get("address", "")
    c["address"] = address
    c["city"]    = parse_city(address)

    lat = doc.get("latitude")
    lng = doc.get("longitude")
    if lat is not None and lng is not None:
        if -90 <= lat <= 90 and -180 <= lng <= 180:
            c["latitude"]  = round(lat, 6)
            c["longitude"] = round(lng, 6)

    c["municipality_id"] = doc.get("municipality_id") or None

    # ── Price — normalize to monthly ──
    c["price"]    = normalize_price_monthly(doc.get("price"), doc.get("price_period"))
    c["currency"] = doc.get("currency") or "CAD"

    # ── Characteristics ──
    chars = doc.get("characteristics") or {}

    raw_surface = (
        doc.get("living_area")
        or chars.get("Superficie habitable")
        or chars.get("Superficie brute")
    )
    sqft = parse_surface_sqft(raw_surface)
    if sqft:
        c["surface_sqft"] = sqft
        c["surface_m2"]   = round(sqft * SQFT_TO_M2, 1)

    lot_raw = doc.get("lot_size") or chars.get("Superficie du terrain")
    lot_sqft = parse_surface_sqft(lot_raw)
    if lot_sqft:
        c["lot_sqft"] = lot_sqft
        c["lot_m2"]   = round(lot_sqft * SQFT_TO_M2, 1)

    # ── Rooms ──
    c["rooms"]     = parse_int(doc.get("rooms"))
    c["bedrooms"]  = parse_bedrooms(doc.get("bedrooms"))
    bathroom_info  = parse_bathrooms(doc.get("bathrooms"))
    if bathroom_info:
        c["bathrooms_full"]  = bathroom_info["full"]
        c["bathrooms_half"]  = bathroom_info["half"]
        c["bathrooms_total"] = bathroom_info["total"]

    # ── Building ──
    c["building_style"] = doc.get("building_style") or chars.get("Style de bâtiment") or None
    c["year_built"]     = parse_year_built(doc.get("year_built") or chars.get("Année de construction"))
    c["is_new"]         = c["year_built"] == "new"
    c["floor"]          = parse_int(chars.get("Étage")) if chars.get("Étage") else None

    # ── Parking ──
    parking_raw = doc.get("parking") or chars.get("Stationnement total")
    c["parking"]       = parking_raw or None
    c["parking_spots"] = parse_int(parking_raw)

    # ── Move-in date ──
    c["move_in_date"] = parse_move_in_date(
        doc.get("move_in_date") or chars.get("Date d'emménagement")
    )

    # ── Features from characteristics ──
    features_raw = chars.get("Caractéristiques additionnelles", "")
    features = parse_features(features_raw)
    c["features"]       = features if features else None
    c["has_elevator"]   = any("ascenseur" in f.lower() for f in features)
    c["allows_pets"]    = not any("animaux non" in f.lower() for f in features)
    c["is_furnished"]   = any("meublé" in f.lower() for f in features)
    c["has_ev_charger"] = any("borne" in f.lower() for f in features)
    c["has_balcony"]    = any("balcon" in f.lower() for f in features)
    c["no_smoking"]     = any("fumeurs non" in f.lower() for f in features)
    c["internet_incl"]  = any("internet inclus" in f.lower() for f in features)

    # ── WalkScore ──
    ws = doc.get("walkscore")
    c["walkscore"] = int(ws) if ws and str(ws).isdigit() else None

    # ── Description ──
    c["description"] = clean_description(doc.get("description"))

    # ── Brokers → agency + agent ──
    brokers = doc.get("brokers") or []
    if brokers:
        first = brokers[0] if brokers else {}
        # Agency: the firm/brokerage
        agency_name = first.get("agency") or first.get("agency_name") or first.get("firm")
        if agency_name:
            c["agency"] = {"name": agency_name}

        # Agent: the individual (everything except the agency name field)
        agent = {k: v for k, v in first.items()
                 if k not in ("agency", "agency_name", "firm") and v}
        if agent:
            c["agent"] = agent

        # Second broker if present (co-agent)
        if len(brokers) > 1:
            second = brokers[1]
            co_agent = {k: v for k, v in second.items()
                        if k not in ("agency", "agency_name", "firm") and v}
            if co_agent:
                c["co_agent"] = co_agent

    # ── Photos — upload to Backblaze B2 ──
    source_id  = c["source_id"]
    raw_photos = doc.get("photos") or []
    photos     = upload_images("centris", source_id, raw_photos)
    c["photos"]      = photos
    c["photo_count"] = len(photos)

    # ── Derived metrics ──
    price  = c.get("price")
    sqft_c = c.get("surface_sqft")
    m2     = c.get("surface_m2")
    rooms  = c.get("rooms")
    beds   = c.get("bedrooms")

    if price and sqft_c and sqft_c > 0:
        c["price_per_sqft"] = round(price / sqft_c, 2)
    if price and m2 and m2 > 0:
        c["price_per_m2"] = round(price / m2, 2)
    if price and beds and beds > 0:
        c["price_per_bedroom"] = round(price / beds, 2)
    if sqft_c and rooms and rooms > 0:
        c["sqft_per_room"] = round(sqft_c / rooms, 2)

    # ── Timestamps ──
    c["first_seen"]  = doc.get("first_seen")
    c["scraped_at"]  = doc.get("scraped_at")
    c["cleaned_at"]  = datetime.now(timezone.utc)

    return {k: v for k, v in c.items() if v is not None and v != [] and v != {}}


# ============================================================
# VALIDATION
# ============================================================

# Types NOT in the wanted residential/commercial list → reject
EXCLUDED_TYPES = {"garage", "parking", "land", "farm", "other"}


def validate(doc: dict) -> tuple[bool, str | None]:
    ptype = doc.get("property_type")
    if ptype in EXCLUDED_TYPES:
        return False, "excluded_type"

    # Price: only reject if completely absent (currency varies per listing)
    if doc.get("price") is None:
        return False, "missing_price"

    if not doc.get("city"):
        return False, "missing_city"

    beds = doc.get("bedrooms")
    if beds is not None and beds > MAX_BEDROOMS:
        return False, "aberrant_bedrooms"

    sqft = doc.get("surface_sqft")
    if sqft is not None and (sqft < MIN_SURFACE_SQFT or sqft > MAX_SURFACE_SQFT):
        return False, "aberrant_surface"

    return True, None


# ============================================================
# DB HELPERS
# ============================================================

def connect_db():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return client, db


def ensure_clean_collection(db):
    col = db[CLEAN_COLLECTION]
    col.create_index([("source_id", ASCENDING)], unique=True, name="source_id_unique")
    col.create_index([("city", ASCENDING)])
    col.create_index([("property_type", ASCENDING)])
    col.create_index([("price", ASCENDING)])
    col.create_index([("surface_m2", ASCENDING)])
    col.create_index([("bedrooms", ASCENDING)])
    col.create_index([("latitude", ASCENDING), ("longitude", ASCENDING)])
    col.create_index([("city", ASCENDING), ("property_type", ASCENDING), ("price", ASCENDING)])
    return col


def setup_clean_collection_full(db):
    col = db[CLEAN_COLLECTION]
    col.drop()
    print(f"  '{CLEAN_COLLECTION}' reset (full mode)")
    return ensure_clean_collection(db)


def insert_batch(col, batch):
    """Upsert batch by source_id (idempotent)."""
    if not batch:
        return 0, 0
    ops = [
        UpdateOne(
            {"source_id": doc["source_id"]},
            {
                # first_seen must NOT appear in $set — it would conflict with $setOnInsert
                "$set": {k: v for k, v in doc.items() if k != "first_seen"},
                "$setOnInsert": {"first_seen": doc.get("first_seen")},
            },
            upsert=True,
        )
        for doc in batch
    ]
    try:
        r = col.bulk_write(ops, ordered=False)
        ins = r.upserted_count
        dup = r.matched_count
    except BulkWriteError as e:
        ins = e.details.get("nUpserted", 0)
        dup = len(batch) - ins
    return ins, dup


# ============================================================
# PIPELINE
# ============================================================

def run(source, clean, dry_run=False):
    from bson import ObjectId

    total = source.count_documents({})
    print(f"  Source total: {total} docs")

    existing_ids = set()
    if not dry_run and clean is not None:
        print("  Loading already-cleaned IDs...")
        existing_ids = {
            str(d["source_id"]) for d in clean.find({}, {"source_id": 1, "_id": 0})
            if d.get("source_id") is not None
        }
        print(f"  Already cleaned: {len(existing_ids)}")

    base_query = {"centris_no": {"$nin": list(existing_ids)}} if existing_ids else {}
    pending    = source.count_documents(base_query)

    print(f"  Pending (not yet cleaned): {pending}\n")

    if pending == 0:
        print("  Nothing new to clean.")
        return

    stats = {
        "total": total, "pending": pending, "cleaned": 0, "inserted": 0,
        "invalid_price": 0, "missing_city": 0, "aberrant_bedrooms": 0,
        "aberrant_surface": 0, "excluded_type": 0, "duplicates": 0, "errors": 0,
    }

    batch            = []
    rejection_samples = {}
    processed        = 0
    last_id          = None

    # ── Pagination par _id (évite les curseurs longue durée) ──
    while True:
        page_query = dict(base_query)
        if last_id is not None:
            page_query["_id"] = {"$gt": last_id}

        page = list(source.find(page_query).sort("_id", ASCENDING).limit(BATCH_SIZE))
        if not page:
            break

        last_id = page[-1]["_id"]

        for doc in page:
            try:
                cleaned = clean_document(doc)

                if not cleaned.get("source_id"):
                    stats["errors"] += 1
                    continue

                stats["cleaned"] += 1

                valid, reason = validate(cleaned)
                if not valid:
                    stats[reason] = stats.get(reason, 0) + 1
                    if reason not in rejection_samples:
                        rejection_samples[reason] = {
                            "source_id": cleaned.get("source_id"),
                            "price": cleaned.get("price"),
                            "city": cleaned.get("city"),
                        }
                    continue

                cleaned.pop("_id", None)

                if dry_run:
                    stats["inserted"] += 1
                    continue

                batch.append(cleaned)

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 10:
                    import traceback
                    print(f"\n  Error on {doc.get('centris_no')}: {str(e)[:200]}")
                    traceback.print_exc()

        processed += len(page)

        # Flush batch après chaque page
        if batch and not dry_run:
            ins, dup = insert_batch(clean, batch)
            stats["inserted"] += ins
            stats["duplicates"] += dup
            batch = []

        pct = processed / pending * 100 if pending else 0
        print(
            f"  {processed}/{pending} ({pct:.1f}%) — "
            f"{stats['inserted']} inserted | {stats['duplicates']} updated | "
            f"{stats.get('invalid_price',0)} bad_price | {stats.get('missing_city',0)} no_city | "
            f"{stats['errors']} errors",
            flush=True,
        )

    if batch and not dry_run:
        ins, dup = insert_batch(clean, batch)
        stats["inserted"] += ins
        stats["duplicates"] += dup

    print_stats(stats, dry_run)

    if rejection_samples:
        print("\n  Rejection samples (first per reason):")
        for reason, sample in rejection_samples.items():
            print(f"    [{reason}] source_id={sample['source_id']} "
                  f"price={sample['price']} city={sample['city']}")


def print_stats(s, dry_run=False):
    ins = s["inserted"]
    rejected = s["cleaned"] - ins - s["duplicates"]

    print(f"\n\n{'='*60}")
    print(f"  CLEANING RESULTS {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"  Source total:    {s['total']}")
    print(f"  Pending:         {s['pending']}")
    print(f"  Processed:       {s['cleaned']}")
    print(f"  Inserted:        {ins}")
    if rejected > 0:
        print(f"  Rejected:        {rejected}")
        print(f"    Bad price:       {s.get('invalid_price', 0)}")
        print(f"    Missing city:    {s.get('missing_city', 0)}")
        print(f"    Bad bedrooms:    {s.get('aberrant_bedrooms', 0)}")
        print(f"    Bad surface:     {s.get('aberrant_surface', 0)}")
    if s["duplicates"]:
        print(f"  Duplicates:      {s['duplicates']}")
    if s["errors"]:
        print(f"  Errors:          {s['errors']}")
    print(f"{'='*60}")


def show_sample(clean, n=3):
    print(f"\n  SAMPLE CLEANED DOCUMENTS ({n}):")
    for doc in clean.find({}, {"_id": 0}).limit(n):
        print("  " + "-" * 58)
        for k, v in doc.items():
            if k == "photos":
                print(f"    {k}: [{len(v)} urls]")
            elif k == "description":
                print(f"    {k}: {str(v)[:80]}...")
            elif k == "features":
                print(f"    {k}: {v[:3]}{'...' if len(v) > 3 else ''}")
            else:
                print(f"    {k}: {v}")
    print("  " + "-" * 58)


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Centris Cleaner — Incremental")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing")
    parser.add_argument("--full",    action="store_true", help="Drop + recreate (full re-clean)")
    parser.add_argument("--sample",  type=int, default=0, help="Show N sample docs after")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  CENTRIS CLEANER — LOCATIONS")
    print(f"  {SOURCE_COLLECTION} → {CLEAN_COLLECTION}")
    mode = "DRY RUN" if args.dry_run else ("FULL RE-CLEAN" if args.full else "INCREMENTAL")
    print(f"  Mode: {mode}")
    print("=" * 60 + "\n")

    check_b2()
    print()

    client, db = connect_db()
    source = db[SOURCE_COLLECTION]

    if args.dry_run:
        run(source, None, dry_run=True)
    elif args.full:
        clean = setup_clean_collection_full(db)
        run(source, clean)
        if args.sample > 0:
            show_sample(clean, args.sample)
    else:
        clean = ensure_clean_collection(db)
        run(source, clean)
        if args.sample > 0:
            show_sample(clean, args.sample)
        print(f"\n  Done! '{CLEAN_COLLECTION}': {clean.count_documents({})} total docs")

    client.close()


if __name__ == "__main__":
    main()
