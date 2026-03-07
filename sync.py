#!/usr/bin/env python3
"""
Centris — MongoDB (locations_clean) -> PostgreSQL Sync + Archive Checker
Incremental: only syncs new docs, archives dead listings.

Usage:
    python sync.py               # Run once (sync + archive)
    python sync.py --sync-only   # Skip archive check
    python sync.py --archive-only
"""

import os
import time
import logging
import argparse
import requests
from datetime import datetime, timezone

from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGO_URI  = os.environ["MONGODB_URI"]
MONGO_DB   = os.getenv("MONGO_DB", "centris")
MONGO_COL  = os.getenv("MONGO_COLLECTION_CLEAN", "locations_clean")

PG_DSN     = os.environ.get("POSTGRES_DSN", "")
if not PG_DSN:
    raise RuntimeError(
        "POSTGRES_DSN is not set or empty. "
        "Set it in your .env: POSTGRES_DSN=postgresql://user:pass@host:5432/dbname"
    )
PG_TABLE   = os.getenv("PG_TABLE_CENTRIS", "centris_listings")
PG_ARCHIVE = os.getenv("PG_ARCHIVE_CENTRIS", "centris_archive")

BATCH_SIZE      = int(os.getenv("BATCH_SIZE", "500"))
CYCLE_SLEEP     = int(os.getenv("CYCLE_SLEEP", "86400"))
ARCHIVE_DELAY   = 2
ARCHIVE_TIMEOUT = 15

# Centris home/search pages — if the listing redirects here it's dead
DEAD_REDIRECTS = {
    "https://www.centris.ca",
    "https://www.centris.ca/fr",
    "https://www.centris.ca/fr/propriete~a-louer",
    "https://www.centris.ca/fr/propriete~a-vendre",
}

CHECK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("centris-sync")

# ============================================================
# HELPERS
# ============================================================

def to_pg_array(lst):
    if not lst or not isinstance(lst, list):
        return None
    cleaned = [str(x) for x in lst if x]
    return cleaned if cleaned else None


def to_pg_date(v):
    if not v:
        return None
    if isinstance(v, str):
        try:
            return datetime.strptime(v, "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def to_pg_timestamp(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, dict) and "$date" in v:
        try:
            return datetime.fromisoformat(v["$date"].replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


# ============================================================
# ROW BUILDER
# ============================================================

def build_row(d):
    """Build PG row tuple from a cleaned centris document."""
    return (
        d.get("source_id"),                           # source_id
        d.get("source", "centris"),                   # source
        d.get("country", "CA"),                       # country
        d.get("transaction_type", "rent"),            # transaction_type
        d.get("url"),                                 # url
        d.get("url_en"),                              # url_en
        d.get("url_fr"),                              # url_fr
        d.get("property_type"),                       # property_type
        d.get("address"),                             # address
        d.get("city"),                                # city
        d.get("municipality_id"),                     # municipality_id
        d.get("latitude"),                            # latitude
        d.get("longitude"),                           # longitude
        d.get("price"),                               # price
        d.get("currency", "CAD"),                     # currency
        d.get("price_per_sqft"),                      # price_per_sqft
        d.get("price_per_m2"),                        # price_per_m2
        d.get("price_per_bedroom"),                   # price_per_bedroom
        d.get("surface_sqft"),                        # surface_sqft
        d.get("surface_m2"),                          # surface_m2
        d.get("lot_sqft"),                            # lot_sqft
        d.get("lot_m2"),                              # lot_m2
        d.get("sqft_per_room"),                       # sqft_per_room
        d.get("rooms"),                               # rooms
        d.get("bedrooms"),                            # bedrooms
        d.get("bathrooms_full"),                      # bathrooms_full
        d.get("bathrooms_half"),                      # bathrooms_half
        d.get("bathrooms_total"),                     # bathrooms_total
        d.get("floor"),                               # floor
        d.get("building_style"),                      # building_style
        d.get("year_built"),                          # year_built
        d.get("is_new", False),                       # is_new
        d.get("parking"),                             # parking
        d.get("parking_spots"),                       # parking_spots
        to_pg_date(d.get("move_in_date")),            # move_in_date
        d.get("has_elevator", False),                 # has_elevator
        d.get("allows_pets", True),                   # allows_pets
        d.get("is_furnished", False),                 # is_furnished
        d.get("has_ev_charger", False),               # has_ev_charger
        d.get("has_balcony", False),                  # has_balcony
        d.get("no_smoking", False),                   # no_smoking
        d.get("internet_incl", False),                # internet_incl
        to_pg_array(d.get("features")),               # features
        d.get("walkscore"),                           # walkscore
        d.get("description"),                         # description
        d.get("agency_name"),                         # agency_name
        d.get("broker_count"),                        # broker_count
        d.get("main_photo"),                          # main_photo
        d.get("photo_count", 0),                      # photo_count
        to_pg_array(d.get("photos")),                 # photos
        to_pg_timestamp(d.get("first_seen")),         # first_seen
        to_pg_timestamp(d.get("scraped_at")),         # scraped_at
        to_pg_timestamp(d.get("cleaned_at")),         # cleaned_at
    )


INSERT_SQL = f"""
    INSERT INTO {{table}} (
        source_id, source, country, transaction_type,
        url, url_en, url_fr,
        property_type,
        address, city, municipality_id,
        latitude, longitude,
        price, currency,
        price_per_sqft, price_per_m2, price_per_bedroom,
        surface_sqft, surface_m2, lot_sqft, lot_m2, sqft_per_room,
        rooms, bedrooms, bathrooms_full, bathrooms_half, bathrooms_total, floor,
        building_style, year_built, is_new,
        parking, parking_spots, move_in_date,
        has_elevator, allows_pets, is_furnished, has_ev_charger,
        has_balcony, no_smoking, internet_incl,
        features, walkscore, description,
        agency_name, broker_count,
        main_photo, photo_count, photos,
        first_seen, scraped_at, cleaned_at
    ) VALUES %s
    ON CONFLICT (source_id) DO UPDATE SET
        price           = EXCLUDED.price,
        url             = EXCLUDED.url,
        description     = EXCLUDED.description,
        photos          = EXCLUDED.photos,
        photo_count     = EXCLUDED.photo_count,
        main_photo      = EXCLUDED.main_photo,
        move_in_date    = EXCLUDED.move_in_date,
        scraped_at      = EXCLUDED.scraped_at,
        cleaned_at      = EXCLUDED.cleaned_at,
        updated_at      = NOW()
"""


# ============================================================
# SYNC: MongoDB -> PostgreSQL
# ============================================================

def get_existing_ids(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(f"SELECT source_id FROM {PG_TABLE} WHERE source_id IS NOT NULL")
    return {str(row[0]) for row in cur.fetchall()}


def _flush_batch(pg_conn, batch, stats, table=None):
    tbl = table or PG_TABLE
    sql = INSERT_SQL.format(table=tbl)
    try:
        cur = pg_conn.cursor()
        execute_values(cur, sql, batch)
        pg_conn.commit()
        stats["new_synced"] += len(batch)
    except Exception as e:
        pg_conn.rollback()
        log.error(f"Batch insert error: {e}")
        # Row-by-row fallback
        for row in batch:
            try:
                cur = pg_conn.cursor()
                execute_values(cur, sql, [row])
                pg_conn.commit()
                stats["new_synced"] += 1
            except Exception:
                pg_conn.rollback()
                stats["errors"] += 1


def sync(mongo_col, pg_conn):
    stats = {"total_mongo": 0, "already_in_pg": 0, "new_synced": 0, "errors": 0}
    stats["total_mongo"] = mongo_col.count_documents({})

    existing_ids = get_existing_ids(pg_conn)
    stats["already_in_pg"] = len(existing_ids)

    log.info(f"  MongoDB (clean): {stats['total_mongo']} | PostgreSQL: {stats['already_in_pg']}")

    if stats["total_mongo"] == 0:
        log.info("  No documents in MongoDB — skipping")
        return stats

    batch = []
    for doc in mongo_col.find({}, batch_size=BATCH_SIZE):
        doc_id = doc.get("source_id")
        if not doc_id:
            continue
        try:
            batch.append(build_row(doc))
        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                log.warning(f"  Row build error (source_id={doc_id}): {e}")

        if len(batch) >= BATCH_SIZE:
            _flush_batch(pg_conn, batch, stats)
            batch = []
            log.info(f"  Synced {stats['new_synced']} so far...")

    if batch:
        _flush_batch(pg_conn, batch, stats)

    return stats


# ============================================================
# ARCHIVE: Check if listings still live
# ============================================================

def check_url_alive(url: str) -> bool:
    try:
        resp = requests.head(
            url, headers=CHECK_HEADERS,
            timeout=ARCHIVE_TIMEOUT, allow_redirects=True
        )
        if resp.status_code in (404, 410):
            return False
        final = resp.url.rstrip("/").split("?")[0]
        if final in DEAD_REDIRECTS:
            return False
        if resp.status_code == 403:
            return True
        if resp.status_code < 400:
            return True
        if resp.status_code >= 500:
            return True   # server error ≠ listing gone
        return False
    except requests.RequestException:
        return True       # network error — keep listing


def ensure_archive_table(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
        (PG_ARCHIVE,)
    )
    if cur.fetchone()[0]:
        return
    cur.execute(f"""
        CREATE TABLE {PG_ARCHIVE} (
            LIKE {PG_TABLE} INCLUDING DEFAULTS
        )
    """)
    cur.execute(f"""
        ALTER TABLE {PG_ARCHIVE}
            ADD COLUMN IF NOT EXISTS archived_at     TIMESTAMPTZ DEFAULT NOW(),
            ADD COLUMN IF NOT EXISTS archive_reason  VARCHAR(50) DEFAULT 'listing_removed'
    """)
    # Drop unique constraint from archive so we can insert duplicates
    cur.execute(f"""
        SELECT conname FROM pg_constraint
        WHERE conrelid = '{PG_ARCHIVE}'::regclass
        AND contype = 'u'
    """)
    for row in cur.fetchall():
        cur.execute(f"ALTER TABLE {PG_ARCHIVE} DROP CONSTRAINT IF EXISTS {row[0]}")
    pg_conn.commit()
    log.info(f"  Archive table {PG_ARCHIVE} created")


def archive_listing(pg_conn, source_id, reason="listing_removed"):
    cur = pg_conn.cursor()
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name NOT IN ('archived_at', 'archive_reason')
            ORDER BY ordinal_position
        """, (PG_TABLE,))
        columns  = [r[0] for r in cur.fetchall()]
        cols_str = ", ".join(columns)
        cur.execute(f"""
            INSERT INTO {PG_ARCHIVE} ({cols_str}, archived_at, archive_reason)
            SELECT {cols_str}, NOW(), %s FROM {PG_TABLE} WHERE source_id = %s
        """, (reason, source_id))
        cur.execute(f"DELETE FROM {PG_TABLE} WHERE source_id = %s", (source_id,))
        pg_conn.commit()
        return True
    except Exception as e:
        pg_conn.rollback()
        log.error(f"Archive error for {source_id}: {e}")
        return False


def archive_check(pg_conn):
    stats = {"checked": 0, "alive": 0, "archived": 0, "errors": 0}
    cur   = pg_conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f"SELECT source_id, url FROM {PG_TABLE} WHERE url IS NOT NULL")
    rows  = cur.fetchall()
    log.info(f"  {len(rows)} listings to check")

    for i, row in enumerate(rows):
        url = row.get("url")
        if not url:
            continue

        alive = check_url_alive(url)
        stats["checked"] += 1

        if alive:
            stats["alive"] += 1
        else:
            if archive_listing(pg_conn, row["source_id"]):
                stats["archived"] += 1
                log.info(f"  Archived: {row['source_id']}")
            else:
                stats["errors"] += 1

        if (i + 1) % 100 == 0:
            log.info(f"  Progress: {i+1}/{len(rows)} | "
                     f"alive={stats['alive']} archived={stats['archived']}")

        time.sleep(ARCHIVE_DELAY)

    return stats


# ============================================================
# MAIN CYCLE
# ============================================================

def run_cycle(mongo_col, pg_conn, do_sync=True, do_archive=True):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"\n{'='*60}")
    log.info(f"[CA CENTRIS] CYCLE START: {now}")
    log.info(f"{'='*60}")

    ensure_archive_table(pg_conn)

    if do_sync:
        log.info("\n--- PHASE 1: SYNC MongoDB (clean) -> PostgreSQL ---")
        stats = sync(mongo_col, pg_conn)
        log.info(f"  +{stats['new_synced']} new | {stats['errors']} errors")

    if do_archive:
        log.info("\n--- PHASE 2: ARCHIVE CHECK (dead listings) ---")
        stats = archive_check(pg_conn)
        log.info(f"  {stats['checked']} checked | {stats['alive']} alive | "
                 f"{stats['archived']} archived | {stats['errors']} errors")

    cur = pg_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
    active = cur.fetchone()[0]
    try:
        cur.execute(f"SELECT COUNT(*) FROM {PG_ARCHIVE}")
        archived = cur.fetchone()[0]
    except Exception:
        pg_conn.rollback()
        archived = 0

    log.info(f"\n  {PG_TABLE}: {active} active | {PG_ARCHIVE}: {archived} archived")
    log.info("  CYCLE COMPLETE\n")


def main():
    parser = argparse.ArgumentParser(description="Centris Sync + Archive")
    parser.add_argument("--sync-only",    action="store_true")
    parser.add_argument("--archive-only", action="store_true")
    args = parser.parse_args()

    do_sync    = not args.archive_only
    do_archive = not args.sync_only

    log.info("Connecting to MongoDB...")
    mongo = MongoClient(MONGO_URI)
    mongo.admin.command("ping")
    log.info("  MongoDB connected")

    log.info("Connecting to PostgreSQL...")
    pg = psycopg2.connect(PG_DSN)
    log.info("  PostgreSQL connected")

    try:
        col = mongo[MONGO_DB][MONGO_COL]
        run_cycle(col, pg, do_sync, do_archive)
    except KeyboardInterrupt:
        log.info("\n  Stopped by user")
    finally:
        pg.close()
        mongo.close()
        log.info("  Connections closed")


if __name__ == "__main__":
    main()
