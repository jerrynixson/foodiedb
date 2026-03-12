"""
upload_indian_foods.py
----------------------
Reads indian_foods.csv and inserts all foods, variants, and nutrients
directly into the Supabase PostgreSQL database.

Usage:
    uv run python upload_indian_foods.py
    uv run python upload_indian_foods.py --csv path/to/file.csv --dry-run
"""

import argparse
import csv
import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv(".env")

ALLOWED_NUTRIENT_FIELDS = {"calories", "protein", "carbs", "fat", "fiber", "sugar", "sodium"}

DEFAULT_CSV = os.path.join(os.path.dirname(__file__), "indian_foods_regional_200.csv")


def get_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sys.exit("ERROR: DATABASE_URL environment variable is not set.")
    return psycopg2.connect(db_url, sslmode="require")


def parse_nutrients(row: dict) -> dict:
    """Extract and validate nutrient values from a CSV row."""
    nutrients = {}
    for field in ALLOWED_NUTRIENT_FIELDS:
        raw = row.get(field, "").strip()
        if raw:
            try:
                nutrients[field] = float(raw)
            except ValueError:
                print(f"    WARNING: could not parse {field}={raw!r}, skipping field.")
    return nutrients


def upload(csv_path: str, dry_run: bool = False) -> None:
    with open(csv_path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    print(f"Loaded {len(rows)} rows from {csv_path}")

    conn = get_connection()
    cur = conn.cursor()

    food_ids: dict[str, int] = {}   # food_name (lower) -> food_id
    stats = {"foods_added": 0, "foods_skipped": 0, "variants_added": 0, "variants_skipped": 0}

    try:
        for row in rows:
            food_name = row.get("food_name", "").strip()
            if not food_name:
                print("  WARNING: empty food_name, skipping row.")
                continue

            food_key = food_name.lower()

            # ── Resolve food_id ──────────────────────────────────────────────
            if food_key not in food_ids:
                cur.execute(
                    "SELECT food_id FROM Foods WHERE LOWER(name) = %s",
                    (food_key,)
                )
                existing = cur.fetchone()
                if existing:
                    food_ids[food_key] = existing[0]
                    stats["foods_skipped"] += 1
                    print(f"  [food exists]  {food_name!r}  (id={existing[0]})")
                else:
                    is_packaged = row.get("is_packaged", "false").strip().lower() in ("true", "1", "yes")
                    barcode = row.get("barcode", "").strip() or None
                    category = row.get("category", "").strip() or None

                    if not dry_run:
                        cur.execute(
                            "INSERT INTO Foods (name, category, is_packaged, barcode) "
                            "VALUES (%s, %s, %s, %s) RETURNING food_id",
                            (food_name, category, is_packaged, barcode),
                        )
                        food_id = cur.fetchone()[0]
                    else:
                        food_id = -1  # placeholder for dry-run

                    food_ids[food_key] = food_id
                    stats["foods_added"] += 1
                    prefix = "[DRY RUN] " if dry_run else ""
                    print(f"  {prefix}[food added]   {food_name!r}  (id={food_id})")

            food_id = food_ids[food_key]

            # ── Resolve variant ──────────────────────────────────────────────
            variant_label = row.get("variant_label", "").strip()
            if not variant_label:
                print(f"    WARNING: empty variant_label for {food_name!r}, skipping.")
                continue

            if not dry_run:
                cur.execute(
                    "SELECT variant_id FROM Food_Variants "
                    "WHERE food_id = %s AND LOWER(variant_label) = LOWER(%s)",
                    (food_id, variant_label),
                )
                existing_v = cur.fetchone()
            else:
                existing_v = None  # always insert in dry-run print mode

            if existing_v:
                stats["variants_skipped"] += 1
                print(f"    [variant exists] {variant_label!r}")
                continue

            serving_size = row.get("serving_size", "100g").strip() or "100g"
            notes = row.get("notes", "").strip() or None
            nutrients = parse_nutrients(row)

            if not dry_run:
                cur.execute(
                    "INSERT INTO Food_Variants (food_id, variant_label, serving_size, notes) "
                    "VALUES (%s, %s, %s, %s) RETURNING variant_id",
                    (food_id, variant_label, serving_size, notes),
                )
                variant_id = cur.fetchone()[0]

                if nutrients:
                    # Field names are validated against the allow-list; values use parameterised query
                    safe = {k: v for k, v in nutrients.items() if k in ALLOWED_NUTRIENT_FIELDS}
                    cols = ", ".join(safe.keys())
                    placeholders = ", ".join(["%s"] * len(safe))
                    cur.execute(
                        f"INSERT INTO Food_Nutrients (variant_id, {cols}, confidence_score) "
                        f"VALUES (%s, {placeholders}, 0.8)",
                        [variant_id] + list(safe.values()),
                    )
            else:
                variant_id = -1

            stats["variants_added"] += 1
            prefix = "[DRY RUN] " if dry_run else ""
            print(f"    {prefix}[variant added] {variant_label!r}  nutrients={list(nutrients.keys())}")

        if not dry_run:
            conn.commit()
            print("\nTransaction committed.")
        else:
            print("\nDry-run mode: no changes written to the database.")

    except Exception as exc:
        conn.rollback()
        print(f"\nERROR: {exc}")
        raise
    finally:
        cur.close()
        conn.close()

    print(
        f"\nSummary:\n"
        f"  Foods  added={stats['foods_added']}  skipped={stats['foods_skipped']}\n"
        f"  Variants added={stats['variants_added']}  skipped={stats['variants_skipped']}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed Indian foods into the OpenFood database.")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Path to the CSV file (default: indian_foods.csv)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be inserted without writing to the DB")
    args = parser.parse_args()

    upload(args.csv, dry_run=args.dry_run)
