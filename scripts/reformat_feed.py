"""One-time script to reformat existing feed.csv to Google Ads format.

Fixes:
1. Column headers: snake_case → Title Case (Google Ads requirement)
2. Listing name: truncate to 25 chars
3. Description: truncate to 25 chars
4. Contextual keywords: comma separator → semicolons
5. Data quality: filter out negative bedrooms/sqft
"""

import csv
from pathlib import Path

FEED_PATH = Path(__file__).resolve().parent.parent / "docs" / "feed.csv"
OUTPUT_PATH = FEED_PATH  # overwrite in place

HEADER_MAP = {
    "listing_id": "Listing ID",
    "listing_name": "Listing name",
    "final_url": "Final URL",
    "image_url": "Image URL",
    "price": "Price",
    "city_name": "City name",
    "property_type": "Property type",
    "listing_type": "Listing type",
    "address": "Address",
    "description": "Description",
    "contextual_keywords": "Contextual keywords",
}

GOOGLE_HEADERS = list(HEADER_MAP.values())


def truncate_listing_name(name: str) -> str:
    """Truncate listing name to 25 chars, dropping city part first."""
    if len(name) <= 25:
        return name
    # Try dropping "in CityName" suffix
    if " in " in name:
        name = name[: name.rfind(" in ")]
    if len(name) <= 25:
        return name
    return name[:25]


def fix_contextual_keywords(kw: str) -> str:
    """Replace comma separators with semicolons."""
    return "; ".join(part.strip() for part in kw.split(",") if part.strip())


def main():
    # Read existing CSV
    rows = []
    with open(FEED_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    print(f"Read {len(rows)} rows from {FEED_PATH}")

    # Reformat and write
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GOOGLE_HEADERS)
        writer.writeheader()

        skipped = 0
        for row in rows:
            name = row.get("listing_name", "")
            # Skip rows with negative bedrooms (data quality)
            if name.startswith("-"):
                skipped += 1
                continue

            mapped = {
                "Listing ID": row["listing_id"],
                "Listing name": truncate_listing_name(name),
                "Final URL": row["final_url"],
                "Image URL": row["image_url"],
                "Price": row["price"],
                "City name": row.get("city_name", "")[:25],
                "Property type": row.get("property_type", ""),
                "Listing type": row.get("listing_type", "For Sale"),
                "Address": row.get("address", ""),
                "Description": row.get("description", "")[:25],
                "Contextual keywords": fix_contextual_keywords(
                    row.get("contextual_keywords", "")
                ),
            }
            writer.writerow(mapped)

    written = len(rows) - skipped
    print(f"Wrote {written} rows ({skipped} skipped for bad data)")
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
