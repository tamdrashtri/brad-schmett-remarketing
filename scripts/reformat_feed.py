"""Reformat feed.csv to Google Ads format.

Handles both snake_case (old) and Title Case (current) headers.

Fixes:
1. Column headers: snake_case → Title Case (Google Ads requirement)
2. Listing name: truncate to 25 chars
3. Description: truncate to 25 chars
4. Contextual keywords: comma separator → semicolons
5. Data quality: filter out negative bedrooms/sqft
6. Address: append city + state for Google geocoding
7. Image URL: decode img.chime.me proxy URLs to stable sparkplatform CDN URLs
"""

import base64
import csv
import zlib
from pathlib import Path


def decode_chime_image_url(chime_url: str) -> str:
    """Decode an img.chime.me imageemb proxy URL to its stable CDN source URL.

    img.chime.me wraps MLS photos from cdn.photos.sparkplatform.com (and
    possibly other sources) in a deflate-compressed, base64url-encoded token
    embedded in the URL path.  The proxy enforces hotlink protection (returns
    403 for external fetchers), making these URLs unusable for Google Ads feeds.
    The decoded sparkplatform URLs are permanent CDN assets (keyed by upload
    timestamp) that return 200 with no authentication required.
    """
    if not chime_url or "img.chime.me" not in chime_url:
        return chime_url
    try:
        parts = chime_url.rstrip(".jpg").split("original_", 1)
        if len(parts) != 2:
            return chime_url
        token = parts[1]
        token_std = token.replace("-", "+").replace("_", "/")
        token_std += "=" * (4 - len(token_std) % 4)
        compressed = base64.b64decode(token_std)
        return zlib.decompress(compressed, -15).decode("utf-8")
    except Exception:
        return chime_url

FEED_PATH = Path(__file__).resolve().parent.parent / "docs" / "feed.csv"
OUTPUT_PATH = FEED_PATH  # overwrite in place

# Support both old snake_case and new Title Case headers
HEADER_MAP_SNAKE = {
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

GOOGLE_HEADERS = list(HEADER_MAP_SNAKE.values())

# Identity map for already-Title-Case headers
HEADER_MAP_TITLE = {v: v for v in GOOGLE_HEADERS}


def _get(row: dict, snake_key: str, title_key: str, default: str = "") -> str:
    """Get value from row supporting both header formats."""
    return row.get(snake_key, row.get(title_key, default))


def truncate_listing_name(name: str) -> str:
    """Truncate listing name to 25 chars, dropping city part first."""
    if len(name) <= 25:
        return name
    if " in " in name:
        name = name[: name.rfind(" in ")]
    if len(name) <= 25:
        return name
    return name[:25]


def fix_address(address: str, city: str) -> str:
    """Ensure address includes city and state for Google geocoding."""
    if not address:
        return ""
    # Already has city appended (contains comma with city name)
    if city and city.lower() in address.lower():
        return address
    # Append city, CA
    if city:
        return f"{address}, {city}, CA"
    return address


def format_price(price_str: str) -> str:
    """Convert '935000.00 USD' to '$935,000' for US luxury market display."""
    if not price_str:
        return ""
    try:
        # Extract numeric part from "935000.00 USD" or "$935,000" (idempotent)
        cleaned = price_str.replace("$", "").replace(",", "").split()[0]
        amount = float(cleaned)
        if amount == int(amount):
            return f"${int(amount):,}"
        return f"${amount:,.2f}"
    except (ValueError, IndexError):
        return price_str


def fix_description(desc: str) -> str:
    """Truncate to 25 chars and fix excessive capitalization."""
    desc = desc[:25]
    # Google disapproves excessive caps — title case if >50% uppercase
    if desc and sum(1 for c in desc if c.isupper()) > len(desc) * 0.5:
        desc = desc.title()
    return desc


def fix_contextual_keywords(kw: str) -> str:
    """Replace comma separators with semicolons."""
    return "; ".join(part.strip() for part in kw.split(",") if part.strip())


def main():
    rows = []
    with open(FEED_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    print(f"Read {len(rows)} rows from {FEED_PATH}")

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GOOGLE_HEADERS)
        writer.writeheader()

        skipped = 0
        for row in rows:
            name = _get(row, "listing_name", "Listing name")
            if name.startswith("-"):
                skipped += 1
                continue

            city = _get(row, "city_name", "City name")
            address = _get(row, "address", "Address")

            mapped = {
                "Listing ID": _get(row, "listing_id", "Listing ID"),
                "Listing name": truncate_listing_name(name),
                "Final URL": _get(row, "final_url", "Final URL"),
                "Image URL": decode_chime_image_url(_get(row, "image_url", "Image URL")),
                "Price": format_price(_get(row, "price", "Price")),
                "City name": city[:25],
                "Property type": _get(row, "property_type", "Property type"),
                "Listing type": _get(row, "listing_type", "Listing type") or "For Sale",
                "Address": fix_address(address, city),
                "Description": fix_description(_get(row, "description", "Description")),
                "Contextual keywords": fix_contextual_keywords(
                    _get(row, "contextual_keywords", "Contextual keywords")
                ),
            }
            writer.writerow(mapped)

    written = len(rows) - skipped
    print(f"Wrote {written} rows ({skipped} skipped for bad data)")
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
