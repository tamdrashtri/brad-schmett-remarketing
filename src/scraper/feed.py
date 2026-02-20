"""Generate Google Ads DynamicRealEstateAsset CSV feed."""

import csv

from loguru import logger

from scraper.config import settings
from scraper.models import FeedRow, Listing

# Google Ads requires Title Case headers with spaces
FEED_COLUMNS = [
    "Listing ID",
    "Listing name",
    "Final URL",
    "Image URL",
    "Price",
    "City name",
    "Property type",
    "Listing type",
    "Address",
    "Description",
    "Contextual keywords",
]

# Map from FeedRow field names to Google Ads header names
_FIELD_TO_HEADER = {
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


def write_feed(listings: list[Listing]) -> int:
    """Write active listings to CSV feed. Returns count of rows written."""
    active = [lst for lst in listings if lst.is_active and lst.mls_id]
    if not active:
        logger.warning("No active listings to write")
        return 0

    path = settings.abs_feed_path
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FEED_COLUMNS)
        writer.writeheader()
        for listing in active:
            row = FeedRow.from_listing(listing)
            # Remap snake_case keys to Google Ads Title Case headers
            mapped = {_FIELD_TO_HEADER[k]: v for k, v in row.model_dump().items()}
            writer.writerow(mapped)

    logger.info("Wrote {} rows to {}", len(active), path)
    return len(active)
