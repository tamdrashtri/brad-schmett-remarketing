"""Generate Google Ads DynamicRealEstateAsset CSV feed."""

import csv

from loguru import logger

from scraper.config import settings
from scraper.models import FeedRow, Listing

FEED_COLUMNS = [
    "listing_id",
    "listing_name",
    "final_url",
    "image_url",
    "price",
    "city_name",
    "property_type",
    "listing_type",
    "address",
    "description",
    "contextual_keywords",
]


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
            writer.writerow(row.model_dump())

    logger.info("Wrote {} rows to {}", len(active), path)
    return len(active)
