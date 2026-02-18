"""Discover listings via Lofty's internal realTimeListings API.

Strategy: Navigate to /featured-listing (no CAPTCHA) to establish a browser
session, then paginate through the search API to get all active listings
with full data (no need to visit individual detail pages).
"""

import json

from loguru import logger
from playwright.async_api import Page

from scraper.browser import StealthBrowser, human_delay
from scraper.config import settings
from scraper.models import Listing

# All cities Brad covers in the Coachella Valley
CITIES = [
    "Palm Springs, CA",
    "Palm Desert, CA",
    "La Quinta, CA",
    "Indian Wells, CA",
    "Rancho Mirage, CA",
    "Indio, CA",
    "Bermuda Dunes, CA",
    "Desert Hot Springs, CA",
    "Coachella, CA",
    "Cathedral City, CA",
    "Thermal, CA",
    "Thousand Palms, CA",
]

SEARCH_CONDITION = {
    "location": {"city": CITIES},
    "listingStatus": ["Active"],
    "purchaseType": ["For Sale"],
    "propertyType": [
        "Single Family Home",
        "Multi-Family",
        "Condo",
        "Townhouse",
        "Manufactured Home",
        "Land",
        "Commercial",
        "Farm",
    ],
}

PAGE_SIZE = 100


async def discover_and_extract(browser: StealthBrowser) -> list[Listing]:
    """Navigate to a CAPTCHA-free page, then paginate the search API.

    Returns fully populated Listing objects — no per-page scraping needed.
    """
    page = await browser.new_page()

    # Step 1: Establish session by visiting a page without CAPTCHA
    logger.info("Establishing browser session via /featured-listing")
    try:
        await page.goto(
            f"{settings.base_url}/featured-listing",
            wait_until="domcontentloaded",
            timeout=45_000,
        )
        await page.wait_for_selector("a[href*='/listing-detail/']", state="attached", timeout=30_000)
        await human_delay(1.0)
    except Exception as e:
        logger.error("Failed to establish session: {}", e)
        await page.context.close()
        return []

    # Step 2: Paginate the search API
    logger.info("Querying Lofty search API (pageSize={})", PAGE_SIZE)
    all_listings: list[Listing] = []
    current_page = 1

    while True:
        batch = await _fetch_page(page, current_page)
        if not batch:
            break

        all_listings.extend(batch)
        logger.info(
            "Page {}: got {} listings (total: {})", current_page, len(batch), len(all_listings)
        )

        if len(batch) < PAGE_SIZE:
            break
        if settings.max_listings and len(all_listings) >= settings.max_listings:
            all_listings = all_listings[: settings.max_listings]
            break

        current_page += 1
        await human_delay(0.5)

    await page.context.close()
    logger.info("Discovered {} total listings via API", len(all_listings))
    return all_listings


async def _fetch_page(page: Page, page_num: int) -> list[Listing]:
    """Fetch one page of listings from the Lofty search API."""
    try:
        result = await page.evaluate(
            """
            async ([condition, pageSize, pageNum]) => {
                const params = new URLSearchParams({
                    condition: JSON.stringify(condition),
                    cache: 'false',
                    timezone: 'GMT+0000',
                    pageSize: String(pageSize),
                    page: String(pageNum),
                    listingSort: 'MLS_LIST_DATE_L_DESC',
                });
                const resp = await fetch(
                    `/api-site/search/realTimeListings?${params}`,
                    {
                        headers: {
                            'accept': 'application/json',
                            'currentsiteid': '128008',
                            'site-search-listings': 'true',
                        },
                    }
                );
                if (!resp.ok) return { error: resp.status };
                const data = await resp.json();
                return {
                    listings: (data.listings || []).map(l => ({
                        id: l.id,
                        mlsId: l.mlsListingId || '',
                        price: l.price || 0,
                        bedrooms: l.bedrooms || 0,
                        bathrooms: l.bathrooms || 0,
                        sqft: l.sqft || 0,
                        propertyType: l.propertyType || '',
                        status: l.flag || l.openHouseDesc || 'Active',
                        street: l.streetAddress || '',
                        city: l.city || '',
                        state: l.state || 'CA',
                        zip: l.zipCode || '',
                        image: l.previewPicture || '',
                        description: (l.detailsDescribe || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim().substring(0, 300),
                        detailUrl: l.detailUrl || '',
                        subdivision: l.subDivisionName || l.neighborhoodName || '',
                    })),
                    total: data.counts,
                };
            }
        """,
            [SEARCH_CONDITION, PAGE_SIZE, page_num],
        )

        if "error" in result:
            logger.error("API returned status {}", result["error"])
            return []

        if page_num == 1:
            logger.info("Total listings available: {}", result.get("total", "?"))

        listings = []
        for item in result.get("listings", []):
            if not item.get("id"):
                continue
            detail_url = item["detailUrl"]
            if detail_url and not detail_url.startswith("http"):
                detail_url = f"{settings.base_url}{detail_url}"

            listings.append(
                Listing(
                    url=detail_url,
                    lofty_id=str(item["id"]),
                    mls_id=item["mlsId"],
                    price=item["price"],
                    bedrooms=item["bedrooms"],
                    bathrooms=item["bathrooms"],
                    sqft=item["sqft"],
                    property_type=item["propertyType"],
                    status=item["status"] or "Active",
                    address=item["street"],
                    city=f"{item['city']}, {item['state']} {item['zip']}".strip(),
                    image_url=item["image"],
                    description=item["description"],
                    subdivision=item["subdivision"],
                )
            )
        return listings

    except Exception as e:
        logger.error("Failed to fetch page {}: {}", page_num, e)
        return []
