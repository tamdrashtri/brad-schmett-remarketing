"""Extract listing data from a detail page: JSON-LD first, DOM fallback."""

import base64
import json
import re
import zlib

from loguru import logger
from playwright.async_api import Page

from scraper.browser import human_delay
from scraper.models import Listing


def decode_chime_image_url(chime_url: str) -> str:
    """Decode an img.chime.me imageemb URL to its underlying stable CDN URL.

    Lofty/Chime wraps MLS photos in a proxy URL of the form:
        https://img.chime.me/imageemb/mls-listing/{id1}/{id2}/{hash}/{ts}/original_{token}.jpg

    The {token} segment is a URL-safe base64-encoded raw-deflate (no zlib header)
    compressed string containing the actual photo URL, typically on
    cdn.photos.sparkplatform.com.  These sparkplatform URLs are permanent CDN
    assets (filename is the upload timestamp, not an expiry token).

    The img.chime.me proxy enforces hotlink protection and returns 403 for
    external fetchers regardless of Referer, making it unusable for Google Ads
    image feeds.  The decoded sparkplatform URLs return 200 with no auth.

    Returns the decoded URL if decoding succeeds, or the original chime_url
    unchanged so callers always get a non-empty string.
    """
    if not chime_url or "img.chime.me" not in chime_url:
        return chime_url
    try:
        # Split on 'original_' to isolate the token, strip trailing .jpg
        parts = chime_url.rstrip(".jpg").split("original_", 1)
        if len(parts) != 2:
            return chime_url
        token = parts[1]
        # URL-safe base64 -> standard base64
        token_std = token.replace("-", "+").replace("_", "/")
        # Pad to 4-byte boundary
        token_std += "=" * (4 - len(token_std) % 4)
        compressed = base64.b64decode(token_std)
        # Raw deflate (wbits=-15 skips the zlib header)
        decoded_url = zlib.decompress(compressed, -15).decode("utf-8")
        return decoded_url
    except Exception as e:
        logger.debug("Failed to decode chime image URL {}: {}", chime_url, e)
        return chime_url


async def extract_listing(page: Page, url: str) -> Listing | None:
    """Navigate to a listing page and extract all fields."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        # Wait for the price element in DOM (not visibility — overlays may block)
        await page.wait_for_selector(".price-number", state="attached", timeout=30_000)
        # Dismiss any registration modal that may appear
        try:
            close_btn = await page.query_selector(
                '[class*="modal"] [class*="close"], .modal-close, [aria-label="Close"]'
            )
            if close_btn:
                await close_btn.click()
        except Exception:
            pass
        await human_delay(0.5)
    except Exception as e:
        logger.warning("Failed to load {}: {}", url, e)
        return None

    try:
        listing = Listing(url=url)

        # --- Phase 1: JSON-LD extraction ---
        jsonld = await _extract_jsonld(page)
        if jsonld:
            listing.price = _parse_price(jsonld.get("offers", {}).get("price", ""))
            listing.image_url = decode_chime_image_url(jsonld.get("image", ""))
            # JSON-LD name often has duplicated city — prefer DOM address below
            name = jsonld.get("name", "")

        # --- Phase 2: DOM extraction (fills gaps + overrides) ---
        # Wait for address to render (h1 with address-container class)
        try:
            await page.wait_for_selector("h1.address-container .street", state="attached", timeout=10_000)
        except Exception:
            pass
        # Use evaluate() for all DOM reads in a single call to avoid race conditions
        dom = await page.evaluate("""
            () => {
                const txt = (sel) => {
                    const el = document.querySelector(sel);
                    return el ? (el.textContent || '').trim() : '';
                };
                const attr = (sel, a) => {
                    const el = document.querySelector(sel);
                    return el ? el.getAttribute(a) || '' : '';
                };
                return {
                    street: txt('.address-container .street'),
                    city: txt('.address-container .city'),
                    status: txt('.house-status .status-text'),
                    beds: txt('.bed-count .number'),
                    baths: txt('.bath-count .number'),
                    sqft: txt('.sqft-count .number'),
                    price: txt('.price-number'),
                    image: attr('.slide-left .img-content img', 'src'),
                    description: txt('.read-more-content .info-data'),
                };
            }
        """)

        listing.address = dom.get("street") or name or ""
        listing.city = dom.get("city", "")
        listing.status = dom.get("status", "")
        listing.bedrooms = _int(dom.get("beds"))
        listing.bathrooms = _int(dom.get("baths"))
        listing.sqft = _int(dom.get("sqft"))

        if not listing.price:
            listing.price = _parse_price(dom.get("price"))

        if not listing.image_url:
            listing.image_url = decode_chime_image_url(dom.get("image", ""))

        if dom.get("description"):
            listing.description = dom["description"]

        # Key Details section (MLS ID, Property Type, Subdivision)
        key_details = await _extract_key_details(page)
        listing.mls_id = key_details.get("MLS Listing ID", "")
        listing.property_type = key_details.get("Property Type", "")
        listing.subdivision = key_details.get("Subdivision", "")

        if not listing.mls_id:
            logger.warning("No MLS ID found for {}", url)
            return None

        logger.debug(
            "Extracted: {} | ${:,.0f} | {}", listing.mls_id, listing.price, listing.status
        )
        return listing

    except Exception as e:
        logger.error("Extraction failed for {}: {}", url, e)
        return None


async def _extract_jsonld(page: Page) -> dict | None:
    """Parse the first Product-type JSON-LD block (handles array wrapping)."""
    scripts = await page.query_selector_all('script[type="application/ld+json"]')
    for script in scripts:
        try:
            text = await script.inner_text()
            data = json.loads(text)
            # Handle both [{...}] and {...} formats
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "Product":
                        return item
            elif isinstance(data, dict) and data.get("@type") == "Product":
                return data
        except (json.JSONDecodeError, Exception):
            continue
    return None


async def _extract_key_details(page: Page) -> dict[str, str]:
    """Extract key-value pairs from the Key Details section."""
    try:
        pairs = await page.evaluate("""
            () => {
                const out = {};
                document.querySelectorAll('.info-title').forEach(el => {
                    const key = el.textContent.trim();
                    const val = el.nextElementSibling;
                    if (val && val.classList.contains('info-data')) {
                        out[key] = val.textContent.trim();
                    }
                });
                return out;
            }
        """)
        return pairs or {}
    except Exception as e:
        logger.debug("Key details extraction failed: {}", e)
        return {}


def _parse_price(raw: str | None) -> float:
    """Parse price string like '$ 575,000' or '575000' to float."""
    if not raw:
        return 0.0
    digits = re.sub(r"[^\d.]", "", raw)
    try:
        return float(digits)
    except ValueError:
        return 0.0


def _int(raw: str | None) -> int:
    """Parse a number string like '1,833' to int."""
    if not raw:
        return 0
    digits = re.sub(r"[^\d]", "", raw)
    try:
        return int(digits)
    except ValueError:
        return 0
