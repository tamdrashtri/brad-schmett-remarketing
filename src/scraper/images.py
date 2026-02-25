"""Download auth-protected listing images and self-host on GitHub Pages.

img.chime.me enforces hotlink protection — context.request.get() sends bare
HTTP requests without browser-level Referer/Cookie headers, so it gets 403.
Instead, we use page.goto() which navigates the real browser to the image URL,
inheriting all session cookies and sending proper headers.
"""

import asyncio
from pathlib import Path

from loguru import logger
from playwright.async_api import BrowserContext

from scraper.extract import decode_chime_image_url
from scraper.models import Listing

IMAGES_DIR = Path(__file__).resolve().parent.parent.parent / "docs" / "images"
PAGES_BASE = "https://tamdrashtri.github.io/brad-schmett-remarketing/images"


def needs_download(listing: Listing) -> bool:
    """Check if this listing's image is from an auth-protected CDN.

    After optimize_image_url(), cotality/corelogic listings retain their
    original img.chime.me proxy URL (only sparkplatform gets wsrv.nl wrapped).
    """
    url = listing.image_url
    if not url:
        return False
    # Catch img.chime.me URLs that decode to non-sparkplatform sources
    if "img.chime.me" in url:
        decoded = decode_chime_image_url(url)
        return "sparkplatform.com" not in decoded
    # Catch direct cotality/corelogic URLs (shouldn't happen with current
    # optimize_image_url, but defensive)
    if "cotality.com" in url or "corelogic.com" in url or "crmls.org" in url:
        return True
    return False


def self_hosted_url(lofty_id: str) -> str:
    return f"{PAGES_BASE}/{lofty_id}.jpg"


async def download_images(
    context: BrowserContext, listings: list[Listing], concurrency: int = 5
) -> int:
    """Download auth-protected images using browser page navigation.

    Uses page.goto() instead of context.request.get() because img.chime.me
    rejects bare HTTP requests (403). Page navigation sends proper browser
    headers, cookies, and Referer automatically.
    """
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    pending = [
        l
        for l in listings
        if needs_download(l) and not (IMAGES_DIR / f"{l.lofty_id}.jpg").exists()
    ]
    if not pending:
        logger.info("No new cotality images to download")
        return 0

    logger.info("Downloading {} cotality images (concurrency={})", len(pending), concurrency)

    # Create a pool of pages for concurrent downloads
    pages = []
    for _ in range(concurrency):
        pages.append(await context.new_page())

    queue: asyncio.Queue[Listing] = asyncio.Queue()
    for listing in pending:
        queue.put_nowait(listing)

    count = 0
    errors = 0

    async def _worker(page) -> None:
        nonlocal count, errors
        while not queue.empty():
            try:
                listing = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                response = await page.goto(
                    listing.image_url,
                    wait_until="load",
                    timeout=15000,
                )
                if response and response.ok:
                    body = await response.body()
                    if len(body) > 1000:  # skip broken/empty responses
                        (IMAGES_DIR / f"{listing.lofty_id}.jpg").write_bytes(body)
                        count += 1
                    else:
                        logger.warning(
                            "Image too small for {} ({} bytes)",
                            listing.lofty_id,
                            len(body),
                        )
                        errors += 1
                else:
                    status = response.status if response else "no response"
                    logger.warning(
                        "Image download HTTP {} for {}", status, listing.lofty_id
                    )
                    errors += 1
            except Exception as e:
                logger.warning("Image download failed for {}: {}", listing.lofty_id, e)
                errors += 1
            finally:
                queue.task_done()

    # Run workers concurrently — each page handles items from the queue
    await asyncio.gather(*[_worker(p) for p in pages])

    # Clean up pages
    for p in pages:
        await p.close()

    logger.info(
        "Downloaded {}/{} cotality images ({} errors)", count, len(pending), errors
    )
    return count


def cleanup_stale_images(active_ids: set[str]) -> int:
    """Remove images for listings that are no longer active."""
    removed = 0
    if IMAGES_DIR.exists():
        for f in IMAGES_DIR.glob("*.jpg"):
            if f.stem not in active_ids:
                f.unlink()
                removed += 1
    if removed:
        logger.info("Cleaned up {} stale images", removed)
    return removed
