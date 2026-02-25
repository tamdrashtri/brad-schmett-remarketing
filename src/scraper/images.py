"""Download auth-protected listing images and self-host on GitHub Pages.

img.chime.me enforces hotlink protection — bare HTTP requests get 403.
We use page.goto() which sends proper browser headers and cookies.

Rate limiting strategy (based on observed ~400 req/5min limit):
- 2 workers with 1.5s + random jitter delay (~40-50 req/min total)
- Global pause on 429: ALL workers stop (CDN rate limits are IP-based)
- Exponential backoff with jitter on retries
- Respects Retry-After header when present
"""

import asyncio
import random
from pathlib import Path

from loguru import logger
from playwright.async_api import BrowserContext

from scraper.extract import decode_chime_image_url
from scraper.models import Listing

IMAGES_DIR = Path(__file__).resolve().parent.parent.parent / "docs" / "images"
PAGES_BASE = "https://tamdrashtri.github.io/brad-schmett-remarketing/images"

# Rate limiting tuning
CONCURRENCY = 2
BASE_DELAY = 1.5  # seconds between requests per worker
JITTER_RANGE = 1.0  # random 0-1s added to base delay
MAX_RETRIES = 4
BACKOFF_RANGES = [  # (min, max) seconds for each retry attempt
    (5, 15),
    (15, 45),
    (45, 120),
    (120, 300),
]


def needs_download(listing: Listing) -> bool:
    """Check if this listing's image is from an auth-protected CDN."""
    url = listing.image_url
    if not url:
        return False
    if "img.chime.me" in url:
        decoded = decode_chime_image_url(url)
        return "sparkplatform.com" not in decoded
    if "cotality.com" in url or "corelogic.com" in url or "crmls.org" in url:
        return True
    return False


def self_hosted_url(lofty_id: str) -> str:
    return f"{PAGES_BASE}/{lofty_id}.jpg"


# Global rate limit event — when set, ALL workers pause
_rate_limit_event: asyncio.Event | None = None


async def download_images(
    context: BrowserContext, listings: list[Listing], concurrency: int = CONCURRENCY
) -> int:
    """Download auth-protected images using browser page navigation."""
    global _rate_limit_event
    _rate_limit_event = asyncio.Event()
    _rate_limit_event.set()  # Start unblocked

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

    # Create worker pages
    pages = []
    for _ in range(concurrency):
        pages.append(await context.new_page())

    queue: asyncio.Queue[Listing] = asyncio.Queue()
    for listing in pending:
        queue.put_nowait(listing)

    count = 0
    errors = 0

    async def _worker(page, worker_id: int) -> None:
        nonlocal count, errors
        while not queue.empty():
            # Wait if global rate limit pause is active
            await _rate_limit_event.wait()

            try:
                listing = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            success = False
            for attempt in range(MAX_RETRIES + 1):
                # Re-check rate limit before each attempt
                await _rate_limit_event.wait()

                try:
                    response = await page.goto(
                        listing.image_url,
                        wait_until="commit",
                        timeout=15000,
                    )
                    if response and response.ok:
                        body = await response.body()
                        if len(body) > 1000:
                            (IMAGES_DIR / f"{listing.lofty_id}.jpg").write_bytes(body)
                            count += 1
                            success = True
                        else:
                            logger.warning(
                                "Image too small for {} ({} bytes)",
                                listing.lofty_id,
                                len(body),
                            )
                        break
                    elif response and response.status == 429:
                        # Extract Retry-After if present
                        retry_after = response.headers.get("retry-after")
                        if attempt < MAX_RETRIES:
                            if retry_after:
                                wait = float(retry_after)
                                logger.info(
                                    "W{}: Rate limited, server says wait {}s",
                                    worker_id,
                                    wait,
                                )
                            else:
                                lo, hi = BACKOFF_RANGES[attempt]
                                wait = random.uniform(lo, hi)
                                logger.info(
                                    "W{}: Rate limited, backing off {:.0f}s (attempt {}/{})",
                                    worker_id,
                                    wait,
                                    attempt + 1,
                                    MAX_RETRIES,
                                )
                            # Global pause — block ALL workers
                            _rate_limit_event.clear()
                            await asyncio.sleep(wait)
                            _rate_limit_event.set()
                            continue
                        else:
                            logger.warning(
                                "W{}: Rate limited after {} retries for {}",
                                worker_id,
                                MAX_RETRIES,
                                listing.lofty_id,
                            )
                            break
                    else:
                        status = response.status if response else "no response"
                        logger.warning(
                            "W{}: HTTP {} for {}", worker_id, status, listing.lofty_id
                        )
                        break
                except Exception as e:
                    logger.warning(
                        "W{}: Failed for {}: {}", worker_id, listing.lofty_id, e
                    )
                    break

            if not success:
                errors += 1

            # Throttle: base delay + random jitter
            delay = BASE_DELAY + random.uniform(0, JITTER_RANGE)
            await asyncio.sleep(delay)
            queue.task_done()

    await asyncio.gather(*[_worker(p, i) for i, p in enumerate(pages)])

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
