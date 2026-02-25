"""Typer CLI — orchestrates API discovery → feed generation."""

import asyncio
import sys

import typer
from loguru import logger

from scraper.browser import StealthBrowser, human_delay
from scraper.config import settings
from scraper.discover import discover_and_extract
from scraper.extract import extract_listing
from scraper.feed import write_feed
from scraper.images import (
    cleanup_stale_images,
    download_images,
    needs_download,
    self_hosted_url,
)
from scraper.models import Listing
from scraper.state import StateManager

app = typer.Typer(help="Brad Schmett listing feed scraper")

# Configure loguru
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>",
)


@app.command()
def run(
    max_listings: int = typer.Option(0, help="Max listings to scrape (0=all)"),
    headless: bool = typer.Option(True, help="Run browser headless"),
) -> None:
    """Run the full pipeline: API discovery → feed generation."""
    if max_listings:
        settings.max_listings = max_listings
    settings.headless = headless
    asyncio.run(_run_pipeline())


async def _run_pipeline() -> None:
    state = StateManager()

    async with StealthBrowser() as browser:
        # Discover + extract all listings via Lofty API
        listings = await discover_and_extract(browser)

        if not listings:
            logger.error("No listings found")
            raise typer.Exit(1)

        # Download cotality/corelogic auth-protected images
        cotality_listings = [l for l in listings if needs_download(l)]
        if cotality_listings:
            logger.info(
                "{}/{} listings need cotality image download",
                len(cotality_listings),
                len(listings),
            )
            # Create a new context and establish session cookies
            ctx = await browser.new_context()
            page = await ctx.new_page()
            # Visit a listing page to prime img.chime.me cookies in the context
            # (featured-listing loads thumbnail images from chime CDN)
            await page.goto(
                f"{settings.base_url}/featured-listing",
                wait_until="networkidle",
                timeout=60_000,
            )
            await human_delay(1.0)
            # Close the session page — download_images creates its own pages
            await page.close()

            downloaded = await download_images(ctx, cotality_listings)
            logger.info("Downloaded {} new images", downloaded)
            await ctx.close()

            # Replace image URLs with self-hosted GitHub Pages URLs for downloaded images
            from scraper.images import IMAGES_DIR

            for listing in listings:
                if needs_download(listing) and (
                    IMAGES_DIR / f"{listing.lofty_id}.jpg"
                ).exists():
                    listing.image_url = self_hosted_url(listing.lofty_id)

        # Clean up images for delisted properties
        active_ids = {l.lofty_id for l in listings}
        cleanup_stale_images(active_ids)

        # Update state
        for listing in listings:
            state.update(listing)

        # Generate feed
        count = write_feed(listings)
        logger.info("Feed generated with {} active listings", count)

        # Save state
        state.save()


@app.command()
def test_extract(
    url: str = typer.Argument(help="Single listing URL to test extraction"),
    headless: bool = typer.Option(True),
) -> None:
    """Test DOM extraction on a single listing URL."""
    settings.headless = headless

    async def _test():
        async with StealthBrowser() as browser:
            page = await browser.new_page()
            listing = await extract_listing(page, url)
            if listing:
                from rich import print as rprint

                rprint(listing.model_dump())
            else:
                logger.error("Extraction returned None")
            await page.context.close()

    asyncio.run(_test())


if __name__ == "__main__":
    app()
