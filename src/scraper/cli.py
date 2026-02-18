"""Typer CLI — orchestrates API discovery → feed generation."""

import asyncio
import sys

import typer
from loguru import logger

from scraper.browser import StealthBrowser
from scraper.config import settings
from scraper.discover import discover_and_extract
from scraper.extract import extract_listing
from scraper.feed import write_feed
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
