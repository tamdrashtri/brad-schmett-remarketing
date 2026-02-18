"""Incremental state management — skip recently scraped listings."""

import json
from datetime import datetime, timedelta

from loguru import logger

from scraper.config import settings
from scraper.models import Listing, StateEntry


class StateManager:
    """Track per-listing scrape timestamps for incremental runs."""

    def __init__(self) -> None:
        self._entries: dict[str, StateEntry] = {}
        self._load()

    def _load(self) -> None:
        path = settings.abs_state_path
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            for url, entry in data.items():
                self._entries[url] = StateEntry(**entry)
            logger.info("Loaded state for {} listings", len(self._entries))
        except Exception as e:
            logger.warning("Failed to load state: {}", e)

    def save(self) -> None:
        path = settings.abs_state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {url: entry.model_dump(mode="json") for url, entry in self._entries.items()}
        path.write_text(json.dumps(data, indent=2, default=str))
        logger.debug("Saved state for {} listings", len(self._entries))

    def is_stale(self, url: str) -> bool:
        """True if URL needs re-scraping (not in state or older than stale_hours)."""
        entry = self._entries.get(url)
        if not entry:
            return True
        cutoff = datetime.utcnow() - timedelta(hours=settings.stale_hours)
        return entry.last_scraped < cutoff

    def filter_stale(self, urls: list[str]) -> list[str]:
        """Return only URLs that need scraping."""
        stale = [u for u in urls if self.is_stale(u)]
        logger.info("{}/{} URLs need scraping", len(stale), len(urls))
        return stale

    def update(self, listing: Listing) -> None:
        """Record a successful scrape."""
        self._entries[listing.url] = StateEntry(
            url=listing.url,
            mls_id=listing.mls_id,
            last_scraped=listing.scraped_at,
            last_price=listing.price,
            status=listing.status,
        )

    def get_all_listings_data(self) -> list[dict]:
        """Return state data for all tracked listings."""
        return [e.model_dump(mode="json") for e in self._entries.values()]
