"""Configuration via environment variables."""

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "SCRAPER_"}

    base_url: str = "https://bradschmett.com"
    search_path: str = "/listing"
    concurrency: int = 3
    delay_seconds: float = 2.0
    stale_hours: int = 12
    max_listings: int = 0  # 0 = unlimited
    headless: bool = True

    # Paths (relative to project root)
    project_root: Path = Path(__file__).resolve().parents[2]
    feed_path: Path = Path("docs/feed.csv")
    state_path: Path = Path("state/listings.json")

    @property
    def abs_feed_path(self) -> Path:
        return self.project_root / self.feed_path

    @property
    def abs_state_path(self) -> Path:
        return self.project_root / self.state_path


settings = Settings()
