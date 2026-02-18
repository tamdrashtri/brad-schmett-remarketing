"""Stealth Playwright browser factory."""

import asyncio
import random

from loguru import logger
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from scraper.config import settings

# Stealth init script: patch navigator properties to avoid detection
STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
window.chrome = {runtime: {}};
"""

# Realistic viewport sizes to rotate through
VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1366, "height": 768},
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]


class StealthBrowser:
    """Manages a stealth Playwright browser instance with concurrent tab support."""

    def __init__(self) -> None:
        self._pw = None
        self._browser: Browser | None = None

    async def start(self) -> None:
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            headless=settings.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        logger.info("Browser started (headless={})", settings.headless)

    async def new_context(self) -> BrowserContext:
        viewport = random.choice(VIEWPORTS)
        ua = random.choice(USER_AGENTS)
        ctx = await self._browser.new_context(
            viewport=viewport,
            user_agent=ua,
            locale="en-US",
            timezone_id="America/Los_Angeles",
        )
        await ctx.add_init_script(STEALTH_SCRIPT)
        return ctx

    async def new_page(self) -> Page:
        ctx = await self.new_context()
        return await ctx.new_page()

    async def close(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()
        logger.info("Browser closed")

    async def __aenter__(self) -> "StealthBrowser":
        await self.start()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()


async def human_delay(base: float | None = None) -> None:
    """Sleep with jitter to mimic human pacing."""
    base = base or settings.delay_seconds
    jitter = random.uniform(0.5, 1.5)
    await asyncio.sleep(base * jitter)
