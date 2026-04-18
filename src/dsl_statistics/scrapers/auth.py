import json
import logging
from pathlib import Path

from playwright.sync_api import BrowserContext, Page

logger = logging.getLogger(__name__)

COOKIES_PATH = Path(".cookies.json")
TOURNAMENT_BASE = "https://players.deadlockdeathslam.com"
LOGIN_URL = f"{TOURNAMENT_BASE}/accounts/discord/login/"


def save_cookies(context: BrowserContext) -> None:
    """Save browser cookies to disk."""
    cookies = context.cookies()
    COOKIES_PATH.write_text(json.dumps(cookies, indent=2))
    logger.info("Saved %d cookies to %s", len(cookies), COOKIES_PATH)


def load_cookies(context: BrowserContext) -> bool:
    """Load cookies from disk. Returns True if cookies were loaded."""
    if not COOKIES_PATH.exists():
        logger.info("No saved cookies found")
        return False
    try:
        cookies = json.loads(COOKIES_PATH.read_text())
        context.add_cookies(cookies)
        logger.info("Loaded %d cookies from %s", len(cookies), COOKIES_PATH)
        return True
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("Failed to load cookies: %s", e)
        return False


def is_logged_in(page: Page) -> bool:
    """Check if the current page is authenticated (not redirected to login)."""
    page.goto(f"{TOURNAMENT_BASE}/teams/", wait_until="domcontentloaded")
    return "/accounts/" not in page.url and "login" not in page.url.lower()


def interactive_login(context: BrowserContext) -> None:
    """Open a visible browser for the user to log in via Discord."""
    page = context.new_page()
    page.goto(LOGIN_URL)
    logger.info("Please log in via Discord in the browser window...")
    page.wait_for_url(f"{TOURNAMENT_BASE}/**", timeout=300_000)  # 5 min timeout
    save_cookies(context)
    page.close()


def get_authenticated_context(playwright, headless: bool = True) -> BrowserContext:
    """Get a browser context with valid session cookies.

    Falls back to interactive login if cookies are missing or expired.
    """
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context()

    if load_cookies(context):
        page = context.new_page()
        if is_logged_in(page):
            page.close()
            logger.info("Session is valid")
            return context
        page.close()
        logger.info("Saved cookies are expired")

    # Need interactive login — must be visible
    browser.close()
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    interactive_login(context)
    return context
