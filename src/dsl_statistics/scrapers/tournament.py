import re
import logging
from dataclasses import dataclass, field

from playwright.sync_api import Page

logger = logging.getLogger(__name__)

TEAMS_URL = "https://players.deadlockdeathslam.com/teams/"


@dataclass
class PlayerData:
    display_name: str
    role: str  # "core" or "substitute"
    is_poc: bool
    discord_name: str | None = None
    discord_id: str | None = None
    steam_profile_url: str | None = None
    statlocker_url: str | None = None
    steam_account_id: str | None = None


@dataclass
class TeamData:
    name: str
    page_url: str
    division: str
    players: list[PlayerData] = field(default_factory=list)


def extract_steam_account_id(statlocker_url: str | None) -> str | None:
    """Extract Steam32 account ID from statlocker URL.

    Example: https://statlocker.gg/profile/12345678 -> '12345678'
    """
    if not statlocker_url:
        return None
    match = re.search(r"/profile/(\d+)", statlocker_url)
    return match.group(1) if match else None


def scrape_teams_list(page: Page, division_filter: str | None = None) -> list[dict]:
    """Scrape the main /teams/ page to get division names and team links.

    Returns list of {"division": str, "name": str, "url": str}.

    NOTE: CSS selectors below are best-guess placeholders. Update after
    inspecting the live site HTML structure.
    """
    page.goto(TEAMS_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    teams = []
    content = page.content()
    logger.debug(
        "Teams page loaded, URL: %s, content length: %d", page.url, len(content)
    )

    divisions = page.query_selector_all(
        "[class*='division'], [class*='Division'], h2, h3"
    )
    logger.info("Found %d potential division headers", len(divisions))

    if not divisions:
        logger.warning(
            "Could not find division headers. Run with --debug and inspect "
            "scrape.log for page content details."
        )

    return teams


def scrape_team_page(page: Page, team_url: str, division: str) -> TeamData:
    """Scrape a single team page for roster information.

    NOTE: CSS selectors below are best-guess placeholders. Update after
    inspecting the live site HTML structure.
    """
    page.goto(team_url, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    team_name_el = page.query_selector(
        "h1, h2, [class*='team-name'], [class*='teamName']"
    )
    team_name = team_name_el.inner_text().strip() if team_name_el else "Unknown Team"

    team = TeamData(name=team_name, page_url=team_url, division=division)

    player_elements = page.query_selector_all(
        "[class*='member'], [class*='player'], [class*='roster'] tr, [class*='roster'] li"
    )
    logger.info("Team '%s': found %d player elements", team_name, len(player_elements))

    for el in player_elements:
        try:
            player = _parse_player_element(el)
            if player:
                team.players.append(player)
        except Exception as e:
            logger.warning(
                "Failed to parse player element in team '%s': %s", team_name, e
            )

    return team


def _parse_player_element(el) -> PlayerData | None:
    """Parse a single player element from a team page.

    NOTE: Selectors below are best-guess placeholders. Update after
    inspecting the live site HTML structure.
    """
    name_el = el.query_selector("[class*='name'], a, td:first-child")
    if not name_el:
        return None
    display_name = name_el.inner_text().strip()
    if not display_name:
        return None

    text = el.inner_text().lower()
    role = "substitute" if "sub" in text or "substitute" in text else "core"
    is_poc = "poc" in text or "point of contact" in text or "captain" in text

    links = el.query_selector_all("a[href]")
    steam_url = None
    statlocker_url = None
    for link in links:
        href = link.get_attribute("href") or ""
        if "steamcommunity.com" in href or "store.steampowered.com" in href:
            steam_url = href
        elif "statlocker.gg" in href:
            statlocker_url = href

    steam_account_id = extract_steam_account_id(statlocker_url)

    return PlayerData(
        display_name=display_name,
        role=role,
        is_poc=is_poc,
        steam_profile_url=steam_url,
        statlocker_url=statlocker_url,
        steam_account_id=steam_account_id,
    )
