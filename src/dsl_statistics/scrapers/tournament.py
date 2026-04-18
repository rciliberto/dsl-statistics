import re
import logging
from dataclasses import dataclass, field

from playwright.sync_api import Page

logger = logging.getLogger(__name__)

TEAMS_URL = "https://players.deadlockdeathslam.com/teams/"
BASE_URL = "https://players.deadlockdeathslam.com"


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

    Handles URLs like:
      https://statlocker.gg/profile/12345678
      https://statlocker.gg/profile/12345678/matches
    """
    if not statlocker_url:
        return None
    match = re.search(r"/profile/(\d+)", statlocker_url)
    return match.group(1) if match else None


def scrape_teams_list(page: Page, division_filter: str | None = None) -> list[dict]:
    """Scrape the main /teams/ page to get division names and team links.

    Returns list of {"division": str, "name": str, "url": str}.

    Site structure:
      <h2>Division Name</h2>
      <table class="table-simple">
        <tr><td><a href="/teams/ID/">Team Name</a></td></tr>
        ...
      </table>
    """
    page.goto(TEAMS_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    teams = []

    # Each division is an <h2> followed by a <table class="table-simple">
    h2_elements = page.query_selector_all("h2")
    logger.info("Found %d division headers", len(h2_elements))

    for h2 in h2_elements:
        division_name = h2.inner_text().strip()

        if division_filter and division_filter.lower() != division_name.lower():
            continue

        # The team table is the next sibling element after the h2
        table = h2.evaluate_handle("e => e.nextElementSibling")
        tag = table.evaluate("e => e ? e.tagName : null")
        if tag != "TABLE":
            logger.warning("Expected TABLE after h2 '%s', got %s", division_name, tag)
            continue

        # Each team is a link in the table
        team_links = table.as_element().query_selector_all("a[href*='/teams/']")
        for link in team_links:
            href = link.get_attribute("href") or ""
            name = link.inner_text().strip()
            if href and name:
                full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                teams.append({
                    "division": division_name,
                    "name": name,
                    "url": full_url,
                })

        logger.info("Division '%s': %d teams", division_name, len(team_links))

    return teams


def scrape_team_page(page: Page, team_url: str, division: str) -> TeamData:
    """Scrape a single team page for roster information.

    Site structure:
      <h1>Team Name</h1>
      <table class="table-simple">
        <tr><th>Discord</th><th>Team Role</th><th>Steam</th><th>Pronouns</th></tr>
        <tr>
          <td><a href="statlocker.gg/profile/ID/matches">discord_name</a> (DisplayName)</td>
          <td><span class="tag">Core|Substitute|Point of Contact</span></td>
          <td><a href="steamcommunity.com/profiles/...">SteamName</a></td>
          <td>Pronouns</td>
        </tr>
      </table>
    """
    page.goto(team_url, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    team_name_el = page.query_selector("h1")
    team_name = team_name_el.inner_text().strip() if team_name_el else "Unknown Team"

    team = TeamData(name=team_name, page_url=team_url, division=division)

    # Get all data rows (skip the header row)
    rows = page.query_selector_all("table.table-simple tr")
    for row in rows:
        # Skip header rows (contain <th>)
        if row.query_selector("th"):
            continue
        try:
            player = _parse_player_row(row)
            if player:
                team.players.append(player)
        except Exception as e:
            logger.warning(
                "Failed to parse player row in team '%s': %s", team_name, e
            )

    logger.info("Team '%s': found %d players", team_name, len(team.players))
    return team


def _parse_player_row(row) -> PlayerData | None:
    """Parse a single <tr> from the team roster table.

    Columns: Discord | Team Role | Steam | Pronouns
    """
    cells = row.query_selector_all("td")
    if len(cells) < 3:
        return None

    # Column 0: Discord — contains statlocker link and discord name
    discord_cell = cells[0]
    discord_text = discord_cell.inner_text().strip()
    statlocker_link = discord_cell.query_selector("a[href*='statlocker.gg']")
    statlocker_url = statlocker_link.get_attribute("href") if statlocker_link else None

    # The display name shown on the link is the discord username
    discord_name = statlocker_link.inner_text().strip() if statlocker_link else discord_text

    # Some entries have "(DisplayName)" after the link — use the full cell text
    display_name = discord_text

    # Column 1: Team Role — contains <span class="tag">
    role_cell = cells[1]
    role_tag = role_cell.query_selector("span.tag")
    role_text = role_tag.inner_text().strip() if role_tag else role_cell.inner_text().strip()

    is_poc = role_text.lower() == "point of contact"
    if role_text.lower() in ("substitute", "sub"):
        role = "substitute"
    else:
        # Both "Core" and "Point of Contact" are core players
        role = "core"

    # Column 2: Steam — contains steam profile link
    steam_cell = cells[2]
    steam_link = steam_cell.query_selector("a[href*='steamcommunity.com']")
    steam_url = steam_link.get_attribute("href") if steam_link else None

    steam_account_id = extract_steam_account_id(statlocker_url)

    return PlayerData(
        display_name=display_name,
        role=role,
        is_poc=is_poc,
        discord_name=discord_name,
        steam_profile_url=steam_url,
        statlocker_url=statlocker_url,
        steam_account_id=steam_account_id,
    )
