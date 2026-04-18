import json
import logging
import time
from dataclasses import dataclass, field

from playwright.sync_api import Page

from dsl_statistics.db import RANK_NAMES

logger = logging.getLogger(__name__)

STATLOCKER_BASE = "https://statlocker.gg"
RATE_LIMIT_SECONDS = 2


@dataclass
class HeroStats:
    hero_name: str
    matches_played: int
    win_rate: float
    is_most_played: bool = False


@dataclass
class MatchData:
    match_id: str
    hero_name: str | None = None
    pp_before: float | None = None
    pp_after: float | None = None
    pp_change: float | None = None
    result: str | None = None  # "win" or "loss"
    match_date: str | None = None


@dataclass
class StatlockerData:
    pp_score: float | None = None
    rank_number: int | None = None
    rank_subrank: int | None = None
    first_game_at: str | None = None
    heroes: list[HeroStats] = field(default_factory=list)
    matches: list[MatchData] = field(default_factory=list)


def scrape_player_stats(
    page: Page,
    steam_account_id: str,
) -> StatlockerData:
    """Scrape a player's statlocker profile via network interception.

    Navigates to the player's profile page and intercepts API responses
    to extract PP score, rank, hero stats, and match history.
    """
    data = StatlockerData()
    api_responses: list[dict] = []

    def capture_response(response):
        url = response.url
        if response.status == 200 and "api" in url.lower():
            try:
                body = response.json()
                api_responses.append({"url": url, "data": body})
                logger.debug("Captured API response: %s", url)
            except Exception:
                pass

    page.on("response", capture_response)

    profile_url = f"{STATLOCKER_BASE}/profile/{steam_account_id}"
    logger.info("Scraping statlocker profile: %s", profile_url)

    try:
        page.goto(profile_url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=30_000)
        time.sleep(1)  # Extra wait for delayed API calls
    except Exception as e:
        logger.error(
            "Failed to load statlocker profile %s: %s", steam_account_id, e
        )
        return data

    for resp in api_responses:
        try:
            _parse_api_response(resp["url"], resp["data"], data)
        except Exception as e:
            logger.warning("Failed to parse API response %s: %s", resp["url"], e)

    logger.info(
        "Statlocker %s: PP=%.1f, rank=%s %s, %d heroes, %d matches",
        steam_account_id,
        data.pp_score or 0,
        RANK_NAMES.get(data.rank_number, "?"),
        data.rank_subrank or "?",
        len(data.heroes),
        len(data.matches),
    )

    time.sleep(RATE_LIMIT_SECONDS)
    return data


def _parse_api_response(
    url: str, body: dict | list, data: StatlockerData
) -> None:
    """Parse a captured API response and extract relevant data.

    Field names cover common patterns — may need adjustment after inspecting
    actual API responses. Run with --debug to log raw responses.
    """
    logger.debug("Parsing API response from: %s", url)
    logger.debug(
        "Response body (truncated): %s", json.dumps(body, default=str)[:500]
    )

    if not isinstance(body, dict):
        return

    # Look for PP score
    for key in ("pp", "pp_score", "performancePoints", "mmr", "rating"):
        if key in body:
            try:
                data.pp_score = float(body[key])
            except (ValueError, TypeError):
                pass

    # Look for rank info
    for key in ("rank", "rankInfo", "rank_info"):
        if key in body and isinstance(body[key], dict):
            rank_data = body[key]
            data.rank_number = rank_data.get(
                "tier", rank_data.get("rank", rank_data.get("number"))
            )
            data.rank_subrank = rank_data.get(
                "subrank", rank_data.get("sub_rank", rank_data.get("division"))
            )
        elif key in body and isinstance(body[key], (int, float)):
            data.rank_number = int(body[key])

    # Look for first game date
    for key in ("firstGame", "first_game", "firstGameAt", "first_match"):
        if key in body:
            data.first_game_at = str(body[key])

    # Look for hero stats
    for key in ("heroes", "heroStats", "hero_stats", "topHeroes"):
        if key in body and isinstance(body[key], list):
            _parse_heroes(body[key], data)

    # Look for match history
    for key in ("matches", "matchHistory", "match_history", "recentMatches", "games"):
        if key in body and isinstance(body[key], list):
            _parse_matches(body[key], data)


def _parse_heroes(heroes_list: list, data: StatlockerData) -> None:
    """Parse hero stats from API response."""
    for i, hero in enumerate(heroes_list):
        if not isinstance(hero, dict):
            continue
        name = hero.get("name", hero.get("hero_name", hero.get("heroName", "")))
        if not name:
            continue
        matches = hero.get(
            "matches", hero.get("matches_played", hero.get("games", 0))
        )
        wins = hero.get("wins", 0)
        total = hero.get("matches", hero.get("games", matches))
        win_rate = hero.get(
            "win_rate",
            hero.get("winRate", wins / total if total > 0 else 0),
        )
        if isinstance(win_rate, (int, float)) and win_rate > 1:
            win_rate = win_rate / 100.0  # Convert percentage to decimal

        data.heroes.append(
            HeroStats(
                hero_name=name,
                matches_played=int(matches) if matches else 0,
                win_rate=float(win_rate) if win_rate else 0.0,
                is_most_played=i < 3,
            )
        )


def _parse_matches(matches_list: list, data: StatlockerData) -> None:
    """Parse match history from API response. Captures up to 100 matches."""
    for match in matches_list[:100]:
        if not isinstance(match, dict):
            continue
        match_id = str(
            match.get("match_id", match.get("matchId", match.get("id", "")))
        )
        if not match_id:
            continue

        hero = match.get("hero", match.get("hero_name", match.get("heroName")))
        result_raw = match.get("result", match.get("outcome", match.get("win")))
        if isinstance(result_raw, bool):
            result = "win" if result_raw else "loss"
        elif isinstance(result_raw, str):
            result = (
                "win"
                if result_raw.lower() in ("win", "won", "victory")
                else "loss"
            )
        else:
            result = None

        pp_before = match.get("pp_before", match.get("ppBefore"))
        pp_after = match.get("pp_after", match.get("ppAfter"))
        pp_change = match.get(
            "pp_change", match.get("ppChange", match.get("pp_delta"))
        )

        if pp_before is not None and pp_after is not None and pp_change is None:
            pp_change = float(pp_after) - float(pp_before)

        date = match.get(
            "date",
            match.get(
                "match_date", match.get("played_at", match.get("timestamp"))
            ),
        )

        data.matches.append(
            MatchData(
                match_id=match_id,
                hero_name=hero,
                pp_before=float(pp_before) if pp_before is not None else None,
                pp_after=float(pp_after) if pp_after is not None else None,
                pp_change=float(pp_change) if pp_change is not None else None,
                result=result,
                match_date=str(date) if date else None,
            )
        )
