import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from playwright.sync_api import Page

from dsl_statistics.db import RANK_NAMES

logger = logging.getLogger(__name__)

STATLOCKER_BASE = "https://statlocker.gg"
RATE_LIMIT_SECONDS = 0
MATCH_PAGE_SIZE = 50
MATCH_LOOKBACK_DAYS = 90


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
    known_match_ids: set[str] | None = None,
) -> StatlockerData:
    """Scrape a player's statlocker profile via network interception.

    Intercepts these API endpoints:
      - /api/info/heroes-full → hero ID-to-name mapping
      - /api/profile/steam-profile/{id} → ppScore, estimatedRankNumber
      - /api/profile/data/matches/{id}/concise?gameMode=1 → matchHistory, mostPlayedHeroes, storedPPScore
    """
    data = StatlockerData()
    api_responses: list[dict] = []
    hero_id_map: dict[int, str] = {}

    def capture_response(response):
        url = response.url
        if response.status == 200 and "/api/" in url:
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
        # First load triggers statlocker to recalculate PP from matches.
        # The data returned may be stale/cached (PP can show as 0).
        page.goto(profile_url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=30_000)

        # Build hero ID map from the first load (heroes-full is static)
        for resp in api_responses:
            if "heroes-full" in resp["url"] and isinstance(resp["data"], list):
                for hero in resp["data"]:
                    if "id" in hero and "name" in hero:
                        hero_id_map[hero["id"]] = hero["name"]
                logger.debug("Built hero map with %d heroes", len(hero_id_map))
                break

        # Wait for statlocker to recalculate PP, then reload for fresh data
        time.sleep(2)
        api_responses.clear()
        page.reload(wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=30_000)
        time.sleep(1)
    except Exception as e:
        logger.error(
            "Failed to load statlocker profile %s: %s", steam_account_id, e
        )
        page.remove_listener("response", capture_response)
        return data

    page.remove_listener("response", capture_response)

    # Parse profile and hero data from the second (fresh) load
    for resp in api_responses:
        try:
            _parse_api_response(resp["url"], resp["data"], data, hero_id_map, steam_account_id)
        except Exception as e:
            logger.warning("Failed to parse API response %s: %s", resp["url"], e)

    # Paginate through match history via API to cover the full lookback window
    _fetch_all_matches(page, steam_account_id, data, hero_id_map, known_match_ids or set())

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


def _decode_rank_number(estimated_rank: int) -> tuple[int, int]:
    """Decode statlocker's estimatedRankNumber into (rank_number, subrank).

    Format: tier * 10 + subrank, e.g. 103 = tier 10 (Ascendant), subrank 3.
    """
    rank_number = estimated_rank // 10
    rank_subrank = estimated_rank % 10
    return rank_number, rank_subrank


def _parse_api_response(
    url: str, body: dict | list, data: StatlockerData,
    hero_id_map: dict[int, str], steam_account_id: str,
) -> None:
    """Parse a captured API response from statlocker."""
    if not isinstance(body, dict):
        return

    # /api/profile/steam-profile/{id} — PP score and rank
    if f"steam-profile/{steam_account_id}" in url:
        if "ppScore" in body:
            data.pp_score = float(body["ppScore"])
        if "estimatedRankNumber" in body:
            rank_num, subrank = _decode_rank_number(body["estimatedRankNumber"])
            data.rank_number = rank_num
            data.rank_subrank = subrank
        return

    # /api/profile/data/matches/{id}/concise — match history and hero stats
    if f"matches/{steam_account_id}/concise" in url:
        # Prefer storedPPScore over steam-profile ppScore if available
        if "storedPPScore" in body and body["storedPPScore"] is not None:
            data.pp_score = float(body["storedPPScore"])

        # Hero stats from profileAggregateStats.mostPlayedHeroes
        agg = body.get("profileAggregateStats", {})
        most_played = agg.get("mostPlayedHeroes", [])
        if most_played and not data.heroes:
            for i, hero in enumerate(most_played):
                hero_id = hero.get("heroId")
                hero_name = hero_id_map.get(hero_id, f"Hero {hero_id}")
                matches_count = hero.get("matches", 0)
                win_rate = hero.get("winRate", 0)
                if isinstance(win_rate, (int, float)) and win_rate > 1:
                    win_rate = win_rate / 100.0

                data.heroes.append(
                    HeroStats(
                        hero_name=hero_name,
                        matches_played=int(matches_count),
                        win_rate=float(win_rate),
                        is_most_played=i < 3,
                    )
                )

        # Match history is fetched separately via pagination in _fetch_all_matches


def _fetch_all_matches(
    page: Page,
    steam_account_id: str,
    data: StatlockerData,
    hero_id_map: dict[int, str],
    known_match_ids: set[str],
) -> None:
    """Paginate through match history until we've covered at least MATCH_LOOKBACK_DAYS.

    All matches on every fetched page are kept (no hard date filter on individual
    matches).  Pagination stops once we hit a match older than the lookback window,
    a match already in the database, or there are no more results.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MATCH_LOOKBACK_DAYS)
    base_url = (
        f"{STATLOCKER_BASE}/api/profile/data/matches/"
        f"{steam_account_id}/concise?gameMode=1"
    )
    offset = 0
    seen_ids: set[str] = set()

    while True:
        url = f"{base_url}&offset={offset}&limit={MATCH_PAGE_SIZE}"
        try:
            result = page.evaluate(
                """async (url) => {
                    const resp = await fetch(url);
                    if (!resp.ok) return { ok: false, status: resp.status };
                    const data = await resp.json();
                    return { ok: true, matches: data.matchHistory || [] };
                }""",
                url,
            )
        except Exception as e:
            logger.warning("Failed to fetch matches at offset %d: %s", offset, e)
            break

        if not result.get("ok"):
            logger.warning(
                "Match API returned %s at offset %d",
                result.get("status"),
                offset,
            )
            break

        matches = result.get("matches", [])
        if not matches:
            break

        stop_after_page = False
        new_in_page = 0
        for match in matches:
            if not isinstance(match, dict):
                continue
            match_id = str(match.get("match_id", ""))
            if not match_id or match_id in seen_ids:
                continue

            # Stop paginating if we've reached a match we already have
            if match_id in known_match_ids:
                stop_after_page = True
                continue

            start_time = match.get("start_time")
            if not start_time:
                continue
            try:
                match_date = datetime.fromtimestamp(
                    start_time / 1000, tz=timezone.utc
                )
            except (ValueError, OSError):
                continue

            if match_date < cutoff:
                stop_after_page = True

            seen_ids.add(match_id)
            new_in_page += 1

            hero_id = match.get("hero_id")
            hero_name = (
                hero_id_map.get(hero_id, f"Hero {hero_id}") if hero_id else None
            )

            match_result = match.get("match_result")
            result_str = None
            if match_result is not None:
                result_str = "win" if match_result == 1 else "loss"

            pp_change = match.get("ppImpact")

            data.matches.append(
                MatchData(
                    match_id=match_id,
                    hero_name=hero_name,
                    pp_change=float(pp_change) if pp_change is not None else None,
                    result=result_str,
                    match_date=match_date.isoformat(),
                )
            )

        logger.debug(
            "Fetched offset=%d: %d matches, %d new",
            offset,
            len(matches),
            new_in_page,
        )

        if stop_after_page or new_in_page == 0:
            break

        offset += MATCH_PAGE_SIZE
