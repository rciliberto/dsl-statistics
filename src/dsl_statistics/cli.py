"""DSL Tournament Scraper — CLI entry point."""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import click
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

from dsl_statistics.db import (
    fix_hero_names,
    get_connection,
    get_hero_id_map,
    get_known_match_ids,
    get_latest_stats_time,
    init_db,
    insert_player_heroes,
    insert_player_match,
    insert_player_stats,
    mark_departed_members,
    upsert_division,
    upsert_heroes,
    upsert_player,
    upsert_team,
    upsert_team_member,
)
from dsl_statistics.scrapers.auth import get_authenticated_context
from dsl_statistics.scrapers.statlocker import scrape_heroes_full, scrape_player_stats
from dsl_statistics.scrapers.steam import fetch_steam_info
from dsl_statistics.scrapers.tournament import scrape_team_page, scrape_teams_list

load_dotenv()

CACHE_HOURS = 24


console = Console(stderr=True)


def setup_logging(debug: bool = False) -> None:
    level = logging.DEBUG if debug else logging.INFO
    file_fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(console=console, show_path=False, markup=False),
            logging.FileHandler("scrape.log", encoding="utf-8"),
        ],
    )
    # File handler gets the detailed format
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.FileHandler):
            h.setFormatter(logging.Formatter(file_fmt))


def is_cache_fresh(conn, player_id: int) -> bool:
    """Check if player stats were scraped within the last CACHE_HOURS."""
    latest = get_latest_stats_time(conn, player_id)
    if latest is None:
        return False
    latest_dt = datetime.fromisoformat(latest)
    if latest_dt.tzinfo is None:
        latest_dt = latest_dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - latest_dt < timedelta(hours=CACHE_HOURS)


def scrape_tournament(page, conn, division_filter=None, team_filter=None):
    """Scrape tournament site for divisions, teams, and rosters."""
    logger = logging.getLogger("dsl.tournament")

    teams_data = scrape_teams_list(page, division_filter=division_filter)
    logger.info("Found %d teams", len(teams_data))

    scraped_players = []

    for team_info in teams_data:
        if team_filter and team_info.get("name", "").lower() != team_filter.lower():
            continue

        try:
            team_data = scrape_team_page(
                page, team_info["url"], team_info["division"]
            )
        except Exception as e:
            logger.error(
                "Failed to scrape team page %s: %s", team_info["url"], e
            )
            continue

        div_id = upsert_division(conn, team_data.division)
        team_id = upsert_team(
            conn,
            {
                "division_id": div_id,
                "name": team_data.name,
                "page_url": team_data.page_url,
            },
        )

        current_player_ids = []
        for player in team_data.players:
            if not player.steam_account_id:
                logger.warning(
                    "Player '%s' has no steam account ID, skipping",
                    player.display_name,
                )
                continue

            player_id = upsert_player(
                conn,
                {
                    "display_name": player.display_name,
                    "discord_name": player.discord_name,
                    "discord_id": player.discord_id,
                    "steam_profile_url": player.steam_profile_url,
                    "steam_account_id": player.steam_account_id,
                    "statlocker_url": player.statlocker_url,
                },
            )
            current_player_ids.append(player_id)

            upsert_team_member(
                conn, team_id, player_id, player.role, player.is_poc
            )
            scraped_players.append(
                {
                    "player_id": player_id,
                    "steam_account_id": player.steam_account_id,
                    "statlocker_url": player.statlocker_url,
                    "display_name": player.display_name,
                }
            )

        mark_departed_members(conn, team_id, current_player_ids)

    return scraped_players


def scrape_statlocker_all(page, conn, players, hero_id_map, force=False, refresh=False):
    """Scrape statlocker for all players."""
    logger = logging.getLogger("dsl.statlocker")
    stats_count = 0
    fail_count = 0
    skip_cache = force or refresh

    # Pre-filter to players we'll actually scrape so the progress bar is accurate
    to_scrape = []
    for p in players:
        if not p["statlocker_url"]:
            logger.warning("Player '%s' has no statlocker URL", p["display_name"])
            continue
        if not skip_cache and is_cache_fresh(conn, p["player_id"]):
            logger.debug("Skipping '%s' (cached)", p["display_name"])
            continue
        to_scrape.append(p)

    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        TextColumn("{task.fields[player]}"),
        console=console,
    )

    with progress:
        task = progress.add_task("Scraping statlocker", total=len(to_scrape), player="")
        for p in to_scrape:
            progress.update(task, player=p["display_name"])
            try:
                known_ids = set() if force else get_known_match_ids(conn, p["player_id"])
                data = scrape_player_stats(page, p["steam_account_id"], hero_id_map, known_match_ids=known_ids)

                stats_id = insert_player_stats(
                    conn,
                    p["player_id"],
                    data.pp_score,
                    data.rank_number,
                    data.rank_subrank,
                )

                if data.heroes:
                    hero_dicts = [
                        {
                            "hero_name": h.hero_name,
                            "matches_played": h.matches_played,
                            "win_rate": h.win_rate,
                            "is_most_played": h.is_most_played,
                        }
                        for h in data.heroes
                    ]
                    insert_player_heroes(conn, stats_id, hero_dicts)

                new_matches = 0
                for match in data.matches:
                    inserted = insert_player_match(
                        conn,
                        p["player_id"],
                        {
                            "match_id": match.match_id,
                            "hero_name": match.hero_name,
                            "pp_before": match.pp_before,
                            "pp_after": match.pp_after,
                            "pp_change": match.pp_change,
                            "result": match.result,
                            "match_date": match.match_date,
                        },
                    )
                    if inserted:
                        new_matches += 1

                if data.first_game_at:
                    conn.execute(
                        "UPDATE players SET first_game_at = %s WHERE id = %s AND first_game_at IS NULL",
                        (data.first_game_at, p["player_id"]),
                    )
                    conn.commit()

                stats_count += 1
                logger.info(
                    "Player '%s': PP=%.1f, %d heroes, %d new matches",
                    p["display_name"],
                    data.pp_score or 0,
                    len(data.heroes),
                    new_matches,
                )
            except Exception as e:
                fail_count += 1
                logger.error(
                    "Failed to scrape statlocker for '%s': %s", p["display_name"], e
                )
            progress.advance(task)

    return stats_count, fail_count


def scrape_steam_all(conn, players, refresh=False):
    """Fetch Steam account info for players."""
    logger = logging.getLogger("dsl.steam")
    api_key = os.getenv("STEAM_API_KEY")
    if not api_key:
        logger.warning("STEAM_API_KEY not set in .env, skipping Steam API scrape")
        return 0

    count = 0
    for p in players:
        if not refresh:
            row = conn.execute(
                "SELECT steam_profile_visible FROM players WHERE id = %s",
                (p["player_id"],),
            ).fetchone()
            if row and row[0] is not None:
                continue

        try:
            info = fetch_steam_info(api_key, p["steam_account_id"])
            conn.execute(
                """UPDATE players SET
                       steam_profile_visible = %s,
                       steam_account_created = %s,
                       steam_games_owned = %s
                   WHERE id = %s""",
                (
                    info["visible"],
                    info["account_created"],
                    info["games_owned"],
                    p["player_id"],
                ),
            )
            conn.commit()
            count += 1
            logger.info(
                "Steam info for '%s': visible=%s, games=%s",
                p["display_name"],
                info["visible"],
                info["games_owned"],
            )
        except Exception as e:
            logger.error(
                "Failed to fetch Steam info for '%s': %s", p["display_name"], e
            )

    return count


@click.command()
@click.option("--division", default=None, help="Scrape only this division")
@click.option("--team", default=None, help="Scrape only this team")
@click.option("--refresh", is_flag=True, help="Bypass 24h cache but stop on known matches")
@click.option("--force", is_flag=True, help="Bypass 24h cache and re-pull all matches")
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.option("--skip-statlocker", is_flag=True, help="Skip statlocker scrape")
@click.option("--skip-steam", is_flag=True, help="Skip Steam API calls")
@click.option(
    "--refresh-steam", is_flag=True, help="Re-fetch Steam data for all players"
)
def main(division, team, refresh, force, debug, skip_statlocker, skip_steam, refresh_steam):
    """DSL Tournament Scraper — collect player data from tournament site, statlocker, and Steam."""
    setup_logging(debug=debug)
    logger = logging.getLogger("dsl")

    conn = get_connection()
    init_db(conn)

    try:
        with sync_playwright() as p:
            # Auth + Tournament scrape
            context = get_authenticated_context(p)
            page = context.new_page()

            logger.info("Scraping tournament site...")
            players = scrape_tournament(page, conn, division, team)
            logger.info("Found %d players from tournament site", len(players))

            page.close()
            context.close()

            # Statlocker scrape (separate headless browser)
            if not skip_statlocker:
                browser = p.chromium.launch(headless=True)
                sl_context = browser.new_context()
                sl_page = sl_context.new_page()

                # Fetch hero ID → name mapping (once per session)
                hero_id_map = get_hero_id_map(conn)
                if not hero_id_map:
                    logger.info("Fetching hero definitions from statlocker...")
                    heroes_raw = scrape_heroes_full(sl_page)
                    if heroes_raw:
                        upsert_heroes(conn, heroes_raw)
                        hero_id_map = {h["id"]: h["name"] for h in heroes_raw}
                        # Fix any existing "Hero N" entries from previous scrapes
                        fixed = fix_hero_names(conn)
                        if fixed:
                            logger.info("Fixed %d hero name entries", fixed)

                logger.info("Scraping statlocker profiles...")
                stats_count, fail_count = scrape_statlocker_all(
                    sl_page, conn, players, hero_id_map, force=force, refresh=refresh
                )

                sl_page.close()
                sl_context.close()
                browser.close()

                logger.info(
                    "Statlocker: %d scraped, %d failed", stats_count, fail_count
                )
    except KeyboardInterrupt:
        # Suppress Playwright's async teardown errors
        logging.getLogger("asyncio").setLevel(logging.CRITICAL)
        logger.info("Interrupted — shutting down cleanly")
        conn.close()
        return

    # Steam API (no browser needed)
    if not skip_steam:
        logger.info("Fetching Steam account info...")
        steam_count = scrape_steam_all(conn, players, refresh=refresh_steam)
        logger.info("Steam: %d profiles updated", steam_count)

    conn.close()
    logger.info("Done!")
