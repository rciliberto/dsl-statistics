import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

STEAM_API_BASE = "https://api.steampowered.com"
STEAM64_OFFSET = 76561197960265728


def steam32_to_steam64(account_id: str) -> int:
    """Convert Steam32 account ID to Steam64.

    If the ID is already 64-bit (>= offset), return as-is.
    """
    id_int = int(account_id)
    if id_int >= STEAM64_OFFSET:
        return id_int
    return id_int + STEAM64_OFFSET


def fetch_player_summary(api_key: str, steam_account_id: str) -> dict:
    """Fetch player summary from Steam API.

    Returns dict with keys: visible (bool), timecreated (int|None).
    """
    steam64 = steam32_to_steam64(steam_account_id)
    url = f"{STEAM_API_BASE}/ISteamUser/GetPlayerSummaries/v2/"
    resp = requests.get(
        url, params={"key": api_key, "steamids": str(steam64)}, timeout=10
    )
    resp.raise_for_status()

    players = resp.json().get("response", {}).get("players", [])
    if not players:
        logger.warning("No player data found for Steam ID %s", steam_account_id)
        return {"visible": False, "timecreated": None}

    player = players[0]
    visible = player.get("communityvisibilitystate", 1) == 3
    timecreated = player.get("timecreated") if visible else None

    return {"visible": visible, "timecreated": timecreated}


def fetch_owned_games_count(api_key: str, steam_account_id: str) -> int | None:
    """Fetch the number of games owned. Returns None if profile is private."""
    steam64 = steam32_to_steam64(steam_account_id)
    url = f"{STEAM_API_BASE}/IPlayerService/GetOwnedGames/v1/"
    resp = requests.get(
        url,
        params={
            "key": api_key,
            "steamid": str(steam64),
            "include_played_free_games": True,
        },
        timeout=10,
    )
    resp.raise_for_status()

    data = resp.json().get("response", {})
    return data.get("game_count")


def fetch_steam_info(api_key: str, steam_account_id: str) -> dict:
    """Fetch all Steam info for a player.

    Returns dict with: visible, account_created (str|None), games_owned (int|None).
    """
    summary = fetch_player_summary(api_key, steam_account_id)

    result = {
        "visible": summary["visible"],
        "account_created": None,
        "games_owned": None,
    }

    if summary["timecreated"]:
        result["account_created"] = datetime.fromtimestamp(
            summary["timecreated"], tz=timezone.utc
        ).isoformat()

    if summary["visible"]:
        result["games_owned"] = fetch_owned_games_count(api_key, steam_account_id)

    return result
