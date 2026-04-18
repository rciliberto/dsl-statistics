from unittest.mock import MagicMock, patch

from dsl_statistics.scrapers.steam import (
    fetch_owned_games_count,
    fetch_player_summary,
    fetch_steam_info,
    steam32_to_steam64,
)


def test_steam32_to_steam64():
    assert steam32_to_steam64("12345678") == 76561198972531406


def test_steam32_to_steam64_already_64bit():
    assert steam32_to_steam64("76561198012345678") == 76561198012345678


@patch("dsl_statistics.scrapers.steam.requests.get")
def test_fetch_player_summary_public(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "response": {
                "players": [
                    {
                        "steamid": "76561198012345678",
                        "communityvisibilitystate": 3,
                        "timecreated": 1300000000,
                    }
                ]
            }
        },
    )
    result = fetch_player_summary("fake_key", "12345678")
    assert result["visible"] is True
    assert result["timecreated"] == 1300000000


@patch("dsl_statistics.scrapers.steam.requests.get")
def test_fetch_player_summary_private(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "response": {
                "players": [
                    {
                        "steamid": "76561198012345678",
                        "communityvisibilitystate": 1,
                    }
                ]
            }
        },
    )
    result = fetch_player_summary("fake_key", "12345678")
    assert result["visible"] is False
    assert result["timecreated"] is None


@patch("dsl_statistics.scrapers.steam.requests.get")
def test_fetch_player_summary_no_players(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {"response": {"players": []}},
    )
    result = fetch_player_summary("fake_key", "12345678")
    assert result["visible"] is False
    assert result["timecreated"] is None


@patch("dsl_statistics.scrapers.steam.requests.get")
def test_fetch_owned_games_count(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {"response": {"game_count": 42}},
    )
    assert fetch_owned_games_count("fake_key", "12345678") == 42


@patch("dsl_statistics.scrapers.steam.requests.get")
def test_fetch_owned_games_private_profile(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {"response": {}},
    )
    assert fetch_owned_games_count("fake_key", "12345678") is None


@patch("dsl_statistics.scrapers.steam.fetch_owned_games_count", return_value=42)
@patch(
    "dsl_statistics.scrapers.steam.fetch_player_summary",
    return_value={"visible": True, "timecreated": 1300000000},
)
def test_fetch_steam_info_public(mock_summary, mock_games):
    result = fetch_steam_info("fake_key", "12345678")
    assert result["visible"] is True
    assert result["account_created"] is not None
    assert result["games_owned"] == 42


@patch(
    "dsl_statistics.scrapers.steam.fetch_player_summary",
    return_value={"visible": False, "timecreated": None},
)
def test_fetch_steam_info_private(mock_summary):
    result = fetch_steam_info("fake_key", "12345678")
    assert result["visible"] is False
    assert result["account_created"] is None
    assert result["games_owned"] is None
