import sqlite3

import pytest

from dsl_statistics.db import init_db
from dsl_statistics.db import (
    upsert_division,
    upsert_team,
    upsert_player,
    upsert_team_member,
    mark_departed_members,
)
from dsl_statistics.db import (
    insert_player_stats,
    insert_player_heroes,
    insert_player_match,
    get_latest_stats_time,
)


@pytest.fixture
def conn():
    """In-memory SQLite database for testing."""
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON")
    init_db(connection)
    yield connection
    connection.close()


def test_all_tables_exist(conn):
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    expected = [
        "divisions",
        "player_heroes",
        "player_matches",
        "player_stats",
        "players",
        "team_members",
        "teams",
    ]
    assert tables == expected


def test_division_name_unique(conn):
    conn.execute("INSERT INTO divisions (name) VALUES ('Division 1')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO divisions (name) VALUES ('Division 1')")


def test_player_steam_id_unique(conn):
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '123')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO players (display_name, steam_account_id) VALUES ('Bob', '123')"
        )


def test_team_member_unique_constraint(conn):
    conn.execute("INSERT INTO divisions (name) VALUES ('Div 1')")
    conn.execute(
        "INSERT INTO teams (division_id, name, page_url) VALUES (1, 'Team A', 'http://a')"
    )
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '123')"
    )
    conn.execute(
        "INSERT INTO team_members (team_id, player_id, role, is_poc, joined_at) "
        "VALUES (1, 1, 'core', 0, '2026-01-01')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO team_members (team_id, player_id, role, is_poc, joined_at) "
            "VALUES (1, 1, 'substitute', 0, '2026-02-01')"
        )


def test_player_match_unique_constraint(conn):
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '123')"
    )
    conn.execute(
        "INSERT INTO player_matches (player_id, match_id, hero_name, result, match_date, scraped_at) "
        "VALUES (1, 'match_1', 'Haze', 'win', '2026-01-01', '2026-01-02')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO player_matches (player_id, match_id, hero_name, result, match_date, scraped_at) "
            "VALUES (1, 'match_1', 'Haze', 'win', '2026-01-01', '2026-01-02')"
        )


def test_team_member_role_check(conn):
    conn.execute("INSERT INTO divisions (name) VALUES ('Div 1')")
    conn.execute(
        "INSERT INTO teams (division_id, name, page_url) VALUES (1, 'Team A', 'http://a')"
    )
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '123')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO team_members (team_id, player_id, role, is_poc, joined_at) "
            "VALUES (1, 1, 'invalid_role', 0, '2026-01-01')"
        )


def test_upsert_division_creates(conn):
    div_id = upsert_division(conn, "Division 1")
    assert div_id == 1
    row = conn.execute("SELECT name FROM divisions WHERE id = ?", (div_id,)).fetchone()
    assert row[0] == "Division 1"


def test_upsert_division_returns_existing(conn):
    id1 = upsert_division(conn, "Division 1")
    id2 = upsert_division(conn, "Division 1")
    assert id1 == id2


def test_upsert_player_creates(conn):
    player_id = upsert_player(
        conn,
        {
            "display_name": "Alice",
            "discord_name": "alice#1234",
            "steam_account_id": "12345",
            "steam_profile_url": "https://steam/12345",
            "statlocker_url": "https://statlocker.gg/profile/12345",
        },
    )
    assert player_id == 1


def test_upsert_player_updates_display_name(conn):
    upsert_player(conn, {"display_name": "Alice", "steam_account_id": "12345"})
    upsert_player(conn, {"display_name": "Alice_New", "steam_account_id": "12345"})
    row = conn.execute(
        "SELECT display_name FROM players WHERE steam_account_id = '12345'"
    ).fetchone()
    assert row[0] == "Alice_New"


def test_upsert_player_preserves_existing_fields(conn):
    """COALESCE logic: re-upserting without discord_name keeps existing value."""
    upsert_player(
        conn,
        {
            "display_name": "Alice",
            "discord_name": "alice#1234",
            "steam_account_id": "12345",
        },
    )
    upsert_player(conn, {"display_name": "Alice_Updated", "steam_account_id": "12345"})
    row = conn.execute(
        "SELECT display_name, discord_name FROM players WHERE steam_account_id = '12345'"
    ).fetchone()
    assert row[0] == "Alice_Updated"
    assert row[1] == "alice#1234"


def test_upsert_team_member_detects_leave(conn):
    upsert_division(conn, "Div 1")
    upsert_team(conn, {"name": "Team A", "page_url": "http://a", "division_id": 1})
    upsert_player(conn, {"display_name": "Alice", "steam_account_id": "1"})
    upsert_player(conn, {"display_name": "Bob", "steam_account_id": "2"})
    upsert_team_member(conn, team_id=1, player_id=1, role="core", is_poc=True)
    upsert_team_member(conn, team_id=1, player_id=2, role="core", is_poc=False)
    mark_departed_members(conn, team_id=1, current_player_ids=[1])
    row = conn.execute(
        "SELECT left_at FROM team_members WHERE player_id = 2"
    ).fetchone()
    assert row[0] is not None


def test_upsert_team_member_reactivates(conn):
    upsert_division(conn, "Div 1")
    upsert_team(conn, {"name": "Team A", "page_url": "http://a", "division_id": 1})
    upsert_player(conn, {"display_name": "Alice", "steam_account_id": "1"})
    upsert_team_member(conn, team_id=1, player_id=1, role="core", is_poc=False)
    mark_departed_members(conn, team_id=1, current_player_ids=[])
    upsert_team_member(conn, team_id=1, player_id=1, role="substitute", is_poc=True)
    row = conn.execute(
        "SELECT left_at, role, is_poc FROM team_members WHERE player_id = 1"
    ).fetchone()
    assert row[0] is None
    assert row[1] == "substitute"
    assert row[2] == 1


def test_insert_player_stats(conn):
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '1')"
    )
    stats_id = insert_player_stats(
        conn, player_id=1, pp_score=1500.0, rank_number=9, rank_subrank=3
    )
    assert stats_id == 1
    row = conn.execute(
        "SELECT pp_score, rank_number FROM player_stats WHERE id = 1"
    ).fetchone()
    assert row[0] == 1500.0
    assert row[1] == 9


def test_insert_player_match_dedup(conn):
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '1')"
    )
    match_data = {
        "match_id": "m1",
        "hero_name": "Haze",
        "pp_before": 1490.0,
        "pp_after": 1500.0,
        "pp_change": 10.0,
        "result": "win",
        "match_date": "2026-01-01",
    }
    inserted = insert_player_match(conn, player_id=1, match_data=match_data)
    assert inserted is True
    inserted2 = insert_player_match(conn, player_id=1, match_data=match_data)
    assert inserted2 is False


def test_get_latest_stats_time_none(conn):
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '1')"
    )
    assert get_latest_stats_time(conn, player_id=1) is None


def test_get_latest_stats_time_returns_most_recent(conn):
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '1')"
    )
    conn.execute(
        "INSERT INTO player_stats (player_id, pp_score, rank_number, rank_subrank, scraped_at) "
        "VALUES (1, 1500, 9, 3, '2026-01-01 12:00:00')"
    )
    conn.execute(
        "INSERT INTO player_stats (player_id, pp_score, rank_number, rank_subrank, scraped_at) "
        "VALUES (1, 1510, 9, 3, '2026-01-02 12:00:00')"
    )
    result = get_latest_stats_time(conn, player_id=1)
    assert result == "2026-01-02 12:00:00"


def test_insert_player_heroes(conn):
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '1')"
    )
    stats_id = insert_player_stats(
        conn, player_id=1, pp_score=1500.0, rank_number=9, rank_subrank=3
    )
    heroes = [
        {"hero_name": "Haze", "matches_played": 50, "win_rate": 0.62, "is_most_played": True},
        {"hero_name": "Infernus", "matches_played": 30, "win_rate": 0.55, "is_most_played": False},
    ]
    insert_player_heroes(conn, stats_id, heroes)
    rows = conn.execute(
        "SELECT hero_name, matches_played FROM player_heroes WHERE stats_id = ?",
        (stats_id,),
    ).fetchall()
    assert len(rows) == 2


def test_insert_player_heroes_dedup(conn):
    conn.execute(
        "INSERT INTO players (display_name, steam_account_id) VALUES ('Alice', '1')"
    )
    stats_id = insert_player_stats(
        conn, player_id=1, pp_score=1500.0, rank_number=9, rank_subrank=3
    )
    heroes = [{"hero_name": "Haze", "matches_played": 50, "win_rate": 0.62}]
    insert_player_heroes(conn, stats_id, heroes)
    insert_player_heroes(conn, stats_id, heroes)
    rows = conn.execute(
        "SELECT COUNT(*) FROM player_heroes WHERE stats_id = ?", (stats_id,)
    ).fetchone()
    assert rows[0] == 1
