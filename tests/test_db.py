import sqlite3

import pytest

from dsl_statistics.db import init_db


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
