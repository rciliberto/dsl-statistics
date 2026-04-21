import os

import psycopg
from psycopg import errors as pg_errors

RANK_NAMES = {
    0: "Obscurus",
    1: "Initiate",
    2: "Seeker",
    3: "Alchemist",
    4: "Arcanist",
    5: "Ritualist",
    6: "Emissary",
    7: "Archon",
    8: "Oracle",
    9: "Phantom",
    10: "Ascendant",
    11: "Eternus",
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS heroes (
    id INTEGER PRIMARY KEY,
    class_name TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS divisions (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    division_id INTEGER NOT NULL REFERENCES divisions(id),
    name TEXT NOT NULL,
    page_url TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    display_name TEXT NOT NULL,
    discord_name TEXT,
    discord_id TEXT,
    steam_profile_url TEXT,
    steam_account_id TEXT UNIQUE,
    statlocker_url TEXT,
    first_game_at TIMESTAMPTZ,
    steam_account_created TIMESTAMPTZ,
    steam_games_owned INTEGER,
    steam_profile_visible BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS team_members (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES teams(id),
    player_id INTEGER NOT NULL REFERENCES players(id),
    role TEXT NOT NULL CHECK(role IN ('core', 'substitute')),
    is_poc BOOLEAN NOT NULL DEFAULT FALSE,
    joined_at TIMESTAMPTZ NOT NULL,
    left_at TIMESTAMPTZ,
    UNIQUE(team_id, player_id)
);

CREATE TABLE IF NOT EXISTS player_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id),
    pp_score DOUBLE PRECISION,
    rank_number INTEGER,
    rank_subrank INTEGER,
    scraped_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_player_stats_player_scraped
    ON player_stats(player_id, scraped_at);

CREATE TABLE IF NOT EXISTS player_heroes (
    id SERIAL PRIMARY KEY,
    stats_id INTEGER NOT NULL REFERENCES player_stats(id),
    hero_name TEXT NOT NULL,
    matches_played INTEGER,
    win_rate DOUBLE PRECISION,
    is_most_played BOOLEAN DEFAULT FALSE,
    UNIQUE(stats_id, hero_name)
);

CREATE TABLE IF NOT EXISTS player_matches (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id),
    match_id TEXT NOT NULL,
    hero_name TEXT,
    pp_before DOUBLE PRECISION,
    pp_after DOUBLE PRECISION,
    pp_change DOUBLE PRECISION,
    result TEXT,
    match_date TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ NOT NULL,
    UNIQUE(player_id, match_id)
);

CREATE INDEX IF NOT EXISTS idx_player_matches_player_date
    ON player_matches(player_id, match_date);
"""


def init_db(conn: psycopg.Connection) -> None:
    """Create all tables and indexes if they don't exist."""
    conn.execute(SCHEMA_SQL)
    conn.commit()


def get_connection() -> psycopg.Connection:
    """Open a connection using DATABASE_URL from the environment."""
    url = os.environ["DATABASE_URL"]
    conn = psycopg.connect(url)
    return conn


def upsert_division(conn: psycopg.Connection, name: str) -> int:
    """Insert or get existing division. Returns division id."""
    conn.execute(
        "INSERT INTO divisions (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,)
    )
    row = conn.execute("SELECT id FROM divisions WHERE name = %s", (name,)).fetchone()
    conn.commit()
    return row[0]


def upsert_team(conn: psycopg.Connection, data: dict) -> int:
    """Insert or update team by page_url. Returns team id."""
    conn.execute(
        """INSERT INTO teams (division_id, name, page_url, updated_at)
           VALUES (%(division_id)s, %(name)s, %(page_url)s, NOW())
           ON CONFLICT(page_url) DO UPDATE SET
               name = excluded.name,
               division_id = excluded.division_id,
               updated_at = NOW()""",
        data,
    )
    row = conn.execute(
        "SELECT id FROM teams WHERE page_url = %(page_url)s", data
    ).fetchone()
    conn.commit()
    return row[0]


def upsert_player(conn: psycopg.Connection, data: dict) -> int:
    """Insert or update player by steam_account_id. Returns player id."""
    conn.execute(
        """INSERT INTO players (display_name, discord_name, discord_id,
               steam_profile_url, steam_account_id, statlocker_url)
           VALUES (%(display_name)s, %(discord_name)s, %(discord_id)s,
               %(steam_profile_url)s, %(steam_account_id)s, %(statlocker_url)s)
           ON CONFLICT(steam_account_id) DO UPDATE SET
               display_name = excluded.display_name,
               discord_name = COALESCE(excluded.discord_name, players.discord_name),
               discord_id = COALESCE(excluded.discord_id, players.discord_id),
               steam_profile_url = COALESCE(excluded.steam_profile_url, players.steam_profile_url),
               statlocker_url = COALESCE(excluded.statlocker_url, players.statlocker_url)""",
        {
            "display_name": data.get("display_name"),
            "discord_name": data.get("discord_name"),
            "discord_id": data.get("discord_id"),
            "steam_profile_url": data.get("steam_profile_url"),
            "steam_account_id": data.get("steam_account_id"),
            "statlocker_url": data.get("statlocker_url"),
        },
    )
    row = conn.execute(
        "SELECT id FROM players WHERE steam_account_id = %(steam_account_id)s", data
    ).fetchone()
    conn.commit()
    return row[0]


def upsert_team_member(
    conn: psycopg.Connection,
    team_id: int,
    player_id: int,
    role: str,
    is_poc: bool,
) -> None:
    """Insert or update team membership. Reactivates departed members."""
    conn.execute(
        """INSERT INTO team_members (team_id, player_id, role, is_poc, joined_at)
           VALUES (%s, %s, %s, %s, NOW())
           ON CONFLICT(team_id, player_id) DO UPDATE SET
               role = excluded.role,
               is_poc = excluded.is_poc,
               left_at = NULL""",
        (team_id, player_id, role, is_poc),
    )
    conn.commit()


def mark_departed_members(
    conn: psycopg.Connection, team_id: int, current_player_ids: list[int]
) -> None:
    """Set left_at for players no longer on the roster."""
    if not current_player_ids:
        conn.execute(
            "UPDATE team_members SET left_at = NOW() "
            "WHERE team_id = %s AND left_at IS NULL",
            (team_id,),
        )
    else:
        placeholders = ",".join(["%s"] * len(current_player_ids))
        conn.execute(
            f"UPDATE team_members SET left_at = NOW() "
            f"WHERE team_id = %s AND left_at IS NULL AND player_id NOT IN ({placeholders})",
            [team_id] + current_player_ids,
        )
    conn.commit()


def insert_player_stats(
    conn: psycopg.Connection,
    player_id: int,
    pp_score: float | None,
    rank_number: int | None,
    rank_subrank: int | None,
) -> int:
    """Insert a new stats snapshot. Returns stats id."""
    row = conn.execute(
        """INSERT INTO player_stats (player_id, pp_score, rank_number, rank_subrank, scraped_at)
           VALUES (%s, %s, %s, %s, NOW()) RETURNING id""",
        (player_id, pp_score, rank_number, rank_subrank),
    ).fetchone()
    conn.commit()
    return row[0]


def insert_player_heroes(
    conn: psycopg.Connection, stats_id: int, heroes: list[dict]
) -> None:
    """Insert hero stats for a given stats snapshot."""
    for hero in heroes:
        conn.execute(
            """INSERT INTO player_heroes
               (stats_id, hero_name, matches_played, win_rate, is_most_played)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (
                stats_id,
                hero["hero_name"],
                hero.get("matches_played"),
                hero.get("win_rate"),
                hero.get("is_most_played", False),
            ),
        )
    conn.commit()


def insert_player_match(
    conn: psycopg.Connection, player_id: int, match_data: dict
) -> bool:
    """Insert a match record. Returns True if inserted, False if duplicate."""
    try:
        conn.execute(
            """INSERT INTO player_matches
               (player_id, match_id, hero_name, pp_before, pp_after, pp_change,
                result, match_date, scraped_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (
                player_id,
                match_data["match_id"],
                match_data.get("hero_name"),
                match_data.get("pp_before"),
                match_data.get("pp_after"),
                match_data.get("pp_change"),
                match_data.get("result"),
                match_data.get("match_date"),
            ),
        )
        conn.commit()
        return True
    except pg_errors.UniqueViolation:
        conn.rollback()
        return False


def get_latest_stats_time(conn: psycopg.Connection, player_id: int) -> str | None:
    """Return the most recent scraped_at for a player, or None."""
    row = conn.execute(
        "SELECT MAX(scraped_at) FROM player_stats WHERE player_id = %s",
        (player_id,),
    ).fetchone()
    return row[0].isoformat() if row and row[0] else None


def get_known_match_ids(conn: psycopg.Connection, player_id: int) -> set[str]:
    """Return the set of match IDs already stored for a player."""
    rows = conn.execute(
        "SELECT match_id FROM player_matches WHERE player_id = %s",
        (player_id,),
    ).fetchall()
    return {row[0] for row in rows}


def get_prior_player_data(conn: psycopg.Connection, player_id: int) -> dict | None:
    """Return prior scrape data for a player, or None if never scraped.

    Returns dict with 'pp_score' (float|None) and 'known_match_ids' (set[str]).
    """
    match_rows = conn.execute(
        "SELECT match_id FROM player_matches WHERE player_id = %s",
        (player_id,),
    ).fetchall()
    known_ids = {row[0] for row in match_rows}
    if not known_ids:
        return None
    pp_row = conn.execute(
        "SELECT pp_score FROM player_stats WHERE player_id = %s ORDER BY scraped_at DESC LIMIT 1",
        (player_id,),
    ).fetchone()
    pp_score = pp_row[0] if pp_row else None
    return {"pp_score": pp_score, "known_match_ids": known_ids}


def upsert_heroes(conn: psycopg.Connection, heroes: list[dict]) -> None:
    """Insert or update hero ID → name mappings."""
    for hero in heroes:
        conn.execute(
            """INSERT INTO heroes (id, class_name, name, updated_at)
               VALUES (%s, %s, %s, NOW())
               ON CONFLICT(id) DO UPDATE SET
                   class_name = excluded.class_name,
                   name = excluded.name,
                   updated_at = NOW()""",
            (hero["id"], hero["class_name"], hero["name"]),
        )
    conn.commit()


def get_hero_id_map(conn: psycopg.Connection) -> dict[int, str]:
    """Return a mapping of hero ID → display name from the database."""
    rows = conn.execute("SELECT id, name FROM heroes").fetchall()
    return {row[0]: row[1] for row in rows}


def fix_hero_names(conn: psycopg.Connection) -> int:
    """Fix 'Hero N' entries in player_matches and player_heroes using the heroes table.

    Returns the number of rows updated.
    """
    hero_map = get_hero_id_map(conn)
    if not hero_map:
        return 0

    # Build reverse map: "Hero {id}" → display name
    fix_map = {f"Hero {hid}": name for hid, name in hero_map.items()}

    updated = 0
    for old_name, new_name in fix_map.items():
        cur = conn.execute(
            "UPDATE player_matches SET hero_name = %s WHERE hero_name = %s",
            (new_name, old_name),
        )
        updated += cur.rowcount
        cur = conn.execute(
            "UPDATE player_heroes SET hero_name = %s WHERE hero_name = %s",
            (new_name, old_name),
        )
        updated += cur.rowcount

    conn.commit()
    return updated
