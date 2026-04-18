import sqlite3

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
CREATE TABLE IF NOT EXISTS divisions (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY,
    division_id INTEGER NOT NULL REFERENCES divisions(id),
    name TEXT NOT NULL,
    page_url TEXT NOT NULL UNIQUE,
    updated_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL,
    discord_name TEXT,
    discord_id TEXT,
    steam_profile_url TEXT,
    steam_account_id TEXT UNIQUE,
    statlocker_url TEXT,
    first_game_at DATETIME,
    steam_account_created DATETIME,
    steam_games_owned INTEGER,
    steam_profile_visible BOOLEAN,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_members (
    id INTEGER PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES teams(id),
    player_id INTEGER NOT NULL REFERENCES players(id),
    role TEXT NOT NULL CHECK(role IN ('core', 'substitute')),
    is_poc BOOLEAN NOT NULL DEFAULT 0,
    joined_at DATETIME NOT NULL,
    left_at DATETIME,
    UNIQUE(team_id, player_id)
);

CREATE TABLE IF NOT EXISTS player_stats (
    id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id),
    pp_score REAL,
    rank_number INTEGER,
    rank_subrank INTEGER,
    scraped_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_player_stats_player_scraped
    ON player_stats(player_id, scraped_at);

CREATE TABLE IF NOT EXISTS player_heroes (
    id INTEGER PRIMARY KEY,
    stats_id INTEGER NOT NULL REFERENCES player_stats(id),
    hero_name TEXT NOT NULL,
    matches_played INTEGER,
    win_rate REAL,
    is_most_played BOOLEAN DEFAULT 0,
    UNIQUE(stats_id, hero_name)
);

CREATE TABLE IF NOT EXISTS player_matches (
    id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id),
    match_id TEXT NOT NULL,
    hero_name TEXT,
    pp_before REAL,
    pp_after REAL,
    pp_change REAL,
    result TEXT,
    match_date DATETIME,
    scraped_at DATETIME NOT NULL,
    UNIQUE(player_id, match_id)
);

CREATE INDEX IF NOT EXISTS idx_player_matches_player_date
    ON player_matches(player_id, match_date);
"""


def init_db(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't exist."""
    conn.executescript(SCHEMA_SQL)


def get_connection(db_path: str = "dsl.db") -> sqlite3.Connection:
    """Open a connection with FK enforcement and row factory."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn
