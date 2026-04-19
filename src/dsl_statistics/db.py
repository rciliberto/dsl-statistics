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
CREATE TABLE IF NOT EXISTS heroes (
    id INTEGER PRIMARY KEY,
    class_name TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

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


def upsert_division(conn: sqlite3.Connection, name: str) -> int:
    """Insert or get existing division. Returns division id."""
    conn.execute("INSERT OR IGNORE INTO divisions (name) VALUES (?)", (name,))
    row = conn.execute("SELECT id FROM divisions WHERE name = ?", (name,)).fetchone()
    conn.commit()
    return row[0]


def upsert_team(conn: sqlite3.Connection, data: dict) -> int:
    """Insert or update team by page_url. Returns team id."""
    conn.execute(
        """INSERT INTO teams (division_id, name, page_url, updated_at)
           VALUES (:division_id, :name, :page_url, CURRENT_TIMESTAMP)
           ON CONFLICT(page_url) DO UPDATE SET
               name = excluded.name,
               division_id = excluded.division_id,
               updated_at = CURRENT_TIMESTAMP""",
        data,
    )
    row = conn.execute(
        "SELECT id FROM teams WHERE page_url = :page_url", data
    ).fetchone()
    conn.commit()
    return row[0]


def upsert_player(conn: sqlite3.Connection, data: dict) -> int:
    """Insert or update player by steam_account_id. Returns player id."""
    conn.execute(
        """INSERT INTO players (display_name, discord_name, discord_id,
               steam_profile_url, steam_account_id, statlocker_url)
           VALUES (:display_name, :discord_name, :discord_id,
               :steam_profile_url, :steam_account_id, :statlocker_url)
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
        "SELECT id FROM players WHERE steam_account_id = :steam_account_id", data
    ).fetchone()
    conn.commit()
    return row[0]


def upsert_team_member(
    conn: sqlite3.Connection,
    team_id: int,
    player_id: int,
    role: str,
    is_poc: bool,
) -> None:
    """Insert or update team membership. Reactivates departed members."""
    conn.execute(
        """INSERT INTO team_members (team_id, player_id, role, is_poc, joined_at)
           VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(team_id, player_id) DO UPDATE SET
               role = excluded.role,
               is_poc = excluded.is_poc,
               left_at = NULL""",
        (team_id, player_id, role, is_poc),
    )
    conn.commit()


def mark_departed_members(
    conn: sqlite3.Connection, team_id: int, current_player_ids: list[int]
) -> None:
    """Set left_at for players no longer on the roster."""
    if not current_player_ids:
        conn.execute(
            "UPDATE team_members SET left_at = CURRENT_TIMESTAMP "
            "WHERE team_id = ? AND left_at IS NULL",
            (team_id,),
        )
    else:
        placeholders = ",".join("?" * len(current_player_ids))
        conn.execute(
            f"UPDATE team_members SET left_at = CURRENT_TIMESTAMP "
            f"WHERE team_id = ? AND left_at IS NULL AND player_id NOT IN ({placeholders})",
            [team_id] + current_player_ids,
        )
    conn.commit()


def insert_player_stats(
    conn: sqlite3.Connection,
    player_id: int,
    pp_score: float | None,
    rank_number: int | None,
    rank_subrank: int | None,
) -> int:
    """Insert a new stats snapshot. Returns stats id."""
    cursor = conn.execute(
        """INSERT INTO player_stats (player_id, pp_score, rank_number, rank_subrank, scraped_at)
           VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)""",
        (player_id, pp_score, rank_number, rank_subrank),
    )
    conn.commit()
    return cursor.lastrowid


def insert_player_heroes(
    conn: sqlite3.Connection, stats_id: int, heroes: list[dict]
) -> None:
    """Insert hero stats for a given stats snapshot."""
    for hero in heroes:
        conn.execute(
            """INSERT OR IGNORE INTO player_heroes
               (stats_id, hero_name, matches_played, win_rate, is_most_played)
               VALUES (?, ?, ?, ?, ?)""",
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
    conn: sqlite3.Connection, player_id: int, match_data: dict
) -> bool:
    """Insert a match record. Returns True if inserted, False if duplicate."""
    try:
        conn.execute(
            """INSERT INTO player_matches
               (player_id, match_id, hero_name, pp_before, pp_after, pp_change,
                result, match_date, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
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
    except sqlite3.IntegrityError:
        return False


def get_latest_stats_time(conn: sqlite3.Connection, player_id: int) -> str | None:
    """Return the most recent scraped_at for a player, or None."""
    row = conn.execute(
        "SELECT MAX(scraped_at) FROM player_stats WHERE player_id = ?",
        (player_id,),
    ).fetchone()
    return row[0] if row else None


def get_known_match_ids(conn: sqlite3.Connection, player_id: int) -> set[str]:
    """Return the set of match IDs already stored for a player."""
    rows = conn.execute(
        "SELECT match_id FROM player_matches WHERE player_id = ?",
        (player_id,),
    ).fetchall()
    return {row[0] for row in rows}


def upsert_heroes(conn: sqlite3.Connection, heroes: list[dict]) -> None:
    """Insert or update hero ID → name mappings."""
    for hero in heroes:
        conn.execute(
            """INSERT INTO heroes (id, class_name, name, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(id) DO UPDATE SET
                   class_name = excluded.class_name,
                   name = excluded.name,
                   updated_at = CURRENT_TIMESTAMP""",
            (hero["id"], hero["class_name"], hero["name"]),
        )
    conn.commit()


def get_hero_id_map(conn: sqlite3.Connection) -> dict[int, str]:
    """Return a mapping of hero ID → display name from the database."""
    rows = conn.execute("SELECT id, name FROM heroes").fetchall()
    return {row[0]: row[1] for row in rows}


def fix_hero_names(conn: sqlite3.Connection) -> int:
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
            "UPDATE player_matches SET hero_name = ? WHERE hero_name = ?",
            (new_name, old_name),
        )
        updated += cur.rowcount
        cur = conn.execute(
            "UPDATE player_heroes SET hero_name = ? WHERE hero_name = ?",
            (new_name, old_name),
        )
        updated += cur.rowcount

    conn.commit()
    return updated
