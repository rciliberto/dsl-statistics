# DSL Tournament Scraper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Playwright-based CLI scraper that collects Deadlock Death Slam tournament data from three sources (tournament site, statlocker.gg, Steam API) into SQLite, with a Jupyter notebook for analysis.

**Architecture:** `src/dsl_statistics/` package with a `click` CLI entry point (`cli.py`) that orchestrates a database layer (`db.py`) and three scrapers (`scrapers/auth.py`, `scrapers/tournament.py`, `scrapers/statlocker.py`, `scrapers/steam.py`). Data flows: tournament site → divisions/teams/players → statlocker → stats/heroes/matches → Steam API → account metadata. Analysis via Jupyter notebook querying SQLite directly.

**Tech Stack:** Python 3.13+, uv, click, Playwright (Chromium), SQLite, pandas, matplotlib, seaborn, scipy, requests, python-dotenv

**Spec:** `docs/superpowers/specs/2026-04-18-dsl-scraper-design.md`

---

## File Map

| File | Responsibility |
|------|---------------|
| `.gitignore` | Ignore .cookies.json, dsl.db, scrape.log, .env, .venv, __pycache__, .idea |
| `pyproject.toml` | Project metadata, dependencies, CLI entry point, dev deps |
| `src/dsl_statistics/__init__.py` | Package init |
| `src/dsl_statistics/db.py` | Schema creation, upsert helpers, query helpers, RANK_NAMES constant |
| `src/dsl_statistics/cli.py` | Click CLI with all flags, orchestrates scrapers, logging setup |
| `src/dsl_statistics/scrapers/__init__.py` | Scrapers package init |
| `src/dsl_statistics/scrapers/auth.py` | Cookie load/save, interactive Discord login, session validation |
| `src/dsl_statistics/scrapers/tournament.py` | Scrape divisions, teams, rosters from tournament site |
| `src/dsl_statistics/scrapers/statlocker.py` | Scrape PP, rank, heroes, matches via network interception |
| `src/dsl_statistics/scrapers/steam.py` | Steam Web API client for account age and games owned |
| `tests/__init__.py` | Test package init |
| `tests/test_db.py` | Tests for database layer |
| `tests/test_steam.py` | Tests for Steam API client (mocked HTTP) |
| `analysis.ipynb` | Jupyter notebook for statistical analysis |

---

## Task 1: Project Setup

**Files:**
- Create: `.gitignore`
- Modify: `pyproject.toml`
- Create: `src/dsl_statistics/__init__.py`
- Create: `src/dsl_statistics/scrapers/__init__.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create `.gitignore`**

```
# Secrets and session
.env
.cookies.json

# Database
dsl.db

# Logs
scrape.log

# Python
__pycache__/
*.pyc
.venv/

# IDE
.idea/

# Jupyter
.ipynb_checkpoints/
```

- [ ] **Step 2: Update `pyproject.toml`**

Replace the current `pyproject.toml` with:

```toml
[project]
name = "dsl-statistics"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "click>=8.1",
    "playwright>=1.52",
    "requests>=2.32",
    "python-dotenv>=1.1",
    "pandas>=2.2",
    "matplotlib>=3.10",
    "seaborn>=0.13",
    "scipy>=1.15",
]

[project.scripts]
dsl-scrape = "dsl_statistics.cli:main"

[dependency-groups]
dev = [
    "pytest>=8.3",
    "jupyterlab>=4.5",
]
```

- [ ] **Step 3: Create package directories and init files**

Run:
```bash
mkdir -p src/dsl_statistics/scrapers tests
touch src/dsl_statistics/__init__.py src/dsl_statistics/scrapers/__init__.py tests/__init__.py
```

- [ ] **Step 4: Install dependencies and Playwright browser**

Run:
```bash
uv python install 3.13
uv python pin 3.13
uv sync
uv run playwright install chromium
```

- [ ] **Step 5: Verify the CLI entry point resolves**

Run:
```bash
uv run dsl-scrape --help
```

Expected: Error about `main` not existing in `dsl_statistics.cli` (module doesn't exist yet). This confirms the entry point wiring is correct.

- [ ] **Step 6: Commit**

```bash
git add .gitignore pyproject.toml uv.lock src/dsl_statistics/__init__.py src/dsl_statistics/scrapers/__init__.py tests/__init__.py .python-version
git commit -m "chore: project setup with uv, src layout, and dependencies"
```

---

## Task 2: Database Layer — Schema

**Files:**
- Create: `src/dsl_statistics/db.py`
- Create: `tests/test_db.py`

- [ ] **Step 1: Write failing tests for schema creation**

`tests/test_db.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_db.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dsl_statistics.db'`

- [ ] **Step 3: Implement `src/dsl_statistics/db.py` with schema creation**

```python
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
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    division_id INTEGER NOT NULL REFERENCES divisions(id),
    name TEXT NOT NULL,
    page_url TEXT NOT NULL UNIQUE,
    updated_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id INTEGER NOT NULL REFERENCES teams(id),
    player_id INTEGER NOT NULL REFERENCES players(id),
    role TEXT NOT NULL CHECK(role IN ('core', 'substitute')),
    is_poc BOOLEAN NOT NULL DEFAULT 0,
    joined_at DATETIME NOT NULL,
    left_at DATETIME,
    UNIQUE(team_id, player_id)
);

CREATE TABLE IF NOT EXISTS player_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL REFERENCES players(id),
    pp_score REAL,
    rank_number INTEGER,
    rank_subrank INTEGER,
    scraped_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_player_stats_player_scraped
    ON player_stats(player_id, scraped_at);

CREATE TABLE IF NOT EXISTS player_heroes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stats_id INTEGER NOT NULL REFERENCES player_stats(id),
    hero_name TEXT NOT NULL,
    matches_played INTEGER,
    win_rate REAL,
    is_most_played BOOLEAN DEFAULT 0,
    UNIQUE(stats_id, hero_name)
);

CREATE TABLE IF NOT EXISTS player_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_db.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/dsl_statistics/db.py tests/test_db.py
git commit -m "feat: database schema with all 7 tables, indexes, and constraints"
```

---

## Task 3: Database Layer — Upsert Helpers

**Files:**
- Modify: `src/dsl_statistics/db.py`
- Modify: `tests/test_db.py`

- [ ] **Step 1: Write failing tests for upsert helpers**

Add to `tests/test_db.py`:
```python
from dsl_statistics.db import (
    upsert_division,
    upsert_team,
    upsert_player,
    upsert_team_member,
    mark_departed_members,
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
    # First scrape: Alice and Bob
    upsert_team_member(conn, team_id=1, player_id=1, role="core", is_poc=True)
    upsert_team_member(conn, team_id=1, player_id=2, role="core", is_poc=False)
    # Second scrape: only Alice still on roster
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
    # Alice rejoins
    upsert_team_member(conn, team_id=1, player_id=1, role="substitute", is_poc=True)
    row = conn.execute(
        "SELECT left_at, role, is_poc FROM team_members WHERE player_id = 1"
    ).fetchone()
    assert row[0] is None
    assert row[1] == "substitute"
    assert row[2] == 1
```

- [ ] **Step 2: Run tests to verify the new tests fail**

Run: `uv run pytest tests/test_db.py -v`
Expected: FAIL — `ImportError: cannot import name 'upsert_division'`

- [ ] **Step 3: Implement upsert helpers in `src/dsl_statistics/db.py`**

Add to `db.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_db.py -v`
Expected: All 13 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/dsl_statistics/db.py tests/test_db.py
git commit -m "feat: database upsert helpers for divisions, teams, players, and team members"
```

---

## Task 4: Database Layer — Stats & Match Insert Helpers

**Files:**
- Modify: `src/dsl_statistics/db.py`
- Modify: `tests/test_db.py`

- [ ] **Step 1: Write failing tests for stats/match helpers**

Add to `tests/test_db.py`:
```python
from dsl_statistics.db import (
    insert_player_stats,
    insert_player_heroes,
    insert_player_match,
    get_latest_stats_time,
)


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
    insert_player_heroes(conn, stats_id, heroes)  # Should not raise
    rows = conn.execute(
        "SELECT COUNT(*) FROM player_heroes WHERE stats_id = ?", (stats_id,)
    ).fetchone()
    assert rows[0] == 1
```

- [ ] **Step 2: Run tests to verify the new tests fail**

Run: `uv run pytest tests/test_db.py -v`
Expected: FAIL — `ImportError: cannot import name 'insert_player_stats'`

- [ ] **Step 3: Implement stats/match helpers in `src/dsl_statistics/db.py`**

Add to `db.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_db.py -v`
Expected: All 19 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/dsl_statistics/db.py tests/test_db.py
git commit -m "feat: stats, heroes, and match insert helpers with deduplication"
```

---

## Task 5: Steam API Client

**Files:**
- Create: `src/dsl_statistics/scrapers/steam.py`
- Create: `tests/test_steam.py`

- [ ] **Step 1: Write failing tests**

`tests/test_steam.py`:
```python
from unittest.mock import MagicMock, patch

from dsl_statistics.scrapers.steam import (
    fetch_owned_games_count,
    fetch_player_summary,
    fetch_steam_info,
    steam32_to_steam64,
)


def test_steam32_to_steam64():
    # Steam64 = Steam32 + 76561197960265728
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_steam.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement `src/dsl_statistics/scrapers/steam.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_steam.py -v`
Expected: All 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/dsl_statistics/scrapers/steam.py tests/test_steam.py
git commit -m "feat: Steam API client for account age, games owned, and profile visibility"
```

---

## Task 6: Cookie Management & Auth

**Files:**
- Create: `src/dsl_statistics/scrapers/auth.py`

This module manages Playwright session cookies for the Discord-authenticated tournament site. It requires interactive browser login and cannot be unit tested — it is tested manually during integration (Task 9).

- [ ] **Step 1: Implement `src/dsl_statistics/scrapers/auth.py`**

```python
import json
import logging
from pathlib import Path

from playwright.sync_api import BrowserContext, Page

logger = logging.getLogger(__name__)

COOKIES_PATH = Path(".cookies.json")
TOURNAMENT_BASE = "https://players.deadlockdeathslam.com"
LOGIN_URL = f"{TOURNAMENT_BASE}/accounts/discord/login/"


def save_cookies(context: BrowserContext) -> None:
    """Save browser cookies to disk."""
    cookies = context.cookies()
    COOKIES_PATH.write_text(json.dumps(cookies, indent=2))
    logger.info("Saved %d cookies to %s", len(cookies), COOKIES_PATH)


def load_cookies(context: BrowserContext) -> bool:
    """Load cookies from disk. Returns True if cookies were loaded."""
    if not COOKIES_PATH.exists():
        logger.info("No saved cookies found")
        return False
    try:
        cookies = json.loads(COOKIES_PATH.read_text())
        context.add_cookies(cookies)
        logger.info("Loaded %d cookies from %s", len(cookies), COOKIES_PATH)
        return True
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("Failed to load cookies: %s", e)
        return False


def is_logged_in(page: Page) -> bool:
    """Check if the current page is authenticated (not redirected to login)."""
    page.goto(f"{TOURNAMENT_BASE}/teams/", wait_until="domcontentloaded")
    return "/accounts/" not in page.url and "login" not in page.url.lower()


def interactive_login(context: BrowserContext) -> None:
    """Open a visible browser for the user to log in via Discord."""
    page = context.new_page()
    page.goto(LOGIN_URL)
    logger.info("Please log in via Discord in the browser window...")
    # Wait for redirect back to the tournament site after OAuth
    page.wait_for_url(f"{TOURNAMENT_BASE}/**", timeout=300_000)  # 5 min timeout
    save_cookies(context)
    page.close()


def get_authenticated_context(playwright, headless: bool = True) -> BrowserContext:
    """Get a browser context with valid session cookies.

    Falls back to interactive login if cookies are missing or expired.
    """
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context()

    if load_cookies(context):
        page = context.new_page()
        if is_logged_in(page):
            page.close()
            logger.info("Session is valid")
            return context
        page.close()
        logger.info("Saved cookies are expired")

    # Need interactive login — must be visible
    browser.close()
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    interactive_login(context)
    return context
```

- [ ] **Step 2: Commit**

```bash
git add src/dsl_statistics/scrapers/auth.py
git commit -m "feat: Discord auth with cookie persistence and interactive login"
```

---

## Task 7: Tournament Site Scraper

**Files:**
- Create: `src/dsl_statistics/scrapers/tournament.py`

CSS selectors in this file are best-guess placeholders. They will need updating after first manual inspection of the live site HTML during integration testing (Task 9).

- [ ] **Step 1: Implement `src/dsl_statistics/scrapers/tournament.py`**

```python
import re
import logging
from dataclasses import dataclass, field

from playwright.sync_api import Page

logger = logging.getLogger(__name__)

TEAMS_URL = "https://players.deadlockdeathslam.com/teams/"


@dataclass
class PlayerData:
    display_name: str
    role: str  # "core" or "substitute"
    is_poc: bool
    discord_name: str | None = None
    discord_id: str | None = None
    steam_profile_url: str | None = None
    statlocker_url: str | None = None
    steam_account_id: str | None = None


@dataclass
class TeamData:
    name: str
    page_url: str
    division: str
    players: list[PlayerData] = field(default_factory=list)


def extract_steam_account_id(statlocker_url: str | None) -> str | None:
    """Extract Steam32 account ID from statlocker URL.

    Example: https://statlocker.gg/profile/12345678 -> '12345678'
    """
    if not statlocker_url:
        return None
    match = re.search(r"/profile/(\d+)", statlocker_url)
    return match.group(1) if match else None


def scrape_teams_list(page: Page, division_filter: str | None = None) -> list[dict]:
    """Scrape the main /teams/ page to get division names and team links.

    Returns list of {"division": str, "name": str, "url": str}.

    NOTE: CSS selectors below are best-guess placeholders. Update after
    inspecting the live site HTML structure.
    """
    page.goto(TEAMS_URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    teams = []
    content = page.content()
    logger.debug(
        "Teams page loaded, URL: %s, content length: %d", page.url, len(content)
    )

    # Try to find division sections and team links.
    # Common Django patterns: <h2>Division Name</h2> followed by team listings.
    # These selectors need adjustment after first manual inspection.
    divisions = page.query_selector_all(
        "[class*='division'], [class*='Division'], h2, h3"
    )
    logger.info("Found %d potential division headers", len(divisions))

    if not divisions:
        logger.warning(
            "Could not find division headers. Run with --debug and inspect "
            "scrape.log for page content details."
        )

    return teams


def scrape_team_page(page: Page, team_url: str, division: str) -> TeamData:
    """Scrape a single team page for roster information.

    NOTE: CSS selectors below are best-guess placeholders. Update after
    inspecting the live site HTML structure.
    """
    page.goto(team_url, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    team_name_el = page.query_selector(
        "h1, h2, [class*='team-name'], [class*='teamName']"
    )
    team_name = team_name_el.inner_text().strip() if team_name_el else "Unknown Team"

    team = TeamData(name=team_name, page_url=team_url, division=division)

    player_elements = page.query_selector_all(
        "[class*='member'], [class*='player'], [class*='roster'] tr, [class*='roster'] li"
    )
    logger.info("Team '%s': found %d player elements", team_name, len(player_elements))

    for el in player_elements:
        try:
            player = _parse_player_element(el)
            if player:
                team.players.append(player)
        except Exception as e:
            logger.warning(
                "Failed to parse player element in team '%s': %s", team_name, e
            )

    return team


def _parse_player_element(el) -> PlayerData | None:
    """Parse a single player element from a team page.

    NOTE: Selectors below are best-guess placeholders. Update after
    inspecting the live site HTML structure.
    """
    name_el = el.query_selector("[class*='name'], a, td:first-child")
    if not name_el:
        return None
    display_name = name_el.inner_text().strip()
    if not display_name:
        return None

    text = el.inner_text().lower()
    role = "substitute" if "sub" in text or "substitute" in text else "core"
    is_poc = "poc" in text or "point of contact" in text or "captain" in text

    links = el.query_selector_all("a[href]")
    steam_url = None
    statlocker_url = None
    for link in links:
        href = link.get_attribute("href") or ""
        if "steamcommunity.com" in href or "store.steampowered.com" in href:
            steam_url = href
        elif "statlocker.gg" in href:
            statlocker_url = href

    steam_account_id = extract_steam_account_id(statlocker_url)

    return PlayerData(
        display_name=display_name,
        role=role,
        is_poc=is_poc,
        steam_profile_url=steam_url,
        statlocker_url=statlocker_url,
        steam_account_id=steam_account_id,
    )
```

- [ ] **Step 2: Commit**

```bash
git add src/dsl_statistics/scrapers/tournament.py
git commit -m "feat: tournament site scraper with placeholder selectors"
```

---

## Task 8: Statlocker Scraper

**Files:**
- Create: `src/dsl_statistics/scrapers/statlocker.py`

Uses Playwright network interception to capture API responses from statlocker.gg's SPA. Field names in the API response parser may need updating after inspecting actual responses during integration testing (Task 9).

- [ ] **Step 1: Implement `src/dsl_statistics/scrapers/statlocker.py`**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add src/dsl_statistics/scrapers/statlocker.py
git commit -m "feat: statlocker scraper with network interception for PP, rank, heroes, matches"
```

---

## Task 9: CLI Entry Point

**Files:**
- Create: `src/dsl_statistics/cli.py`

- [ ] **Step 1: Implement `src/dsl_statistics/cli.py`**

```python
"""DSL Tournament Scraper — CLI entry point."""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import click
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from dsl_statistics.db import (
    get_connection,
    get_latest_stats_time,
    init_db,
    insert_player_heroes,
    insert_player_match,
    insert_player_stats,
    mark_departed_members,
    upsert_division,
    upsert_player,
    upsert_team,
    upsert_team_member,
)
from dsl_statistics.scrapers.auth import get_authenticated_context
from dsl_statistics.scrapers.statlocker import scrape_player_stats
from dsl_statistics.scrapers.steam import fetch_steam_info
from dsl_statistics.scrapers.tournament import scrape_team_page, scrape_teams_list

load_dotenv()

CACHE_HOURS = 24


def setup_logging(debug: bool = False) -> None:
    level = logging.DEBUG if debug else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("scrape.log", encoding="utf-8"),
        ],
    )


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
        logger.info(
            "Team '%s' (%s): %d players",
            team_data.name,
            team_data.division,
            len(current_player_ids),
        )

    return scraped_players


def scrape_statlocker_all(page, conn, players, force=False):
    """Scrape statlocker for all players."""
    logger = logging.getLogger("dsl.statlocker")
    stats_count = 0
    fail_count = 0

    for p in players:
        if not p["statlocker_url"]:
            logger.warning("Player '%s' has no statlocker URL", p["display_name"])
            continue

        if not force and is_cache_fresh(conn, p["player_id"]):
            logger.debug("Skipping '%s' (cached)", p["display_name"])
            continue

        try:
            data = scrape_player_stats(page, p["steam_account_id"])

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
                    "UPDATE players SET first_game_at = ? WHERE id = ? AND first_game_at IS NULL",
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
                "SELECT steam_profile_visible FROM players WHERE id = ?",
                (p["player_id"],),
            ).fetchone()
            if row and row[0] is not None:
                continue

        try:
            info = fetch_steam_info(api_key, p["steam_account_id"])
            conn.execute(
                """UPDATE players SET
                       steam_profile_visible = ?,
                       steam_account_created = ?,
                       steam_games_owned = ?
                   WHERE id = ?""",
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
@click.option("--force", is_flag=True, help="Ignore 24h cache")
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.option("--skip-statlocker", is_flag=True, help="Skip statlocker scrape")
@click.option("--skip-steam", is_flag=True, help="Skip Steam API calls")
@click.option(
    "--refresh-steam", is_flag=True, help="Re-fetch Steam data for all players"
)
def main(division, team, force, debug, skip_statlocker, skip_steam, refresh_steam):
    """DSL Tournament Scraper — collect player data from tournament site, statlocker, and Steam."""
    setup_logging(debug=debug)
    logger = logging.getLogger("dsl")

    conn = get_connection()
    init_db(conn)

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
            logger.info("Scraping statlocker profiles...")
            browser = p.chromium.launch(headless=True)
            sl_context = browser.new_context()
            sl_page = sl_context.new_page()

            stats_count, fail_count = scrape_statlocker_all(
                sl_page, conn, players, force=force
            )

            sl_page.close()
            sl_context.close()
            browser.close()

            logger.info(
                "Statlocker: %d scraped, %d failed", stats_count, fail_count
            )

    # Steam API (no browser needed)
    if not skip_steam:
        logger.info("Fetching Steam account info...")
        steam_count = scrape_steam_all(conn, players, refresh=refresh_steam)
        logger.info("Steam: %d profiles updated", steam_count)

    conn.close()
    logger.info("Done!")
```

- [ ] **Step 2: Verify the CLI entry point resolves**

Run: `uv run dsl-scrape --help`
Expected: Click help output showing all flags.

- [ ] **Step 3: Commit**

```bash
git add src/dsl_statistics/cli.py
git commit -m "feat: click CLI entry point orchestrating all scrapers"
```

---

## Task 10: Integration Test & Selector Refinement

**Files:**
- Modify: `src/dsl_statistics/scrapers/tournament.py` (update CSS selectors)
- Modify: `src/dsl_statistics/scrapers/statlocker.py` (update API field names)

This task requires running against the live sites. It cannot be automated.

- [ ] **Step 1: Test tournament site login**

Run: `uv run dsl-scrape --skip-statlocker --skip-steam --debug`

1. A browser window should open
2. Log in via Discord
3. Cookies should be saved to `.cookies.json`
4. Check `scrape.log` for HTML structure info

- [ ] **Step 2: Inspect tournament site HTML**

After login, use browser DevTools (or the debug log output) to inspect:
- How divisions are listed on `/teams/`
- How team links are structured
- How player rows are formatted on team pages
- Where role (core/sub), POC, Discord name, Steam link, and statlocker link appear

Update the CSS selectors in `src/dsl_statistics/scrapers/tournament.py`:
- `scrape_teams_list()` — update division header and team link selectors
- `_parse_player_element()` — update player name, role, link selectors

- [ ] **Step 3: Test tournament scrape with updated selectors**

Run: `uv run dsl-scrape --team "YOUR_TEAM" --skip-statlocker --skip-steam --debug`

Verify in the database:
```bash
sqlite3 dsl.db "SELECT * FROM divisions;"
sqlite3 dsl.db "SELECT * FROM teams;"
sqlite3 dsl.db "SELECT display_name, steam_account_id FROM players;"
sqlite3 dsl.db "SELECT * FROM team_members;"
```

- [ ] **Step 4: Test statlocker scrape**

Run: `uv run dsl-scrape --team "YOUR_TEAM" --skip-steam --debug`

Check debug log for captured API responses. Update field names in `src/dsl_statistics/scrapers/statlocker.py` `_parse_api_response()` if needed.

Verify:
```bash
sqlite3 dsl.db "SELECT p.display_name, ps.pp_score, ps.rank_number, ps.rank_subrank FROM players p JOIN player_stats ps ON p.id = ps.player_id;"
sqlite3 dsl.db "SELECT COUNT(*) FROM player_heroes;"
sqlite3 dsl.db "SELECT COUNT(*) FROM player_matches;"
```

- [ ] **Step 5: Test Steam API**

Create `.env`:
```
STEAM_API_KEY=your_key_here
```

Run: `uv run dsl-scrape --team "YOUR_TEAM" --skip-statlocker --debug`

Verify:
```bash
sqlite3 dsl.db "SELECT display_name, steam_profile_visible, steam_games_owned, steam_account_created FROM players;"
```

- [ ] **Step 6: Test --refresh-steam**

Run: `uv run dsl-scrape --team "YOUR_TEAM" --skip-statlocker --refresh-steam --debug`

Verify that Steam data is re-fetched for all players (not just those missing data).

- [ ] **Step 7: Full scrape test**

Run: `uv run dsl-scrape --division "Division 2" --debug`

Verify all data populated. Check `scrape.log` for errors.

- [ ] **Step 8: Commit selector fixes**

```bash
git add src/dsl_statistics/scrapers/tournament.py src/dsl_statistics/scrapers/statlocker.py
git commit -m "fix: update CSS selectors and API field names from live site inspection"
```

---

## Task 11: Analysis Notebook

**Files:**
- Create: `analysis.ipynb`

- [ ] **Step 1: Create notebook with all analysis sections**

Create `analysis.ipynb` as a Jupyter notebook with these cells:

**Cell 1 — Setup:**
```python
import sqlite3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import numpy as np

sns.set_theme(style="whitegrid")

conn = sqlite3.connect("dsl.db")

from dsl_statistics.db import RANK_NAMES
```

**Cell 2 — Load base data:**
```python
# Get latest stats per player (most recent snapshot)
players_df = pd.read_sql("""
    SELECT
        p.id as player_id, p.display_name, p.steam_account_id,
        p.first_game_at, p.steam_account_created, p.steam_games_owned,
        p.steam_profile_visible,
        d.name as division, t.name as team,
        tm.role, tm.is_poc,
        ps.pp_score, ps.rank_number, ps.rank_subrank, ps.scraped_at
    FROM players p
    JOIN team_members tm ON p.id = tm.player_id AND tm.left_at IS NULL
    JOIN teams t ON tm.team_id = t.id
    JOIN divisions d ON t.division_id = d.id
    LEFT JOIN player_stats ps ON p.id = ps.player_id
        AND ps.scraped_at = (
            SELECT MAX(ps2.scraped_at) FROM player_stats ps2 WHERE ps2.player_id = p.id
        )
    ORDER BY d.name, t.name, p.display_name
""", conn)

players_df["rank_label"] = players_df["rank_number"].map(RANK_NAMES)
print(f"Loaded {len(players_df)} active players across {players_df['division'].nunique()} divisions")
players_df.head()
```

**Cell 3 — Division Overview:**
```python
div_stats = players_df.groupby("division")["pp_score"].agg(
    ["count", "mean", "median", "std", "min", "max"]
).round(1)
div_stats.columns = ["Players", "Avg PP", "Median PP", "Std Dev", "Min PP", "Max PP"]
print("=== Division Overview ===")
div_stats
```

**Cell 4 — Team Rankings:**
```python
# Average PP of core players per team
core_players = players_df[players_df["role"] == "core"]
team_rankings = core_players.groupby(["division", "team"])["pp_score"].agg(
    ["mean", "count"]
).round(1)
team_rankings.columns = ["Avg Core PP", "Core Players"]
team_rankings = team_rankings.sort_values(["Avg Core PP"], ascending=False)

for div in sorted(players_df["division"].unique()):
    div_teams = team_rankings.loc[div].sort_values("Avg Core PP", ascending=True)
    fig, ax = plt.subplots(figsize=(10, max(4, len(div_teams) * 0.4)))
    div_teams["Avg Core PP"].plot(
        kind="barh", ax=ax, color=sns.color_palette("viridis", len(div_teams))
    )
    ax.set_title(f"{div} — Team Rankings by Avg Core PP")
    ax.set_xlabel("Average PP (Core Players)")
    plt.tight_layout()
    plt.show()
```

**Cell 5 — Outlier Detection:**
```python
team_pp = core_players.groupby(["division", "team"])["pp_score"].mean()

print("=== Outlier Teams (>1 std dev from division mean) ===\n")
for div in sorted(players_df["division"].unique()):
    div_team_pp = team_pp[div]
    mean = div_team_pp.mean()
    std = div_team_pp.std()
    outliers = div_team_pp[(div_team_pp > mean + std) | (div_team_pp < mean - std)]
    if len(outliers) > 0:
        print(f"{div} (mean={mean:.1f}, std={std:.1f}):")
        for team_name, pp in outliers.items():
            direction = "ABOVE" if pp > mean else "BELOW"
            print(f"  {team_name}: {pp:.1f} ({direction}, {abs(pp - mean)/std:.1f}σ)")
        print()
```

**Cell 6 — Player Distribution:**
```python
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# PP histogram per division
for div in sorted(players_df["division"].unique()):
    div_data = players_df[players_df["division"] == div]["pp_score"].dropna()
    axes[0].hist(div_data, bins=20, alpha=0.5, label=div)
axes[0].set_title("PP Distribution by Division")
axes[0].set_xlabel("PP Score")
axes[0].set_ylabel("Count")
axes[0].legend()

# Rank distribution
rank_counts = players_df["rank_label"].value_counts()
rank_order = [RANK_NAMES[i] for i in range(12) if RANK_NAMES[i] in rank_counts.index]
rank_counts = rank_counts.reindex(rank_order)
rank_counts.plot(
    kind="bar", ax=axes[1], color=sns.color_palette("coolwarm", len(rank_counts))
)
axes[1].set_title("Rank Distribution (All Divisions)")
axes[1].set_xlabel("Rank")
axes[1].set_ylabel("Count")
axes[1].tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.show()
```

**Cell 7 — Scouting View:**
```python
def scout_player(player_name: str):
    """Display scouting info for a player."""
    player = players_df[
        players_df["display_name"].str.contains(player_name, case=False)
    ]
    if player.empty:
        print(f"Player '{player_name}' not found")
        return

    p = player.iloc[0]
    print(f"=== {p['display_name']} ===")
    print(f"Team: {p['team']} ({p['division']})")
    print(f"Role: {p['role']}{'  [POC]' if p['is_poc'] else ''}")
    print(f"PP: {p['pp_score']:.1f}")
    print(f"Rank: {p['rank_label']} {p['rank_subrank']}")
    print(f"Steam Account Age: {p['steam_account_created'] or 'Unknown'}")
    print(f"Games Owned: {p['steam_games_owned'] or 'Unknown'}")
    print()

    # Top heroes
    heroes = pd.read_sql("""
        SELECT ph.hero_name, ph.matches_played, ph.win_rate
        FROM player_heroes ph
        JOIN player_stats ps ON ph.stats_id = ps.id
        WHERE ps.player_id = ?
        AND ps.scraped_at = (SELECT MAX(scraped_at) FROM player_stats WHERE player_id = ?)
        ORDER BY ph.matches_played DESC
        LIMIT 10
    """, conn, params=(int(p["player_id"]), int(p["player_id"])))

    if not heroes.empty:
        print("Top Heroes:")
        for _, h in heroes.iterrows():
            print(
                f"  {h['hero_name']}: {h['matches_played']} games, "
                f"{h['win_rate']*100:.0f}% WR"
            )

    # PP trend from match history
    matches = pd.read_sql("""
        SELECT match_date, pp_after, pp_change, hero_name, result
        FROM player_matches
        WHERE player_id = ?
        ORDER BY match_date DESC
        LIMIT 20
    """, conn, params=(int(p["player_id"]),))

    if not matches.empty and matches["pp_after"].notna().any():
        fig, ax = plt.subplots(figsize=(10, 4))
        matches_sorted = matches.sort_values("match_date")
        colors = [
            "green" if r == "win" else "red" for r in matches_sorted["result"]
        ]
        ax.plot(
            range(len(matches_sorted)),
            matches_sorted["pp_after"],
            marker="o",
            color="steelblue",
        )
        ax.scatter(
            range(len(matches_sorted)),
            matches_sorted["pp_after"],
            c=colors,
            zorder=5,
        )
        ax.set_title(f"{p['display_name']} — Recent PP Trend")
        ax.set_xlabel("Match")
        ax.set_ylabel("PP")
        plt.tight_layout()
        plt.show()


# Example usage:
# scout_player("YourName")
```

**Cell 8 — Division Comparison:**
```python
fig, ax = plt.subplots(figsize=(10, 5))
div_order = sorted(players_df["division"].unique())
data = [
    players_df[players_df["division"] == d]["pp_score"].dropna() for d in div_order
]
ax.boxplot(data, labels=div_order)
ax.set_title("PP Distribution Across Divisions")
ax.set_ylabel("PP Score")
plt.tight_layout()
plt.show()
```

**Cell 9 — PP Trends:**
```python
# PP trajectory from match history for top players in a division
my_division = "Division 2"  # Change as needed

div_players = players_df[players_df["division"] == my_division].nlargest(
    5, "pp_score"
)

fig, ax = plt.subplots(figsize=(12, 5))
for _, p in div_players.iterrows():
    matches = pd.read_sql("""
        SELECT match_date, pp_after FROM player_matches
        WHERE player_id = ? AND pp_after IS NOT NULL
        ORDER BY match_date
    """, conn, params=(int(p["player_id"]),))
    if not matches.empty:
        ax.plot(
            range(len(matches)),
            matches["pp_after"],
            label=p["display_name"],
            marker=".",
        )

ax.set_title(f"PP Trends — Top 5 Players in {my_division}")
ax.set_xlabel("Match Index")
ax.set_ylabel("PP")
ax.legend(loc="best")
plt.tight_layout()
plt.show()
```

**Cell 10 — Alt Account Flags:**
```python
alt_flags = players_df[
    (players_df["pp_score"].notna())
    & (
        (players_df["steam_profile_visible"] == False)
        | (
            players_df["steam_games_owned"].notna()
            & (players_df["steam_games_owned"] < 10)
        )
        | (
            players_df["steam_account_created"].notna()
            & (
                pd.to_datetime(players_df["steam_account_created"])
                > pd.Timestamp("2024-06-01")
            )
        )
    )
].sort_values("pp_score", ascending=False)

print(f"=== Potential Alt Accounts ({len(alt_flags)} flagged) ===\n")
for _, p in alt_flags.iterrows():
    flags = []
    if p["steam_profile_visible"] == False:
        flags.append("PRIVATE PROFILE")
    if pd.notna(p["steam_games_owned"]) and p["steam_games_owned"] < 10:
        flags.append(f"ONLY {int(p['steam_games_owned'])} GAMES")
    if pd.notna(p["steam_account_created"]):
        created = pd.to_datetime(p["steam_account_created"])
        if created > pd.Timestamp("2024-06-01"):
            flags.append(f"ACCOUNT CREATED {created.strftime('%Y-%m')}")

    print(f"  {p['display_name']} ({p['team']}, {p['division']})")
    print(
        f"    PP: {p['pp_score']:.1f} | Rank: {p['rank_label']} | "
        f"Flags: {', '.join(flags)}"
    )
```

**Cell 11 — Cleanup:**
```python
conn.close()
```

- [ ] **Step 2: Verify notebook opens**

Run: `uv run jupyter lab analysis.ipynb`

Verify the notebook opens and cells are present. Full execution requires data in `dsl.db` from Task 10.

- [ ] **Step 3: Commit**

```bash
git add analysis.ipynb
git commit -m "feat: analysis notebook with division stats, rankings, scouting, and alt detection"
```

---

## Task 12: Final Verification

- [ ] **Step 1: Run all unit tests**

Run: `uv run pytest -v`
Expected: All tests pass.

- [ ] **Step 2: Run full pipeline end-to-end**

Run: `uv run dsl-scrape --debug`

Verify: no errors in `scrape.log`, all tables populated.

- [ ] **Step 3: Verify cache works**

Run: `uv run dsl-scrape`

Second run should skip statlocker scrapes (cached within 24h). Check log for "Skipping (cached)" messages.

- [ ] **Step 4: Verify --force flag**

Run: `uv run dsl-scrape --division "Division 2" --force`

Should re-scrape all statlocker profiles regardless of cache.

- [ ] **Step 5: Open and run analysis notebook**

Run: `uv run jupyter lab analysis.ipynb`

Run all cells, verify charts render and data looks correct.

- [ ] **Step 6: Final commit**

```bash
git add -A
git commit -m "chore: final cleanup and verification"
```