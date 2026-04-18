# DSL Tournament Scraper & Analysis Tool — Design Spec

## Overview

A Python CLI tool that scrapes player data from the Deadlock Death Slam (DSL) tournament site, Statlocker.gg, and the Steam Web API, stores it in SQLite, and provides analysis via a Jupyter notebook. Designed for on-demand use with 24h caching.

**Tech stack:** Python 3.13+, uv, click, Playwright (Chromium), SQLite, pandas, matplotlib, seaborn, scipy

## Project Structure

```
dsl-statistics/
├── src/
│   └── dsl_statistics/
│       ├── __init__.py
│       ├── cli.py              # CLI entry point (click)
│       ├── db.py               # SQLite schema + upsert/query helpers
│       └── scrapers/
│           ├── __init__.py
│           ├── auth.py         # Discord OAuth + cookie management
│           ├── tournament.py   # Tournament site scraper
│           ├── statlocker.py   # Statlocker.gg scraper (network interception)
│           └── steam.py        # Steam Web API client
├── tests/
│   ├── __init__.py
│   ├── test_db.py
│   └── test_steam.py
├── analysis.ipynb              # Jupyter notebook for analysis
├── pyproject.toml              # Metadata, deps, CLI entry point
├── uv.lock
├── .env                        # STEAM_API_KEY (gitignored)
├── .cookies.json               # Session cookies (gitignored)
├── dsl.db                      # SQLite database (gitignored)
├── scrape.log                  # Log file (gitignored)
├── .gitignore
└── docs/
    └── superpowers/
        ├── specs/
        └── plans/
```

### Tooling

**Package manager:** uv — all dependencies managed via `pyproject.toml` and `uv.lock`. No `requirements.txt`.

**Entry point** defined in `pyproject.toml`:
```toml
[project.scripts]
dsl-scrape = "dsl_statistics.cli:main"
```

**Dependencies:**
- Runtime: `click`, `playwright`, `requests`, `python-dotenv`, `pandas`, `matplotlib`, `seaborn`, `scipy`
- Dev: `pytest`, `jupyterlab`

**Commands:**
- `uv run dsl-scrape` — run the scraper
- `uv run pytest` — run tests
- `uv run jupyter lab` — open analysis notebook

## Data Sources

### 1. Tournament Site (`players.deadlockdeathslam.com/teams/`)
- **Auth:** Discord OAuth (interactive login, session cookies saved locally)
- **Data:** Divisions, teams, rosters (player names, roles, Discord/Steam links, statlocker URLs)
- **Framework:** Django
- **Approach:** Playwright with Chromium, authenticated via saved cookies

### 2. Statlocker.gg (`statlocker.gg/profile/{steamAccountId}`)
- **Auth:** Public, no login required
- **Data:** PP score (MMR estimate), rank, hero stats, match history (last 100 matches)
- **Framework:** JavaScript SPA — requires browser rendering or network interception
- **Approach:** Intercept XHR/fetch API responses via Playwright rather than scraping rendered DOM

### 3. Steam Web API
- **Auth:** API key required (free, from `steamcommunity.com/dev/apikey`), stored in `.env` (gitignored)
- **Data:** Account creation date, number of games owned, profile visibility
- **Endpoints:** `GetPlayerSummaries` (account age via `timecreated`), `GetOwnedGames` (game count)
- **Purpose:** Alt account detection — new accounts with few games but high rank are suspicious
- **Approach:** Simple HTTP requests via `requests` library (no browser needed)

## Database Schema (SQLite)

### `divisions`
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| name | TEXT | UNIQUE. e.g., "Premier", "Division 1" |
| created_at | DATETIME | DEFAULT CURRENT_TIMESTAMP |

### `teams`
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| division_id | INTEGER FK | → divisions.id |
| name | TEXT | |
| page_url | TEXT | UNIQUE. Full URL to team page on tournament site |
| updated_at | DATETIME | Last time this team's roster was scraped |
| created_at | DATETIME | DEFAULT CURRENT_TIMESTAMP |

### `players`
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| display_name | TEXT | Name shown on tournament site |
| discord_name | TEXT | |
| discord_id | TEXT | Nullable |
| steam_profile_url | TEXT | |
| steam_account_id | TEXT | UNIQUE. Steam32 account ID (numeric string) extracted from statlocker URL |
| statlocker_url | TEXT | |
| first_game_at | DATETIME | Nullable, backfilled from statlocker during stats scrape |
| steam_account_created | DATETIME | Nullable, from Steam API |
| steam_games_owned | INTEGER | Nullable, from Steam API |
| steam_profile_visible | BOOLEAN | Whether Steam profile is public |
| created_at | DATETIME | DEFAULT CURRENT_TIMESTAMP |

### `team_members`
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| team_id | INTEGER FK | → teams.id |
| player_id | INTEGER FK | → players.id |
| role | TEXT | CHECK(role IN ('core', 'substitute')) |
| is_poc | BOOLEAN | Point of Contact (orthogonal to role) |
| joined_at | DATETIME | First seen on roster |
| left_at | DATETIME | Nullable — null means still active |

UNIQUE constraint: `(team_id, player_id)` — one row per player per team. If a player leaves and rejoins the same team, `left_at` is reset to NULL on the existing row.

### `player_stats` (snapshot model — new row per scrape)
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| player_id | INTEGER FK | → players.id |
| pp_score | REAL | Statlocker's PP (MMR estimate) |
| rank_number | INTEGER | 0–11 (see rank reference) |
| rank_subrank | INTEGER | 1–6 (6 = star) |
| scraped_at | DATETIME | When this snapshot was taken |

Index: `(player_id, scraped_at)`

### `player_heroes` (snapshot-scoped — regenerated each scrape)
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| stats_id | INTEGER FK | → player_stats.id |
| hero_name | TEXT | |
| matches_played | INTEGER | |
| win_rate | REAL | 0.0–1.0 |
| is_most_played | BOOLEAN | Top 3 flag |

UNIQUE constraint: `(stats_id, hero_name)`

### `player_matches` (append-only — deduplicated by match_id)
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| player_id | INTEGER FK | → players.id |
| match_id | TEXT | From statlocker/Valve |
| hero_name | TEXT | |
| pp_before | REAL | |
| pp_after | REAL | |
| pp_change | REAL | |
| result | TEXT | "win" / "loss" |
| match_date | DATETIME | |
| scraped_at | DATETIME | |

UNIQUE constraint: `(player_id, match_id)`. Index: `(player_id, match_date)`.

### Data Lifecycle
- **`player_heroes`** is snapshot-scoped: each scrape creates a new `player_stats` row and a fresh set of `player_heroes` rows tied to it via `stats_id`. This captures hero pool changes over time.
- **`player_matches`** is append-only: new matches are inserted, existing matches (by `player_id + match_id`) are skipped. This builds a running history across scrapes.

### Rank Reference
| # | Name | | # | Name |
|---|------|-|---|------|
| 0 | Obscurus (unranked) | | 6 | Emissary |
| 1 | Initiate | | 7 | Archon |
| 2 | Seeker | | 8 | Oracle |
| 3 | Alchemist | | 9 | Phantom |
| 4 | Arcanist | | 10 | Ascendant |
| 5 | Ritualist | | 11 | Eternus |

Subranks: I (1) through V (5), Star (6).

## CLI Interface

Entry point: `uv run dsl-scrape`

### Flags
| Flag | Description |
|------|-------------|
| `--division TEXT` | Scrape only a specific division |
| `--team TEXT` | Scrape only a specific team |
| `--force` | Ignore 24h cache, re-scrape all statlocker profiles |
| `--debug` | Enable debug logging |
| `--skip-statlocker` | Skip statlocker scrape |
| `--skip-steam` | Skip Steam API calls (takes precedence over `--refresh-steam`) |
| `--refresh-steam` | Re-fetch Steam data for all players (not just those missing it) |

### Orchestration Flow

```
1. Init DB (create tables if needed)

2. Session Management
   ├─ Check for saved cookies (.cookies.json)
   ├─ If missing/expired → open visible browser for Discord login
   └─ Save session cookies for reuse

3. Tournament Site Scrape (authenticated Playwright)
   ├─ Navigate to /teams/
   ├─ Parse divisions and team links
   └─ For each team page:
       ├─ Extract team name, division
       └─ For each member:
           ├─ Display name, role (core/sub), POC flag
           ├─ Discord name/ID, Steam profile URL
           └─ Statlocker URL → extract steam_account_id
   └─ Upsert divisions, teams, players, team_members
       ├─ New members → insert with joined_at = now
       └─ Missing members → set left_at = now

4. Statlocker Scrape (unless --skip-statlocker)
   ├─ Headless Playwright with network interception
   ├─ For each player with statlocker_url:
   │   ├─ Cache check: skip if scraped within 24h (unless --force)
   │   ├─ Navigate to profile, intercept API responses
   │   └─ Extract: PP, rank, first game date, hero stats, match history
   └─ Insert player_stats, player_heroes, player_matches
       └─ Deduplicate matches by match_id

5. Steam API (unless --skip-steam)
   ├─ Requires STEAM_API_KEY in .env
   ├─ Default: fetch for players where steam_account_created IS NULL
   ├─ With --refresh-steam: re-fetch for all players
   │   └─ Overwrites steam_games_owned, steam_account_created, steam_profile_visible
   ├─ Convert steam_account_id (Steam32) → Steam64 ID
   ├─ GetPlayerSummaries → visibility + timecreated
   └─ If public: GetOwnedGames → game count

6. Summary
   └─ "Scraped X players across Y teams, Z new stat snapshots, W failed (see log)"
```

### Rate Limiting
- ~2 second delay between statlocker page loads
- Sequential navigation (no parallel browser contexts needed for ~100–200 players)

### Error Handling
- Individual player failures (statlocker timeout, private profile, missing data) are logged and skipped
- Failures reported in summary output
- Structured logging via Python `logging` to both console and `scrape.log`
- Unexpected tournament site redirects during team scraping: log and skip that team

### Cookie Management
- Cookies saved to `.cookies.json` in project root (gitignored)
- Each run attempts saved cookies first
- If tournament site redirects to login → cookies expired → open visible browser for re-login

## Analysis (Jupyter Notebook)

### `analysis.ipynb` Sections

1. **Division Overview** — per-division: player count, avg/median/stddev/min/max PP
2. **Team Rankings** — teams ranked by avg core-player PP within each division, horizontal bar chart
3. **Outlier Detection** — teams >1 stddev from division mean PP (potential sandbaggers or overmatched)
4. **Player Distribution** — PP histogram per division, rank distribution bar chart
5. **Scouting View** — callable `scout_player("name")`: hero pool, top heroes, PP trend from match history
6. **Division Comparison** — box plot of PP across all divisions showing overlap/gaps
7. **PP Trends** — PP trajectory over time for top N players in a division
8. **Alt Account Flags** — high rank/PP but: private Steam profile, <10 games owned, or account created after June 2024

### Libraries
- `pandas` (with `read_sql`) for queries
- `matplotlib` / `seaborn` for charts
- `scipy` for statistics

## Technical Decisions
- **uv over pip:** Reproducible lockfile, fast installs, manages Python versions
- **click over argparse:** Cleaner decorator-based CLI definition
- **src layout:** Proper Python packaging, avoids import ambiguity, CLI entry point via `pyproject.toml`
- **Playwright over Selenium:** Better API, built-in network interception, modern async support
- **Network interception over DOM scraping** for statlocker: More robust against UI changes, richer data from raw JSON
- **SQLite over CSV:** Relational queries, timestamped snapshots, future extensibility
- **Players decoupled from teams:** `team_members` join table with `joined_at`/`left_at` tracks roster changes
- **24h cache:** Avoids hammering statlocker on repeated runs; `--force` overrides

## Future Work
- Dedicated database (PostgreSQL) if the tool grows
- Web UI for querying stats and viewing charts
- Challonge integration for team standings/match results
- Automated scheduling (periodic scrapes)
- Player matchmaking statistics (independent of team membership)
