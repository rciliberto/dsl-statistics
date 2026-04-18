# DSL Tournament Scraper & Analysis Tool — Design Spec

## Overview

A Python tool that scrapes player data from the Deadlock Death Slam (DSL) tournament site and Statlocker.gg, stores it in SQLite, and provides analysis via Jupyter notebooks. Designed for on-demand use with 24h caching, with a future web UI in mind.

## Data Sources

### 1. Tournament Site (`players.deadlockdeathslam.com/teams/`)
- **Auth:** Discord OAuth (interactive login, session cookies saved locally)
- **Data:** Divisions, teams, rosters (player names, roles, Discord/Steam links, statlocker URLs)
- **Framework:** Django

### 2. Statlocker.gg (`statlocker.gg/profile/{steamAccountId}`)
- **Auth:** Public, no login required
- **Data:** PP score (MMR estimate), rank, hero stats, match history (last 100 matches)
- **Framework:** JavaScript SPA — requires browser rendering or network interception
- **Approach:** Intercept XHR/fetch API responses rather than scraping rendered DOM

### 3. Steam Web API
- **Auth:** API key required (free, from `steamcommunity.com/dev/apikey`), stored in `.env` (gitignored)
- **Data:** Account creation date, number of games owned
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
| steam_account_id | TEXT | UNIQUE. Steam32 account ID (numeric string, e.g., "12345678") extracted from statlocker URL path |
| statlocker_url | TEXT | |
| first_game_at | DATETIME | Nullable, backfilled from statlocker during stats scrape |
| steam_account_created | DATETIME | Nullable, from Steam profile — for alt account detection |
| steam_games_owned | INTEGER | Nullable, from Steam profile — for alt account detection |
| steam_profile_visible | BOOLEAN | Whether Steam profile is public (false = private/friends-only) |
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

UNIQUE constraint: `(team_id, player_id)` — one row per player per team. If a player leaves and later rejoins the same team, `left_at` is reset to NULL on the existing row rather than inserting a new row.

### `player_stats` (snapshot model — new row per scrape)
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| player_id | INTEGER FK | → players.id |
| pp_score | REAL | Statlocker's PP (MMR estimate) |
| rank_number | INTEGER | 0-11 (0=unranked/Obscurus, 1=Initiate … 11=Eternus) |
| rank_subrank | INTEGER | 1-6 (6 = star) |
| scraped_at | DATETIME | When this snapshot was taken |

Index: `(player_id, scraped_at)` for efficient "most recent stats" lookups and PP trend queries.

### `player_heroes` (snapshot-scoped — regenerated each scrape, tied to a stats snapshot)
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| stats_id | INTEGER FK | → player_stats.id |
| hero_name | TEXT | |
| matches_played | INTEGER | |
| win_rate | REAL | 0.0 - 1.0 |
| is_most_played | BOOLEAN | Top 3 flag |

UNIQUE constraint: `(stats_id, hero_name)` — prevents duplicate hero entries within a single snapshot.

### `player_matches` (append-only log — deduplicated by match_id, accumulates over time)
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

UNIQUE constraint: `(player_id, match_id)` — same match can appear for different players but not duplicated per player.

Index: `(player_id, match_date)` for efficient chronological PP trend queries.

### Data Lifecycle Notes
- **`player_heroes`** is snapshot-scoped: each scrape creates a new `player_stats` row and a fresh set of `player_heroes` rows tied to it. This captures hero pool changes over time.
- **`player_matches`** is an append-only log: new matches are inserted, existing matches (by `player_id + match_id`) are skipped. This builds a running history across scrapes.

### Rank Reference
| rank_number | Name | Notes |
|-------------|------|-------|
| 0 | Obscurus | Unranked / not enough matches |
| 1 | Initiate | |
| 2 | Seeker | |
| 3 | Alchemist | |
| 4 | Arcanist | |
| 5 | Ritualist | |
| 6 | Emissary | |
| 7 | Archon | Approx. average |
| 8 | Oracle | |
| 9 | Phantom | |
| 10 | Ascendant | |
| 11 | Eternus | |

Subranks: I (1) through V (5), Star (6).

## Scraper Architecture

### Entry Point
`python scrape.py` with CLI flags:
- `--division "Division 2"` — scrape only a specific division
- `--team "Team Name"` — scrape only a specific team (useful for debugging)
- `--force` — ignore 24h cache, re-scrape all statlocker profiles

### Browser
Uses Playwright with **Chromium** for best network interception compatibility with SPAs.

### Flow

```
1. Session Management
   ├─ Check for saved cookies (.cookies.json, gitignored)
   ├─ If missing/expired → open visible Playwright browser
   ├─ User logs into Discord manually
   └─ Save session cookies for reuse

2. Tournament Site Scrape (authenticated Playwright)
   ├─ Navigate to /teams/
   ├─ Parse divisions and team links from page
   └─ For each team page:
       ├─ Extract team name, division
       └─ For each member:
           ├─ Display name
           ├─ Role (core/substitute) + POC flag
           ├─ Discord name/ID
           ├─ Steam profile URL
           └─ Statlocker URL → extract steam_account_id

3. SQLite Upsert
   ├─ Create/update divisions, teams, players
   └─ Update team_members:
       ├─ New members → insert with joined_at = now
       └─ Missing members → set left_at = now

4. Statlocker Scrape (public, Playwright with network interception)
   ├─ For each player with statlocker_url:
   │   ├─ Cache check: skip if most recent player_stats.scraped_at is within 24h (unless --force)
   │   ├─ Navigate to statlocker profile
   │   ├─ Intercept API responses (XHR/fetch) → capture JSON
   │   └─ Extract:
   │       ├─ PP score, rank (number + subrank)
   │       ├─ First game date
   │       ├─ Hero stats (name, matches, win rate)
   │       └─ Match history (last 100): hero, PP change, result, date
   └─ Insert player_stats, player_heroes, player_matches rows
       └─ Skip matches already in DB (by match_id)

5. Steam API Scrape (for players missing account info)
   ├─ Requires STEAM_API_KEY in .env
   ├─ For each player where steam_account_created IS NULL:
   │   ├─ Convert steam_account_id (Steam32) to Steam64 ID
   │   ├─ GetPlayerSummaries → extract communityvisibilitystate + timecreated
   │   ├─ If profile is public (communityvisibilitystate=3):
   │   │   ├─ Store timecreated as steam_account_created
   │   │   └─ GetOwnedGames → store game_count, set steam_profile_visible=true
   │   └─ If profile is private/friends-only:
   │       └─ Set steam_profile_visible=false, leave other fields NULL
   └─ Update players table

6. Summary output
   └─ "Scraped X players across Y teams, Z new stat snapshots"
```

### Rate Limiting
- ~2 second delay between statlocker page loads
- Sequential navigation (no parallel browser contexts needed for ~100-200 players)

### Error Handling
- Individual player failures (statlocker timeout, private profile, missing data) are logged and skipped — the scraper continues with remaining players
- Failures are reported in the summary output: "Scraped X players, Y failed (see log)"
- Structured logging via Python `logging` module to both console and a `scrape.log` file
- If the tournament site redirects unexpectedly during team scraping, log the URL and skip that team

### Cookie Management
- Cookies saved to `.cookies.json` in project root (gitignored)
- On each run, attempt to use saved cookies
- If the tournament site redirects to login, cookies are expired → re-prompt interactive login

## Analysis (Jupyter Notebook)

### `analysis.ipynb` Sections

1. **Division Overview** — per-division: player count, avg PP, median PP, stddev, min/max
2. **Team Rankings** — teams ranked by avg core-player PP within each division, bar chart
3. **Outlier Detection** — teams >1 stddev from division mean PP (potential sandbaggers or overmatched)
4. **Player Distribution** — histogram of PP per division, rank distribution charts
5. **Scouting View** — for a given player/team: hero pool, most-played heroes, PP trend from match history
6. **Division Comparison** — box plot of PP across all divisions to show overlap/gaps
7. **PP Trends** — player PP trajectory over time from match history data
8. **Alt Account Flags** — players with high rank/PP but young Steam accounts, very few games owned, or private Steam profiles

### Libraries
- `sqlite3` or `pandas.read_sql` for queries
- `matplotlib` / `seaborn` for charts
- `numpy` / `scipy` for statistics

## Project Structure

```
DSL Statistics/
├── scrape.py              # Main scraper entry point
├── db.py                  # SQLite schema setup + query helpers
├── scrapers/
│   ├── __init__.py
│   ├── auth.py            # Discord login + cookie management
│   ├── tournament.py      # Tournament site scraper
│   ├── statlocker.py      # Statlocker.gg scraper (network interception)
│   └── steam.py           # Steam Web API client (account age, games owned)
├── analysis.ipynb         # Jupyter notebook for analysis
├── requirements.txt       # playwright, pandas, matplotlib, seaborn, jupyter
├── .env                   # STEAM_API_KEY (gitignored)
├── .cookies.json          # Saved session cookies (gitignored)
├── dsl.db                 # SQLite database (gitignored)
├── scrape.log             # Scraper log file (gitignored)
├── .gitignore
└── docs/
    └── superpowers/
        └── specs/
            └── 2026-04-17-dsl-scraper-design.md
```

## Future Work
- Web UI for querying stats and viewing charts
- Challonge integration for team standings/match results
- Automated scheduling (periodic scrapes)
- Player matchmaking statistics (independent of team membership)

## Technical Decisions
- **Playwright over Selenium:** Better API, built-in network interception, modern async support
- **Network interception over DOM scraping** for statlocker: More robust against UI changes, richer data from raw JSON
- **SQLite over CSV:** Supports relational queries, timestamped snapshots, future web UI
- **Players decoupled from teams:** `team_members` join table with `joined_at`/`left_at` tracks roster changes over time
- **24h cache:** Avoids hammering statlocker on repeated runs; `--force` flag overrides
