# Charlton Season Tracker

A small pipeline for loading football-data.org fixtures/results into DuckDB, transforming them with dbt, and serving a Streamlit dashboard focused on Charlton Athletic.

## What’s inside
- **Ingest** (`ingest/load_raw.py`): pulls teams and matches from football-data.org for a competition/season and stores them in `warehouse/charlton.duckdb` (`raw_teams`, `raw_matches`).
- **dbt project** (`charlton_dbt/`): builds staging, fact, and mart models (league table, Charlton position through time, match list).
- **Dashboard** (`app.py`): Streamlit UI that reads the marts (read-only DuckDB connection).

## Prerequisites
- Python 3.9+
- A football-data.org API token
- DuckDB CLI (optional, for ad-hoc inspection)
- dbt-duckdb (for `dbt run`) and Streamlit/Pandas/Altair for the app

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# App + dbt deps (if not already installed globally)
pip install streamlit pandas altair dbt-duckdb
```

## Configure environment
Copy `.env.example` to `.env` and fill in your token:
```
FOOTBALL_DATA_TOKEN=your_token
COMP_CODE=ELC
SEASON=2025
TEAM_NAME=Charlton Athletic FC
DUCKDB_PATH=warehouse/charlton.duckdb
```
> The dbt project also pins `team_id: 348` in `charlton_dbt/dbt_project.yml`; keep DUCKDB_PATH consistent with that file.

## Ingest raw data
From repo root:
```bash
python -m ingest.load_raw            # or: python ingest/load_raw.py
# optional: add --full-refresh to truncate raw tables before load
```
This creates/updates `warehouse/charlton.duckdb` with raw tables and an `ingest_runs` log.

## Run dbt transforms
From `charlton_dbt/`:
```bash
dbt run
dbt test            # optional
```
Key models:
- `fct_team_match`: one row per team per match (finished matches only)
- `fct_standings_matchday`: cumulative standings per matchday with ranking
- `mart_league_table_current`: latest matchday league table
- `mart_team_position_through_time`: Charlton’s position per matchday
- `mart_team_last_5`: all Charlton matches (most recent first)

## Launch the dashboard
From repo root (after ingest + dbt):
```bash
streamlit run app.py
```
If the DB file is missing, the app will prompt you to run ingest first.

## Troubleshooting
- **Missing/invalid DB file**: delete/move `warehouse/charlton.duckdb` and re-run ingest.
- **No data for Charlton**: verify `team_id` (348) and `TEAM_NAME` in `.env` match the ingest data; rerun ingest + dbt.
- **API issues/rate limits**: the ingest script will surface HTTP errors with response text; check your football-data.org plan/limits.
