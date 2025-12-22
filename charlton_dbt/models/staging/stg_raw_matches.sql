with src as (
  select
    match_id,
    competition_code,
    season_start_year,
    utc_date,
    status,
    matchday,
    stage,
    group_name,

    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,

    home_score_full,
    away_score_full,
    home_score_half,
    away_score_half,

    winner,
    last_updated_utc,
    fetched_at_utc
  from {{ source('raw', 'raw_matches') }}
)
select * from src
