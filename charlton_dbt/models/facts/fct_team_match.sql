{{ config(materialized='table') }}

with m as (
  select *
  from {{ ref('stg_raw_matches') }}
  where status = 'FINISHED'
    and home_team_id is not null
    and away_team_id is not null
    and home_score_full is not null
    and away_score_full is not null
),

home as (
  select
    match_id,
    cast(utc_date as date) as match_date,
    matchday,
    competition_code,
    season_start_year,
    home_team_id as team_id,
    away_team_id as opponent_team_id,
    1 as is_home,
    home_score_full as goals_for,
    away_score_full as goals_against
  from m
),

away as (
  select
    match_id,
    cast(utc_date as date) as match_date,
    matchday,
    competition_code,
    season_start_year,
    away_team_id as team_id,
    home_team_id as opponent_team_id,
    0 as is_home,
    away_score_full as goals_for,
    home_score_full as goals_against
  from m
),

u as (
  select * from home
  union all
  select * from away
),

scored as (
  select
    *,
    case
      when goals_for > goals_against then 3
      when goals_for = goals_against then 1
      else 0
    end as points,
    case
      when goals_for > goals_against then 'W'
      when goals_for = goals_against then 'D'
      else 'L'
    end as result
  from u
)

select * from scored
