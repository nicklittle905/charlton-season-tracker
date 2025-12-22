{{ config(materialized='table') }}

with tm as (
  select *
  from {{ ref('fct_team_match') }}
  where matchday is not null
),

per_matchday as (
  select
    competition_code,
    season_start_year,
    matchday,
    team_id,

    count(*) as played,
    sum(case when result = 'W' then 1 else 0 end) as won,
    sum(case when result = 'D' then 1 else 0 end) as drawn,
    sum(case when result = 'L' then 1 else 0 end) as lost,

    sum(goals_for) as gf,
    sum(goals_against) as ga,
    sum(goals_for - goals_against) as gd,
    sum(points) as points,

    max(match_date) as last_match_date
  from tm
  group by 1,2,3,4
),

cumulative as (
  select
    pm.competition_code,
    pm.season_start_year,
    pm.matchday,
    pm.team_id,

    sum(pm.played) over w as played,
    sum(pm.won) over w as won,
    sum(pm.drawn) over w as drawn,
    sum(pm.lost) over w as lost,
    sum(pm.gf) over w as gf,
    sum(pm.ga) over w as ga,
    sum(pm.gd) over w as gd,
    sum(pm.points) over w as points,

    max(pm.last_match_date) over w as last_match_date
  from per_matchday pm
  window w as (
    partition by pm.competition_code, pm.season_start_year, pm.team_id
    order by pm.matchday
  )
),

ranked as (
  select
    *,
    row_number() over (
      partition by competition_code, season_start_year, matchday
      order by points desc, gd desc, gf desc, team_id asc
    ) as position
  from cumulative
)

select * from ranked
