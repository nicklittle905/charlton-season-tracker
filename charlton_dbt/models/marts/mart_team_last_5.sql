{{ config(materialized='view') }}

with params as (
  select {{ var('team_id', 348) | int }} as team_id
),

tm as (
  select *
  from {{ ref('fct_team_match') }}
  where team_id = (select team_id from params)
)

select
  tm.match_id,
  tm.match_date,
  tm.matchday,
  case when tm.is_home = 1 then 'H' else 'A' end as home_away,
  opp.team_name as opponent,
  tm.goals_for,
  tm.goals_against,
  tm.result,
  tm.points
from tm
left join {{ ref('stg_raw_teams') }} opp
  on tm.opponent_team_id = opp.team_id
order by tm.match_date desc
