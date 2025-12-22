{{ config(materialized='view') }}

with params as (
  select {{ var('team_id', 348) | int }} as team_id
)

select
  s.matchday,
  s.last_match_date as as_of_date,
  s.position,
  s.points,
  s.gd
from {{ ref('fct_standings_matchday') }} s
join params p on s.team_id = p.team_id
order by s.matchday
