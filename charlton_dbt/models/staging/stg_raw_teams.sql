with src as (
  select
    team_id,
    name as team_name,
    short_name,
    tla,
    crest,
    fetched_at_utc
  from {{ source('raw', 'raw_teams') }}
)
select * from src