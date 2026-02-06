create table if not exists match_metadata (
  match_id text primary key,
  competition_id text,
  competition_name text,
  match_date timestamptz,
  season text,
  week int,

  home_team_id text,
  home_team_name text,
  home_team_short text,
  away_team_id text,
  away_team_name text,
  away_team_short text,

  stadium_id text,
  stadium_name text,

  fps double precision,
  video_url text,

  raw jsonb not null
);
