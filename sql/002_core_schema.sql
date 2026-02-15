-- ============================================================
-- 002_core_schema.sql
-- Tactical Viewer App — CORE schema (clean tables)
--
-- Purpose:
--   Clean, normalized tables used by the application + analytics.
--   Includes transformation SQL from staging → clean for CSV imports.
--
-- Run order:
--   1) Run 001_staging_tables.sql
--   2) Import players.csv → players_staging
--   3) Import competitions.csv → competitions_staging
--   4) Run THIS file (creates clean tables + loads from staging)
--   5) Run Python loaders for JSON (rosters, events, metadata)
-- ============================================================


-- ============================================================
-- SECTION A — CORE DIMENSIONS
-- ============================================================

-- ----------------------------
-- Players (clean)
-- ----------------------------
drop table if exists players_cascade;

create table if not exists players (
  id int primary key,
  dob date,
  first_name text,
  last_name text,
  nickname text,
  height real,
  position_group_type text
);

-- Load players from staging (dedupe by id, keep best row)
insert into players (id, dob, first_name, last_name, nickname, height, position_group_type)
select distinct on (id)
  id,
  dob,
  "firstName" as first_name,
  "lastName"  as last_name,
  nickname,
  height,
  "positionGroupType" as position_group_type
from players_staging
where id is not null
order by
  id,
  (nickname is not null) desc,
  ("firstName" is not null) desc,
  ("lastName" is not null) desc,
  ("positionGroupType" is not null) desc,
  (dob is not null) desc,
  (height is not null) desc
on conflict (id) do update set
  dob = excluded.dob,
  first_name = excluded.first_name,
  last_name = excluded.last_name,
  nickname = excluded.nickname,
  height = excluded.height,
  position_group_type = excluded.position_group_type;


-- ----------------------------
-- Competitions (clean)
-- ----------------------------
drop table if exists competitions cascade;

create table if not exists competitions (
  id int primary key,
  name text not null
);

insert into competitions (id, name)
select distinct id, name
from competitions_staging
where id is not null
on conflict (id) do update set
  name = excluded.name;


-- ----------------------------
-- Competition ↔ Games mapping (clean)
-- ----------------------------
drop table if exists competition_games;

create table if not exists competition_games (
  competition_id int not null references competitions(id) on delete cascade,
  game_id int not null,
  season text,
  primary key (competition_id, game_id)
);

-- Parse competitions_staging.games into rows
insert into competition_games (competition_id, game_id, season)
select
  cs.id as competition_id,
  (g->>'id')::int as game_id,
  g->>'season' as season
from competitions_staging cs
cross join lateral jsonb_array_elements(
  replace(cs.games, '''', '"')::jsonb
) as g
where cs.id is not null
on conflict (competition_id, game_id) do update set
  season = excluded.season;


-- ============================================================
-- SECTION B — MATCH METADATA (clean)
-- ============================================================
-- This is populated by your teammate’s metadata loader.
-- We store pitch size and key match identifiers.

drop table if exists match_metadata;

create table if not exists match_metadata (
  match_id int primary key,             -- game id, e.g. 3812
  competition_id int null references competitions(id),
  season text null,

  match_date timestamptz null,

  home_team_id int null,
  home_team_name text null,
  away_team_id int null,
  away_team_name text null,

  stadium_id int null,
  stadium_name text null,
  pitch_length double precision null,
  pitch_width double precision null,

  home_team_start_left boolean null,
  home_team_start_left_extra_time boolean null,

  fps double precision null
);


-- ============================================================
-- SECTION C — ROSTERS 
-- ============================================================
-- Populated by the roster loader.

drop table if exists roster_players;
drop table if exists match_rosters;

create table if not exists roster_players (
  id int primary key,     -- roster JSON player.id
  nickname text
);

create table if not exists match_rosters (
  match_id int not null,
  team_id int not null,
  player_id int not null references roster_players(id) on delete cascade,
  position_group_type text,
  shirt_number int,
  started boolean,
  primary key (match_id, team_id, player_id)
);

create index if not exists idx_match_rosters_match on match_rosters(match_id);
create index if not exists idx_match_rosters_team  on match_rosters(team_id);


-- ============================================================
-- SECTION D — EVENTS (clean, normalized)
-- ============================================================
-- Populated by load_events_multi.py
-- Note: you can paste your existing event table SQL here.
-- create index if not exists idx_event_timeline_match_time on event_timeline(match_id, event_time);

-- 1) timeline: one row per JSON object (game_event_id is unique per match)
create table if not exists event_timeline (
  match_id int not null,
  game_event_id bigint not null,
  possession_event_id bigint null,

  start_time double precision,
  end_time double precision,
  duration double precision,
  event_time double precision,
  sequence int,

  primary key (match_id, game_event_id)
);

-- 2) game event wrapper (kickoff, out, on-the-ball, etc.)
create table if not exists game_events (
  match_id int not null,
  game_event_id bigint not null,

  game_event_type text,
  period int,
  team_id int,
  team_name text,
  player_id int,
  player_name text,

  start_game_clock int,
  start_formatted_game_clock text,
  initial_non_event boolean,
  video_missing boolean,

  primary key (match_id, game_event_id),
  foreign key (match_id, game_event_id)
    references event_timeline(match_id, game_event_id)
    on delete cascade
);

-- 3) possession event (pass/shot/etc.) keyed by possession_event_id
create table if not exists possession_events (
  match_id int not null,
  possession_event_id bigint not null,

  possession_event_type text,
  non_event boolean,
  game_clock double precision,
  formatted_game_clock text,

  ball_height_type text,
  body_type text,
  high_point_type text,

  primary key (match_id, possession_event_id)
);

-- 4) passes (subset of possession events)
create table if not exists passes (
  match_id int not null,
  possession_event_id bigint not null,

  passer_player_id int,
  passer_player_name text,
  receiver_player_id int,
  receiver_player_name text,
  target_player_id int,
  target_player_name text,

  pass_type text,
  pass_outcome_type text,

  primary key (match_id, possession_event_id),
  foreign key (match_id, possession_event_id)
    references possession_events(match_id, possession_event_id)
    on delete cascade
);

-- 5) shots (subset of possession events)
create table if not exists shots (
  match_id int not null,
  possession_event_id bigint not null,

  shooter_player_id int,
  shooter_player_name text,

  shot_type text,
  shot_nature_type text,
  shot_initial_height_type text,
  shot_outcome_type text,

  ball_moving boolean,

  primary key (match_id, possession_event_id),
  foreign key (match_id, possession_event_id)
    references possession_events(match_id, possession_event_id)
    on delete cascade
);

-- Helpful indexes
create index if not exists idx_event_timeline_match_time
on event_timeline (match_id, event_time);

create index if not exists idx_possession_events_type
on possession_events (possession_event_type);





