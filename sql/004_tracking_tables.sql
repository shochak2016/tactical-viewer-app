-- tracking ball positions

create table public.tracking_ball_positions (
  game_id integer not null,
  frame_num integer not null,
  ball_idx integer not null,
  visibility text null,
  x double precision null,
  y double precision null,
  z double precision null,
  constraint tracking_ball_positions_pkey primary key (game_id, frame_num, ball_idx),
  constraint fk_tracking_ball_positions_frame foreign KEY (game_id, frame_num) references tracking_frames (game_id, frame_num) on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_tbp_game_frame_ballidx on public.tracking_ball_positions using btree (game_id, frame_num, ball_idx) TABLESPACE pg_default;

create index IF not exists idx_tbp_game_frame on public.tracking_ball_positions using btree (game_id, frame_num) TABLESPACE pg_default;

-- tracking frames

create table public.tracking_frames (
  game_id integer not null,
  frame_num integer not null,
  video_time_ms double precision null,
  period integer null,
  period_elapsed_time double precision null,
  period_game_clock_time double precision null,
  generated_time timestamp with time zone null,
  smoothed_time timestamp with time zone null,
  game_event_id bigint null,
  possession_event_id bigint null,
  game_event jsonb null,
  possession_event jsonb null,
  version text null,
  constraint tracking_frames_pkey primary key (game_id, frame_num)
) TABLESPACE pg_default;

create index IF not exists idx_tf_game_time on public.tracking_frames using btree (game_id, video_time_ms) TABLESPACE pg_default;

create index IF not exists idx_tracking_frames_game on public.tracking_frames using btree (game_id) TABLESPACE pg_default;

create index IF not exists idx_tracking_frames_time on public.tracking_frames using btree (game_id, video_time_ms) TABLESPACE pg_default;

create index IF not exists idx_tracking_frames_possession_event on public.tracking_frames using btree (possession_event_id) TABLESPACE pg_default;

create index IF not exists idx_tracking_frames_game_event on public.tracking_frames using btree (game_event_id) TABLESPACE pg_default;

-- tracking players positions smoothed

create table public.tracking_player_positions_smoothed (
  game_id integer not null,
  frame_num integer not null,
  side text not null,
  jersey_num integer not null,
  confidence text null,
  visibility text null,
  x double precision null,
  y double precision null,
  speed double precision null,
  constraint tracking_player_positions_smoothed_pkey primary key (game_id, frame_num, side, jersey_num),
  constraint fk_tpp_s_frame foreign KEY (game_id, frame_num) references tracking_frames (game_id, frame_num) on delete CASCADE,
  constraint tracking_player_positions_smoothed_side_check check ((side = any (array['home'::text, 'away'::text])))
) TABLESPACE pg_default;

create index IF not exists idx_tpp_s_game_frame on public.tracking_player_positions_smoothed using btree (game_id, frame_num) TABLESPACE pg_default;

create index IF not exists idx_tpp_s_game_side on public.tracking_player_positions_smoothed using btree (game_id, side) TABLESPACE pg_default;

create index IF not exists idx_tpp_s_jersey on public.tracking_player_positions_smoothed using btree (game_id, jersey_num) TABLESPACE pg_default;

create index IF not exists idx_tpps_game_frame on public.tracking_player_positions_smoothed using btree (game_id, frame_num) TABLESPACE pg_default;