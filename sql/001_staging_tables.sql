-- ============================================================
-- 001_staging_tables.sql
-- Tactical Viewer App — STAGING tables
--
-- Purpose:
--   Staging tables match raw CSV / raw JSON shapes to make bulk import easy.
--   You can truncate/drop staging anytime without breaking the app.
--
-- How judges should use this:
--   1) Run this file first in Supabase SQL Editor
--   2) Import CSVs into staging tables (Table Editor → Import data)
--   3) Then run 002_core_schema.sql to create clean tables + transform inserts
-- ============================================================

-- ----------------------------
-- PLAYERS (CSV staging)
-- ----------------------------
drop table if exists players_staging;

create table if not exists players_staging (
  dob date,
  "firstName" text,
  height real,
  id int,
  "lastName" text,
  nickname text,
  "positionGroupType" text
);

-- ----------------------------
-- COMPETITIONS (CSV staging)
-- games is a string that looks like a JSON array
-- Example:
--   "[{'id': '3812', 'season': '2022'}, ...]"
-- ----------------------------
drop table if exists competitions_staging;

create table if not exists competitions_staging (
  games text,
  id int,
  name text
);