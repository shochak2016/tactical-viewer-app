#!/usr/bin/env python3
"""
load_smooth_balls_debug.py

Goal: load ballsSmoothed at ~10fps AND make it impossible to "silently succeed".

What this script adds vs your current one:
- Verifies env + confirms which Supabase project you're connected to
- Prints per-file summary: kept_frames, frames_with_nonempty_ballsSmoothed
- Flush logs include row counts, and sample rows
- After each file, runs READ-AFTER-WRITE verification queries (counts by game_id)
- Optional: stop after N games, or a specific --only-game
- Optional: "dry-run" mode to test parsing/downsample without writing

Tables assumed:
- tracking_frames(game_id, frame_num, video_time_ms, ...)
- tracking_ball_positions_smoothed(game_id, frame_num, ball_idx, visibility, x, y, z)
"""

import os, glob, bz2, json, time, random, argparse
from typing import Any, Dict, List, Optional, Iterable, Tuple
from supabase import create_client, Client

TRANSIENT_KEYWORDS = (
    "ConnectionTerminated",
    "RemoteProtocolError",
    "ReadTimeout",
    "WriteTimeout",
    "ConnectTimeout",
    "Server disconnected",
    "502",
    "503",
    "504",
)

def is_transient(exc: Exception) -> bool:
    s = repr(exc)
    return any(k in s for k in TRANSIENT_KEYWORDS)

def require_env() -> Tuple[str, str]:
    url = os.environ.get("SUPABASE_URL")
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
        or os.environ.get("SUPABASE_ANON_KEY")
    )
    if not url or not key:
        raise RuntimeError(
            "Missing env vars. Need SUPABASE_URL and one of: SUPABASE_SERVICE_ROLE_KEY / SUPABASE_KEY / SUPABASE_ANON_KEY.\n"
            "Tip: in your shell run: set -a; source .env; set +a"
        )
    return url, key

def make_client() -> Client:
    url, key = require_env()
    return create_client(url, key)

def guess_game_id_from_filename(path: str) -> Optional[int]:
    base = os.path.basename(path)
    try:
        return int(base.split(".")[0])
    except Exception:
        return None

def iter_jsonl_bz2(path: str) -> Iterable[Dict[str, Any]]:
    with bz2.open(path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def dedupe_rows(rows: List[Dict[str, Any]], key_fields: List[str]) -> List[Dict[str, Any]]:
    """Deduplicate by composite key (keeps LAST)."""
    out: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for r in rows:
        k = tuple(r.get(f) for f in key_fields)
        out[k] = r
    return list(out.values())

def safe_upsert(
    sb_ref: Dict[str, Client],
    table: str,
    rows: List[Dict[str, Any]],
    max_retries: int = 8,
) -> None:
    """Upsert with retry on transient HTTP issues."""
    if not rows:
        return

    for attempt in range(max_retries):
        try:
            # NOTE: don't rely on resp.data length; it can be empty even when write succeeds.
            sb_ref["sb"].table(table).upsert(rows).execute()
            return
        except Exception as e:
            if (not is_transient(e)) or attempt == max_retries - 1:
                raise
            wait = min(60.0, (2 ** attempt)) + random.random()
            print(f"[WARN] upsert failed table={table} rows={len(rows)} attempt={attempt+1}/{max_retries}: {e}")
            print(f"       recreating client + sleeping {wait:.1f}s then retrying...")
            sb_ref["sb"] = make_client()
            time.sleep(wait)

def count_rows(sb: Client, table: str, game_id: int) -> int:
    """READ-AFTER-WRITE verification."""
    resp = sb.table(table).select("game_id", count="exact").eq("game_id", game_id).execute()
    return int(resp.count or 0)

def build_frame(obj: Dict[str, Any], game_id: int) -> Optional[Dict[str, Any]]:
    fn = obj.get("frameNum")
    if fn is None:
        return None
    return {
        "game_id": game_id,
        "frame_num": int(fn),
        "video_time_ms": float(obj["videoTimeMs"]) if obj.get("videoTimeMs") is not None else None,
        "period": int(obj["period"]) if obj.get("period") is not None else None,
        "period_elapsed_time": float(obj["periodElapsedTime"]) if obj.get("periodElapsedTime") is not None else None,
        "period_game_clock_time": float(obj["periodGameClockTime"]) if obj.get("periodGameClockTime") is not None else None,
        "generated_time": obj.get("generatedTime"),
        "smoothed_time": obj.get("smoothedTime"),
        "version": obj.get("version"),
        "game_event_id": int(obj["game_event_id"]) if obj.get("game_event_id") not in (None, "") else None,
        "possession_event_id": int(obj["possession_event_id"]) if obj.get("possession_event_id") not in (None, "") else None,
    }

def build_balls_smoothed(obj: Dict[str, Any], game_id: int) -> List[Dict[str, Any]]:
    bs = obj.get("ballsSmoothed")

    # Normalize:
    # - dict -> [dict]
    # - list -> list
    # - None/other -> []
    if bs is None:
        balls = []
    elif isinstance(bs, dict):
        balls = [bs]
    elif isinstance(bs, list):
        balls = bs
    else:
        return []

    fn = obj.get("frameNum")
    if fn is None:
        return []
    frame_num = int(fn)

    out: List[Dict[str, Any]] = []
    for idx, b in enumerate(balls):
        if not isinstance(b, dict):
            continue
        # skip empty dicts and missing coords
        x, y = b.get("x"), b.get("y")
        if x is None or y is None:
            continue

        out.append({
            "game_id": game_id,
            "frame_num": frame_num,
            "ball_idx": idx,  # will be 0 for dict-shaped data
            "visibility": b.get("visibility"),
            "x": float(x),
            "y": float(y),
            "z": float(b["z"]) if b.get("z") is not None else None,
        })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help='Glob like "data/tracking_data/*.jsonl.bz2"')
    ap.add_argument("--min-dt-ms", type=float, default=100.0, help="Downsample: keep frames >= this many ms apart (100ms ≈ 10fps)")
    ap.add_argument("--batch-frames", type=int, default=400)
    ap.add_argument("--batch-balls", type=int, default=2000)
    ap.add_argument("--only-game", type=int, default=None, help="Only load this game_id")
    ap.add_argument("--max-games", type=int, default=None, help="Stop after processing this many games")
    ap.add_argument("--dry-run", action="store_true", help="Parse + stats only, do not write to DB")
    ap.add_argument("--verify", action="store_true", help="After each game, query DB counts to confirm inserts")
    ap.add_argument("--print-every-flush", type=int, default=1, help="Print every N flushes")
    args = ap.parse_args()

    url, _ = require_env()
    print("SUPABASE_URL:", url)

    files = sorted(glob.glob(args.input))
    if args.only_game is not None:
        files = [fp for fp in files if guess_game_id_from_filename(fp) == args.only_game]

    print(f"Found {len(files)} tracking files (after filters)")

    sb_ref = {"sb": make_client()}

    frames_buf: List[Dict[str, Any]] = []
    balls_buf: List[Dict[str, Any]] = []
    flush_count = 0
    games_done = 0

    def flush(reason: str):
        nonlocal frames_buf, balls_buf, flush_count
        flush_count += 1

        frames_u = dedupe_rows(frames_buf, ["game_id", "frame_num"]) if frames_buf else []
        balls_u  = dedupe_rows(balls_buf,  ["game_id", "frame_num", "ball_idx"]) if balls_buf else []

        if flush_count % args.print_every_flush == 0:
            sample_ball = balls_u[0] if balls_u else None
            sample_frame = frames_u[0] if frames_u else None
            print(
                f"[FLUSH#{flush_count}] reason={reason} "
                f"frames_raw={len(frames_buf)} frames_u={len(frames_u)} "
                f"balls_raw={len(balls_buf)} balls_u={len(balls_u)} "
                f"sample_frame={sample_frame} sample_ball={sample_ball}"
            )

        if not args.dry_run:
            if frames_u:
                safe_upsert(sb_ref, "tracking_frames", frames_u)
            if balls_u:
                safe_upsert(sb_ref, "tracking_ball_positions_smoothed", balls_u)

        frames_buf = []
        balls_buf = []

    for fp in files:
        game_id = guess_game_id_from_filename(fp)
        if game_id is None:
            print(f"[SKIP] Could not infer game_id from filename: {fp}")
            continue

        print(f"\n==> Loading {fp} (game_id={game_id})")
        last_keep_ms: Optional[float] = None

        total_lines = 0
        kept_frames = 0
        frames_with_nonempty_balls = 0
        balls_rows_added_in_memory = 0

        for obj in iter_jsonl_bz2(fp):
            total_lines += 1
            t = obj.get("videoTimeMs")
            if t is None:
                continue
            t = float(t)

            if last_keep_ms is not None and (t - last_keep_ms) < args.min_dt_ms:
                continue
            last_keep_ms = t

            fr = build_frame(obj, game_id)
            if fr is None:
                continue

            kept_frames += 1
            frames_buf.append(fr)

            b = build_balls_smoothed(obj, game_id)
            if b:
                frames_with_nonempty_balls += 1
                balls_rows_added_in_memory += len(b)
                balls_buf.extend(b)

            if len(frames_buf) >= args.batch_frames or len(balls_buf) >= args.batch_balls:
                flush("batch_threshold")

        if frames_buf or balls_buf:
            flush("end_of_file")

        print(
            f"Done {os.path.basename(fp)}: "
            f"total_lines={total_lines} kept_frames={kept_frames} "
            f"frames_with_nonempty_ballsSmoothed={frames_with_nonempty_balls} "
            f"ball_rows_buffered={balls_rows_added_in_memory}"
        )

        if args.verify and not args.dry_run:
            fr_ct = count_rows(sb_ref["sb"], "tracking_frames", game_id)
            bl_ct = count_rows(sb_ref["sb"], "tracking_ball_positions_smoothed", game_id)
            print(f"[VERIFY] game_id={game_id} tracking_frames_rows={fr_ct} balls_smoothed_rows={bl_ct}")

        games_done += 1
        if args.max_games is not None and games_done >= args.max_games:
            print(f"\nStopping after max_games={args.max_games}")
            break

    print("\nAll done.")

if __name__ == "__main__":
    main()