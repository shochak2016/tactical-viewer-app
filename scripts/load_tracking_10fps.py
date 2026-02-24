#!/usr/bin/env python3
import os, glob, bz2, json, time, random, argparse
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

from supabase import create_client, Client




# -----------------------------
# Config / Supabase client
# -----------------------------
def make_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL and (SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY or SUPABASE_ANON_KEY) in env")
    return create_client(url, key)

def guess_game_id_from_filename(path: str) -> Optional[int]:
    base = os.path.basename(path)
    # e.g. 3812.jsonl.bz2 -> 3812
    try:
        return int(base.split(".")[0])
    except Exception:
        return None
    

# -----------------------------
# Build rows
# -----------------------------
def build_frame(obj: Dict[str, Any], game_id: int) -> Dict[str, Any]:
    return {
        "game_id": game_id,
        "frame_num": int(obj.get("frameNum")) if obj.get("frameNum") is not None else None,
        "video_time_ms": float(obj.get("videoTimeMs")) if obj.get("videoTimeMs") is not None else None,
        "period": int(obj.get("period")) if obj.get("period") is not None else None,
        "period_elapsed_time": float(obj.get("periodElapsedTime")) if obj.get("periodElapsedTime") is not None else None,
        "period_game_clock_time": float(obj.get("periodGameClockTime")) if obj.get("periodGameClockTime") is not None else None,
        "game_event_id": int(obj.get("game_event_id")) if obj.get("game_event_id") not in (None, "") else None,
        "possession_event_id": int(obj.get("possession_event_id")) if obj.get("possession_event_id") not in (None, "") else None,
    }


def build_balls(obj: Dict[str, Any], game_id: int, smoothed: bool=False) -> List[Dict[str, Any]]:
    key = "ballsSmoothed" if smoothed else "balls"
    balls = obj.get(key) or []
    out: List[Dict[str, Any]] = []
    if not isinstance(balls, list):
        return out

    frame_num = int(obj.get("frameNum")) if obj.get("frameNum") is not None else None
    for idx, b in enumerate(balls):
        # IMPORTANT: sometimes b can be a string; guard it
        if not isinstance(b, dict):
            continue
        out.append({
            "game_id": game_id,
            "frame_num": frame_num,
            "ball_idx": idx,
            "visibility": b.get("visibility"),
            "x": b.get("x"),
            "y": b.get("y"),
            "z": b.get("z"),
        })
    return out

# -----------------------------
# Robust upsert with retry + client recreation
# -----------------------------
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

def safe_upsert(sb_ref: Dict[str, Client], table: str, rows: List[Dict[str, Any]], max_retries: int = 8):
    if not rows:
        return

    for attempt in range(max_retries):
        try:
            sb_ref["sb"].table(table).upsert(rows).execute()
            return
        except Exception as e:
            if not is_transient(e) or attempt == max_retries - 1:
                raise
            wait = min(60.0, (2 ** attempt)) + random.random()
            print(f"[WARN] upsert failed table={table} rows={len(rows)} attempt={attempt+1}/{max_retries}: {e}")
            print(f"       recreating client + sleeping {wait:.1f}s then retrying...")
            # recreate client (important for HTTP/2 terminated connections)
            sb_ref["sb"] = make_client()
            time.sleep(wait)

# -----------------------------
# Main loader
# -----------------------------
def iter_jsonl_bz2(path: str):
    with bz2.open(path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help='Glob like "data/tracking_data/*.jsonl.bz2"')
    ap.add_argument("--min-dt-ms", type=float, default=100.0, help="Keep frames at least this many ms apart (100ms ≈ 10fps)")
    ap.add_argument("--batch", type=int, default=60, help="Upsert batch size")
    ap.add_argument("--no-smoothed", action="store_true", help="Skip smoothed arrays entirely")
    args = ap.parse_args()

    files = sorted(glob.glob(args.input))
    print(f"Found {len(files)} tracking files")

    sb_ref = {"sb": make_client()}

    frames_buf: List[Dict[str, Any]] = []
    players_buf: List[Dict[str, Any]] = []
    balls_buf: List[Dict[str, Any]] = []

    def flush():
        nonlocal frames_buf, players_buf, balls_buf
        if frames_buf:
            safe_upsert(sb_ref, "tracking_frames", frames_buf)
        if players_buf:
            safe_upsert(sb_ref, "tracking_player_positions", players_buf)
        if balls_buf:
            safe_upsert(sb_ref, "tracking_ball_positions", balls_buf)

        print(f"flush frames={len(frames_buf)} players={len(players_buf)} balls={len(balls_buf)}")
        frames_buf = []
        players_buf = []
        balls_buf = []

    for fp in files:
        game_id = guess_game_id_from_filename(fp)
        if game_id is None:
            print(f"[SKIP] Could not infer game_id from filename: {fp}")
            continue

        print(f"\n==> Loading {fp} (game_id={game_id})")
        last_keep_ms: Optional[float] = None
        kept = 0
        total = 0

        for obj in iter_jsonl_bz2(fp):
            total += 1
            # some lines have gameRefId None; we trust filename game_id
            t = obj.get("videoTimeMs")
            if t is None:
                continue
            t = float(t)

            if last_keep_ms is not None and (t - last_keep_ms) < args.min_dt_ms:
                continue

            last_keep_ms = t
            kept += 1

            fr = build_frame(obj, game_id)
            if fr["frame_num"] is None:
                continue

            frames_buf.append(fr)
            players_buf.extend(build_players(obj, game_id, "home", smoothed=False))
            players_buf.extend(build_players(obj, game_id, "away", smoothed=False))
            balls_buf.extend(build_balls(obj, game_id, smoothed=False))

            # smoothed optional
            if not args.no_smoothed:
                players_buf.extend(build_players(obj, game_id, "home", smoothed=True))
                players_buf.extend(build_players(obj, game_id, "away", smoothed=True))
                balls_buf.extend(build_balls(obj, game_id, smoothed=True))

            if (
                len(frames_buf) >= args.batch
                or len(players_buf) >= args.batch * 22
                or len(balls_buf) >= args.batch * 4
    ):
                flush()


        # final flush per file
        flush()
        print(f"Done {os.path.basename(fp)}: total_lines={total} kept_frames={kept}")

    print("\nAll done.")

if __name__ == "__main__":
    main()
