#!/usr/bin/env python3
import os, glob, bz2, json, time, random, argparse
from typing import Any, Dict, List, Optional

from supabase import create_client, Client

def dedupe_rows(rows, key_fields):
    """
    Deduplicate a list of dict rows by a composite key.
    Keeps the LAST occurrence (so newest wins).
    """
    out = {}
    for r in rows:
        k = tuple(r.get(f) for f in key_fields)
        out[k] = r
    return list(out.values())


def make_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
        or os.environ.get("SUPABASE_ANON_KEY")
    )
    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL and (SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY or SUPABASE_ANON_KEY) in env"
        )
    return create_client(url, key)


def guess_game_id_from_filename(path: str) -> Optional[int]:
    base = os.path.basename(path)
    try:
        return int(base.split(".")[0])
    except Exception:
        return None


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
            sb_ref["sb"] = make_client()
            time.sleep(wait)


def iter_jsonl_bz2(path: str):
    with bz2.open(path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def build_frame(obj: Dict[str, Any], game_id: int) -> Optional[Dict[str, Any]]:
    if obj.get("frameNum") is None:
        return None
    return {
        "game_id": game_id,
        "frame_num": int(obj["frameNum"]),
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


def build_players_smoothed(obj: Dict[str, Any], game_id: int, side: str) -> List[Dict[str, Any]]:
    key = f"{side}PlayersSmoothed"
    players = obj.get(key) or []
    if not isinstance(players, list):
        return []

    frame_num = obj.get("frameNum")
    if frame_num is None:
        return []
    frame_num = int(frame_num)

    out: List[Dict[str, Any]] = []
    for p in players:
        if not isinstance(p, dict):
            continue
        jersey = p.get("jerseyNum")
        try:
            jersey_num = int(jersey) if jersey is not None else None
        except Exception:
            jersey_num = None
        if jersey_num is None:
            continue

        out.append({
            "game_id": game_id,
            "frame_num": frame_num,
            "side": side,  # 'home'/'away'
            "jersey_num": jersey_num,
            "confidence": p.get("confidence"),
            "visibility": p.get("visibility"),
            "x": p.get("x"),
            "y": p.get("y"),
            "speed": p.get("speed"),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help='Glob like "data/tracking_data/*.jsonl.bz2"')
    ap.add_argument("--min-dt-ms", type=float, default=100.0, help="100ms ≈ 10fps")
    ap.add_argument("--batch-frames", type=int, default=400)
    ap.add_argument("--batch-players", type=int, default=8000)
    args = ap.parse_args()

    files = sorted(glob.glob(args.input))
    print(f"Found {len(files)} tracking files")

    sb_ref = {"sb": make_client()}

    frames_buf: List[Dict[str, Any]] = []
    players_buf: List[Dict[str, Any]] = []

    def flush():
        nonlocal frames_buf, players_buf

        if frames_buf:
            # frames are usually unique, but safe to dedupe anyway
            frames_buf = dedupe_rows(frames_buf, ["game_id", "frame_num"])
            safe_upsert(sb_ref, "tracking_frames", frames_buf)

        if players_buf:
            # IMPORTANT: dedupe within this batch to avoid 21000
            players_buf = [r for r in players_buf if r.get("jersey_num") is not None]
            players_buf = dedupe_rows(players_buf, ["game_id", "frame_num", "side", "jersey_num"])
            safe_upsert(sb_ref, "tracking_player_positions_smoothed", players_buf)

        print(f"flush frames={len(frames_buf)} smoothed_players={len(players_buf)}")
        frames_buf = []
        players_buf = []


    for fp in files:
        game_id = guess_game_id_from_filename(fp)
        if game_id is None:
            print(f"[SKIP] Could not infer game_id from filename: {fp}")
            continue

        print(f"\n==> Loading {fp} (game_id={game_id})")
        last_keep_ms: Optional[float] = None
        total_lines = 0
        kept_frames = 0

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

            frames_buf.append(fr)
            kept_frames += 1

            players_buf.extend(build_players_smoothed(obj, game_id, "home"))
            players_buf.extend(build_players_smoothed(obj, game_id, "away"))

            if len(frames_buf) >= args.batch_frames or len(players_buf) >= args.batch_players:
                print(f"flush frames={len(frames_buf)} smoothed_players={len(players_buf)}")
                flush()

        if frames_buf or players_buf:
            print(f"flush frames={len(frames_buf)} smoothed_players={len(players_buf)}")
            flush()

        print(f"Done {os.path.basename(fp)}: total_lines={total_lines} kept_frames={kept_frames}")

    print("\nAll done.")


if __name__ == "__main__":
    main()
