import os
import json
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SERVICE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")

supabase = create_client(SUPABASE_URL, SERVICE_KEY)

EVENTS_DIR = Path("data/event_data")


def chunks(lst, n=250):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def dedup_by_key(rows, key_fn):
    """Keeps the last row for each key."""
    d = {}
    for r in rows:
        d[key_fn(r)] = r
    return list(d.values())


def safe_int(x):
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def safe_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def main():
    files = sorted(EVENTS_DIR.glob("*.json"))
    if not files:
        raise SystemExit(f"No event files found in {EVENTS_DIR.resolve()}")

    total_timeline = 0
    total_game_events = 0
    total_possessions = 0
    total_passes = 0
    total_shots = 0

    for fp in files:
        match_id = int(fp.stem)  # filenames are 3812.json etc.

        events = json.loads(fp.read_text(encoding="utf-8"))

        timeline_rows = []
        game_event_rows = []
        possession_rows = []
        pass_rows = []
        shot_rows = []

        for e in events:
            ge = e.get("gameEvents") or {}
            pe = e.get("possessionEvents") or {}

            game_event_id = int(e["gameEventId"])
            possession_event_id = safe_int(e.get("possessionEventId"))

            # 1) timeline
            timeline_rows.append({
                "match_id": match_id,
                "game_event_id": game_event_id,
                "possession_event_id": possession_event_id,
                "start_time": safe_float(e.get("startTime")),
                "end_time": safe_float(e.get("endTime")),
                "duration": safe_float(e.get("duration")),
                "event_time": safe_float(e.get("eventTime")),
                "sequence": safe_int(e.get("sequence")),
            })

            # 2) game_events wrapper
            game_event_rows.append({
                "match_id": match_id,
                "game_event_id": game_event_id,
                "game_event_type": ge.get("gameEventType"),
                "period": safe_int(ge.get("period")),
                "team_id": safe_int(ge.get("teamId")),
                "team_name": ge.get("teamName"),
                "player_id": safe_int(ge.get("playerId")),
                "player_name": ge.get("playerName"),
                "start_game_clock": safe_int(ge.get("startGameClock")),
                "start_formatted_game_clock": ge.get("startFormattedGameClock"),
                "initial_non_event": ge.get("initialNonEvent"),
                "video_missing": ge.get("videoMissing"),
            })

            # 3) possession events (only if we actually have an id)
            if possession_event_id is not None:
                possession_event_type = pe.get("possessionEventType")

                possession_rows.append({
                    "match_id": match_id,
                    "possession_event_id": possession_event_id,
                    "possession_event_type": possession_event_type,
                    "non_event": pe.get("nonEvent"),
                    "game_clock": safe_float(pe.get("gameClock")),
                    "formatted_game_clock": pe.get("formattedGameClock"),
                    "ball_height_type": pe.get("ballHeightType"),
                    "body_type": pe.get("bodyType"),
                    "high_point_type": pe.get("highPointType"),
                })

                # 4) passes (PA and also keep CR if it appears)
                if possession_event_type in ("PA", "CR"):
                    pass_rows.append({
                        "match_id": match_id,
                        "possession_event_id": possession_event_id,
                        "passer_player_id": safe_int(pe.get("passerPlayerId")),
                        "passer_player_name": pe.get("passerPlayerName"),
                        "receiver_player_id": safe_int(pe.get("receiverPlayerId")),
                        "receiver_player_name": pe.get("receiverPlayerName"),
                        "target_player_id": safe_int(pe.get("targetPlayerId")),
                        "target_player_name": pe.get("targetPlayerName"),
                        "pass_type": pe.get("passType"),
                        "pass_outcome_type": pe.get("passOutcomeType"),
                    })

                # 5) shots
                if possession_event_type == "SH":
                    shot_rows.append({
                        "match_id": match_id,
                        "possession_event_id": possession_event_id,
                        "shooter_player_id": safe_int(pe.get("shooterPlayerId")),
                        "shooter_player_name": pe.get("shooterPlayerName"),
                        "shot_type": pe.get("shotType"),
                        "shot_nature_type": pe.get("shotNatureType"),
                        "shot_initial_height_type": pe.get("shotInitialHeightType"),
                        "shot_outcome_type": pe.get("shotOutcomeType"),
                        "ball_moving": pe.get("ballMoving"),
                    })

        # Deduplicate within-file to avoid "cannot affect row a second time"
        timeline_rows = dedup_by_key(timeline_rows, lambda r: (r["match_id"], r["game_event_id"]))
        game_event_rows = dedup_by_key(game_event_rows, lambda r: (r["match_id"], r["game_event_id"]))
        possession_rows = dedup_by_key(possession_rows, lambda r: (r["match_id"], r["possession_event_id"]))
        pass_rows = dedup_by_key(pass_rows, lambda r: (r["match_id"], r["possession_event_id"]))
        shot_rows = dedup_by_key(shot_rows, lambda r: (r["match_id"], r["possession_event_id"]))

        # Upsert in safe batches (rerunnable)
        for b in chunks(timeline_rows, 500):
            supabase.table("event_timeline").upsert(b).execute()
        for b in chunks(game_event_rows, 500):
            supabase.table("game_events").upsert(b).execute()
        for b in chunks(possession_rows, 500):
            supabase.table("possession_events").upsert(b).execute()
        for b in chunks(pass_rows, 500):
            supabase.table("passes").upsert(b).execute()
        for b in chunks(shot_rows, 500):
            supabase.table("shots").upsert(b).execute()


        total_timeline += len(timeline_rows)
        total_game_events += len(game_event_rows)
        total_possessions += len(possession_rows)
        total_passes += len(pass_rows)
        total_shots += len(shot_rows)

        print(fp.name, "timeline=", len(timeline_rows),
              "possession=", len(possession_rows),
              "passes=", len(pass_rows),
              "shots=", len(shot_rows))

    print("DONE")
    print("timeline_total=", total_timeline)
    print("game_events_total=", total_game_events)
    print("possession_total=", total_possessions)
    print("passes_total=", total_passes)
    print("shots_total=", total_shots)


if __name__ == "__main__":
    main()
