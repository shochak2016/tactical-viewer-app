import os
import re
import json
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


# ---------- Config ----------
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise SystemExit(
        "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env (repo root)."
    )

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

ROSTERS_DIR = Path("data/rosters")  # from repo root


# ---------- Helpers ----------
def match_id_from_filename(filename: str) -> int:
    """
    Extract the first 3+ digit number from filename as match_id.
    Example: 'roster_3812.json' -> 3812
    """
    m = re.search(r"(\d{3,})", filename)
    if not m:
        raise ValueError(f"Can't infer match_id from filename: {filename}")
    return int(m.group(1))


def chunk_list(items, chunk_size: int = 500):
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


# ---------- Main ----------
def main():
    if not ROSTERS_DIR.exists():
        raise SystemExit(f"Missing folder: {ROSTERS_DIR.resolve()}")

    files = sorted(ROSTERS_DIR.glob("*.json"))
    if not files:
        raise SystemExit(f"No roster JSON files found in: {ROSTERS_DIR.resolve()}")

    total_files = 0
    total_roster_rows = 0
    total_player_upserts = 0

    for fp in files:
        total_files += 1
        match_id = match_id_from_filename(fp.name)

        roster = json.loads(fp.read_text(encoding="utf-8"))

        # Build:
        # 1) roster_players: unique players in this file
        # 2) match_rosters: one row per (match_id, team_id, player_id)
        roster_players = {}
        roster_rows = []

        for r in roster:
            pid = int(r["player"]["id"])
            roster_players[pid] = {
                "id": pid,
                "nickname": r["player"].get("nickname"),
            }

            shirt = r.get("shirtNumber")
            shirt_number = None
            if shirt not in (None, "", "nan"):
                shirt_number = int(shirt)

            roster_rows.append(
                {
                    "match_id": match_id,
                    "team_id": int(r["team"]["id"]),
                    "player_id": pid,
                    "position_group_type": r.get("positionGroupType"),
                    "shirt_number": shirt_number,
                    "started": bool(r.get("started", False)),
                }
            )

        # Upsert players
        players_payload = list(roster_players.values())
        for batch in chunk_list(players_payload, 500):
            supabase.table("roster_players").upsert(batch).execute()

        # Upsert rosters (rerunnable because of PK on (match_id, team_id, player_id))
        for batch in chunk_list(roster_rows, 500):
            supabase.table("match_rosters").upsert(batch).execute()

        total_player_upserts += len(players_payload)
        total_roster_rows += len(roster_rows)

        print(
            fp.name,
            "match_id=",
            match_id,
            "players=",
            len(players_payload),
            "roster_rows=",
            len(roster_rows),
        )

    print("DONE")
    print("files_processed=", total_files)
    print("player_upserts(file_unique_total)=", total_player_upserts)
    print("roster_rows_upserted=", total_roster_rows)


if __name__ == "__main__":
    main()
