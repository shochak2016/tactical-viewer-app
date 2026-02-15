import os, json
from glob import glob
from dotenv import load_dotenv
from supabase import create_client

# Loads match metadata JSON files into the `match_metadata` table in Supabase.
# Each JSON file is typically named by match id (ex: 3812.json) and contains match-level info
# like teams, competition, stadium, and timing fields.

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

def parse_one_file(path: str) -> dict:
    # Opens one JSON file and loads it into Python as a dict/list.
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)


    # Many of these metadata files are a list with one object: [ { ... } ].
    # If it's a list, take the first object; otherwise assume it's already a dict.
    obj = data[0] if isinstance(data, list) else data

    # Pull out nested sections so we can easily access team/competition/stadium fields.
    comp = obj.get("competition", {}) or {}
    home = obj.get("homeTeam", {}) or {}
    away = obj.get("awayTeam", {}) or {}
    stadium = obj.get("stadium", {}) or {}

    # Build the row we want to store in Supabase.
    # We keep some commonly-used fields as normal columns AND store the full JSON in `raw`
    # so we don't lose any extra metadata that might be useful later.

    return {
        "match_id": str(obj.get("id")),
        "competition_id": str(comp.get("id")) if comp.get("id") is not None else None,
        "competition_name": comp.get("name"),
        "match_date": obj.get("date"),
        "season": str(obj.get("season")) if obj.get("season") is not None else None,
        "week": obj.get("week"),

        "home_team_id": str(home.get("id")) if home.get("id") is not None else None,
        "home_team_name": home.get("name"),
        "home_team_short": home.get("shortName"),

        "away_team_id": str(away.get("id")) if away.get("id") is not None else None,
        "away_team_name": away.get("name"),
        "away_team_short": away.get("shortName"),

        "stadium_id": str(stadium.get("id")) if stadium.get("id") is not None else None,
        "stadium_name": stadium.get("name"),

        "fps": obj.get("fps"),
        "video_url": obj.get("videoUrl"),

        # Full original JSON metadata (stored as jsonb in Supabase)
        "raw": obj,
    }

def chunked(seq, size):
    # Splits a big list into smaller lists of length `size`.
    # This helps avoid sending too many rows in one request to Supabase.
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

paths = sorted(glob("data/metadata/*.json"))
print("Found files:", len(paths))
rows = [parse_one_file(p) for p in paths]

for batch in chunked(rows, 500):
    supabase.table("match_metadata").upsert(batch).execute()
    print("Uploaded batch:", len(batch))
