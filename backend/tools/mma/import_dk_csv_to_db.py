import pandas as pd
from app.sports.mma.dk.repository import upsert_slate_players


def import_dk_csv(csv_path: str, slate_id: str, slate_name: str = None, slate_date: str = None):
    df = pd.read_csv(csv_path)

    players = []
    for _, r in df.iterrows():
        players.append(
            {
                "player_id": str(r.get("ID", "")),
                "player_name": str(r.get("Name", "")).strip(),
                "roster_position": str(r.get("Roster Position", "F")).strip(),
                "salary": int(r.get("Salary", 0) or 0),
                "avg_points_per_game": float(r.get("AvgPointsPerGame", 0.0) or 0.0),
                "game_info": str(r.get("Game Info", "")).strip(),
                "raw_json": {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()},
            }
        )

    return upsert_slate_players(
        slate_id=slate_id,
        slate_name=slate_name,
        slate_date=slate_date,
        players=players,
    )
