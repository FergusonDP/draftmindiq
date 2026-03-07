import pandas as pd

csv_path = "data/dk_slates/mma/DK_UFC_3_7_2026.csv"

def load_dk_slate(csv_path):
    df = pd.read_csv(csv_path)

    fighters = []

    for _, r in df.iterrows():

        fight = r["Game Info"]

        fighters.append(
            {
                "player_id": str(r["ID"]),
                "name": r["Name"],
                "salary": int(r["Salary"]),
                "fight_id": fight,
                "proj": float(r["AvgPointsPerGame"]),  # temporary projection
            }
        )

    return fighters
