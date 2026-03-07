# backend/tools/mma/build_mma_historical_db.py

from __future__ import annotations

import os
import sqlite3
import pandas as pd

DB_PATH = r"data/mma_historical.sqlite"
BASE = r"tools\mma\archives\data"

FILES = {
    "fighters": os.path.join(BASE, "fighters.csv"),
    "events": os.path.join(BASE, "events.csv"),
    "fights": os.path.join(BASE, "event_data.csv"),
    "fight_fighter_stats": os.path.join(BASE, "fighter_stats.csv"),
    "fight_features_v1": os.path.join(BASE, "feature_1.csv"),
    "fight_features_v2": os.path.join(BASE, "feature_2.csv"),
    "fight_features_v3": os.path.join(BASE, "feature_3.csv"),
}


def connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con


def _require_file(path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing CSV: {path}")


def main() -> None:
    # Validate inputs up front
    for _, path in FILES.items():
        _require_file(path)

    with connect() as con:
        cur = con.cursor()

        # Drop existing tables to rebuild clean
        for table in FILES.keys():
            cur.execute(f"DROP TABLE IF EXISTS {table};")

        # Load CSVs
        for table, path in FILES.items():
            print(f"Loading {table} from {path} ...")
            df = pd.read_csv(path)
            df.to_sql(table, con, if_exists="replace", index=False)

        # Compatibility views (so older code can SELECT FROM feature_2 / feature_3)
        cur.execute("CREATE VIEW IF NOT EXISTS feature_2 AS SELECT * FROM fight_features_v2;")
        cur.execute("CREATE VIEW IF NOT EXISTS feature_3 AS SELECT * FROM fight_features_v3;")

        # Core indexes (best-effort: won’t hard fail if a column name differs)
        def safe_index(sql: str) -> None:
            try:
                cur.execute(sql)
            except Exception as e:
                print("WARN (index skipped):", sql, "->", repr(e))

        safe_index("CREATE INDEX IF NOT EXISTS idx_fighter_name ON fighters(name);")
        safe_index("CREATE INDEX IF NOT EXISTS idx_event_date ON events(date);")

        safe_index("CREATE INDEX IF NOT EXISTS idx_fights_event ON fights(event_id);")
        safe_index("CREATE INDEX IF NOT EXISTS idx_fights_r ON fights(r_id);")
        safe_index("CREATE INDEX IF NOT EXISTS idx_fights_b ON fights(b_id);")
        safe_index("CREATE INDEX IF NOT EXISTS idx_fights_winner ON fights(winner_id);")

        safe_index(
            "CREATE INDEX IF NOT EXISTS idx_stats_fighter ON fight_fighter_stats(fighter_id);"
        )

        safe_index("CREATE INDEX IF NOT EXISTS idx_v1_fight ON fight_features_v1(fight_id);")
        safe_index("CREATE INDEX IF NOT EXISTS idx_v2_fight ON fight_features_v2(fight_id);")
        safe_index("CREATE INDEX IF NOT EXISTS idx_v3_fight ON fight_features_v3(fight_id);")

        con.commit()

    print(f"MMA Historical Database Built Successfully: {DB_PATH}")


if __name__ == "__main__":
    main()
