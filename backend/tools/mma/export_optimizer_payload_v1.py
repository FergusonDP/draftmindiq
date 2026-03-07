# backend/tools/mma/export_optimizer_payload_v1.py

import os
import json
import sqlite3
from datetime import datetime

DB = r"..\..\data\marts\mma_historical_ss_full.sqlite"

ROLLUPS = "mart_fighter_rollups_v1"

# This produces one row per fighter: their most recent rollups (fight entering next bout)
# plus opponent deltas for a given matchup list.
#
# For now we’ll just export "latest per fighter" so you can join to DK slate players.


def connect():
    if not os.path.exists(DB):
        raise FileNotFoundError(DB)
    return sqlite3.connect(DB)


def main():
    t0 = datetime.now()
    con = connect()
    cur = con.cursor()

    # 1) Build "latest rollups per fighter"
    q_latest = f"""
    WITH ranked AS (
      SELECT
        r.*,
        ROW_NUMBER() OVER (PARTITION BY fighter ORDER BY fight_url DESC) AS rn
      FROM {ROLLUPS} r
    )
    SELECT
      fighter,
      fights_before,

      sig_landed_l3, sig_attempted_l3, td_landed_l3, ctrl_sec_l3, win_rate_l3,
      sig_landed_l5, sig_attempted_l5, td_landed_l5, ctrl_sec_l5, win_rate_l5,
      sig_landed_l10, sig_attempted_l10, td_landed_l10, ctrl_sec_l10, win_rate_l10,

      sig_landed_career, td_landed_career, ctrl_sec_career, win_rate_career
    FROM ranked
    WHERE rn = 1;
    """

    rows = cur.execute(q_latest).fetchall()
    cols = [d[0] for d in cur.description]

    out = [dict(zip(cols, r)) for r in rows]

    # 2) Write JSON
    out_path = r"tools\mma\data\optimizer_payload_latest_rollups_v1.json"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "ok": True,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "db": DB,
                "table": ROLLUPS,
                "count": len(out),
                "data": out,
            },
            f,
            ensure_ascii=False,
        )

    con.close()

    print("Wrote:", out_path)
    print("Rows:", len(out))
    print("Done in", datetime.now() - t0)


if __name__ == "__main__":
    main()
