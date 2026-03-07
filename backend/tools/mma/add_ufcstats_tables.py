import os, sqlite3

DB_PATH = os.environ.get("MMA_HIST_DB_PATH", r"data/mma_historical.sqlite")

def main():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS mma_ufcstats_fighter_profile (
      fighter_id TEXT PRIMARY KEY,
      fighter_name TEXT,
      ufcstats_url TEXT,

      height_in REAL,
      reach_in REAL,
      stance TEXT,
      dob TEXT,

      slpm REAL,
      strapct REAL,
      sapm REAL,
      strdef REAL,

      td_avg_15 REAL,
      td_acc REAL,
      td_def REAL,

      sub_avg_15 REAL,

      updated_at TEXT DEFAULT (datetime('now'))
    )
    """)
    con.commit()
    con.close()
    print({"ok": True, "db": DB_PATH, "table": "mma_ufcstats_fighter_profile"})

if __name__ == "__main__":
    main()