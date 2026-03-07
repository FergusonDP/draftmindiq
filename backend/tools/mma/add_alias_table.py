import os, sqlite3

DAYOF_DB = os.environ.get("MMA_DAYOF_DB_PATH", r"data/mma_dayof.sqlite")

def main():
    con = sqlite3.connect(DAYOF_DB)
    con.execute("""
    CREATE TABLE IF NOT EXISTS mma_name_aliases (
      source TEXT NOT NULL,          -- e.g. 'dk'
      raw_name TEXT NOT NULL,        -- e.g. 'Ryan Gandra'
      fighter_id TEXT NOT NULL,      -- matches historical fighters.id
      fighter_name TEXT,             -- canonical
      confidence REAL DEFAULT 1.0,
      created_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (source, raw_name)
    )
    """)
    con.commit()
    con.close()
    print({"ok": True, "db": DAYOF_DB, "table": "mma_name_aliases"})
if __name__ == "__main__":
    main()