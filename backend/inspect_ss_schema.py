import sqlite3

db = r"data/marts/mma_historical_ss_full.sqlite"

con = sqlite3.connect(db)
con.row_factory = sqlite3.Row

print("DB:", db)

tables = ["canon_fight_round_totals", "canon_strike_breakdown", "canon_fight_totals"]

for t in tables:
    print("\n====", t, "====")
    try:
        rows = con.execute(f"PRAGMA table_info({t})").fetchall()
        print([(r["name"], r["type"]) for r in rows])
    except Exception as e:
        print("ERR:", e)

print("\nTables containing 'round':")
rows = con.execute(
    """
SELECT type,name
FROM sqlite_master
WHERE name LIKE '%round%'
ORDER BY type,name
"""
).fetchall()

for r in rows:
    print(r["type"], r["name"])

con.close()
