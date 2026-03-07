\
# backend/tools/mma/canon/inspect_db.py
import argparse, sqlite3, os

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row

    tabs = con.execute("SELECT type,name FROM sqlite_master WHERE type IN ('table','view') ORDER BY type,name").fetchall()
    print("DB:", args.db)
    print("Objects:", len(tabs))
    for r in tabs:
        name = r["name"]
        if name.startswith("sqlite_"):
            continue
        c = con.execute(f"SELECT COUNT(*) c FROM {name}").fetchone()["c"] if r["type"]=="table" else None
        print(f"{r['type']:5}  {name:35}  rows={c if c is not None else '-'}")

    # unmapped SS fighters
    if any(r["name"]=="ss_fighter_map" for r in tabs):
        u = con.execute("SELECT COUNT(*) c FROM ss_fighter_map WHERE fighter_id IS NULL").fetchone()["c"]
        print("\nSS unmapped fighters:", u)

    con.close()

if __name__ == "__main__":
    main()
