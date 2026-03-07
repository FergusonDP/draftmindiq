# tools/mma/canon/patch_manual_fighters.py

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("data/beast.sqlite")

MANUAL_FIGHTERS = [
    ("3a99827145848851", "Donte Johnson", "donte johnson"),
    ("fe061d54f96c5e19", "Rafael Tobias", "rafael tobias"),
    ("bd0a6baa4b50f160", "Luke Fernandez", "luke fernandez"),
    ("3ee69ced168ff79a", "Alberto Montes", "alberto montes"),
]

MANUAL_ALIASES = [
    ("dk", "donte johnson", "3a99827145848851"),
    ("dk", "rafael tobias", "fe061d54f96c5e19"),
    ("dk", "su sumudaerji", "3cf18e01cb6cbde3"),
    ("dk", "luke fernandez", "bd0a6baa4b50f160"),
    ("dk", "alberto montes", "3ee69ced168ff79a"),
]


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("PRAGMA foreign_keys=ON;")

        for fighter_id, name, name_norm in MANUAL_FIGHTERS:
            con.execute(
                """
                INSERT OR IGNORE INTO dim_fighter (fighter_id, name, name_norm)
                VALUES (?, ?, ?)
                """,
                (fighter_id, name, name_norm),
            )

        for source, raw_name, fighter_id in MANUAL_ALIASES:
            con.execute(
                """
                INSERT OR REPLACE INTO mma_name_aliases (source, raw_name, fighter_id)
                VALUES (?, ?, ?)
                """,
                (source, raw_name, fighter_id),
            )

        con.commit()
        print("Manual fighters and aliases patched into Beast.")

    finally:
        con.close()


if __name__ == "__main__":
    main()
