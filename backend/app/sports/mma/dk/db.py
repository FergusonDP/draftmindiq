# backend/app/sports/mma/dk/db.py

from __future__ import annotations
import os
import sqlite3


def hist_db_path() -> str:
    # Beast DB is the canonical modeling source for MMA projections
    return os.environ.get("MMA_HIST_DB_PATH", "data/beast.sqlite")


def slate_db_path() -> str:
    return os.environ.get("MMA_SLATEDB_PATH", "data/mma_dayof.sqlite")


def connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    return con
