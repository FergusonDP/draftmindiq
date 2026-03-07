import os
import sqlite3
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/data", tags=["data-explorer"])

# Map sports -> sqlite path (env overrides)
SPORT_DB = {
    "MMA": os.environ.get("MMA_HIST_MART_PATH", r"data/marts/mma_historical_ss_full.sqlite"),
    # Add later:
    # "NFL": os.environ.get("NFL_DB_PATH", r"data/marts/nfl.sqlite"),
    # "NBA": os.environ.get("NBA_DB_PATH", r"data/marts/nba.sqlite"),
}

# Optional: lock to safe tables per sport (prevents arbitrary SQL exposure)
ALLOWED_TABLES = {
    "MMA": {
        "mart_fighter_fight_features_v1",
        "mart_fighter_rollups_v1",
        "canon_fight_totals",
        "canon_fight_round_totals",
        "canon_strike_breakdown",
        "ss_fact_fighter_fights",
        "ss_fights",
        "ss_events",
        "ufc_fights",
        "ufc_events",
    }
}


def _db_path(sport: str) -> str:
    key = sport.strip().upper()
    if key not in SPORT_DB:
        raise HTTPException(status_code=400, detail=f"Unknown sport: {sport}")
    return SPORT_DB[key]


def _connect(db_path: str):
    if not os.path.exists(db_path):
        raise HTTPException(status_code=500, detail=f"DB not found: {db_path}")
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


@router.get("/sports")
def list_sports():
    return {"ok": True, "sports": sorted(SPORT_DB.keys())}


@router.get("/{sport}/tables")
def list_tables(sport: str):
    db = _db_path(sport)
    with _connect(db) as con:
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        ).fetchall()
        tables = [r["name"] for r in rows]

    allowed = ALLOWED_TABLES.get(sport.upper())
    if allowed:
        tables = [t for t in tables if t in allowed]

    return {"ok": True, "sport": sport.upper(), "db": db, "tables": tables}


@router.get("/{sport}/table_preview")
def table_preview(
    sport: str,
    table: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0, le=200000),
):
    sport_key = sport.upper()
    db = _db_path(sport)

    allowed = ALLOWED_TABLES.get(sport_key)
    if allowed and table not in allowed:
        raise HTTPException(status_code=400, detail=f"table not allowed for {sport_key}: {table}")

    with _connect(db) as con:
        cols = [r["name"] for r in con.execute(f"PRAGMA table_info({table});").fetchall()]
        rows = con.execute(f"SELECT * FROM {table} LIMIT ? OFFSET ?;", (limit, offset)).fetchall()
        data = [dict(r) for r in rows]

    return {
        "ok": True,
        "sport": sport_key,
        "table": table,
        "columns": cols,
        "limit": limit,
        "offset": offset,
        "rows": data,
    }


@router.get("/{sport}/search")
def search_rows(
    sport: str,
    table: str = Query(..., min_length=1),
    column: str = Query(..., min_length=1),
    q: str = Query(..., min_length=1, max_length=120),
    limit: int = Query(50, ge=1, le=500),
):
    sport_key = sport.upper()
    db = _db_path(sport)

    allowed = ALLOWED_TABLES.get(sport_key)
    if allowed and table not in allowed:
        raise HTTPException(status_code=400, detail=f"table not allowed for {sport_key}: {table}")

    with _connect(db) as con:
        # validate column exists
        cols = [r["name"] for r in con.execute(f"PRAGMA table_info({table});").fetchall()]
        if column not in cols:
            raise HTTPException(status_code=400, detail=f"unknown column: {column}")

        rows = con.execute(
            f"SELECT * FROM {table} WHERE {column} LIKE ? LIMIT ?;",
            (f"%{q.strip()}%", limit),
        ).fetchall()

    return {
        "ok": True,
        "sport": sport_key,
        "table": table,
        "column": column,
        "q": q,
        "count": len(rows),
        "rows": [dict(r) for r in rows],
    }
