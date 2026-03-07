from __future__ import annotations

import os
import sqlite3
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/mma/cards", tags=["MMA Cards"])

DB_PATH = os.environ.get("MMA_FIGHTER_CARDS_PATH", r"data/mma_fighter_cards.sqlite")


def _connect():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail=f"DB not found: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


@router.get("/fighters")
def list_fighters(
    q: str = Query("", min_length=0),
    limit: int = Query(500, ge=1, le=2000),
):
    like = f"%{q.strip()}%"
    with _connect() as con:
        rows = con.execute(
            """
            SELECT fighter_id, name, nickname
            FROM fighters
            WHERE name LIKE ?
            ORDER BY name ASC
            LIMIT ?
            """,
            (like, limit),
        ).fetchall()

    return {
        "ok": True,
        "fighters": [dict(r) for r in rows],
        "count": len(rows),
    }


@router.get("/fighter")
def get_fighter(fighter_id: str = Query(..., min_length=4)):
    with _connect() as con:
        row = con.execute(
            """
            SELECT *
            FROM fighters
            WHERE fighter_id = ?
            """,
            (fighter_id,),
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Fighter not found")

    return {
        "ok": True,
        "fighter": dict(row),
    }


@router.get("/fighter/fights")
def get_fighter_fights(
    fighter_id: str = Query(..., min_length=4),
    limit: int = Query(250, ge=1, le=1000),
    offset: int = Query(0, ge=0, le=200000),
):
    with _connect() as con:
        rows = con.execute(
            """
            SELECT *
            FROM fighter_fights
            WHERE fighter_id = ?
            ORDER BY event_date DESC, fighter_fight_id DESC
            LIMIT ? OFFSET ?
            """,
            (fighter_id, limit, offset),
        ).fetchall()

    return {
        "ok": True,
        "fighter_id": fighter_id,
        "fights": [dict(r) for r in rows],
        "count": len(rows),
        "limit": limit,
        "offset": offset,
    }


@router.get("/fight/detail")
def get_fight_detail(fight_url: str = Query(..., min_length=8)):
    with _connect() as con:
        meta = con.execute(
            """
            SELECT *
            FROM fighter_fights
            WHERE fight_url = ?
            ORDER BY fighter_name ASC
            """,
            (fight_url,),
        ).fetchall()

        rounds = con.execute(
            """
            SELECT *
            FROM fighter_rounds
            WHERE fight_url = ?
            ORDER BY round ASC, fighter_name ASC
            """,
            (fight_url,),
        ).fetchall()

    if not meta:
        raise HTTPException(status_code=404, detail="Fight not found")

    return {
        "ok": True,
        "meta": [dict(r) for r in meta],
        "rounds": [dict(r) for r in rounds],
    }
