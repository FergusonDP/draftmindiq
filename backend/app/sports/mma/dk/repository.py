# backend/app/sports/mma/dk/repository.py

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from app.sports.mma.dk.db import connect, slate_db_path


def ensure_slate_schema() -> None:
    with connect(slate_db_path()) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS mma_dk_slates (
            slate_id TEXT PRIMARY KEY,
            slate_name TEXT,
            slate_date TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS mma_dk_slate_players (
            slate_id TEXT NOT NULL,
            player_id TEXT NOT NULL,
            player_name TEXT,
            roster_position TEXT,
            salary INTEGER,
            avg_points_per_game REAL,
            game_info TEXT,
            raw_json TEXT,
            PRIMARY KEY (slate_id, player_id)
        )
        """)


def list_slates(limit: int = 50) -> List[Dict[str, Any]]:
    ensure_slate_schema()
    with connect(slate_db_path()) as con:
        rows = con.execute(
            """
            SELECT slate_id, slate_name, slate_date, created_at
            FROM mma_dk_slates
            ORDER BY COALESCE(slate_date, created_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_dk_slate_rows(slate_id: str) -> List[Dict[str, Any]]:
    ensure_slate_schema()
    with connect(slate_db_path()) as con:
        rows = con.execute(
            """
            SELECT slate_id, player_id, player_name, roster_position, salary,
                   avg_points_per_game, game_info
            FROM mma_dk_slate_players
            WHERE slate_id=?
            ORDER BY salary DESC, player_name ASC
            """,
            (slate_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def upsert_slate_players(
    slate_id: str,
    slate_name: Optional[str],
    slate_date: Optional[str],
    players: List[Dict[str, Any]],
) -> Dict[str, Any]:
    ensure_slate_schema()
    inserted = 0
    with connect(slate_db_path()) as con:
        con.execute(
            """
            INSERT INTO mma_dk_slates(slate_id, slate_name, slate_date)
            VALUES (?, ?, ?)
            ON CONFLICT(slate_id) DO UPDATE SET
              slate_name=excluded.slate_name,
              slate_date=excluded.slate_date
            """,
            (slate_id, slate_name, slate_date),
        )
        for p in players:
            raw = p.get("raw_json")
            if raw is not None and not isinstance(raw, str):
                raw = json.dumps(raw)
            con.execute(
                """
                INSERT INTO mma_dk_slate_players
                (slate_id, player_id, player_name, roster_position, salary, avg_points_per_game, game_info, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slate_id, player_id) DO UPDATE SET
                  player_name=excluded.player_name,
                  roster_position=excluded.roster_position,
                  salary=excluded.salary,
                  avg_points_per_game=excluded.avg_points_per_game,
                  game_info=excluded.game_info,
                  raw_json=excluded.raw_json
                """,
                (
                    slate_id,
                    str(p.get("player_id", "")),
                    p.get("player_name"),
                    p.get("roster_position") or "FLEX",
                    p.get("salary"),
                    p.get("avg_points_per_game"),
                    p.get("game_info"),
                    raw,
                ),
            )
            inserted += 1
        con.commit()
    return {"ok": True, "slate_id": slate_id, "inserted": inserted}
