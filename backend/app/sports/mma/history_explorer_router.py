from __future__ import annotations

import os
import sqlite3
import traceback
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/mma/history", tags=["mma-history"])

BASE_DIR = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = str(BASE_DIR / "data" / "mma_fighter_cards.sqlite")
DB_PATH = os.environ.get("MMA_FIGHTER_CARDS_DB_PATH", DEFAULT_DB_PATH)


def _connect():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail=f"DB not found: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _rows(rows):
    return [dict(r) for r in rows]


@router.get("/tables")
def list_tables():
    with _connect() as con:
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        ).fetchall()
    return {"ok": True, "db": DB_PATH, "tables": [r["name"] for r in rows]}


@router.get("/table_preview")
def table_preview(
    table: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=500),
):
    allowed = {
        "fighters",
        "fighter_fights",
        "fighter_rounds",
        "fighter_cards",
        "build_meta",
    }
    if table not in allowed:
        raise HTTPException(status_code=400, detail=f"table not allowed: {table}")

    with _connect() as con:
        cols = [r["name"] for r in con.execute(f"PRAGMA table_info({table});").fetchall()]
        rows = con.execute(f"SELECT * FROM {table} LIMIT ?;", (limit,)).fetchall()

    return {"ok": True, "table": table, "columns": cols, "rows": _rows(rows)}


@router.get("/fighters")
def list_fighters(
    q: str = Query("", max_length=80),
    limit: int = Query(500, ge=1, le=5000),
):
    try:
        with _connect() as con:
            q_clean = q.strip()
            if q_clean:
                rows = con.execute(
                    """
                    SELECT
                        name AS fighter,
                        fighter_id,
                        nickname,
                        stance,
                        height_in,
                        weight_lbs,
                        reach_in,
                        total_fights AS fights,
                        wins,
                        losses,
                        draws,
                        no_contests,
                        last_fight_date
                    FROM fighter_cards
                    WHERE name LIKE ? COLLATE NOCASE
                    ORDER BY name ASC
                    LIMIT ?;
                    """,
                    (f"%{q_clean}%", limit),
                ).fetchall()
            else:
                rows = con.execute(
                    """
                    SELECT
                        name AS fighter,
                        fighter_id,
                        nickname,
                        stance,
                        height_in,
                        weight_lbs,
                        reach_in,
                        total_fights AS fights,
                        wins,
                        losses,
                        draws,
                        no_contests,
                        last_fight_date
                    FROM fighter_cards
                    ORDER BY name ASC
                    LIMIT ?;
                    """,
                    (limit,),
                ).fetchall()

        return {"ok": True, "count": len(rows), "fighters": _rows(rows)}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fighter/latest")
def fighter_latest(fighter: str = Query(..., min_length=2)):
    try:
        with _connect() as con:
            row = con.execute(
                """
                SELECT *
                FROM fighter_cards
                WHERE name = ?
                LIMIT 1;
                """,
                (fighter,),
            ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="fighter not found")

        return {"ok": True, "fighter": fighter, "latest": dict(row)}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fighter_v1/profile")
def fighter_profile(fighter: str = Query(..., min_length=2)):
    try:
        with _connect() as con:
            card = con.execute(
                """
                SELECT *
                FROM fighter_cards
                WHERE name = ?
                LIMIT 1;
                """,
                (fighter,),
            ).fetchone()

            if not card:
                raise HTTPException(status_code=404, detail="fighter not found")

            fighter_id = card["fighter_id"]

            latest = con.execute(
                """
                SELECT
                    fight_id,
                    fight_url,
                    opponent_id,
                    opponent_name,
                    event_id,
                    event_name,
                    event_date,
                    weight_class,
                    method,
                    finish_round,
                    finish_time_sec,
                    is_win,
                    kd,
                    sig_landed,
                    sig_att,
                    total_landed,
                    total_att,
                    td_landed,
                    td_att,
                    sub_att,
                    rev,
                    ctrl_sec,
                    src
                FROM fighter_fights
                WHERE fighter_id = ?
                ORDER BY event_date DESC, fighter_fight_id DESC
                LIMIT 1;
                """,
                (fighter_id,),
            ).fetchone()

            fights = con.execute(
                """
                SELECT
                    fight_id,
                    fight_url,
                    opponent_id,
                    opponent_name AS opponent,
                    event_id,
                    event_name,
                    event_date,
                    weight_class,
                    method,
                    finish_round,
                    finish_time_sec AS finish_time,
                    is_win,
                    kd,
                    sig_landed,
                    sig_att AS sig_attempted,
                    total_landed,
                    total_att,
                    td_landed,
                    td_att AS td_attempted,
                    sub_att,
                    rev,
                    ctrl_sec,
                    src
                FROM fighter_fights
                WHERE fighter_id = ?
                ORDER BY event_date DESC, fighter_fight_id DESC
                LIMIT 5;
                """,
                (fighter_id,),
            ).fetchall()

        last5_fights = _rows(fights)
        n = len(last5_fights)
        wins = sum(1 for r in last5_fights if r.get("is_win") == 1)

        def avg(key: str):
            vals = [r[key] for r in last5_fights if r.get(key) is not None]
            if not vals:
                return None
            return sum(float(v) for v in vals) / len(vals)

        career_total = int(card["total_fights"] or 0)
        career_wins = int(card["wins"] or 0)

        bio = {
            "fighter": card["name"],
            "nickname": card["nickname"],
            "height_in": card["height_in"],
            "reach_in": card["reach_in"],
            "stance": card["stance"],
            "style": None,
            "weight_class": latest["weight_class"] if latest else None,
            "dob": None,
            "weight_lbs": card["weight_lbs"],
        }

        latest_rollup = {
            "fighter_id": card["fighter_id"],
            "name": card["name"],
            "nickname": card["nickname"],
            "stance": card["stance"],
            "height_in": card["height_in"],
            "weight_lbs": card["weight_lbs"],
            "reach_in": card["reach_in"],
            "total_fights": card["total_fights"],
            "wins": card["wins"],
            "losses": card["losses"],
            "draws": card["draws"],
            "no_contests": card["no_contests"],
            "career_win_rate": (career_wins / career_total) if career_total else None,
            "sig_landed_total": card["sig_landed_total"],
            "sig_att_total": card["sig_att_total"],
            "td_landed_total": card["td_landed_total"],
            "td_att_total": card["td_att_total"],
            "sub_att_total": card["sub_att_total"],
            "kd_total": card["kd_total"],
            "ctrl_sec_total": card["ctrl_sec_total"],
            "last_fight_date": card["last_fight_date"],
            "weight_class": latest["weight_class"] if latest else None,
            "event_date": latest["event_date"] if latest else None,
            "event_name": latest["event_name"] if latest else None,
            "opponent_name": latest["opponent_name"] if latest else None,
            "method": latest["method"] if latest else None,
            "finish_round": latest["finish_round"] if latest else None,
            "finish_time_sec": latest["finish_time_sec"] if latest else None,
        }

        last5_summary = {
            "n": n,
            "win_rate": (wins / n) if n else None,
            "sig_landed_avg": avg("sig_landed"),
            "sig_attempted_avg": avg("sig_attempted"),
            "td_landed_avg": avg("td_landed"),
            "td_attempted_avg": avg("td_attempted"),
            "kd_avg": avg("kd"),
            "sub_att_avg": avg("sub_att"),
            "rev_avg": avg("rev"),
            "passes_avg": None,
            "ctrl_sec_avg": avg("ctrl_sec"),
        }

        return {
            "ok": True,
            "fighter": fighter,
            "bio": bio,
            "latest_rollup": latest_rollup,
            "last5_summary": last5_summary,
            "last5_fights": last5_fights,
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fighter_v1/fight_list")
def fighter_fight_list(
    fighter: str = Query(..., min_length=2),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0, le=200000),
):
    try:
        with _connect() as con:
            fighter_row = con.execute(
                """
                SELECT fighter_id, name
                FROM fighter_cards
                WHERE name = ?
                LIMIT 1;
                """,
                (fighter,),
            ).fetchone()

            if not fighter_row:
                raise HTTPException(status_code=404, detail="fighter not found")

            rows = con.execute(
                """
                SELECT
                    event_date,
                    fight_url,
                    opponent_name AS opponent,
                    is_win,
                    weight_class,
                    method,
                    finish_round,
                    finish_time_sec AS finish_time,
                    sig_landed,
                    sig_att AS sig_attempted,
                    total_landed,
                    total_att,
                    td_landed,
                    td_att AS td_attempted,
                    kd,
                    sub_att,
                    rev,
                    NULL AS passes
                FROM fighter_fights
                WHERE fighter_id = ?
                ORDER BY event_date DESC, fighter_fight_id DESC
                LIMIT ? OFFSET ?;
                """,
                (fighter_row["fighter_id"], limit, offset),
            ).fetchall()

        return {
            "ok": True,
            "fighter": fighter,
            "limit": limit,
            "offset": offset,
            "count": len(rows),
            "fights": _rows(rows),
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fight_v1/detail")
def fight_detail(fight_url: str = Query(..., min_length=3)):
    try:
        with _connect() as con:
            meta = con.execute(
                """
                SELECT
                    fighter_name AS fighter,
                    opponent_name AS opponent,
                    is_win,
                    method,
                    finish_round AS round,
                    finish_time_sec AS time,
                    event_name,
                    event_date,
                    weight_class,
                    fight_url,
                    fight_id
                FROM fighter_fights
                WHERE fight_url = ?
                ORDER BY fighter_name ASC;
                """,
                (fight_url,),
            ).fetchall()

            if not meta:
                raise HTTPException(status_code=404, detail="fight not found")

            rounds = con.execute(
                """
                SELECT
                    fight_url,
                    fight_id,
                    fighter_name AS fighter,
                    round,
                    'sig_landed' AS stat_key,
                    sig_landed AS value,
                    sig_landed AS a_value,
                    sig_landed AS a_landed
                FROM fighter_rounds
                WHERE fight_url = ?

                UNION ALL

                SELECT
                    fight_url,
                    fight_id,
                    fighter_name AS fighter,
                    round,
                    'sig_attempted' AS stat_key,
                    sig_attempted AS value,
                    sig_attempted AS a_value,
                    sig_attempted AS a_landed
                FROM fighter_rounds
                WHERE fight_url = ?

                UNION ALL

                SELECT
                    fight_url,
                    fight_id,
                    fighter_name AS fighter,
                    round,
                    'td_landed' AS stat_key,
                    td_landed AS value,
                    td_landed AS a_value,
                    td_landed AS a_landed
                FROM fighter_rounds
                WHERE fight_url = ?

                UNION ALL

                SELECT
                    fight_url,
                    fight_id,
                    fighter_name AS fighter,
                    round,
                    'td_attempted' AS stat_key,
                    td_attempted AS value,
                    td_attempted AS a_value,
                    td_attempted AS a_landed
                FROM fighter_rounds
                WHERE fight_url = ?

                UNION ALL

                SELECT
                    fight_url,
                    fight_id,
                    fighter_name AS fighter,
                    round,
                    'kd' AS stat_key,
                    kd AS value,
                    kd AS a_value,
                    kd AS a_landed
                FROM fighter_rounds
                WHERE fight_url = ?

                UNION ALL

                SELECT
                    fight_url,
                    fight_id,
                    fighter_name AS fighter,
                    round,
                    'sub_att' AS stat_key,
                    sub_att AS value,
                    sub_att AS a_value,
                    sub_att AS a_landed
                FROM fighter_rounds
                WHERE fight_url = ?

                UNION ALL

                SELECT
                    fight_url,
                    fight_id,
                    fighter_name AS fighter,
                    round,
                    'ctrl_sec' AS stat_key,
                    ctrl_sec AS value,
                    ctrl_sec AS a_value,
                    ctrl_sec AS a_landed
                FROM fighter_rounds
                WHERE fight_url = ?

                ORDER BY round ASC, fighter ASC, stat_key ASC;
                """,
                (
                    fight_url,
                    fight_url,
                    fight_url,
                    fight_url,
                    fight_url,
                    fight_url,
                    fight_url,
                ),
            ).fetchall()

            totals = con.execute(
                """
                SELECT fighter, stat_key, SUM(value) AS value, SUM(value) AS a_value, SUM(value) AS a_landed
                FROM (
                    SELECT fighter_name AS fighter, 'sig_landed' AS stat_key, COALESCE(sig_landed, 0) AS value
                    FROM fighter_rounds
                    WHERE fight_url = ?

                    UNION ALL

                    SELECT fighter_name AS fighter, 'sig_attempted' AS stat_key, COALESCE(sig_attempted, 0) AS value
                    FROM fighter_rounds
                    WHERE fight_url = ?

                    UNION ALL

                    SELECT fighter_name AS fighter, 'td_landed' AS stat_key, COALESCE(td_landed, 0) AS value
                    FROM fighter_rounds
                    WHERE fight_url = ?

                    UNION ALL

                    SELECT fighter_name AS fighter, 'td_attempted' AS stat_key, COALESCE(td_attempted, 0) AS value
                    FROM fighter_rounds
                    WHERE fight_url = ?

                    UNION ALL

                    SELECT fighter_name AS fighter, 'kd' AS stat_key, COALESCE(kd, 0) AS value
                    FROM fighter_rounds
                    WHERE fight_url = ?

                    UNION ALL

                    SELECT fighter_name AS fighter, 'sub_att' AS stat_key, COALESCE(sub_att, 0) AS value
                    FROM fighter_rounds
                    WHERE fight_url = ?

                    UNION ALL

                    SELECT fighter_name AS fighter, 'ctrl_sec' AS stat_key, COALESCE(ctrl_sec, 0) AS value
                    FROM fighter_rounds
                    WHERE fight_url = ?
                )
                GROUP BY fighter, stat_key
                ORDER BY fighter ASC, stat_key ASC;
                """,
                (
                    fight_url,
                    fight_url,
                    fight_url,
                    fight_url,
                    fight_url,
                    fight_url,
                    fight_url,
                ),
            ).fetchall()

        return {
            "ok": True,
            "fight_url": fight_url,
            "meta": _rows(meta),
            "totals": _rows(totals),
            "rounds": _rows(rounds),
            "strike_breakdown": [],
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
