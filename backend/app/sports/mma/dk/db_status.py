from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List

from app.sports.mma.dk.db import hist_db_path, slate_db_path


def _fmt_dt(ts: float | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _table_count(con: sqlite3.Connection, table: str) -> int | None:
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except Exception:
        return None


def _exists_table(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def _db_info(key: str, label: str, path: str, tables: List[str]) -> Dict[str, Any]:
    exists = os.path.exists(path)
    info: Dict[str, Any] = {
        "key": key,
        "label": label,
        "path": path,
        "exists": exists,
        "size_bytes": os.path.getsize(path) if exists else 0,
        "modified_at": _fmt_dt(os.path.getmtime(path)) if exists else None,
        "tables": [],
        "row_summary": None,
    }

    if not exists:
        return info

    con = sqlite3.connect(path)
    try:
        counts = []
        for t in tables:
            c = _table_count(con, t)
            info["tables"].append({"name": t, "count": c})
            if c is not None:
                counts.append(c)

        if counts:
            info["row_summary"] = sum(counts)
    finally:
        con.close()

    return info


def get_mma_db_status() -> Dict[str, Any]:
    beast_path = os.environ.get("MMA_BEAST_DB_PATH", r"data/beast.sqlite")
    ss_path = os.environ.get("MMA_SS_DB_PATH", r"data/marts/mma_historical_ss_full.sqlite")
    hist_path = hist_db_path()
    dayof_path = slate_db_path()

    dbs = [
        _db_info(
            "beast",
            "Beast",
            beast_path,
            [
                "dim_fighter",
                "dim_fight",
                "fact_fighter_fight_stats",
                "fact_fighter_round_stats",
                "mma_model_coeffs",
                "mma_name_aliases",
            ],
        ),
        _db_info(
            "ss_full",
            "SS Historical Full",
            ss_path,
            [
                "ss_events",
                "ss_fights",
                "ss_fight_html",
                "canon_fights",
                "ss_fact_fighter_fights",
                "opt_fighter_fight_ready_v1",
            ],
        ),
        _db_info(
            "historical",
            "Historical Legacy",
            hist_path,
            [
                "fighters",
                "events",
                "fights",
                "fight_fighter_stats",
                "fight_features_v2",
                "fight_features_v3",
            ],
        ),
        _db_info(
            "dayof",
            "Day-Of Slate",
            dayof_path,
            [
                "mma_dk_slates",
                "mma_dk_slate_players",
            ],
        ),
    ]

    flags = {
        "manual_fighters_patched": False,
        "models_ready": False,
        "alias_count": None,
        "latest_slate_player_rows": None,
        "optimizer_ready_rows": None,
    }

    if os.path.exists(beast_path):
        con = sqlite3.connect(beast_path)
        try:
            flags["manual_fighters_patched"] = (
                _table_count(
                    con,
                    "(SELECT fighter_id FROM dim_fighter WHERE fighter_id IN ('3a99827145848851','fe061d54f96c5e19'))",
                )
                is not None
            )
        except Exception:
            pass

        try:
            if _exists_table(con, "mma_model_coeffs"):
                flags["models_ready"] = _table_count(con, "mma_model_coeffs") >= 2
        except Exception:
            pass

        try:
            flags["alias_count"] = _table_count(con, "mma_name_aliases")
        except Exception:
            pass

        con.close()

    if os.path.exists(dayof_path):
        con = sqlite3.connect(dayof_path)
        try:
            flags["latest_slate_player_rows"] = _table_count(con, "mma_dk_slate_players")
        except Exception:
            pass
        con.close()

    if os.path.exists(ss_path):
        con = sqlite3.connect(ss_path)
        try:
            flags["optimizer_ready_rows"] = _table_count(con, "opt_fighter_fight_ready_v1")
        except Exception:
            pass
        con.close()

    return {"ok": True, "databases": dbs, "flags": flags}
