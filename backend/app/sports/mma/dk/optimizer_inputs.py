# backend/app/sports/mma/dk/optimizer_inputs.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.sports.mma.dk.match import build_fight_map
from app.sports.mma.dk.proj_engine import project_player_from_optimizer_row
from app.sports.mma.dk.repository import get_dk_slate_rows
from app.sports.mma.dk.slate_analysis import analyze_mma_dk_slate
from app.sports.mma.dk.optimizer import build_lineups_bruteforce
from app.sports.mma.dk.db import connect, slate_db_path


def _as_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _as_int(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        s = str(x).strip().replace("$", "").replace(",", "")
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def _rows_for_slate(slate_id: str) -> List[Dict[str, Any]]:
    raw_rows = get_dk_slate_rows(slate_id) or []
    rows: List[Dict[str, Any]] = []

    for r in raw_rows:
        raw_blob = r.get("raw") if isinstance(r.get("raw"), dict) else {}

        game_info = (
            _as_str(r.get("game_info"))
            or _as_str(r.get("Game Info"))
            or _as_str(raw_blob.get("Game Info"))
            or _as_str(raw_blob.get("GameInfo"))
        )

        avg_pts = (
            _as_str(r.get("avg_points_per_game"))
            or _as_str(r.get("AvgPointsPerGame"))
            or _as_str(raw_blob.get("AvgPointsPerGame"))
            or _as_str(raw_blob.get("AvgPoints Per Game"))
        )

        player_id = _as_str(r.get("player_id") or raw_blob.get("ID") or raw_blob.get("Id") or "")
        player_name = _as_str(r.get("player_name") or raw_blob.get("Name") or "")

        roster_position = _as_str(
            r.get("roster_position") or raw_blob.get("Roster Position") or "FLEX"
        )

        salary = r.get("salary")
        if salary is None:
            salary = raw_blob.get("Salary")
        salary_i = _as_int(salary)

        rows.append(
            {
                "slate_id": slate_id,
                "player_id": player_id,
                "player_name": player_name,
                "roster_position": roster_position,
                "salary": salary_i,
                "avg_points_per_game": avg_pts or None,
                "game_info": game_info or None,
            }
        )

    return rows


def _norm_name_key(x: Any) -> str:
    return str(x or "").strip().lower()


def _moneyline_to_implied_prob(ml: Any) -> Optional[float]:
    try:
        ml = int(ml)
    except Exception:
        return None

    if ml < 0:
        return (-ml) / ((-ml) + 100.0)
    if ml > 0:
        return 100.0 / (ml + 100.0)
    return None


def _get_odds_map(slate_id: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    try:
        with connect(slate_db_path()) as con:
            rows = con.execute(
                """
                SELECT fighter_name, moneyline
                FROM mma_dk_odds
                WHERE slate_id=?
                """,
                (slate_id,),
            ).fetchall()

        for r in rows:
            name = _norm_name_key(r["fighter_name"])
            out[name] = {
                "fighter_name": r["fighter_name"],
                "moneyline": r["moneyline"],
                "implied_prob": _moneyline_to_implied_prob(r["moneyline"]),
            }
    except Exception:
        return {}

    return out


def get_mma_dk_optimizer_inputs(slate_id: str) -> Dict[str, Any]:
    rows = _rows_for_slate(slate_id)
    out = build_fight_map(rows)

    odds_map = _get_odds_map(slate_id)

    for p in out["ok_players"]:
        p.update(project_player_from_optimizer_row(p))

        odds = odds_map.get(_norm_name_key(p.get("player_name")))
        if odds:
            p["moneyline"] = odds.get("moneyline")
            p["vegas_win_prob"] = odds.get("implied_prob")

            vegas_prob = odds.get("implied_prob")
            model_prob = p.get("p_win")

            if vegas_prob is not None and model_prob is not None:
                blended = 0.65 * float(vegas_prob) + 0.35 * float(model_prob)
                p["p_win"] = round(blended, 6)

            # simple ownership placeholder from vegas + finish signal
            finish_eq = float(p.get("finish_equity") or 0.0)
            base_own = (float(p.get("p_win") or 0.0) * 24.0) + (finish_eq * 18.0)
            p["own_proj"] = round(max(3.0, min(42.0, base_own)), 2)

    return {
        "ok": True,
        "action": "mma_dk_optimizer_inputs",
        "slate_id": slate_id,
        "meta": out["meta"],
        "fights": out["fights"],
        "players": out["ok_players"],
        "excluded_count": out["meta"]["excluded"],
    }


def get_mma_dk_slate_analysis(slate_id: str) -> Dict[str, Any]:
    inputs = get_mma_dk_optimizer_inputs(slate_id)
    analysis = analyze_mma_dk_slate(inputs["players"])
    return {
        "ok": True,
        "action": "mma_dk_slate_analysis",
        "slate_id": slate_id,
        "meta": inputs["meta"],
        "analysis": analysis,
    }


def get_mma_dk_optimize(slate_id: str, mode: str = "gpp") -> Dict[str, Any]:
    inputs = get_mma_dk_optimizer_inputs(slate_id)
    analysis = analyze_mma_dk_slate(inputs["players"])

    opt = build_lineups_bruteforce(
        inputs["players"],
        n_lineups=20,
        mode=mode,
        salary_cap=50000,
        salary_floor=47000,
        roster_size=6,
        allow_fight_stack=False,
        max_avg_risk=0.42 if mode == "gpp" else 0.36,
        ownership_weight=0.20 if mode == "gpp" else 0.0,
        min_unique_players=2 if mode == "gpp" else 1,
        max_exposure=0.50 if mode == "gpp" else 0.75,
    )

    return {
        "ok": True,
        "action": "mma_dk_optimize",
        "slate_id": slate_id,
        "mode": mode,
        "meta": inputs["meta"],
        "optimizer": opt,
        "analysis": analysis,
        "players": inputs["players"],  # include full player pool so UI can inspect everything
    }
