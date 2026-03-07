# backend/app/sports/mma/dk/csv_playerpool.py

from __future__ import annotations
from typing import Dict, Any, List

from tools.mma.dk_slate_ingest import load_dk_mma_slate, attach_temp_projection
from app.sports.mma.dk.proj_engine import project_player_from_optimizer_row


def build_playerpool_from_csv(csv_path: str, slate_id: str) -> Dict[str, Any]:
    out = load_dk_mma_slate(csv_path)
    if not out.get("ok"):
        return {"ok": False, "error": "dk_slate_ingest_failed", "detail": out}

    # start with baseline proj so we always have something
    fighters = attach_temp_projection(out["fighters"])

    # compute model-based projections
    projected = []
    proj_ok = 0
    proj_fail = 0

    for f in fighters:
        # IMPORTANT: shape row for proj_engine
        row = {
            "slate_id": slate_id,
            "player_name": f["name"],
            "game_info": f.get("game_info", ""),  # comes from ingest
            "salary": f["salary"],
        }

        try:
            p = project_player_from_optimizer_row(row)
            # attach results; proj_points becomes the optimizer projection
            f2 = dict(f)
            f2.update(p)
            f2["proj"] = float(p.get("proj_points", f2.get("proj", 0.0)))
            projected.append(f2)
            proj_ok += 1
        except Exception as e:
            # keep baseline projection if model fails
            f2 = dict(f)
            f2["proj_engine_error"] = repr(e)
            projected.append(f2)
            proj_fail += 1

    # group into fight blocks
    fights_map = {}
    for f in projected:
        fights_map.setdefault(f["fight_id"], []).append(f)

    fights: List[Dict[str, Any]] = []
    for fight_id, rows in fights_map.items():
        rows = sorted(rows, key=lambda r: r["salary"], reverse=True)
        fights.append(
            {
                "slate_id": slate_id,
                "fight_id": fight_id,
                "fighters": [
                    {
                        "player_id": r["player_id"],
                        "name": r["name"],
                        "salary": r["salary"],
                        "team": r.get("team"),
                        "game_info": r.get("game_info"),
                        "avg_points": r.get("avg_points", 0.0),
                        # optimizer uses this
                        "proj": float(r.get("proj", 0.0)),
                        # extra fields for UI / analysis
                        "floor": r.get("floor"),
                        "ceiling": r.get("ceiling"),
                        "stdev": r.get("stdev"),
                        "p_win": r.get("p_win"),
                        "p_finish_given_win": r.get("p_finish_given_win"),
                        "finish_equity": r.get("finish_equity"),
                        "value_per_1k": r.get("value_per_1k"),
                        "opponent_name": r.get("opponent_name"),
                        "edge_note": r.get("edge_note"),
                    }
                    for r in rows
                ],
            }
        )

    fights = sorted(fights, key=lambda x: x["fight_id"])

    return {
        "ok": True,
        "action": "mma_dk_playerpool_from_csv",
        "slate_id": slate_id,
        "meta": {
            "fighters": len(projected),
            "fights": len(fights),
            "proj_ok": proj_ok,
            "proj_fail": proj_fail,
        },
        "fights": fights,
    }
