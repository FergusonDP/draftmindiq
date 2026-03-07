# backend/app/sports/mma/dk/match.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple


def _norm(s: Any) -> str:
    return str(s or "").strip()


GI_RE = re.compile(r"(.+?)@(.+?)\s+(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}[AP]M\s+ET)$")


def _parse_game_info(game_info: str) -> Tuple[str, str, str]:
    gi = _norm(game_info)
    m = GI_RE.match(gi)
    if not m:
        return "", "", ""
    return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()


def build_fight_map(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    ok_players: List[Dict[str, Any]] = []
    excluded = 0

    fights_map: Dict[str, List[Dict[str, Any]]] = {}

    for r in rows:
        salary = r.get("salary")
        if salary is None:
            excluded += 1
            continue

        gi = r.get("game_info") or ""
        a, b, t = _parse_game_info(str(gi))

        if a and b:
            key = " vs ".join(sorted([a.lower(), b.lower()]))
        else:
            key = f"unknown::{_norm(r.get('player_name')).lower()}"

        rr = dict(r)
        rr["fight_time"] = t or None
        rr["fight_key"] = key
        rr["_parsed_a"] = a
        rr["_parsed_b"] = b
        ok_players.append(rr)
        fights_map.setdefault(key, []).append(rr)

    fights = []
    ambiguous = 0
    unmatched = 0
    final_players: List[Dict[str, Any]] = []

    for key, group in fights_map.items():
        if key.startswith("unknown::"):
            unmatched += len(group)
            for g in group:
                gg = dict(g)
                gg["fight_id"] = None
                gg["side"] = None
                gg["opponent_name"] = None
                final_players.append(gg)
            continue

        if len(group) < 2:
            unmatched += len(group)
        elif len(group) > 2:
            ambiguous += len(group) - 2

        group = group[:2]
        fight_id = f"{group[0].get('slate_id')}:{key}"

        name0 = _norm(group[0].get("player_name"))
        name1 = _norm(group[1].get("player_name")) if len(group) > 1 else ""

        for idx, g in enumerate(group):
            gg = dict(g)

            if idx == 0:
                side = "A"
                opp_name = name1
            else:
                side = "B"
                opp_name = name0

            gg.pop("_parsed_a", None)
            gg.pop("_parsed_b", None)
            gg["fight_id"] = fight_id
            gg["side"] = side
            gg["opponent_name"] = opp_name or None
            final_players.append(gg)

        fights.append(
            {
                "fight_id": fight_id,
                "slate_id": group[0].get("slate_id"),
                "fight_time": group[0].get("fight_time"),
                "fighters": [
                    {
                        "player_id": g.get("player_id"),
                        "player_name": g.get("player_name"),
                        "salary": g.get("salary"),
                    }
                    for g in group
                ],
            }
        )

    meta = {
        "total": total,
        "ok": len(final_players),
        "excluded": excluded,
        "ambiguous": ambiguous,
        "unmatched": unmatched,
    }

    return {"meta": meta, "fights": fights, "ok_players": final_players}
