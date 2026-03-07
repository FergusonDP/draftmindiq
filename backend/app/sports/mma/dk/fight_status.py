from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from app.sports.mma.dk.db import connect, slate_db_path


_DATE_RE = re.compile(r"\s+\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}[AP]M\s+ET\s*$")


def _norm_name(x: Any) -> str:
    return str(x or "").strip().lower()


def _parse_game_info(game_info: str) -> Tuple[str, str, str]:
    gi = str(game_info or "").strip()
    if "@" not in gi:
        return "", "", ""
    left, right = gi.split("@", 1)
    left = left.strip()
    right = right.strip()

    fight_time = ""
    m = _DATE_RE.search(right)
    if m:
        fight_time = m.group(0).strip()
        right = _DATE_RE.sub("", right).strip()

    return left, right, fight_time


def _fight_key(a: str, b: str) -> str:
    aa = _norm_name(a)
    bb = _norm_name(b)
    return " vs ".join(sorted([aa, bb])) if aa and bb else ""


def _load_slate_fights(slate_id: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    with connect(slate_db_path()) as con:
        rows = con.execute(
            """
            SELECT player_name, game_info
            FROM mma_dk_slate_players
            WHERE slate_id=?
            ORDER BY player_name
            """,
            (slate_id,),
        ).fetchall()

    for r in rows:
        player_name = str(r["player_name"] or "").strip()
        game_info = str(r["game_info"] or "").strip()

        left, right, fight_time = _parse_game_info(game_info)
        if not left or not right:
            continue

        key = _fight_key(left, right)
        if not key:
            continue

        slot = out.setdefault(
            key,
            {
                "fight_key": key,
                "fight_time": fight_time or None,
                "fighters": set(),
                "game_infos": set(),
            },
        )
        slot["fighters"].add(player_name)
        slot["game_infos"].add(game_info)

    return out


def _load_odds_fights(slate_id: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    try:
        with connect(slate_db_path()) as con:
            odds_rows = con.execute(
                """
                SELECT fighter_name, moneyline
                FROM mma_dk_odds
                WHERE slate_id=?
                """,
                (slate_id,),
            ).fetchall()

            slate_rows = con.execute(
                """
                SELECT player_name, game_info
                FROM mma_dk_slate_players
                WHERE slate_id=?
                """,
                (slate_id,),
            ).fetchall()
    except Exception:
        return out

    name_to_game: Dict[str, str] = {}
    for r in slate_rows:
        name_to_game[_norm_name(r["player_name"])] = str(r["game_info"] or "")

    for r in odds_rows:
        fighter_name = str(r["fighter_name"] or "").strip()
        moneyline = r["moneyline"]
        game_info = name_to_game.get(_norm_name(fighter_name), "")

        left, right, fight_time = _parse_game_info(game_info)
        if not left or not right:
            continue

        key = _fight_key(left, right)
        if not key:
            continue

        slot = out.setdefault(
            key,
            {
                "fight_key": key,
                "fight_time": fight_time or None,
                "fighters": set(),
                "moneylines": [],
            },
        )
        slot["fighters"].add(fighter_name)
        if moneyline is not None:
            slot["moneylines"].append(moneyline)

    return out


def get_mma_fight_status(slate_id: str) -> Dict[str, Any]:
    slate_fights = _load_slate_fights(slate_id)
    odds_fights = _load_odds_fights(slate_id)

    all_keys = sorted(set(slate_fights.keys()) | set(odds_fights.keys()))
    fights: List[Dict[str, Any]] = []

    active_count = 0
    changed_count = 0
    scratched_count = 0

    for key in all_keys:
        s = slate_fights.get(key)
        o = odds_fights.get(key)

        slate_present = bool(s)
        odds_present = bool(o)

        slate_fighters = sorted(list(s["fighters"])) if s else []
        odds_fighters = sorted(list(o["fighters"])) if o else []

        slate_count = len(slate_fighters)
        odds_count = len(odds_fighters)

        if slate_present and odds_present and slate_count == 2:
            status = "active"
            reason = "present_in_slate_and_odds"
            active_count += 1
        elif slate_present and slate_count == 2 and not odds_present:
            status = "changed"
            reason = "present_in_slate_missing_in_odds"
            changed_count += 1
        elif not slate_present and odds_present:
            status = "changed"
            reason = "present_in_odds_missing_in_slate"
            changed_count += 1
        else:
            status = "scratched"
            reason = "incomplete_or_missing"
            scratched_count += 1

        fights.append(
            {
                "fight_key": key,
                "status": status,
                "reason": reason,
                "fight_time": (s or o or {}).get("fight_time"),
                "slate_present": slate_present,
                "odds_present": odds_present,
                "slate_fighters": slate_fighters,
                "odds_fighters": odds_fighters,
                "slate_count": slate_count,
                "odds_count": odds_count,
            }
        )

    return {
        "ok": True,
        "slate_id": slate_id,
        "summary": {
            "fight_count": len(fights),
            "active": active_count,
            "changed": changed_count,
            "scratched": scratched_count,
        },
        "fights": fights,
    }
