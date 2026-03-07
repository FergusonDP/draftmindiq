# backend/app/sports/mma/dk/optimizer.py

from __future__ import annotations

import itertools
from typing import Any, Dict, List, Tuple


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def player_score(
    p: Dict[str, Any],
    mode: str = "gpp",
    ownership_weight: float = 0.0,
) -> float:
    mean = _f(p.get("proj_mean"))
    floor = _f(p.get("proj_floor"))
    ceil = _f(p.get("proj_ceiling"))
    val = _f(p.get("value"))
    risk = _f(p.get("risk"))
    p_win = _f(p.get("p_win"))
    finish_eq = _f(p.get("finish_equity"))

    own = _f(p.get("own_proj"))
    leverage_bonus = 0.0
    if ownership_weight > 0 and own > 0:
        leverage_bonus = ownership_weight * ((ceil + mean) / max(1.0, own))

    if mode == "cash":
        return floor * 0.95 + mean * 0.35 + val * 1.10 + p_win * 18.0 - risk * 10.0

    # gpp
    return (
        mean * 1.00
        + ceil * 0.30
        + val * 1.50
        + p_win * 14.0
        + finish_eq * 22.0
        + leverage_bonus
        - risk * 12.0
    )


def _lineup_key(lineup: List[Dict[str, Any]]) -> Tuple[str, ...]:
    return tuple(sorted(str(x.get("player_id") or "") for x in lineup))


def _fight_ids(lineup: List[Dict[str, Any]]) -> List[str]:
    return [str(x.get("fight_id") or "") for x in lineup]


def _avg_risk(lineup: List[Dict[str, Any]]) -> float:
    if not lineup:
        return 0.0
    return sum(_f(x.get("risk")) for x in lineup) / len(lineup)


def _total_ownership(lineup: List[Dict[str, Any]]) -> float:
    return sum(_f(x.get("own_proj")) for x in lineup)


def build_lineups_bruteforce(
    players: List[Dict[str, Any]],
    *,
    n_lineups: int = 20,
    mode: str = "gpp",
    salary_cap: int = 50000,
    salary_floor: int = 47000,
    roster_size: int = 6,
    allow_fight_stack: bool = False,
    max_avg_risk: float = 0.42,
    ownership_weight: float = 0.0,
    min_unique_players: int = 1,
    max_exposure: float = 0.65,
) -> Dict[str, Any]:
    pool: List[Dict[str, Any]] = []

    for p in players:
        pid = str(p.get("player_id") or "")
        fid = str(p.get("fight_id") or "")
        sal = _i(p.get("salary"))

        if not pid or not fid or sal <= 0:
            continue

        pp = dict(p)
        pp["_opt_score"] = player_score(pp, mode=mode, ownership_weight=ownership_weight)
        pool.append(pp)

    best: List[Tuple[float, int, float, List[Dict[str, Any]]]] = []

    for combo in itertools.combinations(pool, roster_size):
        sal_sum = sum(_i(x.get("salary")) for x in combo)
        if sal_sum > salary_cap or sal_sum < salary_floor:
            continue

        if not allow_fight_stack:
            fids = _fight_ids(list(combo))
            if len(set(fids)) != len(fids):
                continue

        avg_risk = _avg_risk(list(combo))
        if avg_risk > max_avg_risk:
            continue

        score_sum = sum(_f(x.get("_opt_score")) for x in combo)
        total_own = _total_ownership(list(combo))

        best.append((score_sum, sal_sum, total_own, list(combo)))

    best.sort(key=lambda x: (x[0], x[1]), reverse=True)

    out_lineups: List[Dict[str, Any]] = []
    seen: List[Tuple[str, ...]] = []
    exposure_counts: Dict[str, int] = {}
    max_count_per_fighter = max(1, int(round(n_lineups * max_exposure)))

    for score_sum, sal_sum, total_own, lineup in best:
        key = _lineup_key(lineup)

        keep = True
        for prev in seen:
            overlap = len(set(key) & set(prev))
            if overlap > roster_size - min_unique_players:
                keep = False
                break
        if not keep:
            continue

        # exposure gate
        blocked = False
        for p in lineup:
            pid = str(p.get("player_id") or "")
            if exposure_counts.get(pid, 0) >= max_count_per_fighter:
                blocked = True
                break
        if blocked:
            continue

        seen.append(key)

        for p in lineup:
            pid = str(p.get("player_id") or "")
            exposure_counts[pid] = exposure_counts.get(pid, 0) + 1

        total_mean = sum(_f(x.get("proj_mean")) for x in lineup)
        total_floor = sum(_f(x.get("proj_floor")) for x in lineup)
        total_ceil = sum(_f(x.get("proj_ceiling")) for x in lineup)
        avg_risk = _avg_risk(lineup)

        out_lineups.append(
            {
                "score": round(score_sum, 4),
                "salary": sal_sum,
                "total_mean": round(total_mean, 2),
                "total_floor": round(total_floor, 2),
                "total_ceiling": round(total_ceil, 2),
                "total_ownership": round(total_own, 2),
                "avg_risk": round(avg_risk, 4),
                "players": [
                    {
                        "player_name": x.get("player_name"),
                        "player_id": x.get("player_id"),
                        "fighter_id": x.get("fighter_id"),
                        "salary": x.get("salary"),
                        "fight_id": x.get("fight_id"),
                        "opponent_name": x.get("opponent_name"),
                        "proj_mean": x.get("proj_mean"),
                        "proj_floor": x.get("proj_floor"),
                        "proj_ceiling": x.get("proj_ceiling"),
                        "value": x.get("value"),
                        "risk": x.get("risk"),
                        "p_win": x.get("p_win"),
                        "finish_equity": x.get("finish_equity"),
                        "own_proj": x.get("own_proj"),
                    }
                    for x in sorted(lineup, key=lambda y: _f(y.get("proj_mean")), reverse=True)
                ],
            }
        )

        if len(out_lineups) >= n_lineups:
            break

    exposure_summary = []
    for p in pool:
        pid = str(p.get("player_id") or "")
        cnt = exposure_counts.get(pid, 0)
        if cnt > 0:
            exposure_summary.append(
                {
                    "player_name": p.get("player_name"),
                    "player_id": pid,
                    "count": cnt,
                    "exposure": round(cnt / max(1, len(out_lineups)), 4),
                }
            )

    exposure_summary.sort(key=lambda x: (-x["count"], x["player_name"]))

    return {
        "ok": True,
        "mode": mode,
        "salary_cap": salary_cap,
        "salary_floor": salary_floor,
        "roster_size": roster_size,
        "allow_fight_stack": allow_fight_stack,
        "max_avg_risk": max_avg_risk,
        "max_exposure": max_exposure,
        "count": len(out_lineups),
        "lineups": out_lineups,
        "exposures": exposure_summary,
    }
