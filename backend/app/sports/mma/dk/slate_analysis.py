# backend/app/sports/mma/dk/slate_analysis.py

from __future__ import annotations

import math
from typing import Any, Dict, List


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _percentile(sorted_vals: List[float], p: float) -> float:
    # p in [0, 1]
    if not sorted_vals:
        return 0.0
    if p <= 0:
        return float(sorted_vals[0])
    if p >= 1:
        return float(sorted_vals[-1])
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return float(d0 + d1)


def _tier_by_value(v: float, p75: float, p50: float, p25: float) -> str:
    if v >= p75:
        return "Elite"
    if v >= p50:
        return "Strong"
    if v >= p25:
        return "OK"
    return "Bad"


def _tier_by_risk(r: float) -> str:
    # risk ~ 0.20–0.45 in Slice 9
    if r <= 0.27:
        return "Low"
    if r <= 0.33:
        return "Med"
    return "High"


def _tier_by_salary(s: int) -> str:
    # DK MMA typical salary bands
    if s <= 7200:
        return "Punt"
    if s <= 8600:
        return "Mid"
    return "Stud"


def _tier_by_own(own: float) -> str:
    # DK MMA rough tiers (tune later)
    if own >= 35.0:
        return "Mega"
    if own >= 22.0:
        return "High"
    if own >= 12.0:
        return "Mid"
    return "Low"


def normalize_ownership_inplace(
    players: List[Dict[str, Any]],
    target_sum: float = 260.0,
    max_own: float = 55.0,
    min_own: float = 0.5,
) -> None:
    """
    Deterministic slate-wide ownership normalization.
    Mutates players in-place.

    Assumes each player may already have:
      - own_proj (float, percent)
      - proj_mean / proj_ceiling / value

    Produces:
      - own_proj (normalized)
      - own_tier
      - lev_score / pivot_score recomputed off normalized ownership
    """
    owns = [_safe_float(p.get("own_proj"), 0.0) for p in players]
    total = sum(owns)

    if total <= 0.0:
        # No ownership present; still ensure fields exist
        for p in players:
            own = _safe_float(p.get("own_proj"), 0.0)
            p["own_proj"] = round(own, 2)
            p["own_tier"] = _tier_by_own(own)
        return

    scale = target_sum / total

    for p in players:
        raw = _safe_float(p.get("own_proj"), 0.0)
        adj = raw * scale
        adj = max(min_own, min(max_own, adj))
        p["own_proj"] = round(adj, 2)
        p["own_tier"] = _tier_by_own(adj)

    # Recompute leverage/pivot off normalized ownership so rankings are meaningful
    for p in players:
        own = max(_safe_float(p.get("own_proj"), 0.0), 1.0)
        ceil = _safe_float(p.get("proj_ceiling"), 0.0)
        mean = _safe_float(p.get("proj_mean"), 0.0)
        val = _safe_float(p.get("value"), 0.0)

        lev = (0.65 * ceil + 0.35 * mean) * (0.5 + val / 12.0) / own
        piv = (0.70 * mean + 0.30 * ceil) / own

        p["lev_score"] = round(lev, 4)
        p["pivot_score"] = round(piv, 4)


def analyze_mma_dk_slate(players: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Pure function: takes 'players' (already matched, already projected)
    returns shark-style slate analysis.

    Ownership:
      - If players contain own_proj, this function normalizes it slate-wide and adds ownership leaders + fight chalk signals.
      - If not, it still returns the normal analysis without breaking.
    """
    # --- ownership normalization (safe if missing) ---
    normalize_ownership_inplace(players, target_sum=260.0, max_own=55.0)

    # --- collect arrays for percentile thresholds ---
    values = sorted([_safe_float(p.get("value")) for p in players])
    means = sorted([_safe_float(p.get("proj_mean")) for p in players])
    ceilings = sorted([_safe_float(p.get("proj_ceiling")) for p in players])
    floors = sorted([_safe_float(p.get("proj_floor")) for p in players])
    risks = sorted([_safe_float(p.get("risk")) for p in players])
    owns = sorted([_safe_float(p.get("own_proj")) for p in players])

    v25, v50, v75 = (
        _percentile(values, 0.25),
        _percentile(values, 0.50),
        _percentile(values, 0.75),
    )
    c75, c90 = _percentile(ceilings, 0.75), _percentile(ceilings, 0.90)
    f75 = _percentile(floors, 0.75)

    o50, o75, o90 = (
        _percentile(owns, 0.50),
        _percentile(owns, 0.75),
        _percentile(owns, 0.90),
    )

    # --- per player tags ---
    enriched: List[Dict[str, Any]] = []
    for p in players:
        salary = _safe_int(p.get("salary"))
        v = _safe_float(p.get("value"))
        c = _safe_float(p.get("proj_ceiling"))
        f = _safe_float(p.get("proj_floor"))
        r = _safe_float(p.get("risk"))

        tier_value = _tier_by_value(v, v75, v50, v25)
        tier_salary = _tier_by_salary(salary)
        tier_risk = _tier_by_risk(r)

        # ceiling tiers: use slate percentiles
        if c >= c90:
            tier_ceiling = "SlateBreaker"
        elif c >= c75:
            tier_ceiling = "GPP"
        elif c >= _percentile(ceilings, 0.50):
            tier_ceiling = "Neutral"
        else:
            tier_ceiling = "Low"

        # floor tiers
        tier_floor = "Safe" if f >= f75 else ("OK" if f >= _percentile(floors, 0.50) else "Shaky")

        enriched.append(
            {
                **p,
                "tier_salary": tier_salary,
                "tier_value": tier_value,
                "tier_ceiling": tier_ceiling,
                "tier_floor": tier_floor,
                "tier_risk": tier_risk,
            }
        )

    # --- fight aggregation ---
    fights: Dict[str, List[Dict[str, Any]]] = {}
    for p in enriched:
        fid = str(p.get("fight_id") or "")
        if not fid:
            continue
        fights.setdefault(fid, []).append(p)

    fight_cards: List[Dict[str, Any]] = []
    for fid, ps in fights.items():
        # Expect 2 fighters; if not, still handle.
        ps_sorted = sorted(ps, key=lambda x: str(x.get("side") or ""))
        a = ps_sorted[0] if len(ps_sorted) > 0 else {}
        b = ps_sorted[1] if len(ps_sorted) > 1 else {}

        a_mean = _safe_float(a.get("proj_mean"))
        b_mean = _safe_float(b.get("proj_mean"))
        a_ceil = _safe_float(a.get("proj_ceiling"))
        b_ceil = _safe_float(b.get("proj_ceiling"))
        a_val = _safe_float(a.get("value"))
        b_val = _safe_float(b.get("value"))

        a_own = _safe_float(a.get("own_proj"))
        b_own = _safe_float(b.get("own_proj"))
        fight_own_sum = a_own + b_own
        chalk_side = "A" if a_own >= b_own else "B"
        chalk_own = max(a_own, b_own)

        fight_mean = a_mean + b_mean
        fight_ceiling = max(a_ceil, b_ceil)  # DK MMA lineup ceiling usually driven by winner
        # stack_score is a heuristic placeholder (evolve later with odds/finish probs)
        stack_score = (a_mean + b_mean) * 0.6 + (a_ceil + b_ceil) * 0.4

        flags: List[str] = []
        if max(a_ceil, b_ceil) >= c90:
            flags.append("HighCeilingWinner")
        if max(a_val, b_val) >= v75:
            flags.append("ValueSide")
        if min(a_val, b_val) <= v25 and max(a_val, b_val) <= v50:
            flags.append("LowValueFight")

        # Ownership-based flags (shark)
        if fight_own_sum >= 55.0:
            flags.append("ChalkFight")
        if chalk_own >= 35.0:
            flags.append(f"ChalkSide:{chalk_side}")

        fight_cards.append(
            {
                "fight_id": fid,
                "fight_time": a.get("fight_time") or b.get("fight_time"),
                "a_name": a.get("player_name"),
                "a_salary": a.get("salary"),
                "a_mean": round(a_mean, 2),
                "a_ceiling": round(a_ceil, 2),
                "a_value": round(a_val, 3),
                "a_own": round(a_own, 2),
                "b_name": b.get("player_name"),
                "b_salary": b.get("salary"),
                "b_mean": round(b_mean, 2),
                "b_ceiling": round(b_ceil, 2),
                "b_value": round(b_val, 3),
                "b_own": round(b_own, 2),
                "fight_mean": round(fight_mean, 2),
                "fight_ceiling_winner": round(fight_ceiling, 2),
                "stack_score": round(stack_score, 2),
                "fight_own_sum": round(fight_own_sum, 2),
                "chalk_side": chalk_side,
                "chalk_own": round(chalk_own, 2),
                "flags": flags,
            }
        )

    # --- ranked lists ---
    def top_n(key: str, n: int = 10, reverse: bool = True) -> List[Dict[str, Any]]:
        return sorted(enriched, key=lambda x: _safe_float(x.get(key)), reverse=reverse)[:n]

    report = {
        "summary": {
            "count": len(players),
            "value_p25": round(v25, 3),
            "value_p50": round(v50, 3),
            "value_p75": round(v75, 3),
            "ceiling_p75": round(c75, 2),
            "ceiling_p90": round(c90, 2),
            "own_p50": round(o50, 2),
            "own_p75": round(o75, 2),
            "own_p90": round(o90, 2),
            "own_sum": round(sum(owns), 2),
        },
        "leaders": {
            "top_mean": [
                {
                    "name": p["player_name"],
                    "salary": p["salary"],
                    "mean": p["proj_mean"],
                    "value": p["value"],
                    "risk": p["risk"],
                    "own": p.get("own_proj"),
                }
                for p in top_n("proj_mean", 10)
            ],
            "top_ceiling": [
                {
                    "name": p["player_name"],
                    "salary": p["salary"],
                    "ceiling": p["proj_ceiling"],
                    "mean": p["proj_mean"],
                    "risk": p["risk"],
                    "own": p.get("own_proj"),
                }
                for p in top_n("proj_ceiling", 10)
            ],
            "top_value": [
                {
                    "name": p["player_name"],
                    "salary": p["salary"],
                    "value": p["value"],
                    "mean": p["proj_mean"],
                    "risk": p["risk"],
                    "own": p.get("own_proj"),
                }
                for p in top_n("value", 10)
            ],
            "best_floor": [
                {
                    "name": p["player_name"],
                    "salary": p["salary"],
                    "floor": p["proj_floor"],
                    "mean": p["proj_mean"],
                    "risk": p["risk"],
                    "own": p.get("own_proj"),
                }
                for p in top_n("proj_floor", 10)
            ],
            "highest_risk": [
                {
                    "name": p["player_name"],
                    "salary": p["salary"],
                    "risk": p["risk"],
                    "ceiling": p["proj_ceiling"],
                    "value": p["value"],
                    "own": p.get("own_proj"),
                }
                for p in top_n("risk", 10)
            ],
            "top_ownership": [
                {
                    "name": p["player_name"],
                    "salary": p["salary"],
                    "own": p.get("own_proj"),
                    "tier": p.get("own_tier"),
                    "mean": p.get("proj_mean"),
                    "value": p.get("value"),
                }
                for p in top_n("own_proj", 10)
            ],
            "top_leverage": [
                {
                    "name": p["player_name"],
                    "salary": p["salary"],
                    "lev": p.get("lev_score"),
                    "own": p.get("own_proj"),
                    "tier": p.get("own_tier"),
                    "ceiling": p.get("proj_ceiling"),
                    "mean": p.get("proj_mean"),
                }
                for p in top_n("lev_score", 10)
            ],
        },
        "players_tagged": enriched,
        "fights": sorted(fight_cards, key=lambda x: x["stack_score"], reverse=True),
    }

    return report
