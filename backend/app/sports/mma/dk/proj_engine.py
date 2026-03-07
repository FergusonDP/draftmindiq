from __future__ import annotations

import json
import math
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from rapidfuzz import fuzz, process

from app.sports.mma.dk.db import connect, hist_db_path, slate_db_path


# ---------- Constants and utils ----------

PROJ_MEAN_MULT = 0.72
PROJ_FLOOR_MULT = 0.68
PROJ_CEILING_MULT = 0.78

P_WIN_MIN = 0.05
P_WIN_MAX = 0.95
P_FIN_MIN = 0.02
P_FIN_MAX = 0.80


# ---------- name normalization ----------

_SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", re.IGNORECASE)
_DATE_RE = re.compile(r"\s+\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}[AP]M\s+ET\s*$")


def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_suffix(name: str) -> str:
    n = _norm_name(name)
    n = _SUFFIX_RE.sub("", n).strip()
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _sigmoid(z: float) -> float:
    z = max(-50.0, min(50.0, z))
    return 1.0 / (1.0 + math.exp(-z))


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


# ---------- model loading ----------


def _load_model(con, name: str) -> Dict[str, Any]:
    row = con.execute(
        "SELECT payload_json FROM mma_model_coeffs WHERE model_name=?",
        (name,),
    ).fetchone()
    if not row:
        raise RuntimeError(
            f"Missing model {name} in mma_model_coeffs. Train models before using proj_engine."
        )
    return json.loads(row["payload_json"])


# ---------- DK slate helper ----------


def _dk_name_from_token(slate_id: str, token: str, exclude_full_name: str) -> str:
    """
    DK Game Info may use last names or partial surnames.
    Try to map token -> full DK slate name if uniquely resolvable.
    """
    token_n = _norm_name(token)
    if not token_n:
        return ""

    token_parts = token_n.split()
    if not token_parts:
        return ""

    ex = _norm_name(exclude_full_name)
    cand: List[str] = []

    with connect(slate_db_path()) as con:
        rows = con.execute(
            """
            SELECT player_name
            FROM mma_dk_slate_players
            WHERE slate_id=?
            """,
            (slate_id,),
        ).fetchall()

    for r in rows:
        full = str(r["player_name"] or "")
        fn = _norm_name(full)
        if not fn or fn == ex:
            continue

        parts = fn.split()
        if not parts:
            continue

        last = parts[-1]
        last2 = " ".join(parts[-2:]) if len(parts) >= 2 else last

        if token_n == fn or token_n == last or token_n == last2 or fn.endswith(" " + token_n):
            cand.append(full)

    return cand[0] if len(cand) == 1 else ""


def _parse_game_info_names(game_info: str) -> Tuple[str, str]:
    gi = (game_info or "").strip()
    if "@" not in gi:
        return "", ""
    left, right = gi.split("@", 1)
    left = left.strip()
    right = _DATE_RE.sub("", right.strip()).strip()
    return left, right


# ---------- alias + fighter resolution against BEAST ----------


def _lookup_alias(con, raw_name: str) -> Optional[str]:
    raw = (raw_name or "").strip()
    if not raw:
        return None

    row = con.execute(
        """
        SELECT fighter_id
        FROM mma_name_aliases
        WHERE source IN ('dk', 'ss')
          AND raw_name = ?
        LIMIT 1
        """,
        (raw,),
    ).fetchone()
    if row and row["fighter_id"]:
        return str(row["fighter_id"])

    raw_norm = _norm_name(raw)
    row = con.execute(
        """
        SELECT fighter_id
        FROM mma_name_aliases
        WHERE source IN ('dk', 'ss')
          AND raw_name = ?
        LIMIT 1
        """,
        (raw_norm,),
    ).fetchone()
    if row and row["fighter_id"]:
        return str(row["fighter_id"])

    return None


def _fighter_name_map(con) -> Dict[str, Tuple[str, str]]:
    rows = con.execute(
        """
        SELECT fighter_id, name, name_norm
        FROM dim_fighter
        """
    ).fetchall()

    name_map: Dict[str, Tuple[str, str]] = {}
    for r in rows:
        name_norm = str(r["name_norm"] or "").strip()
        if not name_norm:
            continue
        if name_norm not in name_map:
            name_map[name_norm] = (str(r["fighter_id"]), str(r["name"]))
    return name_map


def _top_name_candidates(
    name_map: Dict[str, Tuple[str, str]],
    raw_name: str,
    k: int = 5,
) -> List[Dict[str, Any]]:
    n1 = _strip_suffix(raw_name)
    choices = list(name_map.keys())
    hits = process.extract(n1, choices, scorer=fuzz.token_sort_ratio, limit=k) or []
    out: List[Dict[str, Any]] = []
    for cand, score, _ in hits:
        fid, canonical = name_map[cand]
        out.append({"id": fid, "name": canonical, "score": int(score)})
    return out


def _resolve_fighter_id(
    con,
    raw_name: str,
    *,
    want_candidates: bool = False,
    k_candidates: int = 5,
) -> Tuple[Any, ...]:
    """
    Returns:
      - default: (fighter_id, method, confidence)
      - if want_candidates=True: (fighter_id, method, confidence, candidates)
    """
    ali = _lookup_alias(con, raw_name)
    if ali:
        return (ali, "alias", 1.0, []) if want_candidates else (ali, "alias", 1.0)

    name_map = _fighter_name_map(con)

    n0 = _norm_name(raw_name)
    if n0 in name_map:
        fid, canonical = name_map[n0]
        return (fid, "exact", 1.0, []) if want_candidates else (fid, "exact", 1.0)

    n1 = _strip_suffix(raw_name)
    if n1 in name_map:
        fid, canonical = name_map[n1]
        return (fid, "exact", 0.98, []) if want_candidates else (fid, "exact", 0.98)

    choices = list(name_map.keys())
    best = process.extractOne(n1, choices, scorer=fuzz.token_sort_ratio)
    if not best:
        return (None, "none", 0.0, []) if want_candidates else (None, "none", 0.0)

    cand, score, _ = best
    conf = float(score) / 100.0
    cands = _top_name_candidates(name_map, raw_name, k=k_candidates)

    if score >= 92:
        fid, canonical = name_map[cand]
        return (fid, "fuzzy", conf, cands) if want_candidates else (fid, "fuzzy", conf)

    return (None, "none", conf, cands) if want_candidates else (None, "none", conf)


def _cand_note(fcands: List[Dict[str, Any]], ocands: List[Dict[str, Any]]) -> str:
    parts = []
    if fcands:
        parts.append(f"f_cands={fcands[:3]}")
    if ocands:
        parts.append(f"o_cands={ocands[:3]}")
    return (" " + " ".join(parts)) if parts else ""


# ---------- BEAST feature payload access ----------

F2_COLS = [
    "pre_career_sig_land_pm",
    "pre_last3_sig_land_pm",
    "pre_last5_sig_land_pm",
    "pre_career_sig_att_pm",
    "pre_last3_sig_att_pm",
    "pre_last5_sig_att_pm",
    "pre_career_td_att_pm",
    "pre_last3_td_att_pm",
    "pre_last5_td_att_pm",
    "pre_career_td_land_pm",
    "pre_last3_td_land_pm",
    "pre_last5_td_land_pm",
    "pre_career_ctrl_sec_pm",
    "pre_last3_ctrl_sec_pm",
    "pre_last5_ctrl_sec_pm",
    "pre_career_kd_pm",
    "pre_last3_kd_pm",
    "pre_last5_kd_pm",
    "pre_career_sub_att_pm",
    "pre_last3_sub_att_pm",
    "pre_last5_sub_att_pm",
    "pre_fights_count",
    "pre_days_since_last_fight",
    "pre_age_years",
    "is_5_round",
]


def _load_payload_json(s: Any) -> Dict[str, Any]:
    if s is None:
        return {}
    if isinstance(s, dict):
        return s
    text = str(s).strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _latest_f2(con, fighter_id: str) -> Dict[str, float]:
    """
    BEAST feature_2 schema:
      fight_id, fighter_id, payload_json
    There is no event_date column, so we join dim_fight to sort by fight_date.
    """
    rows = con.execute(
        """
        SELECT x.payload_json, f.fight_date
        FROM feature_2 x
        LEFT JOIN dim_fight f
          ON x.fight_id = f.fight_id
        WHERE x.fighter_id = ?
        ORDER BY f.fight_date DESC, x.fight_id DESC
        LIMIT 1
        """,
        (fighter_id,),
    ).fetchall()

    if not rows:
        return {c: 0.0 for c in F2_COLS}

    payload = _load_payload_json(rows[0]["payload_json"])
    out: Dict[str, float] = {}

    for c in F2_COLS:
        if c == "is_5_round":
            out[c] = 1.0 if bool(payload.get(c, False)) else 0.0
        else:
            out[c] = _safe_float(payload.get(c), 0.0)

    return out


def _diff_features(a: Dict[str, float], b: Dict[str, float]) -> Dict[str, float]:
    return {f"diff_{c}": float(a.get(c, 0.0)) - float(b.get(c, 0.0)) for c in F2_COLS}


# ---------- DK scoring simulation ----------


def _simulate_dk_points(
    p_win: float,
    p_finish_given_win: float,
    sig_land_pm: float,
    td_land_pm: float,
    ctrl_sec_pm: float,
    kd_pm: float,
    sub_att_pm: float,
    is_5_round: bool,
    n_sims: int = 4000,
) -> Dict[str, float]:
    rng = np.random.default_rng(7)
    max_mins = 25.0 if is_5_round else 15.0

    wins = rng.random(n_sims) < p_win
    finishes = wins & (rng.random(n_sims) < p_finish_given_win)

    mins = np.where(
        finishes,
        rng.uniform(2.5, max_mins * 0.55, n_sims),
        rng.uniform(max_mins * 0.75, max_mins, n_sims),
    )

    sig = rng.poisson(lam=np.clip(sig_land_pm, 0, 12) * mins)
    td = rng.poisson(lam=np.clip(td_land_pm, 0, 5) * mins)
    kd = rng.poisson(lam=np.clip(kd_pm, 0, 1.5) * mins)
    sub = rng.poisson(lam=np.clip(sub_att_pm, 0, 2.5) * mins)
    ctrl = np.clip(ctrl_sec_pm, 0, 240) * mins

    # current simplified DK scoring model used by this codebase
    pts = sig * 0.5 + td * 5.0 + kd * 10.0 + sub * 2.0 + ctrl * 0.03
    pts += np.where(wins, 20.0, 0.0)
    pts += np.where(finishes, 25.0, 0.0)

    mean = float(np.mean(pts))
    floor = float(np.percentile(pts, 10))
    ceiling = float(np.percentile(pts, 90))
    stdev = float(np.std(pts))
    return {
        "proj_points": mean,
        "floor": floor,
        "ceiling": ceiling,
        "stdev": stdev,
    }


# ---------- projection main ----------


def project_player(row: Dict[str, Any]) -> Dict[str, Any]:
    name = str(row.get("player_name") or "")
    salary = float(row.get("salary") or 0.0)

    slate_id = str(row.get("slate_id") or "")

    # Prefer opponent_name from match.py, but always try to expand it to a full DK slate name
    opp_name = str(row.get("opponent_name") or "").strip()

    if opp_name:
        expanded = _dk_name_from_token(slate_id, opp_name, name)
        if expanded:
            opp_name = expanded
    else:
        gi = str(row.get("game_info") or "")
        a, b = _parse_game_info_names(gi)

        opp_token = ""
        nn = _norm_name(name)
        na = _norm_name(a)
        nb = _norm_name(b)

        if a and b:
            my_last = nn.split()[-1] if nn else ""
            if nn == na or (my_last and na == my_last):
                opp_token = b
            elif nn == nb or (my_last and nb == my_last):
                opp_token = a
            else:
                opp_token = b

        expanded = _dk_name_from_token(slate_id, opp_token, name) if opp_token else ""
        opp_name = expanded if expanded else opp_token

    with connect(hist_db_path()) as con:
        try:
            win_m = _load_model(con, "win_model")
            fin_m = _load_model(con, "finish_model")
            models_ok = True
            model_err = ""
        except Exception as e:
            win_m = {}
            fin_m = {}
            models_ok = False
            model_err = repr(e)

        fid, fmethod, fconf, fcands = _resolve_fighter_id(con, name, want_candidates=True)
        if opp_name:
            oid, omethod, oconf, ocands = _resolve_fighter_id(con, opp_name, want_candidates=True)
        else:
            oid, omethod, oconf, ocands = (None, "none", 0.0, [])

        if (not fid) or (not oid) or (not models_ok):
            sim = _simulate_dk_points(0.50, 0.35, 3.0, 0.6, 20.0, 0.06, 0.25, False, n_sims=1500)
            out = {
                "fighter_id": fid,
                "opponent_id": oid,
                "opponent_name": opp_name,
                "p_win": 0.50,
                "p_finish_given_win": 0.35,
                "finish_equity": 0.175,
                "edge_note": (
                    f"missing_fighter_match f={fmethod}:{fconf:.2f} "
                    f"o={omethod}:{oconf:.2f}{_cand_note(fcands, ocands)}"
                    if (not fid or not oid)
                    else f"model_missing:{model_err}{_cand_note(fcands, ocands)}"
                ),
                **sim,
                "value_per_1k": ((sim["proj_points"] / salary) * 1000.0 if salary > 0 else 0.0),
            }
            return _standardize_for_optimizer(out, salary)

        try:
            fa = _latest_f2(con, fid)
            fb = _latest_f2(con, oid)
            diffs = _diff_features(fa, fb)

            cols = win_m["feature_cols"]
            x = np.array([float(diffs.get(c, 0.0)) for c in cols], dtype=float)

            z_win = float(np.dot(x, np.array(win_m["w"], dtype=float)) + float(win_m["b"]))
            p_win = _sigmoid(z_win)

            z_fin = float(np.dot(x, np.array(fin_m["w"], dtype=float)) + float(fin_m["b"]))
            p_fin = _sigmoid(z_fin)

            # keep probabilities from becoming unrealistically extreme
            p_win = max(P_WIN_MIN, min(P_WIN_MAX, p_win))
            p_fin = max(P_FIN_MIN, min(P_FIN_MAX, p_fin))

            sig_land_pm = float(
                fa.get("pre_last3_sig_land_pm", fa.get("pre_career_sig_land_pm", 3.0))
            )
            td_land_pm = float(fa.get("pre_last3_td_land_pm", fa.get("pre_career_td_land_pm", 0.6)))
            ctrl_sec_pm = float(
                fa.get("pre_last3_ctrl_sec_pm", fa.get("pre_career_ctrl_sec_pm", 20.0))
            )
            kd_pm = float(fa.get("pre_last3_kd_pm", fa.get("pre_career_kd_pm", 0.06)))
            sub_att_pm = float(
                fa.get("pre_last3_sub_att_pm", fa.get("pre_career_sub_att_pm", 0.25))
            )

            # hard clamps so bad feature rows do not create absurd DK sims
            sig_land_pm = max(0.5, min(8.0, sig_land_pm))
            td_land_pm = max(0.0, min(2.5, td_land_pm))
            ctrl_sec_pm = max(0.0, min(90.0, ctrl_sec_pm))
            kd_pm = max(0.0, min(0.35, kd_pm))
            sub_att_pm = max(0.0, min(1.5, sub_att_pm))

            is_5_round = bool(int(fa.get("is_5_round", 0.0)))

            sim = _simulate_dk_points(
                p_win=p_win,
                p_finish_given_win=p_fin,
                sig_land_pm=sig_land_pm,
                td_land_pm=td_land_pm,
                ctrl_sec_pm=ctrl_sec_pm,
                kd_pm=kd_pm,
                sub_att_pm=sub_att_pm,
                is_5_round=is_5_round,
            )

            finish_equity = p_win * p_fin

            # low-win fighters cannot carry insane means just from pace
            win_gate = 0.35 + 0.65 * p_win
            ceil_gate = 0.55 + 0.45 * p_win

            adj_mean = sim["proj_points"] * win_gate
            adj_floor = sim["floor"] * max(0.30, p_win)
            adj_ceil = sim["ceiling"] * ceil_gate
            adj_stdev = sim["stdev"] * (0.70 + 0.30 * p_win)

            value_per_1k = (adj_mean / salary) * 1000.0 if salary > 0 else 0.0
            fight_volatility = adj_stdev / max(1.0, adj_mean)

            out = {
                "fighter_id": fid,
                "opponent_id": oid,
                "opponent_name": opp_name,
                "p_win": p_win,
                "p_finish_given_win": p_fin,
                "finish_equity": finish_equity,
                "pace_sig_land_pm": sig_land_pm,
                "pace_td_land_pm": td_land_pm,
                "pace_ctrl_sec_pm": ctrl_sec_pm,
                "fight_volatility": fight_volatility,
                "edge_note": f"model_ok f={fmethod}:{fconf:.2f} o={omethod}:{oconf:.2f}",
                "proj_points": adj_mean,
                "floor": adj_floor,
                "ceiling": adj_ceil,
                "stdev": adj_stdev,
                "value_per_1k": value_per_1k,
            }
            return _standardize_for_optimizer(out, salary)

        except Exception as e:
            sim = _simulate_dk_points(0.50, 0.35, 3.0, 0.6, 20.0, 0.06, 0.25, False, n_sims=1500)
            out = {
                "fighter_id": fid,
                "opponent_id": oid,
                "opponent_name": opp_name,
                "p_win": 0.50,
                "p_finish_given_win": 0.35,
                "finish_equity": 0.175,
                "edge_note": f"proj_bug:{type(e).__name__}:{repr(e)}{_cand_note(fcands, ocands)}",
                **sim,
                "value_per_1k": ((sim["proj_points"] / salary) * 1000.0 if salary > 0 else 0.0),
            }
            return _standardize_for_optimizer(out, salary)


def _standardize_for_optimizer(out: Dict[str, Any], salary: float) -> Dict[str, Any]:
    raw_mean = float(out.get("proj_points", 0.0) or 0.0)
    raw_floor = float(out.get("floor", 0.0) or 0.0)
    raw_ceil = float(out.get("ceiling", 0.0) or 0.0)
    raw_stdev = float(out.get("stdev", 0.0) or 0.0)

    mean = raw_mean * PROJ_MEAN_MULT
    floor = raw_floor * PROJ_FLOOR_MULT
    ceil = raw_ceil * PROJ_CEILING_MULT
    stdev = raw_stdev * 0.78

    value = (mean / salary) * 1000.0 if salary and salary > 0 else 0.0
    risk = stdev / max(1.0, mean)
    risk = float(max(0.0, min(1.0, risk)))

    out["proj_mean"] = round(mean, 2)
    out["proj_floor"] = round(floor, 2)
    out["proj_ceiling"] = round(ceil, 2)
    out["value"] = round(value, 4)
    out["risk"] = round(risk, 4)
    return out


def project_player_from_optimizer_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return project_player(row)
