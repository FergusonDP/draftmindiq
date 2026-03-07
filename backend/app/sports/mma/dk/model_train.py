from __future__ import annotations

import json
import math
import sqlite3
from typing import Any, Dict, List, Tuple

import numpy as np


def _sigmoid(z: np.ndarray) -> np.ndarray:
    z = np.clip(z, -50, 50)
    return 1.0 / (1.0 + np.exp(-z))


def _fit_logreg(
    X: np.ndarray,
    y: np.ndarray,
    l2: float = 1e-3,
    lr: float = 0.05,
    steps: int = 2000,
) -> Tuple[np.ndarray, float]:
    n, d = X.shape
    w = np.zeros(d, dtype=float)
    b = 0.0

    for _ in range(steps):
        p = _sigmoid(X @ w + b)
        gw = (X.T @ (p - y)) / n + l2 * w
        gb = float(np.mean(p - y))
        w -= lr * gw
        b -= lr * gb

    return w, b


def _safe_float(x: Any) -> float:
    try:
        if x is None or x == "":
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")


def _safe_binary_label(x: Any) -> float:
    if x is None:
        return float("nan")
    s = str(x).strip()
    if s in {"0", "0.0"}:
        return 0.0
    if s in {"1", "1.0"}:
        return 1.0
    return float("nan")       


def _load_payload_json(x: Any) -> Dict[str, Any]:
    if x is None:
        return {}
    if isinstance(x, dict):
        return x
    s = str(x).strip()
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def ensure_model_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mma_model_coeffs (
            model_name TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    con.commit()


def _extract_feature3_rows(con: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT fight_id, fighter_id, payload_json
        FROM feature_3
        """
    ).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        payload = _load_payload_json(r["payload_json"] if isinstance(r, sqlite3.Row) else r[2])
        if not payload:
            continue

        rr = dict(payload)
        rr["fight_id"] = r["fight_id"] if isinstance(r, sqlite3.Row) else r[0]
        rr["fighter_id"] = r["fighter_id"] if isinstance(r, sqlite3.Row) else r[1]
        out.append(rr)

    return out


def train_models_from_feature3(con: sqlite3.Connection) -> Dict[str, Dict]:
    rows = _extract_feature3_rows(con)
    if not rows:
        raise RuntimeError("feature_3 payload_json rows are empty")

    feat_cols = sorted({k for r in rows for k in r.keys() if k.startswith("diff_")})
    if not feat_cols:
        raise RuntimeError("No diff_* features found in feature_3.payload_json")

    def to_matrix(filter_fn, label_key: str):
        X_list: List[List[float]] = []
        y_list: List[float] = []

        for rr in rows:
            if not filter_fn(rr):
                continue

            yv = _safe_binary_label(rr.get(label_key))
            if math.isnan(yv):
                continue

            xrow: List[float] = []
            ok = True
            for c in feat_cols:
                v = _safe_float(rr.get(c))
                if math.isnan(v):
                    ok = False
                    break
                xrow.append(v)

            if not ok:
                continue

            X_list.append(xrow)
            y_list.append(yv)

        if not X_list:
            raise RuntimeError(f"No training rows for label={label_key}")

        return np.asarray(X_list, dtype=float), np.asarray(y_list, dtype=float)

    # win model
    Xw, yw = to_matrix(lambda r: not math.isnan(_safe_binary_label(r.get("win"))), "win")
    w_win, b_win = _fit_logreg(Xw, yw, l2=1e-3, lr=0.05, steps=2500)

    # finish given win model
    Xf, yf = to_matrix(
        lambda r: _safe_binary_label(r.get("win")) == 1.0
        and not math.isnan(_safe_binary_label(r.get("finish_win"))),
        "finish_win",
    )
    w_fin, b_fin = _fit_logreg(Xf, yf, l2=1e-3, lr=0.05, steps=2500)

    return {
        "win_model": {
            "feature_cols": feat_cols,
            "w": w_win.tolist(),
            "b": b_win,
            "n": int(len(yw)),
        },
        "finish_model": {
            "feature_cols": feat_cols,
            "w": w_fin.tolist(),
            "b": b_fin,
            "n": int(len(yf)),
        },
    }


def save_models(con: sqlite3.Connection, models: Dict[str, Dict]) -> None:
    ensure_model_schema(con)

    for name, payload in models.items():
        con.execute(
            """
            INSERT INTO mma_model_coeffs(model_name, payload_json)
            VALUES (?, ?)
            ON CONFLICT(model_name) DO UPDATE SET
              payload_json=excluded.payload_json,
              updated_at=datetime('now')
            """,
            (name, json.dumps(payload)),
        )
    con.commit()


def train_and_save(db_path: str = r"data/beast.sqlite") -> Dict[str, Dict]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        ensure_model_schema(con)
        models = train_models_from_feature3(con)
        save_models(con, models)
        return models
    finally:
        con.close()
