from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from rapidfuzz import fuzz, process


# ---------------------------
# util / normalization
# ---------------------------

_WS = re.compile(r"\s+")
_NONALNUM = re.compile(r"[^a-z0-9\s\-\']+")
_SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", re.IGNORECASE)

_BAD_SS_SUBSTRINGS = [
    "fight statistics",
    "sports-statistics.com",
    "topics sports",
    "stat leaders",
    "events & fights",
    "sports stats",
    "ufc stats",
    "nba",
    "nfl",
    "mlb",
    "nhl",
    "tennis",
]

_COUNTRY_PREFIXES = {
    "usa",
    "united states",
    "australia",
    "brazil",
    "canada",
    "england",
    "ireland",
    "scotland",
    "wales",
    "mexico",
    "france",
    "germany",
    "poland",
    "sweden",
    "norway",
    "finland",
    "russia",
    "ukraine",
    "georgia",
    "china",
    "japan",
    "korea",
    "south korea",
    "philippines",
    "thailand",
    "vietnam",
    "new zealand",
    "south africa",
    "nigeria",
    "cameroon",
    "senegal",
    "ghana",
    "united arab emirates",
    "uae",
    "kazakhstan",
    "kyrgyzstan",
    "uzbekistan",
    "tajikistan",
}


def norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = _NONALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def strip_suffix(s: str) -> str:
    s2 = norm_name(s)
    s2 = _SUFFIX_RE.sub("", s2).strip()
    s2 = _WS.sub(" ", s2).strip()
    return s2


def clean_ss_fighter_name(raw: str) -> str:
    """
    SS fighter strings sometimes look like:
      - "USA Zach Reese"  -> "zach reese"
      - nav / garbage text -> ""
    """
    s = (raw or "").strip()
    if not s:
        return ""

    sn = norm_name(s)
    if any(b in sn for b in _BAD_SS_SUBSTRINGS):
        return ""

    parts = sn.split()
    if len(parts) >= 2:
        for k in (3, 2, 1):
            if len(parts) >= k + 1:
                pref = " ".join(parts[:k])
                if pref in _COUNTRY_PREFIXES:
                    sn = " ".join(parts[k:]).strip()
                    break

    return sn


def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _parse_pair_landed(value: Any) -> Optional[float]:
    """
    Parse "12 of 34" -> 12
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s+of\s+(\d+(?:\.\d+)?)\s*$", s, re.IGNORECASE)
    if not m:
        return None
    return float(m.group(1))


def _parse_pair_attempted(value: Any) -> Optional[float]:
    """
    Parse "12 of 34" -> 34
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s+of\s+(\d+(?:\.\d+)?)\s*$", s, re.IGNORECASE)
    if not m:
        return None
    return float(m.group(2))


def _parse_ctrl_sec(value: Any) -> Optional[float]:
    """
    Parse control time:
      "1:23" -> 83
      "83"   -> 83
      "83.0" -> 83
      ""     -> None
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    if ":" in s:
        parts = s.split(":")
        if len(parts) == 2:
            try:
                minutes = int(parts[0].strip())
                seconds = int(parts[1].strip())
                return float(minutes * 60 + seconds)
            except Exception:
                return None

    try:
        return float(s)
    except Exception:
        return None


# ---------------------------
# csv helpers
# ---------------------------


def _read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        rdr = csv.DictReader(f)
        return [dict(r) for r in rdr]


# ---------------------------
# matching (SS -> dim_fighter)
# ---------------------------


@dataclass
class MatchResult:
    fighter_id: Optional[str]
    method: str
    confidence: float
    candidates_json: str


def _top_candidates(
    name_map: Dict[str, Tuple[str, str]], raw_norm: str, k: int = 5
) -> List[Dict[str, Any]]:
    choices = list(name_map.keys())
    hits = process.extract(raw_norm, choices, scorer=fuzz.token_sort_ratio, limit=k) or []
    out: List[Dict[str, Any]] = []
    for cand, score, _ in hits:
        fid, cname = name_map[cand]
        out.append({"id": fid, "name": cname, "score": int(score)})
    return out


def _best_match(name_map: Dict[str, Tuple[str, str]], raw_norm: str) -> MatchResult:
    if not raw_norm:
        return MatchResult(None, "none", 0.0, "[]")

    if raw_norm in name_map:
        fid, _ = name_map[raw_norm]
        return MatchResult(fid, "exact", 1.0, "[]")

    stripped = strip_suffix(raw_norm)
    if stripped in name_map:
        fid, _ = name_map[stripped]
        return MatchResult(fid, "exact_suffix_strip", 0.98, "[]")

    best = process.extractOne(raw_norm, list(name_map.keys()), scorer=fuzz.token_sort_ratio)
    if not best:
        return MatchResult(None, "none", 0.0, "[]")

    cand, score, _ = best
    conf = float(score) / 100.0
    cands = _top_candidates(name_map, raw_norm, k=5)

    if score >= 92:
        fid, _ = name_map[cand]
        return MatchResult(fid, "fuzzy", conf, json.dumps(cands))

    return MatchResult(None, "none", conf, json.dumps(cands))


def _lookup_alias(con: sqlite3.Connection, source: str, raw_name: str) -> Optional[str]:
    raw = (raw_name or "").strip()
    if not raw:
        return None

    row = con.execute(
        "SELECT fighter_id FROM mma_name_aliases WHERE source=? AND raw_name=? LIMIT 1",
        (source, raw),
    ).fetchone()
    if row and row[0]:
        return str(row[0])

    raw_norm = norm_name(raw)
    row = con.execute(
        "SELECT fighter_id FROM mma_name_aliases WHERE source=? AND raw_name=? LIMIT 1",
        (source, raw_norm),
    ).fetchone()
    if row and row[0]:
        return str(row[0])

    return None


# ---------------------------
# sqlite helpers
# ---------------------------


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    # UDFs for SQL pivoting/parsing
    con.create_function("PAIR_LANDED", 1, _parse_pair_landed)
    con.create_function("PAIR_ATTEMPTED", 1, _parse_pair_attempted)
    con.create_function("CTRL_TO_SEC", 1, _parse_ctrl_sec)

    return con


def execmany(cur: sqlite3.Cursor, stmts: Iterable[str]) -> None:
    for s in stmts:
        cur.execute(s)


# ---------------------------
# schema
# ---------------------------

SCHEMA_SQL = r"""
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS dim_fighter (
  fighter_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  name_norm TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_fighter_name_norm
ON dim_fighter(name_norm);

CREATE TABLE IF NOT EXISTS dim_event (
  event_id TEXT PRIMARY KEY,
  event_name TEXT,
  event_date TEXT
);

CREATE TABLE IF NOT EXISTS dim_fight (
  fight_id TEXT PRIMARY KEY,
  event_id TEXT,
  fight_date TEXT,
  fighter_a_id TEXT,
  fighter_b_id TEXT,
  fighter_a_name TEXT,
  fighter_b_name TEXT,
  weight_class TEXT,
  method TEXT,
  round INTEGER,
  time TEXT,
  url TEXT,
  FOREIGN KEY(event_id) REFERENCES dim_event(event_id),
  FOREIGN KEY(fighter_a_id) REFERENCES dim_fighter(fighter_id),
  FOREIGN KEY(fighter_b_id) REFERENCES dim_fighter(fighter_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_fight_url
ON dim_fight(url);

CREATE TABLE IF NOT EXISTS mma_name_aliases (
  source TEXT NOT NULL,
  raw_name TEXT NOT NULL,
  fighter_id TEXT NOT NULL,
  PRIMARY KEY(source, raw_name),
  FOREIGN KEY(fighter_id) REFERENCES dim_fighter(fighter_id)
);

CREATE INDEX IF NOT EXISTS idx_mma_name_aliases_fighter
ON mma_name_aliases(fighter_id);

CREATE TABLE IF NOT EXISTS fact_fighter_fight_stats (
  fight_id TEXT NOT NULL,
  fighter_id TEXT NOT NULL,
  opponent_id TEXT,
  is_win INTEGER,
  kd INTEGER,
  sig_landed INTEGER,
  sig_att INTEGER,
  total_landed INTEGER,
  total_att INTEGER,
  td_landed INTEGER,
  td_att INTEGER,
  sub_att INTEGER,
  rev INTEGER,
  ctrl_sec INTEGER,
  src TEXT,
  PRIMARY KEY(fight_id, fighter_id),
  FOREIGN KEY(fight_id) REFERENCES dim_fight(fight_id),
  FOREIGN KEY(fighter_id) REFERENCES dim_fighter(fighter_id),
  FOREIGN KEY(opponent_id) REFERENCES dim_fighter(fighter_id)
);

CREATE TABLE IF NOT EXISTS fact_fighter_round_stats_long (
  fight_url TEXT NOT NULL,
  round INTEGER NOT NULL,
  fighter_id TEXT,
  ss_fighter TEXT NOT NULL,
  stat_key TEXT NOT NULL,
  a_landed REAL,
  a_attempted REAL,
  a_value TEXT,
  src TEXT NOT NULL,
  PRIMARY KEY(fight_url, round, ss_fighter, stat_key, src)
);

CREATE TABLE IF NOT EXISTS ss_fighter_map (
  fight_url TEXT NOT NULL,
  round INTEGER NOT NULL,
  ss_fighter TEXT NOT NULL,
  ss_fighter_norm TEXT NOT NULL,
  fighter_id TEXT,
  method TEXT,
  confidence REAL,
  candidates_json TEXT,
  PRIMARY KEY(fight_url, round, ss_fighter_norm)
);

CREATE TABLE IF NOT EXISTS fact_fighter_round_stats (
  fight_url TEXT NOT NULL,
  round INTEGER NOT NULL,
  fighter_id TEXT,
  ss_fighter TEXT NOT NULL,

  sig_landed REAL,
  sig_attempted REAL,
  tot_landed REAL,
  tot_attempted REAL,
  td_landed REAL,
  td_attempted REAL,
  kd REAL,
  sub_att REAL,
  rev REAL,
  passes REAL,
  ctrl_sec REAL,

  head_landed REAL,
  head_attempted REAL,
  body_landed REAL,
  body_attempted REAL,
  leg_landed REAL,
  leg_attempted REAL,

  dist_landed REAL,
  dist_attempted REAL,
  clinch_landed REAL,
  clinch_attempted REAL,
  ground_landed REAL,
  ground_attempted REAL,

  src TEXT NOT NULL,
  PRIMARY KEY(fight_url, round, ss_fighter, src)
);

CREATE TABLE IF NOT EXISTS feature_1 (
  fight_id TEXT NOT NULL,
  fighter_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY(fight_id, fighter_id),
  FOREIGN KEY(fight_id) REFERENCES dim_fight(fight_id),
  FOREIGN KEY(fighter_id) REFERENCES dim_fighter(fighter_id)
);

CREATE TABLE IF NOT EXISTS feature_2 (
  fight_id TEXT NOT NULL,
  fighter_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY(fight_id, fighter_id),
  FOREIGN KEY(fight_id) REFERENCES dim_fight(fight_id),
  FOREIGN KEY(fighter_id) REFERENCES dim_fighter(fighter_id)
);

CREATE TABLE IF NOT EXISTS feature_3 (
  fight_id TEXT NOT NULL,
  fighter_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY(fight_id, fighter_id),
  FOREIGN KEY(fight_id) REFERENCES dim_fight(fight_id),
  FOREIGN KEY(fighter_id) REFERENCES dim_fighter(fighter_id)
);
"""


def build_schema(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA_SQL)


# ---------------------------
# ingest UFCStats CSVs
# ---------------------------


def ingest_dims_and_facts(
    con: sqlite3.Connection,
    fighters_csv: str,
    events_csv: str,
    fights_csv: str,
    fighter_stats_csv: str,
) -> Dict[str, int]:
    fighters = _read_csv(fighters_csv)
    events = _read_csv(events_csv)
    fights = _read_csv(fights_csv)
    stats = _read_csv(fighter_stats_csv)

    cur = con.cursor()

    for r in fighters:
        fid = str(r.get("fighter_id") or r.get("id") or "").strip()
        name = str(r.get("name") or "").strip()
        if not fid or not name:
            continue
        cur.execute(
            "INSERT OR REPLACE INTO dim_fighter(fighter_id,name,name_norm) VALUES (?,?,?)",
            (fid, name, norm_name(name)),
        )

    for r in events:
        eid = str(r.get("event_id") or r.get("id") or "").strip()
        if not eid:
            continue
        ename = str(r.get("event_name") or r.get("name") or "").strip()
        edate = str(r.get("event_date") or r.get("date") or "").strip()
        cur.execute(
            "INSERT OR REPLACE INTO dim_event(event_id,event_name,event_date) VALUES (?,?,?)",
            (eid, ename, edate),
        )

    for r in fights:
        fight_id = str(r.get("fight_id") or r.get("id") or "").strip()
        if not fight_id:
            continue

        event_id = str(r.get("event_id") or "").strip() or None
        fdate = str(r.get("fight_date") or r.get("event_date") or "").strip() or None

        fa = str(
            r.get("fighter_a_id")
            or r.get("fighter_id_a")
            or r.get("r_id")
            or ""
        ).strip() or None

        fb = str(
            r.get("fighter_b_id")
            or r.get("fighter_id_b")
            or r.get("b_id")
            or ""
        ).strip() or None

        fan = str(
            r.get("fighter_a_name")
            or r.get("fighter_a")
            or ""
        ).strip() or None

        fbn = str(
            r.get("fighter_b_name")
            or r.get("fighter_b")
            or ""
        ).strip() or None

        wc = str(
            r.get("weight_class")
            or r.get("division")
            or ""
        ).strip() or None

        method = str(r.get("method") or "").strip() or None
        rnd = _to_int(r.get("round") or r.get("finish_round"))
        t = str(r.get("time") or r.get("match_time_sec") or "").strip() or None
        url = str(r.get("url") or r.get("fight_url") or "").strip() or None

        cur.execute(
            """
            INSERT OR REPLACE INTO dim_fight
            (fight_id,event_id,fight_date,fighter_a_id,fighter_b_id,fighter_a_name,fighter_b_name,weight_class,method,round,time,url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (fight_id, event_id, fdate, fa, fb, fan, fbn, wc, method, rnd, t, url),
        )

    for r in stats:
        fight_id = str(r.get("fight_id") or "").strip()
        fighter_id = str(r.get("fighter_id") or "").strip()
        if not fight_id or not fighter_id:
            continue

        opp = str(r.get("opponent_id") or "").strip() or None

        cur.execute(
            """
            INSERT OR REPLACE INTO fact_fighter_fight_stats
            (fight_id,fighter_id,opponent_id,is_win,kd,sig_landed,sig_att,total_landed,total_att,td_landed,td_att,sub_att,rev,ctrl_sec,src)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                fight_id,
                fighter_id,
                opp,
                _to_int(r.get("is_win")),
                _to_int(r.get("kd")),
                _to_int(r.get("sig_landed")),
                _to_int(r.get("sig_att")),
                _to_int(r.get("total_landed")),
                _to_int(r.get("total_att")),
                _to_int(r.get("td_landed")),
                _to_int(r.get("td_att")),
                _to_int(r.get("sub_att")),
                _to_int(r.get("rev")),
                _to_int(r.get("ctrl_sec")),
                "ufcstats",
            ),
        )

    con.commit()

    return {
        "dim_fighter": int(con.execute("SELECT COUNT(*) FROM dim_fighter").fetchone()[0]),
        "dim_event": int(con.execute("SELECT COUNT(*) FROM dim_event").fetchone()[0]),
        "dim_fight": int(con.execute("SELECT COUNT(*) FROM dim_fight").fetchone()[0]),
        "fact_fighter_fight_stats": int(
            con.execute("SELECT COUNT(*) FROM fact_fighter_fight_stats").fetchone()[0]
        ),
    }


def ingest_aliases(con: sqlite3.Connection, aliases_csv: str) -> Dict[str, int]:
    if not aliases_csv:
        return {"aliases_rows": 0, "aliases_inserted": 0, "aliases_skipped": 0}
    if not os.path.exists(aliases_csv):
        raise FileNotFoundError(aliases_csv)

    cur = con.cursor()

    valid = {
        r["fighter_id"]
        for r in con.execute("SELECT fighter_id FROM dim_fighter").fetchall()
        if r["fighter_id"]
    }

    cand_re = re.compile(r"^\s*([0-9a-f]{16,})\s*\|", re.IGNORECASE)

    rows = 0
    inserted = 0
    skipped = 0

    with open(aliases_csv, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return {"aliases_rows": 0, "aliases_inserted": 0, "aliases_skipped": 0}

        fieldnames = [h.strip() for h in reader.fieldnames if h]
        if "source" not in fieldnames or "raw_name" not in fieldnames:
            raise RuntimeError(
                f"aliases_csv missing required headers. Found: {fieldnames}. "
                "Need: source, raw_name, fighter_id"
            )

        for r in reader:
            rows += 1
            source = (r.get("source") or "").strip()
            raw_name = (r.get("raw_name") or "").strip()
            fighter_id = (r.get("fighter_id") or "").strip()
            method = (r.get("method") or "").strip().lower()

            if not fighter_id and method in {"alias", "exact"}:
                c1 = (r.get("candidate_1") or "").strip()
                m = cand_re.match(c1)
                if m:
                    fighter_id = m.group(1).strip()

            if not source or not raw_name or not fighter_id:
                skipped += 1
                continue

            if fighter_id not in valid:
                skipped += 1
                continue

            # store both raw and normalized versions for better fallback
            cur.execute(
                "INSERT OR REPLACE INTO mma_name_aliases(source, raw_name, fighter_id) VALUES (?,?,?)",
                (source, raw_name, fighter_id),
            )
            inserted += 1

            raw_norm = norm_name(raw_name)
            if raw_norm and raw_norm != raw_name:
                cur.execute(
                    "INSERT OR REPLACE INTO mma_name_aliases(source, raw_name, fighter_id) VALUES (?,?,?)",
                    (source, raw_norm, fighter_id),
                )

    con.commit()
    return {"aliases_rows": rows, "aliases_inserted": inserted, "aliases_skipped": skipped}


def ingest_feature_table(con: sqlite3.Connection, table: str, csv_path: str) -> int:
    if table not in {"feature_1", "feature_2", "feature_3"}:
        raise ValueError(f"Unexpected feature table: {table}")

    rows = _read_csv(csv_path)
    cur = con.cursor()
    n = 0

    for r in rows:
        fight_id = str(r.get("fight_id") or "").strip()
        fighter_id = str(r.get("fighter_id") or "").strip()
        if not fight_id or not fighter_id:
            continue

        cur.execute(
            f"INSERT OR REPLACE INTO {table}(fight_id,fighter_id,payload_json) VALUES (?,?,?)",
            (fight_id, fighter_id, json.dumps(r, ensure_ascii=False)),
        )
        n += 1

    con.commit()
    return n


# ---------------------------
# ingest Sports-Statistics round stats
# ---------------------------


def ingest_ss_round_stats(
    con: sqlite3.Connection,
    ss_db_path: str,
    *,
    limit_rows: Optional[int] = None,
) -> Dict[str, int]:
    if not os.path.exists(ss_db_path):
        raise FileNotFoundError(ss_db_path)

    cur = con.cursor()

    try:
        cur.execute("ATTACH DATABASE ? AS ss;", (ss_db_path,))

        for t in (
            "canon_fight_round_totals",
            "canon_strike_breakdown",
            "canon_fight_totals",
            "ss_fact_fighter_fights",
        ):
            row = cur.execute(
                "SELECT name FROM ss.sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (t,),
            ).fetchone()
            if not row:
                raise RuntimeError(f"Missing {t} in SS DB: {ss_db_path}")

        limit_sql = f" LIMIT {int(limit_rows)}" if limit_rows else ""

        before_changes = con.total_changes

        # round totals -> long
        cur.execute(
            f"""
            INSERT OR REPLACE INTO fact_fighter_round_stats_long
            (fight_url, round, fighter_id, ss_fighter, stat_key, a_landed, a_attempted, a_value, src)
            SELECT
              fight_url,
              CAST(round AS INTEGER) AS round,
              NULL AS fighter_id,
              fighter AS ss_fighter,
              stat_key,
              CAST(a_landed AS REAL)    AS a_landed,
              CAST(a_attempted AS REAL) AS a_attempted,
              a_value,
              'sports_statistics' AS src
            FROM ss.canon_fight_round_totals
            WHERE fight_url IS NOT NULL AND trim(fight_url) <> ''
              AND fighter   IS NOT NULL AND trim(fighter)   <> ''
              AND stat_key  IS NOT NULL AND trim(stat_key)  <> ''
              AND round IS NOT NULL
              AND CAST(round AS INTEGER) > 0
            {limit_sql};
            """
        )
        round_totals_inserted = con.total_changes - before_changes
        before_changes = con.total_changes

        # strike breakdown -> long
        cur.execute(
            f"""
            INSERT OR REPLACE INTO fact_fighter_round_stats_long
            (fight_url, round, fighter_id, ss_fighter, stat_key, a_landed, a_attempted, a_value, src)
            SELECT
              fight_url,
              CAST(round AS INTEGER) AS round,
              NULL AS fighter_id,
              fighter AS ss_fighter,
              stat_key,
              CAST(landed AS REAL)    AS a_landed,
              CAST(attempted AS REAL) AS a_attempted,
              value AS a_value,
              'sports_statistics' AS src
            FROM ss.canon_strike_breakdown
            WHERE fight_url IS NOT NULL AND trim(fight_url) <> ''
              AND fighter   IS NOT NULL AND trim(fighter)   <> ''
              AND stat_key  IS NOT NULL AND trim(stat_key)  <> ''
              AND round IS NOT NULL
              AND CAST(round AS INTEGER) > 0
            {limit_sql};
            """
        )
        strike_breakdown_inserted = con.total_changes - before_changes

        # fighter name map
        rows = con.execute("SELECT fighter_id, name, name_norm FROM dim_fighter").fetchall()
        name_map = {
            str(r["name_norm"]): (str(r["fighter_id"]), str(r["name"]))
            for r in rows
            if r["name_norm"]
        }
        if not name_map:
            raise RuntimeError("dim_fighter is empty; ingest UFCStats CSVs first.")

        # map SS fighter text -> fighter_id
        ss_rows = con.execute(
            """
            SELECT DISTINCT fight_url, round, ss_fighter
            FROM fact_fighter_round_stats_long
            WHERE src='sports_statistics'
            """
        ).fetchall()

        mapped = 0
        unmapped = 0
        skipped_garbage = 0

        for r in ss_rows:
            fight_url = str(r["fight_url"])
            rnd = int(r["round"])
            ss_raw = str(r["ss_fighter"])
            ss_clean = clean_ss_fighter_name(ss_raw)

            if not ss_clean:
                skipped_garbage += 1
                continue

            ali = _lookup_alias(con, "ss", ss_raw) or _lookup_alias(con, "ss", ss_clean)
            if ali:
                mr = MatchResult(ali, "alias", 1.0, "[]")
            else:
                mr = _best_match(name_map, ss_clean)

            if mr.fighter_id:
                mapped += 1
            else:
                unmapped += 1

            con.execute(
                """
                INSERT OR REPLACE INTO ss_fighter_map
                (fight_url, round, ss_fighter, ss_fighter_norm, fighter_id, method, confidence, candidates_json)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    fight_url,
                    rnd,
                    ss_raw,
                    ss_clean,
                    mr.fighter_id,
                    mr.method,
                    float(mr.confidence),
                    str(mr.candidates_json),
                ),
            )

        # apply mapping to long table
        con.execute(
            """
            UPDATE fact_fighter_round_stats_long
            SET fighter_id = (
              SELECT m.fighter_id
              FROM ss_fighter_map m
              WHERE m.fight_url  = fact_fighter_round_stats_long.fight_url
                AND m.round      = fact_fighter_round_stats_long.round
                AND m.ss_fighter = fact_fighter_round_stats_long.ss_fighter
              LIMIT 1
            )
            WHERE fighter_id IS NULL
              AND src='sports_statistics';
            """
        )

        # rebuild wide table from clean long table
        cur.execute("DROP TABLE IF EXISTS _tmp_round_wide;")
        cur.execute(
            """
            CREATE TABLE _tmp_round_wide AS
            WITH parsed AS (
              SELECT
                fight_url,
                round,
                ss_fighter,
                fighter_id,
                stat_key,
                COALESCE(a_landed, PAIR_LANDED(a_value)) AS landed_num,
                COALESCE(a_attempted, PAIR_ATTEMPTED(a_value)) AS attempted_num,
                CASE
                  WHEN lower(stat_key) LIKE '%control%' THEN COALESCE(a_landed, CTRL_TO_SEC(a_value))
                  ELSE COALESCE(a_landed, PAIR_LANDED(a_value))
                END AS scalar_num,
                a_value
              FROM fact_fighter_round_stats_long
              WHERE src='sports_statistics'
            )
            SELECT
              fight_url,
              round,
              fighter_id,
              ss_fighter,

              MAX(CASE WHEN stat_key IN ('Significant Strikes', 'Significant Strikes %') THEN landed_num END) AS sig_landed,
              MAX(CASE WHEN stat_key IN ('Significant Strikes', 'Significant Strikes %') THEN attempted_num END) AS sig_attempted,

              MAX(CASE WHEN stat_key IN ('Total Strikes', 'Total Strikes %') THEN landed_num END) AS tot_landed,
              MAX(CASE WHEN stat_key IN ('Total Strikes', 'Total Strikes %') THEN attempted_num END) AS tot_attempted,

              MAX(CASE WHEN stat_key IN ('Takedowns', 'Takedowns %') THEN landed_num END) AS td_landed,
              MAX(CASE WHEN stat_key IN ('Takedowns', 'Takedowns %') THEN attempted_num END) AS td_attempted,

              MAX(CASE WHEN stat_key='Knockdowns' THEN scalar_num END) AS kd,
              MAX(CASE WHEN stat_key='Submissions Attempted' THEN scalar_num END) AS sub_att,
              MAX(CASE WHEN stat_key='Reversals' THEN scalar_num END) AS rev,
              MAX(CASE WHEN stat_key='Passes' THEN scalar_num END) AS passes,
              MAX(CASE WHEN lower(stat_key) LIKE '%control%' THEN scalar_num END) AS ctrl_sec,

              MAX(CASE WHEN stat_key='Head' THEN landed_num END) AS head_landed,
              MAX(CASE WHEN stat_key='Head' THEN attempted_num END) AS head_attempted,
              MAX(CASE WHEN stat_key='Body' THEN landed_num END) AS body_landed,
              MAX(CASE WHEN stat_key='Body' THEN attempted_num END) AS body_attempted,
              MAX(CASE WHEN stat_key='Leg' THEN landed_num END) AS leg_landed,
              MAX(CASE WHEN stat_key='Leg' THEN attempted_num END) AS leg_attempted,

              MAX(CASE WHEN lower(stat_key) LIKE 'distance%' THEN landed_num END) AS dist_landed,
              MAX(CASE WHEN lower(stat_key) LIKE 'distance%' THEN attempted_num END) AS dist_attempted,
              MAX(CASE WHEN lower(stat_key) LIKE 'clinch%' THEN landed_num END) AS clinch_landed,
              MAX(CASE WHEN lower(stat_key) LIKE 'clinch%' THEN attempted_num END) AS clinch_attempted,
              MAX(CASE WHEN lower(stat_key) LIKE 'ground%' THEN landed_num END) AS ground_landed,
              MAX(CASE WHEN lower(stat_key) LIKE 'ground%' THEN attempted_num END) AS ground_attempted

            FROM parsed
            GROUP BY fight_url, round, ss_fighter, fighter_id;
            """
        )

        con.execute(
            """
            INSERT OR REPLACE INTO fact_fighter_round_stats
            (fight_url, round, fighter_id, ss_fighter,
             sig_landed, sig_attempted, tot_landed, tot_attempted, td_landed, td_attempted,
             kd, sub_att, rev, passes, ctrl_sec,
             head_landed, head_attempted, body_landed, body_attempted, leg_landed, leg_attempted,
             dist_landed, dist_attempted, clinch_landed, clinch_attempted, ground_landed, ground_attempted,
             src)
            SELECT
              fight_url, round, fighter_id, ss_fighter,
              sig_landed, sig_attempted, tot_landed, tot_attempted, td_landed, td_attempted,
              kd, sub_att, rev, passes, ctrl_sec,
              head_landed, head_attempted, body_landed, body_attempted, leg_landed, leg_attempted,
              dist_landed, dist_attempted, clinch_landed, clinch_attempted, ground_landed, ground_attempted,
              'sports_statistics'
            FROM _tmp_round_wide;
            """
        )

        con.commit()

        return {
            "round_totals_inserted": int(round_totals_inserted),
            "strike_breakdown_inserted": int(strike_breakdown_inserted),
            "fact_fighter_round_stats_long": int(
                con.execute("SELECT COUNT(*) FROM fact_fighter_round_stats_long").fetchone()[0]
            ),
            "fact_fighter_round_stats": int(
                con.execute("SELECT COUNT(*) FROM fact_fighter_round_stats").fetchone()[0]
            ),
            "ss_fighter_map_rows": int(
                con.execute("SELECT COUNT(*) FROM ss_fighter_map").fetchone()[0]
            ),
            "ss_mapped": int(mapped),
            "ss_unmapped": int(unmapped),
            "ss_skipped_garbage": int(skipped_garbage),
        }

    finally:
        try:
            cur.execute("DETACH DATABASE ss;")
        except Exception:
            pass


# ---------------------------
# main
# ---------------------------


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--fighters_csv", required=True)
    ap.add_argument("--events_csv", required=True)
    ap.add_argument("--fights_csv", required=True)
    ap.add_argument("--fighter_stats_csv", required=True)
    ap.add_argument("--features_v1_csv", required=True)
    ap.add_argument("--features_v2_csv", required=True)
    ap.add_argument("--features_v3_csv", required=True)
    ap.add_argument("--aliases_csv", required=True)
    ap.add_argument("--ss_db", required=True)
    ap.add_argument("--limit_ss_rows", type=int, default=None)
    ap.add_argument(
        "--fresh",
        action="store_true",
        help="Delete existing output DB before rebuilding.",
    )

    args = ap.parse_args(argv)
    t0 = dt.datetime.now()

    out_path = args.out
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    if args.fresh and os.path.exists(out_path):
        os.remove(out_path)

    con = connect(out_path)
    try:
        build_schema(con)

        ufc_stats = ingest_dims_and_facts(
            con,
            fighters_csv=args.fighters_csv,
            events_csv=args.events_csv,
            fights_csv=args.fights_csv,
            fighter_stats_csv=args.fighter_stats_csv,
        )

        alias_n = ingest_aliases(con, args.aliases_csv)

        f1 = ingest_feature_table(con, "feature_1", args.features_v1_csv)
        f2 = ingest_feature_table(con, "feature_2", args.features_v2_csv)
        f3 = ingest_feature_table(con, "feature_3", args.features_v3_csv)

        ss_stats = ingest_ss_round_stats(
            con,
            args.ss_db,
            limit_rows=args.limit_ss_rows,
        )

        counts = {
            "dim_fighter": int(con.execute("SELECT COUNT(*) FROM dim_fighter").fetchone()[0]),
            "dim_event": int(con.execute("SELECT COUNT(*) FROM dim_event").fetchone()[0]),
            "dim_fight": int(con.execute("SELECT COUNT(*) FROM dim_fight").fetchone()[0]),
            "fact_fighter_fight_stats": int(
                con.execute("SELECT COUNT(*) FROM fact_fighter_fight_stats").fetchone()[0]
            ),
            "fact_fighter_round_stats": int(
                con.execute("SELECT COUNT(*) FROM fact_fighter_round_stats").fetchone()[0]
            ),
            "ss_unmapped": int(
                con.execute(
                    "SELECT COUNT(*) FROM ss_fighter_map WHERE fighter_id IS NULL"
                ).fetchone()[0]
            ),
        }

        dtook = dt.datetime.now() - t0

        print(f"Built: {out_path}")
        print("Counts:", counts)
        print("UFC CSV ingest:", ufc_stats)
        print("Aliases ingested:", alias_n)
        print("Feature tables:", {"feature_1": f1, "feature_2": f2, "feature_3": f3})
        print("SS rounds:", ss_stats)
        print("Done in", dtook)

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
