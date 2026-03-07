from __future__ import annotations

import csv
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"

BEAST_DB = DATA_DIR / "beast.sqlite"
UFC_DB = DATA_DIR / "ufc.sqlite"
OUT_DB = DATA_DIR / "mma_fighter_cards.sqlite"
OVERRIDES_CSV = DATA_DIR / "fighter_bio_overrides.csv"
AMBIGUITY_CSV = DATA_DIR / "fighter_name_ambiguities.csv"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(name: str | None) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def cm_to_inches(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / 2.54, 2)


def kg_to_lbs(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value) * 2.2046226218, 2)


def normalize_height_to_inches(value: Any) -> float | None:
    x = to_float(value)
    if x is None:
        return None
    # UFC source may store centimeters
    if x > 100:
        return cm_to_inches(x)
    return x


def normalize_weight_to_lbs(value: Any) -> float | None:
    x = to_float(value)
    if x is None:
        return None
    # UFC source may store kilograms
    if x < 120:
        return kg_to_lbs(x)
    return x


def normalize_reach_to_inches(value: Any) -> float | None:
    x = to_float(value)
    if x is None:
        return None
    # UFC source may store centimeters
    if x > 100:
        return cm_to_inches(x)
    return x


@dataclass
class FighterBio:
    fighter_id: str
    name: str
    name_norm: str
    nickname: str | None = None
    stance: str | None = None
    height_in: float | None = None
    weight_lbs: float | None = None
    reach_in: float | None = None
    bio_source: str = "beast"
    bio_completeness: int = 0


def connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def create_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        PRAGMA foreign_keys = ON;

        DROP TABLE IF EXISTS fighter_rounds;
        DROP TABLE IF EXISTS fighter_fights;
        DROP TABLE IF EXISTS fighter_cards;
        DROP TABLE IF EXISTS fighters;
        DROP TABLE IF EXISTS build_meta;

        CREATE TABLE fighters (
            fighter_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            name_norm TEXT NOT NULL,
            nickname TEXT,
            stance TEXT,
            height_in REAL,
            weight_lbs REAL,
            reach_in REAL,
            bio_source TEXT NOT NULL,
            bio_completeness INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX idx_fighters_name ON fighters(name);
        CREATE INDEX idx_fighters_name_norm ON fighters(name_norm);

        CREATE TABLE fighter_fights (
            fighter_fight_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fight_id TEXT NOT NULL,
            fighter_id TEXT NOT NULL,
            fighter_name TEXT NOT NULL,
            opponent_id TEXT,
            opponent_name TEXT,
            event_id TEXT,
            event_name TEXT,
            event_date TEXT,
            weight_class TEXT,
            method TEXT,
            finish_round INTEGER,
            finish_time_sec INTEGER,
            fight_url TEXT,
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
            FOREIGN KEY (fighter_id) REFERENCES fighters(fighter_id)
        );

        CREATE INDEX idx_fighter_fights_fighter_id ON fighter_fights(fighter_id);
        CREATE INDEX idx_fighter_fights_event_date ON fighter_fights(event_date);
        CREATE INDEX idx_fighter_fights_fight_id ON fighter_fights(fight_id);
        CREATE INDEX idx_fighter_fights_url ON fighter_fights(fight_url);

        CREATE TABLE fighter_rounds (
            fighter_round_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fight_url TEXT NOT NULL,
            fight_id TEXT,
            fighter_id TEXT,
            fighter_name TEXT,
            round INTEGER NOT NULL,
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
            src TEXT
        );

        CREATE INDEX idx_fighter_rounds_fight_url ON fighter_rounds(fight_url);
        CREATE INDEX idx_fighter_rounds_fighter_id ON fighter_rounds(fighter_id);
        CREATE INDEX idx_fighter_rounds_round ON fighter_rounds(round);

        CREATE TABLE fighter_cards (
            fighter_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            name_norm TEXT NOT NULL,
            nickname TEXT,
            stance TEXT,
            height_in REAL,
            weight_lbs REAL,
            reach_in REAL,
            total_fights INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            draws INTEGER NOT NULL DEFAULT 0,
            no_contests INTEGER NOT NULL DEFAULT 0,
            sig_landed_total INTEGER NOT NULL DEFAULT 0,
            sig_att_total INTEGER NOT NULL DEFAULT 0,
            td_landed_total INTEGER NOT NULL DEFAULT 0,
            td_att_total INTEGER NOT NULL DEFAULT 0,
            sub_att_total INTEGER NOT NULL DEFAULT 0,
            kd_total INTEGER NOT NULL DEFAULT 0,
            ctrl_sec_total INTEGER NOT NULL DEFAULT 0,
            last_fight_date TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (fighter_id) REFERENCES fighters(fighter_id)
        );

        CREATE INDEX idx_fighter_cards_name ON fighter_cards(name);
        CREATE INDEX idx_fighter_cards_name_norm ON fighter_cards(name_norm);
        CREATE INDEX idx_fighter_cards_last_fight_date ON fighter_cards(last_fight_date);

        CREATE TABLE build_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    con.commit()


def load_beast_fighters(beast: sqlite3.Connection) -> dict[str, FighterBio]:
    rows = beast.execute(
        """
        SELECT fighter_id, name, name_norm
        FROM dim_fighter
        WHERE name IS NOT NULL
          AND trim(name) <> ''
        ORDER BY name ASC
        """
    ).fetchall()

    fighters: dict[str, FighterBio] = {}

    for r in rows:
        fighter_id = r["fighter_id"]
        name = r["name"]
        name_norm = r["name_norm"] or normalize_name(name)

        fighters[fighter_id] = FighterBio(
            fighter_id=fighter_id,
            name=name,
            name_norm=name_norm,
            bio_source="beast",
            bio_completeness=0,
        )

    return fighters


def score_bio(bio: FighterBio) -> int:
    fields = [
        bio.nickname,
        bio.stance,
        bio.height_in,
        bio.weight_lbs,
        bio.reach_in,
    ]
    return sum(1 for x in fields if x not in (None, ""))


def list_tables(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name
        """
    ).fetchall()
    return [r["name"] for r in rows]


def find_ufc_bio_rows(ufc: sqlite3.Connection) -> list[sqlite3.Row]:
    tables = list_tables(ufc)

    candidate_tables: list[tuple[str, set[str]]] = []
    for table in tables:
        cols = ufc.execute(f"PRAGMA table_info({table})").fetchall()
        col_names = {c["name"].lower() for c in cols}

        has_name = any(c in col_names for c in ["fighter", "name", "fighter_name"])
        has_bio = any(
            c in col_names
            for c in [
                "nickname",
                "nick_name",
                "stance",
                "height_in",
                "height",
                "reach_in",
                "reach",
                "weight_lbs",
                "weight",
            ]
        )
        if has_name and has_bio:
            candidate_tables.append((table, col_names))

    for table, col_names in candidate_tables:
        select_cols: list[str] = []
        mapping: dict[str, str] = {}

        def pick(possible: list[str], alias: str) -> None:
            for p in possible:
                if p in col_names:
                    select_cols.append(f"{p} AS {alias}")
                    mapping[alias] = p
                    return

        pick(["fighter", "fighter_name", "name"], "name")
        pick(["nickname", "nick_name"], "nickname")
        pick(["stance"], "stance")
        pick(["height_in", "height"], "height_in")
        pick(["weight_lbs", "weight"], "weight_lbs")
        pick(["reach_in", "reach"], "reach_in")

        if "name" not in mapping:
            continue

        sql = f"SELECT {', '.join(select_cols)} FROM {table}"
        try:
            rows = ufc.execute(sql).fetchall()
            if rows:
                return rows
        except Exception:
            continue

    return []


def enrich_from_ufc(fighters: dict[str, FighterBio], ufc: sqlite3.Connection) -> None:
    rows = find_ufc_bio_rows(ufc)
    if not rows:
        print("[fighter_cards] UFC bio rows found: 0")
        return

    by_norm: dict[str, list[FighterBio]] = {}
    for f in fighters.values():
        by_norm.setdefault(f.name_norm, []).append(f)

    skipped_ambiguous = 0
    matched = 0

    for r in rows:
        name = r["name"]
        name_norm = normalize_name(name)
        candidates = by_norm.get(name_norm, [])

        if len(candidates) != 1:
            if len(candidates) > 1:
                skipped_ambiguous += 1
            continue

        fighter = candidates[0]

        if r["nickname"]:
            fighter.nickname = r["nickname"]
        if r["stance"]:
            fighter.stance = r["stance"]
        if r["height_in"] is not None:
            fighter.height_in = normalize_height_to_inches(r["height_in"])
        if r["weight_lbs"] is not None:
            fighter.weight_lbs = normalize_weight_to_lbs(r["weight_lbs"])
        if r["reach_in"] is not None:
            fighter.reach_in = normalize_reach_to_inches(r["reach_in"])

        fighter.bio_source = "ufc"
        fighter.bio_completeness = score_bio(fighter)
        matched += 1

    print(f"[fighter_cards] UFC bio matches applied: {matched}")
    print(f"[fighter_cards] UFC bio rows skipped as ambiguous: {skipped_ambiguous}")


def apply_overrides(fighters: dict[str, FighterBio], csv_path: Path) -> None:
    if not csv_path.exists():
        print("[fighter_cards] overrides CSV not found, skipping")
        return

    by_norm: dict[str, list[FighterBio]] = {}
    for f in fighters.values():
        by_norm.setdefault(f.name_norm, []).append(f)

    applied = 0
    skipped_ambiguous = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("name", "")
            name_norm = normalize_name(name)
            candidates = by_norm.get(name_norm, [])

            if len(candidates) != 1:
                if len(candidates) > 1:
                    skipped_ambiguous += 1
                continue

            fighter = candidates[0]

            if row.get("nickname"):
                fighter.nickname = row["nickname"]
            if row.get("stance"):
                fighter.stance = row["stance"]
            if row.get("height_in"):
                fighter.height_in = normalize_height_to_inches(row["height_in"])
            if row.get("weight_lbs"):
                fighter.weight_lbs = normalize_weight_to_lbs(row["weight_lbs"])
            if row.get("reach_in"):
                fighter.reach_in = normalize_reach_to_inches(row["reach_in"])

            fighter.bio_source = "override"
            fighter.bio_completeness = score_bio(fighter)
            applied += 1

    print(f"[fighter_cards] override rows applied: {applied}")
    print(f"[fighter_cards] override rows skipped as ambiguous: {skipped_ambiguous}")


def debug_duplicate_name_norms(fighters: dict[str, FighterBio]) -> None:
    by_norm: dict[str, list[FighterBio]] = {}

    for fighter in fighters.values():
        by_norm.setdefault(fighter.name_norm, []).append(fighter)

    dupes = {k: v for k, v in by_norm.items() if len(v) > 1}

    print(f"[fighter_cards] duplicate normalized names: {len(dupes)}")

    for norm, group in list(dupes.items())[:25]:
        print(f"  {norm}")
        for fighter in group:
            print(f"    - {fighter.fighter_id} | {fighter.name} | source={fighter.bio_source}")


def write_ambiguous_name_report(fighters: dict[str, FighterBio], out_path: Path) -> None:
    by_norm: dict[str, list[FighterBio]] = {}
    for fighter in fighters.values():
        by_norm.setdefault(fighter.name_norm, []).append(fighter)

    rows: list[dict[str, str]] = []
    for norm, group in by_norm.items():
        if len(group) < 2:
            continue
        for fighter in group:
            rows.append(
                {
                    "name_norm": norm,
                    "fighter_id": fighter.fighter_id,
                    "name": fighter.name,
                    "bio_source": fighter.bio_source,
                }
            )

    if not rows:
        if out_path.exists():
            out_path.unlink()
        print("[fighter_cards] ambiguity report rows: 0")
        return

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["name_norm", "fighter_id", "name", "bio_source"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"[fighter_cards] ambiguity report written: {out_path}")


def insert_fighters(out: sqlite3.Connection, fighters: dict[str, FighterBio]) -> None:
    now = utc_now()

    deduped: dict[str, FighterBio] = {}
    for fighter in fighters.values():
        existing = deduped.get(fighter.fighter_id)
        if existing is None:
            deduped[fighter.fighter_id] = fighter
            continue

        if score_bio(fighter) > score_bio(existing):
            deduped[fighter.fighter_id] = fighter

    rows = [
        (
            f.fighter_id,
            f.name,
            f.name_norm,
            f.nickname,
            f.stance,
            f.height_in,
            f.weight_lbs,
            f.reach_in,
            f.bio_source,
            score_bio(f),
            now,
            now,
        )
        for f in deduped.values()
    ]

    out.executemany(
        """
        INSERT INTO fighters (
            fighter_id, name, name_norm, nickname, stance, height_in, weight_lbs, reach_in,
            bio_source, bio_completeness, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    out.commit()
    print(f"[fighter_cards] fighters inserted: {len(rows)}")


def insert_fighter_fights(beast: sqlite3.Connection, out: sqlite3.Connection) -> None:
    rows = beast.execute(
        """
        SELECT
            ff.fight_id,
            ff.fighter_id,
            COALESCE(df.name, ff.fighter_id) AS fighter_name,
            ff.opponent_id,
            COALESCE(odf.name, '') AS opponent_name,
            f.event_id,
            COALESCE(e.event_name, '') AS event_name,
            COALESCE(e.event_date, f.fight_date, '') AS event_date,
            COALESCE(f.weight_class, '') AS weight_class,
            COALESCE(f.method, '') AS method,
            f.round AS finish_round,
            f.time AS finish_time_sec,
            f.url AS fight_url,

            CASE
                WHEN ff.is_win IN (1, '1', 'W', 'w', 'WIN', 'win', 'Win', 'TRUE', 'true') THEN 1
                WHEN ff.is_win IN (0, '0', 'L', 'l', 'LOSS', 'loss', 'Loss', 'FALSE', 'false') THEN 0
                ELSE NULL
            END AS is_win,

            ff.kd,
            ff.sig_landed,
            ff.sig_att,
            ff.total_landed,
            ff.total_att,
            ff.td_landed,
            ff.td_att,
            ff.sub_att,
            ff.rev,
            ff.ctrl_sec,
            ff.src
        FROM fact_fighter_fight_stats ff
        LEFT JOIN dim_fighter df
          ON df.fighter_id = ff.fighter_id
        LEFT JOIN dim_fighter odf
          ON odf.fighter_id = ff.opponent_id
        LEFT JOIN dim_fight f
          ON f.fight_id = ff.fight_id
        LEFT JOIN dim_event e
          ON e.event_id = f.event_id
        ORDER BY event_date DESC, fighter_name ASC
        """
    ).fetchall()

    out.executemany(
        """
        INSERT INTO fighter_fights (
            fight_id, fighter_id, fighter_name, opponent_id, opponent_name, event_id, event_name,
            event_date, weight_class, method, finish_round, finish_time_sec, fight_url, is_win,
            kd, sig_landed, sig_att, total_landed, total_att, td_landed, td_att, sub_att, rev,
            ctrl_sec, src
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(r) for r in rows],
    )
    out.commit()
    print(f"[fighter_cards] fighter_fights inserted: {len(rows)}")


def repair_fight_outcomes(out: sqlite3.Connection) -> None:
    rows = out.execute(
        """
        SELECT
            fight_id,
            fight_url,
            COUNT(*) AS row_count,
            SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) AS wins_marked,
            SUM(CASE WHEN is_win = 0 THEN 1 ELSE 0 END) AS losses_marked
        FROM fighter_fights
        GROUP BY fight_id, fight_url
        """
    ).fetchall()

    repaired = 0

    for r in rows:
        fight_id = r["fight_id"]
        fight_url = r["fight_url"]
        row_count = r["row_count"]
        wins_marked = r["wins_marked"] or 0
        losses_marked = r["losses_marked"] or 0

        if row_count != 2:
            continue

        if wins_marked == 1 and losses_marked == 1:
            continue

        pair = out.execute(
            """
            SELECT fighter_fight_id, fighter_id, kd, sig_landed, td_landed, sub_att, ctrl_sec
            FROM fighter_fights
            WHERE fight_id = ?
            ORDER BY fighter_fight_id ASC
            """,
            (fight_id,),
        ).fetchall()

        if len(pair) != 2:
            continue

        a, b = pair[0], pair[1]

        def score(x):
            return (
                (x["kd"] or 0) * 100
                + (x["sub_att"] or 0) * 10
                + (x["td_landed"] or 0) * 8
                + (x["sig_landed"] or 0) * 1
                + (x["ctrl_sec"] or 0) * 0.01
            )

        sa = score(a)
        sb = score(b)

        if sa == sb:
            continue

        a_win = 1 if sa > sb else 0
        b_win = 0 if sa > sb else 1

        out.execute(
            "UPDATE fighter_fights SET is_win = ? WHERE fighter_fight_id = ?",
            (a_win, a["fighter_fight_id"]),
        )
        out.execute(
            "UPDATE fighter_fights SET is_win = ? WHERE fighter_fight_id = ?",
            (b_win, b["fighter_fight_id"]),
        )
        repaired += 2

    out.commit()
    print(f"[fighter_cards] fight outcomes repaired: {repaired}") 


def insert_fighter_rounds(beast: sqlite3.Connection, out: sqlite3.Connection) -> None:
    rows = beast.execute(
        """
        SELECT
            fr.fight_url,
            f.fight_id,
            fr.fighter_id,
            df.name AS fighter_name,
            fr.round,
            fr.sig_landed,
            fr.sig_attempted,
            fr.tot_landed,
            fr.tot_attempted,
            fr.td_landed,
            fr.td_attempted,
            fr.kd,
            fr.sub_att,
            fr.rev,
            fr.passes,
            fr.ctrl_sec,
            fr.head_landed,
            fr.head_attempted,
            fr.body_landed,
            fr.body_attempted,
            fr.leg_landed,
            fr.leg_attempted,
            fr.dist_landed,
            fr.dist_attempted,
            fr.clinch_landed,
            fr.clinch_attempted,
            fr.ground_landed,
            fr.ground_attempted,
            fr.src
        FROM fact_fighter_round_stats fr
        LEFT JOIN dim_fighter df
          ON df.fighter_id = fr.fighter_id
        LEFT JOIN dim_fight f
          ON f.url = fr.fight_url
        ORDER BY fr.fight_url, fr.round
        """
    ).fetchall()

    out.executemany(
        """
        INSERT INTO fighter_rounds (
            fight_url, fight_id, fighter_id, fighter_name, round,
            sig_landed, sig_attempted, tot_landed, tot_attempted,
            td_landed, td_attempted, kd, sub_att, rev, passes, ctrl_sec,
            head_landed, head_attempted, body_landed, body_attempted,
            leg_landed, leg_attempted, dist_landed, dist_attempted,
            clinch_landed, clinch_attempted, ground_landed, ground_attempted, src
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(r) for r in rows],
    )
    out.commit()
    print(f"[fighter_cards] fighter_rounds inserted: {len(rows)}")


def insert_fighter_cards(out: sqlite3.Connection) -> None:
    now = utc_now()

    rows = out.execute(
        """
        SELECT
            f.fighter_id,
            f.name,
            f.name_norm,
            f.nickname,
            f.stance,
            f.height_in,
            f.weight_lbs,
            f.reach_in,
            COUNT(ff.fight_id) AS total_fights,
            SUM(CASE WHEN ff.is_win = 1 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN ff.is_win = 0 THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN ff.is_win IS NULL THEN 1 ELSE 0 END) AS no_contests,
            0 AS draws,
            SUM(COALESCE(ff.sig_landed, 0)) AS sig_landed_total,
            SUM(COALESCE(ff.sig_att, 0)) AS sig_att_total,
            SUM(COALESCE(ff.td_landed, 0)) AS td_landed_total,
            SUM(COALESCE(ff.td_att, 0)) AS td_att_total,
            SUM(COALESCE(ff.sub_att, 0)) AS sub_att_total,
            SUM(COALESCE(ff.kd, 0)) AS kd_total,
            SUM(COALESCE(ff.ctrl_sec, 0)) AS ctrl_sec_total,
            MAX(ff.event_date) AS last_fight_date
        FROM fighters f
        LEFT JOIN fighter_fights ff
          ON ff.fighter_id = f.fighter_id
        GROUP BY
            f.fighter_id,
            f.name,
            f.name_norm,
            f.nickname,
            f.stance,
            f.height_in,
            f.weight_lbs,
            f.reach_in
        ORDER BY f.name ASC
        """
    ).fetchall()

    payload = []
    for r in rows:
        payload.append(
            (
                r["fighter_id"],
                r["name"],
                r["name_norm"],
                r["nickname"],
                r["stance"],
                r["height_in"],
                r["weight_lbs"],
                r["reach_in"],
                int(r["total_fights"] or 0),
                int(r["wins"] or 0),
                int(r["losses"] or 0),
                int(r["draws"] or 0),
                int(r["no_contests"] or 0),
                int(r["sig_landed_total"] or 0),
                int(r["sig_att_total"] or 0),
                int(r["td_landed_total"] or 0),
                int(r["td_att_total"] or 0),
                int(r["sub_att_total"] or 0),
                int(r["kd_total"] or 0),
                int(r["ctrl_sec_total"] or 0),
                r["last_fight_date"],
                now,
            )
        )

    out.executemany(
        """
        INSERT INTO fighter_cards (
            fighter_id,
            name,
            name_norm,
            nickname,
            stance,
            height_in,
            weight_lbs,
            reach_in,
            total_fights,
            wins,
            losses,
            draws,
            no_contests,
            sig_landed_total,
            sig_att_total,
            td_landed_total,
            td_att_total,
            sub_att_total,
            kd_total,
            ctrl_sec_total,
            last_fight_date,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    out.commit()
    print(f"[fighter_cards] fighter_cards inserted: {len(payload)}")


def print_sample_fighter_cards(out: sqlite3.Connection, limit: int = 15) -> None:
    rows = out.execute(
        """
        SELECT
            name,
            nickname,
            stance,
            height_in,
            weight_lbs,
            reach_in,
            total_fights,
            wins,
            losses,
            draws,
            no_contests,
            sig_landed_total,
            sig_att_total,
            td_landed_total,
            td_att_total,
            kd_total,
            last_fight_date
        FROM fighter_cards
        ORDER BY total_fights DESC, name ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    print("\n[fighter_cards] sample output")
    if not rows:
        print("  no fighter cards found")
        return

    for r in rows:
        record = f"{r['wins']}-{r['losses']}"
        if (r["draws"] or 0) > 0:
            record += f"-{r['draws']}"
        if (r["no_contests"] or 0) > 0:
            record += f" ({r['no_contests']} NC)"

        print(
            f"  {r['name']} | "
            f"record={record} | "
            f"fights={r['total_fights']} | "
            f"sig={r['sig_landed_total']}/{r['sig_att_total']} | "
            f"td={r['td_landed_total']}/{r['td_att_total']} | "
            f"kd={r['kd_total']} | "
            f"stance={r['stance'] or '—'} | "
            f"ht={r['height_in'] or '—'} | "
            f"wt={r['weight_lbs'] or '—'} | "
            f"reach={r['reach_in'] or '—'} | "
            f"last={r['last_fight_date'] or '—'}"
        )


def write_build_meta(out: sqlite3.Connection, fighters_count: int) -> None:
    meta = [
        ("built_at", utc_now()),
        ("fighters_count", str(fighters_count)),
        ("source_beast", str(BEAST_DB)),
        ("source_ufc", str(UFC_DB)),
        ("source_overrides", str(OVERRIDES_CSV)),
        ("out_db", str(OUT_DB)),
    ]
    out.executemany(
        "INSERT INTO build_meta (key, value) VALUES (?, ?)",
        meta,
    )
    out.commit()


def main() -> None:
    if not BEAST_DB.exists():
        raise FileNotFoundError(f"Missing beast DB: {BEAST_DB}")

    beast = connect(BEAST_DB)
    out = connect(OUT_DB)

    try:
        create_schema(out)

        fighters = load_beast_fighters(beast)

        if UFC_DB.exists():
            ufc = connect(UFC_DB)
            try:
                enrich_from_ufc(fighters, ufc)
            finally:
                ufc.close()
        else:
            print("[fighter_cards] UFC DB not found, skipping enrichment")

        apply_overrides(fighters, OVERRIDES_CSV)
        debug_duplicate_name_norms(fighters)

        insert_fighters(out, fighters)
        insert_fighter_fights(beast, out)
        repair_fight_outcomes(out)
        insert_fighter_rounds(beast, out)
        insert_fighter_cards(out)
        
        print(f"\nBuilt fighter card DB: {OUT_DB}")

    finally:
        beast.close()
        out.close()


if __name__ == "__main__":
    main()
