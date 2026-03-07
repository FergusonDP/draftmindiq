from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


@dataclass
class AuditResult:
    ok: bool
    db_path: str
    counts: Dict[str, int]
    checks: Dict[str, Any]
    samples: Dict[str, Any]


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def scalar(con: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> int:
    row = con.execute(sql, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def rows_as_dicts(
    con: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()
) -> List[Dict[str, Any]]:
    rows = con.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_counts(con: sqlite3.Connection) -> Dict[str, int]:
    tables = [
        "dim_fighter",
        "dim_event",
        "dim_fight",
        "fact_fighter_fight_stats",
        "fact_fighter_round_stats_long",
        "fact_fighter_round_stats",
        "feature_1",
        "feature_2",
        "feature_3",
        "ss_fighter_map",
    ]
    out: Dict[str, int] = {}
    for t in tables:
        out[t] = scalar(con, f"SELECT COUNT(*) FROM {t}") if table_exists(con, t) else -1
    return out


def audit_missing_fight_sides(con: sqlite3.Connection) -> int:
    return scalar(
        con,
        """
        SELECT COUNT(*)
        FROM dim_fight
        WHERE fighter_a_id IS NULL OR fighter_b_id IS NULL
        """,
    )


def audit_fighters_without_stats(con: sqlite3.Connection) -> int:
    return scalar(
        con,
        """
        SELECT COUNT(*)
        FROM dim_fighter f
        LEFT JOIN fact_fighter_fight_stats s
          ON f.fighter_id = s.fighter_id
        WHERE s.fighter_id IS NULL
        """,
    )


def audit_round_rows_with_without_fighter_id(con: sqlite3.Connection) -> Dict[str, int]:
    with_id = scalar(
        con,
        """
        SELECT COUNT(*)
        FROM fact_fighter_round_stats
        WHERE fighter_id IS NOT NULL
        """,
    )
    without_id = scalar(
        con,
        """
        SELECT COUNT(*)
        FROM fact_fighter_round_stats
        WHERE fighter_id IS NULL
        """,
    )
    return {
        "with_fighter_id": with_id,
        "without_fighter_id": without_id,
    }


def audit_ss_unmapped(con: sqlite3.Connection) -> int:
    return scalar(
        con,
        """
        SELECT COUNT(*)
        FROM ss_fighter_map
        WHERE fighter_id IS NULL
        """,
    )


def top_unmapped_ss_names(con: sqlite3.Connection, limit: int = 25) -> List[Dict[str, Any]]:
    return rows_as_dicts(
        con,
        """
        SELECT
            ss_fighter_norm,
            COUNT(*) AS n
        FROM ss_fighter_map
        WHERE fighter_id IS NULL
        GROUP BY ss_fighter_norm
        ORDER BY n DESC, ss_fighter_norm
        LIMIT ?
        """,
        (limit,),
    )


def duplicate_fighter_names(con: sqlite3.Connection, limit: int = 25) -> List[Dict[str, Any]]:
    return rows_as_dicts(
        con,
        """
        SELECT
            name_norm,
            COUNT(*) AS n,
            GROUP_CONCAT(fighter_id, ', ') AS fighter_ids
        FROM dim_fighter
        GROUP BY name_norm
        HAVING COUNT(*) > 1
        ORDER BY n DESC, name_norm
        LIMIT ?
        """,
        (limit,),
    )


def duplicate_fight_urls(con: sqlite3.Connection, limit: int = 25) -> List[Dict[str, Any]]:
    return rows_as_dicts(
        con,
        """
        SELECT
            url,
            COUNT(*) AS n,
            GROUP_CONCAT(fight_id, ', ') AS fight_ids
        FROM dim_fight
        WHERE url IS NOT NULL AND TRIM(url) <> ''
        GROUP BY url
        HAVING COUNT(*) > 1
        ORDER BY n DESC, url
        LIMIT ?
        """,
        (limit,),
    )


def orphan_fight_stat_rows(con: sqlite3.Connection) -> Dict[str, int]:
    missing_fight = scalar(
        con,
        """
        SELECT COUNT(*)
        FROM fact_fighter_fight_stats s
        LEFT JOIN dim_fight f
          ON s.fight_id = f.fight_id
        WHERE f.fight_id IS NULL
        """,
    )
    missing_fighter = scalar(
        con,
        """
        SELECT COUNT(*)
        FROM fact_fighter_fight_stats s
        LEFT JOIN dim_fighter f
          ON s.fighter_id = f.fighter_id
        WHERE f.fighter_id IS NULL
        """,
    )
    return {
        "fight_stats_missing_dim_fight": missing_fight,
        "fight_stats_missing_dim_fighter": missing_fighter,
    }


def orphan_feature_rows(con: sqlite3.Connection) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for t in ("feature_1", "feature_2", "feature_3"):
        if not table_exists(con, t):
            out[f"{t}_missing_dim_fight"] = -1
            out[f"{t}_missing_dim_fighter"] = -1
            continue

        out[f"{t}_missing_dim_fight"] = scalar(
            con,
            f"""
            SELECT COUNT(*)
            FROM {t} x
            LEFT JOIN dim_fight f
              ON x.fight_id = f.fight_id
            WHERE f.fight_id IS NULL
            """,
        )
        out[f"{t}_missing_dim_fighter"] = scalar(
            con,
            f"""
            SELECT COUNT(*)
            FROM {t} x
            LEFT JOIN dim_fighter df
              ON x.fighter_id = df.fighter_id
            WHERE df.fighter_id IS NULL
            """,
        )
    return out


def suspicious_fight_stats(con: sqlite3.Connection, limit: int = 25) -> List[Dict[str, Any]]:
    return rows_as_dicts(
        con,
        """
        SELECT
            s.fight_id,
            s.fighter_id,
            df.name,
            s.kd,
            s.sig_landed,
            s.sig_att,
            s.total_landed,
            s.total_att,
            s.td_landed,
            s.td_att,
            s.sub_att,
            s.rev,
            s.ctrl_sec
        FROM fact_fighter_fight_stats s
        LEFT JOIN dim_fighter df
          ON s.fighter_id = df.fighter_id
        WHERE
            (s.kd IS NOT NULL AND s.kd < 0)
            OR (s.sig_landed IS NOT NULL AND s.sig_landed < 0)
            OR (s.sig_att IS NOT NULL AND s.sig_att < 0)
            OR (s.total_landed IS NOT NULL AND s.total_landed < 0)
            OR (s.total_att IS NOT NULL AND s.total_att < 0)
            OR (s.td_landed IS NOT NULL AND s.td_landed < 0)
            OR (s.td_att IS NOT NULL AND s.td_att < 0)
            OR (s.sub_att IS NOT NULL AND s.sub_att < 0)
            OR (s.rev IS NOT NULL AND s.rev < 0)
            OR (s.ctrl_sec IS NOT NULL AND s.ctrl_sec < 0)
            OR (s.sig_landed IS NOT NULL AND s.sig_att IS NOT NULL AND s.sig_landed > s.sig_att)
            OR (s.total_landed IS NOT NULL AND s.total_att IS NOT NULL AND s.total_landed > s.total_att)
            OR (s.td_landed IS NOT NULL AND s.td_att IS NOT NULL AND s.td_landed > s.td_att)
            OR (s.ctrl_sec IS NOT NULL AND s.ctrl_sec > 3600)
            OR (s.sig_att IS NOT NULL AND s.sig_att > 500)
            OR (s.total_att IS NOT NULL AND s.total_att > 700)
            OR (s.td_att IS NOT NULL AND s.td_att > 50)
        ORDER BY s.fight_id, s.fighter_id
        LIMIT ?
        """,
        (limit,),
    )


def suspicious_round_stats(con: sqlite3.Connection, limit: int = 25) -> List[Dict[str, Any]]:
    return rows_as_dicts(
        con,
        """
        SELECT
            fight_url,
            round,
            fighter_id,
            ss_fighter,
            sig_landed,
            sig_attempted,
            tot_landed,
            tot_attempted,
            td_landed,
            td_attempted,
            kd,
            sub_att,
            rev,
            passes,
            ctrl_sec
        FROM fact_fighter_round_stats
        WHERE
            (sig_landed IS NOT NULL AND sig_landed < 0)
            OR (sig_attempted IS NOT NULL AND sig_attempted < 0)
            OR (tot_landed IS NOT NULL AND tot_landed < 0)
            OR (tot_attempted IS NOT NULL AND tot_attempted < 0)
            OR (td_landed IS NOT NULL AND td_landed < 0)
            OR (td_attempted IS NOT NULL AND td_attempted < 0)
            OR (kd IS NOT NULL AND kd < 0)
            OR (sub_att IS NOT NULL AND sub_att < 0)
            OR (rev IS NOT NULL AND rev < 0)
            OR (passes IS NOT NULL AND passes < 0)
            OR (ctrl_sec IS NOT NULL AND ctrl_sec < 0)
            OR (sig_landed IS NOT NULL AND sig_attempted IS NOT NULL AND sig_landed > sig_attempted)
            OR (tot_landed IS NOT NULL AND tot_attempted IS NOT NULL AND tot_landed > tot_attempted)
            OR (td_landed IS NOT NULL AND td_attempted IS NOT NULL AND td_landed > td_attempted)
            OR (ctrl_sec IS NOT NULL AND ctrl_sec > 900)
            OR (sig_attempted IS NOT NULL AND sig_attempted > 200)
            OR (tot_attempted IS NOT NULL AND tot_attempted > 300)
            OR (td_attempted IS NOT NULL AND td_attempted > 20)
        ORDER BY fight_url, round, ss_fighter
        LIMIT ?
        """,
        (limit,),
    )


def build_summary(result: AuditResult) -> str:
    counts = result.counts
    checks = result.checks

    lines: List[str] = []
    lines.append("=" * 72)
    lines.append("BEAST AUDIT")
    lines.append("=" * 72)
    lines.append(f"DB: {result.db_path}")
    lines.append("")
    lines.append("TABLE COUNTS")
    for k, v in counts.items():
        lines.append(f"  {k:28} {v}")
    lines.append("")
    lines.append("CHECKS")
    lines.append(f"  missing_fight_sides           {checks['missing_fight_sides']}")
    lines.append(f"  fighters_without_stats        {checks['fighters_without_stats']}")
    lines.append(f"  ss_unmapped                   {checks['ss_unmapped']}")
    lines.append(f"  round_rows_with_fighter_id    {checks['round_rows']['with_fighter_id']}")
    lines.append(f"  round_rows_without_fighter_id {checks['round_rows']['without_fighter_id']}")
    lines.append(f"  duplicate_fighter_names       {checks['duplicate_fighter_name_count']}")
    lines.append(f"  duplicate_fight_urls          {checks['duplicate_fight_url_count']}")
    lines.append(f"  suspicious_fight_stat_rows    {checks['suspicious_fight_stat_row_count']}")
    lines.append(f"  suspicious_round_stat_rows    {checks['suspicious_round_stat_row_count']}")

    orphan = checks["orphan_fight_stats"]
    lines.append(f"  orphan_fight_stats_dim_fight  {orphan['fight_stats_missing_dim_fight']}")
    lines.append(f"  orphan_fight_stats_dim_ftr    {orphan['fight_stats_missing_dim_fighter']}")

    feat = checks["orphan_features"]
    for key in sorted(feat):
        lines.append(f"  {key:28} {feat[key]}")

    lines.append("")
    lines.append("TOP UNMAPPED SS NAMES")
    if result.samples["top_unmapped_ss_names"]:
        for row in result.samples["top_unmapped_ss_names"]:
            lines.append(f"  {row['ss_fighter_norm']!r:30} {row['n']}")
    else:
        lines.append("  none")

    lines.append("")
    lines.append("DUPLICATE FIGHTER NAMES")
    if result.samples["duplicate_fighter_names"]:
        for row in result.samples["duplicate_fighter_names"]:
            lines.append(f"  {row['name_norm']!r:30} {row['n']}  {row['fighter_ids']}")
    else:
        lines.append("  none")

    lines.append("")
    lines.append("DUPLICATE FIGHT URLS")
    if result.samples["duplicate_fight_urls"]:
        for row in result.samples["duplicate_fight_urls"]:
            lines.append(f"  {row['url']!r:50} {row['n']}")
    else:
        lines.append("  none")

    lines.append("")
    lines.append("SUSPICIOUS FIGHT STAT ROWS")
    if result.samples["suspicious_fight_stats"]:
        for row in result.samples["suspicious_fight_stats"][:10]:
            lines.append(
                f"  fight_id={row['fight_id']} fighter_id={row['fighter_id']} "
                f"name={row.get('name')} sig={row.get('sig_landed')}/{row.get('sig_att')} "
                f"tot={row.get('total_landed')}/{row.get('total_att')} "
                f"td={row.get('td_landed')}/{row.get('td_att')} ctrl={row.get('ctrl_sec')}"
            )
    else:
        lines.append("  none")

    lines.append("")
    lines.append("SUSPICIOUS ROUND STAT ROWS")
    if result.samples["suspicious_round_stats"]:
        for row in result.samples["suspicious_round_stats"][:10]:
            lines.append(
                f"  url={row['fight_url']} r={row['round']} ss_fighter={row['ss_fighter']} "
                f"sig={row.get('sig_landed')}/{row.get('sig_attempted')} "
                f"tot={row.get('tot_landed')}/{row.get('tot_attempted')} "
                f"td={row.get('td_landed')}/{row.get('td_attempted')} ctrl={row.get('ctrl_sec')}"
            )
    else:
        lines.append("  none")

    lines.append("")
    lines.append(f"OVERALL STATUS: {'PASS' if result.ok else 'WARN/FAIL'}")
    lines.append("=" * 72)
    return "\n".join(lines)


def run_audit(db_path: str, unmapped_warn_threshold: int = 500) -> AuditResult:
    con = connect(db_path)
    try:
        counts = get_table_counts(con)

        checks: Dict[str, Any] = {}
        checks["missing_fight_sides"] = audit_missing_fight_sides(con)
        checks["fighters_without_stats"] = audit_fighters_without_stats(con)
        checks["ss_unmapped"] = audit_ss_unmapped(con)
        checks["round_rows"] = audit_round_rows_with_without_fighter_id(con)
        checks["orphan_fight_stats"] = orphan_fight_stat_rows(con)
        checks["orphan_features"] = orphan_feature_rows(con)

        dup_names = duplicate_fighter_names(con, limit=50)
        dup_urls = duplicate_fight_urls(con, limit=50)
        bad_fight_stats = suspicious_fight_stats(con, limit=50)
        bad_round_stats = suspicious_round_stats(con, limit=50)
        top_unmapped = top_unmapped_ss_names(con, limit=50)

        checks["duplicate_fighter_name_count"] = len(dup_names)
        checks["duplicate_fight_url_count"] = len(dup_urls)
        checks["suspicious_fight_stat_row_count"] = len(bad_fight_stats)
        checks["suspicious_round_stat_row_count"] = len(bad_round_stats)

        samples = {
            "top_unmapped_ss_names": top_unmapped,
            "duplicate_fighter_names": dup_names,
            "duplicate_fight_urls": dup_urls,
            "suspicious_fight_stats": bad_fight_stats,
            "suspicious_round_stats": bad_round_stats,
        }

        ok = True

        # Hard failures
        if counts.get("dim_fighter", 0) <= 0:
            ok = False
        if counts.get("dim_fight", 0) <= 0:
            ok = False
        if counts.get("fact_fighter_fight_stats", 0) <= 0:
            ok = False
        if checks["missing_fight_sides"] > 0:
            ok = False
        if checks["orphan_fight_stats"]["fight_stats_missing_dim_fight"] > 0:
            ok = False
        if checks["orphan_fight_stats"]["fight_stats_missing_dim_fighter"] > 0:
            ok = False

        # Soft warnings can still pass, but if they are extreme, fail
        if checks["ss_unmapped"] > unmapped_warn_threshold:
            ok = False

        total_round_rows = (
            checks["round_rows"]["with_fighter_id"] + checks["round_rows"]["without_fighter_id"]
        )
        if total_round_rows > 0:
            frac_unmapped_round = checks["round_rows"]["without_fighter_id"] / total_round_rows
            if frac_unmapped_round > 0.10:
                ok = False

        return AuditResult(
            ok=ok,
            db_path=db_path,
            counts=counts,
            checks=checks,
            samples=samples,
        )
    finally:
        con.close()


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Audit DraftMindIQ Beast SQLite DB")
    ap.add_argument(
        "--db",
        default="data/beast.sqlite",
        help="Path to beast.sqlite (default: data/beast.sqlite)",
    )
    ap.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of human-readable text",
    )
    ap.add_argument(
        "--unmapped-warn-threshold",
        type=int,
        default=500,
        help="Fail audit if ss_unmapped exceeds this threshold (default: 500)",
    )
    args = ap.parse_args(argv)

    db_path = str(Path(args.db).resolve())
    if not Path(db_path).exists():
        print(f"ERROR: DB not found: {db_path}", file=sys.stderr)
        return 2

    result = run_audit(
        db_path=db_path,
        unmapped_warn_threshold=args.unmapped_warn_threshold,
    )

    if args.json:
        print(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    else:
        print(build_summary(result))

    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
