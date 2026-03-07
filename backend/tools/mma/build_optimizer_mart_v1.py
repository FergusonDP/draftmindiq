# backend\tools\mma\build_optimizer_mart_v1.py

import os
import sqlite3
from datetime import datetime

DB = r"data\marts\mma_historical_ss_full.sqlite"

FEATURES_TABLE = "mart_fighter_fight_features_v1"
ROLLUPS_TABLE = "mart_fighter_rollups_v1"
FIGHTDIM_VIEW = "mart_fight_dim_v1"
READY_TABLE = "opt_fighter_fight_ready_v1"


# -----------------------------
# DB helpers
# -----------------------------


def connect(db_path: str):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB not found: {db_path}")
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys=ON;")
    # window functions are fine on modern sqlite; keep default row_factory
    return con


def execmany(cur, stmts):
    for s in stmts:
        cur.execute(s)


# -----------------------------
# Common filters
# -----------------------------


def _fighter_is_garbage_sql(alias: str = "f"):
    """
    SQL boolean expression that flags known garbage rows that came from
    bad scraping / navigation text.
    """
    return f"""
    (
      {alias}.fighter IS NULL
      OR length(trim({alias}.fighter)) < 2
      OR length(trim({alias}.fighter)) > 60
      OR {alias}.fighter LIKE '%Sports-Statistics%'
      OR {alias}.fighter LIKE '%Fight Statistics%'
      OR {alias}.fighter LIKE '%Odds Calculator%'
      OR {alias}.fighter LIKE '%Scheduled UFC Events%'
      OR {alias}.fighter LIKE '%Privacy Policy%'
      OR {alias}.fighter LIKE '%Contact Us%'
      OR {alias}.fighter LIKE '%BY SPORT%'
      OR {alias}.fighter LIKE '%All SPORTS%'
      OR {alias}.fighter LIKE '%Home > UFC%'
    )
    """


# -----------------------------
# Fight dimension (ordering)
# -----------------------------


def build_fight_dim(cur):
    """
    Stable ordering proxy:
      ss_fights (fight_url -> event_url)
      ss_events (event crawl timestamp)
      then fight_url within event

    Produces:
      mart_fight_dim_v1(fight_url, event_date, fight_order)
    """
    cur.execute(f"DROP VIEW IF EXISTS {FIGHTDIM_VIEW};")

    cur.execute(
        f"""
        CREATE VIEW {FIGHTDIM_VIEW} AS
        WITH base AS (
          SELECT
            f.fight_url,
            f.event_url,
            e.discovered_at AS event_discovered_at,
            e.last_seen_at  AS event_last_seen_at
          FROM ss_fights f
          LEFT JOIN ss_events e
            ON e.event_url = f.event_url
          WHERE f.fight_url IS NOT NULL AND f.fight_url <> ''
        ),
        ranked AS (
          SELECT
            fight_url,
            COALESCE(event_discovered_at, event_last_seen_at) AS event_date_proxy,
            ROW_NUMBER() OVER (
              ORDER BY COALESCE(event_discovered_at, event_last_seen_at), event_url, fight_url
            ) AS fight_order
          FROM base
        )
        SELECT
          fight_url,
          event_date_proxy AS event_date,
          fight_order
        FROM ranked;
        """
    )

    return {"mode": "ss_fights_to_ss_events_proxy_time"}


# -----------------------------
# Features (per fighter per fight)
# -----------------------------


def build_features(cur):
    cur.execute(f"DROP TABLE IF EXISTS {FEATURES_TABLE};")

    # Stat keys present in your canon_fight_totals table (per your schema dump)
    K_SIG_LANDED = "Significant Strikes"
    K_SIG_ATT = "Significant Strikes Attempted"
    K_TOT_LANDED = "Total Strikes"
    K_TOT_ATT = "Total Strikes Attempted"
    K_TD_LINE = "Takedowns"  # a_value often "X of Y"
    K_SUB_ATT = "Submissions Attempted"  # numeric string
    K_REV = "Reversals"  # numeric string
    K_KD = "Knockdowns"  # numeric string
    K_PASSES = "Passes"  # numeric string

    # Optional control-like keys (if any exist)
    ctrl_keys = [
        r[0]
        for r in cur.execute(
            "SELECT DISTINCT stat_key FROM canon_fight_totals WHERE lower(stat_key) LIKE '%control%';"
        ).fetchall()
        if r[0] is not None
    ]
    K_CTRL = ctrl_keys[0] if ctrl_keys else None

    # Create table
    cur.execute(
        f"""
        CREATE TABLE {FEATURES_TABLE} (
            fight_url TEXT NOT NULL,
            fighter   TEXT NOT NULL,
            opponent  TEXT,
            is_win    INTEGER,
            weight_class TEXT,
            method    TEXT,
            finish_round INTEGER,
            finish_time TEXT,

            fight_seconds REAL,

            sig_landed REAL, sig_attempted REAL,
            tot_landed REAL, tot_attempted REAL,
            td_landed  REAL, td_attempted  REAL,
            sub_att    REAL,
            rev        REAL,
            kd         REAL,
            passes     REAL,
            ctrl_sec   REAL,

            PRIMARY KEY (fight_url, fighter)
        );
        """
    )

    # Insert base rows
    # NOTE: filter garbage fighters here so they never enter mart
    cur.execute(
        f"""
        INSERT INTO {FEATURES_TABLE} (
            fight_url, fighter, opponent, is_win, weight_class, method, finish_round, finish_time,
            fight_seconds,
            sig_landed, sig_attempted, tot_landed, tot_attempted, td_landed, td_attempted,
            sub_att, rev, kd, passes, ctrl_sec
        )
        SELECT
            s.fight_url,
            trim(s.fighter) AS fighter,
            trim(s.opponent) AS opponent,
            s.is_win,
            s.weight_class,
            s.method,
            CAST(s.round AS INTEGER),
            s.time,

            CASE
              WHEN s.round IS NULL OR s.round = '' THEN 900
              WHEN s.time IS NULL OR s.time = '' THEN (CAST(s.round AS INTEGER) - 1) * 300 + 300
              WHEN instr(s.time, ':') = 0 THEN (CAST(s.round AS INTEGER) - 1) * 300 + 300
              ELSE
                (CAST(s.round AS INTEGER) - 1) * 300
                + (CAST(substr(s.time, 1, instr(s.time, ':')-1) AS INTEGER) * 60)
                + (CAST(substr(s.time, instr(s.time, ':')+1) AS INTEGER))
            END AS fight_seconds,

            0,0,0,0,0,0,0,0,0,0,0
        FROM ss_fact_fighter_fights s
        WHERE s.fight_url IS NOT NULL AND s.fight_url <> ''
          AND NOT {_fighter_is_garbage_sql("s")}
        ;
        """
    )

    # Helpers to parse canon_fight_totals.a_value
    # numeric string -> REAL
    def num_expr(key, out_col):
        return f"""
        SUM(
          CASE WHEN t.stat_key='{key}' THEN
            CASE
              WHEN t.a_value IS NULL THEN 0
              WHEN trim(t.a_value)='' THEN 0
              WHEN trim(t.a_value)='--' THEN 0
              ELSE CAST(t.a_value AS REAL)
            END
          ELSE 0 END
        ) AS {out_col}
        """

    # "X of Y" -> landed/attempted (fallback to a_landed/a_attempted if present)
    def ofy_landed_expr(key, out_col):
        return f"""
        SUM(
          CASE WHEN t.stat_key='{key}' THEN
            CASE
              WHEN t.a_landed IS NOT NULL THEN CAST(t.a_landed AS REAL)
              WHEN t.a_value LIKE '% of %' THEN CAST(substr(t.a_value, 1, instr(t.a_value, ' of ')-1) AS REAL)
              ELSE 0
            END
          ELSE 0 END
        ) AS {out_col}
        """

    def ofy_attempted_expr(key, out_col):
        return f"""
        SUM(
          CASE WHEN t.stat_key='{key}' THEN
            CASE
              WHEN t.a_attempted IS NOT NULL THEN CAST(t.a_attempted AS REAL)
              WHEN t.a_value LIKE '% of %' THEN CAST(substr(t.a_value, instr(t.a_value, ' of ')+4) AS REAL)
              ELSE 0
            END
          ELSE 0 END
        ) AS {out_col}
        """

    wide_expr = ",\n            ".join(
        [
            num_expr(K_SIG_LANDED, "sig_landed"),
            num_expr(K_SIG_ATT, "sig_attempted"),
            num_expr(K_TOT_LANDED, "tot_landed"),
            num_expr(K_TOT_ATT, "tot_attempted"),
            ofy_landed_expr(K_TD_LINE, "td_landed"),
            ofy_attempted_expr(K_TD_LINE, "td_attempted"),
            num_expr(K_SUB_ATT, "sub_att"),
            num_expr(K_REV, "rev"),
            num_expr(K_KD, "kd"),
            num_expr(K_PASSES, "passes"),
            (num_expr(K_CTRL, "ctrl_sec") if K_CTRL else "0 AS ctrl_sec"),
        ]
    )

    # Update wide stats from canon_fight_totals (long -> wide)
    cur.execute(
        f"""
        WITH wide AS (
          SELECT
            t.fight_url,
            trim(t.fighter) AS fighter,
            {wide_expr}
          FROM canon_fight_totals t
          WHERE t.fight_url IS NOT NULL AND t.fight_url <> ''
            AND t.fighter IS NOT NULL AND trim(t.fighter) <> ''
          GROUP BY t.fight_url, trim(t.fighter)
        )
        UPDATE {FEATURES_TABLE}
        SET
          sig_landed    = COALESCE((SELECT sig_landed    FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          sig_attempted = COALESCE((SELECT sig_attempted FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          tot_landed    = COALESCE((SELECT tot_landed    FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          tot_attempted = COALESCE((SELECT tot_attempted FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0)
        ;
        """
    )
    # Fix the typo in tot_attempted update (above) by running a correct update pass.
    cur.execute(
        f"""
        WITH wide AS (
          SELECT
            t.fight_url,
            trim(t.fighter) AS fighter,
            {wide_expr}
          FROM canon_fight_totals t
          WHERE t.fight_url IS NOT NULL AND t.fight_url <> ''
            AND t.fighter IS NOT NULL AND trim(t.fighter) <> ''
          GROUP BY t.fight_url, trim(t.fighter)
        )
        UPDATE {FEATURES_TABLE}
        SET
          tot_attempted = COALESCE((SELECT tot_attempted FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          td_landed     = COALESCE((SELECT td_landed     FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          td_attempted  = COALESCE((SELECT td_attempted  FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          sub_att       = COALESCE((SELECT sub_att       FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          rev           = COALESCE((SELECT rev           FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          kd            = COALESCE((SELECT kd            FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          passes        = COALESCE((SELECT passes        FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0),
          ctrl_sec      = COALESCE((SELECT ctrl_sec      FROM wide w WHERE w.fight_url={FEATURES_TABLE}.fight_url AND w.fighter={FEATURES_TABLE}.fighter), 0)
        ;
        """
    )

    execmany(
        cur,
        [
            f"CREATE INDEX IF NOT EXISTS idx_{FEATURES_TABLE}_fighter ON {FEATURES_TABLE}(fighter);",
            f"CREATE INDEX IF NOT EXISTS idx_{FEATURES_TABLE}_fighturl ON {FEATURES_TABLE}(fight_url);",
        ],
    )

    rows = cur.execute(f"SELECT COUNT(*) FROM {FEATURES_TABLE};").fetchone()[0]
    fights = cur.execute(f"SELECT COUNT(DISTINCT fight_url) FROM {FEATURES_TABLE};").fetchone()[0]
    zeros = cur.execute(
        f"""
        SELECT COUNT(*) FROM {FEATURES_TABLE}
        WHERE sig_landed=0 AND tot_landed=0 AND td_landed=0 AND kd=0 AND ctrl_sec=0;
        """
    ).fetchone()[0]

    return {
        "rows": rows,
        "fights": fights,
        "zeros": zeros,
        "stat_keys": {
            "sig": K_SIG_LANDED,
            "sig_att": K_SIG_ATT,
            "tot": K_TOT_LANDED,
            "tot_att": K_TOT_ATT,
            "td": K_TD_LINE,
            "sub": K_SUB_ATT,
            "rev": K_REV,
            "kd": K_KD,
            "passes": K_PASSES,
            "ctrl": K_CTRL,
        },
    }


# -----------------------------
# Rollups (pre-fight priors)
# -----------------------------


def build_rollups(cur, fight_dim_mode: str):
    cur.execute(f"DROP TABLE IF EXISTS {ROLLUPS_TABLE};")

    cur.execute(
        f"""
        CREATE TABLE {ROLLUPS_TABLE} AS
        WITH base AS (
          SELECT
            f.*,
            d.event_date,
            d.fight_order
          FROM {FEATURES_TABLE} f
          LEFT JOIN {FIGHTDIM_VIEW} d
            ON d.fight_url = f.fight_url
        ),
        w AS (
          SELECT
            *,

            AVG(sig_landed)    OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sig_landed_l3,
            AVG(sig_attempted) OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sig_attempted_l3,
            AVG(td_landed)     OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS td_landed_l3,
            AVG(ctrl_sec)      OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ctrl_sec_l3,
            AVG(is_win)        OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS win_rate_l3,

            AVG(sig_landed)    OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sig_landed_l5,
            AVG(sig_attempted) OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sig_attempted_l5,
            AVG(td_landed)     OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS td_landed_l5,
            AVG(ctrl_sec)      OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS ctrl_sec_l5,
            AVG(is_win)        OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS win_rate_l5,

            AVG(sig_landed)    OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS sig_landed_l10,
            AVG(sig_attempted) OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS sig_attempted_l10,
            AVG(td_landed)     OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS td_landed_l10,
            AVG(ctrl_sec)      OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS ctrl_sec_l10,
            AVG(is_win)        OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS win_rate_l10,

            AVG(sig_landed)    OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS sig_landed_career,
            AVG(td_landed)     OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS td_landed_career,
            AVG(ctrl_sec)      OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS ctrl_sec_career,
            AVG(is_win)        OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS win_rate_career,

            COUNT(*)           OVER (PARTITION BY fighter ORDER BY fight_order ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS fights_before
          FROM base
        )
        SELECT
          fight_url, fighter, opponent, is_win, weight_class, method, finish_round, finish_time,
          event_date,
          fights_before,

          sig_landed_l3, sig_attempted_l3, td_landed_l3, ctrl_sec_l3, win_rate_l3,
          sig_landed_l5, sig_attempted_l5, td_landed_l5, ctrl_sec_l5, win_rate_l5,
          sig_landed_l10, sig_attempted_l10, td_landed_l10, ctrl_sec_l10, win_rate_l10,

          sig_landed_career, td_landed_career, ctrl_sec_career, win_rate_career
        FROM w
        ;
        """
    )

    execmany(
        cur,
        [
            f"CREATE INDEX IF NOT EXISTS idx_{ROLLUPS_TABLE}_fighter ON {ROLLUPS_TABLE}(fighter);",
            f"CREATE INDEX IF NOT EXISTS idx_{ROLLUPS_TABLE}_fighturl ON {ROLLUPS_TABLE}(fight_url);",
        ],
    )

    rows = cur.execute(f"SELECT COUNT(*) FROM {ROLLUPS_TABLE};").fetchone()[0]
    return {"rows": rows, "order_mode": fight_dim_mode}


# -----------------------------
# Optimizer-ready table
# -----------------------------


def build_ready(cur):
    """
    Build a clean slice that the optimizer can consume immediately:
      - requires method/round/time and is_win
      - filters garbage fighters
      - includes opponent + key numeric stats
      - includes fight ordering for time-series modeling
    """
    cur.execute(f"DROP TABLE IF EXISTS {READY_TABLE};")

    cur.execute(
        f"""
        CREATE TABLE {READY_TABLE} AS
        SELECT
          f.fight_url,
          f.fighter,
          f.opponent,
          f.is_win,
          f.weight_class,
          f.method,
          f.finish_round,
          f.finish_time,
          f.fight_seconds,

          d.event_date,
          d.fight_order,

          -- core numeric features
          f.sig_landed,
          f.sig_attempted,
          f.tot_landed,
          f.tot_attempted,
          f.td_landed,
          f.td_attempted,
          f.kd,
          f.sub_att,
          f.rev,
          f.passes,
          f.ctrl_sec
        FROM {FEATURES_TABLE} f
        LEFT JOIN {FIGHTDIM_VIEW} d
          ON d.fight_url = f.fight_url
        WHERE
          NOT {_fighter_is_garbage_sql("f")}
          AND f.is_win IS NOT NULL
          AND f.method IS NOT NULL AND trim(f.method) <> ''
          AND f.finish_round IS NOT NULL
          AND f.finish_time IS NOT NULL AND trim(f.finish_time) <> ''
        ;
        """
    )

    execmany(
        cur,
        [
            f"CREATE INDEX IF NOT EXISTS idx_{READY_TABLE}_fighter ON {READY_TABLE}(fighter);",
            f"CREATE INDEX IF NOT EXISTS idx_{READY_TABLE}_fighturl ON {READY_TABLE}(fight_url);",
            f"CREATE INDEX IF NOT EXISTS idx_{READY_TABLE}_fightorder ON {READY_TABLE}(fight_order);",
        ],
    )

    rows = cur.execute(f"SELECT COUNT(*) FROM {READY_TABLE};").fetchone()[0]
    fights = cur.execute(f"SELECT COUNT(DISTINCT fight_url) FROM {READY_TABLE};").fetchone()[0]
    return {"rows": rows, "fights": fights}


# -----------------------------
# main
# -----------------------------


def main():
    t0 = datetime.now()
    con = connect(DB)
    cur = con.cursor()

    fight_dim_info = build_fight_dim(cur)
    feat_info = build_features(cur)
    roll_info = build_rollups(cur, fight_dim_info["mode"])
    ready_info = build_ready(cur)

    con.commit()
    con.close()

    print("DB:", DB)
    print("FightDim mode:", fight_dim_info["mode"])
    print(
        "Features:",
        FEATURES_TABLE,
        "rows=",
        feat_info["rows"],
        "fights=",
        feat_info["fights"],
        "all_zero_rows=",
        feat_info["zeros"],
    )
    print("Used stat_keys:", feat_info["stat_keys"])
    print("Rollups:", ROLLUPS_TABLE, "rows=", roll_info["rows"])
    print("Ready:", READY_TABLE, "rows=", ready_info["rows"], "fights=", ready_info["fights"])
    print("Done in", datetime.now() - t0)


if __name__ == "__main__":
    main()
