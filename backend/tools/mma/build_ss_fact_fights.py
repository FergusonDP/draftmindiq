# backend/tools/mma/build_ss_fact_fights.py

import sqlite3

DB = r"data\marts\mma_historical_ss_full.sqlite"

TOTALS_MAP = {
    "Knockdowns": "kd",
    "Significant Strikes": "sig",  # landed/attempted
    "Total Strikes": "tot",  # landed/attempted
    "Takedowns": "td",  # landed/attempted
    "Submissions Attempted": "sub_att",
    "Reversals": "rev",
    "Passes": "pass",
    "Control": "ctrl",  # mm:ss -> seconds
}

SIG_MAP = {
    "Head": "head",
    "Body": "body",
    "Leg": "leg",
    "Distance": "dist",
    "Clinch": "clinch",
    "Ground": "ground",
}


def parse_mmss_to_sec(x: str):
    if not x:
        return None
    x = x.strip()
    if x in {"--", "—"}:
        return None
    if ":" not in x:
        return None
    try:
        mm, ss = x.split(":", 1)
        return int(mm) * 60 + int(ss)
    except Exception:
        return None


def main():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS ss_fact_fighter_fights")
    cur.execute(
        """
    CREATE TABLE ss_fact_fighter_fights (
        fight_url TEXT,
        fighter TEXT,
        opponent TEXT,
        is_win INTEGER,
        weight_class TEXT,
        method TEXT,
        round INTEGER,
        time TEXT,

        kd INTEGER,
        sig_l INTEGER, sig_a INTEGER,
        tot_l INTEGER, tot_a INTEGER,
        td_l INTEGER, td_a INTEGER,
        sub_att INTEGER,
        rev INTEGER,
        pass INTEGER,
        ctrl_sec INTEGER,

        head_l INTEGER, head_a INTEGER,
        body_l INTEGER, body_a INTEGER,
        leg_l INTEGER,  leg_a INTEGER,
        dist_l INTEGER, dist_a INTEGER,
        clinch_l INTEGER, clinch_a INTEGER,
        ground_l INTEGER, ground_a INTEGER,

        PRIMARY KEY (fight_url, fighter)
    )
    """
    )

    fights = cur.execute(
        """
        SELECT fight_url, fighter_a, fighter_b, winner, weight_class, method, round, time
        FROM canon_fights
    """
    ).fetchall()

    # skeleton rows (2 per fight)
    inserted = 0
    for r in fights:
        fa = r["fighter_a"]
        fb = r["fighter_b"]
        if not fa or not fb:
            continue

        winner = (r["winner"] or "").strip()
        is_win_a = 1 if winner and winner == fa else (0 if winner and winner == fb else None)
        is_win_b = 1 if winner and winner == fb else (0 if winner and winner == fa else None)

        base = (r["fight_url"], r["weight_class"], r["method"], r["round"], r["time"])

        cur.execute(
            """
            INSERT OR REPLACE INTO ss_fact_fighter_fights
            (fight_url, fighter, opponent, is_win, weight_class, method, round, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (r["fight_url"], fa, fb, is_win_a, *base[1:]),
        )

        cur.execute(
            """
            INSERT OR REPLACE INTO ss_fact_fighter_fights
            (fight_url, fighter, opponent, is_win, weight_class, method, round, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (r["fight_url"], fb, fa, is_win_b, *base[1:]),
        )

        inserted += 2

    con.commit()
    print("base rows inserted:", inserted)

    # totals
    totals = cur.execute(
        """
        SELECT fight_url, fighter, stat_key, a_landed, a_attempted, a_value
        FROM canon_fight_totals
    """
    ).fetchall()

    for t in totals:
        fight_url = t["fight_url"]
        fighter = t["fighter"]
        key = (t["stat_key"] or "").strip()

        if key not in TOTALS_MAP:
            continue

        col = TOTALS_MAP[key]

        if col == "ctrl":
            sec = parse_mmss_to_sec(t["a_value"] or "")
            cur.execute(
                """
                UPDATE ss_fact_fighter_fights
                SET ctrl_sec = COALESCE(?, ctrl_sec)
                WHERE fight_url=? AND fighter=?
            """,
                (sec, fight_url, fighter),
            )
            continue

        if col in {"sig", "tot", "td"}:
            l = t["a_landed"]
            a = t["a_attempted"]
            if col == "sig":
                cur.execute(
                    """
                    UPDATE ss_fact_fighter_fights
                    SET sig_l=?, sig_a=?
                    WHERE fight_url=? AND fighter=?
                """,
                    (l, a, fight_url, fighter),
                )
            elif col == "tot":
                cur.execute(
                    """
                    UPDATE ss_fact_fighter_fights
                    SET tot_l=?, tot_a=?
                    WHERE fight_url=? AND fighter=?
                """,
                    (l, a, fight_url, fighter),
                )
            else:  # td
                cur.execute(
                    """
                    UPDATE ss_fact_fighter_fights
                    SET td_l=?, td_a=?
                    WHERE fight_url=? AND fighter=?
                """,
                    (l, a, fight_url, fighter),
                )
        else:
            v = None
            try:
                v = int((t["a_value"] or "").strip())
            except Exception:
                v = t["a_landed"]

            cur.execute(
                f"""
                UPDATE ss_fact_fighter_fights
                SET {col} = COALESCE(?, {col})
                WHERE fight_url=? AND fighter=?
            """,
                (v, fight_url, fighter),
            )

    con.commit()

    # sig breakdown
    sigs = cur.execute(
        """
        SELECT fight_url, fighter, stat_key, landed, attempted
        FROM canon_strike_breakdown
        WHERE scope='overall'
    """
    ).fetchall()

    for s in sigs:
        key = (s["stat_key"] or "").strip()
        if key not in SIG_MAP:
            continue

        base = SIG_MAP[key]
        cur.execute(
            f"""
            UPDATE ss_fact_fighter_fights
            SET {base}_l=?, {base}_a=?
            WHERE fight_url=? AND fighter=?
        """,
            (s["landed"], s["attempted"], s["fight_url"], s["fighter"]),
        )

    con.commit()

    n = cur.execute("SELECT COUNT(*) FROM ss_fact_fighter_fights").fetchone()[0]
    print("ss_fact_fighter_fights rows:", n)

    sample = cur.execute(
        """
        SELECT fighter, opponent, is_win, weight_class, method, round, time,
               sig_l, sig_a, td_l, td_a, ctrl_sec
        FROM ss_fact_fighter_fights
        WHERE sig_a IS NOT NULL
        ORDER BY random()
        LIMIT 8
    """
    ).fetchall()

    for r in sample:
        print(dict(r))

    con.close()


if __name__ == "__main__":
    main()
