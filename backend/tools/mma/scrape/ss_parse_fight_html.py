# ss_parse_fight_html.py
import argparse
import re
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup, Tag


# -------------------------
# Helpers
# -------------------------
def canon_stat_key(s: str) -> str:
    """
    Canonical stat key used for storage + downstream joins.
    - collapses whitespace
    - normalizes case (Title Case)
    """
    s = norm_space(s)
    if not s:
        return ""
    # keep % and words, normalize casing
    return s.lower().title()


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def text_of(el) -> str:
    return norm_space(el.get_text(" ", strip=True)) if el else ""


def page_text(soup: BeautifulSoup) -> str:
    """
    Use newlines to preserve label boundaries (Method:, Round:, etc.).
    This makes regex extraction much more reliable.
    """
    return norm_space(soup.get_text("\n", strip=True))


def looks_like_of(x: str) -> bool:
    # "32 of 90"
    return bool(re.match(r"^\s*\d+\s+of\s+\d+\s*$", x or ""))


def parse_of(x: str) -> Tuple[Optional[int], Optional[int]]:
    m = re.match(r"^\s*(\d+)\s+of\s+(\d+)\s*$", x or "")
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def parse_int(x: str) -> Optional[int]:
    x = norm_space(x)
    if not x or x in {"--", "—"}:
        return None
    x = x.replace(",", "")
    return int(x) if re.match(r"^-?\d+$", x) else None


def parse_percent(x: str) -> Optional[float]:
    x = norm_space(x).replace("%", "")
    if not x or x in {"--", "—"}:
        return None
    try:
        return float(x)
    except Exception:
        return None


def extract_round_num(s: str) -> Optional[int]:
    m = re.search(r"\bRound\s+(\d+)\b", s, re.I)
    return int(m.group(1)) if m else None


def clean_fighter_label(name: Optional[str]) -> Optional[str]:
    """
    Fix cases like:
      "USA Jesus Aguilar"
      "United Arab Emirates Umar Nurmagomedov"
    by stripping common country prefixes / 3-letter codes.
    """
    if not name:
        return name
    s = norm_space(name)

    # e.g. "USA Jesus Aguilar"
    m = re.match(r"^(?:[A-Z]{2,3})\s+(.+)$", s)
    if m:
        s = m.group(1).strip()

    # e.g. "United Arab Emirates Umar ..."
    for prefix in [
        "United Arab Emirates",
        "United Kingdom",
        "New Zealand",
        "South Korea",
        "North Korea",
        "Czech Republic",
        "South Africa",
    ]:
        if s.startswith(prefix + " "):
            s = s[len(prefix) + 1 :].strip()
            break

    return s or None


def extract_label(full: str, label: str) -> Optional[str]:
    """
    Extract value after a label, stopping before the next known label/section.
    Works with newline-separated or one-line pages.
    """
    if not full:
        return None

    stop = (
        r"(?:\n|$|"
        r"\b(?:Method|Round|Time|Time Format|Referee|Details):|"
        r"\bFight Totals\b|"
        r"\bFight Totals By Round\b|"
        r"\bSignificant Strikes\b|"
        r"\bSignificant Strikes By Round\b)"
    )

    m = re.search(
        rf"\b{re.escape(label)}\s*(.+?)(?={stop})",
        full,
        flags=re.I | re.S,
    )
    if not m:
        return None
    return norm_space(m.group(1))


def find_heading_nodes(soup: BeautifulSoup) -> List[Tag]:
    # headings can be h1/h2/h3/h4/div/strong; scan common text tags
    return soup.find_all(["h1", "h2", "h3", "h4", "h5", "div", "p", "strong", "span"])


def find_table_after(node: Optional[Tag]) -> Optional[Tag]:
    if node is None:
        return None
    next_table = node.find_next("table")
    return next_table if isinstance(next_table, Tag) else None


def table_rows(table: Optional[Tag]) -> List[List[str]]:
    rows: List[List[str]] = []
    if table is None:
        return rows

    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        row = [cell.get_text(" ", strip=True) for cell in cells]
        rows.append(row)
    return rows


def detect_two_fighter_header(rows: List[List[str]]) -> Optional[Tuple[str, str]]:
    for r in rows[:6]:
        vals = [v for v in r if v]
        if len(vals) == 2 and len(vals[0]) >= 3 and len(vals[1]) >= 3:
            if " " in vals[0] and " " in vals[1]:
                return vals[0], vals[1]
    return None


def _is_value_cell(x: str) -> bool:
    x = norm_space(x)
    if not x:
        return False
    if looks_like_of(x):
        return True
    if x in {"--", "—"}:
        return True
    # numbers or percentages or times
    return bool(re.match(r"^-?\d+(\.\d+)?%?$", x))


def _is_stat_cell(x: str) -> bool:
    x = norm_space(x)
    if not x:
        return False
    # stat labels are mostly letters/spaces/% (not pure numbers)
    return bool(re.search(r"[A-Za-z]", x)) and not _is_value_cell(x)


def parse_two_side_stat_table(
    table: Tag,
) -> Tuple[Optional[str], Optional[str], Dict[str, Tuple[str, str]]]:
    """
    Generic parser for tables that look like:
      FighterA   FighterB
      <valA>  <StatName>  <valB>

    Returns (fighter_a, fighter_b, stats dict: stat_name -> (a_val, b_val))
    """
    rows = table_rows(table)
    if not rows:
        return None, None, {}

    fighter_a = None
    fighter_b = None

    hdr = detect_two_fighter_header(rows)
    if hdr:
        fighter_a, fighter_b = hdr

    stats: Dict[str, Tuple[str, str]] = {}

    for r in rows:
        # Some SS tables include spacer columns; normalize row by trimming,
        # but keep empties so we can still detect 3+ shape.
        r2 = [norm_space(x) for x in r]

        # Most stat rows are effectively [A, StatName, B] — sometimes with extra blanks.
        # Collapse purely-empty cells in the middle without losing A/B.
        nonempty = [x for x in r2 if x]
        if len(nonempty) < 3:
            continue

        # Prefer the classic shape if present
        if len(r2) >= 3 and r2[0] and r2[1] and r2[2]:
            a, mid, b = r2[0], r2[1], r2[2]
        else:
            # Fallback: assume [A, StatName, B] from non-empty cells
            a, mid, b = nonempty[0], nonempty[1], nonempty[2]

        if not mid:
            continue

        mid_l = mid.strip().lower()

        # Skip only obvious section headers, NOT real stat rows.
        # IMPORTANT: do NOT skip "significant strikes" because it's a real stat row in sig breakdown.
        if mid_l in {"fight totals"}:
            continue

        if a and mid and b:
            stats[mid] = (a, b)

    return fighter_a, fighter_b, stats


def extract_meta_from_text(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    """
    Robust label extraction for:
      Method, Round, Time, Referee, Details, Weight class
    """
    meta: Dict[str, Optional[str]] = {
        "method": None,
        "round": None,
        "time": None,
        "referee": None,
        "weight_class": None,
        "details": None,
    }

    full = page_text(soup)

    # weight class often: "Flyweight Bout" / "Women's Strawweight Bout"
    m = re.search(r"\b([A-Za-z’' ]+weight)\s+Bout\b", full, re.I)
    if m:
        meta["weight_class"] = norm_space(m.group(1))

    meta["method"] = extract_label(full, "Method:")
    meta["round"] = extract_label(full, "Round:")
    meta["time"] = extract_label(full, "Time:")
    meta["referee"] = extract_label(full, "Referee:")
    details = extract_label(full, "Details:")
    if details:
        meta["details"] = details[:200]

    # normalize round to digits
    if meta["round"]:
        m2 = re.search(r"\d+", meta["round"])
        meta["round"] = m2.group(0) if m2 else meta["round"]

    # normalize time to M:SS
    if meta["time"]:
        m3 = re.search(r"\b(\d{1,2}:\d{2})\b", meta["time"])
        meta["time"] = m3.group(1) if m3 else meta["time"]

    return meta


def extract_fighters_and_winner(
    soup: BeautifulSoup,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Best-effort fighter names + winner.
    Prefer "(Win)/(Loss)" patterns, fallback to "A v B Fight Statistics".
    """
    full = page_text(soup)

    win = re.search(r"\b(.+?)\s+\(Win\)\b", full, re.I)
    loss = re.search(r"\b(.+?)\s+\(Loss\)\b", full, re.I)
    if win and loss:
        a = clean_fighter_label(win.group(1))
        b = clean_fighter_label(loss.group(1))
        return a, b, a

    m = re.search(r"\b(.+?)\s+v(?:s)?\.?\s+(.+?)\s+Fight Statistics\b", full, re.I)
    if m:
        a = clean_fighter_label(m.group(1))
        b = clean_fighter_label(m.group(2))
        return a, b, None

    return None, None, None


# -------------------------
# DB schema (canonical)
# -------------------------


def ensure_canon_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS canon_fights (
            fight_url TEXT PRIMARY KEY,
            fighter_a TEXT,
            fighter_b TEXT,
            winner TEXT,
            weight_class TEXT,
            method TEXT,
            round INTEGER,
            time TEXT,
            referee TEXT,
            details TEXT,
            parsed_at TEXT DEFAULT (datetime('now'))
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS canon_fight_totals (
            fight_url TEXT,
            fighter TEXT,
            stat_key TEXT,
            a_landed INTEGER,
            a_attempted INTEGER,
            a_value TEXT,
            PRIMARY KEY (fight_url, fighter, stat_key)
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS canon_fight_round_totals (
            fight_url TEXT,
            round INTEGER,
            fighter TEXT,
            stat_key TEXT,
            a_landed INTEGER,
            a_attempted INTEGER,
            a_value TEXT,
            PRIMARY KEY (fight_url, round, fighter, stat_key)
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS canon_strike_breakdown (
            fight_url TEXT,
            fighter TEXT,
            stat_key TEXT,
            landed INTEGER,
            attempted INTEGER,
            value TEXT,
            scope TEXT DEFAULT 'overall',  -- overall | round
            round INTEGER,
            PRIMARY KEY (fight_url, fighter, scope, round, stat_key)
        );
        """
    )
    con.commit()


# -------------------------
# Main parse routine
# -------------------------


@dataclass
class FightParseResult:
    fight_url: str
    ok: bool
    err: Optional[str] = None


def upsert_canon_fight(
    con: sqlite3.Connection,
    fight_url: str,
    meta: Dict[str, Optional[str]],
    fighter_a: Optional[str],
    fighter_b: Optional[str],
    winner: Optional[str],
) -> None:
    con.execute(
        """
        INSERT INTO canon_fights (
            fight_url, fighter_a, fighter_b, winner, weight_class, method, round, time, referee, details
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fight_url) DO UPDATE SET
            fighter_a=excluded.fighter_a,
            fighter_b=excluded.fighter_b,
            winner=excluded.winner,
            weight_class=excluded.weight_class,
            method=excluded.method,
            round=excluded.round,
            time=excluded.time,
            referee=excluded.referee,
            details=excluded.details,
            parsed_at=datetime('now')
        """,
        (
            fight_url,
            fighter_a,
            fighter_b,
            winner,
            meta.get("weight_class"),
            meta.get("method"),
            int(meta["round"]) if meta.get("round") and str(meta["round"]).isdigit() else None,
            meta.get("time"),
            meta.get("referee"),
            meta.get("details"),
        ),
    )


def write_totals(
    con: sqlite3.Connection,
    fight_url: str,
    fighter_a: str,
    fighter_b: str,
    stats: Dict[str, Tuple[str, str]],
) -> None:
    for stat_name, (a_raw, b_raw) in stats.items():
        stat_key = canon_stat_key(stat_name)

        # A-side
        if looks_like_of(a_raw):
            l, att = parse_of(a_raw)
            con.execute(
                """
                INSERT OR REPLACE INTO canon_fight_totals
                (fight_url, fighter, stat_key, a_landed, a_attempted, a_value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (fight_url, fighter_a, stat_key, l, att, a_raw),
            )
        else:
            con.execute(
                """
                INSERT OR REPLACE INTO canon_fight_totals
                (fight_url, fighter, stat_key, a_landed, a_attempted, a_value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (fight_url, fighter_a, stat_key, None, None, a_raw),
            )

        # B-side
        if looks_like_of(b_raw):
            l, att = parse_of(b_raw)
            con.execute(
                """
                INSERT OR REPLACE INTO canon_fight_totals
                (fight_url, fighter, stat_key, a_landed, a_attempted, a_value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (fight_url, fighter_b, stat_key, l, att, b_raw),
            )
        else:
            con.execute(
                """
                INSERT OR REPLACE INTO canon_fight_totals
                (fight_url, fighter, stat_key, a_landed, a_attempted, a_value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (fight_url, fighter_b, stat_key, None, None, b_raw),
            )


def write_round_totals(
    con: sqlite3.Connection,
    fight_url: str,
    rnd: int,
    fighter_a: str,
    fighter_b: str,
    stats: Dict[str, Tuple[str, str]],
) -> None:
    for stat_name, (a_raw, b_raw) in stats.items():
        stat_key = canon_stat_key(stat_name)

        # A-side
        if looks_like_of(a_raw):
            l, att = parse_of(a_raw)
            con.execute(
                """
                INSERT OR REPLACE INTO canon_fight_round_totals
                (fight_url, round, fighter, stat_key, a_landed, a_attempted, a_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (fight_url, rnd, fighter_a, stat_key, l, att, a_raw),
            )
        else:
            con.execute(
                """
                INSERT OR REPLACE INTO canon_fight_round_totals
                (fight_url, round, fighter, stat_key, a_landed, a_attempted, a_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (fight_url, rnd, fighter_a, stat_key, None, None, a_raw),
            )

        # B-side
        if looks_like_of(b_raw):
            l, att = parse_of(b_raw)
            con.execute(
                """
                INSERT OR REPLACE INTO canon_fight_round_totals
                (fight_url, round, fighter, stat_key, a_landed, a_attempted, a_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (fight_url, rnd, fighter_b, stat_key, l, att, b_raw),
            )
        else:
            con.execute(
                """
                INSERT OR REPLACE INTO canon_fight_round_totals
                (fight_url, round, fighter, stat_key, a_landed, a_attempted, a_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (fight_url, rnd, fighter_b, stat_key, None, None, b_raw),
            )


def parse_fight_html(
    fight_url: str,
    raw_html: str,
) -> Tuple[
    Dict[str, Optional[str]],
    Optional[str],
    Optional[str],
    Optional[str],
    Dict[str, Tuple[str, str]],
    Dict[int, Dict[str, Tuple[str, str]]],
    Dict[str, Tuple[str, str]],
]:
    """
    Returns:
      meta, fighter_a, fighter_b, winner,
      totals_stats,
      round_totals_stats_by_round,
      sig_breakdown_overall_stats
    """
    soup = BeautifulSoup(raw_html, "lxml")

    meta = extract_meta_from_text(soup)
    fighter_a, fighter_b, winner = extract_fighters_and_winner(soup)

    totals_stats: Dict[str, Tuple[str, str]] = {}
    round_totals: Dict[int, Dict[str, Tuple[str, str]]] = {}

    headings = find_heading_nodes(soup)

    # 1) Fight Totals table
    fight_totals_table = None
    for h in headings:
        if text_of(h).strip().lower() == "fight totals":
            fight_totals_table = find_table_after(h)
            break

    if fight_totals_table:
        fa2, fb2, stats = parse_two_side_stat_table(fight_totals_table)
        if not fighter_a and fa2:
            fighter_a = clean_fighter_label(fa2)
        if not fighter_b and fb2:
            fighter_b = clean_fighter_label(fb2)
        totals_stats = stats

    # 2) Fight Totals By Round
    start_node = None
    for h in headings:
        if text_of(h).strip().lower() == "fight totals by round":
            start_node = h
            break

    if start_node:
        node = start_node
        for _ in range(1, 7):
            rn_node = node.find_next(string=re.compile(r"\bRound\s+\d+\b", re.I))
            if not rn_node:
                break
            rn_text = norm_space(str(rn_node))
            rnd = extract_round_num(rn_text)
            if not rnd:
                node = rn_node.parent if hasattr(rn_node, "parent") else node
                continue

            tbl = rn_node.parent.find_next("table") if hasattr(rn_node, "parent") else None
            if tbl:
                fa2, fb2, stats = parse_two_side_stat_table(tbl)
                if not fighter_a and fa2:
                    fighter_a = clean_fighter_label(fa2)
                if not fighter_b and fb2:
                    fighter_b = clean_fighter_label(fb2)
                if fighter_a and fighter_b and stats:
                    round_totals[rnd] = stats

            node = tbl or (rn_node.parent if hasattr(rn_node, "parent") else start_node)

    # 3) Significant Strikes (overall)
    sig_stats: Dict[str, Tuple[str, str]] = {}
    sig_table = None
    for h in headings:
        if text_of(h).strip().lower() == "significant strikes":
            sig_table = find_table_after(h)
            break
    if sig_table:
        _, _, sig_stats = parse_two_side_stat_table(sig_table)

    return meta, fighter_a, fighter_b, winner, totals_stats, round_totals, sig_stats


def get_html_rows(con: sqlite3.Connection, limit: int, offset: int) -> List[Tuple[str, str]]:
    cols = [r[1] for r in con.execute("PRAGMA table_info(ss_fight_html)").fetchall()]
    if "fight_url" not in cols:
        raise RuntimeError("ss_fight_html is missing fight_url column")
    if "raw_html" not in cols:
        raise RuntimeError("ss_fight_html is missing raw_html column")

    rows = con.execute(
        """
        SELECT fight_url, raw_html
        FROM ss_fight_html
        ORDER BY rowid ASC
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    ).fetchall()

    return [(r[0], r[1]) for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to sqlite db")
    ap.add_argument("--limit", type=int, default=200, help="How many html rows to parse per run")
    ap.add_argument("--offset", type=int, default=0, help="Offset for batching")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    ensure_canon_schema(con)

    rows = get_html_rows(con, limit=args.limit, offset=args.offset)
    if not rows:
        print("[parse] no ss_fight_html rows found")
        return

    ok = 0
    bad = 0

    for fight_url, raw_html in rows:
        try:
            meta, fa, fb, winner, totals, round_totals, sig = parse_fight_html(fight_url, raw_html)

            # clean names once more at the end (safe)
            fa = clean_fighter_label(fa)
            fb = clean_fighter_label(fb)
            winner = clean_fighter_label(winner)

            upsert_canon_fight(con, fight_url, meta, fa, fb, winner)

            if fa and fb and totals:
                write_totals(con, fight_url, fa, fb, totals)

            if fa and fb and round_totals:
                for rnd, stats in round_totals.items():
                    if stats:
                        write_round_totals(con, fight_url, rnd, fa, fb, stats)

            if fa and fb and sig:
                for stat_name, (a_raw, b_raw) in sig.items():
                    stat_key = canon_stat_key(stat_name)

                    # A
                    if looks_like_of(a_raw):
                        l, att = parse_of(a_raw)
                        con.execute(
                            """
                            INSERT OR REPLACE INTO canon_strike_breakdown
                            (fight_url, fighter, stat_key, landed, attempted, value, scope, round)
                            VALUES (?, ?, ?, ?, ?, ?, 'overall', NULL)
                            """,
                            (fight_url, fa, stat_key, l, att, a_raw),
                        )
                    else:
                        con.execute(
                            """
                            INSERT OR REPLACE INTO canon_strike_breakdown
                            (fight_url, fighter, stat_key, landed, attempted, value, scope, round)
                            VALUES (?, ?, ?, NULL, NULL, ?, 'overall', NULL)
                            """,
                            (fight_url, fa, stat_key, a_raw),
                        )

                    # B
                    if looks_like_of(b_raw):
                        l, att = parse_of(b_raw)
                        con.execute(
                            """
                            INSERT OR REPLACE INTO canon_strike_breakdown
                            (fight_url, fighter, stat_key, landed, attempted, value, scope, round)
                            VALUES (?, ?, ?, ?, ?, ?, 'overall', NULL)
                            """,
                            (fight_url, fb, stat_key, l, att, b_raw),
                        )
                    else:
                        con.execute(
                            """
                            INSERT OR REPLACE INTO canon_strike_breakdown
                            (fight_url, fighter, stat_key, landed, attempted, value, scope, round)
                            VALUES (?, ?, ?, NULL, NULL, ?, 'overall', NULL)
                            """,
                            (fight_url, fb, stat_key, b_raw),
                        )

            ok += 1

        except Exception as e:
            bad += 1
            print(f"[parse] ERROR fight_url={fight_url} err={type(e).__name__}: {e}")

        if (ok + bad) % 50 == 0:
            con.commit()
            print(f"[parse] progress ok={ok} bad={bad}")

    con.commit()
    con.close()
    print(f"[parse] done ok={ok} bad={bad} db={args.db}")


if __name__ == "__main__":
    main()
