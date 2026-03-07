# tools/mma/ufcstats_scrape.py
import argparse
import os
import random
import re
import sqlite3
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Tag

UFC_BASE = "http://ufcstats.com"
EVENTS_ALL = "http://ufcstats.com/statistics/events/completed?page=all"

DB_DEFAULT = os.environ.get("MMA_HIST_DB_PATH", r"data\marts\mma_historical_ss_full.sqlite")
DEBUG = os.environ.get("DEBUG", "0").strip() == "1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": UFC_BASE,
}

RETRY_STATUSES = {403, 429, 500, 502, 503, 504}


# -------------------------
# Small utils
# -------------------------
def _sleep_polite(a: float = 0.25, b: float = 0.85) -> None:
    time.sleep(random.uniform(a, b))


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _text(el) -> str:
    return norm_space(el.get_text(" ", strip=True)) if el else ""


def safe_int(s: str) -> Optional[int]:
    s = norm_space(s)
    if not s or s in {"--", "—"}:
        return None
    s = s.replace(",", "")
    return int(s) if re.match(r"^-?\d+$", s) else None


def mmss_to_seconds(s: str) -> Optional[int]:
    s = norm_space(s)
    if not s or s in {"--", "—"}:
        return None
    m = re.match(r"^(\d+):(\d{2})$", s)
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))


def normalize_ctrl(raw: str) -> Optional[str]:
    raw = norm_space(raw)
    if not raw:
        return None
    # UFCStats oddities like: "0:02 0:00" or "-- --"
    m = re.search(r"\b(\d+:\d{2}|--)\b", raw)
    if not m:
        return None
    val = m.group(1)
    return val


def fetch_html(client: httpx.Client, url: str) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, 10):
        try:
            r = client.get(url)
            if r.status_code in RETRY_STATUSES:
                raise httpx.HTTPStatusError(
                    f"retryable status={r.status_code}", request=r.request, response=r
                )
            r.raise_for_status()
            if DEBUG:
                print(f"[fetch] {url} status={r.status_code} chars={len(r.text)}")
            return r.text
        except (httpx.TransportError, httpx.HTTPStatusError) as e:
            last_err = e
            backoff = min(25.0, (1.8**attempt)) + random.uniform(0.0, 0.9)
            if DEBUG:
                print(
                    f"[fetch] retry={attempt} sleep={backoff:.2f}s url={url} err={type(e).__name__}"
                )
            time.sleep(backoff)
    raise last_err  # type: ignore


# -------------------------
# DB schema
# -------------------------
def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ufc_events (
        event_url TEXT PRIMARY KEY,
        discovered_at TEXT DEFAULT (datetime('now')),
        last_seen_at TEXT DEFAULT (datetime('now')),
        status TEXT DEFAULT 'queued', -- queued|done|error
        attempts INTEGER DEFAULT 0,
        last_error TEXT
    );
    """
    )
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ufc_fights (
        fight_url TEXT PRIMARY KEY,
        event_url TEXT,
        discovered_at TEXT DEFAULT (datetime('now')),
        last_seen_at TEXT DEFAULT (datetime('now')),
        status TEXT DEFAULT 'queued', -- queued|done|error
        attempts INTEGER DEFAULT 0,
        last_error TEXT
    );
    """
    )
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ufc_fight_html (
        fight_url TEXT PRIMARY KEY,
        scraped_at TEXT DEFAULT (datetime('now')),
        html_len INTEGER,
        raw_html TEXT
    );
    """
    )

    # Parsed truth layer
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ufc_fight_meta (
        fight_url TEXT PRIMARY KEY,
        event_name TEXT,
        event_date TEXT,           -- YYYY-MM-DD
        weight_class TEXT,
        method TEXT,
        round INTEGER,
        time TEXT,                 -- mm:ss
        referee TEXT,
        details TEXT,
        fighter_red TEXT,
        fighter_blue TEXT,
        winner TEXT,
        parsed_at TEXT DEFAULT (datetime('now'))
    );
    """
    )

    # Totals
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ufc_fight_totals (
        fight_url TEXT,
        corner TEXT,               -- 'red'|'blue'
        fighter TEXT,
        kd INTEGER,
        sig_str TEXT,              -- "X of Y"
        sig_landed INTEGER,
        sig_att INTEGER,
        sig_pct REAL,
        total_str TEXT,            -- "X of Y"
        total_landed INTEGER,
        total_att INTEGER,
        td TEXT,                   -- "X of Y"
        td_landed INTEGER,
        td_att INTEGER,
        td_pct REAL,
        sub_att INTEGER,
        rev INTEGER,
        ctrl TEXT,                 -- "mm:ss"
        ctrl_sec INTEGER,
        PRIMARY KEY (fight_url, corner)
    );
    """
    )

    # Per round totals
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ufc_fight_rounds (
        fight_url TEXT,
        round INTEGER,
        corner TEXT,               -- 'red'|'blue'
        fighter TEXT,
        kd INTEGER,
        sig_str TEXT,
        sig_landed INTEGER,
        sig_att INTEGER,
        sig_pct REAL,
        total_str TEXT,
        total_landed INTEGER,
        total_att INTEGER,
        td TEXT,
        td_landed INTEGER,
        td_att INTEGER,
        td_pct REAL,
        sub_att INTEGER,
        rev INTEGER,
        ctrl TEXT,
        ctrl_sec INTEGER,
        PRIMARY KEY (fight_url, round, corner)
    );
    """
    )
    con.commit()


def upsert_event(con: sqlite3.Connection, event_url: str) -> None:
    con.execute(
        """
    INSERT INTO ufc_events(event_url, status)
    VALUES (?, 'queued')
    ON CONFLICT(event_url) DO UPDATE SET last_seen_at=datetime('now')
    """,
        (event_url,),
    )


def upsert_fight(con: sqlite3.Connection, fight_url: str, event_url: str) -> None:
    con.execute(
        """
    INSERT INTO ufc_fights(fight_url, event_url, status)
    VALUES (?, ?, 'queued')
    ON CONFLICT(fight_url) DO UPDATE SET
        event_url=COALESCE(excluded.event_url, ufc_fights.event_url),
        last_seen_at=datetime('now')
    """,
        (fight_url, event_url),
    )


def mark_status(
    con: sqlite3.Connection, table: str, url_col: str, url: str, status: str, err: Optional[str]
) -> None:
    con.execute(
        f"""
    UPDATE {table}
    SET status=?,
        attempts=attempts+1,
        last_error=?,
        last_seen_at=datetime('now')
    WHERE {url_col}=?;
    """,
        (status, err, url),
    )
    con.commit()


def next_queued(con: sqlite3.Connection, table: str, url_col: str) -> Optional[str]:
    row = con.execute(
        f"""
        SELECT {url_col}
        FROM {table}
        WHERE status='queued'
        ORDER BY discovered_at ASC
        LIMIT 1
    """
    ).fetchone()
    return row[0] if row else None


# -------------------------
# Discovery: events + fights
# -------------------------
def discover_events(con: sqlite3.Connection, client: httpx.Client) -> int:
    html = fetch_html(client, EVENTS_ALL)
    soup = BeautifulSoup(html, "lxml")
    found = 0

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "/event-details/" in href:
            url = href if href.startswith("http") else urljoin(UFC_BASE, href)
            upsert_event(con, url)
            found += 1

    con.commit()
    return found


def discover_fights_for_event(con: sqlite3.Connection, client: httpx.Client, event_url: str) -> int:
    html = fetch_html(client, event_url)
    soup = BeautifulSoup(html, "lxml")

    n = 0
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "/fight-details/" in href:
            url = href if href.startswith("http") else urljoin(UFC_BASE, href)
            upsert_fight(con, url, event_url)
            n += 1

    con.commit()
    return n


def ingest_fight_html(con: sqlite3.Connection, client: httpx.Client, fight_url: str) -> str:
    html = fetch_html(client, fight_url)
    con.execute(
        """
        INSERT INTO ufc_fight_html(fight_url, html_len, raw_html)
        VALUES (?, ?, ?)
        ON CONFLICT(fight_url) DO UPDATE SET
          scraped_at=datetime('now'),
          html_len=excluded.html_len,
          raw_html=excluded.raw_html
    """,
        (fight_url, len(html), html),
    )
    con.commit()
    return html


# -------------------------
# Parsing helpers (PATCHED v2)
# -------------------------
def _norm_hdr(raw: str) -> str:
    h = norm_space(raw).upper()
    h2 = re.sub(r"[^A-Z0-9%]+", "", h)

    # drop round label header cells
    if re.fullmatch(r"ROUND\d+", h2):
        return ""

    # canonicalize
    if h2 == "FIGHTER":
        return "FIGHTER"
    if h2 == "KD":
        return "KD"
    if h2 == "REV":
        return "REV"
    if h2 == "CTRL":
        return "CTRL"
    if h2 == "TD":
        return "TD"
    if h2 in {"TD%", "TDPCT", "TDPERCENT"}:
        return "TDPCT"
    if h2 in {"SUBATT", "SUB.ATT", "SUBATT."}:
        return "SUBATT"
    if h2 in {"SIGSTR", "SIG.STR", "SIGSTR."}:
        return "SIGSTR"
    if h2 in {"SIGSTR%", "SIGSTRPCT", "SIGSTRPERCENT"}:
        return "SIGSTRPCT"
    if h2 in {"TOTALSTR", "TOTAL.STR", "TOTALSTR."}:
        return "TOTALSTR"

    return h2


def _round_label_from_table(table: Tag) -> Optional[int]:
    # Look for "Round 1" header cell in the table header
    for th in table.select("thead th"):
        txt = norm_space(th.get_text(" ", strip=True)).lower()
        m = re.search(r"\bround\s*(\d+)\b", txt)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    # Sometimes it appears in any th
    for th in table.find_all("th"):
        txt = norm_space(th.get_text(" ", strip=True)).lower()
        m = re.search(r"\bround\s*(\d+)\b", txt)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None


def _table_headers_raw(table: Tag) -> List[str]:
    thead = table.select("thead th")
    if thead:
        return [norm_space(th.get_text(" ", strip=True)) for th in thead]

    for tr in table.select("tr"):
        ths = tr.find_all("th")
        if ths:
            return [norm_space(th.get_text(" ", strip=True)) for th in ths]

    first = table.select_one("tr")
    if first:
        cells = first.find_all(["th", "td"])
        return [norm_space(c.get_text(" ", strip=True)) for c in cells]
    return []


def _table_headers(table: Tag) -> List[str]:
    raw = _table_headers_raw(table)
    out = []
    for h in raw:
        hn = _norm_hdr(h)
        if hn:
            out.append(hn)
    return out


def _data_rows(table: Tag) -> List[Tag]:
    trs = table.select("tbody tr")
    if not trs:
        trs = table.select("tr")

    out = []
    for tr in trs:
        if tr.find_all("td"):
            out.append(tr)
    return out


def _row_cells_text(tr: Tag) -> List[str]:
    return [norm_space(td.get_text(" ", strip=True)) for td in tr.find_all("td")]


def _split_dual_cell(text: str) -> Optional[Tuple[str, str]]:
    """
    Split a single cell that contains values for BOTH fighters, e.g.:
      "13 of 19 4 of 7" -> ("13 of 19","4 of 7")
      "68% 57%" -> ("68%","57%")
      "1 0" -> ("1","0")
      "0:02 0:00" -> ("0:02","0:00")
      "-- --" -> ("--","--")
    """
    s = norm_space(text)
    if not s:
        return None

    # X of Y
    pairs = re.findall(r"\d+\s+of\s+\d+", s, flags=re.I)
    if len(pairs) == 2:
        return pairs[0], pairs[1]

    # percents
    pcts = re.findall(r"-?\d+(?:\.\d+)?\s*%", s)
    if len(pcts) == 2:
        return norm_space(pcts[0]), norm_space(pcts[1])

    # mm:ss or --
    times = re.findall(r"(?:\d+:\d{2}|--)", s)
    if len(times) == 2:
        return times[0], times[1]

    # plain ints (must be exactly 2)
    ints = re.findall(r"-?\d+", s)
    if len(ints) == 2 and s.strip().replace(" ", "") == f"{ints[0]}{ints[1]}":
        # edge case: "10 0" ok
        return ints[0], ints[1]
    if len(ints) == 2 and s.strip().count(" ") == 1:
        return ints[0], ints[1]

    # "-- --"
    if s == "-- --":
        return "--", "--"

    return None


def _parse_single_row_dual(
    table: Tag,
) -> Optional[Tuple[Optional[int], Dict[str, str], Dict[str, str]]]:
    """
    UFCStats variant: only ONE tbody row; each td contains both fighters' values.
    Returns (round_num, red_dict, blue_dict) with CANONICAL headers.
    """
    rows = _data_rows(table)
    if len(rows) != 1:
        return None

    headers_raw = _table_headers_raw(table)
    round_num = _round_label_from_table(table)

    # Normalize headers; DROP any "Round X" header (it has no td)
    headers = []
    for h in headers_raw:
        if re.search(r"\bround\s*\d+\b", (h or "").lower()):
            continue
        hn = _norm_hdr(h)
        if hn:
            headers.append(hn)

    cells = _row_cells_text(rows[0])
    if not headers or not cells:
        return None

    # Align counts: headers include "FIGHTER" and stats; cells include fighter + stats
    # Some pages have headers len == cells len, but if off, trim from end.
    if len(headers) > len(cells):
        headers = headers[: len(cells)]
    elif len(cells) > len(headers):
        cells = cells[: len(headers)]

    if len(headers) < 2:
        return None
    if headers[0] != "FIGHTER":
        # If UFCStats puts fighter col first, we expect it; otherwise bail.
        return None

    red_d: Dict[str, str] = {}
    blue_d: Dict[str, str] = {}

    for h, cell in zip(headers[1:], cells[1:]):
        pair = _split_dual_cell(cell)
        if not pair:
            continue
        a, b = pair
        red_d[h] = a
        blue_d[h] = b

    # Require at least SIGSTR or TOTALSTR to treat as a stats row
    if ("SIGSTR" not in red_d and "TOTALSTR" not in red_d) and (
        "SIGSTR" not in blue_d and "TOTALSTR" not in blue_d
    ):
        return None

    return round_num, red_d, blue_d


def _tables_with_stat_headers(soup: BeautifulSoup) -> List[Tag]:
    """
    Find stat-like tables by header signature.
    UFCStats variants include:
      - Totals table (no Round header)
      - Round table (includes Round X header)
    """
    out: List[Tag] = []
    for t in soup.find_all("table"):
        hs = set(_table_headers(t))
        hs.discard("")
        # strong signature
        if "SIGSTR" in hs and "TOTALSTR" in hs:
            out.append(t)
            continue
        # weaker signature
        if ("SIGSTR" in hs or "TOTALSTR" in hs) and ("KD" in hs or "TD" in hs or "CTRL" in hs):
            out.append(t)
            continue
    return out


def _parse_totals_table(
    soup: BeautifulSoup,
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]]]:
    """
    Totals table:
      - standard UFCStats: 2 rows (red/blue)
      - NOT the single-row dual-fighter format (that’s round tables)
    """
    candidates = _tables_with_stat_headers(soup)
    if not candidates:
        return None, None

    def score(t: Tag) -> int:
        hs = set(_table_headers(t))
        hs.discard("")
        sc = 0
        sc += 5 if "SIGSTR" in hs else 0
        sc += 5 if "TOTALSTR" in hs else 0
        sc += 2 if "CTRL" in hs else 0
        sc += 2 if "TD" in hs else 0
        sc += 1 if "KD" in hs else 0
        # prefer totals table (no "Round X" header)
        sc += 3 if _round_label_from_table(t) is None else 0
        return sc

    candidates.sort(key=score, reverse=True)

    for table in candidates:
        # skip round-labeled tables as totals
        if _round_label_from_table(table) is not None:
            continue

        rows = _data_rows(table)
        if len(rows) < 2:
            continue

        # old/new 2-row totals
        headers = _table_headers(table)
        if not headers:
            continue

        def row_to_dict(tr: Tag) -> Dict[str, str]:
            cells = _row_cells_text(tr)
            # align
            h = headers[:]
            if len(h) > len(cells):
                h = h[: len(cells)]
            elif len(cells) > len(h):
                cells = cells[: len(h)]
            d: Dict[str, str] = {}
            for hh, vv in zip(h, cells):
                if hh:
                    d[hh] = vv
            return d

        r0 = row_to_dict(rows[0])
        r1 = row_to_dict(rows[1])

        if not r0 or not r1:
            continue
        if ("SIGSTR" not in r0 and "TOTALSTR" not in r0) or (
            "SIGSTR" not in r1 and "TOTALSTR" not in r1
        ):
            continue

        return r0, r1

    return None, None


def _parse_round_tables(soup: BeautifulSoup) -> List[Tuple[int, Dict[str, str], Dict[str, str]]]:
    """
    Round tables:
      - standard UFCStats: per-round tables (2 rows)
      - UFCStats variant: single-row dual-fighter tables, labeled "Round X" in header
    """
    tables = _tables_with_stat_headers(soup)
    if not tables:
        return []

    rounds: List[Tuple[int, Dict[str, str], Dict[str, str]]] = []

    # 1) Prefer explicit Round-labeled tables (single-row dual variant)
    for t in tables:
        rn = _round_label_from_table(t)
        if rn is None:
            continue
        parsed = _parse_single_row_dual(t)
        if not parsed:
            continue
        rn2, red_d, blue_d = parsed
        rounds.append((rn2 or rn, red_d, blue_d))

    # If we found any explicit round tables, return them sorted
    if rounds:
        rounds.sort(key=lambda x: x[0])
        # cap defensively
        return [r for r in rounds if 1 <= r[0] <= 10][:7]

    # 2) Fallback: try 2-row round tables (if site uses them)
    # We’ll take any stat tables *after* totals table as rounds (classic UFCStats layout)
    candidates = tables[:]

    def score(t: Tag) -> int:
        hs = set(_table_headers(t))
        hs.discard("")
        sc = 0
        sc += 5 if "SIGSTR" in hs else 0
        sc += 5 if "TOTALSTR" in hs else 0
        sc += 2 if "CTRL" in hs else 0
        sc += 2 if "TD" in hs else 0
        sc += 1 if "KD" in hs else 0
        sc += 3 if _round_label_from_table(t) is None else 0
        return sc

    totals_idx = None
    best = -1
    for i, t in enumerate(candidates):
        rows = _data_rows(t)
        if len(rows) < 2:
            continue
        sc = score(t)
        if sc > best and _round_label_from_table(t) is None:
            best = sc
            totals_idx = i

    if totals_idx is None:
        return []

    headers = None
    rnd = 1
    for t in candidates[totals_idx + 1 :]:
        rows = _data_rows(t)
        if len(rows) < 2:
            continue
        headers = _table_headers(t)
        if not headers:
            continue

        def row_to_dict(tr: Tag) -> Dict[str, str]:
            cells = _row_cells_text(tr)
            h = headers[:]
            if len(h) > len(cells):
                h = h[: len(cells)]
            elif len(cells) > len(h):
                cells = cells[: len(h)]
            d: Dict[str, str] = {}
            for hh, vv in zip(h, cells):
                if hh:
                    d[hh] = vv
            return d

        r0 = row_to_dict(rows[0])
        r1 = row_to_dict(rows[1])
        if not r0 or not r1:
            continue
        if ("SIGSTR" not in r0 and "TOTALSTR" not in r0) or (
            "SIGSTR" not in r1 and "TOTALSTR" not in r1
        ):
            continue

        rounds.append((rnd, r0, r1))
        rnd += 1
        if rnd > 7:
            break

    return rounds


def _split_of(raw: str) -> Tuple[Optional[int], Optional[int]]:
    raw = norm_space(raw)
    m = re.match(r"^(\d+)\s+of\s+(\d+)$", raw)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _extract_stat_fields(d: Dict[str, str]) -> Dict[str, Optional[object]]:
    """
    PATCH: expects canonical keys from _row_to_dict() such as:
      KD, SIGSTR, SIGSTRPCT, TOTALSTR, TD, TDPCT, SUBATT, REV, CTRL
    """

    def get(*keys: str) -> str:
        for k in keys:
            if k in d and d[k] is not None:
                return d[k]
        return ""

    kd = safe_int(get("KD"))

    sig_raw = get("SIGSTR")
    sig_l, sig_a = _split_of(sig_raw)

    sig_pct = None
    sp = get("SIGSTRPCT")
    if sp:
        try:
            sig_pct = float(sp.replace("%", "").strip())
        except Exception:
            sig_pct = None

    tot_raw = get("TOTALSTR")
    tot_l, tot_a = _split_of(tot_raw)

    td_raw = get("TD")
    td_l, td_a = _split_of(td_raw)

    td_pct = None
    tp = get("TDPCT")
    if tp:
        try:
            td_pct = float(tp.replace("%", "").strip())
        except Exception:
            td_pct = None

    sub_att = safe_int(get("SUBATT"))
    rev = safe_int(get("REV"))
    ctrl_raw = get("CTRL")
    ctrl = normalize_ctrl(ctrl_raw)
    ctrl_sec = mmss_to_seconds(ctrl) if ctrl else None

    return {
        "kd": kd,
        "sig_str": sig_raw or None,
        "sig_landed": sig_l,
        "sig_att": sig_a,
        "sig_pct": sig_pct,
        "total_str": tot_raw or None,
        "total_landed": tot_l,
        "total_att": tot_a,
        "td": td_raw or None,
        "td_landed": td_l,
        "td_att": td_a,
        "td_pct": td_pct,
        "sub_att": sub_att,
        "rev": rev,
        "ctrl": ctrl or None,
        "ctrl_sec": ctrl_sec,
    }


# -------------------------
# Fight meta + winner
# -------------------------
def _parse_fight_meta(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    event_name = None
    event_date = None

    ev = soup.select_one("a[href*='/event-details/']")
    if ev:
        event_name = _text(ev)

    full = _text(soup)
    mdate = re.search(r"\bDate:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})\b", full)
    if mdate:
        try:
            import datetime as _dt

            dt = _dt.datetime.strptime(mdate.group(1), "%B %d, %Y").date()
            event_date = dt.isoformat()
        except Exception:
            event_date = mdate.group(1)

    weight_class = None
    m_wc = re.search(r"\bWeight:\s*([A-Za-z0-9 .'-]+)\b", full)
    if m_wc:
        weight_class = norm_space(m_wc.group(1))

    method = None
    m_method = re.search(r"\bMethod:\s*(.+?)\bRound:\b", full)
    if m_method:
        method = norm_space(m_method.group(1))

    rnd = None
    m_r = re.search(r"\bRound:\s*(\d+)\b", full)
    if m_r:
        rnd = m_r.group(1)

    t = None
    m_t = re.search(r"\bTime:\s*(\d{1,2}:\d{2})\b", full)
    if m_t:
        t = m_t.group(1)

    referee = None
    m_ref = re.search(r"\bReferee:\s*(.+?)\bDetails:\b", full)
    if m_ref:
        referee = norm_space(m_ref.group(1))

    details = None
    m_det = re.search(r"\bDetails:\s*(.+?)\b", full)
    if m_det:
        details = norm_space(m_det.group(1))

    return {
        "event_name": event_name,
        "event_date": event_date,
        "weight_class": weight_class,
        "method": method,
        "round": rnd,
        "time": t,
        "referee": referee,
        "details": details,
    }


def _parse_fighters_and_winner(
    soup: BeautifulSoup,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    names = []
    for a in soup.select("a[href*='/fighter-details/']"):
        nm = _text(a)
        if nm and nm not in names:
            names.append(nm)
        if len(names) >= 2:
            break

    red = names[0] if len(names) > 0 else None
    blue = names[1] if len(names) > 1 else None

    winner = None
    persons = soup.select(".b-fight-details__persons .b-fight-details__person")
    if persons and len(persons) >= 2:

        def status(p):
            lab = p.select_one(".b-fight-details__person-status")
            return _text(lab).upper() if lab else ""

        s0 = status(persons[0])
        s1 = status(persons[1])
        n0 = _text(persons[0].select_one("a[href*='/fighter-details/']"))
        n1 = _text(persons[1].select_one("a[href*='/fighter-details/']"))

        if s0 == "W":
            winner = n0
        elif s1 == "W":
            winner = n1

        if n0 and n1:
            red, blue = n0, n1

    return red, blue, winner


# -------------------------
# Upsert parsed
# -------------------------
def upsert_parsed(con: sqlite3.Connection, fight_url: str, raw_html: str) -> None:
    soup = BeautifulSoup(raw_html, "lxml")

    meta = _parse_fight_meta(soup)
    red, blue, winner = _parse_fighters_and_winner(soup)

    # DEFENSIVE DEFAULTS (prevents UnboundLocalError)
    totals = (None, None)
    rounds: List[Tuple[int, Dict[str, str], Dict[str, str]]] = []

    # --- meta upsert (unchanged) ---
    con.execute(
        """
        INSERT INTO ufc_fight_meta(
            fight_url, event_name, event_date, weight_class, method, round, time,
            referee, details, fighter_red, fighter_blue, winner
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fight_url) DO UPDATE SET
            event_name=excluded.event_name,
            event_date=excluded.event_date,
            weight_class=excluded.weight_class,
            method=excluded.method,
            round=excluded.round,
            time=excluded.time,
            referee=excluded.referee,
            details=excluded.details,
            fighter_red=excluded.fighter_red,
            fighter_blue=excluded.fighter_blue,
            winner=excluded.winner,
            parsed_at=datetime('now')
        """,
        (
            fight_url,
            meta.get("event_name"),
            meta.get("event_date"),
            meta.get("weight_class"),
            meta.get("method"),
            int(meta["round"]) if meta.get("round") and str(meta["round"]).isdigit() else None,
            meta.get("time"),
            meta.get("referee"),
            meta.get("details"),
            red,
            blue,
            winner,
        ),
    )

    # totals + rounds parsing
    totals = _parse_totals_table(soup)
    rounds = _parse_round_tables(soup)

    # totals upsert (unchanged logic)
    if totals and totals[0] and totals[1] and red and blue:
        red_d, blue_d = totals
        red_fields = _extract_stat_fields(red_d)
        blue_fields = _extract_stat_fields(blue_d)

        con.execute(
            """
            INSERT OR REPLACE INTO ufc_fight_totals(
              fight_url, corner, fighter, kd, sig_str, sig_landed, sig_att, sig_pct,
              total_str, total_landed, total_att, td, td_landed, td_att, td_pct,
              sub_att, rev, ctrl, ctrl_sec
            ) VALUES (?, 'red', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fight_url,
                red,
                red_fields["kd"],
                red_fields["sig_str"],
                red_fields["sig_landed"],
                red_fields["sig_att"],
                red_fields["sig_pct"],
                red_fields["total_str"],
                red_fields["total_landed"],
                red_fields["total_att"],
                red_fields["td"],
                red_fields["td_landed"],
                red_fields["td_att"],
                red_fields["td_pct"],
                red_fields["sub_att"],
                red_fields["rev"],
                red_fields["ctrl"],
                red_fields["ctrl_sec"],
            ),
        )

        con.execute(
            """
            INSERT OR REPLACE INTO ufc_fight_totals(
              fight_url, corner, fighter, kd, sig_str, sig_landed, sig_att, sig_pct,
              total_str, total_landed, total_att, td, td_landed, td_att, td_pct,
              sub_att, rev, ctrl, ctrl_sec
            ) VALUES (?, 'blue', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fight_url,
                blue,
                blue_fields["kd"],
                blue_fields["sig_str"],
                blue_fields["sig_landed"],
                blue_fields["sig_att"],
                blue_fields["sig_pct"],
                blue_fields["total_str"],
                blue_fields["total_landed"],
                blue_fields["total_att"],
                blue_fields["td"],
                blue_fields["td_landed"],
                blue_fields["td_att"],
                blue_fields["td_pct"],
                blue_fields["sub_att"],
                blue_fields["rev"],
                blue_fields["ctrl"],
                blue_fields["ctrl_sec"],
            ),
        )

    # rounds upsert (unchanged logic)
    if rounds and red and blue:
        for rnd, red_d, blue_d in rounds:
            rf = _extract_stat_fields(red_d)
            bf = _extract_stat_fields(blue_d)

            con.execute(
                """
                INSERT OR REPLACE INTO ufc_fight_rounds(
                  fight_url, round, corner, fighter, kd, sig_str, sig_landed, sig_att, sig_pct,
                  total_str, total_landed, total_att, td, td_landed, td_att, td_pct,
                  sub_att, rev, ctrl, ctrl_sec
                ) VALUES (?, ?, 'red', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fight_url,
                    rnd,
                    red,
                    rf["kd"],
                    rf["sig_str"],
                    rf["sig_landed"],
                    rf["sig_att"],
                    rf["sig_pct"],
                    rf["total_str"],
                    rf["total_landed"],
                    rf["total_att"],
                    rf["td"],
                    rf["td_landed"],
                    rf["td_att"],
                    rf["td_pct"],
                    rf["sub_att"],
                    rf["rev"],
                    rf["ctrl"],
                    rf["ctrl_sec"],
                ),
            )

            con.execute(
                """
                INSERT OR REPLACE INTO ufc_fight_rounds(
                  fight_url, round, corner, fighter, kd, sig_str, sig_landed, sig_att, sig_pct,
                  total_str, total_landed, total_att, td, td_landed, td_att, td_pct,
                  sub_att, rev, ctrl, ctrl_sec
                ) VALUES (?, ?, 'blue', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fight_url,
                    rnd,
                    blue,
                    bf["kd"],
                    bf["sig_str"],
                    bf["sig_landed"],
                    bf["sig_att"],
                    bf["sig_pct"],
                    bf["total_str"],
                    bf["total_landed"],
                    bf["total_att"],
                    bf["td"],
                    bf["td_landed"],
                    bf["td_att"],
                    bf["td_pct"],
                    bf["sub_att"],
                    bf["rev"],
                    bf["ctrl"],
                    bf["ctrl_sec"],
                ),
            )

    con.commit()

    # -------------------------
    # PATCH: backfill totals from rounds if totals missing
    # -------------------------
    def _sec_to_mmss(sec: Optional[int]) -> Optional[str]:
        if sec is None:
            return None
        if sec < 0:
            sec = 0
        m = sec // 60
        s = sec % 60
        return f"{m}:{s:02d}"

    def _sum_int(vals) -> Optional[int]:
        out = 0
        anyv = False
        for v in vals:
            if v is None:
                continue
            anyv = True
            out += int(v)
        return out if anyv else None

    has_totals = (
        con.execute(
            "SELECT 1 FROM ufc_fight_totals WHERE fight_url=? LIMIT 1", (fight_url,)
        ).fetchone()
        is not None
    )

    if (not has_totals) and red and blue:
        rr = con.execute(
            """
            SELECT corner, kd, sig_landed, sig_att, total_landed, total_att,
                   td_landed, td_att, sub_att, rev, ctrl_sec
            FROM ufc_fight_rounds
            WHERE fight_url=?
            """,
            (fight_url,),
        ).fetchall()

        # If we still have no rounds inserted (older format), do nothing
        if rr:
            by_corner = {"red": [], "blue": []}
            for r in rr:
                by_corner[r["corner"]].append(r)

            def build_corner(corner: str, fighter_name: str) -> Dict[str, Optional[object]]:
                rows_c = by_corner.get(corner, [])

                kd = _sum_int([x["kd"] for x in rows_c])
                sig_l = _sum_int([x["sig_landed"] for x in rows_c])
                sig_a = _sum_int([x["sig_att"] for x in rows_c])
                tot_l = _sum_int([x["total_landed"] for x in rows_c])
                tot_a = _sum_int([x["total_att"] for x in rows_c])
                td_l = _sum_int([x["td_landed"] for x in rows_c])
                td_a = _sum_int([x["td_att"] for x in rows_c])
                sub_att = _sum_int([x["sub_att"] for x in rows_c])
                rev = _sum_int([x["rev"] for x in rows_c])
                ctrl_sec = _sum_int([x["ctrl_sec"] for x in rows_c])

                sig_str = (
                    f"{sig_l} of {sig_a}" if (sig_l is not None and sig_a is not None) else None
                )
                total_str = (
                    f"{tot_l} of {tot_a}" if (tot_l is not None and tot_a is not None) else None
                )
                td = f"{td_l} of {td_a}" if (td_l is not None and td_a is not None) else None

                sig_pct = (
                    (float(sig_l) / float(sig_a) * 100.0) if (sig_l is not None and sig_a) else None
                )
                td_pct = (
                    (float(td_l) / float(td_a) * 100.0) if (td_l is not None and td_a) else None
                )

                ctrl = _sec_to_mmss(ctrl_sec) if ctrl_sec is not None else None

                return {
                    "fighter": fighter_name,
                    "kd": kd,
                    "sig_str": sig_str,
                    "sig_landed": sig_l,
                    "sig_att": sig_a,
                    "sig_pct": sig_pct,
                    "total_str": total_str,
                    "total_landed": tot_l,
                    "total_att": tot_a,
                    "td": td,
                    "td_landed": td_l,
                    "td_att": td_a,
                    "td_pct": td_pct,
                    "sub_att": sub_att,
                    "rev": rev,
                    "ctrl": ctrl,
                    "ctrl_sec": ctrl_sec,
                }

            red_tot = build_corner("red", red)
            blue_tot = build_corner("blue", blue)

            con.execute(
                """
                INSERT OR REPLACE INTO ufc_fight_totals(
                  fight_url, corner, fighter, kd, sig_str, sig_landed, sig_att, sig_pct,
                  total_str, total_landed, total_att, td, td_landed, td_att, td_pct,
                  sub_att, rev, ctrl, ctrl_sec
                ) VALUES (?, 'red', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fight_url,
                    red_tot["fighter"],
                    red_tot["kd"],
                    red_tot["sig_str"],
                    red_tot["sig_landed"],
                    red_tot["sig_att"],
                    red_tot["sig_pct"],
                    red_tot["total_str"],
                    red_tot["total_landed"],
                    red_tot["total_att"],
                    red_tot["td"],
                    red_tot["td_landed"],
                    red_tot["td_att"],
                    red_tot["td_pct"],
                    red_tot["sub_att"],
                    red_tot["rev"],
                    red_tot["ctrl"],
                    red_tot["ctrl_sec"],
                ),
            )

            con.execute(
                """
                INSERT OR REPLACE INTO ufc_fight_totals(
                  fight_url, corner, fighter, kd, sig_str, sig_landed, sig_att, sig_pct,
                  total_str, total_landed, total_att, td, td_landed, td_att, td_pct,
                  sub_att, rev, ctrl, ctrl_sec
                ) VALUES (?, 'blue', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fight_url,
                    blue_tot["fighter"],
                    blue_tot["kd"],
                    blue_tot["sig_str"],
                    blue_tot["sig_landed"],
                    blue_tot["sig_att"],
                    blue_tot["sig_pct"],
                    blue_tot["total_str"],
                    blue_tot["total_landed"],
                    blue_tot["total_att"],
                    blue_tot["td"],
                    blue_tot["td_landed"],
                    blue_tot["td_att"],
                    blue_tot["td_pct"],
                    blue_tot["sub_att"],
                    blue_tot["rev"],
                    blue_tot["ctrl"],
                    blue_tot["ctrl_sec"],
                ),
            )

    con.commit()


# -------------------------
# Orchestration
# -------------------------
def run(
    db_path: str, max_events: Optional[int], max_fights: Optional[int], parse_only: bool
) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    ensure_schema(con)

    timeout = httpx.Timeout(connect=10.0, read=35.0, write=10.0, pool=10.0)
    with httpx.Client(
        headers=HEADERS, timeout=timeout, follow_redirects=True, trust_env=True
    ) as client:
        print(
            f"[ufc] db={db_path} parse_only={parse_only} max_events={max_events} max_fights={max_fights}"
        )

        if not parse_only:
            print("[ufc] discovering events...")
            n = discover_events(con, client)
            print(f"[ufc] discovered events: {n}")

            # events -> fights
            events_done = 0
            while True:
                if max_events is not None and events_done >= max_events:
                    break
                ev = next_queued(con, "ufc_events", "event_url")
                if not ev:
                    break
                try:
                    nf = discover_fights_for_event(con, client, ev)
                    mark_status(con, "ufc_events", "event_url", ev, "done", None)
                    events_done += 1
                    print(f"[event] done fights={nf} url={ev}")
                except Exception as e:
                    mark_status(con, "ufc_events", "event_url", ev, "error", str(e)[:800])
                    print(f"[event] ERROR url={ev} err={str(e)[:160]}")
                _sleep_polite()

            # fights -> html
            fights_done = 0
            fights_err = 0
            while True:
                if max_fights is not None and fights_done >= max_fights:
                    break
                fu = next_queued(con, "ufc_fights", "fight_url")
                if not fu:
                    break
                try:
                    ingest_fight_html(con, client, fu)
                    mark_status(con, "ufc_fights", "fight_url", fu, "done", None)
                    fights_done += 1
                    if fights_done % 25 == 0:
                        print(f"[fight] html done={fights_done} last={fu}")
                except Exception as e:
                    fights_err += 1
                    mark_status(con, "ufc_fights", "fight_url", fu, "error", str(e)[:800])
                    print(f"[fight] html ERROR url={fu} err={str(e)[:160]}")
                _sleep_polite()

            print(f"[ufc] html complete fights_done={fights_done} fights_err={fights_err}")

        # Parse any fight html we have, where meta OR totals OR rounds are missing for that fight_url
        print("[ufc] parsing fight html -> parsed tables...")
        rows = con.execute(
            """
            SELECT h.fight_url, h.raw_html
            FROM ufc_fight_html h
            LEFT JOIN ufc_fight_meta m ON m.fight_url = h.fight_url
            LEFT JOIN (SELECT DISTINCT fight_url FROM ufc_fight_totals) t ON t.fight_url = h.fight_url
            LEFT JOIN (SELECT DISTINCT fight_url FROM ufc_fight_rounds) r ON r.fight_url = h.fight_url
            WHERE m.fight_url IS NULL
               OR t.fight_url IS NULL
               OR r.fight_url IS NULL
            ORDER BY h.scraped_at ASC
            """
        ).fetchall()

        ok = 0
        bad = 0
        for row in rows:
            fu = row["fight_url"]
            try:
                upsert_parsed(con, fu, row["raw_html"])
                ok += 1
            except Exception as e:
                bad += 1
                if DEBUG:
                    print(f"[parse] ERROR fight_url={fu} err={type(e).__name__}: {e}")
            if (ok + bad) % 200 == 0:
                print(f"[parse] progress ok={ok} bad={bad}")

        print(f"[parse] done ok={ok} bad={bad}")

    con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--max-events", type=int, default=None)
    ap.add_argument("--max-fights", type=int, default=None)
    ap.add_argument("--parse-only", action="store_true")
    args = ap.parse_args()

    run(
        db_path=args.db,
        max_events=args.max_events,
        max_fights=args.max_fights,
        parse_only=args.parse_only,
    )


if __name__ == "__main__":
    main()
