# backend/tools/mma/scrape/ss_site_scraper.py

import os
import time
import random
import sqlite3
import re
from typing import Optional, List, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup


# -------------------------
# Config
# -------------------------
BASE_INDEX = "https://sports-statistics.com/ufc/ufc-fight-statistics/"
DOMAIN = "sports-statistics.com"

DB_PATH = os.environ.get("MMA_HIST_DB_PATH", r"data\marts\mma_historical_ss_full.sqlite")
DEBUG = os.environ.get("DEBUG", "0").strip() == "1"

# Hard caps
MAX_EVENT_ATTEMPTS = int(os.environ.get("SS_MAX_EVENT_ATTEMPTS", "6"))
MAX_FIGHT_ATTEMPTS = int(os.environ.get("SS_MAX_FIGHT_ATTEMPTS", "10"))

# Politeness between successful items
SLEEP_MIN = float(os.environ.get("SS_SLEEP_MIN", "0.25"))
SLEEP_MAX = float(os.environ.get("SS_SLEEP_MAX", "0.75"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "identity",
    "Referer": BASE_INDEX,
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

RETRY_STATUSES = {403, 429, 500, 502, 503, 504}


# -------------------------
# Politeness
# -------------------------
def _sleep_polite(min_s: float = SLEEP_MIN, max_s: float = SLEEP_MAX):
    time.sleep(random.uniform(min_s, max_s))


def _jitter(mult: float = 1.0) -> float:
    # 0.6x..1.4x jitter
    return mult * (0.6 + random.random() * 0.8)


# -------------------------
# URL helpers
# -------------------------
def norm_url(u: str) -> str:
    p = urlparse(u)
    scheme = "https"
    fragless = p._replace(scheme=scheme, fragment="")
    u2 = urlunparse(fragless)

    p2 = urlparse(u2)
    path = p2.path or "/"
    if path != "/" and not path.endswith("/"):
        path += "/"
    return urlunparse(p2._replace(path=path))


def is_same_domain(u: str) -> bool:
    try:
        return urlparse(u).netloc.lower().endswith(DOMAIN)
    except Exception:
        return False


def path_segments(u: str) -> List[str]:
    return [s for s in urlparse(u).path.strip("/").split("/") if s]


def is_event_url(u: str) -> bool:
    segs = path_segments(u)
    return len(segs) == 3 and segs[0] == "ufc" and segs[1] == "ufc-fight-statistics"


def looks_like_fight_slug(u: str) -> bool:
    segs = path_segments(u)
    if not segs:
        return False
    last = segs[-1].lower()
    return ("-v-" in last) or ("-vs-" in last)


def is_fight_url(u: str) -> bool:
    segs = path_segments(u)
    return (
        len(segs) == 4
        and segs[0] == "ufc"
        and segs[1] == "ufc-fight-statistics"
        and looks_like_fight_slug(u)
    )


def is_index_family(u: str) -> bool:
    segs = path_segments(u)
    if len(segs) < 2:
        return False
    if segs[0] != "ufc" or segs[1] != "ufc-fight-statistics":
        return False
    if is_event_url(u) or is_fight_url(u):
        return False
    return True


def extract_links(html: str, base_url: str) -> List[str]:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    out: List[str] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        absu = norm_url(urljoin(base_url, href))
        if is_same_domain(absu):
            out.append(absu)
    return out


# -------------------------
# HTTP fetch with retries
# -------------------------
def _is_retryable_exc(e: Exception) -> bool:
    if isinstance(e, httpx.TransportError):
        return True
    if isinstance(e, httpx.HTTPStatusError):
        try:
            code = int(e.response.status_code)
            return code in RETRY_STATUSES
        except Exception:
            return False
    return False


def _status_from_exc(e: Exception) -> Optional[int]:
    if isinstance(e, httpx.HTTPStatusError):
        try:
            return int(e.response.status_code)
        except Exception:
            return None
    return None


def _retry_after_seconds(resp: Optional[httpx.Response]) -> Optional[float]:
    if not resp:
        return None
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    ra = ra.strip()
    # could be seconds or HTTP-date; we only support seconds here
    if re.match(r"^\d+$", ra):
        return float(ra)
    return None


def fetch_html(url: str, client: httpx.Client, max_retries: int = 8) -> str:
    """
    Inner HTTP retry loop: handles transient network + retryable HTTP statuses.
    Separate from DB-level retry (attempts in ss_fights/ss_events).
    """
    last_err: Optional[Exception] = None
    base = 0.8
    cap = 45.0

    for attempt in range(max_retries + 1):
        try:
            r = client.get(url)

            if r.status_code in RETRY_STATUSES:
                # honor Retry-After if present (429)
                if r.status_code == 429:
                    ra = _retry_after_seconds(r)
                    if ra is not None:
                        sleep_s = min(cap, max(1.0, ra)) * _jitter(1.0)
                        if DEBUG:
                            print(f"[fetch] 429 retry-after={ra}s sleep={sleep_s:.2f}s url={url}")
                        time.sleep(sleep_s)
                    raise httpx.HTTPStatusError(
                        f"retryable status={r.status_code}", request=r.request, response=r
                    )

                raise httpx.HTTPStatusError(
                    f"retryable status={r.status_code}", request=r.request, response=r
                )

            r.raise_for_status()

            if DEBUG:
                print(f"[fetch] ok url={url} status={r.status_code} chars={len(r.text)}")
            return r.text

        except (httpx.TransportError, httpx.HTTPStatusError) as e:
            last_err = e
            if attempt >= max_retries:
                break

            sleep_s = min(cap, base * (2**attempt))
            sleep_s = sleep_s * _jitter(1.0)

            if DEBUG:
                code = _status_from_exc(e)
                print(
                    f"[fetch] retry={attempt+1}/{max_retries} sleep={sleep_s:.2f}s url={url} code={code} err={type(e).__name__}"
                )

            time.sleep(sleep_s)

    raise last_err  # type: ignore


# -------------------------
# SQLite schema
# -------------------------
def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ss_events (
        event_url TEXT PRIMARY KEY,
        discovered_at TEXT DEFAULT (datetime('now')),
        last_seen_at TEXT DEFAULT (datetime('now')),
        status TEXT DEFAULT 'queued',   -- queued|done|error
        attempts INTEGER DEFAULT 0,
        last_error TEXT
    );
    """
    )
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ss_fights (
        fight_url TEXT PRIMARY KEY,
        event_url TEXT,
        discovered_at TEXT DEFAULT (datetime('now')),
        last_seen_at TEXT DEFAULT (datetime('now')),
        status TEXT DEFAULT 'queued',   -- queued|done|error
        attempts INTEGER DEFAULT 0,
        last_error TEXT
    );
    """
    )
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS ss_fight_html (
        fight_url TEXT PRIMARY KEY,
        scraped_at TEXT DEFAULT (datetime('now')),
        html_len INTEGER,
        raw_html TEXT
    );
    """
    )
    con.commit()


def upsert_event(con: sqlite3.Connection, event_url: str) -> None:
    con.execute(
        """
    INSERT INTO ss_events(event_url, status)
    VALUES (?, 'queued')
    ON CONFLICT(event_url) DO UPDATE SET last_seen_at=datetime('now')
    """,
        (event_url,),
    )


def upsert_fight(con: sqlite3.Connection, fight_url: str, event_url: Optional[str]) -> None:
    con.execute(
        """
    INSERT INTO ss_fights(fight_url, event_url, status)
    VALUES (?, ?, 'queued')
    ON CONFLICT(fight_url) DO UPDATE SET
        event_url=COALESCE(excluded.event_url, ss_fights.event_url),
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


def next_queued(
    con: sqlite3.Connection, table: str, url_col: str, max_attempts: int
) -> Optional[str]:
    row = con.execute(
        f"""
        SELECT {url_col}
        FROM {table}
        WHERE status='queued'
          AND (attempts IS NULL OR attempts < ?)
        ORDER BY discovered_at ASC
        LIMIT 1
    """,
        (max_attempts,),
    ).fetchone()
    return row[0] if row else None


def get_attempts(con: sqlite3.Connection, table: str, url_col: str, url: str) -> int:
    row = con.execute(f"SELECT attempts FROM {table} WHERE {url_col}=?", (url,)).fetchone()
    try:
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def backoff_for_db_attempt(attempts: int, base: float = 2.0, cap: float = 180.0) -> float:
    """
    DB-level backoff: this is the delay after a failed event/fight (not per-request retries).
    attempts is the *current* attempts count after increment.
    """
    # grows slower than the per-request loop, but can cool down hard after repeated failures
    sleep_s = min(cap, base * (1.8 ** max(0, attempts - 1)))
    return sleep_s * _jitter(1.0)


# -------------------------
# Discovery
# -------------------------
def discover_events(
    con: sqlite3.Connection, client: httpx.Client, start_url: str, max_pages: int = 3000
) -> int:
    start_url = norm_url(start_url)
    seen = set()
    queue = [start_url]
    found = 0

    while queue and len(seen) < max_pages:
        page = queue.pop(0)
        if page in seen:
            continue

        if not (page == norm_url(BASE_INDEX) or is_index_family(page)):
            seen.add(page)
            continue

        try:
            html = fetch_html(page, client)
        except Exception as e:
            if DEBUG:
                print(f"[discover] page error url={page} err={str(e)[:140]}")
            seen.add(page)
            _sleep_polite(1.0, 2.0)
            continue

        links = extract_links(html, page)

        for u in links:
            if is_event_url(u):
                upsert_event(con, u)
                found += 1

        for u in links:
            if (u == norm_url(BASE_INDEX)) or is_index_family(u):
                if u not in seen and u not in queue:
                    queue.append(u)

        seen.add(page)

        if len(seen) % 25 == 0:
            print(
                f"[discover] pages={len(seen)} queue={len(queue)} events_seen~={found} last={page}"
            )

        con.commit()
        _sleep_polite()

    con.commit()
    return found


def discover_fights_for_event(con: sqlite3.Connection, client: httpx.Client, event_url: str) -> int:
    html = fetch_html(event_url, client)
    links = extract_links(html, event_url)
    n = 0
    for u in links:
        if is_fight_url(u) and u.startswith(event_url):
            upsert_fight(con, u, event_url)
            n += 1
    return n


# -------------------------
# Ingestion (minimum viable: save HTML)
# -------------------------
def ingest_fight(con: sqlite3.Connection, client: httpx.Client, fight_url: str) -> None:
    html = fetch_html(fight_url, client)
    con.execute(
        """
        INSERT INTO ss_fight_html(fight_url, html_len, raw_html)
        VALUES (?, ?, ?)
        ON CONFLICT(fight_url) DO UPDATE SET
          scraped_at=datetime('now'),
          html_len=excluded.html_len,
          raw_html=excluded.raw_html
    """,
        (fight_url, len(html), html),
    )


# -------------------------
# Main loop
# -------------------------
def run_site(
    db_path: str,
    start_url: str = BASE_INDEX,
    max_events: Optional[int] = None,
    max_fights: Optional[int] = None,
) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    ensure_schema(con)

    timeout = httpx.Timeout(connect=10.0, read=35.0, write=10.0, pool=10.0)
    with httpx.Client(
        headers=HEADERS, timeout=timeout, follow_redirects=True, trust_env=True
    ) as client:
        print(f"[site] db={db_path}")
        print(f"[site] start={start_url} max_events={max_events} max_fights={max_fights}")
        print(
            f"[site] MAX_EVENT_ATTEMPTS={MAX_EVENT_ATTEMPTS} MAX_FIGHT_ATTEMPTS={MAX_FIGHT_ATTEMPTS}"
        )

        # 1) Discover events
        print("[site] discovering events...")
        discover_events(con, client, start_url=start_url)

        # 2) Process queued events -> discover fights
        events_done = 0
        events_err = 0

        while True:
            if max_events is not None and events_done >= max_events:
                break

            ev = next_queued(con, "ss_events", "event_url", MAX_EVENT_ATTEMPTS)
            if not ev:
                break

            try:
                nf = discover_fights_for_event(con, client, ev)
                mark_status(con, "ss_events", "event_url", ev, "done", None)
                con.commit()
                events_done += 1
                print(f"[event] done fights={nf} url={ev}")

            except Exception as e:
                # retry-aware: retryable stays queued until max attempts hit
                msg = str(e)[:800]
                retryable = _is_retryable_exc(e)
                # increment attempts and decide
                mark_status(
                    con, "ss_events", "event_url", ev, "queued" if retryable else "error", msg
                )
                con.commit()

                attempts = get_attempts(con, "ss_events", "event_url", ev)
                if retryable and attempts < MAX_EVENT_ATTEMPTS:
                    sleep_s = backoff_for_db_attempt(attempts)
                    events_err += 1
                    print(
                        f"[event] retryable attempts={attempts}/{MAX_EVENT_ATTEMPTS} sleep={sleep_s:.1f}s url={ev} err={msg[:160]}"
                    )
                    time.sleep(sleep_s)
                else:
                    # exhausted or not retryable
                    con.execute("UPDATE ss_events SET status='error' WHERE event_url=?", (ev,))
                    con.commit()
                    events_err += 1
                    print(
                        f"[event] ERROR attempts={attempts}/{MAX_EVENT_ATTEMPTS} url={ev} err={msg[:160]}"
                    )

            _sleep_polite()

        # 3) Process queued fights -> ingest (save HTML)
        fights_done = 0
        fights_err = 0

        while True:
            if max_fights is not None and fights_done >= max_fights:
                break

            fu = next_queued(con, "ss_fights", "fight_url", MAX_FIGHT_ATTEMPTS)
            if not fu:
                break

            try:
                ingest_fight(con, client, fu)
                mark_status(con, "ss_fights", "fight_url", fu, "done", None)
                con.commit()

                fights_done += 1
                if fights_done % 25 == 0:
                    print(f"[fight] done={fights_done} last={fu}")

            except Exception as e:
                msg = str(e)[:800]
                retryable = _is_retryable_exc(e)

                # keep queued if retryable and attempts remain; otherwise error
                mark_status(
                    con, "ss_fights", "fight_url", fu, "queued" if retryable else "error", msg
                )
                con.commit()

                attempts = get_attempts(con, "ss_fights", "fight_url", fu)
                if retryable and attempts < MAX_FIGHT_ATTEMPTS:
                    sleep_s = backoff_for_db_attempt(attempts, base=2.5, cap=240.0)
                    fights_err += 1
                    print(
                        f"[fight] retryable attempts={attempts}/{MAX_FIGHT_ATTEMPTS} sleep={sleep_s:.1f}s url={fu} err={msg[:160]}"
                    )
                    time.sleep(sleep_s)
                else:
                    con.execute("UPDATE ss_fights SET status='error' WHERE fight_url=?", (fu,))
                    con.commit()
                    fights_err += 1
                    print(
                        f"[fight] ERROR attempts={attempts}/{MAX_FIGHT_ATTEMPTS} url={fu} err={msg[:160]}"
                    )

            _sleep_polite()

    con.close()
    print(
        f"[site] complete events_done={events_done} events_err={events_err} fights_done={fights_done} fights_err={fights_err} db={db_path}"
    )


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--start", default=BASE_INDEX)
    ap.add_argument("--max-events", type=int, default=None)
    ap.add_argument("--max-fights", type=int, default=None)
    args = ap.parse_args()

    run_site(
        db_path=args.db,
        start_url=args.start,
        max_events=args.max_events,
        max_fights=args.max_fights,
    )


if __name__ == "__main__":
    main()
