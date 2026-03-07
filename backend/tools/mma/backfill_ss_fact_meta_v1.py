# backend/tools/mma/backfill_ss_fact_meta_v1.py

import os, re, sqlite3, time
from typing import Optional, Tuple

DB_PATH = os.environ.get("MMA_HIST_MART_PATH", r"data/marts/mma_historical_ss_full.sqlite")

BAD_SUBSTRINGS = [
    "Sports-Statistics.com",
    "Fight Statistics",
    "Scheduled UFC Events",
    "Upcoming UFC Events",
    "Sports Betting",
    "Odds Calculator",
    "How Odds Work",
    "Privacy Policy",
    "Contact Us",
    "BY SPORT",
    "Meta All Meta",
    "Home > UFC",
]


def is_bad_name(s: Optional[str]) -> bool:
    if not s:
        return True
    t = " ".join(str(s).split())
    if len(t) < 3 or len(t) > 45:
        return True
    low = t.lower()
    for b in BAD_SUBSTRINGS:
        if b.lower() in low:
            return True
    # too many words usually = scraped site chrome
    if len(t.split()) > 6:
        return True
    return False


def connect(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB not found: {db_path}")
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def strip_html(raw_html: str) -> str:
    # remove style/script first so we don't match CSS
    h = re.sub(r"(?is)<style.*?>.*?</style>", " ", raw_html)
    h = re.sub(r"(?is)<script.*?>.*?</script>", " ", h)
    # remove tags
    h = re.sub(r"(?is)<[^>]+>", " ", h)
    # normalize whitespace
    return " ".join(h.split())


def extract_fighters_from_text(text: str) -> Optional[Tuple[str, str, str]]:
    """
    Returns (winner_name, loser_name, (f1,f2 order not important))
    Finds patterns like: "Anthony Hernandez (Loss)" and "Sean Strickland (Win)"
    """
    # capture Name + (Win/Loss)
    # keep it permissive, but avoid pulling huge blobs
    pat = re.compile(r"\b([A-Z][A-Za-z\.\-'\s]{2,40}?)\s*\((Win|Loss)\)\b", re.IGNORECASE)
    hits = pat.findall(text)

    # normalize and de-dupe while preserving first occurrence
    seen = {}
    ordered = []
    for name, wl in hits:
        nm = " ".join(name.split()).strip()
        key = nm.lower()
        if key not in seen:
            seen[key] = wl.lower()
            ordered.append((nm, wl.lower()))

    # need at least 2 distinct names
    if len(ordered) < 2:
        return None

    # pick first winner and first loser
    winner = next((nm for nm, wl in ordered if wl == "win"), None)
    loser = next((nm for nm, wl in ordered if wl == "loss"), None)

    # if we didn't get a clean pair, bail
    if not winner or not loser:
        return None

    return winner, loser, winner  # third value unused, kept for clarity


def parse_method_round_time(text: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    method = None
    rnd = None
    tm = None

    # Method
    m = re.search(r"\bDecision\b(?:\s*-\s*(Unanimous|Split|Majority))?", text, re.IGNORECASE)
    if m:
        sub = m.group(1)
        method = f"Decision - {sub.title()}" if sub else "Decision"
    elif re.search(r"\bKO/TKO\b|\bTKO\b|\bKO\b", text, re.IGNORECASE):
        method = "KO/TKO"
    elif re.search(r"\bSubmission\b", text, re.IGNORECASE):
        method = "Submission"

    # Round (common formats)
    m = re.search(r"\bRound\s*[:\-]?\s*(\d)\b", text, re.IGNORECASE)
    if m:
        rnd = int(m.group(1))
    else:
        m = re.search(r"\bR(\d)\b", text)
        if m:
            rnd = int(m.group(1))

    # Time (mm:ss) — prefer one near the word Time if possible
    m = re.search(r"\bTime\s*[:\-]?\s*(\d{1,2}:\d{2})\b", text, re.IGNORECASE)
    if m:
        tm = m.group(1)
    else:
        m = re.search(r"\b(\d{1,2}:\d{2})\b", text)
        if m:
            tm = m.group(1)

    return method, rnd, tm


def names_from_fight_url(fight_url: str) -> Optional[Tuple[str, str]]:
    # last segment like raulian-paiva-v-sean-omalley
    tail = fight_url.strip("/").split("/")[-1]
    if "-v-" not in tail:
        return None
    left, right = tail.split("-v-", 1)

    def slug_to_name(slug: str) -> str:
        s = slug.replace("039", "'")
        s = re.sub(r"[_\-]+", " ", s)
        s = " ".join(s.split())
        out = []
        for w in s.split():
            if "'" in w:
                parts = w.split("'")
                out.append("'".join([p[:1].upper() + p[1:].lower() if p else "" for p in parts]))
            else:
                out.append(w[:1].upper() + w[1:].lower())
        return " ".join(out)

    f1 = slug_to_name(left)
    f2 = slug_to_name(right)
    if is_bad_name(f1) or is_bad_name(f2):
        return None
    return f1, f2


def main():
    t0 = time.time()
    print("DB:", DB_PATH)

    with connect(DB_PATH) as con:
        cur = con.cursor()

        fight_rows = cur.execute(
            """
            SELECT fight_url
            FROM ss_fact_fighter_fights
            GROUP BY fight_url
            HAVING
              SUM(CASE WHEN method IS NULL OR round IS NULL OR time IS NULL OR is_win IS NULL THEN 1 ELSE 0 END) > 0
              OR SUM(CASE WHEN fighter LIKE '%Sports-Statistics%' OR fighter LIKE '%Fight Statistics%' THEN 1 ELSE 0 END) > 0
              OR SUM(CASE WHEN opponent LIKE '%Sports-Statistics%' OR opponent LIKE '%Fight Statistics%' THEN 1 ELSE 0 END) > 0
            """
        ).fetchall()

        fight_urls = [r["fight_url"] for r in fight_rows if r["fight_url"]]
        print("Fights needing backfill:", len(fight_urls))

        updated = 0
        skipped_no_html = 0
        skipped_parse = 0

        for fight_url in fight_urls:
            # get the two ss_fact rows by rowid so we can update even if names are garbage
            fact = cur.execute(
                "SELECT rowid, fighter, opponent FROM ss_fact_fighter_fights WHERE fight_url=? ORDER BY rowid LIMIT 2;",
                (fight_url,),
            ).fetchall()
            if len(fact) < 2:
                continue

            h = cur.execute(
                "SELECT raw_html FROM ss_fight_html WHERE fight_url=? LIMIT 1;", (fight_url,)
            ).fetchone()
            if not h or not h["raw_html"]:
                skipped_no_html += 1
                continue

            text = strip_html(h["raw_html"])

            # 1) try to extract fighters + winner/loser directly from text
            wl = extract_fighters_from_text(text)
            if wl:
                winner, loser, _ = wl
                f1, f2 = winner, loser
            else:
                # 2) fallback: slug-derived names (better than garbage chrome)
                pair = names_from_fight_url(fight_url)
                if not pair:
                    skipped_parse += 1
                    continue
                f1, f2 = pair
                winner = None  # unknown unless we can detect (Win)/(Loss)

                # attempt win/loss again using slug names with (Win)/(Loss)
                pat_w = re.compile(re.escape(f1) + r".{0,120}\((Win|Loss)\)", re.IGNORECASE)
                pat_b = re.compile(re.escape(f2) + r".{0,120}\((Win|Loss)\)", re.IGNORECASE)
                m1 = pat_w.search(text)
                m2 = pat_b.search(text)
                if m1 and m2:
                    if m1.group(1).lower() == "win" and m2.group(1).lower() == "loss":
                        winner = f1
                    elif m2.group(1).lower() == "win" and m1.group(1).lower() == "loss":
                        winner = f2

            method, rnd, tm = parse_method_round_time(text)

            # if we still got nothing, count as skipped
            if not any([method, rnd, tm, winner]):
                skipped_parse += 1
                continue

            # Update the two rows deterministically
            r1, r2 = fact[0]["rowid"], fact[1]["rowid"]

            # set fighter/opponent (repair garbage names)
            cur.execute(
                "UPDATE ss_fact_fighter_fights SET fighter=?, opponent=? WHERE rowid=?;",
                (f1, f2, r1),
            )
            cur.execute(
                "UPDATE ss_fact_fighter_fights SET fighter=?, opponent=? WHERE rowid=?;",
                (f2, f1, r2),
            )

            # set method/round/time for both
            cur.execute(
                """
                UPDATE ss_fact_fighter_fights
                SET method=COALESCE(method, ?),
                    round=COALESCE(round, ?),
                    time=COALESCE(time, ?)
                WHERE fight_url=?;
                """,
                (method, rnd, tm, fight_url),
            )

            # set win/loss if known
            if winner:
                cur.execute(
                    "UPDATE ss_fact_fighter_fights SET is_win=CASE WHEN fighter=? THEN 1 ELSE 0 END WHERE fight_url=?;",
                    (winner, fight_url),
                )

            updated += 1

        con.commit()

        rem_meta = cur.execute(
            "SELECT COUNT(*) FROM ss_fact_fighter_fights WHERE method IS NULL OR round IS NULL OR time IS NULL;"
        ).fetchone()[0]
        rem_win = cur.execute(
            "SELECT COUNT(*) FROM ss_fact_fighter_fights WHERE is_win IS NULL;"
        ).fetchone()[0]

    print("Updated fights:", updated)
    print("Skipped no html:", skipped_no_html)
    print("Skipped parse:", skipped_parse)
    print("Remaining rows missing meta (method/round/time):", rem_meta)
    print("Remaining rows missing is_win:", rem_win)
    print("Done in", round(time.time() - t0, 2), "sec")


if __name__ == "__main__":
    main()
