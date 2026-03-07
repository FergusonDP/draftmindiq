import os, time, sqlite3, re
import httpx

DB_PATH = os.environ.get("MMA_HIST_MART_PATH", r"data/marts/mma_historical_ss_full.sqlite")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def connect():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB not found: {DB_PATH}")
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def looks_like_real_fight_page(html: str, fight_url: str) -> bool:
    if not html or len(html) < 15000:
        return False

    # Canonical should usually include the fight page path
    # (yours was False -> that's the key symptom)
    if fight_url in html:
        # still require some stats structure
        if re.search(r"Significant Strikes", html, re.I) and re.search(
            r"Total Strikes", html, re.I
        ):
            return True

    # Alternate: win/loss marker is very strong when present
    if re.search(r"\((win|loss)\)", html, re.I) and re.search(r"Significant Strikes", html, re.I):
        return True

    # Alternate: totals/per-round tables show up on real pages
    if re.search(r"totals_summary|ufc_stats_table|Round\s*1", html, re.I) and re.search(
        r"Significant Strikes", html, re.I
    ):
        return True

    return False


def main():
    t0 = time.time()
    print("DB:", DB_PATH)

    with connect() as con:
        cur = con.cursor()
        fight_urls = [
            r["fight_url"]
            for r in cur.execute(
                """
            SELECT f.fight_url
            FROM ss_fact_fighter_fights f
            GROUP BY f.fight_url
            HAVING
              SUM(CASE WHEN f.method IS NULL OR f.round IS NULL OR f.time IS NULL OR f.is_win IS NULL THEN 1 ELSE 0 END) > 0
        """
            ).fetchall()
        ]

    print("Candidate fights needing meta:", len(fight_urls))

    ok = 0
    bad = 0
    http_err = 0

    with (
        httpx.Client(headers=HEADERS, timeout=30.0, follow_redirects=True) as client,
        connect() as con,
    ):
        cur = con.cursor()

        for i, url in enumerate(fight_urls, 1):
            try:
                r = client.get(url)
                if r.status_code != 200:
                    http_err += 1
                    continue

                html = r.text
                if not looks_like_real_fight_page(html, url):
                    bad += 1
                    continue

                # UPSERT into ss_fight_html
                cur.execute(
                    """
                    INSERT INTO ss_fight_html (fight_url, raw_html, html_len, scraped_at)
                    VALUES (?, ?, ?, datetime('now'))
                    ON CONFLICT(fight_url) DO UPDATE SET
                      raw_html=excluded.raw_html,
                      html_len=excluded.html_len,
                      scraped_at=datetime('now');
                """,
                    (url, html, len(html)),
                )

                ok += 1
                if ok % 50 == 0:
                    con.commit()
                    print(f"valid fight pages updated: {ok}")

            except Exception:
                http_err += 1
                continue

        con.commit()

    print("Valid fight pages updated:", ok)
    print("Rejected (still not fight pages):", bad)
    print("HTTP errors:", http_err)
    print("Done in", round(time.time() - t0, 2), "sec")


if __name__ == "__main__":
    main()
