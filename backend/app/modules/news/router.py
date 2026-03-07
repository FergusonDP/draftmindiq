import urllib.request
from datetime import datetime, timezone

import feedparser
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/news", tags=["news"])

FEEDS = {
    "ALL": [
        "https://www.espn.com/espn/rss/mma/news",
        "https://www.espn.com/espn/rss/nfl/news",
        "https://www.espn.com/espn/rss/nba/news",
        "https://www.espn.com/espn/rss/mlb/news",
        "https://www.espn.com/espn/rss/nhl/news",
    ],
    "MMA": ["https://www.espn.com/espn/rss/mma/news"],
    "NFL": ["https://www.espn.com/espn/rss/nfl/news"],
    "NBA": ["https://www.espn.com/espn/rss/nba/news"],
    "MLB": ["https://www.espn.com/espn/rss/mlb/news"],
    "NHL": ["https://www.espn.com/espn/rss/nhl/news"],
}


def _safe_get(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "DraftMindIQ/1.0 (+https://localhost)",
            "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=12) as r:
        return r.read()


def _to_iso(dt_struct) -> str | None:
    try:
        # feedparser provides published_parsed / updated_parsed as time.struct_time
        if not dt_struct:
            return None
        ts = datetime(*dt_struct[:6], tzinfo=timezone.utc)
        return ts.isoformat()
    except Exception:
        return None


@router.get("/{sport_key}")
def get_news(sport_key: str, limit: int = 12):
    sport_key = (sport_key or "ALL").upper()
    urls = FEEDS.get(sport_key) or FEEDS["ALL"]

    items = []
    errors = []

    for url in urls:
        try:
            raw = _safe_get(url)
            feed = feedparser.parse(raw)
            for e in feed.entries[: limit * 2]:
                items.append(
                    {
                        "title": getattr(e, "title", "").strip(),
                        "url": getattr(e, "link", "").strip(),
                        "source": (
                            getattr(feed.feed, "title", "RSS").strip()
                            if getattr(feed, "feed", None)
                            else "RSS"
                        ),
                        "published_at": _to_iso(getattr(e, "published_parsed", None))
                        or _to_iso(getattr(e, "updated_parsed", None)),
                        "summary": (getattr(e, "summary", "") or "").strip(),
                    }
                )
        except Exception as ex:
            errors.append(f"{url}: {type(ex).__name__}")

    # Sort newest first when possible
    def sort_key(x):
        return x["published_at"] or ""

    items.sort(key=sort_key, reverse=True)

    # Deduplicate by URL
    seen = set()
    deduped = []
    for it in items:
        u = it.get("url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        deduped.append(it)
        if len(deduped) >= limit:
            break

    return {
        "ok": True,
        "sport": sport_key,
        "count": len(deduped),
        "items": deduped,
        "errors": errors[:3],
    }
