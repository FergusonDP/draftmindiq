from fastapi import APIRouter
import feedparser
import urllib.request
from datetime import datetime, timezone

router = APIRouter(prefix="/video", tags=["video"])

YT = {
    # keep your channel feeds here
    "MMA": ["https://www.youtube.com/feeds/videos.xml?channel_id=UCvgfXK4nTYKudb0rFR6noLA"],
    "NFL": ["https://www.youtube.com/feeds/videos.xml?channel_id=UCDVYQ4Zhbm3S2dlz7P1GBDg"],
    "NBA": ["https://www.youtube.com/feeds/videos.xml?channel_id=UCWJ2lWNubArHWmf3FIHbfcQ"],
    "MLB": ["https://www.youtube.com/feeds/videos.xml?channel_id=UCoLrcjPV5PbUrUyXq5mjc_A"],
    "NHL": ["https://www.youtube.com/feeds/videos.xml?channel_id=UCqFMzb-4AUf6WAIbl132QKA"],
}


def _safe_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "DraftMindIQ/1.0 (localhost)"})
    with urllib.request.urlopen(req, timeout=12) as r:
        return r.read()


def _iso_from_struct(t):
    try:
        if not t:
            return None
        dt = datetime(*t[:6], tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


@router.get("/{sport_key}")
def get_videos(sport_key: str, limit: int = 6):
    sport_key = (sport_key or "ALL").upper()

    # Build (sport,url) pairs so ALL can label each item
    pairs = []
    if sport_key == "ALL":
        for k, urls in YT.items():
            for u in urls:
                pairs.append((k, u))
    else:
        for u in YT.get(sport_key) or []:
            pairs.append((sport_key, u))

    items = []
    for sk, url in pairs:
        try:
            raw = _safe_get(url)
            feed = feedparser.parse(raw)
            for e in feed.entries[: max(limit, 6)]:
                link = (getattr(e, "link", "") or "").strip()
                title = (getattr(e, "title", "") or "").strip()
                published_at = _iso_from_struct(
                    getattr(e, "published_parsed", None)
                ) or _iso_from_struct(getattr(e, "updated_parsed", None))

                thumb = None
                if hasattr(e, "media_thumbnail") and e.media_thumbnail:
                    thumb = e.media_thumbnail[0].get("url")

                if link and title:
                    items.append(
                        {
                            "title": title,
                            "url": link,
                            "thumb": thumb,
                            "sport": sk,
                            "published_at": published_at,
                        }
                    )
        except Exception:
            continue

    # newest first
    items.sort(key=lambda x: x.get("published_at") or "", reverse=True)

    # dedupe by url
    seen = set()
    out = []
    for it in items:
        u = it.get("url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(it)
        if len(out) >= limit:
            break

    return {"ok": True, "sport": sport_key, "count": len(out), "items": out}
