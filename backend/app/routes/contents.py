from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(tags=["content"])

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"


def _load_items(kind: str, sport: str) -> list[dict[str, Any]]:
    sport_key = sport.strip().lower()
    file_path = DATA_DIR / kind / f"{sport_key}.json"

    if not file_path.exists():
        return []

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []
    except Exception:
        return []


@router.get("/news/{sport}")
def get_news(
    sport: str,
    limit: int = Query(default=6, ge=1, le=20),
) -> dict[str, Any]:
    items = _load_items("news", sport)[:limit]
    return {
        "ok": True,
        "items": items,
        "count": len(items),
    }


@router.get("/video/{sport}")
def get_video(
    sport: str,
    limit: int = Query(default=4, ge=1, le=20),
) -> dict[str, Any]:
    items = _load_items("video", sport)[:limit]
    return {
        "ok": True,
        "items": items,
        "count": len(items),
    }
