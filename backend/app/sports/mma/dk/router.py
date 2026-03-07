# backend/app/sports/mma/dk/router.py

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

from app.sports.mma.dk.optimizer_inputs import (
    get_mma_dk_optimize,
    get_mma_dk_optimizer_inputs,
    get_mma_dk_slate_analysis,
)
from app.sports.mma.dk.repository import list_slates
from app.sports.mma.dk.db_status import get_mma_db_status
from app.sports.mma.dk.fight_status import get_mma_fight_status

router = APIRouter()


@router.get("/mma/dk/list_slates")
def mma_dk_list_slates():
    return {"ok": True, "slates": list_slates(limit=50)}


@router.get("/mma/dk/optimizer_inputs/{slate_id}")
def mma_dk_optimizer_inputs(slate_id: str):
    out = get_mma_dk_optimizer_inputs(slate_id)
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error", "unknown_error"))
    return out


@router.get("/mma/dk/slate_analysis/{slate_id}")
def mma_dk_slate_analysis(slate_id: str):
    out = get_mma_dk_slate_analysis(slate_id)
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error", "unknown_error"))
    return out


@router.get("/mma/dk/optimize/{slate_id}")
def mma_dk_optimize(slate_id: str, mode: str = "gpp"):
    out = get_mma_dk_optimize(slate_id, mode=mode)
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error", "unknown_error"))
    return out


@router.get("/mma/dk/db_status")
def mma_dk_db_status():
    return get_mma_db_status()


@router.get("/mma/dk/fight_status/{slate_id}")
def mma_dk_fight_status(slate_id: str):
    return get_mma_fight_status(slate_id)
