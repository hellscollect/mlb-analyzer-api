# routes/schedule_proxy.py
from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Query
import httpx
import pytz

router = APIRouter()
STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"

def _normalize_date(date_str: str | None) -> str:
    """
    Normalize 'today' to America/New_York date to match your app’s convention.
    Otherwise pass through YYYY-MM-DD.
    """
    if not date_str or date_str.lower() == "today":
        tz = pytz.timezone("America/New_York")
        return datetime.now(tz).strftime("%Y-%m-%d")
    return date_str

@router.get("/mlb/schedule_for_date")
async def schedule_for_date(date: str = Query("today")):
    """Proxy MLB schedule so /mlb/schedule_for_date returns 200 with JSON."""
    date_str = _normalize_date(date)
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"{STATSAPI_BASE}/schedule",
            params={"sportId": 1, "date": date_str},
            headers={"Accept": "application/json"},
        )
        r.raise_for_status()
        return r.json()
