# routes/schedule_proxy.py
from __future__ import annotations

from datetime import datetime, timedelta
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
        et = pytz.timezone("America/New_York")
        return datetime.now(et).strftime("%Y-%m-%d")
    if date_str.lower() in ("tomorrow", "yesterday"):
        et = pytz.timezone("America/New_York")
        now = datetime.now(et)
        if date_str.lower() == "tomorrow":
            return (now + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    # basic sanity: accept YYYY-MM-DD as-is
    return date_str

# Serve both aliases so schema/instructions match code
@router.get("/schedule_for_date")        # NEW alias to match schema/instructions
@router.get("/mlb/schedule_for_date")    # existing path kept for compatibility
async def schedule_for_date(date: str = Query("today")):
    """Proxy MLB schedule so /schedule_for_date and /mlb/schedule_for_date return 200 with JSON."""
    date_str = _normalize_date(date)
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"{STATSAPI_BASE}/schedule",
            params={"sportId": 1, "date": date_str},
            headers={"Accept": "application/json"},
        )
        r.raise_for_status()
        return r.json()
