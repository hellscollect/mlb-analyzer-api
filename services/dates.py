# services/dates.py
from datetime import datetime, timedelta, date as date_cls
from fastapi import HTTPException
import pytz
from typing import Optional

def parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    now = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return now
    s = d.lower()
    if s == "yesterday":
        return now - timedelta(days=1)
    if s == "tomorrow":
        return now + timedelta(days=1)
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|tomorrow|YYYY-MM-DD")
