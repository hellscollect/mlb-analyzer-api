# routes/league_scan.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests

router = APIRouter()
ET = ZoneInfo("America/New_York")

# ---------- helpers ----------
class LeagueScanRequest(BaseModel):
    date: str = "today"   # "today" | "yesterday" | "YYYY-MM-DD"
    debug: int | None = 1

def _normalize_date(s: str) -> str:
    today = datetime.now(ET).date()
    if s == "today":
        return today.isoformat()
    if s == "yesterday":
        return (today - timedelta(days=1)).isoformat()
    return s  # assume ISO already

def _is_late_night_et() -> bool:
    now = datetime.now(ET).time()
    # After 9:30pm ET or before 5:00am ET, auto-try yesterday as fallback
    return (now >= datetime.strptime("21:30", "%H:%M").time()) or (now < datetime.strptime("05:00", "%H:%M").time())

def _post_json(base: str, path: str, body: dict, timeout: int = 15):
    try:
        r = requests.post(base + path, json=body, timeout=timeout)
        if r.status_code >= 400:
            return None, r.status_code
        return r.json(), r.status_code
    except Exception:
        return None, 599

def _get_json(base: str, path: str, timeout: int = 15):
    try:
        r = requests.get(base + path, timeout=timeout)
        if r.status_code >= 400:
            return None, r.status_code
        return r.json(), r.status_code
    except Exception:
        return None, 599

def _extract_games(schedule_json: dict) -> list:
    if not schedule_json:
        return []
    # support either shape
    if isinstance(schedule_json, dict):
        return schedule_json.get("games") \
            or schedule_json.get("matchups") \
            or schedule_json.get("data", {}).get("games") \
            or []
    return []

# ---------- main route ----------
@router.post("/league_scan_post")
def league_scan(req: LeagueScanRequest, request: Request):
    """
    One call that:
      1) Normalizes date in ET
      2) Tries /smoke_post (full scan); falls back to /smoke_post with samples
      3) If no smoke, composes from schedule + hot + cold
      4) If late-night ET, automatically tries yesterday as a fallback date
    Returns unified JSON with counts, matchups, hot_hitters, cold_hitters, and debug.
    """
    base = str(request.base_url).rstrip("/")  # e.g., https://your-service.onrender.com
    primary = _normalize_date(req.date)
    dates_to_try = [primary]
    if _is_late_night_et():
        yday = (datetime.fromisoformat(primary) - timedelta(days=1)).date().isoformat()
        if yday not in dates_to_try:
            dates_to_try.append(yday)

    last_status = None
    last_detail = None

    for d in dates_to_try:
        # --- 1) Try SMOKE full scan
        smoke, status = _post_json(base, "/smoke_post",
                                   {"date": d, "max_teams": 30, "per_team": 9, "debug": req.debug})
        if smoke and isinstance(smoke, dict):
            return {
                "date": d,
                "counts": {
                    "matchups": len(smoke.get("matchups", []) or []),
                    "hot_hitters": len(smoke.get("hot_hitters", []) or []),
                    "cold_hitters": len(smoke.get("cold_hitters", []) or []),
                },
                "matchups": smoke.get("matchups", []) or [],
                "hot_hitters": smoke.get("hot_hitters", []) or [],
                "cold_hitters": smoke.get("cold_hitters", []) or [],
                "debug": {"source": "smoke", "counts": smoke.get("debug", {}).get("counts", {})}
            }

        # --- 2) Try SMOKE samples fallback
        smoke_samples, status = _post_json(base, "/smoke_post",
                                           {"date": d, "samples": 3, "debug": req.debug})
        if smoke_samples and isinstance(smoke_samples, dict):
            return {
                "date": d,
                "counts": {
                    "matchups": len(smoke_samples.get("matchups", []) or []),
                    "hot_hitters": len(smoke_samples.get("hot_hitters", []) or []),
                    "cold_hitters": len(smoke_samples.get("cold_hitters", []) or []),
                },
                "matchups": smoke_samples.get("matchups", []) or [],
                "hot_hitters": smoke_samples.get("hot_hitters", []) or [],
                "cold_hitters": smoke_samples.get("cold_hitters", []) or [],
                "debug": {"source": "smoke(samples)", "counts": smoke_samples.get("debug", {}).get("counts", {})}
            }

        # --- 3) Compose from available parts
        # schedule: try diag, then schedule_post, then schedule_get
        sched, s1 = _post_json(base, "/diag_schedule_post", {"date": d})
        if not sched:
            sched, s2 = _post_json(base, "/schedule_post", {"date": d})
        if not sched:
            sched, s3 = _get_json(base, f"/schedule_get?date={d}")

        games = _extract_games(sched)

        # hot/cold helpers (best-effort)
        hot, _ = _post_json(base, "/hot_streak_hitters_post", {"date": d, "debug": req.debug})
        cold, _ = _post_json(base, "/cold_streak_hitters_post", {"date": d, "debug": req.debug})
        hot = hot or []
        cold = cold or []

        if games:
            return {
                "date": d,
                "counts": {
                    "matchups": len(games),
                    "hot_hitters": len(hot),
                    "cold_hitters": len(cold),
                },
                "matchups": games,
                "hot_hitters": hot,
                "cold_hitters": cold,
                "debug": {
                    "source": "composed",
                    "counts": {
                        "matchups": len(games),
                        "hot_hitters": len(hot),
                        "cold_hitters": len(cold),
                    }
                }
            }

        # record last error-ish info for 404 detail
        last_status = status
        last_detail = f"smoke_post failed for {d}; compose found 0 games"

    # Nothing worked for all dates tried
    raise HTTPException(
        status_code=404,
        detail={
            "message": "No data for requested or fallback dates",
            "dates_tried": dates_to_try,
            "last_status": last_status,
            "last_detail": last_detail
        },
    )
