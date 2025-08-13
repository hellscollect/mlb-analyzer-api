import httpx
from datetime import date as date_cls
from typing import Any, Dict, List, Optional


class StatsApiProvider:
    """
    Provider for MLB StatsAPI data.
    """

    def __init__(self, data_api_base: str = "https://statsapi.mlb.com", api_key: Optional[str] = None):
        self.base = data_api_base.rstrip("/")
        self.key = api_key
        self._timeout = httpx.Timeout(10.0)
        self._limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)

    # ------------- HTTP helpers -------------

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        headers = {}
        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"  # In case your API key is needed
        with httpx.Client(timeout=self._timeout, limits=self._limits, headers=headers) as client:
            r = client.get(url, params=params)
            # Treat 400/404 as "no data" for schedule-like reads to avoid blowing up
            if r.status_code in (400, 404):
                return {}
            r.raise_for_status()
            return r.json()

    # ------------- Core fetches -------------

    def _teams_playing_on(self, d: date_cls) -> List[Dict[str, Any]]:
        # MLB StatsAPI schedule needs sportId=1 for MLB
        try:
            sch = self._get("/api/v1/schedule", {"date": d.isoformat(), "sportId": 1})
        except httpx.HTTPError:
            return []
        dates = sch.get("dates") or []
        if not dates:
            return []
        games = []
        for date_block in dates:
            games.extend(date_block.get("games", []) or [])
        teams = []
        for g in games:
            for side in ("home", "away"):
                t = g.get(f"{side}Team") or {}
                if t:
                    teams.append({"id": t.get("id"), "name": t.get("name")})
        # De-dup by team ID
        uniq = {}
        for t in teams:
            if t.get("id"):
                uniq[t["id"]] = t
        return list(uniq.values())

    # Example public method — you'll have your own methods like fetch_hitters, fetch_pitchers, etc.
    def example_fetch(self, d: date_cls) -> Dict[str, Any]:
        teams = self._teams_playing_on(d)
        return {"teams": teams, "count": len(teams)}
