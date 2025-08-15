# providers/statsapi_client.py
from __future__ import annotations

import time
import json
import random
from typing import Any, Dict, Optional, Tuple

import requests

BASE = "https://statsapi.mlb.com/api/v1"


class _TTLCache:
    """Lightweight in-memory TTL cache (thread-unsafe, fine for single-process web app)."""
    def __init__(self, ttl_seconds: int = 120, maxsize: int = 2048):
        self.ttl = ttl_seconds
        self.maxsize = maxsize
        self._store: Dict[str, Tuple[float, Any]] = {}

    def _evict_if_needed(self) -> None:
        if len(self._store) <= self.maxsize:
            return
        # Evict oldest N entries
        to_evict = len(self._store) - self.maxsize
        keys = sorted(self._store.keys(), key=lambda k: self._store[k][0])
        for k in keys[:to_evict]:
            self._store.pop(k, None)

    def get(self, key: str) -> Optional[Any]:
        rec = self._store.get(key)
        if not rec:
            return None
        ts, val = rec
        if (time.time() - ts) > self.ttl:
            self._store.pop(key, None)
            return None
        return val

    def set(self, key: str, val: Any) -> None:
        self._store[key] = (time.time(), val)
        self._evict_if_needed()


def _jsonable(v: Any) -> Any:
    """Make values JSONable for cache keys; dates -> isoformat, others -> str fallback."""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    # handle stdlib/date/datetime-like things
    iso = getattr(v, "isoformat", None)
    if callable(iso):
        try:
            return v.isoformat()
        except Exception:
            pass
    return str(v)


def _mk_key(path: str, params: Optional[Dict[str, Any]]) -> str:
    # canonicalize params for stable cache key — ensure all values are JSON-serializable
    p = params or {}
    p2 = {k: _jsonable(v) for k, v in p.items()}
    return json.dumps([path, sorted(p2.items(), key=lambda kv: kv[0])], separators=(",", ":"), sort_keys=False)


class StatsApiClient:
    """
    A small, resilient HTTP client for MLB StatsAPI with TTL caching + retries.
    - Sync (requests) for drop-in use.
    - Backoff on transient errors.
    - Per-request timeout.
    """

    def __init__(
        self,
        base_url: str = BASE,
        ttl_seconds: int = 120,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        self.base = base_url.rstrip("/")
        self.cache = _TTLCache(ttl_seconds=ttl_seconds)
        self.timeout = timeout
        self.max_retries = max_retries

    def _log(self, msg: str) -> None:
        print(f"[StatsApiClient] {msg}", flush=True)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, use_cache: bool = True) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        key = _mk_key(path, params)

        if use_cache:
            cached = self.cache.get(key)
            if cached is not None:
                self._log(f"CACHE HIT {url} params={params}")
                return cached

        # retry with decorrelated jitter backoff
        attempt = 0
        wait = 0.5
        while True:
            attempt += 1
            try:
                self._log(f"GET {url} params={params}")
                r = requests.get(url, params=params or {}, timeout=self.timeout)
                self._log(f"HTTP {r.status_code} for {url}")
                r.raise_for_status()
                data = r.json()
                if use_cache:
                    self.cache.set(key, data)
                return data
            except requests.RequestException as e:
                if attempt >= self.max_retries:
                    self._log(f"ERROR giving up after {attempt} attempts: {type(e).__name__}")
                    raise
                # jittered backoff
                sleep_for = wait + random.random() * 0.5 * wait
                self._log(f"Transient error ({type(e).__name__}). Retry {attempt}/{self.max_retries} in {sleep_for:.2f}s")
                time.sleep(sleep_for)
                wait = min(8.0, wait * 1.7)

    # Convenience wrappers (kept simple so provider code reads clearly)
    def schedule(self, date_str: str, hydrate: Optional[str] = None) -> Dict[str, Any]:
        params = {"date": str(date_str), "sportId": 1}
        if hydrate:
            params["hydrate"] = hydrate
        return self.get("/schedule", params)

    def team_roster(self, team_id: int, roster_type: str = "active") -> Dict[str, Any]:
        return self.get(f"/teams/{team_id}/roster", {"rosterType": roster_type})

    def player_stats(self, player_id: int, season: int, stat_type: str) -> Dict[str, Any]:
        return self.get(
            f"/people/{player_id}/stats",
            {"stats": stat_type, "group": "hitting", "season": int(season)}
        )

    def boxscore(self, game_pk: int) -> Dict[str, Any]:
        return self.get(f"/game/{int(game_pk)}/boxscore")
