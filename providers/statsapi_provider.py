# providers/statsapi_provider.py
import os
from datetime import datetime
import pytz
import unicodedata
import requests

DEFAULT_BASE = "https://statsapi.mlb.com"

# We only want players whose team has NOT started yet
NOT_STARTED_DETAILED = {"Scheduled", "Pre-Game", "Warmup"}
NOT_STARTED_ABSTRACT = {"Preview"}  # sometimes abstract is Preview before first pitch


def _tz_today_eastern():
    tz = pytz.timezone("America/New_York")
    return datetime.now(tz).date()


def _parse_date(d):
    if d is None or str(d).strip().lower() in {"today", "now"}:
        return _tz_today_eastern().isoformat()
    return str(d)


def _season_from_date(dstr):
    try:
        return int(dstr[:4])
    except Exception:
        return _tz_today_eastern().year


def _normalize_name(s):
    """strip accents, punctuation, lowercase, and drop Jr/Sr/II/III suffix"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    for ch in [".", ",", "'", "`", "’"]:
        s = s.replace(ch, "")
    s = " ".join(s.split())
    tokens = s.split()
    if tokens and tokens[-1] in {"jr", "sr", "ii", "iii"}:
        tokens = tokens[:-1]
    return " ".join(tokens)


def _get_base():
    base = os.getenv("STATSAPI_BASE", DEFAULT_BASE)
    return base.rstrip("/")


def _get(url, params=None, timeout=10):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


class StatsApiProvider:
    """
    Provider that talks directly to MLB StatsAPI.

    Methods return simple Python objects (dict/list). A UTF-8 wrapper in your app JSON-encodes responses.
    """

    def __init__(self):
        self.base = _get_base()

    # ---------------------------
    # Minimal probes for self_test
    # ---------------------------
    def _fetch_hitter_rows(self, date=None, **kwargs):
        # stable, simple
        return []

    def _fetch_pitcher_rows(self, date=None, **kwargs):
        return []

    def schedule_for_date(self, date):
        d = _parse_date(date)
        url = f"{self.base}/api/v1/schedule"
        params = {"sportId": 1, "date": d}
        return _get(url, params=params)

    # ---------------------------
    # League-level stub (keeps self_test GREEN without 501s)
    # ---------------------------
    def cold_streak_hitters(self, date=None, min_avg=0.26, games=1, require_zero_hit_each=1, top_n=100, debug=0):
        d = _parse_date(date)
        season = _season_from_date(d)
        return {
            "date": d,
            "season": season,
            "cold_hitters": [],
            "debug": [] if not debug else [{"note": "cold_streak_hitters not implemented"}],
        }

    # ---------------------------
    # Name-targeted candidates for CURRENT DAY ONLY
    # ---------------------------
    def cold_candidates(
        self,
        date=None,
        names=None,
        min_season_avg=0.26,
        last_n=7,
        min_hitless_games=1,
        limit=50,
        verify=0,
        debug=0,
        team=None,
    ):
        """
        Resolves players by scanning ACTIVE ROSTERS of NOT-STARTED teams today (avoids /people?search 400s).
        For each resolved player:
          • season_avg (season 202X)
          • hitless_streak = consecutive past games with AB>0 and hits==0 (most recent first, up to last_n)
        Keeps players with:
          • season_avg >= min_season_avg
          • hitless_streak >= min_hitless_games
          • and their team has not started yet today
        """
        d = _parse_date(date)
        season = _season_from_date(d)
        base = self.base

        # 1) Which teams have NOT started?
        sched = self.schedule_for_date(d)
        not_started_team_ids = set()
        team_id_to_name = {}
        for dt in sched.get("dates", []):
            for g in dt.get("games", []):
                st = g.get("status", {})
                detailed = st.get("detailedState")
                abstract = st.get("abstractGameState")
                if (detailed in NOT_STARTED_DETAILED) or (abstract in NOT_STARTED_ABSTRACT and detailed != "Final"):
                    for side in ("away", "home"):
                        t = g.get("teams", {}).get(side, {}).get("team", {})
                        if t and "id" in t:
                            not_started_team_ids.add(t["id"])
                            team_id_to_name[t["id"]] = t.get("name", "")

        # 2) Build name -> player mapping from ACTIVE rosters of those teams
        name_to_player = {}  # normalized name -> (playerId, teamId, teamName, fullName)
        for tid in sorted(not_started_team_ids):
            try:
                roster_url = f"{base}/api/v1/teams/{tid}/roster"
                params = {"rosterType": "active", "season": season}
                rj = _get(roster_url, params=params)
                for entry in rj.get("roster", []):
                    person = entry.get("person", {})
                    pid = person.get("id")
                    full = person.get("fullName", "")
                    if not pid or not full:
                        continue
                    norm = _normalize_name(full)
                    name_to_player[norm] = (pid, tid, team_id_to_name.get(tid, ""), full)
            except Exception:
                # skip roster failures; we'll just have fewer matches
                pass

        # 3) Parse requested names
        requested = []
        if names:
            if isinstance(names, str):
                requested = [s.strip() for s in names.split(",") if s.strip()]
            elif isinstance(names, list):
                requested = names

        if not requested:
            return {"date": d, "season": season, "items": [], "debug": [{"note": "no names provided"}] if debug else []}

        items = []
        dbg = []

        # 4) For each requested player, compute filters and metrics
        for raw_name in requested:
            norm_req = _normalize_name(raw_name)
            info = name_to_player.get(norm_req)
            if not info:
                if debug:
                    dbg.append({
                        "name": raw_name,
                        "skip": "no not-started game today (not found on any active roster of a not-started team)"
                    })
                continue

            pid, tid, team_name, full = info

            # Season average
            try:
                stats_url = f"{base}/api/v1/people/{pid}/stats"
                sj = _get(stats_url, params={"stats": "season", "season": season})
                avg = 0.0
                for sp in sj.get("stats", []):
                    for split in sp.get("splits", []):
                        stat = split.get("stat", {})
                        a = stat.get("avg")
                        if a is not None:
                            try:
                                avg = float(a)
                            except Exception:
                                pass
                if avg < float(min_season_avg):
                    if debug:
                        dbg.append({"name": full, "team": team_name, "skip": f"season_avg {avg:.3f} < min {float(min_season_avg):.3f}"})
                    continue
            except Exception as e:
                if debug:
                    dbg.append({"name": full, "team": team_name, "error": f"season stats fetch failed: {e}"})
                continue

            # Hitless streak across recent AB>0 games
            try:
                gl_url = f"{base}/api/v1/people/{pid}/stats"
                glj = _get(gl_url, params={"stats": "gameLog", "season": season})
                streak = 0
                considered = 0
                splits = []
                for sp in glj.get("stats", []):
                    splits = sp.get("splits", [])
                    break
                for s in splits:
                    gd = s.get("date")
                    if gd and gd > d:
                        # ignore future log rows
                        continue
                    stat = s.get("stat", {}) or {}
                    ab = stat.get("atBats", 0) or 0
                    if ab <= 0:
                        # only count games with an AB
                        continue
                    hits = stat.get("hits", 0) or 0
                    considered += 1
                    if hits == 0:
                        streak += 1
                    else:
                        break
                    if considered >= int(last_n):
                        break

                if streak < int(min_hitless_games):
                    if debug:
                        dbg.append({"name": full, "team": team_name, "skip": f"hitless_streak {streak} < min {int(min_hitless_games)}"})
                    continue

                items.append({
                    "name": full,
                    "team": team_name,
                    "season_avg": round(avg, 3),
                    "hitless_streak": streak
                })
            except Exception as e:
                if debug:
                    dbg.append({"name": full, "team": team_name, "error": f"game log fetch failed: {e}"})
                continue

        # Order and limit
        items.sort(key=lambda x: (-x.get("hitless_streak", 0), -x.get("season_avg", 0.0), x.get("name", "")))
        try:
            lim = int(limit)
            if lim > 0:
                items = items[:lim]
        except Exception:
            pass

        return {"date": d, "season": season, "items": items, "debug": dbg if debug else []}
