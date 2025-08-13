from datetime import date as _date
from typing import Dict, List
# Import the exact Pydantic models your API uses
from main import Hitter, Pitcher

class SimpleProvider:
    """
    Example provider with synthetic data for any given date.
    It returns Hitter and Pitcher *objects* (not dicts), matching main.py v1.6.3 expectations.
    """

    def __init__(self):
        # keyed by ISO date string
        self._pitchers_by_date: Dict[str, List[Pitcher]] = {}
        self._hitters_by_date: Dict[str, List[Hitter]] = {}

    def _ensure_seed(self, d: _date):
        key = d.isoformat()
        if key in self._pitchers_by_date:
            return

        # ---- Pitchers (two hot, two cold) ----
        pitchers = [
            Pitcher(
                player_id="pit-bos-01",
                name="Carl Flame",
                team="BOS",
                opponent_team="NYY",
                era=3.20,
                kbb=4.5,
                k_per_start_last_n=[7, 8, 6],
                runs_allowed_last_n=[2, 1, 2],
                is_probable=True
            ),
            Pitcher(
                player_id="pit-nyy-02",
                name="Nate Ice",
                team="NYY",
                opponent_team="BOS",
                era=5.05,
                kbb=1.9,
                k_per_start_last_n=[5, 4, 3],
                runs_allowed_last_n=[4, 3, 5],
                is_probable=True
            ),
            Pitcher(
                player_id="pit-lad-03",
                name="Leo Heat",
                team="LAD",
                opponent_team="SFG",
                era=2.95,
                kbb=5.2,
                k_per_start_last_n=[9, 7, 8],
                runs_allowed_last_n=[1, 2, 0],
                is_probable=True
            ),
            Pitcher(
                player_id="pit-sfg-04",
                name="Sam Slump",
                team="SFG",
                opponent_team="LAD",
                era=4.90,
                kbb=2.1,
                k_per_start_last_n=[4, 5, 5],
                runs_allowed_last_n=[3, 4, 3],
                is_probable=True
            ),
        ]

        # ---- Hitters (two hot, two cold) ----
        hitters = [
            Hitter(
                player_id="hit-nyy-11",
                name="Johnny Rake",
                team="NYY",
                opponent_team="BOS",
                probable_pitcher_id=None,
                avg=0.305,
                obp=0.370,
                slg=0.510,
                last_n_games=5,
                last_n_hits_each_game=[2, 1, 3],  # last 3 games each ≥ 1 hit
                last_n_hitless_games=0
            ),
            Hitter(
                player_id="hit-bos-12",
                name="Mike Freeze",
                team="BOS",
                opponent_team="NYY",
                probable_pitcher_id="pit-nyy-02",  # cold NYY probable
                avg=0.280,
                obp=0.320,
                slg=0.390,
                last_n_games=3,
                last_n_hits_each_game=[0, 0, 1],
                last_n_hitless_games=2
            ),
            Hitter(
                player_id="hit-lad-13",
                name="Alonzo Torch",
                team="LAD",
                opponent_team="SFG",
                probable_pitcher_id=None,  # infer cold SFG probable from team
                avg=0.315,
                obp=0.380,
                slg=0.560,
                last_n_games=4,
                last_n_hits_each_game=[1, 2, 1],
                last_n_hitless_games=0
            ),
            Hitter(
                player_id="hit-sfg-14",
                name="Rick Quiet",
                team="SFG",
                opponent_team="LAD",
                probable_pitcher_id="pit-lad-03",  # hot LAD probable
                avg=0.276,
                obp=0.310,
                slg=0.360,
                last_n_games=3,
                last_n_hits_each_game=[0, 0, 1],
                last_n_hitless_games=2
            ),
        ]

        self._pitchers_by_date[key] = pitchers
        self._hitters_by_date[key] = hitters

    # === Required by main.py interface ===
    def get_hitters(self, game_date: _date) -> List[Hitter]:
        self._ensure_seed(game_date)
        return self._hitters_by_date[game_date.isoformat()]

    def get_pitchers(self, game_date: _date) -> List[Pitcher]:
        self._ensure_seed(game_date)
        return self._pitchers_by_date[game_date.isoformat()]

    # === Optional but used for better matchup inference ===
    def get_probable_pitchers_by_team(self, game_date: _date) -> Dict[str, Pitcher]:
        self._ensure_seed(game_date)
        return {p.team: p for p in self._pitchers_by_date[game_date.isoformat()] if p.is_probable}
