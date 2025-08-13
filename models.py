# models.py
from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel

class Hitter(BaseModel):
    player_id: str
    name: str
    team: str
    opponent_team: Optional[str] = None
    probable_pitcher_id: Optional[str] = None

    avg: float
    obp: Optional[float] = None
    slg: Optional[float] = None

    last_n_games: int = 0
    last_n_hits_each_game: List[int] = []
    last_n_hitless_games: int = 0

class Pitcher(BaseModel):
    player_id: str
    name: str
    team: str
    opponent_team: Optional[str] = None

    era: float
    kbb: Optional[float] = None
    k_per_start_last_n: List[int] = []
    runs_allowed_last_n: List[int] = []

    is_probable: bool = False
