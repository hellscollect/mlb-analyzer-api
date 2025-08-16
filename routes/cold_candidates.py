from fastapi import APIRouter, Query
import importlib, os
from typing import Optional

router = APIRouter()

provider_path = os.getenv("MLB_PROVIDER", "providers.statsapi_provider:StatsApiProvider")
module_name, class_name = provider_path.split(":")
provider_module = importlib.import_module(module_name)
provider_class = getattr(provider_module, class_name)
provider = provider_class()

@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today"),
    names: Optional[str] = Query(None, description="Comma-separated player names to evaluate"),
    min_season_avg: float = Query(0.26),
    last_n: int = Query(7),
    min_hitless_games: int = Query(1),
    limit: int = Query(30),
    verify: int = Query(1),
    debug: int = Query(0),
):
    """
    Calls provider.cold_candidates(), which supports names= for targeted checks.
    """
    return provider.cold_candidates(
        date=date,
        names=names,
        min_season_avg=min_season_avg,
        last_n=last_n,
        min_hitless_games=min_hitless_games,
        limit=limit,
        verify=verify,
        debug=debug,
    )
