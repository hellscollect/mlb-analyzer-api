from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
import importlib
import os

router = APIRouter()

# Resolve provider from ENV (works with Utf8WrapperProvider too)
provider_path = os.getenv("MLB_PROVIDER", "providers.statsapi_provider:StatsApiProvider")
provider_module_name, provider_class_name = provider_path.split(":")
provider_module = importlib.import_module(provider_module_name)
provider_class = getattr(provider_module, provider_class_name)
provider = provider_class()

@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today"),
    min_season_avg: float = Query(0.26),
    last_n: int = Query(7),
    min_hitless_games: int = Query(3),
    limit: int = Query(30),
    verify: int = Query(1),
    debug: int = Query(0)
):
    """
    League-wide cold hitters with strict slump rules:
    - Count only AB>0, H==0
    - Only regular season
    - Filter by season AVG >= min_season_avg
    """
    try:
        data = provider.cold_candidates(
            date=date,
            min_season_avg=min_season_avg,
            last_n=last_n,
            min_hitless_games=min_hitless_games,
            limit=limit,
            verify=verify,
            debug=debug,
        )
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
