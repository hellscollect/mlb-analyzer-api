# routes/cold_candidates.py
from fastapi import APIRouter, Query
import importlib, os

router = APIRouter()

provider_path = os.getenv("MLB_PROVIDER", "providers.statsapi_provider:StatsApiProvider")
provider_module_name, provider_class_name = provider_path.split(":")
provider_module = importlib.import_module(provider_module_name)
provider_class = getattr(provider_module, provider_class_name)
provider = provider_class()

@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="today|yesterday|tomorrow|YYYY-MM-DD"),
    names: str = Query("", description="Comma-separated list of player names"),
    min_season_avg: float = Query(0.26),
    last_n: int = Query(7),
    min_hitless_games: int = Query(1),
    limit: int = Query(30),
    verify: int = Query(1),
    debug: int = Query(0),
):
    name_list = [n.strip() for n in names.split(",") if n.strip()] if names else []
    # Utf8WrapperProvider (if configured) will wrap this in a UTF-8 JSON response
    return provider.cold_candidates(
        date=date,
        names=name_list,
        min_season_avg=min_season_avg,
        last_n=last_n,
        min_hitless_games=min_hitless_games,
        limit=limit,
        verify=bool(verify),
        debug=bool(debug),
    )
