from fastapi import APIRouter, Query, Request

router = APIRouter()

@router.get("/cold_candidates")
def cold_candidates(
    request: Request,
    date: str = Query("today"),
    min_season_avg: float = Query(0.26),
    last_n: int = Query(7),
    min_hitless_games: int = Query(3),
    limit: int = Query(30),
    verify: int = Query(1),
    debug: int = Query(0),
):
    """
    Thin route: call provider.cold_candidates().
    Utf8WrapperProvider guarantees UTF-8 responses and returns a 501 JSON
    if the inner provider lacks the method.
    """
    provider = getattr(request.app.state, "provider", None)
    if provider is None:
        return {"error": "Provider not loaded"}
    return provider.cold_candidates(
        date=date,
        min_season_avg=min_season_avg,
        last_n=last_n,
        min_hitless_games=min_hitless_games,
        limit=limit,
        verify=verify,
        debug=debug,
    )
