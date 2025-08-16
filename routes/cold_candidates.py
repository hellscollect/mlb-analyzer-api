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
    Calls inner provider even when wrapped by Utf8WrapperProvider.
    Returns plain Python data so your app-wide UTF-8 response class applies.
    """
    prov = getattr(request.app.state, "provider", None)
    if prov is None:
        return {"error": "Provider not loaded"}

    # If we’re using Utf8WrapperProvider, it stores the real provider at .provider
    # Unwrap if needed so we can call methods not exposed by the wrapper.
    inner = getattr(prov, "provider", prov)

    if not hasattr(inner, "cold_candidates"):
        return {"error": "Provider has no cold_candidates(); deploy updated providers/statsapi_provider.py"}

    return inner.cold_candidates(
        date=date,
        min_season_avg=min_season_avg,
        last_n=last_n,
        min_hitless_games=min_hitless_games,
        limit=limit,
        verify=verify,
        debug=debug,
    )
