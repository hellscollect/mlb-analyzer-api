# providers/utf8_wrapper_provider.py
from typing import Any, Callable
from datetime import date as date_cls

def _fix_text(s: Any) -> Any:
    if not isinstance(s, str):
        return s
    # Fix the classic "latin1 read as utf-8" artifacts like "AgustÃ­n", "JesÃºs"
    if "Ã" in s or "Â" in s:
        try:
            return s.encode("latin1").decode("utf-8")
        except Exception:
            return s
    return s

def _deep_fix(obj: Any) -> Any:
    if isinstance(obj, dict):
        return { _fix_text(k): _deep_fix(v) for k, v in obj.items() }
    if isinstance(obj, list):
        return [ _deep_fix(x) for x in obj ]
    if isinstance(obj, str):
        return _fix_text(obj)
    return obj

class Utf8WrapperProvider:
    """
    Wrapper that delegates to your real provider (StatsApiProvider by default)
    and returns a UTF-8-cleaned result for every method.
    """
    def __init__(self, inner: Any = None):
        if inner is None:
            from providers.statsapi_provider import StatsApiProvider
            inner = StatsApiProvider()
        self._inner = inner

        # bubble up common attributes if callers introspect them
        for attr in ("base", "key"):
            if hasattr(inner, attr):
                setattr(self, attr, getattr(inner, attr))

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if callable(attr):
            def wrapped(*args, **kwargs):
                # If a method expects date_str and caller gave date (date obj),
                # provide both to maximize compatibility. Safe no-op if unused.
                if "date" in kwargs and isinstance(kwargs["date"], date_cls):
                    kwargs.setdefault("date_str", kwargs["date"].isoformat())

                # Prefer top_n if both exist
                if "limit" in kwargs and "top_n" not in kwargs:
                    kwargs["top_n"] = kwargs["limit"]

                res = attr(*args, **kwargs)
                return _deep_fix(res)
            wrapped.__name__ = getattr(attr, "__name__", name)
            return wrapped
        return attr
