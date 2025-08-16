from fastapi.responses import JSONResponse
from typing import Any, Dict
import importlib

# Minimal UTF-8 wrapper. No business logic here.
# If the inner provider lacks a method, we respond with a clear 501 JSON.

class Utf8WrapperProvider:
    def __init__(self):
        module = importlib.import_module("providers.statsapi_provider")
        provider_cls = getattr(module, "StatsApiProvider")
        self.provider = provider_cls()

    def __getattr__(self, name: str):
        # Try to proxy to inner provider
        try:
            orig_attr = getattr(self.provider, name)
        except AttributeError:
            # Return a callable that emits a clean 501 JSON error
            def missing_method(*args, **kwargs):
                return JSONResponse(
                    content={"error": f"Provider does not implement {name}()"},
                    status_code=501,
                    media_type="application/json; charset=utf-8",
                )
            return missing_method

        if callable(orig_attr):
            def wrapper(*args, **kwargs):
                try:
                    result = orig_attr(*args, **kwargs)
                    # Standardize to UTF-8 JSON responses
                    if isinstance(result, JSONResponse):
                        return result  # already a Response
                    return JSONResponse(
                        content=result,
                        media_type="application/json; charset=utf-8"
                    )
                except Exception as e:
                    return JSONResponse(
                        content={"error": f"{type(e).__name__}: {e}"},
                        status_code=500,
                        media_type="application/json; charset=utf-8",
                    )
            return wrapper
        else:
            return orig_attr
