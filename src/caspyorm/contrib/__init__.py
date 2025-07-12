"""
Optional integrations for CaspyORM.

This module contains integrations with third-party frameworks
like FastAPI.
"""

try:
    from .fastapi import (
        as_response_model,
        as_response_models,
        create_response_model,
        get_async_session,
        get_session,
        handle_caspyorm_errors,
    )

    __all__ = [
        "get_session",
        "get_async_session",
        "as_response_model",
        "as_response_models",
        "create_response_model",
        "handle_caspyorm_errors",
    ]
except ImportError:
    __all__ = []
