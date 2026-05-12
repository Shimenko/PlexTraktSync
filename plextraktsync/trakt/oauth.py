from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import trakt.core
from trakt.errors import OAuthException

from plextraktsync.factory import logging

P = ParamSpec("P")
T = TypeVar("T")

logger = logging.getLogger(__name__)


def reset_trakt_auth_cache():
    trakt.core.config.cache_clear()
    trakt.core.api.cache_clear()


def call_with_fresh_trakt_auth(fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    try:
        return fn(*args, **kwargs)
    except OAuthException:
        logger.warning("Trakt OAuth failed; reloading auth cache and retrying once")
        reset_trakt_auth_cache()
        return fn(*args, **kwargs)


def retry_trakt_oauth(fn: Callable[P, T]) -> Callable[P, T]:
    @wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        return call_with_fresh_trakt_auth(fn, *args, **kwargs)

    return wrapper
