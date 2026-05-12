from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TraktRateLimitConfig:
    """
    Trakt API rate limit settings.
    """

    get_delay: float = 1.0

    def __post_init__(self):
        if self.get_delay <= 0:
            raise ValueError(f"trakt.rate_limit.get_delay must be a positive number: {self.get_delay}")
