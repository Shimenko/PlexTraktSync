from __future__ import annotations

import logging
from threading import Lock
from time import monotonic, sleep
from urllib.parse import urlsplit

from requests.adapters import BaseAdapter, HTTPAdapter

from plextraktsync.config import TRAKT_RETRY_AFTER_MARGIN


class TraktRequestLimiter:
    DEFAULT_RETRY_AFTER = 60.0
    TRAKT_HOST = "api.trakt.tv"

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        get_delay: float,
        retry_after_margin: float = TRAKT_RETRY_AFTER_MARGIN,
        fallback_retry_after: float = DEFAULT_RETRY_AFTER,
        clock=monotonic,
        sleeper=sleep,
    ):
        if get_delay <= 0:
            raise ValueError(f"Trakt GET delay must be a positive number: {get_delay}")
        self.get_delay = float(get_delay)
        self.retry_after_margin = float(retry_after_margin)
        self.fallback_retry_after = float(fallback_retry_after)
        self.clock = clock
        self.sleeper = sleeper
        self.lock = Lock()
        self.next_get_at = 0.0
        self.backoff_until = 0.0

    def wait_for_request(self, method: str, url: str):
        if not self.is_trakt_url(url):
            return

        method = method.upper()
        while True:
            with self.lock:
                now = self.clock()
                wait_until = self.backoff_until
                if method == "GET":
                    wait_until = max(wait_until, self.next_get_at)

                remaining = wait_until - now
                if remaining <= 0:
                    if method == "GET":
                        self.next_get_at = now + self.get_delay
                    return

            self.logger.debug(f"Sleeping for {remaining:.3f} seconds before Trakt {method}")
            self.sleeper(remaining)

    def observe_response(self, method: str, url: str, response):
        if not self.is_trakt_url(url) or response.status_code != 429:
            return

        retry_after = self.parse_retry_after(response)
        seconds = retry_after + self.retry_after_margin
        with self.lock:
            self.backoff_until = max(self.backoff_until, self.clock() + seconds)

        parsed = urlsplit(url)
        self.logger.warning(
            "Trakt rate limit response: method=%s path=%s status=%s retry_after=%.1f backoff=%.1f",
            method.upper(),
            parsed.path,
            response.status_code,
            retry_after,
            seconds,
        )

    def parse_retry_after(self, response) -> float:
        retry_after = response.headers.get("Retry-After") or response.headers.get("retry-after")
        try:
            seconds = float(retry_after)
        except (TypeError, ValueError):
            return self.fallback_retry_after

        if seconds < 0:
            return self.fallback_retry_after
        return seconds

    @classmethod
    def is_trakt_url(cls, url: str):
        return urlsplit(url).hostname == cls.TRAKT_HOST


class TraktRateLimitedAdapter(BaseAdapter):
    def __init__(self, *args, trakt_request_limiter: TraktRequestLimiter, transport_adapter=None, **kwargs):
        super().__init__()
        self.trakt_request_limiter = trakt_request_limiter
        self.transport_adapter = transport_adapter or HTTPAdapter(*args, **kwargs)

    def send(self, request, **kwargs):
        self.trakt_request_limiter.wait_for_request(request.method, request.url)
        response = self.transport_adapter.send(request, **kwargs)
        self.trakt_request_limiter.observe_response(request.method, request.url, response)
        return response

    def close(self):
        self.transport_adapter.close()
