#!/usr/bin/env python3 -m pytest
from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace

from requests import Response
from requests.adapters import HTTPAdapter
from requests_cache import CachedSession
from urllib3.response import HTTPResponse

from plextraktsync.trakt.TraktRateLimitedAdapter import TraktRateLimitedAdapter, TraktRequestLimiter


class FakeClock:
    def __init__(self):
        self.current = 0.0
        self.sleeps = []

    def __call__(self):
        return self.current

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.current += seconds


class StaticAdapter(HTTPAdapter):
    def __init__(self, responses=None):
        super().__init__()
        self.responses = list(responses or [(200, {})])
        self.calls = 0

    def send(self, request, **kwargs):
        self.calls += 1
        if len(self.responses) > 1:
            status_code, headers = self.responses.pop(0)
        else:
            status_code, headers = self.responses[0]

        response = Response()
        response.status_code = status_code
        response.headers.update(headers)
        response.url = request.url
        response.request = request
        response._content = b"{}"
        response.raw = HTTPResponse(
            body=BytesIO(response.content),
            headers=response.headers,
            status=status_code,
            reason="OK",
            preload_content=False,
            request_method=request.method,
            request_url=request.url,
        )
        return response


def make_session(limiter: TraktRequestLimiter, adapter: StaticAdapter):
    session = CachedSession(cache_name="test", backend="memory")
    session.mount(
        "https://api.trakt.tv/",
        TraktRateLimitedAdapter(
            trakt_request_limiter=limiter,
            transport_adapter=adapter,
        ),
    )
    session.mount("https://example.com/", adapter)
    return session


def make_limiter(clock: FakeClock, get_delay=1.0, retry_after_margin=0.5, fallback_retry_after=60.0):
    return TraktRequestLimiter(
        get_delay=get_delay,
        retry_after_margin=retry_after_margin,
        fallback_retry_after=fallback_retry_after,
        clock=clock,
        sleeper=clock.sleep,
    )


def test_live_trakt_gets_are_spaced():
    clock = FakeClock()
    adapter = StaticAdapter()
    session = make_session(make_limiter(clock), adapter)

    session.get("https://api.trakt.tv/search/tmdb/1?type=movie")
    session.get("https://api.trakt.tv/search/tmdb/2?type=movie")

    assert adapter.calls == 2
    assert clock.sleeps == [1.0]


def test_cached_trakt_get_does_not_sleep():
    clock = FakeClock()
    adapter = StaticAdapter()
    session = make_session(make_limiter(clock), adapter)

    url = "https://api.trakt.tv/search/tmdb/1?type=movie"
    session.get(url)
    session.get(url)

    assert adapter.calls == 1
    assert clock.sleeps == []


def test_non_trakt_urls_are_not_spaced():
    clock = FakeClock()
    adapter = StaticAdapter()
    session = make_session(make_limiter(clock), adapter)

    session.get("https://example.com/one")
    session.get("https://example.com/two")

    assert adapter.calls == 2
    assert clock.sleeps == []


def test_trakt_writes_are_not_get_spaced():
    clock = FakeClock()
    adapter = StaticAdapter()
    session = make_session(make_limiter(clock), adapter)

    session.post("https://api.trakt.tv/scrobble/start", json={})
    session.post("https://api.trakt.tv/scrobble/pause", json={})

    assert adapter.calls == 2
    assert clock.sleeps == []


def test_trakt_rate_limit_response_sets_backoff():
    clock = FakeClock()
    adapter = StaticAdapter([(429, {"Retry-After": "3"}), (200, {})])
    session = make_session(make_limiter(clock, retry_after_margin=0.5), adapter)

    session.get("https://api.trakt.tv/search/tmdb/1?type=movie")
    session.get("https://api.trakt.tv/search/tmdb/2?type=movie")

    assert adapter.calls == 2
    assert clock.sleeps == [3.5]


def test_invalid_retry_after_uses_fallback_backoff():
    clock = FakeClock()
    limiter = make_limiter(clock, retry_after_margin=0.5, fallback_retry_after=12.0)
    response = SimpleNamespace(status_code=429, headers={})

    limiter.observe_response("GET", "https://api.trakt.tv/search/tmdb/1?type=movie", response)
    limiter.wait_for_request("POST", "https://api.trakt.tv/scrobble/start")

    assert clock.sleeps == [12.5]
