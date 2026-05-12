#!/usr/bin/env python3 -m pytest
from __future__ import annotations

from collections import defaultdict

import pytest
import trakt.core
from trakt.errors import OAuthException, RateLimitException

import plextraktsync.decorators.time_limit as time_limit_decorator
from plextraktsync.config import TRAKT_RETRY_AFTER_MARGIN
from plextraktsync.queue.TraktScrobbleWorker import TraktScrobbleWorker
from plextraktsync.trakt.oauth import call_with_fresh_trakt_auth


class ScrobblerMock:
    def __init__(self, fail_times=0, exception_factory=None):
        self.calls = []
        self.fail_times = fail_times
        self.exception_factory = exception_factory or OAuthException

    def update(self, progress):
        self.calls.append(("update", progress))
        if len(self.calls) <= self.fail_times:
            raise self.exception_factory()
        return "updated"

    def stop(self, progress):
        self.calls.append(("stop", progress))
        if len(self.calls) <= self.fail_times:
            raise self.exception_factory()
        return "stopped"


class ResponseMock:
    def __init__(self, retry_after=60):
        self.headers = {"retry-after": str(retry_after), "x-ratelimit": "{}"}


def rate_limit_exception(retry_after=60):
    return RateLimitException(ResponseMock(retry_after))


@pytest.fixture(autouse=True)
def no_time_limit(monkeypatch):
    monkeypatch.setattr(time_limit_decorator.timer, "wait_if_needed", lambda: None)


def patch_auth_cache_clear(monkeypatch):
    calls = []

    def clear_config():
        calls.append("config")

    def clear_api():
        calls.append("api")

    monkeypatch.setattr(trakt.core.config, "cache_clear", clear_config)
    monkeypatch.setattr(trakt.core.api, "cache_clear", clear_api)
    return calls


def test_call_with_fresh_trakt_auth_retries_once_after_oauth(monkeypatch):
    cache_clear_calls = patch_auth_cache_clear(monkeypatch)
    calls = []

    def call():
        calls.append("call")
        if len(calls) == 1:
            raise OAuthException()
        return "ok"

    assert call_with_fresh_trakt_auth(call) == "ok"
    assert calls == ["call", "call"]
    assert cache_clear_calls == ["config", "api"]


def test_call_with_fresh_trakt_auth_reraises_second_oauth_failure(monkeypatch):
    cache_clear_calls = patch_auth_cache_clear(monkeypatch)
    calls = []

    def call():
        calls.append("call")
        raise OAuthException()

    with pytest.raises(OAuthException):
        call_with_fresh_trakt_auth(call)

    assert calls == ["call", "call"]
    assert cache_clear_calls == ["config", "api"]


def test_scrobble_worker_clears_queue_after_successful_oauth_retry(monkeypatch):
    cache_clear_calls = patch_auth_cache_clear(monkeypatch)
    scrobbler = ScrobblerMock(fail_times=1)
    worker = TraktScrobbleWorker()
    queues = defaultdict(list)
    queues["scrobble_update"] = [(scrobbler, 42)]

    worker(queues)

    assert scrobbler.calls == [("update", 42), ("update", 42)]
    assert cache_clear_calls == ["config", "api"]
    assert queues["scrobble_update"] == []


def test_scrobble_worker_backs_off_and_coalesces_after_oauth_retry_fails(monkeypatch):
    cache_clear_calls = patch_auth_cache_clear(monkeypatch)
    scrobbler = ScrobblerMock(fail_times=10)
    other_scrobbler = ScrobblerMock()
    worker = TraktScrobbleWorker(oauth_backoff_seconds=60)
    worker.now = lambda: 100.0
    queues = defaultdict(list)
    queues["scrobble_update"] = [
        (scrobbler, 10),
        (scrobbler, 20),
        (other_scrobbler, 30),
    ]
    queues["scrobble_stop"] = [
        (scrobbler, 80),
        (scrobbler, 90),
    ]

    worker(queues)

    assert scrobbler.calls == [("stop", 90), ("stop", 90)]
    assert other_scrobbler.calls == [("update", 30)]
    assert cache_clear_calls == ["config", "api"]
    assert worker.oauth_unhealthy_until == 160.0
    assert queues["scrobble_update"] == []
    assert queues["scrobble_stop"] == [(scrobbler, 90)]

    worker(queues)

    assert scrobbler.calls == [("stop", 90), ("stop", 90)]
    assert other_scrobbler.calls == [("update", 30)]
    assert cache_clear_calls == ["config", "api"]


def test_scrobble_worker_keeps_only_unsent_items_after_partial_oauth_failure(monkeypatch):
    patch_auth_cache_clear(monkeypatch)
    sent_scrobbler = ScrobblerMock()
    failing_scrobbler = ScrobblerMock(fail_times=10)
    worker = TraktScrobbleWorker(oauth_backoff_seconds=60)
    worker.now = lambda: 100.0
    queues = defaultdict(list)
    queues["scrobble_update"] = [
        (sent_scrobbler, 10),
        (failing_scrobbler, 20),
    ]

    worker(queues)

    assert sent_scrobbler.calls == [("update", 10)]
    assert failing_scrobbler.calls == [("update", 20), ("update", 20)]
    assert queues["scrobble_update"] == [(failing_scrobbler, 20)]
    assert worker.oauth_unhealthy_until == 160.0


def test_scrobble_worker_throttles_periodic_updates():
    scrobbler = ScrobblerMock()
    now = [100.0]
    worker = TraktScrobbleWorker(scrobble_update_interval=300)
    worker.now = lambda: now[0]
    queues = defaultdict(list)

    queues["scrobble_update"] = [(scrobbler, 10)]
    worker(queues)

    queues["scrobble_update"] = [(scrobbler, 20)]
    now[0] = 200.0
    worker(queues)

    queues["scrobble_update"] = [(scrobbler, 30)]
    now[0] = 400.0
    worker(queues)

    assert scrobbler.calls == [("update", 10), ("update", 30)]
    assert queues["scrobble_update"] == []


def test_scrobble_worker_allows_update_when_progress_rolls_back():
    scrobbler = ScrobblerMock()
    now = [100.0]
    worker = TraktScrobbleWorker(scrobble_update_interval=300)
    worker.now = lambda: now[0]
    queues = defaultdict(list)

    queues["scrobble_update"] = [(scrobbler, 50)]
    worker(queues)

    queues["scrobble_update"] = [(scrobbler, 5)]
    now[0] = 120.0
    worker(queues)

    assert scrobbler.calls == [("update", 50), ("update", 5)]


def test_scrobble_worker_stop_supersedes_pending_update():
    scrobbler = ScrobblerMock()
    worker = TraktScrobbleWorker()
    queues = defaultdict(list)
    queues["scrobble_update"] = [(scrobbler, 10), (scrobbler, 20)]
    queues["scrobble_stop"] = [(scrobbler, 80)]

    worker(queues)

    assert scrobbler.calls == [("stop", 80)]
    assert queues["scrobble_update"] == []
    assert queues["scrobble_stop"] == []


def test_scrobble_worker_backs_off_and_keeps_unsent_items_after_rate_limit():
    rate_limited_scrobbler = ScrobblerMock(fail_times=1, exception_factory=lambda: rate_limit_exception(60))
    other_scrobbler = ScrobblerMock()
    now = [100.0]
    worker = TraktScrobbleWorker()
    worker.now = lambda: now[0]
    queues = defaultdict(list)
    queues["scrobble_update"] = [
        (rate_limited_scrobbler, 10),
        (other_scrobbler, 20),
    ]

    worker(queues)

    assert rate_limited_scrobbler.calls == [("update", 10)]
    assert other_scrobbler.calls == []
    assert worker.rate_limit_unhealthy_until == pytest.approx(160.0 + TRAKT_RETRY_AFTER_MARGIN)
    assert queues["scrobble_update"] == [(rate_limited_scrobbler, 10), (other_scrobbler, 20)]

    now[0] = 120.0
    worker(queues)

    assert rate_limited_scrobbler.calls == [("update", 10)]
    assert other_scrobbler.calls == []


def test_scrobble_worker_keeps_stop_after_rate_limit_and_retries_after_backoff():
    scrobbler = ScrobblerMock(fail_times=1, exception_factory=lambda: rate_limit_exception(60))
    now = [100.0]
    worker = TraktScrobbleWorker()
    worker.now = lambda: now[0]
    queues = defaultdict(list)
    queues["scrobble_stop"] = [(scrobbler, 90)]

    worker(queues)

    assert scrobbler.calls == [("stop", 90)]
    assert queues["scrobble_stop"] == [(scrobbler, 90)]

    now[0] = 161.0
    worker(queues)

    assert scrobbler.calls == [("stop", 90), ("stop", 90)]
    assert queues["scrobble_stop"] == []
