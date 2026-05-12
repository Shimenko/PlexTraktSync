#!/usr/bin/env python3 -m pytest
from __future__ import annotations

from collections import defaultdict

import pytest
import trakt.core
from trakt.errors import OAuthException

from plextraktsync.queue.TraktScrobbleWorker import TraktScrobbleWorker
from plextraktsync.trakt.oauth import call_with_fresh_trakt_auth


class ScrobblerMock:
    def __init__(self, fail_times=0):
        self.calls = []
        self.fail_times = fail_times

    def update(self, progress):
        self.calls.append(("update", progress))
        if len(self.calls) <= self.fail_times:
            raise OAuthException()
        return "updated"

    def stop(self, progress):
        self.calls.append(("stop", progress))
        if len(self.calls) <= self.fail_times:
            raise OAuthException()
        return "stopped"


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

    assert scrobbler.calls == [("update", 20), ("update", 20)]
    assert other_scrobbler.calls == []
    assert cache_clear_calls == ["config", "api"]
    assert worker.oauth_unhealthy_until == 160.0
    assert queues["scrobble_update"] == [(scrobbler, 20), (other_scrobbler, 30)]
    assert queues["scrobble_stop"] == [(scrobbler, 90)]

    worker(queues)

    assert scrobbler.calls == [("update", 20), ("update", 20)]
    assert other_scrobbler.calls == []
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
