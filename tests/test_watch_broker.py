#!/usr/bin/env python3 -m pytest
from __future__ import annotations

import pytest

from plextraktsync.watch.broker import WatchBroker
from plextraktsync.watch.events import Error, PlaySessionStateNotification
from tests.conftest import make


class FakePlex:
    def __init__(self, username="David", sessions=None, has_sessions=True):
        self.account = make("Account", username=username)
        self.session_entries = sessions or []
        self._has_sessions = has_sessions

    @property
    def sessions(self):
        return self.session_entries

    def has_sessions(self):
        return self._has_sessions


class FakeUpdater:
    def __init__(self):
        self.events = []
        self.errors = []

    def on_start(self, event):
        self.started = event

    def on_error(self, error):
        self.errors.append(error)

    def on_play(self, event):
        self.events.append(event)


def session(session_key="1", username="David"):
    return make("Session", sessionKey=session_key, usernames=[username])


def playback_event(state="playing", session_key="1"):
    return PlaySessionStateNotification(
        key="/library/metadata/999",
        viewOffset=600000,
        state=state,
        sessionKey=session_key,
        clientIdentifier="plex-web-client-id",
    )


def broker_for(plex: FakePlex, updater: FakeUpdater | None = None, owner_scrobble=True):
    return WatchBroker(
        plex=plex,
        updater=updater or FakeUpdater(),
        config={
            "watch_broker": {
                "owner_scrobble": owner_scrobble,
            },
        },
    )


def test_watch_broker_routes_owner_playback_to_updater():
    updater = FakeUpdater()
    broker = broker_for(FakePlex(sessions=[session(username="David")]), updater)

    event = playback_event("playing")
    broker.on_play(event)

    assert updater.events == [event]
    assert "1" in broker.session_owners


def test_watch_broker_suppresses_owner_scrobble_when_disabled():
    updater = FakeUpdater()
    broker = broker_for(FakePlex(sessions=[session(username="David")]), updater, owner_scrobble=False)

    broker.on_play(playback_event("playing"))

    assert updater.events == []
    assert "1" in broker.session_owners


def test_watch_broker_drops_non_owner_without_phase_1_route():
    updater = FakeUpdater()
    broker = broker_for(FakePlex(sessions=[session(username="Tina")]), updater)

    broker.on_play(playback_event("playing"))

    assert updater.events == []
    assert "1" in broker.session_owners


def test_watch_broker_uses_cached_owner_for_paused_and_stopped_events():
    updater = FakeUpdater()
    plex = FakePlex(sessions=[session(username="David")])
    broker = broker_for(plex, updater)

    playing = playback_event("playing")
    paused = playback_event("paused")
    stopped = playback_event("stopped")

    broker.on_play(playing)
    plex.session_entries = []
    broker.on_play(paused)
    broker.on_play(stopped)

    assert updater.events == [playing, paused, stopped]
    assert "1" not in broker.session_owners


def test_watch_broker_clears_non_owner_session_on_stop():
    updater = FakeUpdater()
    plex = FakePlex(sessions=[session(username="Tina")])
    broker = broker_for(plex, updater)

    broker.on_play(playback_event("playing"))
    plex.session_entries = []
    broker.on_play(playback_event("stopped"))

    assert updater.events == []
    assert "1" not in broker.session_owners


def test_watch_broker_drops_unknown_playing_session():
    updater = FakeUpdater()
    broker = broker_for(FakePlex(sessions=[]), updater)

    broker.on_play(playback_event("playing"))

    assert updater.events == []
    assert "1" not in broker.session_owners


def test_watch_broker_error_clears_cached_sessions():
    updater = FakeUpdater()
    broker = broker_for(FakePlex(sessions=[session(username="David")]), updater)

    broker.on_play(playback_event("playing"))
    broker.on_error(Error(msg="Server closed connection"))

    assert "1" not in broker.session_owners
    assert len(updater.errors) == 1


def test_watch_broker_validate_requires_session_access():
    broker = broker_for(FakePlex(has_sessions=False))

    with pytest.raises(RuntimeError, match="requires Plex session access"):
        broker.validate()


def test_watch_broker_validate_requires_owner_username():
    broker = broker_for(FakePlex(username=None))

    with pytest.raises(RuntimeError, match="Unable to determine Plex owner username"):
        broker.validate()
