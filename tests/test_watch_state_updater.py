from __future__ import annotations

from types import SimpleNamespace

import pytest

from plextraktsync.watch.events import Error
from plextraktsync.watch.WatchStateUpdater import WatchStateUpdater


def make_config(username_filter=True, ignore_clients=None):
    return {
        "watch": {
            "add_collection": False,
            "remove_collection": False,
            "username_filter": username_filter,
            "media_progressbar": False,
            "ignore_clients": ignore_clients,
        },
    }


def make_session(session_key, username):
    return SimpleNamespace(sessionKey=session_key, usernames=[username] if username else [])


def make_event(session_key="1", client_identifier="client"):
    return SimpleNamespace(session_key=session_key, client_identifier=client_identifier)


class FakePlex:
    def __init__(self, has_sessions=True, username="David", sessions=None):
        self._has_sessions = has_sessions
        self.account = SimpleNamespace(username=username)
        self.sessions = sessions or []

    def has_sessions(self):
        return self._has_sessions


def make_updater(plex=None, config=None):
    return WatchStateUpdater(
        plex=plex or FakePlex(),
        trakt=None,
        mf=None,
        config=config or make_config(),
    )


def test_username_filter_disabled_allows_all_users_without_session_access():
    updater = make_updater(
        plex=FakePlex(has_sessions=False),
        config=make_config(username_filter=False),
    )

    updater.validate_username_filter()

    assert updater.username_filter is None
    assert updater.can_scrobble(make_event()) is True


def test_username_filter_enabled_requires_session_access():
    updater = make_updater(plex=FakePlex(has_sessions=False))

    with pytest.raises(RuntimeError, match="requires Plex session access"):
        updater.validate_username_filter()

    with pytest.raises(RuntimeError, match="requires Plex session access"):
        updater.can_scrobble(make_event())


def test_username_filter_enabled_requires_account_username():
    updater = make_updater(plex=FakePlex(has_sessions=True, username=""))

    with pytest.raises(RuntimeError, match="requires Plex session access"):
        updater.validate_username_filter()


def test_can_scrobble_allows_owner_session():
    updater = make_updater(
        plex=FakePlex(
            username="David",
            sessions=[
                make_session("1", "David"),
            ],
        ),
    )

    updater.validate_username_filter()

    assert updater.can_scrobble(make_event(session_key="1")) is True


def test_can_scrobble_rejects_non_owner_session():
    updater = make_updater(
        plex=FakePlex(
            username="David",
            sessions=[
                make_session("1", "Tina"),
            ],
        ),
    )

    updater.validate_username_filter()

    assert updater.can_scrobble(make_event(session_key="1")) is False


def test_can_scrobble_rejects_missing_session_owner():
    updater = make_updater(plex=FakePlex(username="David", sessions=[]))

    updater.validate_username_filter()

    assert updater.can_scrobble(make_event(session_key="1")) is False


def test_on_error_does_not_create_session_filter_state():
    updater = make_updater(plex=FakePlex(has_sessions=False))

    updater.on_error(Error(msg="boom"))

    assert "username_filter" not in updater.__dict__
    assert "sessions" not in updater.__dict__


def test_on_error_clears_existing_sessions_without_disabling_filter():
    updater = make_updater(
        plex=FakePlex(
            username="David",
            sessions=[
                make_session("1", "David"),
            ],
        ),
    )
    updater.sessions["1"] = "David"
    updater.session_media["1"] = object()

    updater.on_error(Error(msg="boom"))

    assert updater.username_filter == "David"
    assert updater.sessions == {}
    assert updater.session_media == {}
