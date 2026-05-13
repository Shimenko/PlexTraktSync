from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

from plextraktsync.factory import logging

if TYPE_CHECKING:
    from plextraktsync.config.Config import Config
    from plextraktsync.plex.PlexApi import PlexApi
    from plextraktsync.watch.events import Error, PlaySessionStateNotification, ServerStarted
    from plextraktsync.watch.WatchStateUpdater import WatchStateUpdater


@dataclass(frozen=True)
class BrokerSessionOwner:
    username: str


class SessionOwnershipCache:
    def __init__(self, lookup_owner: Callable[[str], BrokerSessionOwner | None]):
        self.lookup_owner = lookup_owner
        self.data: dict[str, BrokerSessionOwner] = {}

    def __contains__(self, session_key: object):
        return str(session_key) in self.data

    def clear(self):
        self.data.clear()

    def resolve(self, event: PlaySessionStateNotification) -> BrokerSessionOwner | None:
        session_key = str(event.session_key)

        if event.state == "playing":
            owner = self.lookup_owner(session_key)
            if owner is not None:
                self.data[session_key] = owner
            return owner

        return self.data.get(session_key)

    def release(self, event: PlaySessionStateNotification):
        if event.state == "stopped":
            self.data.pop(str(event.session_key), None)


class WatchBroker:
    logger = logging.getLogger(__name__)

    def __init__(
        self,
        plex: PlexApi,
        updater: WatchStateUpdater,
        config: Config,
        owner_username: str | None = None,
    ):
        self.plex = plex
        self.updater = updater
        self.config = config
        self._owner_username = owner_username
        self.session_owners = SessionOwnershipCache(self.lookup_session_owner)

    @cached_property
    def sessions(self):
        from plextraktsync.plex.SessionCollection import SessionCollection

        return SessionCollection(self.plex)

    @cached_property
    def owner_username(self) -> str:
        if self._owner_username:
            return self._owner_username

        account = self.plex.account
        username = getattr(account, "username", None)
        if not username:
            raise RuntimeError("Unable to determine Plex owner username for watch-broker")

        return username

    @property
    def owner_scrobble(self):
        return self.config.get("watch_broker", {}).get("owner_scrobble", True)

    def validate(self):
        if not self.plex.has_sessions():
            raise RuntimeError("watch-broker requires Plex session access for safe user attribution")

        _ = self.owner_username

    def lookup_session_owner(self, session_key: str) -> BrokerSessionOwner | None:
        self.sessions.update_sessions()
        username = self.sessions.get(str(session_key))
        if username is None:
            return None

        return BrokerSessionOwner(username=username)

    def on_start(self, event: ServerStarted):
        self.updater.on_start(event)

    def on_error(self, error: Error):
        self.session_owners.clear()
        self.sessions.clear()
        self.updater.on_error(error)

    def on_play(self, event: PlaySessionStateNotification):
        owner = self.session_owners.resolve(event)
        if owner is None:
            self.logger.debug("watch-broker: No session owner for %s event on session %s", event.state, event.session_key)
            self.session_owners.release(event)
            return

        try:
            if owner.username == self.owner_username:
                if self.owner_scrobble:
                    self.updater.on_play(event)
                return

            self.logger.debug("watch-broker: No Phase 1 route for non-owner Plex user %s", owner.username)
        finally:
            self.session_owners.release(event)
