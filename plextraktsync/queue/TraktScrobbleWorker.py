from __future__ import annotations

from time import monotonic
from typing import TYPE_CHECKING

from trakt.errors import ConflictException, OAuthException, ProcessException

from plextraktsync.decorators.rate_limit import rate_limit
from plextraktsync.decorators.retry import retry
from plextraktsync.decorators.time_limit import time_limit
from plextraktsync.factory import logging
from plextraktsync.trakt.oauth import retry_trakt_oauth

if TYPE_CHECKING:
    from trakt.sync import Scrobbler

    from plextraktsync.trakt.types import TraktPlayable


class TraktScrobbleWorker:
    # Queues this Worker can handle
    QUEUES = (
        "scrobble_update",
        "scrobble_stop",
    )
    OAUTH_BACKOFF_SECONDS = 300
    logger = logging.getLogger(__name__)

    def __init__(self, oauth_backoff_seconds: int = None):
        self.oauth_backoff_seconds = oauth_backoff_seconds or self.OAUTH_BACKOFF_SECONDS
        self.oauth_unhealthy_until = 0.0

    def __call__(self, queues):
        if self.is_trakt_oauth_unhealthy:
            self.coalesce_queues(queues)
            return

        for name in self.QUEUES:
            items = queues[name]
            if not len(items):
                continue
            try:
                self.submit(name, items)
            except OAuthException:
                self.defer_for_oauth_failure(queues)
                return
            else:
                queues[name].clear()

    def submit(self, name, items):
        name = name.replace("scrobble_", "")
        pending = self.normalize(items)
        results = []
        while pending:
            scrobbler, progress = next(iter(pending.items()))
            try:
                res = self.scrobble(scrobbler, name, progress)
            except OAuthException:
                items[:] = list(pending.items())
                raise
            results.append(res)
            del pending[scrobbler]

        if results:
            self.logger.debug(f"Submitted {name}: {results}")

    @property
    def is_trakt_oauth_unhealthy(self):
        return self.now() < self.oauth_unhealthy_until

    @staticmethod
    def now():
        return monotonic()

    def defer_for_oauth_failure(self, queues):
        self.coalesce_queues(queues)
        self.oauth_unhealthy_until = self.now() + self.oauth_backoff_seconds
        self.logger.error(f"Trakt OAuth failed after reloading auth cache; pausing scrobble submissions for {self.oauth_backoff_seconds} seconds")

    def coalesce_queues(self, queues):
        for name in self.QUEUES:
            items = queues[name]
            if len(items) > 1:
                queues[name][:] = list(self.normalize(items).items())

    @retry_trakt_oauth
    @rate_limit()
    @time_limit()
    @retry()
    def scrobble(self, scrobbler: Scrobbler, name: str, progress: float):
        method = getattr(scrobbler, name)
        try:
            return method(progress)
        except (ConflictException, ProcessException) as e:
            self.logger.error(f"{e} {e.response.text}")
            self.logger.debug(e.response.headers)

    @staticmethod
    def normalize(items: list[TraktPlayable]):
        result = {}
        for scrobbler, progress in items:
            result[scrobbler] = progress

        return result
