from __future__ import annotations

from time import monotonic
from typing import TYPE_CHECKING

from trakt.errors import ConflictException, OAuthException, ProcessException, RateLimitException

from plextraktsync.config import TRAKT_RETRY_AFTER_MARGIN, TRAKT_SCROBBLE_UPDATE_INTERVAL
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

    def __init__(self, oauth_backoff_seconds: int = None, scrobble_update_interval: int = None):
        self.oauth_backoff_seconds = oauth_backoff_seconds or self.OAUTH_BACKOFF_SECONDS
        self.oauth_unhealthy_until = 0.0
        self.rate_limit_unhealthy_until = 0.0
        self.scrobble_update_interval = TRAKT_SCROBBLE_UPDATE_INTERVAL if scrobble_update_interval is None else scrobble_update_interval
        self.last_update_at = {}
        self.last_update_progress = {}

    def __call__(self, queues):
        self.discard_updates_with_pending_stop(queues)

        if self.is_trakt_oauth_unhealthy or self.is_trakt_rate_limit_unhealthy:
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
            except RateLimitException as e:
                self.defer_for_rate_limit(queues, e)
                return
            else:
                queues[name].clear()

    def submit(self, name, items):
        name = name.replace("scrobble_", "")
        pending = self.normalize(items)
        results = []
        while pending:
            scrobbler, progress = next(iter(pending.items()))
            if name == "update" and not self.should_submit_update(scrobbler, progress):
                del pending[scrobbler]
                continue
            try:
                res = self.scrobble(scrobbler, name, progress)
            except OAuthException:
                items[:] = list(pending.items())
                raise
            except RateLimitException:
                items[:] = list(pending.items())
                raise
            self.record_successful_scrobble(scrobbler, name, progress)
            results.append(res)
            del pending[scrobbler]

        if results:
            self.logger.debug(f"Submitted {name}: {results}")

    @property
    def is_trakt_oauth_unhealthy(self):
        return self.now() < self.oauth_unhealthy_until

    @property
    def is_trakt_rate_limit_unhealthy(self):
        return self.now() < self.rate_limit_unhealthy_until

    @staticmethod
    def now():
        return monotonic()

    def defer_for_oauth_failure(self, queues):
        self.coalesce_queues(queues)
        self.oauth_unhealthy_until = self.now() + self.oauth_backoff_seconds
        self.logger.error(f"Trakt OAuth failed after reloading auth cache; pausing scrobble submissions for {self.oauth_backoff_seconds} seconds")

    def defer_for_rate_limit(self, queues, e: RateLimitException):
        self.coalesce_queues(queues)
        seconds = e.retry_after + TRAKT_RETRY_AFTER_MARGIN
        self.rate_limit_unhealthy_until = self.now() + seconds
        self.logger.warning(f"Trakt rate limit exceeded; pausing scrobble submissions for {seconds:.1f} seconds")
        self.logger.debug(e.details)

    def coalesce_queues(self, queues):
        for name in self.QUEUES:
            items = queues[name]
            if len(items) > 1:
                queues[name][:] = list(self.normalize(items).items())
        self.discard_updates_with_pending_stop(queues)

    def discard_updates_with_pending_stop(self, queues):
        stopped_scrobblers = set(self.normalize(queues["scrobble_stop"]).keys())
        if not stopped_scrobblers:
            return
        queues["scrobble_update"][:] = [
            (scrobbler, progress) for scrobbler, progress in queues["scrobble_update"] if scrobbler not in stopped_scrobblers
        ]

    def should_submit_update(self, scrobbler: Scrobbler, progress: float):
        last_update_at = self.last_update_at.get(scrobbler)
        if last_update_at is None:
            return True

        last_progress = self.last_update_progress.get(scrobbler)
        if last_progress is not None and progress < last_progress:
            return True

        return self.now() - last_update_at >= self.scrobble_update_interval

    def record_successful_scrobble(self, scrobbler: Scrobbler, name: str, progress: float):
        if name == "update":
            self.last_update_at[scrobbler] = self.now()
            self.last_update_progress[scrobbler] = progress
            return

        self.last_update_at.pop(scrobbler, None)
        self.last_update_progress.pop(scrobbler, None)

    @retry_trakt_oauth
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
