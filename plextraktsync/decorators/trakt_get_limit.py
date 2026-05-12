from __future__ import annotations

from plextraktsync.config import TRAKT_GET_DELAY
from plextraktsync.util.Timer import Timer

timer = Timer(TRAKT_GET_DELAY)


def wait_for_trakt_get():
    """
    Throttles Trakt GET calls not to be called more often than TRAKT_GET_DELAY.
    """

    timer.wait_if_needed()
