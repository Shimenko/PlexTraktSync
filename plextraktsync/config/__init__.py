"""
Platform name to identify our application
"""

from __future__ import annotations

PLEX_PLATFORM = "PlexTraktSync"

"""
Constant in seconds for how much to wait between Trakt POST API calls.
"""
TRAKT_POST_DELAY = 1.1

"""
Constant in seconds for how much to wait between Trakt GET API calls.
"""
TRAKT_GET_DELAY = 0.5

"""
Constant in seconds between periodic Trakt scrobble update calls for active playback.
"""
TRAKT_SCROBBLE_UPDATE_INTERVAL = 300

"""
Constants in seconds for the margin added to retry-after delay to account for network jitter in rate limiting retries.
"""
TRAKT_RETRY_AFTER_MARGIN = 0.9
