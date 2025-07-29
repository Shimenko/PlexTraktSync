from __future__ import annotations

import time
from functools import wraps
from threading import Lock

from plextraktsync.factory import logging

logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, calls_per_second=10):
        self.calls_per_second = calls_per_second
        self.interval = 1.0 / calls_per_second
        self.last_call_time = 0
        self.lock = Lock()

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self.lock:
                current_time = time.time()
                time_since_last_call = current_time - self.last_call_time
                
                if time_since_last_call < self.interval:
                    sleep_time = self.interval - time_since_last_call
                    logger.debug(f"Throttling API call to {func.__name__}, sleeping for {sleep_time:.4f}s")
                    time.sleep(sleep_time)
                
                self.last_call_time = time.time()
                
            return func(*args, **kwargs)
        return wrapper

throttle = RateLimiter