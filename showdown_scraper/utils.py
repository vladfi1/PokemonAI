"""Utility functions and classes for Pokemon Showdown Replay Scraper."""

import time
from datetime import datetime
from threading import Lock


class RateLimiter:
    """Thread-safe rate limiter for API requests."""

    def __init__(self, min_interval: float = 1.0):
        """
        Initialize rate limiter.

        Args:
            min_interval: Minimum seconds between requests.
        """
        self.min_interval = min_interval
        self._last_request_time = 0.0
        self._lock = Lock()

    def wait(self):
        """Wait if necessary to respect rate limit."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
            self._last_request_time = time.time()

    def set_interval(self, interval: float):
        """Update the rate limit interval."""
        with self._lock:
            self.min_interval = interval


def format_timestamp(timestamp: int) -> str:
    """Format a Unix timestamp as a human-readable string."""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def format_relative_time(timestamp: int) -> str:
    """Format a Unix timestamp as relative time (e.g., '2 minutes ago')."""
    now = int(time.time())
    diff = now - timestamp

    if diff < 60:
        return f"{diff} seconds ago"
    elif diff < 3600:
        minutes = diff // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    elif diff < 86400:
        hours = diff // 3600
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    else:
        days = diff // 86400
        return f"{days} day{'s' if days != 1 else ''} ago"


def format_number(n: int) -> str:
    """Format a number with thousands separators."""
    return f"{n:,}"


def format_percentage(part: int, whole: int) -> str:
    """Format a ratio as a percentage string."""
    if whole == 0:
        return "0.0%"
    return f"{(part / whole * 100):.1f}%"


def parse_format_id(format_string: str) -> str:
    """
    Normalize a format string to a format ID.

    Examples:
        'gen9ou' -> 'gen9ou'
        'Gen 9 OU' -> 'gen9ou'
        'gen9-ou' -> 'gen9ou'
    """
    return format_string.lower().replace(" ", "").replace("-", "")


def sanitize_job_name(name: str) -> str:
    """Sanitize a job name for use as an identifier."""
    # Replace spaces and special characters with hyphens
    sanitized = "".join(c if c.isalnum() or c == "-" else "-" for c in name.lower())
    # Remove consecutive hyphens
    while "--" in sanitized:
        sanitized = sanitized.replace("--", "-")
    # Remove leading/trailing hyphens
    return sanitized.strip("-")


def generate_job_name(format_id: str, min_elo: int = 0) -> str:
    """Generate a default job name from parameters."""
    if min_elo > 0:
        return f"{format_id}-elo{min_elo}"
    return format_id
