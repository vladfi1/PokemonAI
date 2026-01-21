"""Pokemon Showdown API client."""

import time
from typing import Optional

import requests

from .db.models import Replay
from .utils import RateLimiter


class ShowdownAPIError(Exception):
    """Exception raised for API errors."""

    pass


class ShowdownAPI:
    """Client for Pokemon Showdown replay API."""

    BASE_URL = "https://replay.pokemonshowdown.com"

    def __init__(
        self,
        rate_limit: float = 1.0,
        retry_attempts: int = 3,
        retry_delay: float = 5.0,
    ):
        """
        Initialize the API client.

        Args:
            rate_limit: Minimum seconds between requests.
            retry_attempts: Number of retry attempts for failed requests.
            retry_delay: Seconds to wait between retries.
        """
        self.rate_limiter = RateLimiter(rate_limit)
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "PokemonAI-Scraper/0.1 (https://github.com/vladfi1/PokemonAI)",
                "Accept": "application/json",
            }
        )

    def _request(self, url: str, params: Optional[dict] = None) -> dict:
        """
        Make a rate-limited request with retries.

        Args:
            url: The URL to request.
            params: Optional query parameters.

        Returns:
            The JSON response as a dictionary.

        Raises:
            ShowdownAPIError: If the request fails after all retries.
        """
        last_error = None

        for attempt in range(self.retry_attempts):
            self.rate_limiter.wait()

            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
                continue

            except ValueError as e:
                raise ShowdownAPIError(f"Invalid JSON response: {e}")

        raise ShowdownAPIError(f"Request failed after {self.retry_attempts} attempts: {last_error}")

    def search(
        self,
        format_id: Optional[str] = None,
        user: Optional[str] = None,
        before: Optional[int] = None,
    ) -> list[Replay]:
        """
        Search for replays.

        Args:
            format_id: Filter by format (e.g., 'gen9ou').
            user: Filter by username.
            before: Pagination - only return replays uploaded before this timestamp.

        Returns:
            List of Replay objects (up to 51 per request).
        """
        params = {}

        if format_id:
            params["format"] = format_id

        if user:
            params["user"] = user

        if before:
            params["before"] = before

        url = f"{self.BASE_URL}/search.json"
        data = self._request(url, params)

        # API returns a list of replay objects
        if not isinstance(data, list):
            return []

        return [Replay.from_api_response(item) for item in data]

    def get_replay(self, replay_id: str) -> Optional[Replay]:
        """
        Get full replay data including the battle log.

        Args:
            replay_id: The replay ID (e.g., 'gen9ou-1234567890').

        Returns:
            Replay object with full log, or None if not found.
        """
        url = f"{self.BASE_URL}/{replay_id}.json"

        try:
            data = self._request(url)
            return Replay.from_api_response(data)
        except ShowdownAPIError:
            return None

    def get_replay_log(self, replay_id: str) -> Optional[str]:
        """
        Get just the battle log for a replay.

        Args:
            replay_id: The replay ID.

        Returns:
            The battle log string, or None if not found.
        """
        url = f"{self.BASE_URL}/{replay_id}.json"

        try:
            data = self._request(url)
            return data.get("log")
        except ShowdownAPIError:
            return None

    def set_rate_limit(self, interval: float):
        """Update the rate limit interval."""
        self.rate_limiter.set_interval(interval)
