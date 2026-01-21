"""Data classes for Pokemon Showdown Replay Scraper."""

from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class Replay:
    """Represents a Pokemon Showdown replay metadata (logs stored separately)."""

    id: str  # e.g., "gen9ou-1234567890"
    format_id: str  # e.g., "gen9ou"
    p1_name: str
    p2_name: str
    p1_id: str
    p2_id: str
    upload_time: int  # Unix timestamp
    rating: int = 0  # ELO rating (0 if unranked)
    views: int = 0
    log_fetched: bool = False  # Whether log file exists
    log_size: int = 0  # Compressed log file size in bytes
    scraped_at: int = field(default_factory=lambda: int(time.time()))

    @classmethod
    def from_api_response(cls, data: dict) -> "Replay":
        """Create a Replay from API response data."""
        # Extract format_id from replay ID (e.g., "gen9ou-1234567890" -> "gen9ou")
        replay_id = data["id"]
        format_id = replay_id.rsplit("-", 1)[0] if "-" in replay_id else replay_id

        # Handle both search API format (players array) and replay API format (p1/p2)
        players = data.get("players", [])
        if players:
            p1_name = players[0] if len(players) > 0 else ""
            p2_name = players[1] if len(players) > 1 else ""
        else:
            p1_name = data.get("p1", "")
            p2_name = data.get("p2", "")

        # Generate player IDs (lowercase, no spaces)
        p1_id = data.get("p1id", p1_name.lower().replace(" ", ""))
        p2_id = data.get("p2id", p2_name.lower().replace(" ", ""))

        return cls(
            id=replay_id,
            format_id=format_id,
            p1_name=p1_name,
            p2_name=p2_name,
            p1_id=p1_id,
            p2_id=p2_id,
            upload_time=data.get("uploadtime", 0),
            rating=data.get("rating", 0) or 0,
            views=data.get("views", 0) or 0,
            log_fetched=False,
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "format_id": self.format_id,
            "p1_name": self.p1_name,
            "p2_name": self.p2_name,
            "p1_id": self.p1_id,
            "p2_id": self.p2_id,
            "rating": self.rating,
            "upload_time": self.upload_time,
            "views": self.views,
            "log_fetched": self.log_fetched,
            "log_size": self.log_size,
            "scraped_at": self.scraped_at,
        }


@dataclass
class ScraperJob:
    """Represents a scraper job state."""

    job_name: str
    format_id: Optional[str] = None
    user_filter: Optional[str] = None
    min_elo: int = 0
    max_elo: Optional[int] = None
    last_timestamp: Optional[int] = None  # For pagination
    status: str = "idle"  # idle, running, paused, completed
    total_fetched: int = 0
    total_stored: int = 0
    started_at: Optional[int] = None
    updated_at: Optional[int] = None
    config_json: Optional[str] = None  # Additional config as JSON
    id: Optional[int] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "job_name": self.job_name,
            "format_id": self.format_id,
            "user_filter": self.user_filter,
            "min_elo": self.min_elo,
            "max_elo": self.max_elo,
            "last_timestamp": self.last_timestamp,
            "status": self.status,
            "total_fetched": self.total_fetched,
            "total_stored": self.total_stored,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "config_json": self.config_json,
        }

    @classmethod
    def from_row(cls, row: dict) -> "ScraperJob":
        """Create a ScraperJob from a database row."""
        return cls(
            id=row.get("id"),
            job_name=row["job_name"],
            format_id=row.get("format_id"),
            user_filter=row.get("user_filter"),
            min_elo=row.get("min_elo", 0) or 0,
            max_elo=row.get("max_elo"),
            last_timestamp=row.get("last_timestamp"),
            status=row.get("status", "idle"),
            total_fetched=row.get("total_fetched", 0) or 0,
            total_stored=row.get("total_stored", 0) or 0,
            started_at=row.get("started_at"),
            updated_at=row.get("updated_at"),
            config_json=row.get("config_json"),
        )


@dataclass
class LogEntry:
    """Represents a scrape log entry."""

    job_name: str
    timestamp: int
    level: str  # info, warning, error
    message: str
    id: Optional[int] = None

    @classmethod
    def info(cls, job_name: str, message: str) -> "LogEntry":
        """Create an info log entry."""
        return cls(
            job_name=job_name,
            timestamp=int(time.time()),
            level="info",
            message=message,
        )

    @classmethod
    def warning(cls, job_name: str, message: str) -> "LogEntry":
        """Create a warning log entry."""
        return cls(
            job_name=job_name,
            timestamp=int(time.time()),
            level="warning",
            message=message,
        )

    @classmethod
    def error(cls, job_name: str, message: str) -> "LogEntry":
        """Create an error log entry."""
        return cls(
            job_name=job_name,
            timestamp=int(time.time()),
            level="error",
            message=message,
        )
