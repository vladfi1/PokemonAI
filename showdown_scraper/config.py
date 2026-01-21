"""Configuration management for Pokemon Showdown Replay Scraper."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class StorageConfig:
    """Storage configuration."""

    database_path: str = "./data/replays.db"
    logs_path: str = "./data/logs"  # Directory for compressed log files


@dataclass
class ScrapingConfig:
    """Scraping configuration."""

    rate_limit: float = 1.0  # Seconds between requests
    batch_size: int = 50  # Replays per batch (API max is 51)
    fetch_full_log: bool = True  # Whether to fetch full replay data
    retry_attempts: int = 3
    retry_delay: float = 5.0


@dataclass
class DefaultsConfig:
    """Default scraping parameters."""

    min_elo: int = 0
    max_elo: Optional[int] = None
    format: Optional[str] = None


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    file: Optional[str] = "./data/scraper.log"


@dataclass
class Config:
    """Main configuration container."""

    storage: StorageConfig = field(default_factory=StorageConfig)
    scraping: ScrapingConfig = field(default_factory=ScrapingConfig)
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "Config":
        """
        Load configuration from a YAML file.

        Args:
            config_path: Path to config file. If None, uses default locations.

        Returns:
            Config object with loaded values.
        """
        config = cls()

        # Try default locations if no path specified
        if config_path is None:
            search_paths = [
                Path("config.yaml"),
                Path("config.yml"),
                Path.home() / ".config" / "pokemon-scraper" / "config.yaml",
            ]
            for path in search_paths:
                if path.exists():
                    config_path = str(path)
                    break

        if config_path and Path(config_path).exists():
            with open(config_path, "r") as f:
                data = yaml.safe_load(f) or {}
            config._load_from_dict(data)

        return config

    def _load_from_dict(self, data: dict):
        """Load configuration from a dictionary."""
        if "storage" in data:
            storage_data = data["storage"]
            if "database_path" in storage_data:
                self.storage.database_path = storage_data["database_path"]
            if "logs_path" in storage_data:
                self.storage.logs_path = storage_data["logs_path"]

        # Backwards compatibility with old "database" key
        if "database" in data:
            db_data = data["database"]
            if "path" in db_data:
                self.storage.database_path = db_data["path"]

        if "scraping" in data:
            scraping_data = data["scraping"]
            if "rate_limit" in scraping_data:
                self.scraping.rate_limit = float(scraping_data["rate_limit"])
            if "batch_size" in scraping_data:
                self.scraping.batch_size = int(scraping_data["batch_size"])
            if "fetch_full_log" in scraping_data:
                self.scraping.fetch_full_log = bool(scraping_data["fetch_full_log"])
            if "retry_attempts" in scraping_data:
                self.scraping.retry_attempts = int(scraping_data["retry_attempts"])
            if "retry_delay" in scraping_data:
                self.scraping.retry_delay = float(scraping_data["retry_delay"])

        if "defaults" in data:
            defaults_data = data["defaults"]
            if "min_elo" in defaults_data:
                self.defaults.min_elo = int(defaults_data["min_elo"])
            if "max_elo" in defaults_data:
                self.defaults.max_elo = (
                    int(defaults_data["max_elo"])
                    if defaults_data["max_elo"] is not None
                    else None
                )
            if "format" in defaults_data:
                self.defaults.format = defaults_data["format"]

        if "logging" in data:
            logging_data = data["logging"]
            if "level" in logging_data:
                self.logging.level = logging_data["level"]
            if "file" in logging_data:
                self.logging.file = logging_data["file"]

    def save(self, config_path: str):
        """Save configuration to a YAML file."""
        data = {
            "storage": {
                "database_path": self.storage.database_path,
                "logs_path": self.storage.logs_path,
            },
            "scraping": {
                "rate_limit": self.scraping.rate_limit,
                "batch_size": self.scraping.batch_size,
                "fetch_full_log": self.scraping.fetch_full_log,
                "retry_attempts": self.scraping.retry_attempts,
                "retry_delay": self.scraping.retry_delay,
            },
            "defaults": {
                "min_elo": self.defaults.min_elo,
                "max_elo": self.defaults.max_elo,
                "format": self.defaults.format,
            },
            "logging": {
                "level": self.logging.level,
                "file": self.logging.file,
            },
        }

        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    def to_dict(self) -> dict:
        """Convert configuration to a dictionary."""
        return {
            "storage": {
                "database_path": self.storage.database_path,
                "logs_path": self.storage.logs_path,
            },
            "scraping": {
                "rate_limit": self.scraping.rate_limit,
                "batch_size": self.scraping.batch_size,
                "fetch_full_log": self.scraping.fetch_full_log,
                "retry_attempts": self.scraping.retry_attempts,
                "retry_delay": self.scraping.retry_delay,
            },
            "defaults": {
                "min_elo": self.defaults.min_elo,
                "max_elo": self.defaults.max_elo,
                "format": self.defaults.format,
            },
            "logging": {
                "level": self.logging.level,
                "file": self.logging.file,
            },
        }


def get_default_config_path() -> Path:
    """Get the default configuration file path."""
    return Path("config.yaml")


def ensure_config_exists(config_path: Optional[str] = None) -> str:
    """Ensure a config file exists, creating default if necessary."""
    if config_path is None:
        config_path = str(get_default_config_path())

    path = Path(config_path)
    if not path.exists():
        config = Config()
        config.save(config_path)

    return config_path
