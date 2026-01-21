"""File-based storage for battle logs with compression."""

import gzip
import os
from pathlib import Path
from typing import Optional


class LogStorage:
    """File-based storage for battle logs with gzip compression."""

    def __init__(self, base_path: str = "./data/logs"):
        """
        Initialize log storage.

        Args:
            base_path: Base directory for log files.
        """
        self.base_path = Path(base_path)

    def _get_log_path(self, replay_id: str, format_id: str) -> Path:
        """Get the path for a log file."""
        return self.base_path / format_id / f"{replay_id}.log.gz"

    def save(self, replay_id: str, format_id: str, log: str) -> tuple[Path, int]:
        """
        Save a battle log to compressed file.

        Args:
            replay_id: The replay ID.
            format_id: The format ID (used for directory structure).
            log: The battle log content.

        Returns:
            Tuple of (path to the saved file, compressed file size in bytes).
        """
        log_path = self._get_log_path(replay_id, format_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with gzip.open(log_path, "wt", encoding="utf-8") as f:
            f.write(log)

        file_size = log_path.stat().st_size
        return log_path, file_size

    def load(self, replay_id: str, format_id: str) -> Optional[str]:
        """
        Load a battle log from file.

        Args:
            replay_id: The replay ID.
            format_id: The format ID.

        Returns:
            The battle log content, or None if not found.
        """
        log_path = self._get_log_path(replay_id, format_id)

        if not log_path.exists():
            return None

        try:
            with gzip.open(log_path, "rt", encoding="utf-8") as f:
                return f.read()
        except (gzip.BadGzipFile, OSError):
            return None

    def exists(self, replay_id: str, format_id: str) -> bool:
        """Check if a log file exists."""
        return self._get_log_path(replay_id, format_id).exists()

    def delete(self, replay_id: str, format_id: str) -> bool:
        """
        Delete a log file.

        Returns:
            True if deleted, False if not found.
        """
        log_path = self._get_log_path(replay_id, format_id)
        if log_path.exists():
            log_path.unlink()
            return True
        return False

    def get_stats(self) -> dict:
        """Get storage statistics."""
        if not self.base_path.exists():
            return {
                "total_files": 0,
                "total_size_bytes": 0,
                "total_size_mb": 0.0,
                "formats": {},
            }

        total_files = 0
        total_size = 0
        formats = {}

        for format_dir in self.base_path.iterdir():
            if format_dir.is_dir():
                format_files = list(format_dir.glob("*.log.gz"))
                format_size = sum(f.stat().st_size for f in format_files)
                formats[format_dir.name] = {
                    "files": len(format_files),
                    "size_bytes": format_size,
                    "size_mb": format_size / (1024 * 1024),
                }
                total_files += len(format_files)
                total_size += format_size

        return {
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "formats": formats,
        }

    def iter_logs(self, format_id: Optional[str] = None):
        """
        Iterate over all stored logs.

        Args:
            format_id: Optional format to filter by.

        Yields:
            Tuples of (replay_id, format_id, log_content).
        """
        if not self.base_path.exists():
            return

        if format_id:
            format_dirs = [self.base_path / format_id]
        else:
            format_dirs = [d for d in self.base_path.iterdir() if d.is_dir()]

        for format_dir in format_dirs:
            if not format_dir.exists():
                continue
            fmt = format_dir.name
            for log_file in format_dir.glob("*.log.gz"):
                replay_id = log_file.stem.replace(".log", "")
                try:
                    with gzip.open(log_file, "rt", encoding="utf-8") as f:
                        yield replay_id, fmt, f.read()
                except (gzip.BadGzipFile, OSError):
                    continue
