"""Core scraper logic for Pokemon Showdown Replay Scraper."""

import signal
import time
from typing import Callable, Optional

from .api import ShowdownAPI, ShowdownAPIError
from .config import Config
from .db import Database
from .db.models import LogEntry, Replay, ScraperJob
from .storage import LogStorage
from .utils import format_number, format_percentage, format_timestamp


class ReplayScraper:
    """Main scraper class for fetching Pokemon Showdown replays."""

    def __init__(self, config: Config, db: Database, log_storage: LogStorage):
        """
        Initialize the scraper.

        Args:
            config: Configuration object.
            db: Database instance.
            log_storage: Log storage instance.
        """
        self.config = config
        self.db = db
        self.log_storage = log_storage
        self.api = ShowdownAPI(
            rate_limit=config.scraping.rate_limit,
            retry_attempts=config.scraping.retry_attempts,
            retry_delay=config.scraping.retry_delay,
        )
        self.running = True
        self._setup_signal_handlers()
        self._progress_callback: Optional[Callable[[str], None]] = None

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        self.running = False
        self._log_progress("Received shutdown signal, stopping after current batch...")

    def set_progress_callback(self, callback: Callable[[str], None]):
        """Set a callback for progress updates."""
        self._progress_callback = callback

    def _log_progress(self, message: str):
        """Log progress message."""
        if self._progress_callback:
            self._progress_callback(message)
        else:
            print(message)

    def run_job(
        self,
        job_name: str,
        limit: Optional[int] = None,
        fetch_logs: bool = False,
    ) -> bool:
        """
        Run a scraping job.

        Args:
            job_name: Name of the job to run.
            limit: Optional limit on number of replays to fetch.
            fetch_logs: Whether to fetch full logs for each replay.

        Returns:
            True if job completed successfully, False if interrupted.
        """
        job = self.db.get_job(job_name)
        if job is None:
            self._log_progress(f"Job '{job_name}' not found")
            return False

        job.status = "running"
        self.db.update_job(job)
        self.db.add_log(LogEntry.info(job_name, f"Started job: {job_name}"))

        self._log_progress(f"Starting job: {job_name}")
        self._log_progress(f"  Format: {job.format_id or 'all'}")
        self._log_progress(f"  ELO filter: >= {job.min_elo}" + (f", <= {job.max_elo}" if job.max_elo else ""))

        batch_count = 0
        total_new = 0

        try:
            while self.running:
                # Check if we've reached the limit (based on new replays this run)
                if limit and total_new >= limit:
                    self._log_progress(f"Reached limit of {limit} new replays")
                    job.status = "completed"
                    break

                # Check if job has been paused or deleted
                current_job = self.db.get_job(job_name)
                if current_job is None:
                    self._log_progress("Job was deleted, stopping")
                    return False
                if current_job.status == "paused":
                    self._log_progress("Job paused by user")
                    job.status = "paused"
                    break

                # Fetch batch of replays
                try:
                    replays = self.api.search(
                        format_id=job.format_id,
                        user=job.user_filter,
                        before=job.last_timestamp,
                    )
                except ShowdownAPIError as e:
                    self.db.add_log(LogEntry.error(job_name, f"API error: {e}"))
                    self._log_progress(f"API error: {e}")
                    time.sleep(self.config.scraping.retry_delay)
                    continue

                if not replays:
                    self._log_progress("No more replays found")
                    job.status = "completed"
                    break

                batch_count += 1
                job.total_fetched += len(replays)

                # Filter by ELO for log fetching only
                elo_matched = self._filter_by_elo(replays, job.min_elo, job.max_elo)
                logs_fetched_batch = 0

                # Optionally fetch full logs for ELO-matched replays
                if fetch_logs:
                    for replay in elo_matched:
                        if not self.running:
                            break
                        try:
                            log = self.api.get_replay_log(replay.id)
                            if log:
                                _, log_size = self.log_storage.save(replay.id, replay.format_id, log)
                                replay.log_fetched = True
                                replay.log_size = log_size
                                logs_fetched_batch += 1
                        except ShowdownAPIError:
                            pass  # Skip failed log fetches

                # Store ALL replays (not just ELO-filtered)
                for replay in replays:
                    if self.db.insert_replay(replay):
                        total_new += 1
                        job.total_stored += 1

                # Update pagination timestamp
                if replays:
                    job.last_timestamp = replays[-1].upload_time

                self.db.update_job(job)

                # Progress update
                before_date = format_timestamp(job.last_timestamp) if job.last_timestamp else "now"
                log_info = ""
                if fetch_logs:
                    elo_rate = format_percentage(len(elo_matched), len(replays))
                    log_info = f", {logs_fetched_batch} logs ({elo_rate} ELO match)"
                self._log_progress(
                    f"Batch {batch_count}: {len(replays)} fetched, {total_new} new{log_info}. "
                    f"Total: {format_number(job.total_stored)}. Before: {before_date}"
                )

        except Exception as e:
            self.db.add_log(LogEntry.error(job_name, f"Unexpected error: {e}"))
            self._log_progress(f"Error: {e}")
            job.status = "paused"  # Pause on error so it can be resumed
            self.db.update_job(job)
            return False

        # If we exited because of a shutdown signal, mark as paused
        if not self.running and job.status == "running":
            job.status = "paused"

        self.db.update_job(job)
        self.db.add_log(
            LogEntry.info(
                job_name,
                f"Job {job.status}: {job.total_fetched} fetched, {job.total_stored} stored",
            )
        )

        self._log_progress(f"Job {job.status}")
        self._log_progress(f"  Total fetched: {format_number(job.total_fetched)}")
        self._log_progress(f"  Total stored: {format_number(job.total_stored)}")

        return job.status == "completed"

    def _filter_by_elo(
        self,
        replays: list[Replay],
        min_elo: int,
        max_elo: Optional[int],
    ) -> list[Replay]:
        """Filter replays by ELO rating."""
        filtered = []
        for replay in replays:
            rating = replay.rating or 0
            if rating >= min_elo:
                if max_elo is None or rating <= max_elo:
                    filtered.append(replay)
        return filtered

    def fetch_logs(
        self,
        limit: int = 100,
        job_name: Optional[str] = None,
    ) -> int:
        """
        Fetch logs for replays that don't have them.

        Args:
            limit: Maximum number of logs to fetch.
            job_name: Optional job name for logging.

        Returns:
            Number of logs successfully fetched.
        """
        log_job = job_name or "fetch-logs"
        self._log_progress(f"Fetching logs for up to {limit} replays...")

        replays = self.db.get_replays_without_logs(limit)
        if not replays:
            self._log_progress("No replays need log fetching")
            return 0

        self._log_progress(f"Found {len(replays)} replays without logs")

        fetched = 0
        for i, replay in enumerate(replays, 1):
            if not self.running:
                self._log_progress("Interrupted")
                break

            try:
                log = self.api.get_replay_log(replay.id)
                if log:
                    _, log_size = self.log_storage.save(replay.id, replay.format_id, log)
                    self.db.mark_log_fetched(replay.id, log_size)
                    fetched += 1

                if i % 10 == 0 or i == len(replays):
                    self._log_progress(f"Progress: {i}/{len(replays)} ({fetched} successful)")

            except ShowdownAPIError as e:
                self.db.add_log(LogEntry.warning(log_job, f"Failed to fetch log for {replay.id}: {e}"))

        self._log_progress(f"Fetched {fetched} logs")
        return fetched

    def create_job(
        self,
        job_name: str,
        format_id: Optional[str] = None,
        user_filter: Optional[str] = None,
        min_elo: int = 0,
        max_elo: Optional[int] = None,
    ) -> ScraperJob:
        """
        Create a new scraping job.

        Args:
            job_name: Unique name for the job.
            format_id: Format to scrape (e.g., 'gen9ou').
            user_filter: Optional username filter.
            min_elo: Minimum ELO rating.
            max_elo: Maximum ELO rating.

        Returns:
            The created ScraperJob.
        """
        existing = self.db.get_job(job_name)
        if existing:
            raise ValueError(f"Job '{job_name}' already exists")

        job = ScraperJob(
            job_name=job_name,
            format_id=format_id,
            user_filter=user_filter,
            min_elo=min_elo,
            max_elo=max_elo,
            status="idle",
        )

        return self.db.create_job(job)


class LogFetcher:
    """Dedicated worker for fetching replay logs."""

    def __init__(self, config: Config, db: Database, log_storage: LogStorage):
        self.config = config
        self.db = db
        self.log_storage = log_storage
        self.api = ShowdownAPI(
            rate_limit=config.scraping.rate_limit,
            retry_attempts=config.scraping.retry_attempts,
            retry_delay=config.scraping.retry_delay,
        )
        self.running = True
        self._setup_signal_handlers()
        self._progress_callback: Optional[Callable[[str], None]] = None

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        self.running = False

    def set_progress_callback(self, callback: Callable[[str], None]):
        self._progress_callback = callback

    def _log_progress(self, message: str):
        if self._progress_callback:
            self._progress_callback(message)
        else:
            print(message)

    def run(
        self,
        limit: Optional[int] = None,
        min_elo: Optional[int] = None,
        max_elo: Optional[int] = None,
        format_id: Optional[str] = None,
    ) -> int:
        """
        Fetch logs for replays without them.

        Args:
            limit: Maximum number of logs to fetch. If None, runs continuously.
            min_elo: Minimum ELO rating filter.
            max_elo: Maximum ELO rating filter.
            format_id: Format filter (e.g., 'gen9ou').

        Returns:
            Number of logs fetched.
        """
        # Get total count of replays needing logs (with filters)
        total_without_logs = self.db.count_replays_without_logs(min_elo=min_elo, max_elo=max_elo, format_id=format_id)

        if total_without_logs == 0:
            filter_info = ""
            if format_id:
                filter_info += f" format={format_id}"
            if min_elo:
                filter_info += f" ELO >= {min_elo}"
            self._log_progress("No replays need logs" + (f" ({filter_info.strip()})" if filter_info else ""))
            return 0

        target = min(limit, total_without_logs) if limit else total_without_logs
        filter_parts = []
        if format_id is not None:
            filter_parts.append(f"format={format_id}")
        if min_elo is not None:
            filter_parts.append(f"ELO >= {min_elo}")
        if max_elo is not None:
            filter_parts.append(f"ELO <= {max_elo}")
        filter_info = f" ({', '.join(filter_parts)})" if filter_parts else ""
        self._log_progress(f"Fetching logs: {total_without_logs} replays need logs{filter_info}" + (f", limit {limit}" if limit else ""))

        total_fetched = 0
        batch_size = 10

        while self.running:
            current_limit = batch_size if limit is None else min(batch_size, limit - total_fetched)
            if current_limit <= 0:
                break

            replays = self.db.get_replays_without_logs(current_limit, min_elo=min_elo, max_elo=max_elo, format_id=format_id)
            if not replays:
                if limit is None:
                    self._log_progress("No replays need logs, waiting...")
                    time.sleep(10)
                    continue
                else:
                    break

            for replay in replays:
                if not self.running:
                    break

                try:
                    log = self.api.get_replay_log(replay.id)
                    if log:
                        _, log_size = self.log_storage.save(replay.id, replay.format_id, log)
                        self.db.mark_log_fetched(replay.id, log_size)
                        total_fetched += 1
                except ShowdownAPIError:
                    pass

                if limit and total_fetched >= limit:
                    break

            pct = (total_fetched / target * 100) if target > 0 else 100
            self._log_progress(f"Progress: {total_fetched}/{target} ({pct:.1f}%)")

        return total_fetched
