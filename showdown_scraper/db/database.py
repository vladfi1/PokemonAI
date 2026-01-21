"""Database operations for Pokemon Showdown Replay Scraper."""

import sqlite3
import time
from pathlib import Path
from typing import Optional

from .models import LogEntry, Replay, ScraperJob
from .schema import ALL_INDEXES, ALL_TABLES, MIGRATION_V2_TO_V3, SCHEMA_VERSION


class Database:
    """SQLite database wrapper for replay storage and scraper state."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.execute("PRAGMA journal_mode = WAL")
        return self._conn

    @property
    def conn(self) -> sqlite3.Connection:
        """Database connection property."""
        return self._get_connection()

    def close(self):
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def initialize(self):
        """Initialize database schema."""
        cursor = self.conn.cursor()

        # Create tables
        for table_sql in ALL_TABLES:
            cursor.execute(table_sql)

        # Create indexes
        for index_sql in ALL_INDEXES:
            cursor.execute(index_sql)

        # Check and run migrations
        cursor.execute("SELECT version FROM schema_version LIMIT 1")
        row = cursor.fetchone()
        if row is None:
            cursor.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
            )
        else:
            current_version = row[0]
            self._run_migrations(cursor, current_version)

        self.conn.commit()

    def _run_migrations(self, cursor, current_version: int):
        """Run database migrations from current version to latest."""
        if current_version < 3:
            # Migration v2 to v3: add log_size column
            try:
                cursor.execute(MIGRATION_V2_TO_V3)
            except sqlite3.OperationalError:
                pass  # Column already exists
            cursor.execute("UPDATE schema_version SET version = 3")

        # Add future migrations here

    # Replay operations

    def insert_replay(self, replay: Replay) -> bool:
        """Insert a replay into the database. Returns True if inserted, False if exists."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO replays (
                    id, format_id, p1_name, p2_name, p1_id, p2_id,
                    rating, upload_time, views, log_fetched, log_size, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    replay.id,
                    replay.format_id,
                    replay.p1_name,
                    replay.p2_name,
                    replay.p1_id,
                    replay.p2_id,
                    replay.rating,
                    replay.upload_time,
                    replay.views,
                    replay.log_fetched,
                    replay.log_size,
                    replay.scraped_at,
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Replay already exists
            return False

    def insert_replays_batch(self, replays: list[Replay]) -> tuple[int, int]:
        """Insert multiple replays. Returns (inserted_count, skipped_count)."""
        inserted = 0
        skipped = 0
        for replay in replays:
            if self.insert_replay(replay):
                inserted += 1
            else:
                skipped += 1
        return inserted, skipped

    def get_replay(self, replay_id: str) -> Optional[Replay]:
        """Get a replay by ID."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM replays WHERE id = ?", (replay_id,))
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_replay(dict(row))

    def replay_exists(self, replay_id: str) -> bool:
        """Check if a replay exists in the database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM replays WHERE id = ?", (replay_id,))
        return cursor.fetchone() is not None

    def mark_log_fetched(self, replay_id: str, log_size: int = 0) -> bool:
        """Mark a replay's log as fetched and store the compressed file size."""
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE replays SET log_fetched = 1, log_size = ? WHERE id = ?",
            (log_size, replay_id),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def get_replays_without_logs(self, limit: int = 100) -> list[Replay]:
        """Get replays that don't have logs fetched yet."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT * FROM replays
            WHERE log_fetched = 0
            ORDER BY upload_time DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [self._row_to_replay(dict(row)) for row in cursor.fetchall()]

    def query_replays(
        self,
        format_id: Optional[str] = None,
        min_elo: Optional[int] = None,
        max_elo: Optional[int] = None,
        player: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Replay]:
        """Query replays with filters."""
        conditions = []
        params = []

        if format_id:
            conditions.append("format_id = ?")
            params.append(format_id)

        if min_elo is not None:
            conditions.append("rating >= ?")
            params.append(min_elo)

        if max_elo is not None:
            conditions.append("rating <= ?")
            params.append(max_elo)

        if player:
            player_lower = player.lower().replace(" ", "")
            conditions.append("(p1_id = ? OR p2_id = ?)")
            params.extend([player_lower, player_lower])

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.extend([limit, offset])

        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            SELECT * FROM replays
            WHERE {where_clause}
            ORDER BY upload_time DESC
            LIMIT ? OFFSET ?
            """,
            params,
        )
        return [self._row_to_replay(dict(row)) for row in cursor.fetchall()]

    def get_replay_stats(self) -> dict:
        """Get database statistics."""
        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM replays")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM replays WHERE log_fetched = 1")
        with_logs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT format_id) FROM replays")
        formats = cursor.fetchone()[0]

        cursor.execute("SELECT COALESCE(SUM(log_size), 0) FROM replays")
        total_log_size = cursor.fetchone()[0]

        return {
            "total_replays": total,
            "with_logs": with_logs,
            "without_logs": total - with_logs,
            "formats": formats,
            "total_log_size_bytes": total_log_size,
            "total_log_size_mb": total_log_size / (1024 * 1024),
        }

    def _row_to_replay(self, row: dict) -> Replay:
        """Convert a database row to a Replay object."""
        return Replay(
            id=row["id"],
            format_id=row["format_id"],
            p1_name=row["p1_name"],
            p2_name=row["p2_name"],
            p1_id=row["p1_id"],
            p2_id=row["p2_id"],
            rating=row["rating"] or 0,
            upload_time=row["upload_time"],
            views=row["views"] or 0,
            log_fetched=bool(row["log_fetched"]),
            log_size=row.get("log_size", 0) or 0,
            scraped_at=row["scraped_at"],
        )

    # Scraper job operations

    def create_job(self, job: ScraperJob) -> ScraperJob:
        """Create a new scraper job."""
        cursor = self.conn.cursor()
        now = int(time.time())
        cursor.execute(
            """
            INSERT INTO scraper_state (
                job_name, format_id, user_filter, min_elo, max_elo,
                last_timestamp, status, total_fetched, total_stored,
                started_at, updated_at, config_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job.job_name,
                job.format_id,
                job.user_filter,
                job.min_elo,
                job.max_elo,
                job.last_timestamp,
                job.status,
                job.total_fetched,
                job.total_stored,
                now,
                now,
                job.config_json,
            ),
        )
        self.conn.commit()
        job.id = cursor.lastrowid
        job.started_at = now
        job.updated_at = now
        return job

    def get_job(self, job_name: str) -> Optional[ScraperJob]:
        """Get a scraper job by name."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM scraper_state WHERE job_name = ?", (job_name,))
        row = cursor.fetchone()
        if row is None:
            return None
        return ScraperJob.from_row(dict(row))

    def get_all_jobs(self) -> list[ScraperJob]:
        """Get all scraper jobs."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM scraper_state ORDER BY updated_at DESC")
        return [ScraperJob.from_row(dict(row)) for row in cursor.fetchall()]

    def update_job(self, job: ScraperJob):
        """Update a scraper job."""
        cursor = self.conn.cursor()
        job.updated_at = int(time.time())
        cursor.execute(
            """
            UPDATE scraper_state SET
                format_id = ?,
                user_filter = ?,
                min_elo = ?,
                max_elo = ?,
                last_timestamp = ?,
                status = ?,
                total_fetched = ?,
                total_stored = ?,
                updated_at = ?,
                config_json = ?
            WHERE job_name = ?
            """,
            (
                job.format_id,
                job.user_filter,
                job.min_elo,
                job.max_elo,
                job.last_timestamp,
                job.status,
                job.total_fetched,
                job.total_stored,
                job.updated_at,
                job.config_json,
                job.job_name,
            ),
        )
        self.conn.commit()

    def delete_job(self, job_name: str) -> bool:
        """Delete a scraper job."""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM scraper_state WHERE job_name = ?", (job_name,))
        self.conn.commit()
        return cursor.rowcount > 0

    def set_job_status(self, job_name: str, status: str):
        """Set the status of a job."""
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE scraper_state SET status = ?, updated_at = ? WHERE job_name = ?",
            (status, int(time.time()), job_name),
        )
        self.conn.commit()

    # Log operations

    def add_log(self, entry: LogEntry):
        """Add a log entry."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO scrape_log (job_name, timestamp, level, message)
            VALUES (?, ?, ?, ?)
            """,
            (entry.job_name, entry.timestamp, entry.level, entry.message),
        )
        self.conn.commit()

    def get_logs(
        self, job_name: Optional[str] = None, limit: int = 100
    ) -> list[LogEntry]:
        """Get log entries."""
        cursor = self.conn.cursor()
        if job_name:
            cursor.execute(
                """
                SELECT * FROM scrape_log
                WHERE job_name = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (job_name, limit),
            )
        else:
            cursor.execute(
                "SELECT * FROM scrape_log ORDER BY timestamp DESC LIMIT ?", (limit,)
            )

        return [
            LogEntry(
                id=row["id"],
                job_name=row["job_name"],
                timestamp=row["timestamp"],
                level=row["level"],
                message=row["message"],
            )
            for row in cursor.fetchall()
        ]
