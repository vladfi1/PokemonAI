"""CLI interface for Pokemon Showdown Replay Scraper."""

import json
import sys
from pathlib import Path
from typing import Optional

import click
import yaml

from .config import Config, ensure_config_exists
from .db import Database
from .scraper import LogFetcher, ReplayScraper
from .storage import LogStorage
from .utils import (
    format_number,
    format_percentage,
    format_relative_time,
    generate_job_name,
    parse_format_id,
    sanitize_job_name,
)


def get_config(ctx: click.Context) -> Config:
    """Get configuration from context."""
    return ctx.obj["config"]


def get_db(ctx: click.Context) -> Database:
    """Get database from context."""
    return ctx.obj["db"]


def get_log_storage(ctx: click.Context) -> LogStorage:
    """Get log storage from context."""
    return ctx.obj["log_storage"]


@click.group()
@click.option(
    "--config",
    "-c",
    "config_path",
    type=click.Path(),
    help="Path to configuration file.",
)
@click.pass_context
def cli(ctx: click.Context, config_path: Optional[str]):
    """Pokemon Showdown Replay Scraper - Fetch and store battle replays."""
    ctx.ensure_object(dict)

    # Load configuration
    config = Config.load(config_path)
    ctx.obj["config"] = config
    ctx.obj["config_path"] = config_path

    # Initialize database
    db = Database(config.storage.database_path)
    db.initialize()
    ctx.obj["db"] = db

    # Initialize log storage
    log_storage = LogStorage(config.storage.logs_path)
    ctx.obj["log_storage"] = log_storage


@cli.command()
@click.option("--format", "-f", "format_id", help="Battle format to scrape (e.g., gen9ou). Uses config default if not specified.")
@click.option("--user", "-u", help="Filter by username.")
@click.option("--min-elo", type=int, help="Minimum ELO rating. Uses config default if not specified.")
@click.option("--max-elo", type=int, help="Maximum ELO rating. Uses config default if not specified.")
@click.option("--job-name", "-j", help="Name for this scraping job.")
@click.option("--limit", "-l", type=int, help="Maximum number of replays to fetch.")
@click.option("--fetch-logs/--no-fetch-logs", default=None, help="Fetch full battle logs. Uses config default if not specified.")
@click.pass_context
def start(
    ctx: click.Context,
    format_id: Optional[str],
    user: Optional[str],
    min_elo: Optional[int],
    max_elo: Optional[int],
    job_name: Optional[str],
    limit: Optional[int],
    fetch_logs: Optional[bool],
):
    """Start a new scraping job."""
    config = get_config(ctx)
    db = get_db(ctx)
    log_storage = get_log_storage(ctx)

    # Apply config defaults for unspecified options
    if min_elo is None:
        min_elo = config.defaults.min_elo
    if max_elo is None:
        max_elo = config.defaults.max_elo
    if fetch_logs is None:
        fetch_logs = config.scraping.fetch_full_log

    # Use format from config defaults if not specified
    if format_id is None:
        format_id = config.defaults.format

    # Normalize format ID
    if format_id:
        format_id = parse_format_id(format_id)

    # Generate job name if not provided
    if not job_name:
        job_name = generate_job_name(format_id or "all", min_elo)
        job_name = sanitize_job_name(job_name)

    # Check if job already exists
    existing = db.get_job(job_name)
    if existing:
        if existing.status == "running":
            click.echo(f"Job '{job_name}' is already running. Use 'pause' or 'stop' first.")
            sys.exit(1)
        elif existing.status in ("paused", "idle"):
            click.echo(f"Job '{job_name}' exists. Use 'resume' to continue or 'stop' to remove it.")
            sys.exit(1)
        elif existing.status == "completed":
            click.echo(f"Job '{job_name}' is completed. Use 'stop' to remove it first.")
            sys.exit(1)

    # Create scraper and job
    scraper = ReplayScraper(config, db, log_storage)

    try:
        job = scraper.create_job(
            job_name=job_name,
            format_id=format_id,
            user_filter=user,
            min_elo=min_elo,
            max_elo=max_elo,
        )
    except ValueError as e:
        click.echo(f"Error: {e}")
        sys.exit(1)

    click.echo(f"Created job: {job_name}")

    # Run the job
    scraper.run_job(job_name, limit=limit, fetch_logs=fetch_logs)


@cli.command()
@click.option("--job-name", "-j", required=True, help="Name of the job to resume.")
@click.option("--limit", "-l", type=int, help="Maximum number of additional replays to fetch.")
@click.option("--fetch-logs/--no-fetch-logs", default=False, help="Fetch full battle logs.")
@click.pass_context
def resume(
    ctx: click.Context,
    job_name: str,
    limit: Optional[int],
    fetch_logs: bool,
):
    """Resume a paused, interrupted, or completed job."""
    config = get_config(ctx)
    db = get_db(ctx)
    log_storage = get_log_storage(ctx)

    job = db.get_job(job_name)
    if not job:
        click.echo(f"Job '{job_name}' not found.")
        sys.exit(1)

    if job.status == "running":
        click.echo(f"Job '{job_name}' is already running.")
        sys.exit(1)

    if job.status == "completed":
        click.echo(f"Continuing completed job: {job_name}")
    else:
        click.echo(f"Resuming job: {job_name}")
    click.echo(f"  Previous progress: {format_number(job.total_stored)} stored")

    scraper = ReplayScraper(config, db, log_storage)
    scraper.run_job(job_name, limit=limit, fetch_logs=fetch_logs)


@cli.command()
@click.option("--job-name", "-j", required=True, help="Name of the job to pause.")
@click.pass_context
def pause(ctx: click.Context, job_name: str):
    """Pause a running job."""
    db = get_db(ctx)

    job = db.get_job(job_name)
    if not job:
        click.echo(f"Job '{job_name}' not found.")
        sys.exit(1)

    if job.status != "running":
        click.echo(f"Job '{job_name}' is not running (status: {job.status}).")
        sys.exit(1)

    db.set_job_status(job_name, "paused")
    click.echo(f"Pausing job: {job_name}")
    click.echo("Note: The job will pause after completing the current batch.")


@cli.command()
@click.option("--job-name", "-j", help="Name of the job to stop.")
@click.option("--all", "all_jobs", is_flag=True, help="Stop and remove all jobs.")
@click.pass_context
def stop(ctx: click.Context, job_name: Optional[str], all_jobs: bool):
    """Stop and remove a job."""
    db = get_db(ctx)

    if all_jobs:
        jobs = db.get_all_jobs()
        if not jobs:
            click.echo("No jobs to remove.")
            return
        for job in jobs:
            db.delete_job(job.job_name)
            click.echo(f"Removed job: {job.job_name}")
        click.echo(f"Removed {len(jobs)} job(s).")
        click.echo("Note: Replays and logs already fetched remain stored.")
        return

    if not job_name:
        click.echo("Error: Must specify --job-name or --all.")
        sys.exit(1)

    job = db.get_job(job_name)
    if not job:
        click.echo(f"Job '{job_name}' not found.")
        sys.exit(1)

    db.delete_job(job_name)
    click.echo(f"Stopped and removed job: {job_name}")
    click.echo("Note: Replays and logs already fetched remain stored.")


@cli.command()
@click.option("--job-name", "-j", help="Show status for a specific job.")
@click.pass_context
def status(ctx: click.Context, job_name: Optional[str]):
    """Show status of scraping jobs."""
    db = get_db(ctx)
    log_storage = get_log_storage(ctx)

    if job_name:
        job = db.get_job(job_name)
        if not job:
            click.echo(f"Job '{job_name}' not found.")
            sys.exit(1)
        jobs = [job]
    else:
        jobs = db.get_all_jobs()

    if not jobs:
        click.echo("No scraping jobs found.")
    else:
        click.echo("Scraping Jobs:")
        click.echo("-" * 60)
        for job in jobs:
            match_rate = format_percentage(job.total_stored, job.total_fetched) if job.total_fetched > 0 else "N/A"
            last_update = format_relative_time(job.updated_at) if job.updated_at else "Never"

            click.echo(f"  Job: {job.job_name}")
            click.echo(f"    Status: {job.status}")
            click.echo(f"    Format: {job.format_id or 'all'}")
            if job.user_filter:
                click.echo(f"    User: {job.user_filter}")
            click.echo(f"    ELO Filter: >= {job.min_elo}" + (f", <= {job.max_elo}" if job.max_elo else ""))
            click.echo(f"    Progress: {format_number(job.total_fetched)} fetched / {format_number(job.total_stored)} stored ({match_rate} match rate)")
            click.echo(f"    Last Update: {last_update}")
            click.echo()

    # Database stats
    stats = db.get_replay_stats()
    click.echo("Database:")
    click.echo(f"  Total replays: {format_number(stats['total_replays'])}")
    click.echo(f"  With logs: {format_number(stats['with_logs'])} ({format_percentage(stats['with_logs'], stats['total_replays'])})")
    click.echo(f"  Without logs: {format_number(stats['without_logs'])}")
    click.echo(f"  Formats: {stats['formats']}")

    # Log storage stats
    log_stats = log_storage.get_stats()
    if log_stats["total_files"] > 0:
        click.echo()
        click.echo("Log Storage:")
        click.echo(f"  Total log files: {format_number(log_stats['total_files'])}")
        click.echo(f"  Total size: {log_stats['total_size_mb']:.1f} MB (compressed)")


@cli.command("fetch-logs")
@click.option("--limit", "-l", type=int, default=None, help="Maximum number of logs to fetch.")
@click.pass_context
def fetch_logs(ctx: click.Context, limit: int):
    """Fetch battle logs for replays that don't have them."""
    config = get_config(ctx)
    db = get_db(ctx)
    log_storage = get_log_storage(ctx)

    fetcher = LogFetcher(config, db, log_storage)
    count = fetcher.run(limit=limit)
    click.echo(f"Fetched {count} logs.")


@cli.command("update-log-sizes")
@click.pass_context
def update_log_sizes(ctx: click.Context):
    """Update log_size for replays that have logs but missing size info."""
    db = get_db(ctx)
    log_storage = get_log_storage(ctx)

    # Query replays with logs but no size recorded
    cursor = db.conn.cursor()
    cursor.execute(
        "SELECT id, format_id FROM replays WHERE log_fetched = 1 AND (log_size = 0 OR log_size IS NULL)"
    )
    rows = cursor.fetchall()

    if not rows:
        click.echo("No replays need log size updates.")
        return

    click.echo(f"Found {len(rows)} replays with missing log sizes...")

    updated = 0
    missing = 0
    for row in rows:
        replay_id = row["id"]
        format_id = row["format_id"]

        # Get the log file path and check size
        log_path = log_storage._get_log_path(replay_id, format_id)
        if log_path.exists():
            file_size = log_path.stat().st_size
            cursor.execute(
                "UPDATE replays SET log_size = ? WHERE id = ?",
                (file_size, replay_id),
            )
            updated += 1
        else:
            # Log file doesn't exist, mark as not fetched
            cursor.execute(
                "UPDATE replays SET log_fetched = 0, log_size = 0 WHERE id = ?",
                (replay_id,),
            )
            missing += 1

        if (updated + missing) % 1000 == 0:
            click.echo(f"  Progress: {updated + missing}/{len(rows)}")
            db.conn.commit()

    db.conn.commit()
    click.echo(f"Updated {updated} replay log sizes.")
    if missing > 0:
        click.echo(f"Marked {missing} replays as missing logs (files not found).")


@cli.command()
@click.option("--format", "-f", "format_id", help="Filter by format.")
@click.option("--min-elo", type=int, help="Minimum ELO rating.")
@click.option("--max-elo", type=int, help="Maximum ELO rating.")
@click.option("--player", "-p", help="Filter by player name.")
@click.option("--limit", "-l", type=int, default=10, help="Maximum number of results.")
@click.option("--output", "-o", type=click.Path(), help="Output to JSON file.")
@click.pass_context
def query(
    ctx: click.Context,
    format_id: Optional[str],
    min_elo: Optional[int],
    max_elo: Optional[int],
    player: Optional[str],
    limit: int,
    output: Optional[str],
):
    """Query stored replays."""
    db = get_db(ctx)

    if format_id:
        format_id = parse_format_id(format_id)

    replays = db.query_replays(
        format_id=format_id,
        min_elo=min_elo,
        max_elo=max_elo,
        player=player,
        limit=limit,
    )

    if not replays:
        click.echo("No replays found matching criteria.")
        return

    if output:
        # Export to JSON
        data = [r.to_dict() for r in replays]
        with open(output, "w") as f:
            json.dump(data, f, indent=2)
        click.echo(f"Exported {len(replays)} replays to {output}")
    else:
        # Display summary
        click.echo(f"Found {len(replays)} replays:")
        click.echo("-" * 60)
        for replay in replays:
            has_log = "yes" if replay.log_fetched else "no"
            click.echo(f"  {replay.id}")
            click.echo(f"    {replay.p1_name} vs {replay.p2_name}")
            click.echo(f"    Rating: {replay.rating}, Views: {replay.views}, Log: {has_log}")
            click.echo()


@cli.command()
@click.option("--format", "-f", "format_id", help="Filter by format.")
@click.option("--output", "-o", type=click.Path(), required=True, help="Output directory.")
@click.option("--with-logs-only", is_flag=True, help="Only export replays that have logs.")
@click.pass_context
def export(
    ctx: click.Context,
    format_id: Optional[str],
    output: str,
    with_logs_only: bool,
):
    """Export replays to files."""
    db = get_db(ctx)
    log_storage = get_log_storage(ctx)

    if format_id:
        format_id = parse_format_id(format_id)

    # Get all replays (with pagination)
    all_replays = []
    offset = 0
    batch_size = 1000

    while True:
        replays = db.query_replays(
            format_id=format_id,
            limit=batch_size,
            offset=offset,
        )
        if not replays:
            break
        all_replays.extend(replays)
        offset += batch_size

    if with_logs_only:
        all_replays = [r for r in all_replays if r.log_fetched]

    if not all_replays:
        click.echo("No replays to export.")
        return

    # Create output directory
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    # Export metadata
    metadata = [r.to_dict() for r in all_replays]
    metadata_file = output_path / "metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)

    # Export logs from storage
    logs_dir = output_path / "logs"
    logs_dir.mkdir(exist_ok=True)

    exported_logs = 0
    for replay in all_replays:
        if replay.log_fetched:
            log_content = log_storage.load(replay.id, replay.format_id)
            if log_content:
                log_file = logs_dir / f"{replay.id}.log"
                with open(log_file, "w") as f:
                    f.write(log_content)
                exported_logs += 1

    click.echo(f"Exported {len(all_replays)} replays to {output}")
    click.echo(f"  Metadata: {metadata_file}")
    click.echo(f"  Logs: {exported_logs} files in {logs_dir}")


@cli.group()
def config():
    """Manage configuration."""
    pass


@config.command("show")
@click.pass_context
def config_show(ctx: click.Context):
    """Show current configuration."""
    cfg = get_config(ctx)
    click.echo(yaml.dump(cfg.to_dict(), default_flow_style=False, sort_keys=False))


@config.command("set")
@click.argument("key")
@click.argument("value")
@click.pass_context
def config_set(ctx: click.Context, key: str, value: str):
    """Set a configuration value."""
    cfg = get_config(ctx)
    config_path = ctx.obj.get("config_path") or "config.yaml"

    # Parse the key and set the value
    parts = key.replace("-", "_").split(".")
    if len(parts) == 1:
        # Shorthand keys
        key_map = {
            "rate_limit": ("scraping", "rate_limit"),
            "default_min_elo": ("defaults", "min_elo"),
            "default_max_elo": ("defaults", "max_elo"),
            "fetch_full_log": ("scraping", "fetch_full_log"),
            "batch_size": ("scraping", "batch_size"),
            "database_path": ("storage", "database_path"),
            "logs_path": ("storage", "logs_path"),
        }
        if key.replace("-", "_") in key_map:
            parts = key_map[key.replace("-", "_")]
        else:
            click.echo(f"Unknown configuration key: {key}")
            sys.exit(1)

    # Set the value
    if len(parts) == 2:
        section, attr = parts
        section_obj = getattr(cfg, section, None)
        if section_obj is None:
            click.echo(f"Unknown configuration section: {section}")
            sys.exit(1)

        # Convert value to appropriate type
        current_value = getattr(section_obj, attr, None)
        if current_value is not None:
            if isinstance(current_value, bool):
                value = value.lower() in ("true", "1", "yes")
            elif isinstance(current_value, int):
                value = int(value)
            elif isinstance(current_value, float):
                value = float(value)

        setattr(section_obj, attr, value)

    # Save configuration
    ensure_config_exists(config_path)
    cfg.save(config_path)
    click.echo(f"Set {key} = {value}")


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
