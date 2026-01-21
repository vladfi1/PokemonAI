"""SQL schema definitions for Pokemon Showdown Replay Scraper."""

SCHEMA_VERSION = 3

CREATE_REPLAYS_TABLE = """
CREATE TABLE IF NOT EXISTS replays (
    id TEXT PRIMARY KEY,
    format_id TEXT NOT NULL,
    p1_name TEXT NOT NULL,
    p2_name TEXT NOT NULL,
    p1_id TEXT NOT NULL,
    p2_id TEXT NOT NULL,
    rating INTEGER DEFAULT 0,
    upload_time INTEGER NOT NULL,
    views INTEGER DEFAULT 0,
    log_fetched BOOLEAN DEFAULT 0,
    log_size INTEGER DEFAULT 0,
    scraped_at INTEGER NOT NULL,
    UNIQUE(id)
);
"""

# Migration from v1 to v2: remove log column (logs now stored as files)
MIGRATION_V1_TO_V2 = """
ALTER TABLE replays DROP COLUMN log;
"""

# Migration from v2 to v3: add log_size column
MIGRATION_V2_TO_V3 = """
ALTER TABLE replays ADD COLUMN log_size INTEGER DEFAULT 0;
"""

CREATE_REPLAYS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_format ON replays(format_id);",
    "CREATE INDEX IF NOT EXISTS idx_rating ON replays(rating);",
    "CREATE INDEX IF NOT EXISTS idx_upload_time ON replays(upload_time);",
    "CREATE INDEX IF NOT EXISTS idx_players ON replays(p1_id, p2_id);",
    "CREATE INDEX IF NOT EXISTS idx_log_fetched ON replays(log_fetched);",
]

CREATE_SCRAPER_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS scraper_state (
    id INTEGER PRIMARY KEY,
    job_name TEXT UNIQUE NOT NULL,
    format_id TEXT,
    user_filter TEXT,
    min_elo INTEGER DEFAULT 0,
    max_elo INTEGER,
    last_timestamp INTEGER,
    status TEXT DEFAULT 'idle',
    total_fetched INTEGER DEFAULT 0,
    total_stored INTEGER DEFAULT 0,
    started_at INTEGER,
    updated_at INTEGER,
    config_json TEXT
);
"""

CREATE_SCRAPE_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL
);
"""

CREATE_SCRAPE_LOG_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_log_job ON scrape_log(job_name);",
    "CREATE INDEX IF NOT EXISTS idx_log_timestamp ON scrape_log(timestamp);",
]

CREATE_SCHEMA_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""

ALL_TABLES = [
    CREATE_REPLAYS_TABLE,
    CREATE_SCRAPER_STATE_TABLE,
    CREATE_SCRAPE_LOG_TABLE,
    CREATE_SCHEMA_VERSION_TABLE,
]

ALL_INDEXES = CREATE_REPLAYS_INDEXES + CREATE_SCRAPE_LOG_INDEXES
