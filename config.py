#!/usr/bin/env python3
"""Settings read from config.ini, shared by every script in the project.

Relative values in the ini are resolved against the directory holding this file,
so they mean the same thing regardless of the caller's working directory.
"""

import configparser
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / 'config.ini'

# interpolation=None: [promote]'s LIKE patterns contain '%', and so may a password.
_parser = configparser.ConfigParser(interpolation=None)
if not _parser.read(CONFIG_PATH):
    raise FileNotFoundError('missing configuration file: %s' % CONFIG_PATH)


def _path(section, option, fallback=None):
    raw = _parser.get(section, option, fallback=fallback)
    if raw is None:
        raise KeyError('config.ini: [%s] %s is not set' % (section, option))
    path = Path(raw.strip()).expanduser()
    return path if path.is_absolute() else (SCRIPT_DIR / path).resolve()


INVENTORY_DIR = _path('paths', 'inventory_dir')
STAGING_DIR = _path('paths', 'staging_dir', str(INVENTORY_DIR / '_downloads'))
BACKUP_DIR = _path('paths', 'backup_dir')
ALKIS_IMPORT = _path('paths', 'alkis_import')


# Environment wins over the ini so the password can stay out of the file in production.
DB_HOST = _parser.get('database', 'host', fallback='').strip()
DB_PORT = _parser.get('database', 'port', fallback='').strip()
DB_USER = os.environ.get('ALKIS_DB_USER') or _parser.get('database', 'user').strip()
DB_PASSWORD = (os.environ.get('ALKIS_DB_PASSWORD')
               or _parser.get('database', 'password').strip())
DB_MAINTENANCE = _parser.get('database', 'maintenance_db', fallback='postgres').strip()
# Holds update_state and update_history (state_db.py), on the same server.
STATE_DB = _parser.get('database', 'state_db', fallback='alkis_scraper').strip()

SWAP_RETRY_SECONDS = _parser.getint('swap', 'retry_seconds', fallback=30)
SWAP_RETRY_COUNT = _parser.getint('swap', 'retry_count', fallback=20)

# Retired <state>_alkis_<job>_prev databases kept as rollback targets.
KEEP_PREV_DB = _parser.getint('cleanup', 'keep_prev_db', fallback=1)

# Most imports cron.py lets run at once, counting ones an earlier tick started.
CONCURRENT_JOB_COUNT = _parser.getint('cron', 'concurrent_job_count', fallback=3)



# ---- automation_base_by.py / automation_base_st.py ----
# Optional here, so the rest of the project imports without these sections; each
# of those scripts checks for None and fails on any key it needs. They share
# DB_HOST / DB_PORT / DB_USER / DB_PASSWORD and BACKUP_DIR above.

def _lines(section, option):
    """A multi-line value as a list of its non-blank lines, or None if unset."""
    raw = _parser.get(section, option, fallback=None)
    if raw is None:
        return None
    return [line.strip() for line in raw.splitlines() if line.strip()]


# [promote]
CONN_WAIT_INTERVAL_SECONDS = _parser.getint('promote', 'conn_wait_interval_seconds', fallback=None)
CONN_WAIT_TIMEOUT_SECONDS = _parser.getint('promote', 'conn_wait_timeout_seconds', fallback=None)
FORCE_TERMINATE_APP_NAMES = _lines('promote', 'force_terminate_app_names')
FORCE_TERMINATE_EXCLUDE = _lines('promote', 'force_terminate_exclude')

# Bayern
BY_BUILDING_URLS = _lines('building', 'by_URLS')
BY_GPKG_URL = _parser.get('usage', 'by_GPKG_URL', fallback=None)
BY_TABLE_NAME = _parser.get('remote_database', 'TABLE_NAME', fallback=None)
BY_REMOTE_TABLE_NAME = _parser.get('remote_database', 'REMOTE_TABLE_NAME', fallback=None)
BY_REMOTE_PG_USER = _parser.get('remote_database', 'remote_pg_user', fallback=None)
BY_REMOTE_PG_PASSWORD = _parser.get('remote_database', 'remote_pg_password', fallback=None)
BY_REMOTE_PG_DATABASE = _parser.get('remote_database', 'remote_pg_database', fallback=None)
BY_REMOTE_PG_HOST = _parser.get('remote_database', 'remote_pg_host', fallback=None)
BY_REMOTE_PG_PORT = _parser.getint('remote_database', 'remote_pg_port', fallback=None)

# Sachsen-Anhalt
ST_GBIS_GEBAEUDE_ZIP_URL = _parser.get('building', 'st_GBIS_GEBAEUDE_ZIP_URL', fallback=None)
ST_GEBAEUDE_REF_ZIP_URL = _parser.get('building', 'st_GEBAEUDE_REF_ZIP_URL', fallback=None)
ST_BUILDING_BATCH_SIZE = _parser.getint('building', 'st_batch_size', fallback=None)
ST_PARCEL_ZIP_URL = _parser.get('parcel', 'st_ZIP_URL', fallback=None)
ST_PARCEL_BATCH_SIZE = _parser.getint('parcel', 'st_batch_size', fallback=None)
ST_PARCEL_POSTGIS = _parser.getboolean('parcel', 'st_postgis', fallback=None)
ST_USAGE_ZIP_URL = _parser.get('usage', 'st_ZIP_URL', fallback=None)
ST_USAGE_BATCH_SIZE = _parser.getint('usage', 'st_batch_size', fallback=None)
ST_USAGE_POSTGIS = _parser.getboolean('usage', 'st_postgis', fallback=None)


# Staging shares a namespace with the state directories, so overlapping the two
# would put partial downloads where the .lst walk can find them.
if STAGING_DIR == INVENTORY_DIR:
    raise ValueError('config.ini: staging_dir must differ from inventory_dir')
