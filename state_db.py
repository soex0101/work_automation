#!/usr/bin/env python3
"""Update markers and update history, kept in the alkis_scraper database.

Two tables in its public schema, created from sql/state_tables.sql on first
use if they do not exist yet:

    update_state    one marker per state and job, as that job's download saw it
    update_history  one row per successful database update: state, job,
                    started, finished, elapsed

check_update.py compares each portal against the state's newest marker; the
history is for analysing how often each state's database is updated (the
update_frequency view). Each write is one statement, so runs recording at the
same time need no lock of their own.

Connection details are config.ini's [database], with [database] state_db naming
the database.
"""

from pathlib import Path

import psycopg2
from psycopg2.extras import Json

from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, STATE_DB

# A database busy with an import's postprocessing should still answer within
# this; one that does not is down, and saying so beats hanging the run.
CONNECT_TIMEOUT = 30

SCHEMA_SQL = Path(__file__).resolve().parent / 'sql' / 'state_tables.sql'
OBJECTS = ('update_state', 'update_history', 'update_frequency')   # in public

_tables_ready = False   # checked once per process


def connect():
    return psycopg2.connect(dbname=STATE_DB, user=DB_USER, password=DB_PASSWORD,
                            host=DB_HOST or None, port=DB_PORT or None,
                            connect_timeout=CONNECT_TIMEOUT,
                            application_name='alkis-automation')


def ensure_tables(cur):
    """Create the tables and the view from SCHEMA_SQL if any of them is missing.

    Looked up first rather than relying on IF NOT EXISTS alone, so a login without
    CREATE rights still works once the tables are there. The advisory lock keeps
    two runs starting together from both creating them.
    """
    # A plain catalog query, not to_regclass(): that answers from this session's
    # cache, which waiting on an advisory lock does not refresh, so the second
    # look below would not see tables another run has just created.
    exists = ("SELECT count(*) = %s FROM pg_class "
              "WHERE relnamespace = 'public'::regnamespace AND relname = ANY(%s)")
    params = (len(OBJECTS), list(OBJECTS))
    cur.execute(exists, params)
    if cur.fetchone()[0]:
        return
    cur.execute("SELECT pg_advisory_xact_lock(hashtext('alkis-automation state tables'))")
    # Again under the lock: a run that got there first may have created them.
    cur.execute(exists, params)
    if not cur.fetchone()[0]:
        cur.execute(SCHEMA_SQL.read_text())
        print('created update_state / update_history in %s' % STATE_DB)


def execute(sql, params=()):
    """Run one statement in its own transaction; return its rows, if any."""
    global _tables_ready
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            if not _tables_ready:
                ensure_tables(cur)
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
        # Only once committed: a rolled-back creation has to be tried again.
        _tables_ready = True
        return rows
    finally:
        conn.close()


def latest_markers():
    """{state: {'marker', 'job', 'recorded'}} from each state's newest update_state row.

    Raises if the database cannot be read, never returns {} for it: an empty
    answer means "never recorded", which would report every state as needing an
    update.
    """
    rows = execute('SELECT DISTINCT ON (state) state, job, marker, recorded '
                   'FROM public.update_state ORDER BY state, recorded DESC')
    return {state: {'marker': marker, 'job': job, 'recorded': recorded}
            for state, job, marker, recorded in rows}


def record_marker(state, job, marker):
    """Store what job downloaded for state. Recording the same job again replaces it."""
    execute('INSERT INTO public.update_state (state, job, marker) VALUES (%s, %s, %s) '
            'ON CONFLICT (state, job) DO UPDATE '
            'SET marker = EXCLUDED.marker, recorded = now()',
            (state, job, Json(marker)))


def record_update(state, job, started, finished):
    """Store one successful database update. started / finished are local times."""
    # astimezone() makes naive local times explicit, so the server's TimeZone
    # setting cannot shift them.
    execute('INSERT INTO public.update_history (state, job, started, finished) '
            'VALUES (%s, %s, %s, %s) ON CONFLICT (state, job) DO UPDATE '
            'SET started = EXCLUDED.started, finished = EXCLUDED.finished',
            (state, job, started.astimezone(), finished.astimezone()))
