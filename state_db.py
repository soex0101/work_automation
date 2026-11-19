#!/usr/bin/env python3
"""Update markers and update history, kept in the alkis_scraper database.

Two tables in its public schema, created from sql/state_tables.sql on first
use if they do not exist yet:

    update_state    one marker per state and job, as that job's download saw it
    update_history  one row per main.py run: state, job, started, finished,
                    elapsed, status (PROCESSING / COMPLETE / FAILED),
                    error_message

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

# Advisory lock namespace: main.py holds (RUN_LOCK, update_history.pid) for as
# long as its run lives. Postgres frees it however the process ends, SIGKILL and
# reboots included, so a PROCESSING row whose lock is free has no run behind it.
RUN_LOCK = 'alkis-automation run'


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


def start_update(state, job, started):
    """Write the job's PROCESSING row and take its run lock; return the
    connection holding the lock.

    The caller keeps it open until the run's closing record_update() is
    written, then closes it. Row and lock commit together, so the row is never
    seen without its lock.
    """
    global _tables_ready
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            if not _tables_ready:
                ensure_tables(cur)
            cur.execute('INSERT INTO public.update_history (state, job, started, status) '
                        "VALUES (%s, %s, %s, 'PROCESSING') ON CONFLICT (state, job) DO UPDATE "
                        "SET started = EXCLUDED.started, finished = NULL, "
                        "status = 'PROCESSING', error_message = NULL RETURNING pid",
                        (state, job, started.astimezone()))
            pid = cur.fetchone()[0]
            # Session-level, so it outlives this transaction. Taken only if free:
            # held means another live main.py is running this very state and job.
            cur.execute('SELECT pg_try_advisory_lock(hashtext(%s), %s)', (RUN_LOCK, pid))
            if not cur.fetchone()[0]:
                raise RuntimeError('%s job %s is already running in another main.py'
                                   % (state, job))
        _tables_ready = True
        return conn
    except BaseException:
        conn.close()
        raise


def fail_dead_jobs():
    """Set FAILED every PROCESSING row whose main.py is gone; return
    [(state, job, error_message)] for each.

    Gone means its run lock is free: the process ended without writing its
    result -- SIGKILL, a crash, or the server going down. finished is left
    NULL, since when it died is not known.
    """
    return execute(
        # MATERIALIZED, so the lock is only tried for PROCESSING rows.
        "WITH processing AS MATERIALIZED ("
        "    SELECT pid FROM public.update_history WHERE status = 'PROCESSING') "
        "UPDATE public.update_history h SET status = 'FAILED', error_message = "
        "    CASE WHEN pg_postmaster_start_time() > h.started "
        "    THEN 'main.py lost: the database server restarted at ' "
        "         || to_char(pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS') "
        "         || ' while the job ran (server reboot?)' "
        "    ELSE 'main.py ended without recording a result (killed with SIGKILL or crashed)' "
        "    END || '; found by cron.py at ' || to_char(now(), 'YYYY-MM-DD HH24:MI:SS') "
        "FROM processing p "
        "WHERE h.pid = p.pid AND pg_try_advisory_xact_lock(hashtext(%s), p.pid::int) "
        "RETURNING h.state, h.job, h.error_message",
        (RUN_LOCK,))


def running_jobs():
    """{state: {'job', 'started'}} for every state with an update_history row
    still PROCESSING; the newest, if a state has more than one.
    """
    rows = execute("SELECT state, job, started FROM public.update_history "
                   "WHERE status = 'PROCESSING' ORDER BY started")
    return {state: {'job': job, 'started': started} for state, job, started in rows}


def record_marker(state, job, marker):
    """Store what job downloaded for state. Recording the same job again replaces it."""
    execute('INSERT INTO public.update_state (state, job, marker) VALUES (%s, %s, %s) '
            'ON CONFLICT (state, job) DO UPDATE '
            'SET marker = EXCLUDED.marker, recorded = now()',
            (state, job, Json(marker)))


def record_update(state, job, started, status, finished=None, error_message=None):
    """Write the job's update_history row: PROCESSING at the start, then
    COMPLETE or FAILED. started / finished are local times.

    An upsert, so the closing write still lands if the opening one was lost.
    """
    # astimezone() makes naive local times explicit, so the server's TimeZone
    # setting cannot shift them.
    execute('INSERT INTO public.update_history '
            '(state, job, started, finished, status, error_message) '
            'VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (state, job) DO UPDATE '
            'SET started = EXCLUDED.started, finished = EXCLUDED.finished, '
            'status = EXCLUDED.status, error_message = EXCLUDED.error_message',
            (state, job, started.astimezone(),
             finished.astimezone() if finished else None, status, error_message))
