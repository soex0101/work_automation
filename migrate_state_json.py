#!/usr/bin/env python3
"""One-time move of update_state.json / update_history.json into alkis_scraper.

    python3 migrate_state_json.py

Creates the tables (sql/state_tables.sql), then copies in every state's marker
from update_state.json -- one row per state, under the job that recorded it and
at the time it was checked -- and every entry of update_history.json, if that
file exists. Rows already in the tables are left alone, so it is safe to run
again.

Run it BEFORE the new code's first check: with no row for a state, check_update
reports it as needing an update, and cron.py would start every state at once.
The JSON files are not touched; delete them once the tables look right.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

from psycopg2.extras import Json

import state_db

SCRIPT_DIR = Path(__file__).resolve().parent
STATE_JSON = SCRIPT_DIR / 'update_state.json'
HISTORY_JSON = SCRIPT_DIR / 'update_history.json'
SCHEMA_SQL = SCRIPT_DIR / 'sql' / 'state_tables.sql'
TIME_FMT = '%Y-%m-%d %H:%M:%S'


def local(text):
    """A 'YYYY-MM-DD HH:MM:SS' local time from the JSON, made timezone-aware."""
    return datetime.strptime(text, TIME_FMT).astimezone()


def load(path):
    return json.loads(path.read_text()) if path.is_file() and path.read_text().strip() else {}


def main():
    states = load(STATE_JSON)
    history = load(HISTORY_JSON)

    conn = state_db.connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(SCHEMA_SQL.read_text())
            print('tables ready in %s' % state_db.STATE_DB)

            added = 0
            for state, entry in sorted(states.items()):
                cur.execute('INSERT INTO public.update_state (state, job, marker, recorded) '
                            'VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING',
                            (state, entry['job'], Json(entry['marker']), local(entry['checked'])))
                added += cur.rowcount
            print('update_state: %d of %d state(s) added from %s'
                  % (added, len(states), STATE_JSON.name))

            added = total = 0
            for state, entries in sorted(history.items()):
                for e in entries:
                    total += 1
                    cur.execute('INSERT INTO public.update_history (state, job, started, finished) '
                                'VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING',
                                (state, e['job'], local(e['started']), local(e['finished'])))
                    added += cur.rowcount
            print('update_history: %d of %d entr(ies) added from %s'
                  % (added, total, HISTORY_JSON.name))
    finally:
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
