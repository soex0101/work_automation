#!/usr/bin/env python3
"""Write the alkis-import job file (<state>_nas.lst) for a state.

The .lst format is the same everywhere -- a PG connection line, a block of
directives, the list of NAS files to import, then `postprocess`. States differ
only along the axes captured in STATES below, so the writing itself is shared.

Reads <inventory_dir>/<state>/<job>/ and writes
<backup_dir>/<state>/<job>/<state>_nas.lst, beside that job's logs. Acquiring the
data is the other half; see download.py.

Console output is mirrored to <backup_dir>/<state>/<job>/logs.log.

Usage:
    python3 make_nas.py --job-id=J nw           # write the .lst for job J
    python3 make_nas.py --job-id=J nw he th     # several
    python3 make_nas.py --job-id=J sh --dry-run # print it, write nothing at all
    python3 make_nas.py --list

Paths come from config.ini. Without --job-id= a fresh id is minted, which will not
match an existing download -- pass one when working against a real run.
"""

import argparse
import os
import sys
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

# INVENTORY_DIR is the unpacked NAS data; BACKUP_DIR holds everything else this job
# produces -- the .lst and all three logs.
from config import (INVENTORY_DIR, BACKUP_DIR,
                    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD)
from logtee import tee_unless_parent

TIME_FMT = '%Y-%m-%d %H:%M:%S'
JOB_FMT = '%Y%m%d_%H%M%S'

# Scopes the data read and the .lst written to one run. main.py passes the same id
# it gave download.py; run standalone, a fresh one is minted here (which will not
# match any existing download, so pass --job-id= when reading a real job).
JOB_ID = datetime.now().strftime(JOB_FMT)


def elapsed(start, end):
    """Whole-second wall clock between two datetimes, as H:MM:SS."""
    return timedelta(seconds=round((end - start).total_seconds()))


def live_db(state):
    """The name consumers read: <state>_alkis. Stable across every job."""
    return '%s_alkis' % state


def db_name(state, job_id, importing=False):
    """Per-job database, named off the live one: <state>_alkis_<job>.

    The importer writes into `<state>_alkis_<job>_importing`; main.py renames it to
    `<state>_alkis_<job>` once alkis-import exits cleanly, so a database still
    carrying the suffix means the import failed or is still running. The swap then
    makes it `<state>_alkis` and retires the old one under old_db() below.
    """
    name = '%s_%s' % (live_db(state), job_id)
    return name + '_importing' if importing else name


def old_db(state, job_id):
    """The database a job's swap retires: <state>_alkis_old_<job>.

    Built on live_db() like db_name(), so every database for a state stays under
    the one <state>_alkis prefix.
    """
    return '%s_old_%s' % (live_db(state), job_id)


def pg_connection(dbname):
    """The .lst's opening line: a GDAL PG connection string."""
    parts = ['dbname=%s' % dbname, 'user=%s' % DB_USER, 'password=%s' % DB_PASSWORD]
    if DB_HOST:
        parts.append('host=%s' % DB_HOST)
    if DB_PORT:
        parts.append('port=%s' % DB_PORT)
    return 'PG:' + ' '.join(parts)

# Both states ship rows that violate these NOT NULL constraints.
DROP_NOT_NULL = (
    'alter table ax_person alter nachnameoderfirma drop not null;',
    'alter table ax_anschrift alter ort_post drop not null;',
)


@dataclass(frozen=True)
class State:
    epsg: int
    # 'false' writes `usecopy false`; None omits the directive entirely.
    usecopy: str = 'false'
    # 'create' emits create+clean (full rebuild); 'update' emits update (differential).
    mode: str = 'create'
    # Extensions the .lst may reference. Empty tuple means every file found.
    extensions: tuple = ()
    # SQL run after create/clean, before the file list.
    execute: tuple = ()
    # Subdirectory walked for NAS files, relative to <inventory_dir>/<state>/<job>/.
    # None walks that directory itself.
    source: str = None
    # Files written ahead of the walk, relative to <inventory_dir>/<state>/<job>/.
    prepend: tuple = ()
    # Output filename. Defaults to <state>_nas.lst.
    lst_name: str = None

    def db_name(self, state):
        # An `update` runs differentially against the database that is already live,
        # so it names it directly -- there is nothing to create, promote or swap.
        # A `create`+`clean` build gets its own per-job database instead, which
        # main.py renames into place once the importer succeeds.
        if self.mode == 'update':
            return live_db(state)
        return db_name(state, JOB_ID, importing=True)

    def job_dir(self, state):
        return INVENTORY_DIR / state / JOB_ID

    def source_dir(self, state):
        base = self.job_dir(state)
        return base / self.source if self.source else base

    def out_path(self, state):
        # The .lst sits alongside that job's logs.
        return BACKUP_DIR / state / JOB_ID / (self.lst_name or '%s_nas.lst' % state)

    def log_path(self, state):
        # Named after the live database the import ultimately lands in.
        return BACKUP_DIR / state / JOB_ID / ('log_%s_alkis.log' % state)


STATES = {
    # Brandenburg
    'bb': State(epsg=25833),
    # Berlin -- differential update against an already-imported database.
    'be': State(epsg=25833, usecopy=None, mode='update',
                lst_name='be_update_nas.lst'),
    # Baden-Wuerttemberg
    'bw': State(epsg=25832, extensions=('.xml',)),
    # Bremen
    'hb': State(epsg=25832),
    # Hessen
    'he': State(epsg=25832),
    # Hamburg -- omits usecopy; see note in README/CLAUDE.md.
    'hh': State(epsg=25832, usecopy=None),
    # Mecklenburg-Vorpommern
    'mv': State(epsg=25833, extensions=('.xml',)),
    # Nordrhein-Westfalen -- omits usecopy; see note in README/CLAUDE.md.
    'nw': State(epsg=25832, usecopy=None),
    # Rheinland-Pfalz
    'rp': State(epsg=25832, extensions=('.xml',), execute=DROP_NOT_NULL),
    # Schleswig-Holstein -- the synthesized street-name catalogue must load first.
    'sh': State(epsg=25832, prepend=('katalogobjekte_bda_ax_lage.xml',)),
    # Saarland
    'sl': State(epsg=25832),
    # Sachsen
    'sn': State(epsg=25833),
    # Thueringen
    'th': State(epsg=25832, execute=DROP_NOT_NULL),
}
# Not covered by any source: by (Bayern), ni (Niedersachsen), st (Sachsen-Anhalt).


def collect_files(root, extensions=(), exclude=()):
    """Every file under root, sorted, optionally restricted by extension."""
    root = Path(root)
    if not root.is_dir():
        raise NotADirectoryError('no data directory: %s' % root)

    excluded = {Path(p).resolve() for p in exclude}
    found = []
    for current, _dirs, names in os.walk(root):
        for name in names:
            path = Path(current) / name
            if extensions and not name.endswith(extensions):
                continue
            if path.resolve() in excluded:
                continue
            found.append(path)

    return sorted(found)


def nas_lines(state, cfg, files):
    """Build the .lst as a list of lines, in the order alkis-import reads them."""
    lines = [pg_connection(cfg.db_name(state)),
             'epsg %d' % cfg.epsg]

    if cfg.usecopy is not None:
        lines.append('usecopy %s' % cfg.usecopy)

    lines.append('avoiddupes true')
    lines.append('skipfailuresregex .*')

    if cfg.mode == 'update':
        lines.append('update')
    else:
        lines.append('create')
        lines.append('clean')

    lines.extend('execute %s' % sql for sql in cfg.execute)
    lines.append('log %s' % cfg.log_path(state))

    lines.extend(str(f) for f in files)
    lines.append('postprocess')
    return lines


def build(state, dry_run=False):
    cfg = STATES[state]

    prepend = [cfg.job_dir(state) / p for p in cfg.prepend]
    for path in prepend:
        if not path.exists():
            raise FileNotFoundError('required file missing: %s' % path)

    # download.py gunzips the unpacked data, so nothing should still be compressed.
    # Refusing here rather than walking past them: with extensions=('.xml',) a
    # leftover .xml.gz would be dropped from the .lst silently, losing its content.
    leftover = sorted(cfg.source_dir(state).rglob('*.xml.gz'))
    if leftover:
        raise RuntimeError(
            '%d file(s) under %s are still gzipped, e.g. %s -- run download.py, or '
            'gunzip them, so the .lst lists plain XML'
            % (len(leftover), cfg.source_dir(state), leftover[0].name))

    # Excluded from the walk so a prepended file is not also listed a second time.
    walked = collect_files(cfg.source_dir(state), cfg.extensions, exclude=prepend)
    files = prepend + walked

    if not files:
        raise RuntimeError('no files found under %s' % cfg.source_dir(state))

    lines = nas_lines(state, cfg, files)
    out = cfg.out_path(state)

    if dry_run:
        print('--- %s (%d file(s)) ---' % (out, len(files)))
        head = len(lines) - len(files) - 1
        for line in lines[:head]:
            print(line)
        print('... %d file path(s) ...' % len(files))
        print(lines[-1])
        return out

    out.parent.mkdir(parents=True, exist_ok=True)
    # alkis-import will not create the log directory itself.
    cfg.log_path(state).parent.mkdir(parents=True, exist_ok=True)
    out.write_text('\n'.join(lines) + '\n')
    print('%s: %d file(s)' % (out, len(files)))
    return out


def state_arg(known):
    """An argparse `type` that accepts one of `known`, for a list of states.

    Not `choices=`: on Python 3.10 argparse validates a nargs='*' positional's
    empty default against choices too, so "no states given" fails as
    "invalid choice: []" instead of printing the help. `type` is applied to each
    argument string as it is read, so the empty case never reaches it. The
    message is argparse's own, wrapped as "argument STATE: ...".
    """
    def check(value):
        if value not in known:
            raise argparse.ArgumentTypeError(
                'invalid choice: %r (choose from %s)'
                % (value, ', '.join(known)))
        return value
    return check


def build_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # state_arg does the validation: an unknown abbreviation is rejected with the
    # full list, so no hand-rolled check is needed.
    parser.add_argument('states', nargs='*', type=state_arg(sorted(STATES)),
                        metavar='STATE', help='state abbreviations to write a .lst for')
    parser.add_argument('--job-id', metavar='ID',
                        help='job id to read and write under (default: minted)')
    parser.add_argument('--dry-run', action='store_true',
                        help='print the .lst header instead of writing anything')
    parser.add_argument('--list', action='store_true',
                        help='print the known state abbreviations and exit')
    return parser


def main(argv):
    global JOB_ID

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list:
        for abbr in sorted(STATES):
            print(abbr)
        return 0

    if not args.states:
        parser.print_help()
        return 0

    # An empty --job-id= would collapse every path to <root>/<state>/, so only a
    # non-empty value replaces the minted default.
    if args.job_id:
        JOB_ID = args.job_id

    dry_run, states = args.dry_run, args.states
    print('job     %s' % JOB_ID)
    overall = datetime.now()
    for abbr in states:
        started = datetime.now()
        log = BACKUP_DIR / abbr / JOB_ID / 'logs.log'
        if not dry_run:
            print('log     %s' % log)
        # --dry-run writes nothing at all, log included.
        with nullcontext() if dry_run else tee_unless_parent(log):
            print('=' * 20, abbr)
            print('start   %s' % started.strftime(TIME_FMT))
            try:
                build(abbr, dry_run=dry_run)
            finally:
                # Reported even when a state fails, so a partial run stays measurable.
                finished = datetime.now()
                print('end     %s' % finished.strftime(TIME_FMT))
                print('elapsed %s' % elapsed(started, finished))

    if len(states) > 1:
        print('=' * 20, 'total')
        print('elapsed %s' % elapsed(overall, datetime.now()))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
