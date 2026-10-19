#!/usr/bin/env python3
"""Run the full ALKIS pipeline for one state.

Mints a job id from the current time, then:

    unpack --be-update-path into staging   (be only; it has no download)
    be: dump + CREATE DATABASE ... TEMPLATE be_alkis, before the import
    download.py --job-id=<id> <state>      acquire and unpack
    sh_ax_lage.py <inventory>/sh/<id>      (Schleswig-Holstein only)
    make_nas.py --job-id=<id> <state>      write the .lst
    check every listed file is well-formed XML
    zip the files the .lst references      -> backup/<state>/<id>/nas.zip
    CREATE DATABASE / CREATE EXTENSION     fresh db with postgis
    bash <alkis_import> <lst>              run the import
    delete every older job's data and staging folders
    ALTER DATABASE ... RENAME TO ...       drop the _importing suffix
    pg_dump -Fc this job's database        -> backup/<state>/<id>/<db>.dump
    swap into <state>_alkis                once nothing is connected to it
    pg_dump -Fc the retired database       -> backup/<state>/<id>/<state>_old_<id>.dump
    drop surplus <state>_old_<id> databases

Every stage is a subprocess; a non-zero exit aborts the run, so a failed download
never reaches the importer. Paths come from config.ini.

The importer loads into <state>_alkis_<job>_importing; that database is renamed to
<state>_alkis_<job> only after alkis-import exits 0, so a name still carrying the
suffix marks an import that failed or is still running. It then becomes the live
<state>_alkis, and whatever was live is retired to <state>_old_<job> and dumped.

Usage:
    python3 main.py nw                     # one state, end to end
    python3 main.py --job-id=J nw          # use J instead of minting one
    python3 main.py --be-update-path=d.zip be   # Berlin, from a delivery archive
    python3 main.py by                     # Bayern / Sachsen-Anhalt, via their own scripts
    python3 main.py --list                 # known state abbreviations

Exactly one state per run: several at once shared a job id and ran back to back,
which made one long log and one failure that stopped the states behind it. Run
them as separate invocations instead, passing --job-id= if they should share an id.

Berlin has no download stage: --be-update-path names its delivery archive (.zip,
.7z, .tar.gz or a single .gz), which is unpacked into that job's staging directory
for be() to gunzip. It is required for be and rejected for every other state.

by and st are not run through the chain above at all: each is handed to its own
self-contained script (automation_base_by.py / automation_base_st.py), which does its
own download, load, promotion, backups and cleanup. main.py passes the job id through
and captures the output into logs.log.

Without --job-id= a fresh id is minted from the clock. Supplying one is for pinning
the id -- naming a run to match something external, or lining several invocations
up under one id. It does not resume: the chain always starts at download, and
CREATE DATABASE fails if that job's database already exists.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
import zipfile
from xml.parsers import expat
from datetime import datetime, timedelta
from pathlib import Path

from config import (INVENTORY_DIR, STAGING_DIR, BACKUP_DIR, ALKIS_IMPORT,
                    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_MAINTENANCE,
                    SWAP_RETRY_SECONDS, SWAP_RETRY_COUNT, KEEP_PREV_DB)
import download
import logtee
import make_nas
from logtee import tee_output
from make_nas import db_name, live_db, old_db

SCRIPT_DIR = Path(__file__).resolve().parent

TIME_FMT = '%Y-%m-%d %H:%M:%S'

# How job ids are minted. Nothing downstream parses one -- folders are ordered by
# stat time and databases by birth time -- so this may be changed to a hash or any
# other scheme without touching the rest of the code.
JOB_FMT = '%Y%m%d_%H%M%S'

# Written as a comment on every retired database, so cleanup can recognise its own
# work regardless of what the job id looks like.
PREV_MARKER = 'alkis-retired-job:'


def elapsed(start, end):
    """Whole-second wall clock between two datetimes, as H:MM:SS."""
    return timedelta(seconds=round((end - start).total_seconds()))


def run(command, cwd=None, env=None):
    """Run a stage, echoing it first, and relay its output line by line.

    Output is written through sys.stdout, which is the tee while a state is running,
    so a subprocess's console output lands in logs.log as well -- including
    alkis-import's, which writes nothing there itself. Streaming rather than
    capturing keeps a multi-hour import visible as it goes.
    """
    print('+ %s' % ' '.join(str(c) for c in command))

    process = subprocess.Popen([str(c) for c in command], cwd=cwd, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, bufsize=1)
    with process.stdout:
        for line in process.stdout:
            sys.stdout.write(line)

    code = process.wait()
    if code != 0:
        raise subprocess.CalledProcessError(code, command)


# States handled by a self-contained script rather than by this pipeline. Such a script
# owns its whole chain -- download, load, promotion, backups, cleanup -- so main.py only
# runs it. None of the steps below apply to these states; in particular our cleanup
# would be destructive, because these scripts use <staging_dir>/<state>/ as a flat
# working directory rather than a set of job folders.
EXTERNAL = {
    'by': 'automation_base_by.py',   # Bayern: building, usage
    'st': 'automation_base_st.py',   # Sachsen-Anhalt: building, parcel, usage
}


def run_external(state, job_id):
    """Run a self-contained state script, passing this run's job id through.

    The job id is positional there, not --job-id.
    """
    run([sys.executable, SCRIPT_DIR / EXTERNAL[state], job_id])


# Every abbreviation main.py accepts: this pipeline's states and the external ones.
ALL_STATES = sorted(set(download.STATES) | set(EXTERNAL))


def stage_be_delivery(job_id, archive):
    """Unpack the Berlin delivery into that job's staging directory.

    Berlin has no network stage, so this stands in for the download: download.py's
    be() then finds the *.xml.gz here. The path is built from config rather than
    download.staging_dir(), whose JOB_ID is this process's own freshly minted one
    rather than the job actually being run.
    """
    target = STAGING_DIR / 'be' / job_id
    download.extract_delivery(archive, target)
    return target


def lst_files(state, job_id):
    """The data files a .lst references, in the order the importer will read them.

    Data paths are absolute. No directive line starts with '/': they are either bare
    words (`create`) or keyword-first (`log /...`, `PG:dbname=...`), which also keeps
    the password on line 1 out of the list.
    """
    return [Path(l) for l in lst_path(state, job_id).read_text().splitlines()
            if l.startswith('/')]


def validate_xml(path, chunk=1024 * 1024):
    """Raise if path is not well-formed XML.

    expat parses without building a tree, so memory stays flat regardless of file
    size -- a single NAS file runs to hundreds of megabytes. This is a
    well-formedness check only: every tag closed and properly nested, entities and
    encoding valid. It says nothing about whether the content is valid NAS.
    """
    parser = expat.ParserCreate()
    with open(path, 'rb') as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                parser.Parse(b'', True)     # final call closes the document
                return
            parser.Parse(block, False)


def validate_nas_files(state, job_id, report_limit=20):
    """Check every file the .lst lists parses as XML, before the importer sees it.

    alkis-import runs with `skipfailuresregex .*`, so it swallows per-feature errors
    and a truncated file can import partially without the run ever looking wrong.
    Catching it here turns that into a stopped run with a named file and a line
    number.
    """
    files = lst_files(state, job_id)
    if not files:
        print('no files listed in %s' % lst_path(state, job_id))
        return []

    started = datetime.now()
    total = 0
    bad = []
    for path in files:
        try:
            validate_xml(path)
            total += path.stat().st_size
        except (expat.ExpatError, OSError) as exc:
            bad.append((path, exc))

    took = elapsed(started, datetime.now())
    if not bad:
        print('xml ok: %d file(s), %.1f GiB, %s'
              % (len(files), total / (1024 ** 3), took))
        return []

    print('MALFORMED XML in %d of %d file(s):' % (len(bad), len(files)))
    for path, exc in bad[:report_limit]:
        print('  %s: %s' % (path, exc))
    if len(bad) > report_limit:
        print('  ... and %d more' % (len(bad) - report_limit))
    raise RuntimeError('%d malformed XML file(s) for %s; not importing'
                       % (len(bad), state))


def archive_nas_files(state, job_id):
    """Bundle every file the .lst references into <backup_dir>/<state>/<job>/nas.zip.

    Reads the paths back out of the .lst rather than walking the data directory, so
    the archive contains exactly what the importer was given -- which for bw, mv and
    rp is a filtered subset, and for sh includes the synthesized catalogue.
    """
    lst = lst_path(state, job_id)
    files = lst_files(state, job_id)
    if not files:
        print('no files listed in %s' % lst)
        return None

    root = INVENTORY_DIR / state / job_id
    target = lst.parent / 'nas.zip'
    tmp = target.with_suffix('.zip.part')

    # ZIP_DEFLATED here, unlike downloads.zip: NAS payloads are plain XML and
    # compress by roughly an order of magnitude.
    with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for path in files:
            try:
                arcname = path.relative_to(root)
            except ValueError:
                # Anything outside the job directory keeps its path, minus the root.
                arcname = Path(*path.parts[1:])
            zf.write(path, arcname)

    tmp.replace(target)
    print('archived %d NAS file(s) -> %s (%.1f MiB)'
          % (len(files), target, target.stat().st_size / (1024 * 1024)))
    return target


# Directory names under <inventory_dir>/<state>/ that are not job folders. Empty
# since Berlin's delivery moved to --be-update-path and unpacks into staging; kept
# as the hook for any future non-job directory.
NOT_A_JOB = set()


def prune_jobs(base, current):
    """Delete every job folder under base except `current`.

    No ordering is involved: the job to keep is named, not inferred from stat
    times, so the job id stays opaque and a folder with an odd timestamp cannot
    outlive its turn. Eligibility is by exclusion: NOT_A_JOB names and the
    current job are never touched.
    """
    if not base.is_dir():
        return []

    removed = []
    for old in sorted(base.iterdir()):
        if not old.is_dir() or old.name in NOT_A_JOB or old.name == current:
            continue
        shutil.rmtree(old)
        removed.append(old)
    return removed


def cleanup_old_jobs(state, job_id):
    """Keep this job's data for this state and drop every older job folder.

    Covers the unpacked data and the staged archives only. Nothing is lost by
    deleting them: an earlier run's staged archives are in its downloads.zip and
    the files its .lst listed are in its nas.zip, both kept for good under
    backup_dir -- so there is no retention setting here, and the volume carries
    one run's worth of bulk rather than several.
    """
    removed = (prune_jobs(INVENTORY_DIR / state, job_id)
               + prune_jobs(STAGING_DIR / state, job_id))

    for path in removed:
        print('removed %s' % path)
    print('cleanup: removed %d old job folder(s), keeping only %s'
          % (len(removed), job_id))
    return removed


def conn_args():
    """Shared psql/pg_dump connection flags from config.ini's [database].

    host and port are omitted when blank, which selects the local Unix socket.
    """
    args = ['-U', DB_USER]
    if DB_HOST:
        args += ['-h', DB_HOST]
    if DB_PORT:
        args += ['-p', DB_PORT]
    return args


def db_env():
    # Password goes through the environment, never argv -- argv is world-readable in ps.
    return dict(os.environ, PGPASSWORD=DB_PASSWORD)


def psql(sql, dbname):
    """Run one statement. Connection details come from config.ini's [database]."""
    run(['psql', '-v', 'ON_ERROR_STOP=1'] + conn_args() + ['-d', dbname, '-c', sql],
        env=db_env())


def create_database(state, job_id):
    """Create the job's database and enable PostGIS, before the importer runs.

    Deliberately not IF NOT EXISTS: a job id is unique per run, so an existing
    database means something is wrong and should stop the run.
    """
    name = db_name(state, job_id, importing=True)
    psql('CREATE DATABASE "%s";' % name, DB_MAINTENANCE)
    psql('CREATE EXTENSION IF NOT EXISTS postgis;', name)
    print('database %s created with postgis' % name)


def promote_database(state, job_id):
    """Drop the _importing suffix now that alkis-import has finished cleanly.

    Runs against the maintenance database, since a database cannot be renamed while
    connected to it, and fails if anything still holds a session on the source.
    """
    source = db_name(state, job_id, importing=True)
    target = db_name(state, job_id)
    psql('ALTER DATABASE "%s" RENAME TO "%s";' % (source, target), DB_MAINTENANCE)
    print('database %s -> %s' % (source, target))


def psql_ok(sql):
    """Run a statement against the maintenance db, reporting failure instead of raising.

    Used for cleanup, where one undroppable database should not abort a run that has
    already imported and swapped successfully.
    """
    result = subprocess.run(
        ['psql', '-v', 'ON_ERROR_STOP=1'] + conn_args() + ['-d', DB_MAINTENANCE, '-c', sql],
        env=db_env(), capture_output=True, text=True)
    if result.returncode != 0 and result.stderr.strip():
        print(result.stderr.strip())
    return result.returncode == 0


def prev_databases(state):
    """Retired databases for this state, oldest first.

    Identified by the comment the swap writes, not by the shape of the name -- the
    job id is opaque, so `<state>_old_something` alone cannot be trusted to be ours.
    Ordered by the database's birth time, read from the modification time of its
    PG_VERSION file, since pg_database records no creation timestamp. That needs
    permission to stat server files; without it, oid order stands in, which is
    assignment order and so very nearly creation order.
    """
    # Matches the old_db() shape. The prefix's own underscores are escaped too --
    # unescaped, `_` is a LIKE wildcard and nw_alkis would also match nwXalkis.
    prefix = live_db(state).replace('_', r'\_')
    where = ("datname LIKE '%s\\_old\\_%%' "
             "AND shobj_description(oid, 'pg_database') LIKE '%s%%'"
             % (prefix, PREV_MARKER))

    by_birth = ("SELECT datname FROM pg_database WHERE %s "
                "ORDER BY (pg_stat_file('base/' || oid || '/PG_VERSION')).modification;"
                % where)
    try:
        listed = psql_value(by_birth)
    except subprocess.CalledProcessError:
        print('cannot read database birth times (needs pg_read_server_files); '
              'ordering by oid instead')
        listed = psql_value("SELECT datname FROM pg_database WHERE %s ORDER BY oid;" % where)

    return [n for n in listed.splitlines() if n.strip()]


def prune_prev_databases(state):
    """Keep the newest KEEP_PREV_DB retired databases, dropping the rest."""
    names = prev_databases(state)   # oldest first

    # Not names[-KEEP_PREV_DB:] -- at 0 that is names[0:], i.e. everything, which
    # would keep them all instead of dropping them all.
    if KEEP_PREV_DB <= 0:
        keep, stale = [], names
    else:
        keep, stale = names[-KEEP_PREV_DB:], names[:-KEEP_PREV_DB]

    if not stale:
        print('retired databases: %d, nothing to drop (keep_prev_db=%d)'
              % (len(names), KEEP_PREV_DB))
        return []

    print('retired databases: %d, keeping %s' % (len(names), ', '.join(keep) or 'none'))

    dropped = []
    for name in stale:
        if psql_ok('DROP DATABASE "%s";' % name):
            print('dropped %s' % name)
            dropped.append(name)
        else:
            print('WARNING: could not drop %s -- left in place' % name)
    return dropped


def psql_value(sql):
    """Run a scalar query against the maintenance database and return it as text."""
    result = subprocess.run(
        ['psql', '-tA', '-v', 'ON_ERROR_STOP=1'] + conn_args()
        + ['-d', DB_MAINTENANCE, '-c', sql],
        env=db_env(), check=True, capture_output=True, text=True)
    return result.stdout.strip()


def database_exists(name):
    return psql_value("SELECT 1 FROM pg_database WHERE datname = '%s';" % name) == '1'


def connection_count(name):
    value = psql_value(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = '%s';" % name)
    return int(value or 0)


def wait_until_idle(name):
    """Poll until nothing is connected to name. True if it went idle in time."""
    for attempt in range(1, SWAP_RETRY_COUNT + 1):
        open_now = connection_count(name)
        if open_now == 0:
            print('check %d/%d: %s has no open connections'
                  % (attempt, SWAP_RETRY_COUNT, name))
            return True

        print('check %d/%d: %s has %d open connection(s), retrying in %ds'
              % (attempt, SWAP_RETRY_COUNT, name, open_now, SWAP_RETRY_SECONDS))
        time.sleep(SWAP_RETRY_SECONDS)

    return False


def swap_live_database(state, job_id):
    """Put this job's database in place as <state>_alkis, retiring the current one.

    Returns the retired database's name, or None on the first run for a state,
    when there is nothing to retire. Renaming needs the database idle, so the old
    one is polled until every session has gone.
    """
    live = live_db(state)
    fresh = db_name(state, job_id)

    if not database_exists(live):
        print('%s does not exist yet, promoting %s straight into place' % (live, fresh))
        psql('ALTER DATABASE "%s" RENAME TO "%s";' % (fresh, live), DB_MAINTENANCE)
        return None

    if not wait_until_idle(live):
        print('FAILED: %s still has open connections after %d checks over %ds; '
              'left %s in place and did not swap'
              % (live, SWAP_RETRY_COUNT, SWAP_RETRY_COUNT * SWAP_RETRY_SECONDS, fresh))
        raise RuntimeError('%s never went idle; swap abandoned' % live)

    previous = old_db(state, job_id)
    psql('ALTER DATABASE "%s" RENAME TO "%s";' % (live, previous), DB_MAINTENANCE)
    psql('ALTER DATABASE "%s" RENAME TO "%s";' % (fresh, live), DB_MAINTENANCE)
    # Marks the database as ours to retire later, and records the job id the name
    # alone cannot be trusted to carry.
    psql('COMMENT ON DATABASE "%s" IS \'%s%s\';' % (previous, PREV_MARKER, job_id),
         DB_MAINTENANCE)
    print('swapped: %s -> %s, then %s -> %s' % (live, previous, fresh, live))
    return previous


def clone_live_database(state, job_id):
    """Copy <state>_alkis to old_db(): the rollback database an in-place update lacks.

    A create+clean state keeps its predecessor as a renamed database; an update
    overwrites the live one, so the copy has to be taken before the importer runs.
    CREATE DATABASE ... TEMPLATE needs the template idle, exactly like a rename, so
    this waits the same way the swap does. The clone carries the same comment as a
    swap's retired database, so prune_prev_databases manages it alike and
    cleanup.keep_prev_db bounds how many accumulate.
    """
    live = live_db(state)
    clone = old_db(state, job_id)

    if not wait_until_idle(live):
        print('FAILED: %s still has open connections after %d checks over %ds; '
              'cannot clone it to %s, so not importing'
              % (live, SWAP_RETRY_COUNT, SWAP_RETRY_COUNT * SWAP_RETRY_SECONDS, clone))
        raise RuntimeError('%s never went idle; clone abandoned' % live)

    psql('CREATE DATABASE "%s" TEMPLATE "%s";' % (clone, live), DB_MAINTENANCE)
    psql('COMMENT ON DATABASE "%s" IS \'%s%s\';' % (clone, PREV_MARKER, job_id),
         DB_MAINTENANCE)
    print('cloned %s -> %s' % (live, clone))
    return clone


def dump_database(state, job_id, name, filename=None):
    """pg_dump a database into the job's backup directory.

    Called twice per job. A create+clean state dumps two different databases, so the
    file is named after each. An update state dumps the same database twice, before
    and after the import, so `filename` names the file separately -- otherwise the
    second would overwrite the first. Custom format (-Fc): compressed, restorable
    selectively.
    """
    target = BACKUP_DIR / state / job_id / ('%s.dump' % (filename or name))
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix('.dump.part')

    run(['pg_dump'] + conn_args() + ['-d', name, '-Fc', '-f', str(tmp)], env=db_env())

    tmp.replace(target)
    print('dumped %s -> %s (%.1f MiB)'
          % (name, target, target.stat().st_size / (1024 * 1024)))
    return target


def lst_path(state, job_id):
    """Where make_nas.py will have written this state's job file."""
    name = 'be_update_nas.lst' if state == 'be' else '%s_nas.lst' % state
    return BACKUP_DIR / state / job_id / name


def run_state(state, job_id, be_update_path=None):
    py = sys.executable

    if state in EXTERNAL:
        run_external(state, job_id)
        return

    if state == 'be':
        # Stands in for the download stage, which Berlin does not have.
        stage_be_delivery(job_id, be_update_path)

    started = datetime.now()
    run([py, SCRIPT_DIR / 'download.py', '--job-id=%s' % job_id, state])
    print('# TimeLog - %s Downloading elapsed %s' % (state, elapsed(started, datetime.now())))

    if state == 'sh':
        # SH ships no street-name catalogue; make_nas.py refuses to emit a .lst
        # without the synthesized one, so this must run between the two stages.
        run([py, SCRIPT_DIR / 'sh_ax_lage.py', INVENTORY_DIR / 'sh' / job_id])

    run([py, SCRIPT_DIR / 'make_nas.py', '--job-id=%s' % job_id, state])

    # Before the zip and the database work, so a malformed file stops the run early.
    validate_nas_files(state, job_id)
    archive_nas_files(state, job_id)

    # An `update` state writes differentially into the database that is already
    # live, so there is nothing to create, promote or swap -- doing any of those
    # would rename the live database out from under the importer.
    in_place = make_nas.STATES[state].mode == 'update'
    live = live_db(state)

    if in_place:
        if not database_exists(live):
            raise RuntimeError(
                '%s does not exist; %s is a differential update and needs a database '
                'already built by a full import' % (live, state))
        # An in-place update cannot be undone by renaming, so this dump is the only
        # rollback point there is. Taken before the importer touches anything, and
        # filed under the same name a create+clean state gives the database it
        # retires, because that is what it is.
        dump_database(state, job_id, live, filename=old_db(state, job_id))
        # And a live copy to roll back to without a restore. Aborts the run if the
        # live database never goes idle -- safe, since the importer has not started.
        clone_live_database(state, job_id)
    else:
        create_database(state, job_id)

    # cwd matters: alkis-import.sh resolves its own helpers relative to itself.
    started = datetime.now()
    run(['bash', ALKIS_IMPORT, lst_path(state, job_id)], cwd=str(ALKIS_IMPORT.parent))
    print('# TimeLog - %s Alkis-import elapsed %s' % (state, elapsed(started, datetime.now())))

    # The importer has read everything it needs and this job's own files are
    # already in backup_dir, so the older runs' bulk goes now -- before the dumps,
    # which want room on the same volume.
    cleanup_old_jobs(state, job_id)

    if in_place:
        print('%s updated in place; no promote or swap needed' % live)
        # The counterpart of a create+clean state's post-promote dump: this job's
        # result. Same database as the dump above, so it needs its own file name.
        dump_database(state, job_id, live, filename=db_name(state, job_id))
    else:
        # Only reached if the importer exited 0, so the suffix is dropped exactly when
        # the data is known good.
        promote_database(state, job_id)

        # Dumped before the swap: the swap can time out waiting for the live database
        # to go idle, and this job's freshly imported data should be archived either way.
        dump_database(state, job_id, db_name(state, job_id))

        retired = swap_live_database(state, job_id)
        if retired:
            dump_database(state, job_id, retired)
        else:
            print('first job for %s: nothing retired, so nothing to dump' % state)

    prune_prev_databases(state)


def build_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # choices does the validation: an unknown abbreviation is rejected with the
    # full list, so no hand-rolled check is needed.
    parser.add_argument('state', nargs='?', choices=ALL_STATES,
                        metavar='STATE', help='the state abbreviation to run')
    parser.add_argument('--job-id', metavar='ID',
                        help='job id for the whole run (default: minted from the clock)')
    parser.add_argument('--be-update-path', metavar='ARCHIVE', type=Path,
                        help='Berlin delivery archive (.zip, .7z, .tar.gz or .gz); '
                             'required for be, rejected for any other state')
    parser.add_argument('--list', action='store_true',
                        help='print the known state abbreviations and exit')
    return parser


def main(argv):
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list:
        for abbr in ALL_STATES:
            print(abbr)
        return 0

    if not args.state:
        parser.print_help()
        return 0

    state = args.state

    # --be-update-path replaces Berlin's download stage, so it is required for be
    # and meaningless anywhere else. Checked here rather than mid-run, where it
    # would surface only once the download had already run.
    if args.be_update_path and state != 'be':
        parser.error('--be-update-path applies only to be')
    if state == 'be' and not args.be_update_path:
        parser.error('be needs --be-update-path=<archive>; it has no download stage')
    if args.be_update_path and not args.be_update_path.is_file():
        parser.error('--be-update-path: no such file: %s' % args.be_update_path)

    # Children inherit this and skip their own tee: run() already relays their
    # output into logs.log, so teeing too would duplicate every line.
    os.environ[logtee.PARENT_ENV] = '1'

    # An empty --job-id= would collapse every path to <root>/<state>/, so a fresh
    # id is minted for that case too.
    job_id = args.job_id or datetime.now().strftime(JOB_FMT)
    started = datetime.now()

    # One log per state per job. Everything below is printed inside the tee, so
    # the file is a complete record of the run and not just of the stages.
    log = BACKUP_DIR / state / job_id / 'logs.log'
    print()
    with tee_output(log):
        print('#' * 60)
        print('# job     %s' % job_id)
        print('# log     %s' % log)
        print('# %s' % state)
        print('# start   %s' % started.strftime(TIME_FMT))
        print('#' * 60)
        try:
            run_state(state, job_id, args.be_update_path)
        finally:
            finished = datetime.now()
            print('# %s end     %s' % (state, finished.strftime(TIME_FMT)))
            print('# TimeLog - %s Total elapsed %s' % (state, elapsed(started, finished)))

    # The total is only known once the tee has closed, so append it to the log
    # rather than leaving it terminal-only.
    summary = 'job %s complete, elapsed %s' % (job_id, elapsed(started, datetime.now()))
    print()
    print(summary)
    with log.open('a', encoding='utf-8') as fh:
        fh.write(summary + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
