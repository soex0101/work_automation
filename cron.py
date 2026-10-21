#!/usr/bin/env python3
"""Nightly entry point: check every state, start the ones whose data moved.

Takes no arguments -- it is meant for a crontab line:

    30 2 * * *  cd /work/automation && python3 cron.py >> cron.log 2>&1

What it does, in order:

    check_update.py --all           every state's check, in one run; parsed
    main.py --job-id=<id> <state>   one process per state that needs updating,
                                    all started together, then left to run --
                                    at most [cron] concurrent_job_count running
                                    at once, earlier ticks' runs included

The checks are one process working through the states in turn, because several
of them download hundreds of megabytes (bw and hb hash samples, by 6 GB, sh
reads a 243 MB manifest); the imports run at once because each is hours long.
One job id covers the run, so every state started by the same cron tick shares
it.

main.py is started detached and is NOT waited for: cron gets its exit status
from the checks and the launching, not from the imports. Each import's account of
itself is the logs.log main.py already writes under <backup_dir>/<state>/<job>/.

Berlin is never started: it has no check and needs --be-update-path, which only a
human can supply.
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import check_update   # for the list of states that have a check
from config import CONCURRENT_JOB_COUNT
from main import ALL_STATES   # every abbreviation main.py accepts

SCRIPT_DIR = Path(__file__).resolve().parent
MAIN_PY = SCRIPT_DIR / 'main.py'
PY = sys.executable or 'python3'
JOB_FMT = '%Y%m%d_%H%M%S'
TIME_FMT = '%Y-%m-%d %H:%M:%S'


def checkable_states():
    """The abbreviations that have a check.

    Read from the table rather than asked for on the command line: check_update
    reports, and reporting is all its CLI does now.
    """
    return sorted(check_update.CHECKS)


def run_checks(states, job_id):
    """({state: True/False/None}, output) from one check_update.py --all run.

    The tick's job id is passed on, so the checks' own log is logs/<job>.log --
    the same id the imports this tick starts will use.

    Read from the summary check_update prints last -- `update needed: ...`
    and `up to date: ...`, each a space-separated list or '-'. True for a state
    in the first, False in the second; None for anything else -- listed under
    `check failed:`, or no summary at all because check_update itself died --
    in which case the state is left alone rather than run on a guess. All the
    output above the summary is a check's own detail and is carried through to
    the output untouched.
    """
    proc = subprocess.run([PY, str(SCRIPT_DIR / 'check_update.py'), '--all',
                           '--job-id=%s' % job_id],
                          capture_output=True, text=True)

    verdicts = dict.fromkeys(states)
    # Last occurrence wins, so only the closing summary counts.
    summary = {}
    for line in proc.stdout.splitlines():
        label, sep, names = line.partition(': ')
        if sep and label in ('update needed', 'up to date'):
            summary[label] = names.split()
    for state in summary.get('update needed', []):
        if state in verdicts:
            verdicts[state] = True
    for state in summary.get('up to date', []):
        if state in verdicts:
            verdicts[state] = False

    return verdicts, (proc.stdout + proc.stderr).rstrip()


def is_our_main(pid, args):
    """True if args are a python running this project's main.py.

    Matched on the resolved path, not the name: other projects on the machine run
    a main.py of their own. A relative path is resolved against the process's
    cwd; one whose cwd cannot be read (another user's) is not ours to count.
    """
    if not args or not Path(args[0]).name.startswith('python'):
        return False
    for arg in args[1:]:
        if Path(arg).name == 'main.py':
            path = Path(arg)
            if not path.is_absolute():
                try:
                    path = Path(os.readlink('/proc/%d/cwd' % pid)) / path
                except OSError:
                    return False
            return path.resolve() == MAIN_PY
    return False


def still_running():
    """{state: pid} for every run of this project's main.py going now.

    Found by scanning /proc, whatever stage the run is at -- downloading,
    validating, zipping, importing -- and by / st too. Only this main.py counts:
    an alkis-import.sh started some other way, or another project's main.py,
    takes no slot.
    """
    mains = {}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            args = (proc / 'cmdline').read_bytes().decode('utf-8', 'replace').split('\0')
        except OSError:
            continue                      # exited mid-scan
        if is_our_main(int(proc.name), args):
            state = next((a for a in args if a in ALL_STATES), None)
            if state:
                mains.setdefault(state, int(proc.name))
    return mains


def launch(state, job_id):
    """Start main.py for one state and return its pid.

    Its output goes nowhere: main.py tees the whole chain into
    <backup_dir>/<state>/<job>/logs.log itself, tracebacks included, so a second
    copy here would only duplicate it. Inheriting cron's stdout instead would
    hold that pipe open until the import finished, which is the opposite of
    leaving it to run.

    start_new_session so the imports survive cron reaping the job's process
    group: being killed between the import and the database swap is the one
    moment that leaves real mess behind.
    """
    proc = subprocess.Popen(
        [PY, str(MAIN_PY), '--job-id=%s' % job_id, state],
        cwd=str(SCRIPT_DIR),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return proc.pid


def main():
    started = datetime.now()
    job_id = started.strftime(JOB_FMT)
    print('cron    %s' % started.strftime(TIME_FMT))
    print('job     %s' % job_id)

    mains = still_running()
    states = checkable_states()

    verdicts, output = run_checks(states, job_id)
    if output:
        print(output)

    due, current, unknown, busy = [], [], [], []
    for state in states:
        if verdicts[state] is None:
            unknown.append(state)
        elif not verdicts[state]:
            current.append(state)
        elif state in mains:
            # Still downloading, say: its marker is not written yet, so it reads
            # as due, but a second run would share its staging directory.
            busy.append(state)
        else:
            due.append(state)

    print('')
    print('due     %s' % (' '.join(due) or '-'))
    print('current %s' % (' '.join(current) or '-'))
    if unknown:
        print('unknown %s  (check failed; not started)' % ' '.join(unknown))
    if busy:
        print('busy    %s  (main.py already running; not started again)' % ' '.join(busy))
    running = len(mains)
    print('running %d job(s)' % running)
    for state, pid in sorted(mains.items()):
        print('        %-3s main.py pid %d' % (state, pid))

    # Jobs already running -- an earlier tick's or started by hand, at any stage
    # -- take their slots first. A state left over is not marked anywhere: its
    # marker is unchanged, so the next tick reports it as due again.
    slots = max(0, CONCURRENT_JOB_COUNT - running)
    due, deferred = due[:slots], due[slots:]
    if deferred:
        print('deferred %s  (%d of concurrent_job_count=%d already running)'
              % (' '.join(deferred), running, CONCURRENT_JOB_COUNT))

    if not due:
        print('nothing to start')
        return 0

    print('')
    for state in due:
        pid = launch(state, job_id)
        print('started %-3s pid %d' % (state, pid))

    print('')
    print('%d import(s) running under job %s; they are not waited for'
          % (len(due), job_id))
    return 0


if __name__ == '__main__':
    sys.exit(main())
