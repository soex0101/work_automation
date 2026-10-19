#!/usr/bin/env python3
"""Nightly entry point: check every state, start the ones whose data moved.

Takes no arguments -- it is meant for a crontab line:

    30 2 * * *  cd /work/automation && python3 cron.py >> cron.log 2>&1

What it does, in order:

    check_update.py --all           every state's check, in one run; parsed
    main.py --job-id=<id> <state>   one process per state that needs updating,
                                    all started together, then left to run

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

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import check_update   # for the list of states that have a check

SCRIPT_DIR = Path(__file__).resolve().parent
PY = sys.executable or 'python3'
JOB_FMT = '%Y%m%d_%H%M%S'
TIME_FMT = '%Y-%m-%d %H:%M:%S'

# The states this run started, so the next tick can tell whether they are still
# going. Two concurrent imports of one state would fight over its live database.
PID_FILE = SCRIPT_DIR / 'cron_running.json'


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

    True or False from the line it prints for a state; None when it printed
    neither -- a CHECK FAILED, a portal that did not answer -- in which case the
    state is left alone rather than run on a guess. Lines that do not start with
    a known abbreviation are a check's own detail (which file moved, and to
    what) and are carried through to the output untouched.
    """
    proc = subprocess.run([PY, str(SCRIPT_DIR / 'check_update.py'), '--all',
                           '--job-id=%s' % job_id],
                          capture_output=True, text=True)

    verdicts = dict.fromkeys(states)
    for line in proc.stdout.splitlines():
        state, _, rest = line.partition(': ')
        if state not in verdicts:
            continue
        if rest.startswith('update available'):
            verdicts[state] = True
        elif rest.startswith('up to date'):
            verdicts[state] = False

    return verdicts, (proc.stdout + proc.stderr).rstrip()


def still_running():
    """{state: pid} for imports an earlier tick started that have not finished.

    A pid alone could have been reused, so the process must still be a main.py
    running that same state.
    """
    try:
        recorded = json.loads(PID_FILE.read_text())
    except (OSError, ValueError):
        return {}

    alive = {}
    for state, pid in recorded.items():
        try:
            argv = Path('/proc/%d/cmdline' % pid).read_bytes().decode('utf-8', 'replace')
        except OSError:
            continue                      # gone, which is what we hope to find
        args = argv.split('\0')
        if any(a.endswith('main.py') for a in args) and state in args:
            alive[state] = pid
    return alive


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
        [PY, str(SCRIPT_DIR / 'main.py'), '--job-id=%s' % job_id, state],
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

    running = still_running()
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
        elif state in running:
            busy.append(state)
        else:
            due.append(state)

    print('')
    print('due     %s' % (' '.join(due) or '-'))
    print('current %s' % (' '.join(current) or '-'))
    if unknown:
        print('unknown %s  (check failed; not started)' % ' '.join(unknown))
    for state in busy:
        print('busy    %s  (pid %d from an earlier run is still importing)'
              % (state, running[state]))

    if not due:
        print('nothing to start')
        return 0

    print('')
    pids = {}
    for state in due:
        pid = launch(state, job_id)
        pids[state] = pid
        print('started %-3s pid %d' % (state, pid))

    # Merged, so a state still running from an earlier tick stays protected.
    pids.update(running)
    PID_FILE.write_text(json.dumps(pids, indent=2, sort_keys=True) + '\n')

    print('')
    print('%d import(s) running under job %s; they are not waited for'
          % (len(due), job_id))
    return 0


if __name__ == '__main__':
    sys.exit(main())
