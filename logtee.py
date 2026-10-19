#!/usr/bin/env python3
"""Mirror a script's console output into a log file.

Used by download.py and make_nas.py so every run is recorded under backup_dir,
whether it was started by main.py or driven by hand.
"""

import os
import sys
import traceback
from contextlib import contextmanager, nullcontext

# Set by main.py for its children. A child that sees it writes no log of its own,
# because the parent is already capturing its output -- otherwise every line would
# land in logs.log twice.
PARENT_ENV = 'ALKIS_LOG_PARENT'


class _Tee:
    """Write-through to both the real stream and the log file."""

    def __init__(self, stream, fh):
        self.stream = stream
        self.fh = fh

    def write(self, data):
        self.stream.write(data)
        self.fh.write(data)
        # Flush every write: a run killed mid-download still leaves a usable log.
        self.fh.flush()
        return len(data)

    def flush(self):
        self.stream.flush()
        self.fh.flush()

    def isatty(self):
        return self.stream.isatty()


@contextmanager
def tee_output(path):
    """Mirror stdout and stderr into path (appending) for the duration of the block."""
    path.parent.mkdir(parents=True, exist_ok=True)
    saved_out, saved_err = sys.stdout, sys.stderr

    with path.open('a', encoding='utf-8') as fh:
        sys.stdout, sys.stderr = _Tee(saved_out, fh), _Tee(saved_err, fh)
        try:
            yield fh
        except BaseException:
            # The interpreter prints the traceback only after main() has returned,
            # by which point the tee is uninstalled -- so record it here.
            fh.write(traceback.format_exc())
            raise
        finally:
            sys.stdout, sys.stderr = saved_out, saved_err


def tee_unless_parent(path):
    """tee_output, unless a parent process is already capturing this program's output.

    Lets download.py and make_nas.py log themselves when run by hand, while staying
    silent about it under main.py, which pipes their output into the same file.
    """
    if os.environ.get(PARENT_ENV):
        return nullcontext()
    return tee_output(path)
