"""check_update.py records each state's verdict in update_check."""

import sys
from pathlib import Path

import psycopg2
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import check_update  # noqa: E402


@pytest.fixture
def calls(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(check_update, 'LOG_DIR', tmp_path)
    monkeypatch.setattr(check_update.state_db, 'latest_markers',
                        lambda: {'nw': {'marker': 'm1'}})
    monkeypatch.setattr(check_update.state_db, 'start_check',
                        lambda state, job: calls.append(('start', state, job)), raising=False)
    monkeypatch.setattr(check_update.state_db, 'finish_check',
                        lambda state, job, result, detail:
                        calls.append(('finish', state, job, result, detail)), raising=False)
    return calls


def run(monkeypatch, marker_fn):
    monkeypatch.setattr(check_update, 'CHECKS', {'nw': marker_fn})
    check_update.main(['--job-id=J', 'nw'])


def test_records_up_to_date(monkeypatch, calls):
    run(monkeypatch, lambda last: 'm1')
    assert calls[0] == ('start', 'nw', 'J')
    assert calls[1][:4] == ('finish', 'nw', 'J', 'UP_TO_DATE')
    assert isinstance(calls[1][4], str)


def test_records_update_needed(monkeypatch, calls):
    run(monkeypatch, lambda last: 'm2')
    assert calls[1][:4] == ('finish', 'nw', 'J', 'UPDATE_NEEDED')
    assert 'portal m2, last seen m1' in calls[1][4]


def test_records_failed(monkeypatch, calls):
    def boom(last):
        raise RuntimeError('boom')
    run(monkeypatch, boom)
    assert calls[1][:4] == ('finish', 'nw', 'J', 'FAILED')
    assert 'boom' in calls[1][4]


def test_db_write_failure_keeps_summary(monkeypatch, calls, capsys):
    def down(*args):
        raise psycopg2.OperationalError('db down')
    monkeypatch.setattr(check_update.state_db, 'start_check', down, raising=False)
    monkeypatch.setattr(check_update.state_db, 'finish_check', down, raising=False)
    run(monkeypatch, lambda last: 'm1')
    out = capsys.readouterr()
    lines = out.out.rstrip().splitlines()
    assert lines[-3:] == ['update needed: -', 'up to date: nw', 'check failed: -']
    assert 'warning' in out.err
