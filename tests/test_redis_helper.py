"""Unit tests for `helper.redis_helper.RedisHelper` portfolio coordination.

Validates: Requirements R3.1 - R3.5 and R7.1 from
`portfolio-anchor-split/requirements.md`.

Backed by `fakeredis` (Strict server) so we exercise the real Lua
``EVAL`` path without needing a live Redis.
"""

from __future__ import annotations

import json
import os
import sys

import fakeredis
import pytest

# Both the shared config (Constants.py) and the Lambda package live
# outside a Python package — the Lambdas import them as top-level
# modules at deploy time.  Mirror that lookup order here so the helper
# imports work from a repo-root pytest run.
_CONFIG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "rm-allocation-configurations")
)
_LAMBDA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "rm-allocation-process-inputs")
)
for _p in (_CONFIG_DIR, _LAMBDA_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from helper.redis_helper import RedisHelper  # noqa: E402
import Constants  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RUN_ID = "RUN_TEST_001"
SECTOR = "BOMDEL"
DEP_DATE = "08/01/2023"
PORTFOLIO_COUNT = 3


class _StubInputSource:
    """Minimal stand-in for the production ``InputSource`` object.

    ``RedisHelper`` only reads ``logs[CONST_RUN_AUDIT]`` to decide
    whether to bump counters; the portfolio methods don't touch it.
    """

    def __init__(self, run_audit_mode=Constants.DB):
        self.logs = {Constants.CONST_RUN_AUDIT: run_audit_mode}


@pytest.fixture
def fake_client():
    """Fresh fakeredis Strict client per test."""
    return fakeredis.FakeStrictRedis(decode_responses=True)


@pytest.fixture
def helper(fake_client):
    return RedisHelper(RUN_ID, fake_client, _StubInputSource())


def _entry(flight_number, *, is_failure=False, **extra):
    """Build a minimal portfolio entry payload (R3.1)."""
    body = {
        "FlightNumber": flight_number,
        "Origin": SECTOR[:3],
        "Destin": SECTOR[3:],
        "PerStart": DEP_DATE,
        "is_failure": is_failure,
    }
    body.update(extra)
    return body


def _flight_uid(flight_number):
    return f"{flight_number}_{DEP_DATE}"


def _portfolio_key():
    return f"{Constants.PORTFOLIO_KEY_PREFIX}{RUN_ID}_{SECTOR}_{DEP_DATE}"


# ---------------------------------------------------------------------------
# R3.1 / R3.3 — first insert before portfolio is full
# ---------------------------------------------------------------------------


def test_first_insert_returns_one_and_not_won(helper, fake_client):
    """Validates: Requirements R3.1 and R3.3.

    The first insert into an empty portfolio writes the entry, returns
    ``(1, False)`` because the count has not yet reached
    ``portfolio_count``, and SHALL NOT create the ``completed``
    sentinel field on the hash.
    """
    entry = _entry(1127)
    n, won = helper.insert_portfolio_entry(
        RUN_ID, SECTOR, DEP_DATE,
        _flight_uid(1127), json.dumps(entry), PORTFOLIO_COUNT,
    )

    assert (n, won) == (1, False)

    hkey = _portfolio_key()
    # Field stored under the flight_uid; no 'completed' sentinel yet.
    assert fake_client.hget(hkey, _flight_uid(1127)) == json.dumps(entry)
    assert not fake_client.hexists(hkey, "completed")
    # TTL is set on the hash (R3.3).
    assert fake_client.ttl(hkey) > 0
    assert fake_client.ttl(hkey) <= Constants.PORTFOLIO_TTL_SEC


# ---------------------------------------------------------------------------
# R3.3 — completing the count claims the trigger atomically
# ---------------------------------------------------------------------------


def test_insert_completing_count_returns_won(helper, fake_client):
    """Validates: Requirements R3.3.

    The insert that brings the active entry count up to
    ``portfolio_count`` SHALL claim the trigger and return
    ``(portfolio_count, True)``.  All preceding inserts SHALL return
    ``won=False``.
    """
    flights = [1127, 1120, 1411]
    results = [
        helper.insert_portfolio_entry(
            RUN_ID, SECTOR, DEP_DATE,
            _flight_uid(fn), json.dumps(_entry(fn)), PORTFOLIO_COUNT,
        )
        for fn in flights
    ]

    assert results[0] == (1, False)
    assert results[1] == (2, False)
    assert results[2] == (PORTFOLIO_COUNT, True)

    # The claimed trigger is reflected by the 'completed' sentinel
    # being present on the hash.
    assert fake_client.hexists(_portfolio_key(), "completed")


# ---------------------------------------------------------------------------
# R3.2 — duplicate inserts are idempotent and do not re-trigger
# ---------------------------------------------------------------------------


def test_duplicate_insert_with_same_flight_uid_keeps_count_and_loses(
    helper, fake_client
):
    """Validates: Requirements R3.2 and R3.3.

    Re-inserting the same flight_uid (e.g. SQS retry) SHALL overwrite
    the same hash field, leave the active count unchanged, and SHALL
    NOT re-claim the trigger.
    """
    flights = [1127, 1120, 1411]
    for fn in flights:
        helper.insert_portfolio_entry(
            RUN_ID, SECTOR, DEP_DATE,
            _flight_uid(fn), json.dumps(_entry(fn)), PORTFOLIO_COUNT,
        )

    # Re-insert the first flight (same flight_uid).
    n, won = helper.insert_portfolio_entry(
        RUN_ID, SECTOR, DEP_DATE,
        _flight_uid(1127), json.dumps(_entry(1127, retried=True)),
        PORTFOLIO_COUNT,
    )

    assert (n, won) == (PORTFOLIO_COUNT, False)

    # The hash size is still PORTFOLIO_COUNT flights + 1 'completed'.
    assert fake_client.hlen(_portfolio_key()) == PORTFOLIO_COUNT + 1


# ---------------------------------------------------------------------------
# R3.4 — once mark_portfolio_done has run, no further triggers fire
# ---------------------------------------------------------------------------


def test_insert_after_mark_portfolio_done_does_not_re_trigger(helper, fake_client):
    """Validates: Requirements R3.4.

    After ``mark_portfolio_done`` is called, any subsequent insert
    SHALL return ``won=False`` and SHALL leave the active entry count
    at the value it had when the portfolio was completed.  This guards
    against duplicate fan-outs caused by SQS retries hitting after the
    fan-out has already happened.
    """
    flights = [1127, 1120, 1411]
    for fn in flights:
        helper.insert_portfolio_entry(
            RUN_ID, SECTOR, DEP_DATE,
            _flight_uid(fn), json.dumps(_entry(fn)), PORTFOLIO_COUNT,
        )

    helper.mark_portfolio_done(RUN_ID, SECTOR, DEP_DATE)

    # Re-insert an existing flight_uid → no count change, no win.
    n, won = helper.insert_portfolio_entry(
        RUN_ID, SECTOR, DEP_DATE,
        _flight_uid(1127), json.dumps(_entry(1127, retried=True)),
        PORTFOLIO_COUNT,
    )
    assert (n, won) == (PORTFOLIO_COUNT, False)

    assert fake_client.hexists(_portfolio_key(), "completed")
    assert fake_client.ttl(_portfolio_key()) > 0


# ---------------------------------------------------------------------------
# fetch_portfolio_entries — happy-path and post-mark short-circuit
# ---------------------------------------------------------------------------


def test_fetch_portfolio_entries_returns_decoded_entries_after_win(helper):
    """Validates: Requirements R3.4 (the read side of the contract).

    The winning invocation (the insert that claims the trigger and sets
    the 'completed' sentinel) SHALL still be able to read every flight
    entry it inserted, decoded back into Python dicts.  The 'completed'
    sentinel must never leak into the returned list.  Duplicate-fan-out
    protection comes from the atomic ``won`` flag, not from this read.
    """
    flights = [1127, 1120, 1411]
    for fn in flights:
        helper.insert_portfolio_entry(
            RUN_ID, SECTOR, DEP_DATE,
            _flight_uid(fn), json.dumps(_entry(fn)), PORTFOLIO_COUNT,
        )

    # The third insert won, which sets 'completed'.  fetch_portfolio_entries
    # must still return all flight entries (the winner needs them to fan
    # out) while filtering the sentinel.
    entries = helper.fetch_portfolio_entries(RUN_ID, SECTOR, DEP_DATE)

    assert len(entries) == PORTFOLIO_COUNT
    flight_numbers = sorted(e["FlightNumber"] for e in entries)
    assert flight_numbers == sorted(flights)
    # The 'completed' sentinel must not leak into the decoded entries.
    assert all("FlightNumber" in e for e in entries)


def test_fetch_portfolio_entries_returns_entries_for_incomplete_portfolio(helper):
    """fetch_portfolio_entries returns decoded entries while the
    portfolio is still in flight (no 'completed' sentinel yet).
    """
    helper.insert_portfolio_entry(
        RUN_ID, SECTOR, DEP_DATE,
        _flight_uid(1127), json.dumps(_entry(1127)), PORTFOLIO_COUNT,
    )
    helper.insert_portfolio_entry(
        RUN_ID, SECTOR, DEP_DATE,
        _flight_uid(1120), json.dumps(_entry(1120)), PORTFOLIO_COUNT,
    )

    entries = helper.fetch_portfolio_entries(RUN_ID, SECTOR, DEP_DATE)

    assert len(entries) == 2
    flight_numbers = sorted(e["FlightNumber"] for e in entries)
    assert flight_numbers == [1120, 1127]


# ---------------------------------------------------------------------------
# R7.1 — fare-anchor (F) ready counters
# ---------------------------------------------------------------------------


def test_update_anchor_run_audit_ready_increments_F_counters(helper, fake_client):
    """Validates: Requirements R7.1.

    ``updateAnchorRunAuditReady`` SHALL increment the F-tagged ready
    counters for the current run, leaving the unbranded counters
    unchanged.
    """
    helper.updateAnchorRunAuditReady()
    helper.updateAnchorRunAuditReady()

    assert fake_client.get(Constants.KEY_B2B_READY_COUNT_F + RUN_ID) == "2"
    assert fake_client.get(Constants.KEY_B2C_READY_COUNT_F + RUN_ID) == "2"
    # Unbranded counters MUST NOT be touched by the F-tagged helper.
    assert fake_client.get(Constants.KEY_B2B_READY_COUNT + RUN_ID) is None
    assert fake_client.get(Constants.KEY_B2C_READY_COUNT + RUN_ID) is None


def test_update_anchor_run_audit_ready_no_op_when_run_audit_disabled():
    """If the input source has run-audit logging disabled, the helper
    SHALL skip the increments — matching ``updateRunAuditReady``.
    """
    fake_client = fakeredis.FakeStrictRedis(decode_responses=True)
    helper = RedisHelper(
        RUN_ID, fake_client, _StubInputSource(run_audit_mode=Constants.SKIP),
    )

    helper.updateAnchorRunAuditReady()

    assert fake_client.get(Constants.KEY_B2B_READY_COUNT_F + RUN_ID) is None
    assert fake_client.get(Constants.KEY_B2C_READY_COUNT_F + RUN_ID) is None
