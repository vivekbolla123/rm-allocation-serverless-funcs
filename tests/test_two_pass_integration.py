"""Integration tests for the anchor-pass trigger + allocation-pass fan-out
(``portfolio-anchor-split`` Slice 11).

Validates: Requirements R3.2, R3.3, R3.4, R3.5 from
``.kiro/specs/portfolio-anchor-split/requirements.md``.

These tests drive ``DynAlloc._coordinatePortfolioTrigger`` /
``DynAlloc._fan_out_allocation_messages`` — the methods wired by
Slice 11 — against:

* a **real** ``RedisHelper`` backed by ``fakeredis`` (so the Lua
  ``INSERT_AND_CHECK_TRIGGER`` / fetch scripts run for real), and
* a **stubbed** boto3 SQS client that records ``send_message_batch``
  calls.

Each direct-route flight arrives as its own SQS anchor-pass message and
therefore its own Lambda invocation; we simulate that by calling
``_coordinatePortfolioTrigger`` once per flight entry.  The scenarios
mirror the Slice 11.4 acceptance list:

(a) all anchor messages dispatched, only the last triggers fan-out;
(b) a duplicate anchor message (SQS retry) before completion → no extra
    fan-out and no double-counting;
(c) a duplicate trigger after ``mark_portfolio_done`` → SendMessageBatch
    is not called again.
"""

from __future__ import annotations

import json
import os
import sys

import pytest
fakeredis = pytest.importorskip("fakeredis")

# The shared config (Constants.py) and the Lambda package live outside a
# Python package — the Lambdas import them as top-level modules at deploy
# time.  Mirror that lookup order for a repo-root pytest run.
_CONFIG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "rm-allocation-configurations")
)
_LAMBDA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "rm-allocation-process-inputs")
)
for _p in (_CONFIG_DIR, _LAMBDA_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import Constants  # noqa: E402
from helper.redis_helper import RedisHelper  # noqa: E402
from runAlloc import DynAlloc  # noqa: E402


# ---------------------------------------------------------------------------
# Constants + fixtures
# ---------------------------------------------------------------------------

RUN_ID = "RUN_TEST_SLICE11"
SECTOR = "BOMDEL"
DEP_DATE = "08/01/2023"
QUEUE_URL = "https://sqs.local/market-list-queue"
PORTFOLIO_COUNT = 3


class _StubInputSource:
    """Minimal stand-in for the production ``InputSource``.

    ``RedisHelper`` only consults ``logs[CONST_RUN_AUDIT]``; the
    portfolio coordination methods never read it, but the counter
    helpers do.
    """

    def __init__(self, run_audit_mode=Constants.DB):
        self.logs = {Constants.CONST_RUN_AUDIT: run_audit_mode}


class _RecordingSQS:
    """Stub boto3 SQS client that records ``send_message_batch`` calls."""

    def __init__(self):
        self.batches = []

    def send_message_batch(self, QueueUrl=None, Entries=None):
        # Capture a deep-ish copy of what was sent so later mutations of
        # the caller's batch list don't corrupt our recording.
        self.batches.append({"QueueUrl": QueueUrl, "Entries": list(Entries)})
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    # Convenience accessors -------------------------------------------------
    @property
    def call_count(self):
        return len(self.batches)

    def all_entries(self):
        out = []
        for batch in self.batches:
            out.extend(batch["Entries"])
        return out


@pytest.fixture
def fake_client():
    return fakeredis.FakeStrictRedis(decode_responses=True)


@pytest.fixture
def sqs():
    return _RecordingSQS()


@pytest.fixture
def dyn(fake_client, sqs):
    """A ``DynAlloc`` wired with a real RedisHelper(fakeredis) + stub SQS.

    Bypasses ``__init__`` so no live MongoDB / Redis / SQS is needed;
    only the attributes that ``_coordinatePortfolioTrigger`` /
    ``_fan_out_allocation_messages`` touch are populated.
    """
    instance = DynAlloc.__new__(DynAlloc)
    instance.runId = RUN_ID
    instance.cache_client = fake_client
    instance.redishelper = RedisHelper(RUN_ID, fake_client, _StubInputSource())
    instance.sqs = sqs
    instance.MARKET_LIST_QUEUE_URL = QUEUE_URL

    class _Logger:
        def info(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

    instance.logger = _Logger()

    class _Params:
        origin = SECTOR[:3]
        destin = SECTOR[3:]
        per_start = DEP_DATE
        flight_number = 0

    instance.currParams = _Params()
    return instance


# ---------------------------------------------------------------------------
# Entry builder
# ---------------------------------------------------------------------------


def _entry(flight_number, *, plf=70.0, af=20000.0, fare=4500.0, is_failure=False):
    """Build a minimal anchor-pass ``PortfolioEntry`` for one flight.

    Carries the fields ``_coordinatePortfolioTrigger`` keys on
    (``sector`` / ``PerStart`` / ``FlightNumber``) plus the aggregate
    inputs (``Imputed_ForecastPLF`` / ``ForecastAF_Allin`` /
    ``anchorFare_b2c``) so ``compute_portfolio_aggregates`` produces
    real numbers.
    """
    return {
        "FlightNumber": flight_number,
        "Origin": SECTOR[:3],
        "Destin": SECTOR[3:],
        "PerStart": DEP_DATE,
        "sector": SECTOR,
        "pass": "allocation",
        "portfolio_count": PORTFOLIO_COUNT,
        "Imputed_ForecastPLF": plf,
        "ForecastAF_Allin": af,
        "anchorFare_b2c": fare,
        "is_failure": is_failure,
    }


def _flight_uid(flight_number):
    return f"{flight_number}_{DEP_DATE}"


def _portfolio_key():
    return f"{Constants.PORTFOLIO_KEY_PREFIX}{RUN_ID}_{SECTOR}_{DEP_DATE}"


# ---------------------------------------------------------------------------
# (a) all anchor messages dispatched, only the last triggers fan-out
# ---------------------------------------------------------------------------


def test_only_last_anchor_pass_triggers_fan_out(dyn, sqs, fake_client):
    """Validates: Requirements R3.3, R3.4, R3.5.

    For a 3-flight portfolio, each flight's anchor pass calls
    ``_coordinatePortfolioTrigger``.  Only the third (the one that
    fills ``portfolio_count``) SHALL fan out, and that single fan-out
    SHALL dispatch one allocation-pass message per flight in the
    portfolio.
    """
    flights = [1127, 1120, 1411]

    fan_out_flags = [dyn._coordinatePortfolioTrigger(_entry(fn)) for fn in flights]

    # Only the last invocation owned the trigger.
    assert fan_out_flags == [False, False, True]

    # Exactly one SendMessageBatch call (3 flights fit in one chunk of 10).
    assert sqs.call_count == 1

    entries = sqs.all_entries()
    assert len(entries) == PORTFOLIO_COUNT

    # Every dispatched message is an allocation-pass message for this
    # portfolio, carries the computed aggregates, and a unique Id.
    dispatched_flights = []
    ids = set()
    for msg in entries:
        body = json.loads(msg["MessageBody"])
        assert body["pass"] == "allocation"
        assert body["sector"] == SECTOR
        assert "portfolio_aggregates" in body
        # PFA == mean(anchorFare_b2c); all fares equal so PFA == fare.
        assert body["portfolio_aggregates"]["PFA"] == 4500.0
        dispatched_flights.append(body["FlightNumber"])
        ids.add(msg["Id"])

    assert sorted(dispatched_flights) == sorted(flights)
    assert len(ids) == PORTFOLIO_COUNT  # unique message Ids

    # The portfolio was marked done (sentinel present, TTL refreshed).
    assert fake_client.hexists(_portfolio_key(), "completed")
    assert fake_client.ttl(_portfolio_key()) > 0


def test_fan_out_batches_in_chunks_of_ten(dyn, sqs):
    """A portfolio larger than 10 flights SHALL be dispatched in
    ``SendMessageBatch`` chunks of at most 10 (R3.4)."""
    big_count = 23
    flights = list(range(2000, 2000 + big_count))

    fan_out_flags = []
    for fn in flights:
        entry = _entry(fn)
        entry["portfolio_count"] = big_count
        fan_out_flags.append(dyn._coordinatePortfolioTrigger(entry))

    # Only the final flight triggers the fan-out.
    assert fan_out_flags[-1] is True
    assert fan_out_flags.count(True) == 1

    # 23 messages → chunks of 10, 10, 3.
    assert sqs.call_count == 3
    batch_sizes = [len(b["Entries"]) for b in sqs.batches]
    assert batch_sizes == [10, 10, 3]
    assert len(sqs.all_entries()) == big_count


# ---------------------------------------------------------------------------
# (b) duplicate anchor message before completion → no extra fan-out
# ---------------------------------------------------------------------------


def test_duplicate_anchor_message_before_completion_does_not_fan_out(
    dyn, sqs, fake_client
):
    """Validates: Requirements R3.2.

    A redelivered anchor message for an already-inserted flight (same
    flight_uid) SHALL overwrite the same hash field, leave the active
    count unchanged, and SHALL NOT trigger a fan-out while the portfolio
    is still incomplete.
    """
    # Two of three flights have run their anchor pass.
    assert dyn._coordinatePortfolioTrigger(_entry(1127)) is False
    assert dyn._coordinatePortfolioTrigger(_entry(1120)) is False

    # SQS retry redelivers flight 1127 — duplicate flight_uid.
    assert dyn._coordinatePortfolioTrigger(_entry(1127)) is False

    # No fan-out yet; the third distinct flight has not arrived.
    assert sqs.call_count == 0
    assert not fake_client.hexists(_portfolio_key(), "completed")
    # Hash still holds exactly the two distinct flights.
    assert fake_client.hlen(_portfolio_key()) == 2

    # The genuine third flight now completes the portfolio → one fan-out.
    assert dyn._coordinatePortfolioTrigger(_entry(1411)) is True
    assert sqs.call_count == 1
    assert len(sqs.all_entries()) == PORTFOLIO_COUNT


# ---------------------------------------------------------------------------
# (c) duplicate trigger after mark_portfolio_done → no SendMessageBatch
# ---------------------------------------------------------------------------


def test_duplicate_trigger_after_done_does_not_send_message_batch(
    dyn, sqs, fake_client
):
    """Validates: Requirements R3.4.

    Once the winning invocation has fanned out and marked the portfolio
    done, a late redelivery of any anchor message SHALL receive
    ``won=False`` and SHALL NOT call ``send_message_batch`` again.
    """
    flights = [1127, 1120, 1411]
    for fn in flights:
        dyn._coordinatePortfolioTrigger(_entry(fn))

    # The third flight already fanned out exactly once.
    assert sqs.call_count == 1
    first_fan_out_size = len(sqs.all_entries())
    assert first_fan_out_size == PORTFOLIO_COUNT
    assert fake_client.hexists(_portfolio_key(), "completed")

    # SQS redelivers the (already-won) last flight after completion.
    re_triggered = dyn._coordinatePortfolioTrigger(_entry(1411))

    assert re_triggered is False
    # No additional SendMessageBatch call — fan-out happened exactly once.
    assert sqs.call_count == 1
    assert len(sqs.all_entries()) == first_fan_out_size


def test_failure_entries_are_included_in_fan_out(dyn, sqs):
    """Validates: Requirements R8.2.

    Failure entries (``is_failure = True``) SHALL still be dispatched in
    the allocation-pass fan-out (the allocation pass writes their
    failure summary rows), but SHALL be excluded from the aggregate
    statistics (R4.4)."""
    entries = [
        _entry(1127, plf=60.0, fare=4000.0),
        _entry(1120, is_failure=True),  # excluded from aggregates
        _entry(1411, plf=80.0, fare=5000.0),
    ]

    flags = [dyn._coordinatePortfolioTrigger(e) for e in entries]
    assert flags[-1] is True
    assert sqs.call_count == 1

    dispatched = sqs.all_entries()
    # All three (including the failure) are fanned out.
    assert len(dispatched) == PORTFOLIO_COUNT

    # Aggregates exclude the failure entry: PFA == mean(4000, 5000) = 4500.
    body = json.loads(dispatched[0]["MessageBody"])
    assert body["portfolio_aggregates"]["PFA"] == 4500.0
