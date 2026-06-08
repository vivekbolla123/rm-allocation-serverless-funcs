"""Unit tests for the ``lambda_function.lambda_handler`` dispatcher.

Validates: Requirements R1.2, R1.3, R1.4, R1.5 from
`portfolio-anchor-split/requirements.md`.

The dispatcher decides, per SQS record, which allocation entry point
to invoke based on ``saleinvflag``, ``route_type`` and the new ``pass``
field:

* ``saleinvflag == "1"``           -> ``DynAllocSalesInv``        (R1.4)
* ``route_type == "connections"``  -> ``DynAllocConnections``     (R1.4)
* ``route_type == "direct"`` +
    ``pass == "allocation"``       -> ``DynAlloc.from_allocation_payload`` (R1.3)
    ``pass == "anchor"``           -> ``DynAlloc.from_anchor_payload``     (R1.2)
    ``pass`` missing               -> ``DynAlloc.from_anchor_payload``     (R1.5)

``lambda_function`` opens MongoDB / Redis / MySQL connections at import
time, so we stub every heavy dependency in ``sys.modules`` before
importing it.  That keeps the test hermetic — no live infrastructure,
no network — while still exercising the real routing branch.
"""

from __future__ import annotations

import json
import os
import sys
import importlib
from unittest.mock import MagicMock

import pandas as pd
import pytest

_CONFIG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "rm-allocation-configurations")
)
_LAMBDA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "rm-allocation-process-inputs")
)
for _p in (_CONFIG_DIR, _LAMBDA_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)


# Heavy / external modules that ``lambda_function`` touches at import
# time.  Each is replaced with a ``MagicMock`` so the module-level
# connection setup (MongoClient, Redis.ping, create_engine,
# ConstantsUtils) is a no-op and the routing targets become recordable
# mocks.
_STUBBED_MODULES = (
    "configsettings",
    "pymongo",
    "sqlalchemy",
    "boto3",
    "redis",
    "Constants",
    "helper.rm_database_helper",
    "runAlloc",
    "runAlloc_Connections",
    "runAlloc_SalesInv",
    "time_logger",
    "constants_utils",
)


@pytest.fixture
def lambda_module(monkeypatch):
    """Import ``lambda_function`` with all heavy deps stubbed out.

    Yields the freshly-imported module so each test can inspect the
    recordable routing targets (``DynAlloc`` etc.).
    """
    # ``redis.Redis().ping()`` must be truthy so the module-level
    # "Connected to Redis" guard passes without a real server.
    redis_stub = MagicMock(name="redis")
    redis_stub.Redis.return_value.ping.return_value = True

    stubs = {}
    for name in _STUBBED_MODULES:
        if name == "redis":
            stubs[name] = redis_stub
        else:
            stubs[name] = MagicMock(name=name)

    # Ensure a clean import of the module under test.
    monkeypatch.delitem(sys.modules, "lambda_function", raising=False)
    for name, stub in stubs.items():
        monkeypatch.setitem(sys.modules, name, stub)

    module = importlib.import_module("lambda_function")

    # Bypass the MongoDB-backed run-status lookup so records always
    # reach the routing block (status_df empty -> not skipped).
    monkeypatch.setattr(
        module, "getRunStatus", lambda run_id: pd.DataFrame(columns=["run_status"])
    )

    yield module


def _event(**body_overrides):
    """Build a one-record SQS event with a sensible default body."""
    body = {
        "route_type": "direct",
        "runId": "RUN_TEST_DISPATCH",
        "saleinvflag": "0",
        "update_navitaire_method": "api",
        "runTime": "2025-06-17T21:32:27.519504",
    }
    body.update(body_overrides)
    return {"Records": [{"attributes": {}, "body": json.dumps(body)}]}


def _call_counts(module):
    return {
        "anchor": module.DynAlloc.from_anchor_payload.call_count,
        "allocation": module.DynAlloc.from_allocation_payload.call_count,
        "connections": module.DynAllocConnections.call_count,
        "salesinv": module.DynAllocSalesInv.call_count,
    }


def test_direct_anchor_pass_routes_to_from_anchor_payload(lambda_module):
    """R1.2 — direct + pass=anchor -> DynAlloc.from_anchor_payload."""
    lambda_module.lambda_handler(_event(route_type="direct", **{"pass": "anchor"}), None)

    counts = _call_counts(lambda_module)
    assert counts["anchor"] == 1
    assert counts["allocation"] == 0
    assert counts["connections"] == 0
    assert counts["salesinv"] == 0


def test_direct_allocation_pass_routes_to_from_allocation_payload(lambda_module):
    """R1.3 — direct + pass=allocation -> DynAlloc.from_allocation_payload."""
    lambda_module.lambda_handler(
        _event(route_type="direct", **{"pass": "allocation"}), None
    )

    counts = _call_counts(lambda_module)
    assert counts["allocation"] == 1
    assert counts["anchor"] == 0


def test_direct_missing_pass_defaults_to_anchor(lambda_module):
    """R1.5 — direct with no pass field defaults to the fare-anchor pass."""
    lambda_module.lambda_handler(_event(route_type="direct"), None)

    counts = _call_counts(lambda_module)
    assert counts["anchor"] == 1
    assert counts["allocation"] == 0


def test_connections_ignores_pass_field(lambda_module):
    """R1.4 — connections route ignores pass and runs the connections flow."""
    lambda_module.lambda_handler(
        _event(route_type="connections", **{"pass": "allocation"}), None
    )

    counts = _call_counts(lambda_module)
    assert counts["connections"] == 1
    assert counts["anchor"] == 0
    assert counts["allocation"] == 0


def test_sales_inventory_ignores_pass_field(lambda_module):
    """R1.4 — saleinvflag == "1" wins over route_type/pass."""
    lambda_module.lambda_handler(
        _event(route_type="direct", saleinvflag="1", **{"pass": "allocation"}), None
    )

    counts = _call_counts(lambda_module)
    assert counts["salesinv"] == 1
    assert counts["anchor"] == 0
    assert counts["allocation"] == 0


def test_anchor_payload_called_with_input_data_and_message(lambda_module):
    """The dispatcher threads (input_data, message-body) into the constructor."""
    lambda_module.lambda_handler(_event(route_type="direct", **{"pass": "anchor"}), None)

    args, _ = lambda_module.DynAlloc.from_anchor_payload.call_args
    input_data, data = args
    assert data["route_type"] == "direct"
    assert data["pass"] == "anchor"
    # input_data carries the queue URLs / Nav config the constructor needs.
    assert "MARKET_LIST_QUEUE_URL" in input_data
