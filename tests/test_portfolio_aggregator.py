"""Unit tests for `helper.portfolio_aggregator.compute_portfolio_aggregates`.

Validates: Requirements R4.1 - R4.6 from `portfolio-anchor-split/requirements.md`.
"""

from __future__ import annotations

import math
import os
import sys

import numpy as np
import pytest

# Make the Lambda package importable from a repo-root pytest run.
_HELPER_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "rm-allocation-process-inputs")
)
if _HELPER_DIR not in sys.path:
    sys.path.insert(0, _HELPER_DIR)

from helper.portfolio_aggregator import compute_portfolio_aggregates  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_KEYS = (
    "PDS",
    "sd_PDS",
    "PDQ",
    "sd_PDQ",
    "PFA",
    "sd_PFA",
    "PDS88",
    "sd_PDS88",
    "PDQ88",
    "sd_PDQ88",
    "PFA88",
    "sd_PFA88",
)

SUB_KEYS = ("PDS88", "sd_PDS88", "PDQ88", "sd_PDQ88", "PFA88", "sd_PFA88")


def _entry(plf, af, fare, is_failure=False):
    """Build a minimal portfolio entry."""
    return {
        "Imputed_ForecastPLF": plf,
        "ForecastAF_Allin": af,
        "anchorFare_b2c": fare,
        "is_failure": is_failure,
    }


# ---------------------------------------------------------------------------
# Single-flight portfolio (R4.2)
# ---------------------------------------------------------------------------


def test_single_flight_portfolio_means_equal_value_and_std_zero():
    """A portfolio of one flight: every mean equals the entry value, std = 0."""
    entries = [_entry(plf=70.0, af=20000.0, fare=4500.0)]
    agg = compute_portfolio_aggregates(entries)

    # Full-portfolio stats
    assert agg["PDS"] == pytest.approx(70.0)
    assert agg["sd_PDS"] == pytest.approx(0.0)
    assert agg["PDQ"] == pytest.approx(20000.0)
    assert agg["sd_PDQ"] == pytest.approx(0.0)
    assert agg["PFA"] == pytest.approx(4500.0)
    assert agg["sd_PFA"] == pytest.approx(0.0)

    # Single flight has PLF < 88 → also lands in the 88 subset
    assert agg["PDS88"] == pytest.approx(70.0)
    assert agg["sd_PDS88"] == pytest.approx(0.0)
    assert agg["PDQ88"] == pytest.approx(20000.0)
    assert agg["sd_PDQ88"] == pytest.approx(0.0)
    assert agg["PFA88"] == pytest.approx(4500.0)
    assert agg["sd_PFA88"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Multi-flight portfolio (R4.1)
# ---------------------------------------------------------------------------


def test_multi_flight_portfolio_uses_population_std_ddof0():
    """Validates ddof=0 std and mean across multiple flights."""
    plfs = [60.0, 70.0, 80.0]
    afs = [15000.0, 20000.0, 25000.0]
    fares = [4000.0, 5000.0, 6000.0]
    entries = [_entry(p, a, f) for p, a, f in zip(plfs, afs, fares)]

    agg = compute_portfolio_aggregates(entries)

    assert agg["PDS"] == pytest.approx(round(float(np.mean(plfs)), 2))
    assert agg["sd_PDS"] == pytest.approx(round(float(np.std(plfs, ddof=0)), 2))
    assert agg["PDQ"] == pytest.approx(round(float(np.mean(afs)), 2))
    assert agg["sd_PDQ"] == pytest.approx(round(float(np.std(afs, ddof=0)), 2))
    assert agg["PFA"] == pytest.approx(round(float(np.mean(fares)), 2))
    assert agg["sd_PFA"] == pytest.approx(round(float(np.std(fares, ddof=0)), 2))

    # All three flights have PLF < 88 → 88 subset matches the full set
    assert agg["PDS88"] == pytest.approx(agg["PDS"])
    assert agg["sd_PDS88"] == pytest.approx(agg["sd_PDS"])
    assert agg["PDQ88"] == pytest.approx(agg["PDQ"])
    assert agg["sd_PDQ88"] == pytest.approx(agg["sd_PDQ"])
    assert agg["PFA88"] == pytest.approx(agg["PFA"])
    assert agg["sd_PFA88"] == pytest.approx(agg["sd_PFA"])


def test_multi_flight_portfolio_known_values():
    """Concrete numbers — sanity check against hand-computed expected output."""
    entries = [
        _entry(plf=50.0, af=10000.0, fare=4000.0),
        _entry(plf=70.0, af=20000.0, fare=6000.0),
    ]
    agg = compute_portfolio_aggregates(entries)

    # Mean of [50, 70] = 60; population std = 10
    assert agg["PDS"] == pytest.approx(60.0)
    assert agg["sd_PDS"] == pytest.approx(10.0)
    # Mean of [10000, 20000] = 15000; population std = 5000
    assert agg["PDQ"] == pytest.approx(15000.0)
    assert agg["sd_PDQ"] == pytest.approx(5000.0)
    # Mean of [4000, 6000] = 5000; population std = 1000
    assert agg["PFA"] == pytest.approx(5000.0)
    assert agg["sd_PFA"] == pytest.approx(1000.0)


# ---------------------------------------------------------------------------
# All-failure / empty (R4.4, R4.6)
# ---------------------------------------------------------------------------


def test_all_failure_entries_yields_all_null_aggregates():
    """When every entry is is_failure=True the aggregates are all None."""
    entries = [
        _entry(plf=50.0, af=10000.0, fare=4000.0, is_failure=True),
        _entry(plf=70.0, af=20000.0, fare=6000.0, is_failure=True),
    ]
    agg = compute_portfolio_aggregates(entries)

    assert set(agg.keys()) == set(ALL_KEYS)
    for k in ALL_KEYS:
        assert agg[k] is None, f"{k} should be None for all-failure portfolio"


def test_empty_input_yields_all_null_aggregates():
    """Empty entry list → all None (R4.6)."""
    agg = compute_portfolio_aggregates([])

    assert set(agg.keys()) == set(ALL_KEYS)
    for k in ALL_KEYS:
        assert agg[k] is None


# ---------------------------------------------------------------------------
# Mixed failure / success (R4.4)
# ---------------------------------------------------------------------------


def test_mixed_failure_excluded_from_aggregates():
    """Failure entries are dropped before computing means and stds."""
    entries = [
        _entry(plf=50.0, af=10000.0, fare=4000.0, is_failure=True),
        _entry(plf=60.0, af=15000.0, fare=4500.0),
        _entry(plf=70.0, af=20000.0, fare=5500.0),
    ]
    agg = compute_portfolio_aggregates(entries)

    # Only the two successful entries should show up
    assert agg["PDS"] == pytest.approx(np.mean([60.0, 70.0]))
    assert agg["sd_PDS"] == pytest.approx(np.std([60.0, 70.0], ddof=0))
    assert agg["PDQ"] == pytest.approx(np.mean([15000.0, 20000.0]))
    assert agg["sd_PDQ"] == pytest.approx(np.std([15000.0, 20000.0], ddof=0))
    assert agg["PFA"] == pytest.approx(np.mean([4500.0, 5500.0]))
    assert agg["sd_PFA"] == pytest.approx(np.std([4500.0, 5500.0], ddof=0))


# ---------------------------------------------------------------------------
# NaN PLF tolerance (R4.5)
# ---------------------------------------------------------------------------


def test_nan_plf_excluded_from_plf_stats_only():
    """A NaN PLF should not poison AF / fare statistics."""
    entries = [
        _entry(plf=float("nan"), af=10000.0, fare=4000.0),
        _entry(plf=70.0, af=20000.0, fare=6000.0),
    ]
    agg = compute_portfolio_aggregates(entries)

    # PLF stats: only the 70.0 entry contributes
    assert agg["PDS"] == pytest.approx(70.0)
    assert agg["sd_PDS"] == pytest.approx(0.0)

    # AF / fare stats: both entries contribute
    assert agg["PDQ"] == pytest.approx(15000.0)
    assert agg["sd_PDQ"] == pytest.approx(5000.0)
    assert agg["PFA"] == pytest.approx(5000.0)
    assert agg["sd_PFA"] == pytest.approx(1000.0)


def test_none_and_na_string_treated_like_nan():
    """`None` and the string sentinel ``"NA"`` are coerced to NaN."""
    entries = [
        _entry(plf=None, af="NA", fare=None),
        _entry(plf=70.0, af=20000.0, fare=6000.0),
    ]
    agg = compute_portfolio_aggregates(entries)

    assert agg["PDS"] == pytest.approx(70.0)
    assert agg["PDQ"] == pytest.approx(20000.0)
    assert agg["PFA"] == pytest.approx(6000.0)
    assert agg["sd_PDS"] == pytest.approx(0.0)
    assert agg["sd_PDQ"] == pytest.approx(0.0)
    assert agg["sd_PFA"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Empty Portfolio88 (R4.3)
# ---------------------------------------------------------------------------


def test_no_flights_below_88_yields_null_subset_fields():
    """When no flight has PLF < 88, the six 88-suffixed fields are None."""
    entries = [
        _entry(plf=90.0, af=20000.0, fare=5000.0),
        _entry(plf=95.0, af=22000.0, fare=5500.0),
    ]
    agg = compute_portfolio_aggregates(entries)

    # Full-portfolio fields are populated
    assert agg["PDS"] is not None
    assert agg["sd_PDS"] is not None
    assert agg["PDQ"] is not None
    assert agg["sd_PDQ"] is not None
    assert agg["PFA"] is not None
    assert agg["sd_PFA"] is not None

    # 88 subset is empty → all six are None
    for k in SUB_KEYS:
        assert agg[k] is None, f"{k} should be None when Portfolio88 is empty"


def test_partial_overlap_with_88_subset():
    """Only the under-88 flights should land in the 88 subset stats."""
    entries = [
        _entry(plf=60.0, af=15000.0, fare=4500.0),  # in 88 subset
        _entry(plf=70.0, af=20000.0, fare=5500.0),  # in 88 subset
        _entry(plf=92.0, af=25000.0, fare=6500.0),  # NOT in 88 subset
    ]
    agg = compute_portfolio_aggregates(entries)

    # Full set spans all three flights
    assert agg["PDS"] == pytest.approx(np.mean([60.0, 70.0, 92.0]))

    # 88 subset spans only the first two
    assert agg["PDS88"] == pytest.approx(np.mean([60.0, 70.0]))
    assert agg["sd_PDS88"] == pytest.approx(np.std([60.0, 70.0], ddof=0))
    assert agg["PDQ88"] == pytest.approx(np.mean([15000.0, 20000.0]))
    assert agg["sd_PDQ88"] == pytest.approx(np.std([15000.0, 20000.0], ddof=0))
    assert agg["PFA88"] == pytest.approx(np.mean([4500.0, 5500.0]))
    assert agg["sd_PFA88"] == pytest.approx(np.std([4500.0, 5500.0], ddof=0))


def test_plf_exactly_88_is_excluded_from_subset():
    """The Portfolio88 cutoff is strict: PLF == 88 is NOT in the subset."""
    entries = [
        _entry(plf=88.0, af=20000.0, fare=5000.0),
        _entry(plf=87.999, af=21000.0, fare=5100.0),
    ]
    agg = compute_portfolio_aggregates(entries)

    assert agg["PDS88"] == pytest.approx(round(87.999, 2))
    assert agg["sd_PDS88"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Result-shape contract
# ---------------------------------------------------------------------------


def test_result_always_has_all_twelve_keys():
    """All twelve keys are always present, regardless of input."""
    cases = [
        [],
        [_entry(plf=60.0, af=15000.0, fare=4500.0)],
        [_entry(plf=60.0, af=15000.0, fare=4500.0, is_failure=True)],
        [_entry(plf=92.0, af=25000.0, fare=6500.0)],
    ]
    for entries in cases:
        agg = compute_portfolio_aggregates(entries)
        assert set(agg.keys()) == set(ALL_KEYS)


def test_returns_python_floats_not_numpy_scalars():
    """JSON-serialisable: all numeric values are plain Python floats."""
    entries = [
        _entry(plf=60.0, af=15000.0, fare=4500.0),
        _entry(plf=70.0, af=20000.0, fare=5500.0),
    ]
    agg = compute_portfolio_aggregates(entries)

    for k, v in agg.items():
        if v is None:
            continue
        assert isinstance(v, float), f"{k}={v!r} is {type(v).__name__}, expected float"
        assert not math.isnan(v), f"{k} is NaN; expected None or a finite float"
