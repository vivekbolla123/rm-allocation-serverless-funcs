"""Unit tests for `helper.zscore_calc.compute_flight_zscores`.

Validates: Requirements R5.1 - R5.5 from `portfolio-anchor-split/requirements.md`.

Coverage required by Slice 5: std=0, negative-clamp, positive-clamp,
null aggregates, null fare.
"""

from __future__ import annotations

import os
import sys

import pytest

# Make the Lambda package importable from a repo-root pytest run.
_HELPER_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "rm-allocation-process-inputs")
)
if _HELPER_DIR not in sys.path:
    sys.path.insert(0, _HELPER_DIR)

from helper.zscore_calc import compute_flight_zscores  # noqa: E402


class DummyConstants:
    W_PLF = 0.7
    W_AF = 0.3
    W_ANCHOR = -0.25
    MOVEMENT_GAIN = 6
    MOVEMENT_FLOOR_PCT = -12
    MOVEMENT_CEIL_PCT = 0

_DUMMY_CONSTANTS = DummyConstants()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EXPECTED_KEYS = (
    "z_score_composite",
    "Portfolio_Anchor",
    "z_score88",
    "Portfolio88_Anchor",
)


def _entry(plf=70.0, af=20000.0, fare=5000.0):
    return {
        "Imputed_ForecastPLF": plf,
        "ForecastAF_Allin": af,
        "anchorFare_b2c": fare,
    }


def _aggregates(
    pds=None, sd_pds=None, pdq=None, sd_pdq=None, pfa=None, sd_pfa=None,
    pds88=None, sd_pds88=None, pdq88=None, sd_pdq88=None, pfa88=None, sd_pfa88=None,
):
    return {
        "PDS": pds, "sd_PDS": sd_pds,
        "PDQ": pdq, "sd_PDQ": sd_pdq,
        "PFA": pfa, "sd_PFA": sd_pfa,
        "PDS88": pds88, "sd_PDS88": sd_pds88,
        "PDQ88": pdq88, "sd_PDQ88": sd_pdq88,
        "PFA88": pfa88, "sd_PFA88": sd_pfa88,
    }


# ---------------------------------------------------------------------------
# std = 0 path (R5.2, R5.4)
# ---------------------------------------------------------------------------


def test_std_zero_yields_zero_composite_and_anchor_equals_fare():
    """Validates: Requirements 5.2 and 5.4.

    A single-flight portfolio degenerates to mean = entry value, std = 0.
    Every component z-score must be 0, the composite must be 0, movement
    must be 0, and ``Portfolio_Anchor`` must equal ``anchorFare_b2c``
    exactly so the single-flight short-circuit is a no-op (R6.2).
    """
    entry = _entry(plf=70.0, af=20000.0, fare=5000.0)
    aggregates = _aggregates(
        pds=70.0, sd_pds=0.0, pdq=20000.0, sd_pdq=0.0, pfa=5000.0, sd_pfa=0.0,
        pds88=70.0, sd_pds88=0.0, pdq88=20000.0, sd_pdq88=0.0,
        pfa88=5000.0, sd_pfa88=0.0,
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["z_score_composite"] == pytest.approx(0.0)
    assert result["Portfolio_Anchor"] == pytest.approx(5000.0)
    assert result["z_score88"] == pytest.approx(0.0)
    assert result["Portfolio88_Anchor"] == pytest.approx(5000.0)


def test_std_zero_for_one_component_only_zeros_that_component_only():
    """Validates: Requirements 5.2.

    Per R5.2 the zero-divisor rule is per-component: only the component
    whose std is 0 collapses to 0, the others contribute normally.
    """
    entry = _entry(plf=80.0, af=25000.0, fare=5000.0)
    # sd_PDS is 0 → z_PLF = 0; AF and fare remain meaningful.
    aggregates = _aggregates(
        pds=80.0, sd_pds=0.0,
        pdq=20000.0, sd_pdq=5000.0,
        pfa=4000.0, sd_pfa=1000.0,
        # 88 fields all None → z88 = 0 across the board
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    # z_PLF = 0; z_AF = (25000 - 20000) / 5000 = 1; z_anchor = (5000-4000)/1000 = 1
    # composite = 0.7*0 + 0.3*1 - 0.25*1 = 0.05  → movement_pct = 0.3% → clamped to 0
    expected_composite = 0.7 * 0 + 0.3 * 1.0 - 0.25 * 1.0
    assert result["z_score_composite"] == pytest.approx(expected_composite)
    # 0.05 * 6 = 0.3 > 0 → clamp top → movement = 0 → anchor unchanged
    assert result["Portfolio_Anchor"] == pytest.approx(5000.0)


# ---------------------------------------------------------------------------
# Negative clamp (R5.1)
# ---------------------------------------------------------------------------


def test_negative_clamp_caps_movement_at_minus_12_percent():
    """Validates: Requirements 5.1.

    With z_composite well below -2, ``z * 6`` falls below -12 and the
    movement floor pins ``Portfolio_Anchor`` at ``fare * 0.88``.
    """
    entry = _entry(plf=50.0, af=10000.0, fare=6000.0)
    # z_PLF = (50-80)/10 = -3
    # z_AF  = (10000-20000)/5000 = -2
    # z_anchor = (6000-4000)/1000 = +2
    # composite = 0.7*(-3) + 0.3*(-2) - 0.25*(+2) = -2.1 - 0.6 - 0.5 = -3.2
    # composite * 6 = -19.2  → clamps to -12  → movement = -0.12
    aggregates = _aggregates(
        pds=80.0, sd_pds=10.0,
        pdq=20000.0, sd_pdq=5000.0,
        pfa=4000.0, sd_pfa=1000.0,
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["z_score_composite"] == pytest.approx(-3.2)
    assert result["Portfolio_Anchor"] == pytest.approx(6000.0 * (1.0 - 0.12))


def test_negative_no_clamp_in_band_movement_is_proportional():
    """Validates: Requirements 5.1.

    A composite z-score that lands inside the ``[-2, 0]`` band passes
    through to ``movement = z * 6 / 100`` without clamping.
    """
    entry = _entry(plf=65.0, af=18000.0, fare=5000.0)
    # z_PLF = (65-70)/5 = -1; z_AF = (18000-20000)/2000 = -1; z_anchor = 0
    # composite = 0.7*(-1) + 0.3*(-1) - 0 = -1.0
    # composite * 6 = -6 → in-band → movement = -0.06 → anchor = 5000 * 0.94
    aggregates = _aggregates(
        pds=70.0, sd_pds=5.0,
        pdq=20000.0, sd_pdq=2000.0,
        pfa=5000.0, sd_pfa=1000.0,
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["z_score_composite"] == pytest.approx(-1.0)
    assert result["Portfolio_Anchor"] == pytest.approx(5000.0 * (1.0 - 0.06))


# ---------------------------------------------------------------------------
# Positive clamp (R5.1)
# ---------------------------------------------------------------------------


def test_positive_clamp_holds_movement_at_zero_and_keeps_fare_unchanged():
    """Validates: Requirements 5.1 and 5.4.

    A positive composite z-score would suggest moving the anchor
    upward, but the clamp ceiling pins movement at 0 — the portfolio
    anchor never exceeds ``anchorFare_b2c``.
    """
    entry = _entry(plf=90.0, af=25000.0, fare=4000.0)
    # z_PLF = +2, z_AF = +1, z_anchor = -1
    # composite = 0.7*2 + 0.3*1 - 0.25*(-1) = 1.4 + 0.3 + 0.25 = +1.95
    # composite * 6 = 11.7 > 0 → clamp top → movement = 0 → anchor = fare
    aggregates = _aggregates(
        pds=70.0, sd_pds=10.0,
        pdq=20000.0, sd_pdq=5000.0,
        pfa=5000.0, sd_pfa=1000.0,
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["z_score_composite"] == pytest.approx(1.95)
    assert result["Portfolio_Anchor"] == pytest.approx(4000.0)


# ---------------------------------------------------------------------------
# Null aggregates (R5.2, R5.3)
# ---------------------------------------------------------------------------


def test_null_aggregates_yield_zero_composite_and_unchanged_anchor():
    """Validates: Requirements 5.2 and 5.3.

    When every aggregate field is ``None`` (e.g. R4.6 empty portfolio
    case), each component z-score collapses to 0, composite is 0,
    movement is 0, and ``Portfolio_Anchor`` equals ``anchorFare_b2c``.
    """
    entry = _entry(plf=70.0, af=20000.0, fare=5000.0)
    aggregates = _aggregates()  # every field None

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["z_score_composite"] == pytest.approx(0.0)
    assert result["Portfolio_Anchor"] == pytest.approx(5000.0)
    assert result["z_score88"] == pytest.approx(0.0)
    assert result["Portfolio88_Anchor"] == pytest.approx(5000.0)


def test_null_88_aggregates_zero_only_the_88_branch():
    """Validates: Requirements 5.2 and 5.3.

    When the Portfolio88 subset is empty (R4.3) only its z-scores must
    collapse to 0; the full-portfolio path still produces a real
    composite and a moved anchor.
    """
    entry = _entry(plf=60.0, af=15000.0, fare=5000.0)
    # Full-portfolio path: z_PLF = -1, z_AF = -1, z_anchor = 0
    # composite = -1.0 → movement = -0.06 → anchor = 4700
    aggregates = _aggregates(
        pds=70.0, sd_pds=10.0,
        pdq=20000.0, sd_pdq=5000.0,
        pfa=5000.0, sd_pfa=1000.0,
        # 88 fields all None
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["z_score_composite"] == pytest.approx(-1.0)
    assert result["Portfolio_Anchor"] == pytest.approx(5000.0 * 0.94)
    # 88-branch components are all None → z_score88 must be 0 and
    # Portfolio88_Anchor stays at fare.
    assert result["z_score88"] == pytest.approx(0.0)
    assert result["Portfolio88_Anchor"] == pytest.approx(5000.0)


# ---------------------------------------------------------------------------
# Null / NA fare and null inputs (R5.3, R5.5)
# ---------------------------------------------------------------------------


def test_null_fare_yields_null_portfolio_anchor():
    """Validates: Requirements 5.3 and 5.5.

    When ``anchorFare_b2c`` is ``None`` (failure flight per R8.4),
    both ``Portfolio_Anchor`` fields must be ``None``.  ``z_anchor``
    contributes 0 to the composite per R5.3.
    """
    entry = _entry(plf=70.0, af=20000.0, fare=None)
    aggregates = _aggregates(
        pds=80.0, sd_pds=10.0,
        pdq=25000.0, sd_pdq=5000.0,
        pfa=5000.0, sd_pfa=1000.0,
        pds88=80.0, sd_pds88=10.0,
        pdq88=25000.0, sd_pdq88=5000.0,
        pfa88=5000.0, sd_pfa88=1000.0,
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["Portfolio_Anchor"] is None
    assert result["Portfolio88_Anchor"] is None
    # The z-scores still compute from the non-null PLF and AF inputs.
    # z_PLF = -1, z_AF = -1, z_anchor = 0 → composite = -1.0
    assert result["z_score_composite"] == pytest.approx(-1.0)


def test_na_string_fare_yields_null_portfolio_anchor():
    """Validates: Requirements 5.3.

    The ``"NA"`` sentinel produced upstream must be treated like ``None``
    everywhere — in particular ``Portfolio_Anchor`` must be ``None``.
    """
    entry = _entry(plf=70.0, af=20000.0, fare="NA")
    aggregates = _aggregates(
        pds=70.0, sd_pds=5.0,
        pdq=20000.0, sd_pdq=2000.0,
        pfa=4000.0, sd_pfa=1000.0,
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["Portfolio_Anchor"] is None
    assert result["Portfolio88_Anchor"] is None


def test_null_input_value_zeros_only_that_component():
    """Validates: Requirements 5.3.

    A null PLF must zero only ``z_PLF``; AF and fare components still
    contribute to the composite.
    """
    entry = _entry(plf=None, af=25000.0, fare=4000.0)
    # z_PLF = 0 (null), z_AF = (25000-20000)/5000 = 1,
    # z_anchor = (4000-5000)/1000 = -1
    # composite = 0.7*0 + 0.3*1 - 0.25*(-1) = 0.55
    # composite * 6 = 3.3 > 0 → clamp top → movement = 0 → anchor = fare
    aggregates = _aggregates(
        pds=70.0, sd_pds=10.0,
        pdq=20000.0, sd_pdq=5000.0,
        pfa=5000.0, sd_pfa=1000.0,
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert result["z_score_composite"] == pytest.approx(0.55)
    assert result["Portfolio_Anchor"] == pytest.approx(4000.0)


# ---------------------------------------------------------------------------
# Result-shape contract (R5.5)
# ---------------------------------------------------------------------------


def test_result_always_has_the_four_summary_keys():
    """Validates: Requirements 5.5.

    Every call returns exactly the four documented summary keys.
    """
    entry = _entry()
    aggregates = _aggregates(
        pds=70.0, sd_pds=10.0, pdq=20000.0, sd_pdq=5000.0,
        pfa=5000.0, sd_pfa=1000.0,
    )

    result = compute_flight_zscores(entry, aggregates, _DUMMY_CONSTANTS)

    assert set(result.keys()) == set(EXPECTED_KEYS)
