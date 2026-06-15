"""Per-flight z-score and portfolio-anchor calculation for the allocation pass.

Pure-function module — no DB / Redis / SQS dependencies.

`compute_flight_zscores(entry, aggregates)` consumes a single
`PortfolioEntry` dict (built by the fare-anchor pass) plus the twelve
aggregate fields produced by `helper.portfolio_aggregator` and returns
the four summary fields enumerated in `requirements.md` R5 and
`design.md` § `zscore_calc.py`:

- ``z_score_composite``
- ``Portfolio_Anchor``
- ``z_score88``
- ``Portfolio88_Anchor``

Conventions:
- Composite z-score formula (R5.1):
  ``0.7 * z_PLF + 0.3 * z_AF - 0.25 * z_anchor``.
- Movement is clamped into ``[-12, 0]`` percent (the portfolio-aware
  anchor only ever moves downward, never upward), then divided by 100
  to turn it into a fractional adjustment.
- Any z-score whose input value, mean, or std is null / ``"NA"`` / NaN
  returns 0 (R5.2, R5.3).
- Any z-score whose std is exactly 0 returns 0 (R5.2).  This also makes
  a single-flight portfolio degenerate cleanly: every component becomes
  zero, movement is zero, and ``Portfolio_Anchor == anchorFare_b2c``
  exactly (R5.4, R6.2).
- ``Portfolio_Anchor`` and ``Portfolio88_Anchor`` are ``None`` when the
  flight has no usable ``anchorFare_b2c``.
"""

from __future__ import annotations

import math
from typing import Any



def compute_flight_zscores(entry: dict, aggregates: dict, constants_utils: Any = None) -> dict:
    """Compute the four R5 summary fields for a single flight.

    Parameters
    ----------
    entry : dict
        Portfolio entry produced by the fare-anchor pass.  Reads
        ``Imputed_ForecastPLF``, ``ForecastAF_Allin``, and
        ``anchorFare_b2c``.
    aggregates : dict
        Twelve-field dict returned by
        ``helper.portfolio_aggregator.compute_portfolio_aggregates``.

    Returns
    -------
    dict
        ``{"z_score_composite", "Portfolio_Anchor", "z_score88",
        "Portfolio88_Anchor"}``.
    """
    plf = entry.get("Imputed_ForecastPLF")
    af = entry.get("ForecastAF_Allin")
    fare = entry.get("anchorFare_b2c")
    fare_value = _coerce_number(fare)
    plf_value = _coerce_number(plf)

    # Full-portfolio composite + movement.
    z_composite = _composite_z(
        plf, af, fare,
        aggregates.get("PDS"), aggregates.get("sd_PDS"),
        aggregates.get("PDQ"), aggregates.get("sd_PDQ"),
        aggregates.get("PFA"), aggregates.get("sd_PFA"),
        constants_utils
    )
    movement_pct = _movement_fraction(z_composite, constants_utils)
    portfolio_anchor = (
        fare_value * (1.0 + movement_pct) if fare_value is not None else None
    )

    # Portfolio88 composite + movement.
    if plf_value is not None and plf_value < 88:
        z_composite88 = _composite_z(
            plf, af, fare,
            aggregates.get("PDS88"), aggregates.get("sd_PDS88"),
            aggregates.get("PDQ88"), aggregates.get("sd_PDQ88"),
            aggregates.get("PFA88"), aggregates.get("sd_PFA88"),
            constants_utils
        )
        movement88_pct = _movement_fraction(z_composite88, constants_utils)
        portfolio88_anchor = (
            fare_value * (1.0 + movement88_pct) if fare_value is not None else None
        )
    else:
        z_composite88 = None
        portfolio88_anchor = None

    return {
        "z_score_composite": round(z_composite, 2) if z_composite is not None else None,
        "Portfolio_Anchor": round(portfolio_anchor, 2) if portfolio_anchor is not None else None,
        "z_score88": round(z_composite88, 2) if z_composite88 is not None else None,
        "Portfolio88_Anchor": round(portfolio88_anchor, 2) if portfolio88_anchor is not None else None,
    }


def _composite_z(plf, af, fare,
                 pds, sd_pds, pdq, sd_pdq, pfa, sd_pfa, constants_utils=None) -> float:
    """Combine the three component z-scores into the composite (R5.1)."""
    w_plf = constants_utils.W_PLF
    w_af = constants_utils.W_AF
    w_anchor = constants_utils.W_ANCHOR

    z_plf = _safe_z(plf, pds, sd_pds)
    z_af = _safe_z(af, pdq, sd_pdq)
    z_anchor = _safe_z(fare, pfa, sd_pfa)
    return w_plf * z_plf + w_af * z_af + w_anchor * z_anchor


def _movement_fraction(z_composite: float, constants_utils=None) -> float:
    """Clamp ``z * gain`` into ``[-12, 0]`` percent and convert to a fraction."""
    movement_gain = constants_utils.MOVEMENT_GAIN
    movement_floor_pct = constants_utils.MOVEMENT_FLOOR_PCT
    movement_ceil_pct = constants_utils.MOVEMENT_CEIL_PCT

    pct = z_composite * movement_gain
    if pct < movement_floor_pct:
        pct = movement_floor_pct
    elif pct > movement_ceil_pct:
        pct = movement_ceil_pct
    return pct / 100.0


def _safe_z(value: Any, mean: Any, std: Any) -> float:
    """Return ``(value - mean) / std`` or 0 if any input is missing / std=0.

    Implements R5.2 (std=0 or null → z=0) and R5.3 (null / NA value or
    null mean → z=0).  Strings such as ``"NA"`` and NaN floats are also
    treated as missing.
    """
    v = _coerce_number(value)
    m = _coerce_number(mean)
    s = _coerce_number(std)
    if v is None or m is None or s is None:
        return 0.0
    if s == 0.0:
        return 0.0
    return (v - m) / s


def _coerce_number(value: Any):
    """Return ``value`` as a finite ``float``, or ``None`` if not numeric.

    ``None``, NaN, and string sentinels like ``"NA"`` all collapse to
    ``None`` so downstream callers can branch on a single check.
    """
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result
