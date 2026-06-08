"""Portfolio aggregate calculation for the allocation pass.

Pure-function module — no DB / Redis / SQS dependencies.

`compute_portfolio_aggregates(entries)` consumes a list of `PortfolioEntry`
dicts (built by the fare-anchor pass) and returns the twelve summary
statistics defined in `requirements.md` R4 and `design.md` §
`portfolio_aggregator.py`.

Conventions:
- `is_failure == True` entries are excluded from every statistic (R4.4).
- Per-statistic NaN / `None` / `"NA"` values are excluded from that
  statistic only; the entry remains eligible for the other statistics
  (R4.5).
- Standard deviations use population formula (`ddof=0`).
- Portfolio88 = subset where `Imputed_ForecastPLF < 88`. When that subset
  is empty, the six 88-suffixed fields are returned as ``None`` (R4.3).
- When no successful entries remain after filtering, every field is
  returned as ``None`` (R4.6).
"""

from __future__ import annotations

from typing import Any, Iterable

import numpy as np

# Field names of the entry that feed each statistic.  Kept as module
# constants so they line up with the entry schema in `design.md`.
_PLF_FIELD = "Imputed_ForecastPLF"
_AF_FIELD = "ForecastAF_Allin"
_FARE_FIELD = "anchorFare_b2c"

_FULL_KEYS = ("PDS", "sd_PDS", "PDQ", "sd_PDQ", "PFA", "sd_PFA")
_SUB_KEYS = ("PDS88", "sd_PDS88", "PDQ88", "sd_PDQ88", "PFA88", "sd_PFA88")


def compute_portfolio_aggregates(entries: Iterable[dict]) -> dict:
    """Return the 12 portfolio aggregate fields.

    Parameters
    ----------
    entries : iterable of dict
        Portfolio entries produced by the fare-anchor pass.  Each entry
        is expected to carry ``Imputed_ForecastPLF``, ``ForecastAF_Allin``
        and ``anchorFare_b2c`` keys plus an ``is_failure`` flag.

    Returns
    -------
    dict
        Keys: ``PDS, sd_PDS, PDQ, sd_PDQ, PFA, sd_PFA,
        PDS88, sd_PDS88, PDQ88, sd_PDQ88, PFA88, sd_PFA88``.
    """
    successful = [e for e in entries if not e.get("is_failure")]
    if not successful:
        return _null_aggregates()

    plfs = _to_float_array(e.get(_PLF_FIELD) for e in successful)
    afs = _to_float_array(e.get(_AF_FIELD) for e in successful)
    fares = _to_float_array(e.get(_FARE_FIELD) for e in successful)

    full = {
        "PDS": _safe_mean(plfs),
        "sd_PDS": _safe_std(plfs),
        "PDQ": _safe_mean(afs),
        "sd_PDQ": _safe_std(afs),
        "PFA": _safe_mean(fares),
        "sd_PFA": _safe_std(fares),
    }

    # Portfolio88 subset: only flights whose PLF is a real number < 88.
    plf_mask = ~np.isnan(plfs)
    sub_mask = plf_mask & (plfs < 88)

    if not sub_mask.any():
        sub = {k: None for k in _SUB_KEYS}
    else:
        plfs88 = plfs[sub_mask]
        afs88 = afs[sub_mask]
        fares88 = fares[sub_mask]
        sub = {
            "PDS88": _safe_mean(plfs88),
            "sd_PDS88": _safe_std(plfs88),
            "PDQ88": _safe_mean(afs88),
            "sd_PDQ88": _safe_std(afs88),
            "PFA88": _safe_mean(fares88),
            "sd_PFA88": _safe_std(fares88),
        }

    return {**full, **sub}


def _to_float_array(values: Iterable[Any]) -> np.ndarray:
    """Coerce a sequence of mixed values to a float ndarray.

    ``None`` and string sentinels like ``"NA"`` become ``NaN`` so they
    are skipped by ``numpy.nanmean`` / ``numpy.nanstd``.
    """
    coerced = []
    for v in values:
        if v is None:
            coerced.append(np.nan)
            continue
        try:
            coerced.append(float(v))
        except (TypeError, ValueError):
            coerced.append(np.nan)
    return np.array(coerced, dtype=float)


def _safe_mean(arr: np.ndarray):
    if arr.size == 0 or np.all(np.isnan(arr)):
        return None
    return round(float(np.nanmean(arr)), 2)


def _safe_std(arr: np.ndarray):
    if arr.size == 0 or np.all(np.isnan(arr)):
        return None
    return round(float(np.nanstd(arr, ddof=0)), 2)


def _null_aggregates() -> dict:
    return {k: None for k in (*_FULL_KEYS, *_SUB_KEYS)}
