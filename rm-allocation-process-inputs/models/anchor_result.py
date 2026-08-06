from dataclasses import dataclass, field
from typing import Any, Optional, Tuple
from Constants import CONST_NA

@dataclass
class AnchorResult:
    """Structured output of the anchor-selection block of ``DetermineRBD.determineRBDToOpen``.

    This dataclass captures every value that the first half of ``determineRBDToOpen`` produces
    so that the ladder-lookup tail (``applyAnchorToLadder``) can run as a separate method
    without needing to thread a long argument list. It is also the canonical payload that
    will eventually be serialised between the fare-anchor pass and the allocation pass
    (see ``portfolio-anchor-split`` design.md).

    Fields populated by ``computeFinalAnchor``:
        anchorFare, strategyFare, profileFare, lastSellingFare, hardAnchorFare,
        hard_anchor_flag, toleranceValue, anchor_source, channel, openResult, upsell,
        overBookingFare, anchorFareDiscounted

    Fields enumerated in tasks.md §1.1 that are populated by Phases F/G upstream of
    ``DetermineRBD`` (left at ``CONST_NA``/``None``/``""`` defaults here so callers can
    fill them in before serialisation):
        criteria, fareOffset, MktFare_Min, MktFare_Max, AirlineCode_Min, AirlineCode_Max,
        FlightNumber_Min, FlightNumber_Max, lowmktFile, himktFile, isMarketFareAvailable,
        startTime, endTime, TimeRange, StrategyReference

    Internal control fields:
        early_return       — True when the anchor-selection block hit a short-circuit return
                              and ``applyAnchorToLadder`` should return the captured tuple
                              verbatim instead of running the ladder lookup.
        early_return_tuple — the exact 9-tuple to return when ``early_return`` is True.
    """

    # Final anchor outputs
    anchorFare: Any = 0
    anchorFareBeforeTolerance: Any = CONST_NA
    strategyFare: Any = CONST_NA
    anchorFareDiscounted: Any = CONST_NA
    profileFare: Any = CONST_NA
    lastSellingFare: Any = CONST_NA
    hardAnchorFare: Any = CONST_NA
    hard_anchor_flag: bool = False
    toleranceValue: Any = CONST_NA
    T_prime: Any = CONST_NA
    anchor_source: Any = CONST_NA  # the howDetermined string
    channel: str = ""
    upsell: Any = CONST_NA
    overBookingFare: Any = 0
    openResult: Any = None  # pd.DataFrame or dict, mirrors today's openResult intermediate

    # Phase F/G metadata (populated upstream; not computed inside DetermineRBD today)
    criteria: Any = CONST_NA
    fareOffset: Any = CONST_NA
    MktFare_Min: Any = CONST_NA
    MktFare_Max: Any = CONST_NA
    AirlineCode_Min: str = ""
    AirlineCode_Max: str = ""
    FlightNumber_Min: str = ""
    FlightNumber_Max: str = ""
    lowmktFile: str = ""
    himktFile: str = ""
    isMarketFareAvailable: bool = False
    startTime: str = ""
    endTime: str = ""
    TimeRange: str = ""
    StrategyReference: str = ""

    # Internal control flow
    early_return: bool = False
    early_return_tuple: Optional[Tuple[Any, ...]] = None
    is_skipped: bool = False
