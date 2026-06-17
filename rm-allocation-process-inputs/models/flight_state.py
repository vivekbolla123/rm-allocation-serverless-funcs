from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class FlightState:
	"""Mutable container for the per-flight state threaded through ``DynAlloc``'s
	phase methods (``_phaseA_initFlightContext`` … ``_phaseN_writeSummary``).

	The fields mirror the locals that today live inside
	``runDynamicAllocationSingleFlight`` in ``runAlloc.py``. They are populated in
	order by Phases A–N and consumed by the phases that come later in the
	pipeline. Building the state once and passing it through phase methods is the
	cheapest way to split the ~700-line orchestrator without behavioural change
	(R11 — bit-for-bit output preservation).

	This dataclass is internal to ``DynAlloc``; it is **not** the SQS payload that
	travels between the fare-anchor and allocation passes (the payload is a
	serialisable subset that is rebuilt by the allocation pass via
	``_rehydrateFlightState`` in a later slice).

	See ``.kiro/specs/portfolio-anchor-split/design.md`` §FlightState for the
	canonical field list — the structure here matches that section verbatim.
	"""

	# Inputs from the caller (set up by Phase A from method arguments)
	currFlt: Any = ""
	currFltDate: str = ""                       # MM/DD/YYYY string
	currFltDate_dt: Optional[datetime] = None   # parsed datetime
	currFltDepartureTime: str = ""              # HH:MM:SS string
	currLid: Any = 0
	currAdjustedCapacity: Any = 0
	currtbfBookedLoad: Any = 0
	dateFormatted_yyyymmdd: str = ""

	# Phase A
	current_w0Flag: int = 0
	current_directFlag: int = 1
	actualplf: float = 0.0
	param_dow_str: str = ""
	param_flight_number_str: str = ""

	# Phase B
	base_totalCapacity: float = 0.0
	base_adjustedCapacity: float = 0.0
	final_totalCapacity: float = 0.0
	final_plf: float = 0.0
	overBookingCount: float = 0.0
	expectedNoShows: Any = None
	b2boverbookingflag: bool = False
	currBookedLoad: float = 0.0
	initial_currBookedLoad: float = 0.0
	currentTime: str = ""
	tgtBookedLoadPercent: float = 0.0

	# Phase C
	sod_dplfBand: int = 0
	sod_bookingToday: int = 0
	sod_BookingsInHour: int = 0
	sod_eodTgtBookedLoad_abs: float = 0.0
	sod_tgtAtCurrentHour_abs: float = 0.0
	sod_startOfDayBookLoad_factor: float = 0.0
	sod_startOfDayTgtBookedLoad_abs: float = 0.0
	distress_pax_count: int = 0
	variance_full: float = 0.0
	ndoBand_full: int = 0
	dplfBand_full: int = 0
	forecast_plf: Any = None
	forecastBand: Any = None
	imp_forecast: Any = None
	imp_forecast_band: Any = None
	af_forecast_band: Any = None
	Af_Nb_Allin: Any = None
	PLF_Gain: Any = None
	ndocounter: int = 0
	dow: int = 0

	# Phase D
	dow_matched: bool = True
	dow_condition_met: bool = False

	# Phase E (per channel)
	startTime_b2b: str = ""
	endTime_b2b: str = ""
	TimeRange_b2b: str = ""
	hardstartTime_b2b: str = ""
	hardendTime_b2b: str = ""
	criteria_b2b: str = ""
	hardcriteria_b2b: str = ""
	fareOffset_b2b: Any = None
	hardoffset_b2b: Any = None
	StrategyReference_b2b: str = ""
	hardfare_b2b: Any = None
	startTime_b2c: str = ""
	endTime_b2c: str = ""
	TimeRange_b2c: str = ""
	hardstartTime_b2c: str = ""
	hardendTime_b2c: str = ""
	criteria_b2c: str = ""
	hardcriteria_b2c: str = ""
	fareOffset_b2c: Any = None
	hardoffset_b2c: Any = None
	StrategyReference_b2c: str = ""
	hardfare_b2c: Any = None
	DaySpan_full: int = 0
	input_time_str_dep_full: str = ""

	# Phase F (per channel)
	MktFare_Min_b2b: Any = None
	MktFare_Max_b2b: Any = None
	AirlineCode_Min_b2b: str = ""
	AirlineCode_Max_b2b: str = ""
	FlightNumber_Min_b2b: str = ""
	FlightNumber_Max_b2b: str = ""
	isB2BMarketFareAvailable: bool = False
	lowmktFile_b2b: str = ""
	himktFile_b2b: str = ""
	MktFare_Min_b2c: Any = None
	MktFare_Max_b2c: Any = None
	AirlineCode_Min_b2c: str = ""
	AirlineCode_Max_b2c: str = ""
	FlightNumber_Min_b2c: str = ""
	FlightNumber_Max_b2c: str = ""
	isB2CMarketFareAvailable: bool = False
	lowmktFile_b2c: str = ""
	himktFile_b2c: str = ""

	# Phase G
	bookloadPercentile_full: int = 0
	percentile_str_full: str = ""
	upsellValue_full: float = 0.0
	sector_full: str = ""
	profileFare: Any = None
	profileFaresData: Any = None
	profileExtract_df_fullalloc: Any = None
	profileFares_list_sorted_fullalloc: list = field(default_factory=list)
	month_str_for_profile: str = ""
	dow_str_for_profile: str = ""
	deptTime_str_hhmm: str = ""

	# Phase H
	b2c_backstop_final: Any = None
	b2b_backstop_final: Any = None
	auto_b2c_backstop_str: str = "NA"
	auto_b2b_backstop_str: str = "NA"

	# Phase I
	lastSoldFareb2c_val: Any = None
	lastSoldFareb2b_val: Any = None
	last_seven_avg_val: Any = None

	# Phase J
	is_spoilage_reduction_active: bool = False

	# Phase K (anchor)
	anchorResult_b2c: Any = None     # AnchorResult dataclass from DetermineRBD
	anchorResult_b2b: Any = None
	b2b_anchor_source: Any = None
	B2B_thresholdflag: bool = False
	p_thresholdflag: bool = False
	l_thresholdflag: bool = False
	threshold_flags: dict = field(default_factory=dict)  # softblock RBDs

	# Phase K (ladder, post-rehydration)
	openDataB2C_details: dict = field(default_factory=dict)
	openDataB2B_details: dict = field(default_factory=dict)
	determined_overBookingFare: float = 0.0
	# Phase K head outputs threaded into _computeFareAnchors / _phaseK_applyLadder
	# so the two-pass split can reproduce today's interleaved B2C → B2B flow
	# without re-running the candidate-selection branch from scratch.
	should_process_K: bool = False
	is_last7avg_anchor_b2c: bool = False
	is_profile_anchor_b2c: bool = False
	B2C_opening_price_val: Any = None

	# Phase L outputs
	currRec_fullalloc: Any = None
	b2cAllocUnits_val: Any = None
	b2c_opening_price_val: Any = None
	first_rbd_alloc: Any = None
	other_rbd_alloc: Any = None
	current_nav_update_method_full: str = ""
	grp_adj_profile_fare: Any = None
	grp_adj_bookload: Any = None
	grp_fare_anchor: Any = None

	# Phase K (anchor) — additional locals threaded into Phase L/N
	hardfareb2c_updated: Any = None
	hardfareb2b_updated: Any = None
	current_discount_amount: float = 0.0
	b2b_discount_fare_active: bool = False

	# Phase N
	outputSummaryRow_b2c: Any = None
	outputSummaryRow_b2b: Any = None

	# Failure / abort flags
	is_failure: bool = False
	failure_reason: str = ""
	aborted: bool = False

	# Portfolio
	portfolio_count: int = 1
