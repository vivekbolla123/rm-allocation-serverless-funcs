"""Byte-for-byte equivalence tests for the ``DetermineRBD`` Phase K refactor.

These tests exercise ten curated scenarios drawn from the four short-circuit return
paths and the main happy-path branches of ``determineRBDToOpen``. For each scenario
we run both:

    legacy  = self.determinerbd.determineRBDToOpen(...)
    split   = self.determinerbd.applyAnchorToLadder(
                  self.determinerbd.computeFinalAnchor(...), currParams, backstop,
                  offset, strategy_criteria)

and assert ``legacy == split`` byte-for-byte (R2.2 / portfolio-anchor-split tasks 1.5).

Since ``determineRBDToOpen`` is now a thin wrapper around the two split methods, the
equality is structurally guaranteed; the value of these tests is to prove the refactor
chain (a) runs end-to-end on a representative spread of inputs without raising and
(b) returns the same 9-tuple shape today's callers in ``runAlloc.py`` /
``runAlloc_Connections.py`` rely on.

The tests stub out the database-touching helpers and the deep anchor-selection
methods (``finalFareAnchor`` / ``final_fare_anchor_forecast``) so the cases stay
deterministic without needing ``ConstantsUtils`` from MongoDB.
"""

import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

# Stand-in for the shared ``Constants`` module that gets copied into the Lambda
# build context at deploy time. The unit test runs from the source tree where
# ``Constants.py`` lives in ``rm-allocation-configurations``; expose the constants
# we need at the same import path the production code uses.
import sys, os
sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "rm-allocation-configurations")),
)

from determine_rbd import DetermineRBD, AnchorResult  # noqa: E402
from Constants import CONST_NA, B2B_CHANNEL, B2C_CHANNEL  # noqa: E402


def _make_open_result_df(fare):
    """Build the column-shape ``getValueWithOffset`` returns when an entry exists."""
    return pd.DataFrame({
        "RBD": ["Y"],
        "Fare": [fare],
        "FareResult": ["NA"],
        "anchorFare": [fare],
        "statusInd": [0],
        "allocationStatus": [1],
    })


def _make_rbddata_df(fare):
    return pd.DataFrame({"RBD": ["Y"], "TOTAL": [float(fare)]})


def _empty_rbddata_df():
    return pd.DataFrame(columns=["RBD", "TOTAL"])


def _build_curr_params(**overrides):
    """Minimal stand-in for ``MarketListRow`` / ``CurrParams`` used by DetermineRBD.

    Only the attributes ``determineRBDToOpen`` reads are provided.
    """
    base = MagicMock()
    base.origin = "BOM"
    base.destin = "DEL"
    base.forecastAllocFlag = 0
    base.afForecastAllocFlag = 0
    base.opening_fares = 0
    base.profileFares = 0
    base.market_discount = CONST_NA
    base.b2b_factor = 1.0
    base.flight_number = 1127
    base.per_start = "08/01/2023"
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


class _DetermineRBDFactory:
    """Builds a ``DetermineRBD`` instance with all collaborators mocked."""

    def __init__(self):
        self.rmdb = MagicMock()
        self.logger = MagicMock()
        self.inputsource = MagicMock()
        self.constantsutils = MagicMock()
        # Real numeric attributes the production code coerces to int/float.
        self.constantsutils.OVERBOOKING_FARE_LF = 100
        self.constantsutils.OVERBOOKING_LF = 100
        self.constantsutils.OVERBOOKING_MANUAL_LF = 100
        self.constantsutils.OVERBOOKING_FARE_PERCENT_VALUE = 0.1
        self.constantsutils.OVERBOOKING_FARE_ABSOLUTE_VALUE = 200
        self.mathutils = MagicMock()
        self.redishelper = MagicMock()
        self.instance = DetermineRBD(
            self.rmdb,
            self.logger,
            self.inputsource,
            self.constantsutils,
            self.mathutils,
            self.redishelper,
        )

    def stub_anchor_methods(self, anchor_fare=4500.0, how_determined="A"):
        """Replace deep anchor-selection methods with deterministic stubs.

        The production methods reach into many ``ConstantsUtils`` attributes; mocking
        them out removes a large class of irrelevant variability from the test.
        """
        self.instance.finalFareAnchor = MagicMock(
            return_value=(anchor_fare, how_determined, CONST_NA, 0, CONST_NA)
        )
        self.instance.final_fare_anchor_forecast = MagicMock(
            return_value=(anchor_fare, how_determined, CONST_NA, 0, CONST_NA)
        )


def _base_args(**overrides):
    """The 36 positional arguments ``determineRBDToOpen`` accepts.

    Defaults form a B2C + non-forecast happy path; overrides let each test case
    flip just the values it cares about.
    """
    args = dict(
        anchorFare=4500.0,
        offset=0,
        channel=B2C_CHANNEL,
        currFltDate="2023-08-01",
        dplfBand=4,
        ndocounter=0,
        currBookedLoad=10,
        totalCapacity=189,
        currParams=_build_curr_params(),
        backstop=0,
        currenttime="12:00",
        startEndOfDayDplfBand=4,
        dPLF=0.0,
        toleranceFare=CONST_NA,
        plf=5.0,
        overBookingCount=0,
        hardAnchorFare=CONST_NA,
        hardoffset="NA",
        BookingsInHour=0,
        lastSoldFare=CONST_NA,
        last7AvgVal=CONST_NA,
        last7avg=False,
        b2b_discount_fare=False,
        discount_amount=0,
        w0Flag=0,
        directFlag=1,
        obfare=CONST_NA,
        profileFare=CONST_NA,
        isProfileAnchor=False,
        imp_forecast=CONST_NA,
        imp_forecast_band=CONST_NA,
        strategy_criteria="MIN",
        af_forecast_band=CONST_NA,
        Af_Nb_Allin=CONST_NA,
        PLF_Gain=0,
        forecast_plf=CONST_NA,
    )
    args.update(overrides)
    return args


def _compare(legacy, split):
    """Compare the two 9-tuples element-wise, special-casing pandas containers."""
    assert len(legacy) == len(split), f"tuple length mismatch: {len(legacy)} vs {len(split)}"
    for idx, (a, b) in enumerate(zip(legacy, split)):
        if isinstance(a, pd.DataFrame) and isinstance(b, pd.DataFrame):
            pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True))
        elif isinstance(a, dict) and isinstance(b, dict):
            assert a == b, f"slot {idx} dict mismatch: {a} vs {b}"
        else:
            assert a == b, f"slot {idx} mismatch: {a!r} vs {b!r}"


class TestDetermineRBDSplit(unittest.TestCase):
    """Ten curated scenarios drawn from the legacy ``determineRBDToOpen`` paths."""

    def _run_case(self, configure_factory, args):
        # First pass: legacy wrapper.
        f1 = _DetermineRBDFactory()
        configure_factory(f1)
        legacy = f1.instance.determineRBDToOpen(**args)

        # Second pass: split methods invoked manually.
        f2 = _DetermineRBDFactory()
        configure_factory(f2)
        anchor = f2.instance.computeFinalAnchor(**args)
        split = f2.instance.applyAnchorToLadder(
            anchor,
            args["currParams"],
            args["backstop"],
            args["offset"],
            args["strategy_criteria"],
        )

        self.assertIsInstance(anchor, AnchorResult)
        _compare(legacy, split)

    # ------------------------------------------------------------------
    # Case 1: rbddata is None → early return path 1.
    # ------------------------------------------------------------------
    def test_case_01_rbddata_is_none_short_circuit(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(CONST_NA),
                None,
            )

        self._run_case(configure, _base_args())

    # ------------------------------------------------------------------
    # Case 2: rbddata empty + extreme fares lookup yields strategyFare == CONST_NA.
    # ------------------------------------------------------------------
    def test_case_02_strategyfare_is_na_short_circuit(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(CONST_NA),
                _empty_rbddata_df(),
            )
            # ``extremeFaresSkipping`` consults this; returning empty makes the helper
            # produce its "Unable to fetch extreme published Fares" branch where Fare
            # ends up CONST_NA, which short-circuits the second early return.
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                columns=["RBD", "TOTAL"]
            )

        self._run_case(configure, _base_args())

    # ------------------------------------------------------------------
    # Case 3: forecast branch hits anchorValue == 'C' early return.
    # ------------------------------------------------------------------
    def test_case_03_forecast_branch_anchor_value_c(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(4500.0),
                _make_rbddata_df(4500.0),
            )
            # getFares is invoked when rbddata.shape[0] != 0; have it return a result
            # with Fare populated so strategyFare ends up valid.
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                {"RBD": ["Y"], "TOTAL": [4500.0]}
            )
            # Forecast branch + imp_forecast_band == 2 → getRDBTable returns 'C'.
            f.rmdb.getRDBTable.return_value = pd.DataFrame(
                {"B2CValue": ["C"], "B2BValue": ["C"]}
            )
            f.stub_anchor_methods()

        args = _base_args(
            currParams=_build_curr_params(forecastAllocFlag=1),
            imp_forecast_band=2,
        )
        self._run_case(configure, args)

    # ------------------------------------------------------------------
    # Case 4: else branch hits anchorValue == 'C' early return.
    # ------------------------------------------------------------------
    def test_case_04_else_branch_anchor_value_c(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(4500.0),
                _make_rbddata_df(4500.0),
            )
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                {"RBD": ["Y"], "TOTAL": [4500.0]}
            )
            f.rmdb.getRDBTable.return_value = pd.DataFrame(
                {"B2CValue": ["C"], "B2BValue": ["C"]}
            )
            f.stub_anchor_methods()

        args = _base_args()
        self._run_case(configure, args)

    # ------------------------------------------------------------------
    # Case 5: B2B channel happy path → flows through to applyAnchorToLadder.
    # ------------------------------------------------------------------
    def test_case_05_b2b_channel_happy_path(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(4500.0),
                _make_rbddata_df(4500.0),
            )
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                {"RBD": ["Y"], "TOTAL": [4500.0]}
            )
            f.rmdb.getRDBTable.return_value = pd.DataFrame(
                {"B2CValue": ["A"], "B2BValue": ["A"]}
            )
            f.stub_anchor_methods()
            f.constantsutils.HIGHEST_B2B_RBD_VALUE = "Z0"
            f.constantsutils.LOWEST_B2B_RBD_VALUE = "Z9"
            f.constantsutils.LOWEST_B2C_RBD_VALUE = "WA"

        args = _base_args(channel=B2B_CHANNEL)
        self._run_case(configure, args)

    # ------------------------------------------------------------------
    # Case 6: B2C anchorValue == 'A' happy path.
    # ------------------------------------------------------------------
    def test_case_06_b2c_anchor_value_a(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(4500.0),
                _make_rbddata_df(4500.0),
            )
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                {"RBD": ["Y"], "TOTAL": [4500.0]}
            )
            f.rmdb.getRDBTable.return_value = pd.DataFrame(
                {"B2CValue": ["A"], "B2BValue": ["A"]}
            )
            f.stub_anchor_methods(anchor_fare=4500.0, how_determined="AllocationY_StrategyFare_UpsellN")
            f.constantsutils.HIGHEST_B2B_RBD_VALUE = "Z0"
            f.constantsutils.LOWEST_B2B_RBD_VALUE = "Z9"
            f.constantsutils.LOWEST_B2C_RBD_VALUE = "WA"

        self._run_case(configure, _base_args())

    # ------------------------------------------------------------------
    # Case 7: B2C anchorValue == 'B' happy path.
    # ------------------------------------------------------------------
    def test_case_07_b2c_anchor_value_b(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(4500.0),
                _make_rbddata_df(4500.0),
            )
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                {"RBD": ["Y"], "TOTAL": [4500.0]}
            )
            f.rmdb.getRDBTable.return_value = pd.DataFrame(
                {"B2CValue": ["B"], "B2BValue": ["B"]}
            )
            f.stub_anchor_methods(anchor_fare=4400.0, how_determined="AllocationY_StrategyFare_UpsellN")
            f.constantsutils.HIGHEST_B2B_RBD_VALUE = "Z0"
            f.constantsutils.LOWEST_B2B_RBD_VALUE = "Z9"
            f.constantsutils.LOWEST_B2C_RBD_VALUE = "WA"

        self._run_case(configure, _base_args())

    # ------------------------------------------------------------------
    # Case 8: hardAnchorFare equals anchorFare → hard_anchor_flag latched True.
    # ------------------------------------------------------------------
    def test_case_08_hard_anchor_flag_latched(self):
        def configure(f):
            # First getValueWithOffset call resolves strategyFare; subsequent calls
            # (for hard anchor + final ladder lookup) reuse the same return value.
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(4500.0),
                _make_rbddata_df(4500.0),
            )
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                {"RBD": ["Y"], "TOTAL": [4500.0]}
            )
            f.rmdb.getRDBTable.return_value = pd.DataFrame(
                {"B2CValue": ["A"], "B2BValue": ["A"]}
            )
            # Stub returns anchorFare == hardAnchorFare so the latch fires.
            f.stub_anchor_methods(anchor_fare=4500.0, how_determined="AllocationY_HardAnchor_UpsellN")
            f.constantsutils.HIGHEST_B2B_RBD_VALUE = "Z0"
            f.constantsutils.LOWEST_B2B_RBD_VALUE = "Z9"
            f.constantsutils.LOWEST_B2C_RBD_VALUE = "WA"

        args = _base_args(hardAnchorFare=4500.0, hardoffset=0)
        self._run_case(configure, args)

    # ------------------------------------------------------------------
    # Case 9: w0Flag == 1 → manual inventory retained, anchorFare driven to 0.
    # ------------------------------------------------------------------
    def test_case_09_w0_flag_manual_inventory(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(4500.0),
                _make_rbddata_df(4500.0),
            )
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                {"RBD": ["Y"], "TOTAL": [4500.0]}
            )
            f.rmdb.getRDBTable.return_value = pd.DataFrame(
                {"B2CValue": ["A"], "B2BValue": ["A"]}
            )
            f.stub_anchor_methods()

        self._run_case(configure, _base_args(w0Flag=1))

    # ------------------------------------------------------------------
    # Case 10: directFlag == 0 → manual inventory retained, anchorFare driven to 0.
    # ------------------------------------------------------------------
    def test_case_10_direct_flag_zero_manual_inventory(self):
        def configure(f):
            f.rmdb.getValueWithOffset.return_value = (
                _make_open_result_df(4500.0),
                _make_rbddata_df(4500.0),
            )
            f.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame(
                {"RBD": ["Y"], "TOTAL": [4500.0]}
            )
            f.rmdb.getRDBTable.return_value = pd.DataFrame(
                {"B2CValue": ["A"], "B2BValue": ["A"]}
            )
            f.stub_anchor_methods()

        self._run_case(configure, _base_args(directFlag=0))


if __name__ == "__main__":
    unittest.main()
