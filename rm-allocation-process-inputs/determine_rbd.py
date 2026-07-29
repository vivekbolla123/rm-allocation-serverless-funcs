from datetime import datetime, timedelta
from decimal import Decimal
import json
import pandas as pd
from Constants import *
import random
import statistics
from models.anchor_result import AnchorResult

class DetermineRBD:
    
    def __init__(self, rmdatabasehelper, logger, inputsource, constantsutils, mathutils, redisHelper):
        self.inputSource = inputsource
        self.rmdatabasehelper = rmdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils
        self.redishelper = redisHelper
        self.auto_tolerance_config = self.rmdatabasehelper.getAutoToleranceConfig()

    def _format_thresh(self, val):
        if not val:
            return ""
        v = str(val).replace('%', '')
        try:
            return f"_{float(v):.2f}"
        except ValueError:
            return f"_{v}"

    def determineRBDToOpen(self, anchorFare, offset, channel, currFltDate, dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, backstop, currenttime, startEndOfDayDplfBand, dPLF, toleranceFare,plf,overBookingCount,hardAnchorFare,hardoffset,BookingsInHour,lastSoldFare,last7AvgVal,last7avg,b2b_discount_fare,discount_amount,w0Flag,directFlag,obfare, profileFare, isProfileAnchor, imp_forecast, imp_forecast_band, strategy_criteria, af_forecast_band, Af_Nb_Allin, PLF_Gain, forecast_plf):
        """Backwards-compatible wrapper. Delegates to ``computeFinalAnchor`` then
        ``applyAnchorToLadder`` so the existing call-sites in ``runAlloc.py`` and
        ``runAlloc_Connections.py`` keep producing byte-identical output.
        """
        anchor_result = self.computeFinalAnchor(
            anchorFare, offset, channel, currFltDate, dplfBand, ndocounter,
            currBookedLoad, totalCapacity, currParams, backstop, currenttime,
            startEndOfDayDplfBand, dPLF, toleranceFare, plf, overBookingCount,
            hardAnchorFare, hardoffset, BookingsInHour, lastSoldFare, last7AvgVal,
            last7avg, b2b_discount_fare, discount_amount, w0Flag, directFlag,
            obfare, profileFare, isProfileAnchor, imp_forecast, imp_forecast_band,
            strategy_criteria, af_forecast_band, Af_Nb_Allin, PLF_Gain, forecast_plf,
        )
        return self.applyAnchorToLadder(
            anchor_result, currParams, backstop, offset, strategy_criteria,
        )

    def computeFinalAnchor(self, anchorFare, offset, channel, currFltDate, dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, backstop, currenttime, startEndOfDayDplfBand, dPLF, toleranceFare,plf,overBookingCount,hardAnchorFare,hardoffset,BookingsInHour,lastSoldFare,last7AvgVal,last7avg,b2b_discount_fare,discount_amount,w0Flag,directFlag,obfare, profileFare, isProfileAnchor, imp_forecast, imp_forecast_band, strategy_criteria, af_forecast_band, Af_Nb_Allin, PLF_Gain, forecast_plf):
        """Phase K head: candidate-selection + ``final_fare_anchor_*`` / ``finalFareAnchor`` calls.

        Performs the initial price-ladder lookup that establishes ``strategyFare``, runs
        the forecast / RDB-driven anchor selection, applies the manual-inventory override
        (``w0Flag`` / ``directFlag``), and returns an :class:`AnchorResult` describing
        every value the ladder-lookup tail needs.

        When the anchor-selection block hits one of the four short-circuit returns
        present in today's ``determineRBDToOpen`` (no rbddata, ``strategyFare == NA``,
        ``anchorValue == 'C'`` in the forecast branch, or ``anchorValue == 'C'`` in the
        else branch), the result is flagged ``early_return = True`` and the exact
        9-tuple to return is captured in ``early_return_tuple``. ``applyAnchorToLadder``
        will return that tuple verbatim, preserving byte-for-byte compatibility with
        the legacy method (R2.2, R11).
        """
        hard_anchor_flag = False
        anchorFareBeforeTolerance = CONST_NA
        origin = currParams.origin
        destin = currParams.destin
        forecastAllocFlag = int(getattr(currParams, 'forecastAllocFlag', 0))
        afForecastAllocFlag = int(getattr(currParams, 'afForecastAllocFlag', 0))
        forecast_plf = float(forecast_plf) if forecast_plf!= CONST_NA else CONST_NA
        strategyFare = CONST_NA
        howDetermined = CONST_NA
        upsell = CONST_NA
        overBookingFare = 0
        openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
        hardopenResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
        openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult, 'L', strategy_criteria)
        if self.check_int(hardoffset) and hardAnchorFare!=CONST_NA:
            hardopenResult, hardrbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, hardAnchorFare, hardoffset, channel, hardopenResult, 'L')
            if hardrbddata.shape[0] == 0 and  hardAnchorFare!=CONST_NA:
                hardopenResult = self.extremeFaresSkipping(origin, destin, hardAnchorFare, hardoffset, channel, backstop,  hardopenResult, 'L')
            elif  hardAnchorFare!=CONST_NA:
                hardopenResult = self.getFares(origin, destin, hardAnchorFare, hardoffset, channel,  hardopenResult, hardrbddata, howDetermined, 'L')
            hardAnchorFare=hardopenResult['Fare'][0]

        # Helper: build the early-return tuple in the exact slot order the legacy
        # ``determineRBDToOpen`` returns. The slot layout matches the unpacking at the
        # call sites in ``runAlloc.py`` (openData, lastSellingFare, strategyFare,
        # profileFare, hardAnchorFare, upsell, overBookingFare, hard_anchor_flag,
        # toleranceValue) but reproduces today's quirky values verbatim so callers see
        # byte-identical results (R2.2, R11.1).
        def _build_early_return(open_result_value, current_strategy_fare):
            return (
                open_result_value,
                lastSoldFare,
                current_strategy_fare,
                CONST_NA,
                upsell,
                hardAnchorFare,
                overBookingFare,
                hardAnchorFare,
                CONST_NA,
            )

        if rbddata is None:
            return AnchorResult(
                anchorFare=anchorFare,
                anchorFareBeforeTolerance=anchorFareBeforeTolerance,
                strategyFare=strategyFare,
                profileFare=CONST_NA,
                lastSellingFare=lastSoldFare,
                hardAnchorFare=hardAnchorFare,
                hard_anchor_flag=hard_anchor_flag,
                toleranceValue=CONST_NA,
                anchor_source=howDetermined,
                channel=channel,
                upsell=upsell,
                overBookingFare=overBookingFare,
                openResult=openResult,
                early_return=True,
                early_return_tuple=_build_early_return(openResult, strategyFare),
            )
        if rbddata.shape[0] == 0:
            openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop,  openResult, 'L')
            
        else:
            openResult = self.getFares(origin, destin, anchorFare, offset, channel,  openResult, rbddata, howDetermined, 'L', strategy_criteria)
           
        curTime = datetime.strptime(str(currenttime), TIME_FORMAT).time().strftime(TIME_FORMAT)
        timeString = 'Pre10'
        if curTime > TIME_10_AM:
            timeString = 'Post10'
        
        strategyFare = openResult['Fare'][0]
        if strategyFare == CONST_NA:
            return AnchorResult(
                anchorFare=anchorFare,
                anchorFareBeforeTolerance=anchorFareBeforeTolerance,
                strategyFare=strategyFare,
                profileFare=CONST_NA,
                lastSellingFare=lastSoldFare,
                hardAnchorFare=hardAnchorFare,
                hard_anchor_flag=hard_anchor_flag,
                toleranceValue=CONST_NA,
                anchor_source=howDetermined,
                channel=channel,
                upsell=upsell,
                overBookingFare=overBookingFare,
                openResult=openResult,
                early_return=True,
                early_return_tuple=_build_early_return(openResult, strategyFare),
            )

        anchorValue = ''
    
        toleranceValue = CONST_NA

        if channel == B2B_CHANNEL:
            anchorFare = strategyFare
            howDetermined = "Strategy"
            upsell = CONST_NA
            overBookingFare = 0
            toleranceValue = CONST_NA
        else:
            if (forecastAllocFlag == 1 and imp_forecast_band != CONST_NA and imp_forecast_band != 2) or (afForecastAllocFlag == 1 and imp_forecast_band != CONST_NA and imp_forecast_band != 2 and af_forecast_band != CONST_NA) or (afForecastAllocFlag == 1 and af_forecast_band != CONST_NA and PLF_Gain >= 0 and Af_Nb_Allin != CONST_NA):
                if imp_forecast_band == 2:
                    value = self.rmdatabasehelper.getRDBTable(startEndOfDayDplfBand, dplfBand, curTime, ndocounter)
                    if value.shape[0] != 0:
                        anchorValue = value['B2CValue'][0] if channel == B2C_CHANNEL else value['B2BValue'][0]
                    if anchorValue == 'C':
                        openResult['FareResult'] = [f"V_{imp_forecast_band}_{dplfBand}_C_{howDetermined}"]
                        openResult['anchorFare'] = ['NA']
                        openResult['RBD'] = ['NA']
                        openResult['Fare'] = ['NA']
                        return AnchorResult(
                            anchorFare=anchorFare,
                            anchorFareBeforeTolerance=anchorFareBeforeTolerance,
                            strategyFare=strategyFare,
                            profileFare=CONST_NA,
                            lastSellingFare=lastSoldFare,
                            hardAnchorFare=hardAnchorFare,
                            hard_anchor_flag=hard_anchor_flag,
                            toleranceValue=CONST_NA,
                            anchor_source=howDetermined,
                            channel=channel,
                            upsell=upsell,
                            overBookingFare=overBookingFare,
                            openResult=openResult,
                            early_return=True,
                            early_return_tuple=_build_early_return(openResult, strategyFare),
                        )

                    lastSoldFareForAnchor = lastSoldFare
                    if not (int(currParams.opening_fares) == 1) and not (plf >= self.constantsutils.OVERBOOKING_FARE_LF and overBookingCount > 0):
                        lastSoldFareForAnchor = CONST_NA
 
                    anchorFare, _, _, _, _, _ = self.finalFareAnchor(
                        strategyFare, profileFare, lastSoldFareForAnchor, CONST_NA,
                        anchorValue, dPLF, None, currParams, plf, overBookingCount,
                        dplfBand, BookingsInHour, last7avg, isProfileAnchor,
                        b2b_discount_fare, anchorFare, obfare, ndocounter, imp_forecast
                    )
 
                anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance = self.final_fare_anchor_forecast(
                    strategyFare, profileFare, lastSoldFare, last7AvgVal, hardAnchorFare, currParams,
                    plf, overBookingCount, BookingsInHour, last7avg, isProfileAnchor, b2b_discount_fare,
                    anchorFare, obfare, ndocounter, imp_forecast, imp_forecast_band, af_forecast_band,
                    afForecastAllocFlag, Af_Nb_Allin, PLF_Gain, forecast_plf, toleranceFare
                )
                howDetermined = f"F_{imp_forecast_band}_{dplfBand}_{howDetermined}"
                
            else:
                value = self.rmdatabasehelper.getRDBTable(startEndOfDayDplfBand, dplfBand, curTime, ndocounter)
                if value.shape[0] != 0:
                    if channel == B2B_CHANNEL:
                        anchorValue = value['B2BValue'][0]
                    if channel == B2C_CHANNEL:
                        anchorValue = value['B2CValue'][0]
                if anchorValue == 'C':
                    openResult['FareResult']=[f"V_{imp_forecast_band}_{dplfBand}_C_{howDetermined}"]
                    openResult['anchorFare']=['NA']
                    openResult['RBD']=['NA']
                    openResult['Fare']=['NA']

                    return AnchorResult(
                        anchorFare=anchorFare,
                        anchorFareBeforeTolerance=anchorFareBeforeTolerance,
                        strategyFare=strategyFare,
                        profileFare=CONST_NA,
                        lastSellingFare=lastSoldFare,
                        hardAnchorFare=hardAnchorFare,
                        hard_anchor_flag=hard_anchor_flag,
                        toleranceValue=CONST_NA,
                        anchor_source=howDetermined,
                        channel=channel,
                        upsell=upsell,
                        overBookingFare=overBookingFare,
                        openResult=openResult,
                        early_return=True,
                        early_return_tuple=_build_early_return(openResult, strategyFare),
                    )

                self.logger.info("totalCapacity: " + str(totalCapacity))
                self.logger.info("currBookedLoad: " + str(currBookedLoad))


                if not (int(currParams.opening_fares) == 1) and not (plf >= self.constantsutils.OVERBOOKING_FARE_LF and overBookingCount>0):
                    lastSoldFare = CONST_NA
                        
                self.logger.info("Last Sold Price: " + str(lastSoldFare))
                self.logger.info("Profile Fare: " + str(profileFare))
                self.logger.info("Startegy Fare: " + str(strategyFare))
                
                anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance = self.finalFareAnchor(strategyFare, profileFare, lastSoldFare,hardAnchorFare, anchorValue, dPLF, toleranceFare, currParams,plf,overBookingCount,dplfBand,BookingsInHour,last7avg,isProfileAnchor,b2b_discount_fare,anchorFare, obfare, ndocounter, imp_forecast)
                if currParams.market_discount != CONST_NA and currParams.market_discount is not None and not pd.isna(currParams.market_discount):
                    anchorFare = anchorFare+int(currParams.market_discount)

                
                    
                if anchorValue == 'A':
                    howDetermined = f"V_{imp_forecast_band}_{dplfBand}_A_{howDetermined}"
                elif anchorValue == 'B':
                    howDetermined = f"V_{imp_forecast_band}_{dplfBand}_B_{howDetermined}"
                elif anchorValue == 'C':
                    howDetermined = f"V_{imp_forecast_band}_{dplfBand}_C_{howDetermined}"
        
        if anchorValue!='C' and b2b_discount_fare:
                howDetermined+=f"_Discounted_{discount_amount}"
            
        if int(w0Flag)==1 or int(directFlag)==0:
            upsell = CONST_NA
            howDetermined = howDetermined+'_Manual_inventory_retained_skipping_allocation'
            anchorFare =  0
        if anchorFare == hardAnchorFare:
            hard_anchor_flag = True

        # Anchor selection complete. Bundle the resolved values into an AnchorResult so
        # the ladder-lookup tail can run separately (R2.1).
        if anchorFareBeforeTolerance == CONST_NA:
            anchorFareBeforeTolerance = anchorFare

        return AnchorResult(
            anchorFare=anchorFare,
            anchorFareBeforeTolerance=anchorFareBeforeTolerance,
            strategyFare=strategyFare,
            profileFare=profileFare,
            lastSellingFare=lastSoldFare,
            hardAnchorFare=hardAnchorFare,
            hard_anchor_flag=hard_anchor_flag,
            toleranceValue=toleranceValue,
            anchor_source=howDetermined,
            channel=channel,
            upsell=upsell,
            overBookingFare=overBookingFare,
            openResult=openResult,
            early_return=False,
            early_return_tuple=None,
        )

    def applyAnchorToLadder(self, anchor_result, currParams, backstop, offset=0, strategy_criteria=None):
        """Phase K tail: ``getValueWithOffset`` / ``extremeFaresSkipping`` /
        ``getFaresWithBackstop`` for the resolved anchor fare, then the calculated
        overbooking-fare-and-status block (already applied inside ``computeFinalAnchor``
        through ``final_fare_anchor_forecast`` / ``finalFareAnchor``).

        Returns the same 9-tuple ``determineRBDToOpen`` returns today
        ``(openData, lastSellingFare, strategyFare, profileFare, hardAnchorFare,
          upsell, overBookingFare, hard_anchor_flag, toleranceValue)``.

        When ``anchor_result.early_return`` is True the captured legacy tuple is
        returned verbatim, keeping byte-for-byte parity with the pre-refactor method
        (R2.2, R11.1).
        """
        if anchor_result.early_return:
            return anchor_result.early_return_tuple

        anchorFare = anchor_result.anchorFare
        howDetermined = anchor_result.anchor_source
        channel = anchor_result.channel
        openResult = anchor_result.openResult
        origin = currParams.origin
        destin = currParams.destin

        if anchorFare != 0:
            # Final ladder lookup uses offset=0 in today's code; preserve that exactly.
            offset = 0
            openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult, 'L', strategy_criteria)
            if rbddata.shape[0] == 0:
                openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop,  openResult, 'L')
            else:
                openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop,  openResult, rbddata, howDetermined, 'L')
        else:
            openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': [howDetermined], 'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}

        return (
            openResult,
            anchor_result.lastSellingFare,
            anchor_result.strategyFare,
            anchor_result.profileFare,
            anchor_result.hardAnchorFare,
            anchor_result.upsell,
            anchor_result.overBookingFare,
            anchor_result.hard_anchor_flag,
            anchor_result.toleranceValue,
        )

    def calculate_overbooking_fare_and_status(
        self,
        plf,
        overBookingCount,
        lastSoldFare,
        strategyFare,
        obfare,
        ndocounter,
        last7avg,
        isProfileAnchor,
        b2b_discount_fare,
        anchorFare,
        anchorFareDiscounted,
        upsell,
        howDetermined
    ):
        """
        Applies overbooking logic and returns updated anchorFare, howDetermined, upsell, overBookingFare.
        """
        overBookingFare = 0

        should_apply_system_ob = (
            plf >= self.constantsutils.OVERBOOKING_LF and
            overBookingCount > 0 and
            lastSoldFare != CONST_NA and
            (obfare is None or pd.isna(obfare) or obfare == CONST_NA or
            obfare == '' or pd.isnull(obfare))
        )

        should_apply_manual_ob = (
            plf >= self.constantsutils.OVERBOOKING_LF and
            obfare is not None and pd.isna(obfare) == False and
            obfare != CONST_NA and obfare != '' and
            pd.isnull(obfare) == False and 1 >= ndocounter >= 0
        )

        if should_apply_system_ob:
            self.logger.info("Entered System OB since manual obFare is not present. Setting System Overbooking Fare")
            lastSoldFare = float(lastSoldFare)
            upsell = CONST_NA
            calculatedFare = strategyFare

            if lastSoldFare != CONST_NA and lastSoldFare != 0:
                fare1 = lastSoldFare + self.constantsutils.OVERBOOKING_FARE_PERCENT_VALUE * lastSoldFare
                fare2 = lastSoldFare + self.constantsutils.OVERBOOKING_FARE_ABSOLUTE_VALUE
                calculatedFare = max(fare1, fare2)
                self.logger.info("Calculated fare is: " + str(calculatedFare))

            overBookingFare = (calculatedFare + strategyFare) / 2 if calculatedFare < strategyFare else calculatedFare

        if should_apply_manual_ob:
            self.logger.info("Entered Manual OB since obFare is present")
            overBookingFare = float(obfare)
            upsell = CONST_NA

        # Update howDetermined for overbooking scenarios
        if plf >= self.constantsutils.OVERBOOKING_MANUAL_LF and should_apply_manual_ob:
            howDetermined = "Manual Overbooking Fare"
        elif plf >= self.constantsutils.OVERBOOKING_MANUAL_LF and should_apply_system_ob:
            howDetermined = "System Overbooking Fare"

        # Final adjustments
        if last7avg:
            howDetermined += "_last7bkgsAvg"
        elif isProfileAnchor:
            howDetermined += "_profileFare"

        if b2b_discount_fare:
            anchorFare = anchorFareDiscounted
            upsell = CONST_NA

        return anchorFare, howDetermined, upsell, overBookingFare

    def final_fare_anchor_af_forecast(self, forecastFareAnchor, af_forecast_band, afForecastAllocFlag, Af_Nb_Allin, PLF_Gain, strategyFare, plf, forecast_plf, lastSoldFare=None, profileFare=None, maxLastSoldFare=None, hardAnchorFare=CONST_NA, tolerance_per=None, toleranceValue=None, imp_forecast_band=None, plfThreshold=None):
        """
        Step 2: AF Forecast based interpolation using PLF_Gain as a factor.
        Takes ForecastFareAnchor from Step 1 (excluding tolerance/hardanchor),
        applies interpolation logic, then applies tolerance + hardanchor guardrails.
        Returns (anchorFare, howDetermined).
        """
        strategyFare = float(strategyFare)
        forecast_plf = float(forecast_plf)
        howDetermined = ''

        # Configurable guardrails
        PLF_FORECAST_CAP = self.constantsutils.AF_PLF_FORECAST_CAP
        PLF_GAIN_MAX_ZERO = self.constantsutils.AF_PLF_GAIN_MAX_ZERO
        CONSTANT_BAND_2 = self.constantsutils.AF_CONSTANT_BAND_2
        CONSTANT_BAND_3 = self.constantsutils.AF_CONSTANT_BAND_3
        PROFILE_FARE_PLF_THRESHOLD = self.constantsutils.AF_PROFILE_FARE_PLF_THRESHOLD
        PROFILE_FARE_MULTIPLIER = self.constantsutils.AF_PROFILE_FARE_MULTIPLIER
        BAND_1_MULTIPLIERS = self.constantsutils.AF_BAND_1_MULTIPLIERS
        BAND_2_MULTIPLIERS = self.constantsutils.AF_BAND_2_MULTIPLIERS
        toleranceValue = CONST_NA

        # af_forecast_band: 0 = <85%, 1 = 85-95%, 2 = 95%+

        def value_exist(val):
            return (val is not None and 
                    pd.isna(val) == False and 
                    val != CONST_NA and 
                    val != '')

        def has_hard_anchor():
            return hardAnchorFare != CONST_NA and value_exist(hardAnchorFare)

        def has_tolerance(tol):
            return (tol is not None and 
                    pd.isna(tol) == False and 
                    tol != CONST_NA and 
                    tol != '' and 
                    pd.isnull(tol) == False and
                    tol != -1)

        def reduce_by_tolerance(value, tolerance):
            return float(value) - float(tolerance)

        # Restrict forecast_plf to cap
        forecast_plf = min(forecast_plf, PLF_FORECAST_CAP)

        # --- Variance based logic when PLF_Gain < 0 ---
        if PLF_Gain < 0:
            howDetermined = f'AF_Forecast_PLFGainNeg_LowerBound'
            temp_fareanchor = forecastFareAnchor
            # Apply tolerance
            if has_tolerance(tolerance_per):
                t2 = reduce_by_tolerance(strategyFare, tolerance_per)
                temp_fareanchor = max(temp_fareanchor, t2)
                if temp_fareanchor == t2:
                    howDetermined += '_ToleranceHit'
                    toleranceValue = t2
            # Apply hard anchor cap
            if has_hard_anchor():
                capped = min(temp_fareanchor, float(hardAnchorFare))
                if capped == float(hardAnchorFare):
                    thresh_suffix = self._format_thresh(plfThreshold)
                    howDetermined = f'AF_Forecast_harAnchorfare{thresh_suffix}'
                temp_fareanchor = capped
            return temp_fareanchor, howDetermined, toleranceValue

        # --- AF_NB < 0 but PLF_Gain > 0: use ForecastFareAnchor as-is ---
        if Af_Nb_Allin < 0 and PLF_Gain > 0:
            howDetermined = f'AF_Forecast_AfNbNeg_PLFGainPos'
            temp_fareanchor = forecastFareAnchor
            # Apply tolerance
            if has_tolerance(tolerance_per):
                t2 = reduce_by_tolerance(strategyFare, tolerance_per)
                temp_fareanchor = max(temp_fareanchor, t2)
                if temp_fareanchor == t2:
                    howDetermined += '_ToleranceHit'
                    toleranceValue = t2
            # Apply hard anchor cap
            if has_hard_anchor():
                capped = min(temp_fareanchor, float(hardAnchorFare))
                if capped == float(hardAnchorFare):
                    thresh_suffix = self._format_thresh(plfThreshold)
                    howDetermined = f'AF_Forecast_harAnchorfare{thresh_suffix}'
                temp_fareanchor = capped
            return temp_fareanchor, howDetermined, toleranceValue

        # --- All other cases: Interpolation logic ---
        howDetermined = 'AF_Forecast_Interpolated'

        # X = MIN(ForecastFareAnchor, AF_NB)
        X = min(forecastFareAnchor, Af_Nb_Allin)

        # HighestPotentialFare = MAX(StrategyFare, LastSelling, Profile)
        potential_fares = [strategyFare]
        if value_exist(lastSoldFare):
            potential_fares.append(float(lastSoldFare))
        if value_exist(profileFare):
            potential_fares.append(float(profileFare))
        if value_exist(maxLastSoldFare):
            potential_fares.append(float(maxLastSoldFare))
        highestPotentialFare = max(potential_fares)

        # Fare_1 = MAX(ForecastFareAnchor, AF_NB)
        Fare1 = max(forecastFareAnchor, Af_Nb_Allin)

        # Fare_2 depends on af_forecast_band
        if af_forecast_band <= 1:  # <95%
            Fare2 = min(BAND_1_MULTIPLIERS[1] * strategyFare, BAND_1_MULTIPLIERS[0] * highestPotentialFare)
        else:  # band 3: 95%+
            Fare2 = min(BAND_2_MULTIPLIERS[1] * strategyFare, BAND_2_MULTIPLIERS[0] * highestPotentialFare)

        # Y = MIN(Fare_1, Fare_2)
        Y = min(Fare1, Fare2)
        temp_swap = 0
        if X > Y:
            temp_swap = X
            X = Y
            Y = temp_swap

        # F = Constant / PLF_Gain, capped at 1; if PLF_Gain > 60 then F = 0
        if PLF_Gain > PLF_GAIN_MAX_ZERO:
            F = 0
            howDetermined += '_PLFGainHigh_F0'
        elif PLF_Gain == 0:
            F = 0
            howDetermined += '_PLFGainZero_F0'
        else:
            if af_forecast_band == 0:  # <85%: use lower bound
                F = 0
            elif af_forecast_band == 1:  # 85-95%
                F = CONSTANT_BAND_2 / PLF_Gain
            else:  # band 3: 95%+
                F = CONSTANT_BAND_3 / PLF_Gain
            F = min(F, 1)

        # Temp_Fareanchor calculation based on af_forecast_band
        if af_forecast_band == 0:  # <85%
            temp_fareanchor = X
            howDetermined += '_Band1_LowerBound'
        elif af_forecast_band == 1:  # 85-95%
            temp_fareanchor = X + F * (Y - X)
            howDetermined += '_Band2'
        else:  # band 3: 95%+
            temp_fareanchor_1 = X + F * (Y - X)
            if forecast_plf >= PROFILE_FARE_PLF_THRESHOLD and value_exist(profileFare):
                temp_fareanchor = max(temp_fareanchor_1, PROFILE_FARE_MULTIPLIER * float(profileFare))
                if temp_fareanchor > temp_fareanchor_1:
                    howDetermined += '_Band3_ProfileMax'
                else:
                    howDetermined += '_Band3'
            else:
                temp_fareanchor = temp_fareanchor_1
                howDetermined += '_Band3'

        # Final Fare Anchor with Tolerance Constraint
        # T1 = Temp_Fareanchor, T2 = Strategy - Tolerance
        if has_tolerance(tolerance_per):
            t2 = reduce_by_tolerance(strategyFare, tolerance_per)
            temp_fareanchor = max(temp_fareanchor, t2)
            if temp_fareanchor == t2:
                howDetermined += '_ToleranceHit'
                toleranceValue = t2
        # Hard anchor cap
        if has_hard_anchor():
            capped = min(temp_fareanchor, float(hardAnchorFare))
            if capped == float(hardAnchorFare):
                thresh_suffix = self._format_thresh(plfThreshold)
                howDetermined = f'AF_Forecast_harAnchorfare{thresh_suffix}'
            temp_fareanchor = capped

        return temp_fareanchor, howDetermined, toleranceValue

    def final_fare_anchor_forecast(self, strategyFare, profileFare, lastSoldFare, last7AvgVal,hardAnchorFare, currParams, plf, overBookingCount,BookingsInHour,last7avg,isProfileAnchor,b2b_discount_fare,anchorFareDiscounted,obfare, ndocounter, imp_forecast, imp_forecast_band, af_forecast_band, afForecastAllocFlag, Af_Nb_Allin, PLF_Gain, forecast_plf, toleranceFare=None):
        anchorFare = 0
        strategyFare = float(strategyFare)
        howDetermined = ''
        upsell = 0
        overBookingFare = 0
        toleranceValue = CONST_NA
        anchorFareBeforeTolerance = CONST_NA
        should_use_tolerance = False
        B2B_FACTOR_MAP = self.constantsutils.B2B_FACTOR_MAP
        tolerance_per = toleranceFare
        b2b_factor_value = B2B_FACTOR_MAP[str(imp_forecast_band)]

        def value_exist(*values):
            return all(pd.isna(value) == False and 
                    value != CONST_NA and 
                    value != '' 
                    for value in values)

        def is_greater(val1, *others):
            """Check if val1 is greater than all other values."""
            if len(others) == 0:
                return True
            
            if val1 is None:
                return False
            
            for other in others:
                if not value_exist(other):
                    continue 
                if val1 <= other:
                    return False
            
            return True

        
        # Helper functions to reduce code duplication
        def has_tolerance(tolerance_fare):
            return (tolerance_fare is not None and 
                    pd.isna(tolerance_fare) == False and 
                    tolerance_fare != CONST_NA and 
                    tolerance_fare != '' and 
                    pd.isnull(tolerance_fare) == False and
                    tolerance_fare != -1)
        
        def apply_tolerance_logic(anchor_fare, tolerance_fare, how_determined):
            nonlocal toleranceValue
            nonlocal anchorFareBeforeTolerance
            if anchorFareBeforeTolerance == CONST_NA and anchor_fare is not None:
                anchorFareBeforeTolerance = anchor_fare

            has_tol = has_tolerance(tolerance_fare)
            flag_val = getattr(currParams, 'auto_tolerance_flag', 0)
            
            if has_tol:
                T = float(tolerance_fare)
            else:
                T = 0.10 * float(strategyFare)
                
            if has_tol:
                tolerance_limit = float(strategyFare) - T
            else:
                tolerance_limit = 0

            auto_tolerance_applied = False
            
            if anchor_fare is not None:
                redis_key = f"run_history:{currParams.origin}_{currParams.destin}_{currParams.flight_number}_{currParams.per_start}"
                hash_field = "autotolerance_history"
                
                history = []
                try:
                    if self.redishelper:
                        state_str = self.redishelper.hget(redis_key, hash_field)
                    if state_str:
                        history = __import__('json').loads(state_str)
                except Exception:
                    pass
                    
                if not history:
                    try:
                        mongo_state = self.rmdatabasehelper.getAutoToleranceState(redis_key)
                        if mongo_state and "autotolerance_history" in mongo_state:
                            h_val = mongo_state["autotolerance_history"]
                            if isinstance(h_val, str):
                                try:
                                    h_val = __import__('json').loads(h_val)
                                except Exception:
                                    h_val = []
                            history = h_val
                    except Exception:
                        pass
                
                if len(history) > 0:
                    last_record = history[-1]
                    saved_b2c_tolerance = last_record.get("b2c_tolerance")
                    t_prime = last_record.get("T_prime")
                    t_v_prev = last_record.get("T_V")
                    run_count = last_record.get("run_count", 0)
                else:
                    saved_b2c_tolerance = None
                    t_prime = None
                    t_v_prev = None
                    run_count = 0
                
                current_b2c_tol_str = str(tolerance_fare) if has_tol else ""
                analyst_changed = False
                if saved_b2c_tolerance is not None:
                    try:
                        saved_f = float(saved_b2c_tolerance)
                        curr_f = float(tolerance_fare) if has_tol else -1.0
                        if saved_f != curr_f:
                            t_prime = None
                            t_v_prev = None
                            analyst_changed = True
                            run_count = 0
                            history = []
                    except:
                        if str(saved_b2c_tolerance) != current_b2c_tol_str:
                            t_prime = None
                            t_v_prev = None
                            analyst_changed = True
                            run_count = 0
                            history = []
                    
                run_count += 1
                
                ndo_df = self.auto_tolerance_config.get("ndo_config")
                n_value = 1
                if ndo_df is not None:
                    match = ndo_df[(ndo_df['ndo_min'] <= ndocounter) & (ndo_df['ndo_max'] >= ndocounter)]
                    if not match.empty:
                        n_value = match.iloc[0]['n_value']
                        
                try:
                    N = int(float(n_value))
                    if N == 0: N = 1
                except:
                    N = 1                
                
                try:
                    current_dplf = float(dPLF) if dPLF is not None and str(dPLF).strip() != '' else 0
                except Exception:
                    current_dplf = 0
                
                try:
                    current_imp_forecast = float(imp_forecast) if imp_forecast is not None and imp_forecast != CONST_NA else 0
                except Exception:
                    current_imp_forecast = 0
                    
                is_execution_run = False
                if run_count > 0 and run_count % (N + 1) == 0:
                    is_execution_run = True
                    
                target_run_count = run_count - N
                target_record = None
                for r in history:
                    if r.get("run_count") == target_run_count:
                        target_record = r
                        break
                        
                calc_new_t_prime = False
                prev_t_prime = T
                x = 0
                delta_variance = 0
                
                if t_v_prev is None:
                    if not has_tol:
                        final_T = 0.10 * float(strategyFare)
                    else:
                        final_T = float(T)
                    t_prime = final_T
                else:
                    final_T = float(t_v_prev)
                    t_prime = float(t_prime) if t_prime is not None else float(T)

                if flag_val in (1, '1') and is_execution_run and target_record is not None:
                    forecast_flag = getattr(currParams, 'forecastAllocFlag', 0)
                    is_forecast = forecast_flag in (1, '1', 'Y', 'y') and imp_forecast is not None and imp_forecast != CONST_NA

                    target_t_v = target_record.get("T_V", t_v_prev)
                    x = target_t_v - float(anchor_fare) if target_t_v is not None else 0
                    
                    if x > 0:
                        prev_metric = target_record.get("actual_imp_forecast", 0) if is_forecast else target_record.get("actual_dplf", 0)
                        curr_metric = current_imp_forecast if is_forecast else current_dplf
                        delta_variance = curr_metric - prev_metric
                        
                        delta_df = self.auto_tolerance_config.get("delta_bands")
                        delta_band_id = None
                        if delta_df is not None:
                            match = delta_df[(delta_df['min_value'] <= delta_variance) & (delta_df['max_value'] >= delta_variance)]
                            if not match.empty:
                                delta_band_id = match.iloc[0]['id']
                        
                        grid_df = self.auto_tolerance_config.get("grid_values")
                        g_value = 0
                        
                        target_band_id = str(imp_forecast_band)
                        
                        if is_forecast and ndocounter <= 30:
                            if grid_df is not None and 'forecast_band_name' in grid_df.columns:
                                for index, row in grid_df.drop_duplicates(subset=['band_id']).iterrows():
                                    b_name = str(row['forecast_band_name'])
                                    try:
                                        parts = b_name.replace('(', '').replace(')', '').split(',')
                                        if len(parts) == 2:
                                            min_v, max_v = float(parts[0]), float(parts[1])
                                            if min_v <= float(imp_forecast) <= max_v:
                                                target_band_id = str(row['band_id'])
                                                break
                                    except Exception:
                                        continue
                        
                        if grid_df is not None and delta_band_id is not None:
                            match = grid_df[(grid_df['band_id'].astype(str) == target_band_id) & (grid_df['delta_band_id'] == delta_band_id)]
                            if not match.empty:
                                g_value = match.iloc[0]['g_value']
                                
                        prev_t_prime = target_record.get("T_prime", T)
                        calculated_t_prime = (x * g_value) + prev_t_prime
                        
                        if calculated_t_prime > float(strategyFare):
                            calculated_t_prime = prev_t_prime
                            
                        t_prime = calculated_t_prime
                        final_T = t_prime
                        
                    if 0 <= ndocounter <= 7:
                        try:
                            final_T = (float(n_value) / 100.0) * float(strategyFare)
                            t_prime = final_T
                        except:
                            pass

                history.append({
                    "run_count": int(run_count),
                    "T_prime": float(t_prime),
                    "T_V": float(final_T),
                    "actual_dplf": float(current_dplf),
                    "actual_imp_forecast": float(current_imp_forecast),
                    "b2c_tolerance": current_b2c_tol_str
                })
                
                max_history = max(6, N + 2)
                if len(history) > max_history:
                    history = history[-max_history:]
                
                try:
                    if self.redishelper:
                        self.redishelper.hset(redis_key, hash_field, __import__('json').dumps(history))
                    
                except Exception as e:
                    print(f"REDIS HSET ERROR for {redis_key}: {e}")
                
                try:
                    mongo_doc = {
                        "redis_key": redis_key,
                        "autotolerance_history": history
                    }
                    self.rmdatabasehelper.setAutoToleranceState(redis_key, mongo_doc)
                except Exception:
                    pass
                
                auto_limit = float(strategyFare) - final_T
                toleranceValue = f"{auto_limit:.2f}"
                if flag_val in (1, '1'):
                    auto_tolerance_applied = True
                    tolerance_limit = auto_limit

            if tolerance_limit == 0 and not auto_tolerance_applied:
                return anchor_fare, how_determined

            toleranceValue = f"{tolerance_limit:.2f}"
            anchor_fare = max(tolerance_limit, anchor_fare)
            if (anchor_fare == tolerance_limit):
                how_determined += '_ToleranceHit'
            
            return anchor_fare, how_determined
        
        def has_hard_anchor():
            return hardAnchorFare != CONST_NA
        
        def apply_hard_anchor_cap(fare, how_determined):
            if not has_hard_anchor():
                return fare, how_determined
            
            capped_fare = min(fare, float(hardAnchorFare))
            if capped_fare == float(hardAnchorFare):
                thresh_suffix = self._format_thresh(getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', None)))
                return capped_fare, f"AllocationY_harAnchorfare{thresh_suffix}_UpsellN"
            return capped_fare, how_determined
        
        def upsell_by_per(value, percentage=2.5):
            return value * (1 + percentage / 100)
        

        
        def get_fare_and_type(strategyFare=None, maxLastSoldFare=None, profileFare=None, avgLastSoldStrategy=None, avgStrategyProfile=None, func=max):
            
            fares = {
                "_StrategyFare": strategyFare,
                "_LastSoldFare": maxLastSoldFare,
                "_ProfileFare": profileFare,
                "_AvgLastSoldAndStrategyFare": avgLastSoldStrategy,
                "_AvgStrategyAndProfileFare": avgStrategyProfile,
            }
            
            valid_fares = {k: v for k, v in fares.items() if value_exist(v)}
            
            if not valid_fares:
                return 0, ""
            
            anchorFare = func(valid_fares.values())
            matched = next((name for name, value in valid_fares.items() if value == anchorFare), None)

            # When an aggregation function (mean/median) is used across multiple fares,
            # produce a descriptive label like AVG[LastSold,Strategy] or MEDIAN[LastSold,Strategy,Profile]
            if func in (statistics.mean, statistics.median) and len(valid_fares) > 1:
                func_label = "AVG" if func == statistics.mean else "MEDIAN"
                # Strip leading underscore and trailing "Fare" for brevity
                short_names = [k.lstrip("_").replace("Fare", "") for k in valid_fares.keys()]
                fareType = f"_{func_label}[{','.join(short_names)}]"
            elif matched:
                fareType = matched
            else:
                # Build _AvgFare + all contributing fare names stripped of leading underscore
                parts = "And".join(k.lstrip("_") for k in valid_fares.keys())
                fareType = f"_AvgFare{parts}"
            return anchorFare, fareType
        
        def assign_b2b_factor(currParams, b2b_factor_value):
            try:
                currParams.b2b_factor = float(b2b_factor_value)
            except (ValueError, TypeError):
                pass
        
        def get_imp_fcs_diff(hours=6):
            old_imp_fsc = get_imp_forecast_history(hours)
            try:
                diff = imp_forecast - old_imp_fsc
                return diff
            except (TypeError, ValueError):
                return None

        
        def get_imp_forecast_history(hours):
            try:
                try:
                    origin = str(currParams.origin).strip()
                    destin = str(currParams.destin).strip()
                    flight_number = str(currParams.flight_number).strip()
                    departure_date = datetime.strptime(currParams.per_start, DATE_FORMAT).date()

                except ValueError as e:
                    self.logger.error(f"Error parsing redis key for ImputedForecastPLF: {e}.")
                    return None
                
                key_imp_fcs_plf = f"ImputedForecastPLF:{origin}{destin}_{flight_number}_{departure_date}"
                
                if currParams.cache_client.exists(key_imp_fcs_plf):
                    imp_fcs_plf_list = json.loads(currParams.cache_client.get(key_imp_fcs_plf).decode('utf-8'))
                    # imp_fcs_plf_list = redis_data.get('list', [])
                    
                    if not imp_fcs_plf_list:
                        self.logger.warning(f"Empty ImputedForecastPLF list for key: {key_imp_fcs_plf}")
                        return None
                    
                    current_time = datetime.now().replace(microsecond=0)
                    
                    selected_value = find_plf_value(imp_fcs_plf_list, current_time, hours)
                    
                    if selected_value is not None:
                        # self.logger.info(f"Found ImputedForecastPLF value: {selected_value} for target time: {current_time}")
                        return selected_value
                    else:
                        self.logger.warning(f"No suitable ImputedForecastPLF value found for target time: {current_time}")
                        return None
                else:
                    self.logger.warning(f"ImputedForecastPLF key not found: {key_imp_fcs_plf}")
                    return None

            except Exception as e:  
                self.logger.error(f"Error processing ImputedForecastPLF list from redis: {e}")
                return None
            
        def find_plf_value(sorted_plf_list, current_time, hours):
            if not sorted_plf_list:
                return None

            base_time = current_time - timedelta(hours=hours)
            if base_time.minute >= 30:
                target_hour = (base_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            else:
                target_hour = base_time.replace(minute=0, second=0, microsecond=0)

            hour_to_value = {
                datetime.fromisoformat(r['timestamp']).replace(minute=0, second=0, microsecond=0): r['value']
                for r in sorted_plf_list
            }

            return hour_to_value.get(target_hour)

        if value_exist(lastSoldFare):
            maxLastSoldFare = lastSoldFare
        else:
            maxLastSoldFare = None

        # ------
        if imp_forecast_band == 4:

            howDetermined = 'AllocationY'
            anchorFare, fareType = get_fare_and_type(
                strategyFare=strategyFare, maxLastSoldFare=maxLastSoldFare, profileFare=profileFare
                )
            howDetermined += fareType

            # Check if either lastSoldFare or last7AvgVal is the highest fare
            isLastSoldMax = value_exist(maxLastSoldFare) and is_greater(maxLastSoldFare, strategyFare, profileFare)

            if float(BookingsInHour) > 0 and isLastSoldMax:
                upsell_value = upsell_by_per(anchorFare, 2.5)
                anchorFare = max(upsell_value, anchorFare + 200)
                howDetermined += '_UpsellY'
            else:
                howDetermined += '_UpsellN'
            
            # assign_b2b_factor(currParams, b2b_factor_value)
            should_use_tolerance = True

        
        elif imp_forecast_band == 3:

            howDetermined = 'AllocationY'

            if not value_exist(maxLastSoldFare) and not value_exist(profileFare):
                anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare)
                howDetermined += fareType + '_UpsellN'
            
            elif not value_exist(maxLastSoldFare):
                if value_exist(profileFare) and value_exist(strategyFare):
                    if is_greater(profileFare, strategyFare):
                        anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare, profileFare=profileFare, func=statistics.mean)
                    else:
                        anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare)

                howDetermined += fareType + '_UpsellN'

            elif not value_exist(profileFare):
                anchorFare, fareType = get_fare_and_type(maxLastSoldFare=maxLastSoldFare, strategyFare=strategyFare, func=statistics.mean)
                howDetermined += fareType + '_UpsellN'

            else:
                # all 3 fares exist
                if is_greater(strategyFare, maxLastSoldFare, profileFare):
                    avgLastSoldStrategy, _ = get_fare_and_type(maxLastSoldFare=maxLastSoldFare, strategyFare=strategyFare, func=statistics.mean)
                    anchorFare, fareType = get_fare_and_type(profileFare=profileFare, avgLastSoldStrategy=avgLastSoldStrategy)
                        
                    howDetermined += fareType + '_UpsellN'
                    should_use_tolerance = True

                elif is_greater(profileFare, strategyFare) and is_greater(strategyFare, maxLastSoldFare):
                    anchorFare, fareType = get_fare_and_type(maxLastSoldFare=maxLastSoldFare, strategyFare=strategyFare, func=statistics.mean)
                    howDetermined += fareType + '_UpsellN'
                    should_use_tolerance = True
            
                else:
                    anchorFare, fareType = get_fare_and_type(maxLastSoldFare=maxLastSoldFare)
                    howDetermined += fareType

                    if float(BookingsInHour) > 0:
                        upsell_value = upsell_by_per(anchorFare, 2.5)
                        anchorFare = max(upsell_value, anchorFare + 200)
                        howDetermined += '_UpsellY'
                    else:
                        howDetermined += '_UpsellN'

            # assign_b2b_factor(currParams, b2b_factor_value)
        
        elif imp_forecast_band == 2:
            anchorFare = float(anchorFareDiscounted)
            howDetermined = 'AllocationY_Band2_V'

        elif imp_forecast_band == 1:
            howDetermined = 'AllocationY'
            if not value_exist(maxLastSoldFare):
                # Last Selling NA — apply spec logic
                if is_greater(strategyFare, profileFare):  # Strategy > Profile
                    diff = get_imp_fcs_diff()
                    if value_exist(diff) and diff < -1:  # imp forecast dropping
                        anchorFare, fareType = get_fare_and_type(profileFare=profileFare)
                        howDetermined += fareType + '_UpsellN'
                    else:  # imp forecast increasing or flat
                        anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare, profileFare=profileFare, func=statistics.mean)
                        howDetermined += fareType + '_UpsellN'
                else:  # Strategy < Profile
                    anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare)
                    howDetermined += fareType + '_UpsellN'

            else:
                lowestFare, _ = get_fare_and_type(strategyFare=strategyFare, maxLastSoldFare=maxLastSoldFare, profileFare=profileFare, func=min)
                if lowestFare == profileFare:
                    diff = get_imp_fcs_diff()
                    if value_exist(diff) and diff < -1:
                        anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare, profileFare=profileFare, func=statistics.mean)
                        howDetermined += fareType + '_UpsellN'
                    else:
                        anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare, maxLastSoldFare=maxLastSoldFare,profileFare=profileFare, func=statistics.median)
                        howDetermined += fareType + '_UpsellN'
                else:
                    anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare, maxLastSoldFare=maxLastSoldFare,profileFare=profileFare, func=min)
                    howDetermined += fareType + '_UpsellN'

            # assign_b2b_factor(currParams, b2b_factor_value)
            should_use_tolerance = True

        elif imp_forecast_band == 0:
            howDetermined = 'AllocationY'
            anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare,maxLastSoldFare=maxLastSoldFare, profileFare=profileFare, func=min)
            howDetermined += fareType + '_UpsellN'
            
            # assign_b2b_factor(currParams, b2b_factor_value)
            should_use_tolerance = True

        if afForecastAllocFlag == 1 and Af_Nb_Allin != CONST_NA and Af_Nb_Allin > 0:
            anchorFare, howDetermined, toleranceValue = self.final_fare_anchor_af_forecast(
                anchorFare, af_forecast_band, afForecastAllocFlag, Af_Nb_Allin, PLF_Gain,
                strategyFare, plf, forecast_plf, lastSoldFare, profileFare, maxLastSoldFare,
                hardAnchorFare, tolerance_per, toleranceValue, imp_forecast_band, getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', None))
            )
        else:
            if should_use_tolerance:
                anchorFare, howDetermined = apply_tolerance_logic(anchorFare, tolerance_per, howDetermined)
            
            anchorFare, howDetermined = apply_hard_anchor_cap(anchorFare, howDetermined)
        # ------        # Overbooking logic & Final Adjustments
        anchorFare, howDetermined, upsell, overBookingFare = self.calculate_overbooking_fare_and_status(plf,overBookingCount,lastSoldFare,strategyFare,obfare,ndocounter,last7avg,isProfileAnchor,b2b_discount_fare,anchorFare,anchorFareDiscounted,upsell,howDetermined
        )
        
        if should_use_tolerance and "_Auto_Tolerance_Hit" not in howDetermined:
            howDetermined += "_Auto_Tolerance_Hit"

        return anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance
    
    def finalFareAnchor(self, strategyFare, profileFare, lastSoldFare,hardAnchorFare, value, dPLF, toleranceFare, currParams, plf, overBookingCount,dplfBand,BookingsInHour,last7avg,isProfileAnchor,b2b_discount_fare,anchorFareDiscounted,obfare, ndocounter, imp_forecast=None):
        anchorFare = 0
        strategyFare = float(strategyFare)
        howDetermined = ''
        upsell = 0
        overBookingFare = 0
        toleranceValue = CONST_NA
        auto_tolerance_applied = False
        anchorFareBeforeTolerance = CONST_NA
        
        # Helper functions to reduce code duplication
        def has_tolerance_fare():
            res = (toleranceFare is not None and 
                    pd.isna(toleranceFare) == False and 
                    toleranceFare != CONST_NA and 
                    toleranceFare != '' and 
                    pd.isnull(toleranceFare) == False)
            return res
        
        def has_hard_anchor():
            return hardAnchorFare != CONST_NA
        
        def apply_hard_anchor_cap(fare, decision_text):
            if not has_hard_anchor():
                return fare, decision_text
            
            capped_fare = min(fare, float(hardAnchorFare))
            if capped_fare == float(hardAnchorFare):
                thresh_suffix = self._format_thresh(getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', None)))
                return capped_fare, f"AllocationY_harAnchorfare{thresh_suffix}_UpsellN"
            return capped_fare, decision_text
        
        def calculate_tolerance_value(pre_tolerance_anchor=None):
            nonlocal toleranceValue
            nonlocal auto_tolerance_applied
            nonlocal anchorFareBeforeTolerance
            if anchorFareBeforeTolerance == CONST_NA and pre_tolerance_anchor is not None:
                anchorFareBeforeTolerance = pre_tolerance_anchor

            has_tol = has_tolerance_fare()
            flag_val = getattr(currParams, 'auto_tolerance_flag', 0)
            
            if has_tol:
                T = float(toleranceFare)
            else:
                T = 0.10 * float(strategyFare)
                
            if has_tol:
                tolerance_limit = float(strategyFare) - T
            else:
                tolerance_limit = 0

            # Auto-tolerance logic
            if pre_tolerance_anchor is not None:
                redis_key = f"run_history:{currParams.origin}_{currParams.destin}_{currParams.flight_number}_{currParams.per_start}"
                hash_field = "autotolerance_history"
                
                history = []
                try:
                    if self.redishelper:
                        state_str = self.redishelper.hget(redis_key, hash_field)
                    if state_str:
                        history = __import__('json').loads(state_str)
                except Exception:
                    pass
                    
                if not history:
                    try:
                        mongo_state = self.rmdatabasehelper.getAutoToleranceState(redis_key)
                        if mongo_state and "autotolerance_history" in mongo_state:
                            h_val = mongo_state["autotolerance_history"]
                            if isinstance(h_val, str):
                                try:
                                    h_val = __import__('json').loads(h_val)
                                except Exception:
                                    h_val = []
                            history = h_val
                    except Exception:
                        pass
                
                if len(history) > 0:
                    last_record = history[-1]
                    saved_b2c_tolerance = last_record.get("b2c_tolerance")
                    t_prime = last_record.get("T_prime")
                    t_v_prev = last_record.get("T_V")
                    run_count = last_record.get("run_count", 0)
                else:
                    saved_b2c_tolerance = None
                    t_prime = None
                    t_v_prev = None
                    run_count = 0
                
                current_b2c_tol_str = str(toleranceFare) if has_tol else ""
                analyst_changed = False
                if saved_b2c_tolerance is not None:
                    try:
                        saved_f = float(saved_b2c_tolerance)
                        curr_f = float(toleranceFare) if has_tol else -1.0
                        if saved_f != curr_f:
                            t_prime = None
                            t_v_prev = None
                            analyst_changed = True
                            run_count = 0
                            history = []
                    except:
                        if str(saved_b2c_tolerance) != current_b2c_tol_str:
                            t_prime = None
                            t_v_prev = None
                            analyst_changed = True
                            run_count = 0
                            history = []
                    
                run_count += 1
                
                ndo_df = self.auto_tolerance_config.get("ndo_config")
                n_value = 1
                if ndo_df is not None:
                    match = ndo_df[(ndo_df['ndo_min'] <= ndocounter) & (ndo_df['ndo_max'] >= ndocounter)]
                    if not match.empty:
                        n_value = match.iloc[0]['n_value']
                        
                try:
                    N = int(float(n_value))
                    if N == 0: N = 1
                except:
                    N = 1                
                
                try:
                    current_dplf = float(dPLF) if dPLF is not None and str(dPLF).strip() != '' else 0
                except Exception:
                    current_dplf = 0
                
                try:
                    current_imp_forecast = float(imp_forecast) if imp_forecast is not None and imp_forecast != CONST_NA else 0
                except Exception:
                    current_imp_forecast = 0
                    
                is_execution_run = False
                if run_count > 0 and run_count % (N + 1) == 0:
                    is_execution_run = True
                    
                target_run_count = run_count - N
                target_record = None
                for r in history:
                    if r.get("run_count") == target_run_count:
                        target_record = r
                        break
                        
                calc_new_t_prime = False
                prev_t_prime = T
                x = 0
                delta_variance = 0
                
                if t_v_prev is None:
                    if not has_tol:
                        final_T = 0.10 * float(strategyFare)
                    else:
                        final_T = float(T)
                    t_prime = final_T
                else:
                    final_T = float(t_v_prev)
                    t_prime = float(t_prime) if t_prime is not None else float(T)

                if flag_val in (1, '1') and is_execution_run and target_record is not None:
                    forecast_flag = getattr(currParams, 'forecastAllocFlag', 0)
                    is_forecast = forecast_flag in (1, '1', 'Y', 'y') and imp_forecast is not None and imp_forecast != CONST_NA

                    target_t_v = target_record.get("T_V", t_v_prev)
                    x = target_t_v - float(pre_tolerance_anchor) if target_t_v is not None else 0
                    
                    if x > 0:
                        prev_metric = target_record.get("actual_imp_forecast", 0) if is_forecast else target_record.get("actual_dplf", 0)
                        curr_metric = current_imp_forecast if is_forecast else current_dplf
                        delta_variance = curr_metric - prev_metric
                        
                        delta_df = self.auto_tolerance_config.get("delta_bands")
                        delta_band_id = None
                        if delta_df is not None:
                            match = delta_df[(delta_df['min_value'] <= delta_variance) & (delta_df['max_value'] >= delta_variance)]
                            if not match.empty:
                                delta_band_id = match.iloc[0]['id']
                        
                        grid_df = self.auto_tolerance_config.get("grid_values")
                        g_value = 0
                        
                        target_band_id = str(dplfBand)
                        
                        if is_forecast and ndocounter <= 30:
                            if grid_df is not None and 'forecast_band_name' in grid_df.columns:
                                for index, row in grid_df.drop_duplicates(subset=['band_id']).iterrows():
                                    b_name = str(row['forecast_band_name'])
                                    try:
                                        parts = b_name.replace('(', '').replace(')', '').split(',')
                                        if len(parts) == 2:
                                            min_v, max_v = float(parts[0]), float(parts[1])
                                            if min_v <= float(imp_forecast) <= max_v:
                                                target_band_id = str(row['band_id'])
                                                break
                                    except Exception:
                                        continue
                        
                        if grid_df is not None and delta_band_id is not None:
                            match = grid_df[(grid_df['band_id'].astype(str) == target_band_id) & (grid_df['delta_band_id'] == delta_band_id)]
                            if not match.empty:
                                g_value = match.iloc[0]['g_value']
                                
                        prev_t_prime = target_record.get("T_prime", T)
                        calculated_t_prime = (x * g_value) + prev_t_prime
                        
                        if calculated_t_prime > float(strategyFare):
                            calculated_t_prime = prev_t_prime
                            
                        t_prime = calculated_t_prime
                        final_T = t_prime
                        
                    if 0 <= ndocounter <= 7:
                        try:
                            final_T = (float(n_value) / 100.0) * float(strategyFare)
                            t_prime = final_T
                        except:
                            pass

                history.append({
                    "run_count": int(run_count),
                    "T_prime": float(t_prime),
                    "T_V": float(final_T),
                    "actual_dplf": float(current_dplf),
                    "actual_imp_forecast": float(current_imp_forecast),
                    "b2c_tolerance": current_b2c_tol_str
                })
                
                max_history = max(6, N + 2)
                if len(history) > max_history:
                    history = history[-max_history:]
                
                try:
                    if self.redishelper:
                        self.redishelper.hset(redis_key, hash_field, __import__('json').dumps(history))
                except Exception:
                    pass
                
                try:
                    mongo_doc = {
                        "redis_key": redis_key,
                        "autotolerance_history": history
                    }
                    self.rmdatabasehelper.setAutoToleranceState(redis_key, mongo_doc)
                except Exception:
                    pass
                
                auto_limit = float(strategyFare) - final_T
                toleranceValue = f"{auto_limit:.2f}"
                if flag_val in (1, '1'):
                    auto_tolerance_applied = True
                    tolerance_limit = auto_limit

            if tolerance_limit == 0 and not auto_tolerance_applied:
                return 0

            toleranceValue = f"{tolerance_limit:.2f}"
            return tolerance_limit
        
        def build_fares_list():
            fares = []
            if lastSoldFare != CONST_NA:
                fares.append(float(lastSoldFare))
            if profileFare != CONST_NA and int(currParams.profileFares) == 1:
                fares.append(float(profileFare))
            return fares
        
        def handle_upsell_logic(base_fare, max_fare, bookings_in_hour):
            upsell_text = "UpsellN"
            calculated_fare = base_fare
            
            if float(max_fare) > float(strategyFare) and float(bookings_in_hour) > 0:
                upsell_val = float(base_fare) * 0.05
                calculated_fare = float(max_fare) + upsell_val
                upsell_text = f"UpsellY_{upsell_val:.2f}"
            elif float(max_fare) > float(strategyFare):
                calculated_fare = float(max_fare)
            else:
                calculated_fare = (float(max_fare) + float(strategyFare)) / 2
                upsell_text = "AVG[MaxFare,Strategy]_UpsellN"
                
            return calculated_fare, upsell_text
        
        def process_dplf_bands_3(last_sold, strategy_or_profile, max_desc):
            max_val = max(float(strategyFare), float(strategy_or_profile))
            v2 = (float(last_sold) + max_val) / 2
            # Determine which fares contributed to the average
            if max_val == float(strategyFare):
                avg_label = "AVG[LastSold,Strategy]"
            else:
                avg_label = "AVG[LastSold,Profile]"
            
            v1 = calculate_tolerance_value(v2)
            if v1 > v2:
                decision = f"ToleranceHit_max_{max_desc}"
                final_fare = v1
            else:
                decision = f"{avg_label}_max_{max_desc}"
                final_fare = v2
            if has_hard_anchor() and float(hardAnchorFare) < final_fare:
                final_fare = float(hardAnchorFare)
                decision = f"hardanchorfare_{getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', ''))}"
            
            return final_fare, f'AllocationY_{decision}_UpsellN'
        
        def process_dplf_bands_4_5_6(fares_list, max_desc):
            if not fares_list:
                return 0, "UpsellN"
                
            max_value = max(fares_list)
            v1, upsell_text = handle_upsell_logic(upsell, max_value, BookingsInHour)
            
            t1 = calculate_tolerance_value(v1)
            if t1 > v1:
                decision = f"ToleranceHit_max_{max_desc}"
                final_fare = t1
            else:
                decision = f"max_{max_desc}_{upsell_text}"
                final_fare = v1
            
            if has_hard_anchor() and float(hardAnchorFare) < final_fare:
                final_fare = float(hardAnchorFare)
                decision = f"hardanchorfare_{getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', ''))}"
            
            return final_fare, f'AllocationY_{decision}_{upsell_text}'

        # Main logic for value 'A'
        if value == 'A':
            faresList = build_fares_list()
            
            # Case: lastSoldFare exists
            if lastSoldFare != CONST_NA:
                last_sold_val = float(lastSoldFare)
                
                # With profileFare
                if profileFare != CONST_NA and int(currParams.profileFares) == 1:
                    profile_val = float(profileFare)
                    
                    # lastSoldFare < profileFare or lastSoldFare < strategyFare
                    if last_sold_val < profile_val or last_sold_val < float(strategyFare):
                        if dplfBand in [3]:
                            anchorFare, howDetermined = process_dplf_bands_3(lastSoldFare, profileFare, "strategy_and_profile")
                        elif dplfBand in [4, 5, 6]:
                            anchorFare, howDetermined = process_dplf_bands_4_5_6(faresList, "last_sold_and_profile")
                        
                        anchorFare, howDetermined = apply_hard_anchor_cap(anchorFare, howDetermined)
                    
                    # lastSoldFare >= profileFare and lastSoldFare >= strategyFare
                    elif last_sold_val >= profile_val and last_sold_val >= float(strategyFare):
                        if float(BookingsInHour) > 0:
                            howDetermined = 'AllocationY_last_sold_fare_UpsellY'
                            upsell = last_sold_val * 0.05
                            anchorFare = last_sold_val + upsell
                        else:
                            howDetermined = 'AllocationY_last_sold_fare_UpsellN'
                            anchorFare = last_sold_val
                        
                        anchorFare, howDetermined = apply_hard_anchor_cap(anchorFare, howDetermined)
                
                # Without profileFare
                else:
                    # lastSoldFare <= strategyFare
                    if last_sold_val <= float(strategyFare):
                        if dplfBand in [3]:
                            anchorFare, howDetermined = process_dplf_bands_3(lastSoldFare, strategyFare, "strategy_and_profile")
                        elif dplfBand in [4, 5, 6]:
                            anchorFare, howDetermined = process_dplf_bands_4_5_6(faresList, "last_sold_and_profile")
                    # lastSoldFare > strategyFare
                    else:
                        if float(BookingsInHour) > 0:
                            howDetermined = 'AllocationY_last_sold_fare_UpsellY'
                            upsell = last_sold_val * 0.05
                            anchorFare = last_sold_val + upsell
                        else:
                            howDetermined = 'AllocationY_last_sold_fare_UpsellN'
                            anchorFare = last_sold_val
                    
                    anchorFare, howDetermined = apply_hard_anchor_cap(anchorFare, howDetermined)
            
            # Case: No lastSoldFare but profileFare exists
            elif profileFare != CONST_NA and int(currParams.profileFares) == 1:
                max_fare = max(float(profileFare), float(strategyFare))
                anchorFareBeforeTolerance = max_fare
                
                tolerance_val = calculate_tolerance_value(max_fare)
                anchorFare = max(max_fare, tolerance_val)
                howDetermined = 'AllocationY_Maxof_Startegy_Profile_UpsellN' if anchorFare == max_fare else 'AllocationY_ToleranceHit_UpsellN'
                
                anchorFare, howDetermined = apply_hard_anchor_cap(anchorFare, howDetermined)
            
            # Case: Neither lastSoldFare nor profileFare
            else:
                anchorFare = float(strategyFare)
                howDetermined = 'AllocationY_Startegy_fare_UpsellN'
                anchorFare, howDetermined = apply_hard_anchor_cap(anchorFare, howDetermined)

        # Main logic for value 'B'
        elif value == 'B':
            upsell = CONST_NA
            faresList = []
            
            # Build filtered fares list for value B
            if lastSoldFare != CONST_NA and float(lastSoldFare) <= float(strategyFare):
                faresList.append(float(lastSoldFare))
            if profileFare != CONST_NA and float(profileFare) <= float(strategyFare) and int(currParams.profileFares) == 1:
                faresList.append(float(profileFare))
            
            max_value = max(faresList) if faresList else 0
            
            # No valid fares found
            if max_value == 0:
                anchorFare = float(strategyFare)
                decision = "StrategyFare"
                if has_hard_anchor():
                    anchorFare = min(float(strategyFare), float(hardAnchorFare))
                    if anchorFare == float(hardAnchorFare):
                        decision = f"hardanchorfare_{getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', ''))}"
                howDetermined = f"AllocationY_{decision}_UpsellN"
            
            # Max value equals strategy fare
            elif max_value == float(strategyFare):
                anchorFare = float(strategyFare)
                howDetermined = "AllocationY_Startegy_fare_UpsellN"
                if has_hard_anchor():
                    decision = "strategyFare"
                    anchorFare = min(float(strategyFare), float(hardAnchorFare))
                    if anchorFare == float(hardAnchorFare):
                        decision = f"hardanchorfare_{getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', ''))}"
                    howDetermined = f"AllocationY_min_{decision}_UpsellN"
            
            # Max value less than strategy fare
            elif max_value < float(strategyFare):
                delta = float(strategyFare) - float(max_value)
                howDetermined = 'AllocationY_Interpollated_Max_own_fare_UpsellN'
                
                # Calculate v1 based on dPLF
                if dPLF <= -15:
                    dPLF = -15
                if dPLF > -self.constantsutils.OWN_FARE_DELTA_B:
                    v1 = max_value + (delta / self.constantsutils.OWN_FARE_DELTA_B) * (dPLF + self.constantsutils.OWN_FARE_DELTA_B)
                else:
                    v1 = max_value
                
                anchorFareBeforeTolerance = v1
                v2 = calculate_tolerance_value(v1)
                
                if has_hard_anchor():
                    anchorFare = min(max(v1, v2), float(hardAnchorFare))
                    if v2 > v1:
                        howDetermined += '_ToleranceHit'
                    if anchorFare == float(hardAnchorFare):
                        thresh_suffix = self._format_thresh(getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', None)))
                        howDetermined = f"AllocationY_harAnchorfare{thresh_suffix}_UpsellN"
                else:
                    anchorFare = max(v1, v2)
                    if v2 > v1:
                        howDetermined += '_ToleranceHit'
            
            # Special case: both profileFare and lastSoldFare exist and are less than strategyFare
            if (profileFare != CONST_NA and lastSoldFare != CONST_NA and 
                float(strategyFare) > float(profileFare) and float(strategyFare) > float(lastSoldFare)):
                
                howDetermined = "AllocationY_interpolated_lastSold_Profile_fare_UpsellN"
                min_fare = min(float(lastSoldFare), float(profileFare))
                fare_diff = abs(float(lastSoldFare) - float(profileFare))
                if dPLF < -15:
                    dPLF = -15
                v1 = min_fare + (fare_diff / self.constantsutils.OWN_FARE_DELTA_B) * (dPLF + self.constantsutils.OWN_FARE_DELTA_B)
                anchorFareBeforeTolerance = v1
                v2 = calculate_tolerance_value(v1)
                
                if v2 > v1:
                    howDetermined += '_ToleranceHit'
                
                if has_hard_anchor():
                    anchorFare = min(max(v1, v2), float(hardAnchorFare))
                    if anchorFare == float(hardAnchorFare):
                        thresh_suffix = self._format_thresh(getattr(currParams, 'plfThreshold', getattr(currParams, 'plfthreshold', None)))
                        howDetermined = f"AllocationY_harAnchorfare{thresh_suffix}_UpsellN"
                else:
                    anchorFare = max(v1, v2)

        # Main logic for value 'C'
        elif value == 'C':
            upsell = CONST_NA
            howDetermined = 'AllocationN'
            anchorFare = 0
        
        # Overbooking logic & Final Adjustments
        anchorFare, howDetermined, upsell, overBookingFare = self.calculate_overbooking_fare_and_status(plf,overBookingCount,lastSoldFare,strategyFare,obfare,ndocounter,last7avg,isProfileAnchor,b2b_discount_fare,anchorFare,anchorFareDiscounted,upsell,howDetermined
        )
        
        if auto_tolerance_applied and "_Auto_Tolerance_Hit" not in howDetermined:
            howDetermined += "_Auto_Tolerance_Hit"
            
        return anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance

    def extremeFaresSkipping(self, origin, destin, anchorFare, offset, channel, backstop, openResult, route):
        self.logger.info("Market Fare is out of price ladder range - allocating at extreme values")
        # Fetch the Lowest or highest available fares
        condition = 'LOW'
        if int(offset) >= 0:
            condition = 'HI'

        defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, condition, backstop, route)

        # Edge case where backstop is placed at a very large value - allocates at Z0 or Y levels
        if defFareData.shape[0] == 0:
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0, route) 

        if defFareData.shape[0] != 0:
            if channel == B2C_CHANNEL:
                openResult = {'RBD': [defFareData['RBD'][0]], 'Fare': [defFareData['TOTAL'][0]], 'FareResult': ['Unable to Match Mkt - B2C allocation at extreme value'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [1]}
                return openResult

            if channel == B2B_CHANNEL:
                if defFareData['RBD'][0] == self.constantsutils.HIGHEST_B2B_RBD_VALUE:
                    openResult = {'RBD':  [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['Market Fare higher than ' + self.constantsutils.HIGHEST_B2B_RBD_VALUE + ' levels - skipping B2B allocation'], 'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}
                    return openResult
                else:
                    openResult = {'RBD': [defFareData['RBD'][0]], 'Fare': [defFareData['TOTAL'][0]], 'FareResult': ['Unable to Match Mkt - B2B allocation at extreme value'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [1]}
                    return openResult
        else:
            openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['Unable to fetch extreme published Fares'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [0]}
            
            return openResult
        
    def getFaresWithBackstop(self, origin, destin, anchorFare, offset, channel, backstop, openResult, rbddata, howDetermined, route):
        backstopFlag = False
        backstopReason = ""
        extremeFareFlag = False
        # We got back data purely on the basis of anchor fare - comparison with offset remains which is done in the subsequent section

        # Calculating the offset that is required based on entries remaining in the RBD table
        offsetToFetch = min(abs(int(offset)), rbddata.shape[0] - 1)
        if int(offset) < 0:
            offsetToFetch = min(abs(int(offset)+1), rbddata.shape[0] - 1) 

        # Comparison with the backstop table - to ensure we are picking a value higher than the backstop
        defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop, route)

        if defFareData.shape[0] != 0:
            if rbddata['TOTAL'][0] < defFareData['TOTAL'][0] and backstop > 0:
                backstopFlag = True
                backstopReason = f"strategy fare {rbddata['TOTAL'][0]} is below backstop {defFareData['TOTAL'][0]}"
            
        # Edge case if the backstop values are placed very high - in which case it will place the backstop at Z0 and Y levels 
        if defFareData.shape[0] == 0:
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0, route)
            
        if rbddata['RBD'][0] == self.constantsutils.LOWEST_B2B_RBD_VALUE or rbddata['RBD'][0] == self.constantsutils.LOWEST_B2C_RBD_VALUE:
            if len(defFareData) == 0:
                extremeFareFlag = True
                RBDtoOpen = rbddata['RBD'][0]
                FareToOpen = rbddata['TOTAL'][0]
            else:
                if rbddata['RBD'][0] == defFareData['RBD'][0]:
                    extremeFareFlag = True
                    RBDtoOpen = rbddata['RBD'][0]
                    FareToOpen = rbddata['TOTAL'][0]
                else:
                    backstopFlag = True
                    backstopReason = f"strategy fare {rbddata['TOTAL'][0]} is below forced highest backstop {defFareData['TOTAL'][0]}"
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['TOTAL'][0]
        else:
            if defFareData.shape[0] != 0:
                if float(defFareData['TOTAL'][0]) >= float(rbddata['TOTAL'][offsetToFetch]):
                    offsetToFetch = min(len(defFareData['TOTAL'])-1, offsetToFetch)
                    if rbddata['TOTAL'][0] < defFareData['TOTAL'][offsetToFetch] and backstop > 0:
                        backstopFlag = True
                        backstopReason = f"strategy fare {rbddata['TOTAL'][offsetToFetch]} is below backstop {defFareData['TOTAL'][0]}"
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['TOTAL'][0]
                else:
                    RBDtoOpen = rbddata['RBD'][offsetToFetch]
                    FareToOpen = rbddata['TOTAL'][offsetToFetch]
                    
            else:
                RBDtoOpen = rbddata['RBD'][offsetToFetch]
                FareToOpen = rbddata['TOTAL'][offsetToFetch]
                    
        self.logger.info('Determining ' + channel + ' Fare ' + str(FareToOpen) + " " + str(RBDtoOpen))

        if channel == B2B_CHANNEL:
            # Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
            limitB2BFare_Data = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0, route)
            limitB2BFare = 0
            if defFareData.shape[0] != 0:
                limitB2BFare = float(limitB2BFare_Data['TOTAL'][0])

            if float(anchorFare) > limitB2BFare:
                # Outside B2B Levels - stick with B2C
                openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['Market Fare higher than ' + self.constantsutils.HIGHEST_B2B_RBD_VALUE + ' levels - skipping B2B allocation'], 'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}
                return openResult

            if offsetToFetch == rbddata.shape[0]:
                openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Not Enough Levels available - taking the Max/Min Available'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [1]}
                return openResult
            
            if extremeFareFlag:
                openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Unable to Match Mkt - B2B allocation at extreme value'], 'anchorFare': [anchorFare], 'statusInd': [0], 'allocationStatus': [1]}
                return openResult

        else:
            if offsetToFetch == rbddata.shape[0]:
                openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Not Enough Levels available - taking the Max/Min Available'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [1]}
                return openResult
            if extremeFareFlag:
                openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Unable to Match Mkt - B2C allocation at extreme value'], 'anchorFare': [anchorFare], 'statusInd': [0], 'allocationStatus': [1]}
                return openResult
            
        if backstopFlag:
            howDetermined += f" - Backstop Hit ({backstopReason})"
            openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': [howDetermined], 'anchorFare': [anchorFare], 'statusInd': [0], 'allocationStatus': [1]}
            return openResult
        
        openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': [howDetermined], 'anchorFare': [anchorFare], 'statusInd': [0], 'allocationStatus': [1]}
        return openResult
    
    def check_int(self,value):
        try:
            int(value)
            return True
        except:
            return False
        
    def getFares(self, origin, destin, anchorFare, offset, channel, openResult, rbddata, howDetermined, route, strategy_criteria=None):
        extremeFareFlag = False
        # We got back data purely on the basis of anchor fare - comparison with offset remains which is done in the subsequent section

        # Calculating the offset that is required based on entries remaining in the RBD table
        offsetToFetch = min(abs(int(offset)), rbddata.shape[0] - 1)
        if int(offset) < 0 and not (strategy_criteria and (strategy_criteria == MINC_FARE_ANCHOR or strategy_criteria == MINOC_FARE_ANCHOR)):
            offsetToFetch = min(abs(int(offset)+1), rbddata.shape[0] - 1) 

        # Comparison with the backstop table - to ensure we are picking a value higher than the backstop
        defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'LOW', 0, route)
            
        # Edge case if the backstop values are placed very high - in which case it will place the backstop at Z0 and Y levels 
        if defFareData.shape[0] == 0:
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0, route)
            
        if rbddata['RBD'][0] == self.constantsutils.LOWEST_B2B_RBD_VALUE or rbddata['RBD'][0] == self.constantsutils.LOWEST_B2C_RBD_VALUE:
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'LOW', 0, route)
            if len(defFareData) == 0:
                extremeFareFlag = True
                RBDtoOpen = rbddata['RBD'][0]
                FareToOpen = rbddata['TOTAL'][0]
            else:
                if rbddata['RBD'][0] == defFareData['RBD'][0]:
                    extremeFareFlag = True
                    RBDtoOpen = rbddata['RBD'][0]
                    FareToOpen = rbddata['TOTAL'][0]
                else:
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['TOTAL'][0]
        else:
            if defFareData.shape[0] != 0:
                if float(defFareData['TOTAL'][0]) >= float(rbddata['TOTAL'][offsetToFetch]):
                    offsetToFetch = min(len(defFareData['TOTAL'])-1, offsetToFetch)
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['TOTAL'][0]
                else:
                    RBDtoOpen = rbddata['RBD'][offsetToFetch]
                    FareToOpen = rbddata['TOTAL'][offsetToFetch]

            else:
                RBDtoOpen = rbddata['RBD'][offsetToFetch]
                FareToOpen = rbddata['TOTAL'][offsetToFetch]
                    
        self.logger.info('Determining ' + channel + ' Fare ' + str(FareToOpen) + " " + str(RBDtoOpen))

        if channel == B2B_CHANNEL:
            # Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
            limitB2BFare_Data = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0, route)
            limitB2BFare = 0
            if defFareData.shape[0] != 0:
                limitB2BFare = float(limitB2BFare_Data['TOTAL'][0])

            if float(anchorFare) > limitB2BFare:
                # Outside B2B Levels - stick with B2C
                openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['Market Fare higher than ' + self.constantsutils.HIGHEST_B2B_RBD_VALUE + ' levels - skipping B2B allocation'], 'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}
                return openResult

            if offsetToFetch == rbddata.shape[0]:
                openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Not Enough Levels available - taking the Max/Min Available'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [1]}
                return openResult
            
            if extremeFareFlag:
                openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Unable to Match Mkt - B2B allocation at extreme value'], 'anchorFare': [anchorFare], 'statusInd': [0], 'allocationStatus': [1]}
                return openResult

        else:
            if offsetToFetch == rbddata.shape[0]:
                openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Not Enough Levels available - taking the Max/Min Available'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [1]}
                return openResult
            if extremeFareFlag:
                openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Unable to Match Mkt - B2C allocation at extreme value'], 'anchorFare': [anchorFare], 'statusInd': [0], 'allocationStatus': [1]}
                return openResult
        
        openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': [howDetermined], 'anchorFare': [anchorFare], 'statusInd': [0], 'allocationStatus': [1]}
        return openResult
