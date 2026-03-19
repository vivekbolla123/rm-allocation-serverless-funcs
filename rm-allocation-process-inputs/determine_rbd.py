from datetime import datetime, timedelta
from decimal import Decimal
import json
import pandas as pd
from Constants import *
import random
import statistics

class DetermineRBD:
    
    def __init__(self, rmdatabasehelper, logger, inputsource, constantsutils, mathutils, redisHelper):
        self.inputSource = inputsource
        self.rmdatabasehelper = rmdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils
        self.redishelper = redisHelper

    def determineRBDToOpen(self, anchorFare, offset, channel, currFltDate, dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, backstop, currenttime, startEndOfDayDplfBand, dPLF, toleranceFare,plf,overBookingCount,hardAnchorFare,hardoffset,BookingsInHour,lastSoldFare,last7AvgVal,last7avg,b2b_discount_fare,discount_amount,w0Flag,directFlag,obfare, profileFare, isProfileAnchor, imp_forecast, imp_forecast_band, strategy_criteria, af_forecast_band, afForecastAllocFlag, Af_Nb_Allin, PLF_Gain, forecast_plf):
        hard_anchor_flag = False
        origin = currParams.origin
        destin = currParams.destin
        forecastAllocFlag = int(getattr(currParams, 'forecastAllocFlag', 0))
        afForecastAllocFlag = int(getattr(currParams, 'afForecastAllocFlag', 0))
        forecast_plf = float(forecast_plf)
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
            
        if rbddata is None:
            return openResult, lastSoldFare, strategyFare, CONST_NA, upsell,hardAnchorFare, overBookingFare,hardAnchorFare
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
            return openResult, lastSoldFare, strategyFare, CONST_NA, upsell,hardAnchorFare, overBookingFare,hardAnchorFare
        
        anchorValue = ''
        if (forecastAllocFlag == 1 and imp_forecast_band != CONST_NA and imp_forecast_band != 2) or (afForecastAllocFlag == 1 and af_forecast_band != CONST_NA and PLF_Gain >= 0):
            anchorFare, howDetermined = self.final_fare_anchor_forecast(strategyFare, profileFare, lastSoldFare,last7AvgVal,hardAnchorFare, currParams,plf,overBookingCount,BookingsInHour,last7avg,isProfileAnchor,b2b_discount_fare,anchorFare, obfare, ndocounter, imp_forecast, imp_forecast_band, af_forecast_band, afForecastAllocFlag, Af_Nb_Allin, PLF_Gain, forecast_plf)
            howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_' + howDetermined
            
        else:
            value = self.rmdatabasehelper.getRDBTable(startEndOfDayDplfBand, dplfBand, curTime, ndocounter)
            if value.shape[0] != 0:
                if channel == B2B_CHANNEL:
                    anchorValue = value['B2BValue'][0]
                if channel == B2C_CHANNEL:
                    anchorValue = value['B2CValue'][0]
            if anchorValue == 'C':
                openResult['FareResult']=[timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_C_' + howDetermined]
                openResult['anchorFare']=['NA']
                openResult['RBD']=['NA']
                openResult['Fare']=['NA']
                
                return openResult, lastSoldFare, strategyFare, CONST_NA, upsell,hardAnchorFare, overBookingFare,hardAnchorFare

            self.logger.info("totalCapacity: " + str(totalCapacity))
            self.logger.info("currBookedLoad: " + str(currBookedLoad))


            if not (int(currParams.opening_fares) == 1) and not (plf >= self.constantsutils.OVERBOOKING_FARE_LF and overBookingCount>0):
                lastSoldFare = CONST_NA
                    
            # value = self.rmdatabasehelper.getRDBTable(startEndOfDayDplfBand, dplfBand, curTime, ndocounter)
            # if value.shape[0] != 0:
            #     if channel == B2B_CHANNEL:
            #         anchorValue = value['B2BValue'][0]
            #     if channel == B2C_CHANNEL:
            #         anchorValue = value['B2CValue'][0]

            self.logger.info("Last Sold Price: " + str(lastSoldFare))
            self.logger.info("Profile Fare: " + str(profileFare))
            self.logger.info("Startegy Fare: " + str(strategyFare))
            
            anchorFare, howDetermined, upsell, overBookingFare = self.finalFareAnchor(strategyFare, profileFare, lastSoldFare,hardAnchorFare, anchorValue, dPLF, toleranceFare, currParams,plf,overBookingCount,dplfBand,BookingsInHour,last7avg,isProfileAnchor,b2b_discount_fare,anchorFare, obfare, ndocounter)
            if currParams.market_discount != CONST_NA and currParams.market_discount is not None and not pd.isna(currParams.market_discount):
                anchorFare = anchorFare+int(currParams.market_discount)

            
                
            if anchorValue == 'A':
                howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_A_' + howDetermined
            elif anchorValue == 'B':
                howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_B_' + howDetermined
            elif anchorValue == 'C':
                howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_C_' + howDetermined
        
        if anchorValue!='C' and b2b_discount_fare:
                howDetermined+=f"_Discounted_{discount_amount}"
            
        if int(w0Flag)==1 or int(directFlag)==0:
            upsell = CONST_NA
            howDetermined = howDetermined+'_Manual_inventory_retained_skipping_allocation'
            anchorFare =  0
        if anchorFare == hardAnchorFare:
            hard_anchor_flag = True
        if anchorFare != 0:
            offset = 0
            openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult, 'L', strategy_criteria)
            if rbddata.shape[0] == 0:
                openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop,  openResult, 'L')
            else:
                openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop,  openResult, rbddata, howDetermined, 'L')
        else:
            openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': [howDetermined], 'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}
        return openResult, lastSoldFare, strategyFare, profileFare,hardAnchorFare, upsell, overBookingFare,hard_anchor_flag

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

    def final_fare_anchor_af_forecast(self, forecastFareAnchor, af_forecast_band, afForecastAllocFlag, Af_Nb_Allin, PLF_Gain, strategyFare, plf, forecast_plf, lastSoldFare=None, profileFare=None, maxLastSoldFare=None, hardAnchorFare=CONST_NA, tolerance_per=None, imp_forecast_band=None):
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

        def reduce_by_per(value, percentage):
            return value * (1 - percentage / 100)

        # Restrict forecast_plf to cap
        forecast_plf = min(forecast_plf, PLF_FORECAST_CAP)

        # --- Variance based logic when PLF_Gain < 0 ---
        if PLF_Gain < 0:
            howDetermined = f'AF_Forecast_PLFGainNeg_LowerBound'
            temp_fareanchor = forecastFareAnchor
            # Apply tolerance
            if has_tolerance(tolerance_per):
                t2 = reduce_by_per(strategyFare, float(tolerance_per))
                temp_fareanchor = max(temp_fareanchor, t2)
                if temp_fareanchor == t2:
                    howDetermined += '_ToleranceHit'
            # Apply hard anchor cap
            if has_hard_anchor():
                capped = min(temp_fareanchor, float(hardAnchorFare))
                if capped == float(hardAnchorFare):
                    howDetermined = f'AF_Forecast_harAnchorfare'
                temp_fareanchor = capped
            return temp_fareanchor, howDetermined

        # --- AF_NB < 0 but PLF_Gain > 0: use ForecastFareAnchor as-is ---
        if Af_Nb_Allin < 0 and PLF_Gain > 0:
            howDetermined = f'AF_Forecast_AfNbNeg_PLFGainPos'
            temp_fareanchor = forecastFareAnchor
            # Apply tolerance
            if has_tolerance(tolerance_per):
                t2 = reduce_by_per(strategyFare, float(tolerance_per))
                temp_fareanchor = max(temp_fareanchor, t2)
                if temp_fareanchor == t2:
                    howDetermined += '_ToleranceHit'
            # Apply hard anchor cap
            if has_hard_anchor():
                capped = min(temp_fareanchor, float(hardAnchorFare))
                if capped == float(hardAnchorFare):
                    howDetermined = f'AF_Forecast_harAnchorfare'
                temp_fareanchor = capped
            return temp_fareanchor, howDetermined

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
            t2 = reduce_by_per(strategyFare, float(tolerance_per))
            temp_fareanchor = max(temp_fareanchor, t2)
            if temp_fareanchor == t2:
                howDetermined += '_ToleranceHit'

        # Hard anchor cap
        if has_hard_anchor():
            capped = min(temp_fareanchor, float(hardAnchorFare))
            if capped == float(hardAnchorFare):
                howDetermined = f'AF_Forecast_harAnchorfare'
            temp_fareanchor = capped

        return temp_fareanchor, howDetermined

    def final_fare_anchor_forecast(self, strategyFare, profileFare, lastSoldFare, last7AvgVal,hardAnchorFare, currParams, plf, overBookingCount,BookingsInHour,last7avg,isProfileAnchor,b2b_discount_fare,anchorFareDiscounted,obfare, ndocounter, imp_forecast, imp_forecast_band, af_forecast_band, afForecastAllocFlag, Af_Nb_Allin, PLF_Gain, forecast_plf):
        anchorFare = 0
        strategyFare = float(strategyFare)
        howDetermined = ''
        upsell = 0
        overBookingFare = 0
        should_use_tolerance = False
        TOLERANCE_MAP = self.constantsutils.TOLERANCE_MAP
        B2B_FACTOR_MAP = self.constantsutils.B2B_FACTOR_MAP
        tolerance_per = TOLERANCE_MAP[str(imp_forecast_band)]
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
        
        def apply_tolerance_logic(anchor_fare, tolerance_per, how_determined):
            if has_tolerance(tolerance_per):
                tolerance_value = reduce_by_per(float(strategyFare), float(tolerance_per))
                anchor_fare = max(tolerance_value, anchor_fare)
                if (anchor_fare == tolerance_value):
                    how_determined += '_ToleranceHit'
            
            return anchor_fare, how_determined
        
        def has_hard_anchor():
            return hardAnchorFare != CONST_NA
        
        def apply_hard_anchor_cap(fare, how_determined):
            if not has_hard_anchor():
                return fare, how_determined
            
            capped_fare = min(fare, float(hardAnchorFare))
            if capped_fare == float(hardAnchorFare):
                return capped_fare, f"{imp_forecast_band}_AllocationY_harAnchorfare_UpsellN"
            return capped_fare, how_determined
        
        def upsell_by_per(value, percentage=2.5):
            return value * (1 + percentage / 100)
        
        def reduce_by_per(value, percentage):
            return value * (1 - percentage / 100)
        
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
            fareType = next(name for name, value in valid_fares.items() if value == anchorFare)
            
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
        
        if value_exist(lastSoldFare) and value_exist(last7AvgVal):
            maxLastSoldFare = max(lastSoldFare, last7AvgVal)
        elif value_exist(lastSoldFare):
            maxLastSoldFare = lastSoldFare
        elif value_exist(last7AvgVal):
            maxLastSoldFare = last7AvgVal
        else:
            maxLastSoldFare = None
        
        # ------
        if imp_forecast_band == 4:

            howDetermined = f'{imp_forecast_band}_AllocationY'
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
            
            assign_b2b_factor(currParams, b2b_factor_value)
            should_use_tolerance = True

        
        elif imp_forecast_band == 3:

            howDetermined = f'{imp_forecast_band}_AllocationY'

            if not value_exist(maxLastSoldFare) and not value_exist(profileFare):
                anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare)
                howDetermined += fareType + '_UpsellN'
            
            elif not value_exist(maxLastSoldFare):
                anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare, profileFare=profileFare)
                howDetermined += fareType + '_UpsellN'

            elif not value_exist(profileFare):
                avgLastSoldStrategy = (maxLastSoldFare + strategyFare) / 2
                anchorFare, fareType = get_fare_and_type(avgLastSoldStrategy=avgLastSoldStrategy)
                howDetermined += fareType + '_UpsellN'

            else:
                # all 3 fares exist
                if is_greater(strategyFare, maxLastSoldFare, profileFare):
                    avgLastSoldStrategy = (maxLastSoldFare + strategyFare) / 2
                    anchorFare, fareType = get_fare_and_type(profileFare=profileFare, avgLastSoldStrategy=avgLastSoldStrategy)
                        
                    howDetermined += fareType + '_UpsellN'
                    should_use_tolerance = True

                elif is_greater(profileFare, strategyFare) and is_greater(strategyFare, maxLastSoldFare):
                    avgLastSoldStrategy = (maxLastSoldFare + strategyFare) / 2
                    anchorFare, fareType = get_fare_and_type(avgLastSoldStrategy=avgLastSoldStrategy)

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

            assign_b2b_factor(currParams, b2b_factor_value)
        
        elif imp_forecast_band == 2:
            pass

        elif imp_forecast_band == 1:
            howDetermined = f'{imp_forecast_band}_AllocationY'
            lowestFare, _ = get_fare_and_type(strategyFare=strategyFare, maxLastSoldFare=maxLastSoldFare, profileFare=profileFare, func=min)
            
            if (lowestFare == profileFare):
                diff = get_imp_fcs_diff()
                if value_exist(diff) and diff < -1: # imp_forecast drops by > 1% in last 6H
                    avgStrategyProfile = (strategyFare + profileFare) / 2
                    anchorFare, fareType = get_fare_and_type(avgStrategyProfile=avgStrategyProfile)

                    howDetermined += fareType + '_UpsellN'

                else:
                    anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare, maxLastSoldFare=maxLastSoldFare, profileFare=profileFare, func=statistics.median)
                    howDetermined += fareType + '_UpsellN'

            else:
                anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare, maxLastSoldFare=maxLastSoldFare, profileFare=profileFare, func=min)
                howDetermined += fareType + '_UpsellN'
            
            assign_b2b_factor(currParams, b2b_factor_value)
            should_use_tolerance = True

        elif imp_forecast_band == 0:
            howDetermined = f'{imp_forecast_band}_AllocationY'
            anchorFare, fareType = get_fare_and_type(strategyFare=strategyFare,
                                                          maxLastSoldFare=maxLastSoldFare, profileFare=profileFare, func=min)
            howDetermined += fareType + '_UpsellN'
            
            assign_b2b_factor(currParams, b2b_factor_value)
            should_use_tolerance = True

        if afForecastAllocFlag == 1 and Af_Nb_Allin > 0:
            anchorFare, howDetermined = self.final_fare_anchor_af_forecast(
                anchorFare, af_forecast_band, afForecastAllocFlag, Af_Nb_Allin, PLF_Gain,
                strategyFare, plf, forecast_plf, lastSoldFare, profileFare, maxLastSoldFare,
                hardAnchorFare, tolerance_per, imp_forecast_band
            )
        else:
            if should_use_tolerance:
                anchorFare, howDetermined = apply_tolerance_logic(anchorFare, tolerance_per, howDetermined)
            
            anchorFare, howDetermined = apply_hard_anchor_cap(anchorFare, howDetermined)
        # ------

        # Overbooking logic & Final Adjustments
        anchorFare, howDetermined, upsell, overBookingFare = self.calculate_overbooking_fare_and_status(plf,overBookingCount,lastSoldFare,strategyFare,obfare,ndocounter,last7avg,isProfileAnchor,b2b_discount_fare,anchorFare,anchorFareDiscounted,upsell,howDetermined
        )
        
        return anchorFare, howDetermined, upsell, overBookingFare
    
    def finalFareAnchor(self, strategyFare, profileFare, lastSoldFare,hardAnchorFare, value, dPLF, toleranceFare, currParams, plf, overBookingCount,dplfBand,BookingsInHour,last7avg,isProfileAnchor,b2b_discount_fare,anchorFareDiscounted,obfare, ndocounter):
        anchorFare = 0
        strategyFare = float(strategyFare)
        howDetermined = ''
        upsell = 0
        overBookingFare = 0
        
        # Helper functions to reduce code duplication
        def has_tolerance_fare():
            return (toleranceFare is not None and 
                    pd.isna(toleranceFare) == False and 
                    toleranceFare != CONST_NA and 
                    toleranceFare != '' and 
                    pd.isnull(toleranceFare) == False)
        
        def has_hard_anchor():
            return hardAnchorFare != CONST_NA
        
        def apply_hard_anchor_cap(fare, decision_text):
            if not has_hard_anchor():
                return fare, decision_text
            
            capped_fare = min(fare, float(hardAnchorFare))
            if capped_fare == float(hardAnchorFare):
                return capped_fare, "AllocationY_harAnchorfare_UpsellN"
            return capped_fare, decision_text
        
        def calculate_tolerance_value():
            return float(strategyFare) - float(toleranceFare) if has_tolerance_fare() else 0
        
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
                calculated_fare = float(max_fare) + (float(base_fare) * 0.05)
                upsell_text = "UpsellY"
            elif float(max_fare) > float(strategyFare):
                calculated_fare = float(max_fare)
            else:
                calculated_fare = (float(max_fare) + float(strategyFare)) / 2
                
            return calculated_fare, upsell_text
        
        def process_dplf_bands_3(last_sold, strategy_or_profile):
            v2 = (float(last_sold) + max(float(strategyFare), float(strategy_or_profile))) / 2
            
            if has_tolerance_fare():
                v1 = calculate_tolerance_value()
                decision = "TolerenceHit_hardanchorfare_Upsold_Last_sold_fare" if has_hard_anchor() else "TolerenceHit_Upsold_Last_sold_fare"
                final_fare = max(v1, v2)
                if has_hard_anchor():
                    final_fare = min(final_fare, float(hardAnchorFare))
            else:
                decision = "hardanchorfare" if has_hard_anchor() and min(v2, float(hardAnchorFare)) == float(hardAnchorFare) else "Upsold_Last_sold_fare"
                final_fare = min(v2, float(hardAnchorFare)) if has_hard_anchor() else v2
                
            return final_fare, f'AllocationY_{decision}_UpsellN'
        
        def process_dplf_bands_4_5_6(fares_list):
            if not fares_list:
                return 0, "UpsellN"
                
            max_value = max(fares_list)
            v1, upsell_text = handle_upsell_logic(upsell, max_value, BookingsInHour)
            
            if has_tolerance_fare():
                t1 = calculate_tolerance_value()
                decision = "TolerenceHit_hardanchorfare_Upsold_Last_sold_fare" if has_hard_anchor() else "TolerenceHit_Upsold_Last_sold_fare"
                final_fare = max(t1, v1)
                if has_hard_anchor():
                    final_fare = min(final_fare, float(hardAnchorFare))
            else:
                decision = "hardanchorfare" if has_hard_anchor() and min(v1, float(hardAnchorFare)) == float(hardAnchorFare) else "Upsold_Last_sold_fare"
                final_fare = min(v1, float(hardAnchorFare)) if has_hard_anchor() else v1
                
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
                            anchorFare, howDetermined = process_dplf_bands_3(lastSoldFare, profileFare)
                        elif dplfBand in [4, 5, 6]:
                            anchorFare, howDetermined = process_dplf_bands_4_5_6(faresList)
                        
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
                            anchorFare, howDetermined = process_dplf_bands_3(lastSoldFare, strategyFare)
                        elif dplfBand in [4, 5, 6]:
                            anchorFare, howDetermined = process_dplf_bands_4_5_6(faresList)
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
                if has_tolerance_fare():
                    tolerance_val = calculate_tolerance_value()
                    anchorFare = max(max_fare, tolerance_val)
                    howDetermined = 'AllocationY_Maxof_Startegy_Profile_UpsellN' if anchorFare == max_fare else 'AllocationY_ToleranceHit_UpsellN'
                else:
                    anchorFare = max_fare
                    howDetermined = 'AllocationY_Maxof_Startegy_Profile_UpsellN'
                
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
                        decision = "hardanchorfare"
                howDetermined = f"AllocationY_{decision}_UpsellN"
            
            # Max value equals strategy fare
            elif max_value == float(strategyFare):
                anchorFare = float(strategyFare)
                howDetermined = "AllocationY_Startegy_fare_UpsellN"
                if has_hard_anchor():
                    decision = "strategyFare"
                    anchorFare = min(float(strategyFare), float(hardAnchorFare))
                    if anchorFare == float(hardAnchorFare):
                        decision = "hardanchorfare"
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
                
                v2 = calculate_tolerance_value()
                
                if has_hard_anchor():
                    anchorFare = min(max(v1, v2), float(hardAnchorFare))
                    if v2 > v1:
                        howDetermined += '_ToleranceHit'
                    if anchorFare == float(hardAnchorFare):
                        howDetermined = "AllocationY_harAnchorfare_UpsellN"
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
                v2 = calculate_tolerance_value()
                
                if v2 > v1:
                    howDetermined += '_ToleranceHit'
                
                if has_hard_anchor():
                    anchorFare = min(max(v1, v2), float(hardAnchorFare))
                    if anchorFare == float(hardAnchorFare):
                        howDetermined = "AllocationY_harAnchorfare_UpsellN"
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
        
        return anchorFare, howDetermined, upsell, overBookingFare

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
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['TOTAL'][0]
        else:
            if defFareData.shape[0] != 0:
                if float(defFareData['TOTAL'][0]) >= float(rbddata['TOTAL'][offsetToFetch]):
                    offsetToFetch = min(len(defFareData['TOTAL'])-1, offsetToFetch)
                    if rbddata['TOTAL'][0] < defFareData['TOTAL'][offsetToFetch] and backstop > 0:
                        backstopFlag = True
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
            openResult = {'RBD': [RBDtoOpen], 'Fare': [str(FareToOpen)], 'FareResult': ['Matched and allocated - Backstop Hit'], 'anchorFare': [anchorFare], 'statusInd': [0], 'allocationStatus': [1]}
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
