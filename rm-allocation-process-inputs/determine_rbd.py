from datetime import datetime, timedelta
from decimal import Decimal
import json
import pandas as pd
from Constants import *
import random

class DetermineRBD:
    
    def __init__(self, rmdatabasehelper, logger, inputsource, constantsutils, mathutils, redisHelper):
        self.inputSource = inputsource
        self.rmdatabasehelper = rmdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils
        self.redishelper = redisHelper

    def determineRBDToOpen(self, anchorFare, offset, channel, currFltDate, dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, backstop, currenttime, startEndOfDayDplfBand, dPLF, toleranceFare,plf,overBookingCount,hardAnchorFare,hardoffset,BookingsInHour,lastSoldFare,last7avg,b2b_discount_fare,discount_amount,w0Flag,obfare, profileFare, isProfileAnchor):

        origin = currParams.origin
        destin = currParams.destin
        strategyFare = CONST_NA
        howDetermined = CONST_NA
        upsell = CONST_NA
        overBookingFare = 0
        openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
        hardopenResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
        openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult, 'L')
        if self.check_int(hardoffset) and hardAnchorFare!=CONST_NA:
            hardopenResult, hardrbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, hardAnchorFare, hardoffset, channel, hardopenResult, 'L')
            if hardrbddata.shape[0] == 0 and  hardAnchorFare!=CONST_NA:
                hardopenResult = self.extremeFaresSkipping(origin, destin, hardAnchorFare, hardoffset, channel, backstop,  hardopenResult, 'L')
            elif  hardAnchorFare!=CONST_NA:
                hardopenResult = self.getFares(origin, destin, hardAnchorFare, hardoffset, channel,  hardopenResult, hardrbddata, howDetermined, 'L')
            hardAnchorFare=hardopenResult['Fare'][0]
            
        if rbddata is None:
            return openResult, lastSoldFare, strategyFare, CONST_NA, upsell,hardAnchorFare, overBookingFare
        if rbddata.shape[0] == 0:
            openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop,  openResult, 'L')
            
        else:
            openResult = self.getFares(origin, destin, anchorFare, offset, channel,  openResult, rbddata, howDetermined, 'L')
            openResult = self.getFares(origin, destin, anchorFare, offset, channel,  openResult, rbddata, howDetermined, 'L')
        
            openResult = self.getFares(origin, destin, anchorFare, offset, channel,  openResult, rbddata, howDetermined, 'L') 
        curTime = datetime.strptime(str(currenttime), TIME_FORMAT).time().strftime(TIME_FORMAT)
        timeString = 'Pre10'
        if curTime > TIME_10_AM:
            timeString = 'Post10'
        value = self.rmdatabasehelper.getRDBTable(startEndOfDayDplfBand, dplfBand, curTime, ndocounter)
        if value.shape[0] != 0:
            if channel == B2B_CHANNEL:
                anchorValue = value['B2BValue'][0]
            if channel == B2C_CHANNEL:
                anchorValue = value['B2CValue'][0]
        strategyFare = openResult['Fare'][0]
        if anchorValue == 'C':
            openResult['FareResult']=[timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_C_' + howDetermined]
            openResult['anchorFare']=['NA']
            openResult['RBD']=['NA']
            openResult['Fare']=['NA']
            
            return openResult, lastSoldFare, strategyFare, CONST_NA, upsell,hardAnchorFare, overBookingFare
        if strategyFare == CONST_NA:
            return openResult, lastSoldFare, strategyFare, CONST_NA, upsell,hardAnchorFare, overBookingFare

        self.logger.info("totalCapacity: " + str(totalCapacity))
        self.logger.info("currBookedLoad: " + str(currBookedLoad))


        if not (int(currParams.opening_fares) == 1) and not (plf >= self.constantsutils.OVERBOOKING_FARE_LF and overBookingCount>0):
            lastSoldFare = CONST_NA
                
        value = self.rmdatabasehelper.getRDBTable(startEndOfDayDplfBand, dplfBand, curTime, ndocounter)
        if value.shape[0] != 0:
            if channel == B2B_CHANNEL:
                anchorValue = value['B2BValue'][0]
            if channel == B2C_CHANNEL:
                anchorValue = value['B2CValue'][0]

        self.logger.info("Last Sold Price: " + str(lastSoldFare))
        self.logger.info("Profile Fare: " + str(profileFare))
        self.logger.info("Startegy Fare: " + str(strategyFare))
        
        anchorFare, howDetermined, upsell, overBookingFare = self.finalFareAnchor(strategyFare, profileFare, lastSoldFare,hardAnchorFare, anchorValue, dPLF, toleranceFare, currParams,plf,overBookingCount,dplfBand,BookingsInHour,last7avg,isProfileAnchor,b2b_discount_fare,anchorFare, obfare, ndocounter)

        
            
        if anchorValue == 'A':
            howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_A_' + howDetermined
        elif anchorValue == 'B':
            howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_B_' + howDetermined
        elif anchorValue == 'C':
            howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_C_' + howDetermined
        if anchorValue!='C' and b2b_discount_fare:
            howDetermined+=f"_Discounted_{discount_amount}"
        if int(w0Flag)==1:
            upsell = CONST_NA
            howDetermined = howDetermined+'_Manual_inventory_retained_skipping_allocation'
            anchorFare =  0
        if anchorFare != 0:
            offset = 0
            openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult, 'L')
            if rbddata.shape[0] == 0:
                openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop,  openResult, 'L')
            else:
                openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop,  openResult, rbddata, howDetermined, 'L')
        else:
            openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': [howDetermined], 'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}
        return openResult, lastSoldFare, strategyFare, profileFare,hardAnchorFare, upsell, overBookingFare

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
        
        # Overbooking logic
        should_apply_system_ob = (plf >= self.constantsutils.OVERBOOKING_LF and 
                                overBookingCount > 0 and 
                                lastSoldFare != CONST_NA and 
                                (obfare is None or pd.isna(obfare) or obfare == CONST_NA or 
                                obfare == '' or pd.isnull(obfare)))
        
        should_apply_manual_ob = (plf >= self.constantsutils.OVERBOOKING_LF and 
                                obfare is not None and pd.isna(obfare) == False and 
                                obfare != CONST_NA and obfare != '' and 
                                pd.isnull(obfare) == False and 1 >= ndocounter >= 0)
        
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
                openResult = {'RBD': [defFareData['RBD'][0]], 'Fare': [defFareData['Total'][0]], 'FareResult': ['Unable to Match Mkt - B2C allocation at extreme value'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [1]}
                return openResult

            if channel == B2B_CHANNEL:
                if defFareData['RBD'][0] == self.constantsutils.HIGHEST_B2B_RBD_VALUE:
                    openResult = {'RBD':  [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['Market Fare higher than ' + self.constantsutils.HIGHEST_B2B_RBD_VALUE + ' levels - skipping B2B allocation'], 'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}
                    return openResult
                else:
                    openResult = {'RBD': [defFareData['RBD'][0]], 'Fare': [defFareData['Total'][0]], 'FareResult': ['Unable to Match Mkt - B2B allocation at extreme value'], 'anchorFare': [anchorFare], 'statusInd': [2], 'allocationStatus': [1]}
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
            if rbddata['Total'][0] < defFareData['Total'][0] and backstop > 0:
                backstopFlag = True
            
        # Edge case if the backstop values are placed very high - in which case it will place the backstop at Z0 and Y levels 
        if defFareData.shape[0] == 0:
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0, route)
            
        if rbddata['RBD'][0] == self.constantsutils.LOWEST_B2B_RBD_VALUE or rbddata['RBD'][0] == self.constantsutils.LOWEST_B2C_RBD_VALUE:
            if len(defFareData) == 0:
                extremeFareFlag = True
                RBDtoOpen = rbddata['RBD'][0]
                FareToOpen = rbddata['Total'][0]
            else:
                if rbddata['RBD'][0] == defFareData['RBD'][0]:
                    extremeFareFlag = True
                    RBDtoOpen = rbddata['RBD'][0]
                    FareToOpen = rbddata['Total'][0]
                else:
                    backstopFlag = True
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['Total'][0]
        else:
            if defFareData.shape[0] != 0:
                if float(defFareData['Total'][0]) >= float(rbddata['Total'][offsetToFetch]):
                    offsetToFetch = min(len(defFareData['Total'])-1, offsetToFetch)
                    if rbddata['Total'][0] < defFareData['Total'][offsetToFetch] and backstop > 0:
                        backstopFlag = True
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['Total'][0]
                else:
                    RBDtoOpen = rbddata['RBD'][offsetToFetch]
                    FareToOpen = rbddata['Total'][offsetToFetch]
                    
            else:
                RBDtoOpen = rbddata['RBD'][offsetToFetch]
                FareToOpen = rbddata['Total'][offsetToFetch]
                    
        self.logger.info('Determining ' + channel + ' Fare ' + str(FareToOpen) + " " + str(RBDtoOpen))

        if channel == B2B_CHANNEL:
            # Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
            limitB2BFare_Data = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0, route)
            limitB2BFare = 0
            if defFareData.shape[0] != 0:
                limitB2BFare = float(limitB2BFare_Data['Total'][0])

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
        
    def getFares(self, origin, destin, anchorFare, offset, channel, openResult, rbddata, howDetermined, route):
        extremeFareFlag = False
        # We got back data purely on the basis of anchor fare - comparison with offset remains which is done in the subsequent section

        # Calculating the offset that is required based on entries remaining in the RBD table
        offsetToFetch = min(abs(int(offset)), rbddata.shape[0] - 1)
        if int(offset) < 0:
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
                FareToOpen = rbddata['Total'][0]
            else:
                if rbddata['RBD'][0] == defFareData['RBD'][0]:
                    extremeFareFlag = True
                    RBDtoOpen = rbddata['RBD'][0]
                    FareToOpen = rbddata['Total'][0]
                else:
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['Total'][0]
        else:
            if defFareData.shape[0] != 0:
                if float(defFareData['Total'][0]) >= float(rbddata['Total'][offsetToFetch]):
                    offsetToFetch = min(len(defFareData['Total'])-1, offsetToFetch)
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['Total'][0]
                else:
                    RBDtoOpen = rbddata['RBD'][offsetToFetch]
                    FareToOpen = rbddata['Total'][offsetToFetch]

            else:
                RBDtoOpen = rbddata['RBD'][offsetToFetch]
                FareToOpen = rbddata['Total'][offsetToFetch]
                    
        self.logger.info('Determining ' + channel + ' Fare ' + str(FareToOpen) + " " + str(RBDtoOpen))

        if channel == B2B_CHANNEL:
            # Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
            limitB2BFare_Data = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0, route)
            limitB2BFare = 0
            if defFareData.shape[0] != 0:
                limitB2BFare = float(limitB2BFare_Data['Total'][0])

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
