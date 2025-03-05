from datetime import datetime, timedelta
from decimal import Decimal
import json
import pandas as pd
from Constants import *
import random

class DetermineRBD:
    
    def __init__(self, rmdatabasehelper, logger, inputsource, constantsutils, navdatabasehelper, mathutils, redisHelper):
        self.inputSource = inputsource
        self.rmdatabasehelper = rmdatabasehelper
        self.navdatabasehelper = navdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils
        self.redishelper = redisHelper

    def determineRBDToOpen(self, anchorFare, offset, channel, currFltDate, dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, backstop, currenttime, startEndOfDayDplfBand, dPLF, month, deptTime, dow, percentile, upsellValue, toleranceFare,plf,overBookingCount,hardAnchorFare,hardoffset,BookingsInHour,lastSoldFare,last7avg,b2b_discount_fare,discount_amount,w0Flag,obfare):

        origin = currParams.origin
        destin = currParams.destin
        strategyFare = CONST_NA
        profileFare = CONST_NA
        howDetermined = CONST_NA
        upsell = CONST_NA
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
            return openResult, lastSoldFare, strategyFare, profileFare, upsell,hardAnchorFare
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
            
            return openResult, lastSoldFare, strategyFare, profileFare, upsell,hardAnchorFare
        if strategyFare == CONST_NA:
            return openResult, lastSoldFare, strategyFare, profileFare, upsell,hardAnchorFare

        self.logger.info("totalCapacity: " + str(totalCapacity))
        self.logger.info("currBookedLoad: " + str(currBookedLoad))

        sector = origin + destin
        
        if upsellValue is None or str(upsellValue).strip() == '' or str(upsellValue).strip().upper() == 'NA':
            upsellValue = 0

        profileFaresData = self.rmdatabasehelper.getProfileFares(sector, month, dow, deptTime, percentile)
        if len(profileFaresData) != 0:
            if profileFaresData['price'][0]!="":
                profileFare = profileFaresData['price'][0]
                if upsellValue != 0:
                    profileFare = (float(profileFare) * (1 + float(upsellValue)/100))

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
        
        anchorFare, howDetermined, upsell = self.finalFareAnchor(strategyFare, profileFare, lastSoldFare,hardAnchorFare, anchorValue, dPLF, toleranceFare, currParams,plf,overBookingCount,dplfBand,BookingsInHour,last7avg,b2b_discount_fare,anchorFare, obfare, ndocounter)

        
            
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
        return openResult, lastSoldFare, strategyFare, profileFare,hardAnchorFare, upsell

    def finalFareAnchor(self, strategyFare, profileFare, lastSoldFare,hardAnchorFare, value, dPLF, toleranceFare, currParams, plf, overBookingCount,dplfBand,BookingsInHour,last7avg,b2b_discount_fare,anchorFareDiscounted,obfare, ndocounter):
        anchorFare = 0
        strategyFare = float(strategyFare)
        howDetermined = ''
        upsell = 0
        if value == 'A':
            faresList = []
            max_value = 0
            # Check each variable if it exists and add its value to the list
            if lastSoldFare != CONST_NA:
                faresList.append(float(lastSoldFare))
            # if historicOwnFare != CONST_NA:
            #     faresList.append(float(historicOwnFare))
            if profileFare != CONST_NA and int(currParams.profileFares) == 1:
                faresList.append(float(profileFare))
            # if hardAnchorFare != CONST_NA:
            #     faresList.append(float(hardAnchorFare))
            if lastSoldFare != CONST_NA:
                if profileFare != CONST_NA and int(currParams.profileFares) == 1:
                    if float(lastSoldFare)<float(profileFare) or float(lastSoldFare)<float(strategyFare):
                        # delta = max(float(profileFare),float(strategyFare))-float(lastSoldFare)
                        if dplfBand == 3 or dplfBand == 4:
                            V2 = (float(lastSoldFare) + max(float(strategyFare), float(profileFare)))/2
                            if toleranceFare is not None and pd.isna(toleranceFare)==False and toleranceFare != CONST_NA and toleranceFare != '' and pd.isnull(toleranceFare)==False:
                                V1 = float(strategyFare)-float(toleranceFare)
                                dec="TolerenceHit_Upsold_Last_sold_fare"
                                if hardAnchorFare!=CONST_NA:
                                    dec="TolerenceHit_hardanchorfare_Upsold_Last_sold_fare"
                                    anchorFare = min(max(V1,V2), float(hardAnchorFare))
                                else:
                                    anchorFare = max(V1,V2)
                            else:
                                dec="Upsold_Last_sold_fare"
                                if hardAnchorFare!=CONST_NA:
                                    anchorFare = min(V2, float(hardAnchorFare))
                                    if anchorFare == float(hardAnchorFare):
                                        dec="hardanchorfare"    
                                else:
                                    anchorFare = V2
                            howDetermined = f'AllocationY_{dec}_UpsellN'
                        elif dplfBand == 5 or dplfBand == 6:
                            if faresList:
                                upText = "UpsellN"
                                max_value = max(faresList)
                                if float(max_value) > float(strategyFare):
                                    if float(BookingsInHour) > 0:
                                        V1 = float(max_value) + (float(upsell) * 0.05)
                                        upText = "UpsellY"
                                    elif float(BookingsInHour) == 0:
                                        V1 = float(max_value)
                                else:
                                    V1 = (float(max_value) + float(strategyFare))/2
                            if toleranceFare is not None and pd.isna(toleranceFare)==False and toleranceFare != CONST_NA and toleranceFare != '' and pd.isnull(toleranceFare)==False:
                                dec="TolerenceHit_Upsold_Last_sold_fare"
                                T1 = float(strategyFare)-float(toleranceFare)
                                T2 = V1
                                if hardAnchorFare!=CONST_NA:
                                    dec="TolerenceHit_hardanchorfare_Upsold_Last_sold_fare"
                                    anchorFare = min(max(T1,T2), float(hardAnchorFare))
                                else:
                                    anchorFare = max(T1,T2) 
                            else:
                                dec="Upsold_Last_sold_fare"
                                if hardAnchorFare!=CONST_NA:
                                    anchorFare = min(V1, float(hardAnchorFare))
                                    if anchorFare == float(hardAnchorFare):
                                        dec = "hardanchorfare"
                                else:
                                    anchorFare = V1
                            howDetermined = f'AllocationY_{dec}_{upText}'
                        # else:
                        #     howDetermined = f'AllocationY_Max_own_fare_var_15+_UpsellN'
                        #     anchorFare=max(float(profileFare),float(strategyFare))
                        if hardAnchorFare!=CONST_NA:
                            anchorFare = min(anchorFare,float(hardAnchorFare))
                            if min(anchorFare,float(hardAnchorFare))==float(hardAnchorFare):
                                howDetermined = "AllocationY_harAnchorfare_UpsellN"
                    if float(lastSoldFare)>=float(profileFare) and float(lastSoldFare)>=float(strategyFare):
                        if float(BookingsInHour)>0:
                            howDetermined = 'AllocationY_last_sold_fare_UpsellY'
                            upsell=lastSoldFare*0.05
                            anchorFare=lastSoldFare+upsell
                        else:
                            howDetermined = 'AllocationY_last_sold_fare_UpsellN'
                            anchorFare=lastSoldFare
                        if hardAnchorFare!=CONST_NA:
                            anchorFare = min(anchorFare,float(hardAnchorFare))
                            if min(anchorFare,float(hardAnchorFare))==float(hardAnchorFare):
                                howDetermined = "AllocationY_harAnchorfare_UpsellN"
                else:
                    if float(lastSoldFare)<=float(strategyFare):
                        # delta = abs(float(strategyFare) - float(lastSoldFare))
                        if dplfBand == 3 or dplfBand == 4:
                            V2 = (float(lastSoldFare) + float(strategyFare))/2
                            if toleranceFare is not None and pd.isna(toleranceFare)==False and toleranceFare != CONST_NA and toleranceFare != '' and pd.isnull(toleranceFare)==False:
                                V1 = float(strategyFare)-float(toleranceFare)
                                dec="TolerenceHit_Upsold_Last_sold_fare"
                                if hardAnchorFare!=CONST_NA:
                                    dec="TolerenceHit_hardanchorfare_Upsold_Last_sold_fare"
                                    anchorFare = min(max(V1,V2), float(hardAnchorFare))
                                else:
                                    anchorFare = max(V1,V2)
                            else:
                                dec="Upsold_Last_sold_fare"
                                if hardAnchorFare!=CONST_NA:
                                    anchorFare = min(V2, float(hardAnchorFare))
                                    if anchorFare == float(hardAnchorFare):
                                        dec="hardanchorfare"    
                                else:
                                    anchorFare = V2
                            howDetermined = f'AllocationY_{dec}_UpsellN'
                        elif dplfBand == 5 or dplfBand == 6:
                            if faresList:
                                upText = "UpsellN"
                                max_value = max(faresList)
                                if float(max_value) > float(strategyFare):
                                    if float(BookingsInHour) > 0:
                                        V1 = float(max_value) + (float(upsell) * 0.05)
                                        upText = "UpsellY"
                                    elif float(BookingsInHour) == 0:
                                        V1 = float(max_value)
                                else:
                                    V1 = (float(max_value) + float(strategyFare))/2
                            if toleranceFare is not None and pd.isna(toleranceFare)==False and toleranceFare != CONST_NA and toleranceFare != '' and pd.isnull(toleranceFare)==False:
                                dec="TolerenceHit_Upsold_Last_sold_fare"
                                T1 = float(strategyFare)-float(toleranceFare)
                                T2 = V1
                                if hardAnchorFare!=CONST_NA:
                                    dec="TolerenceHit_hardanchorfare_Upsold_Last_sold_fare"
                                    anchorFare = min(max(T1,T2), float(hardAnchorFare))
                                else:
                                    anchorFare = max(T1,T2) 
                            else:
                                dec="Upsold_Last_sold_fare"
                                if hardAnchorFare!=CONST_NA:
                                    anchorFare = min(V1, float(hardAnchorFare))
                                    if anchorFare == float(hardAnchorFare):
                                        dec = "hardanchorfare"
                                else:
                                    anchorFare = V1
                            howDetermined = f'AllocationY_{dec}_{upText}'
                    else:
                        if float(BookingsInHour)>0:
                            howDetermined = 'AllocationY_last_sold_fare_UpsellY'
                            upsell=lastSoldFare*0.05
                            anchorFare=lastSoldFare+upsell
                        else:
                            howDetermined = 'AllocationY_last_sold_fare_UpsellN'
                            anchorFare=lastSoldFare
                    if hardAnchorFare!=CONST_NA:
                        anchorFare = min(anchorFare,float(hardAnchorFare))
                        if min(anchorFare,float(hardAnchorFare))==float(hardAnchorFare):
                            howDetermined = "AllocationY_harAnchorfare_UpsellN"
            else:
                if profileFare!=CONST_NA and int(currParams.profileFares) == 1:
                    if toleranceFare is not None and pd.isna(toleranceFare)==False and toleranceFare != CONST_NA and toleranceFare != '' and pd.isnull(toleranceFare)==False:
                        anchorFare=max(((float(profileFare)+float(strategyFare))/2), (float(strategyFare)-float(toleranceFare)))
                        if anchorFare == ((float(profileFare)+float(strategyFare))/2):
                            howDetermined = 'AllocationY_Avgof_Startegy_Profile_UpsellN'
                        else:
                            howDetermined = 'AllocationY_ToleranceHit_UpsellN'
                    else:
                        anchorFare=(float(profileFare)+float(strategyFare))/2
                        howDetermined = 'AllocationY_Avgof_Startegy_Profile_UpsellN'
                else:
                    anchorFare=float(strategyFare)
                    howDetermined = 'AllocationY_Startegy_fare_UpsellN'
                if hardAnchorFare!=CONST_NA:
                    anchorFare = min(anchorFare,float(hardAnchorFare))
                    if min(anchorFare,float(hardAnchorFare))==float(hardAnchorFare):
                        howDetermined = "AllocationY_harAnchorfare_UpsellN"
            # Check if any values exist
            # if faresList:
            #     max_value = max(faresList)
                
            # if max_value == 0:
            #     anchorFare = strategyFare
            #     howDetermined = 'AllocationY_StrategyFare_UpsellN'
            #     upsell = CONST_NA
            # else:
            #     if max_value > strategyFare:
            #         howDetermined = 'AllocationY_MaxOwnFare_UpsellY'
            #         if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
            #             if random.choice([True, False]):
            #                 upsell = max_value * 0.05
            #         anchorFare = max_value + upsell
            #     else:
            #         upsell = CONST_NA
            #         interpolatedValue = (max_value + strategyFare) / 2
            #         howDetermined = 'AllocationY_UpsoldMaxOwnFare_UpsellN'
                    
            #         if toleranceFare is None or pd.isna(toleranceFare) or toleranceFare == CONST_NA or toleranceFare == '' or pd.isnull(toleranceFare):
            #             anchorFare = interpolatedValue
            #         else:
            #             tolaranceValue = float(strategyFare) - float(toleranceFare)
            #             if interpolatedValue > tolaranceValue:
            #                 anchorFare = interpolatedValue
            #             else:
            #                 anchorFare = tolaranceValue
            #                 howDetermined = howDetermined + '_ToleranceHit'

        if value == 'B':
            upsell = CONST_NA
            faresList = []
            max_value = 0
            if lastSoldFare != CONST_NA and float(lastSoldFare) <= float(strategyFare):
                faresList.append(float(lastSoldFare))
            # if historicOwnFare != CONST_NA and float(historicOwnFare) <= float(strategyFare):
            #     faresList.append(float(historicOwnFare))
            if profileFare != CONST_NA and float(profileFare) <= float(strategyFare) and int(currParams.profileFares) == 1:
                faresList.append(float(profileFare))
            # if hardAnchorFare != CONST_NA and float(hardAnchorFare) <= float(strategyFare):
            #     faresList.append(float(hardAnchorFare))

            if faresList:
                max_value = max(faresList)

            if max_value == 0:
                anchorFare = float(strategyFare)
                howDetermined = 'AllocationY_StrategyFare_UpsellN'
                if hardAnchorFare!=CONST_NA:
                    anchorFare = min(float(strategyFare),float(hardAnchorFare))
                    dec="StrategyFare"
                    if  min(float(strategyFare),float(hardAnchorFare))==float(hardAnchorFare):
                            dec="hardanchorfare"
                    howDetermined =f"AllocationY_{dec}_UpsellN"
            else:
                if max_value == float(strategyFare):
                    if hardAnchorFare!=CONST_NA:
                        dec="strategyFare"
                        if  min(float(strategyFare),float(hardAnchorFare))==float(hardAnchorFare):
                            dec="hardanchorfare"
                        anchorFare = min(float(strategyFare),float(hardAnchorFare))
                        howDetermined =f"AllocationY_min_{dec}_UpsellN"
                    else:
                        anchorFare = float(strategyFare)
                    howDetermined ="AllocationY_Startegy_fare_UpsellN"
                if max_value<float(strategyFare):
                    delta = float(strategyFare) - float(max_value)
                    howDetermined = 'AllocationY_Interpollated_Max_own_fare_UpsellN'
                    if dPLF>-self.constantsutils.OWN_FARE_DELTA_B:
                        v1 = max_value + (delta / self.constantsutils.OWN_FARE_DELTA_B) * (dPLF + self.constantsutils.OWN_FARE_DELTA_B)
                    else:
                        v1=max_value
                    if toleranceFare is None or pd.isna(toleranceFare) or toleranceFare == CONST_NA or toleranceFare == '' or pd.isnull(toleranceFare):
                        v2=0
                    else:
                        v2=float(strategyFare)-float(toleranceFare)
                    if hardAnchorFare!=CONST_NA:
                        anchorFare=min(max(v1,v2),float(hardAnchorFare))
                        if v2>v1:
                            howDetermined = howDetermined + '_ToleranceHit'
                        if hardAnchorFare!=CONST_NA:
                            anchorFare = min(anchorFare,float(hardAnchorFare))
                            if min(anchorFare,float(hardAnchorFare))==float(hardAnchorFare):
                                howDetermined = "AllocationY_harAnchorfare_UpsellN"
                    else:
                        anchorFare=max(v1,v2)
                if profileFare!=CONST_NA and lastSoldFare!=CONST_NA:
                    if float(strategyFare)>float(profileFare) and float(strategyFare)>float(lastSoldFare):
                        howDetermined = "AllocationY_interpolated_lastSold_Profile_fare_UpsellN"
                        v1 = min(float(lastSoldFare),float(profileFare))+(abs(float(lastSoldFare)-float(profileFare))/self.constantsutils.OWN_FARE_DELTA_B)*(dPLF+self.constantsutils.OWN_FARE_DELTA_B)
                        if not (toleranceFare is None or pd.isna(toleranceFare) or toleranceFare == CONST_NA or toleranceFare == '' or pd.isnull(toleranceFare)):
                            v2 = float(strategyFare) - float(toleranceFare)
                        else:
                            v2=0
                        if v2>v1:
                            howDetermined = howDetermined + '_ToleranceHit'
                        if hardAnchorFare!=CONST_NA:
                            anchorFare = min(max(v1,v2),float(hardAnchorFare))
                            if min(anchorFare,float(hardAnchorFare))==float(hardAnchorFare):
                                howDetermined = "AllocationY_harAnchorfare_UpsellN"
                        else:
                            anchorFare = max(v1,v2)
                        
                # delta = float(strategyFare) - float(max_value)
                # howDetermined = 'AllocationY_InterpolatedMaxOwnFare_UpsellN'
                # if float(max_value) <= float(strategyFare):
                #     interpolatedValue = max_value + (delta / self.constantsutils.OWN_FARE_DELTA) * (dPLF + self.constantsutils.OWN_FARE_DELTA)
                # if dPLF <= (float(self.constantsutils.OWN_FARE_DELTA) * -1):
                #     interpolatedValue = max_value

                # if toleranceFare is None or pd.isna(toleranceFare) or toleranceFare == CONST_NA or toleranceFare == '' or pd.isnull(toleranceFare):
                #     anchorFare = interpolatedValue
                # else:
                #     tolaranceValue = float(strategyFare) - float(toleranceFare)
                #     if interpolatedValue > tolaranceValue:
                #         anchorFare = interpolatedValue
                #     else:
                #         anchorFare = tolaranceValue
                #         howDetermined = howDetermined + '_ToleranceHit'

        if value == 'C':
            # Skip allocations
            upsell = CONST_NA
            howDetermined = 'AllocationN'
            anchorFare =  0
        
        if plf >= self.constantsutils.OVERBOOKING_FARE_LF and plf < self.constantsutils.OVERBOOKING_MANUAL_LF and overBookingCount>0 and lastSoldFare != CONST_NA and (obfare is None and pd.isna(obfare)==True and obfare == CONST_NA and obfare == '' and pd.isnull(obfare)==True):
            self.logger.info("Entered System OB since plf between 98 - 100")
            lastSoldFare=float(lastSoldFare)
            howDetermined = "Overbooking Calculated Fare"
            upsell = CONST_NA
            calculatedFare = strategyFare
            if lastSoldFare != CONST_NA and lastSoldFare != 0:
                fare1 = self.constantsutils.OVERBOOKING_FARE_PERCENT_VALUE*lastSoldFare
                fare2 = lastSoldFare+self.constantsutils.OVERBOOKING_FARE_ABSOLUTE_VALUE
                calculatedFare = max(fare1,fare2)
            if calculatedFare < strategyFare:
                anchorFare = (calculatedFare+strategyFare)/2
            else:
                anchorFare = calculatedFare 
        if plf >= self.constantsutils.OVERBOOKING_MANUAL_LF and obfare is not None and pd.isna(obfare)==False and obfare != CONST_NA and obfare != '' and pd.isnull(obfare)==False and 1 >= ndocounter >= 0:
            self.logger.info("Entered Manual OB since plf > 100")
            anchorFare=float(obfare)
            howDetermined="Overbook_fare_(V1)"
            upsell=CONST_NA
        if last7avg:
            howDetermined+="_last7bkgsAvg"
        if b2b_discount_fare:
            anchorFare = anchorFareDiscounted
            upsell = CONST_NA
        return anchorFare, howDetermined, upsell

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
