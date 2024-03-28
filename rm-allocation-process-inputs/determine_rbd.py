from datetime import datetime
import pandas as pd
from Constants import *
import random

class DetermineRBD:
    
    def __init__(self, rmdatabasehelper, logger, inputsource, constantsutils, navdatabasehelper, mathutils):
        self.inputSource = inputsource
        self.rmdatabasehelper = rmdatabasehelper
        self.navdatabasehelper = navdatabasehelper
        self.logger = logger
        self.constantsutils = constantsutils
        self.mathutils = mathutils

    def determineRBDToOpen(self, anchorFare, offset, channel, currFltDate, dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, backstop, currenttime, startEndOfDayDplfBand, dPLF, month, deptTime, dow, percentile, upsellValue):

        origin = currParams.origin
        destin = currParams.destin
        lastSoldFare = CONST_NA
        strategyFare = CONST_NA
        historicOwnFare = CONST_NA
        profileFare = CONST_NA
        howDetermined = CONST_NA
        upsell = CONST_NA

        openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])

        openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult)
        if rbddata is None:
            return openResult, lastSoldFare, strategyFare, historicOwnFare, profileFare, upsell
        if rbddata.shape[0] == 0:
            openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop,  openResult)
        else:
            openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop,  openResult, rbddata, howDetermined)

        strategyFare = openResult['Fare'][0]

        if strategyFare == CONST_NA:
            return openResult, lastSoldFare, strategyFare, historicOwnFare, profileFare, upsell

        self.logger.info("totalCapacity: " + str(totalCapacity))
        self.logger.info("currBookedLoad: " + str(currBookedLoad))

        sector = origin + destin
        curTime = datetime.strptime(str(currenttime), TIME_FORMAT).time().strftime(TIME_FORMAT)

        if currParams.own_fare_flag == YES_FLAG:
            historicData = self.rmdatabasehelper.getFareHistoryData(ndocounter, sector, month, dow, channel, deptTime)
            if len(historicData) != 0:
                historicOwnFare = historicData['price'][0]
                if upsellValue != 0:
                    historicOwnFare = (historicOwnFare * (1 + upsellValue/100))

        if currParams.profileFares == YES_FLAG:
            profileFaresData = self.rmdatabasehelper.getProfileFares(sector, month, dow, deptTime, percentile)
            if len(profileFaresData) != 0:
                profileFare = profileFaresData['price'][0]
                if upsellValue != 0:
                    profileFare = (profileFare * (1 + upsellValue/100))

        if int(currParams.opening_fares) == 1:
            if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == TEST_DATA:
                farePrice = self.rmdatabasehelper.getLastSellingFare(currFltDate, currParams)
            if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == DB:
                farePrice = self.navdatabasehelper.getLastSellingFare(currFltDate, currParams)
            if farePrice.shape[0] != 0:
                if channel == B2C_CHANNEL:
                    lastSoldFare = farePrice["B2Cfare"][0]
                if channel == B2B_CHANNEL:
                    lastSoldFare = farePrice["B2Bfare"][0]

        value = self.rmdatabasehelper.getRDBTable(startEndOfDayDplfBand, dplfBand, curTime)
        if value.shape[0] != 0:
            if channel == B2B_CHANNEL:
                anchorValue = value['B2BValue'][0]
            if channel == B2C_CHANNEL:
                anchorValue = value['B2CValue'][0]

        self.logger.info("Last Sold Price: " + str(lastSoldFare))
        self.logger.info("Profile Fare: " + str(profileFare))
        self.logger.info("Historic Fare: " + str(historicOwnFare))
        self.logger.info("Startegy Fare: " + str(strategyFare))
        
        anchorFare, howDetermined, upsell = self.finalFareAnchor(strategyFare, historicOwnFare, profileFare, lastSoldFare, anchorValue, dPLF)

        timeString = 'Pre10'
        if curTime > TIME_10_AM:
            timeString = 'Post10'
            
        if dPLF > 0:
            howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_A_' + howDetermined
        else:
            howDetermined = timeString + '_' + str(startEndOfDayDplfBand) + '_' + str(dplfBand) + '_B_' + howDetermined

        if anchorFare != 0:
            offset = 0
            openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult)
            if rbddata.shape[0] == 0:
                openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop,  openResult)
            else:
                openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop,  openResult, rbddata, howDetermined)
        else:
            openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': [howDetermined], 'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}
        return openResult, lastSoldFare, strategyFare, historicOwnFare, profileFare, upsell

    def finalFareAnchor(self, strategyFare, historicOwnFare, profileFare, lastSoldFare, value, dPLF):
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
            if historicOwnFare != CONST_NA:
                faresList.append(float(historicOwnFare))
            if profileFare != CONST_NA:
                faresList.append(float(profileFare))

            # Check if any values exist
            if faresList:
                max_value = max(faresList)
                
            if max_value == 0:
                anchorFare = strategyFare
                howDetermined = 'AllocationY_StrategyFare_UpsellN'
                upsell = CONST_NA
            else:
                if max_value > strategyFare:
                    howDetermined = 'AllocationY_MaxOwnFare_UpsellY'
                    if(random.choice([True, False])):
                        upsell = max_value * 0.05
                    anchorFare = max_value + upsell
                else:
                    upsell = CONST_NA
                    anchorFare = (max_value + strategyFare) / 2
                    howDetermined = 'AllocationY_UpsoldMaxOwnFare_UpsellN'

        if value == 'B':
            upsell = CONST_NA
            faresList = []
            max_value = 0
            if lastSoldFare != CONST_NA and float(lastSoldFare) < float(strategyFare):
                faresList.append(float(lastSoldFare))
            if historicOwnFare != CONST_NA and float(historicOwnFare) < float(strategyFare):
                faresList.append(float(historicOwnFare))
            if profileFare != CONST_NA and float(profileFare) < float(strategyFare):
                faresList.append(float(profileFare))

            if faresList:
                max_value = max(faresList)

            if max_value == 0:
                anchorFare = float(strategyFare)
                howDetermined = 'AllocationY_StrategyFare_UpsellN'
            else:
                delta = float(strategyFare) - float(max_value)
                howDetermined = 'AllocationY_InterpolatedMaxOwnFare_UpsellN'
                if(float(max_value) <= float(strategyFare)):
                    anchorFare = max_value + (delta / self.constantsutils.OWN_FARE_DELTA) * (dPLF + self.constantsutils.OWN_FARE_DELTA)
                if(dPLF <= (float(self.constantsutils.OWN_FARE_DELTA) * -1)):
                    anchorFare = max_value

        if value == 'C':
            # Skip allocations
            upsell = CONST_NA
            howDetermined = 'AllocationN'
            anchorFare =  0

        return anchorFare, howDetermined, upsell

    def extremeFaresSkipping(self, origin, destin, anchorFare, offset, channel, backstop, openResult):
        self.logger.info("Market Fare is out of price ladder range - allocating at extreme values")
        # Fetch the Lowest or highest available fares
        condition = 'LOW'
        if int(offset) >= 0:
            condition = 'HI'

        defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, condition, backstop)

        # Edge case where backstop is placed at a very large value - allocates at Z0 or Y levels
        if defFareData.shape[0] == 0:
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0) 	

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
        
    def getFaresWithBackstop(self, origin, destin, anchorFare, offset, channel, backstop, openResult, rbddata, howDetermined):
        backstopFlag = False
        extremeFareFlag = False
        # We got back data purely on the basis of anchor fare - comparison with offset remains which is done in the subsequent section

        # Calculating the offset that is required based on entries remaining in the RBD table
        offsetToFetch = min(abs(int(offset)), rbddata.shape[0] - 1)
        if int(offset) < 0:
            offsetToFetch = min(abs(int(offset)+1), rbddata.shape[0] - 1) 

        # Comparison with the backstop table - to ensure we are picking a value higher than the backstop
        defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)

        if defFareData.shape[0] != 0:
            if rbddata['Total'][0] < defFareData['Total'][0] and backstop > 0:
                backstopFlag = True
            
        # Edge case if the backstop values are placed very high - in which case it will place the backstop at Z0 and Y levels 
        if defFareData.shape[0] == 0:
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
            
        if rbddata['RBD'][0] == self.constantsutils.LOWEST_B2B_RBD_VALUE or rbddata['RBD'][0] == self.constantsutils.LOWEST_B2C_RBD_VALUE:
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)
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
            limitB2BFare_Data = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
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
      
