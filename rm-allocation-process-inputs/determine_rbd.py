from datetime import datetime, time, timedelta
import random
import pandas as pd
from Constants import *

class DetermineRBD:
    def __init__(self, rmdatabasehelper, logger,inputsource,constantsutils,navdatabasehelper):
        self.inputSource=inputsource
        self.rmdatabasehelper = rmdatabasehelper
        self.navdatabasehelper = navdatabasehelper
        self.logger = logger
        self.constantsutils=constantsutils

    def determineRBDToOpen(self, anchorFare, offset, channel, currFltDate, dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, plf, currFltDepartureTime, ndoBand, dPLF, tgtBookloadFactor,backstop,currenttime):
        origin = currParams.origin
        destin = currParams.destin
        lastSoldFare = 'NA'
        endOfDayTgtBookedLoad = 'NA'
        expectedEndOfDayDplfBand = 'NA'
        upsell = 'NA'
        bookingToday = 'NA'
        expectedEndOfDayBooking = 'NA'
        strategyFare = 'NA'
        endOfDayBookings = 'NA'
        historicOwnFare = 'NA'
        state = 'NA'
        timeSectorDrop = 'NA'
        gridDrop = 'NA'
        startEndOfDayDplfBand = 'NA'

        openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
        
        howDetermined = ""
        openingFaresFlag = False
        historicFareFlag = False
        openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult)
        if(rbddata is None):
            return openResult, False, lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, upsell, bookingToday, expectedEndOfDayBooking, strategyFare, endOfDayBookings, historicOwnFare, state, timeSectorDrop, gridDrop
        
        if(rbddata.shape[0] == 0):
            openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop,  openResult)
        else:
            openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop,  openResult, rbddata, openingFaresFlag, howDetermined, expectedEndOfDayDplfBand, startEndOfDayDplfBand, ndocounter, plf, historicFareFlag,currenttime)
        strategyFare = openResult['Fare'][0]
        anchorFare = strategyFare
        
        # Historic Fares
        if(currParams.own_fare_flag == YES_FLAG):
            historicOwnFare, state, timeSectorDrop, gridDrop, openResult, historicFareFlag = self.ownFare(anchorFare, channel, backstop, currFltDate, dplfBand, ndocounter, plf, currFltDepartureTime, ndoBand, dPLF, origin, destin, expectedEndOfDayDplfBand, strategyFare, startEndOfDayDplfBand, openResult, openingFaresFlag, historicFareFlag,currenttime,state)

        anchorFare = openResult['Fare'][0]
        anchorState = 'Strategy fare'
        if(anchorFare != strategyFare):
            anchorState = 'Historic Own fare'

        # Last selling fare
        if(int(currParams.opening_fares) == 1):
            self.logger.info("Calucating Last sold fares")
            self.logger.info("Anchor fare is : " + str(anchorFare))
            if(anchorFare != 'NA'):
                #Get Last Selling B2B and B2C Fare
                lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, fares, upsell, timeValue, bookingToday, expectedEndOfDayBooking, startEndOfDayDplfBand = self.getFares(currBookedLoad, currFltDate, currParams, ndocounter, totalCapacity, dplfBand, channel, plf, currFltDepartureTime, tgtBookloadFactor,currenttime)
                endOfDayTgtBookedLoad = endOfDayTgtBookedLoad * totalCapacity
                endOfDayBookings = (float(expectedEndOfDayBooking) / float(totalCapacity)) * 100
                if(len(fares) > 0):
                    if channel == B2C_CHANNEL:
                        openingValue = fares["B2CValue"][0]
                    if channel == B2B_CHANNEL:
                        openingValue = fares["B2BValue"][0]
                    self.logger.info("Booking Velocity value is :" + openingValue)
                    anchorFare, howDetermined, upsell = self.getFinalAnchorFare(openingValue, float(anchorFare), upsell, timeValue, float(lastSoldFare), anchorState)
                    if(anchorFare == 0):
                        if(ndocounter == 0):
                            howDetermined = str(currenttime) + '_Load Factor_' + str(plf) + '_' + howDetermined
                        else:
                            howDetermined = str(currenttime) + '_' + str(startEndOfDayDplfBand) + '_' + str(expectedEndOfDayDplfBand) + '_' + howDetermined
                        openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':[howDetermined],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
                        return openResult, openingFaresFlag, lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, upsell, bookingToday, expectedEndOfDayBooking, strategyFare, endOfDayBookings, historicOwnFare, state, timeSectorDrop, gridDrop
                    openingFaresFlag = True
                    offset = 0
        
                    self.logger.info("New anchor fare is: " + str(anchorFare))

                    openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult)
                    
                    if(rbddata.shape[0] == 0):
                        openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop, openResult)
                    else:
                        openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop, openResult, rbddata, openingFaresFlag, howDetermined, expectedEndOfDayDplfBand, startEndOfDayDplfBand, ndocounter, plf, historicFareFlag,currenttime)
                else:
                    lastSoldFare = 'NA'
                    endOfDayTgtBookedLoad = 'NA'
                    expectedEndOfDayDplfBand = 'NA'
                    upsell = 'NA'
                    bookingToday = 'NA'
                    expectedEndOfDayBooking = 'NA'
                    endOfDayBookings = 'NA'

        return openResult, openingFaresFlag, lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, upsell, bookingToday, expectedEndOfDayBooking, strategyFare, endOfDayBookings, historicOwnFare, state, timeSectorDrop, gridDrop

    def extremeFaresSkipping(self, origin, destin, anchorFare, offset, channel, backstop, openResult):
        self.logger.info("Market Fare is out of price ladder range - allocating at extreme values")
        #Fetch the Lowest or highest available fares
        condition = 'LOW'
        if (int(offset) >= 0):
            condition = 'HI'

        defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, condition, backstop)

        # Edge case where backstop is placed at a very large value - allocates at Z0 or Y levels
        if (defFareData.shape[0] == 0):
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0) 	

        if(defFareData.shape[0] != 0):
            if (channel == B2C_CHANNEL):
                openResult = {'RBD':[defFareData['RBD'][0]], 'Fare':[defFareData['Total'][0]], 'FareResult':['Unable to Match Mkt - B2C allocation at extreme value'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [1]}
                return openResult

            if (channel == B2B_CHANNEL):
                if (defFareData['RBD'][0] == self.constantsutils.HIGHEST_B2B_RBD_VALUE):
                    openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['Market Fare higher than '+ self.constantsutils.HIGHEST_B2B_RBD_VALUE +' levels - skipping B2B allocation'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
                    
                    return openResult
                else:
                    openResult = {'RBD':[defFareData['RBD'][0]], 'Fare':[defFareData['Total'][0]], 'FareResult':['Unable to Match Mkt - B2B allocation at extreme value'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [1]}
                    return openResult
        else:
            openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['Unable to fetch extreme published Fares'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [0]}
            
            return openResult
        
    def getFaresWithBackstop(self, origin, destin, anchorFare, offset, channel, backstop,openResult, rbddata, openingFaresFlag, howDetermined, endOfDayDplfBand, startEndOfDayDplfBand, ndocounter, plf, historicFareFlag,currenttime):
        backstopFlag = False
        extremeFareFlag = False
        # We got back data purely on the basis of anchor fare - comparison with offset remains which is done in the subsequent section

        # Calculating the offset that is required based on entries remaining in the RBD table
        offsetToFetch = min(abs(int(offset)), rbddata.shape[0] - 1)
        if(int(offset) < 0):
            offsetToFetch = min(abs(int(offset)+1), rbddata.shape[0] - 1) 

        # Comparison with the backstop table - to ensure we are picking a value higher than the backstop
        defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)

        if(defFareData.shape[0] != 0):
            if(rbddata['Total'][0] < defFareData['Total'][0] and backstop > 0):
                backstopFlag = True
            
        # Edge case if the backstop values are placed very high - in which case it will place the backstop at Z0 and Y levels 
        if(defFareData.shape[0] == 0):
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
            
        if(rbddata['RBD'][0] == self.constantsutils.LOWEST_B2B_RBD_VALUE or rbddata['RBD'][0] == self.constantsutils.LOWEST_B2C_RBD_VALUE):
            defFareData = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)
            if(len(defFareData) == 0):
                extremeFareFlag = True
                RBDtoOpen = rbddata['RBD'][0]
                FareToOpen = rbddata['Total'][0]
            else:
                if(rbddata['RBD'][0] == defFareData['RBD'][0]):
                    extremeFareFlag = True
                    RBDtoOpen = rbddata['RBD'][0]
                    FareToOpen = rbddata['Total'][0]
                else:
                    backstopFlag = True
                    RBDtoOpen = defFareData['RBD'][0]
                    FareToOpen = defFareData['Total'][0]
        else:
            if(defFareData.shape[0] != 0):
                if(float(defFareData['Total'][0]) >= float(rbddata['Total'][offsetToFetch])):
                    offsetToFetch=min(len(defFareData['Total'])-1,offsetToFetch)
                    if(rbddata['Total'][0] < defFareData['Total'][offsetToFetch] and backstop > 0):
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

        if (channel == B2B_CHANNEL):
            #Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
            limitB2BFare_Data = self.rmdatabasehelper.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
            limitB2BFare = 0
            if(defFareData.shape[0] != 0):
                limitB2BFare = float(limitB2BFare_Data['Total'][0])

            if (float(anchorFare) > limitB2BFare):
                #Outside of B2B Levels - stick with B2C
                openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['Market Fare higher than '+ self.constantsutils.HIGHEST_B2B_RBD_VALUE +' levels - skipping B2B allocation'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
                return openResult

            if(offsetToFetch == rbddata.shape[0]):
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Not Enough Levels available - taking the Max/Min Available'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [1]}			
                return openResult
            if(extremeFareFlag):
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Unable to Match Mkt - B2B allocation at extreme value'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
                return openResult
            if(backstopFlag):
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Matched and allocated - Backstop Hit'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
                return openResult
            if(openingFaresFlag):
                if(ndocounter == 0):
                    howDetermined = str(currenttime) + '_Load Factor_' + str(plf) + '_' + howDetermined
                else:
                    howDetermined = str(currenttime) + '_' + str(startEndOfDayDplfBand) + '_' + str(endOfDayDplfBand) + '_' + howDetermined
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':[howDetermined],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}
                return openResult
            if historicFareFlag:
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':[howDetermined],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
                return openResult
            openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Matched and allocated'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
            return openResult

        else:
            if(offsetToFetch == rbddata.shape[0]):
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Not Enough Levels available - taking the Max/Min Available'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [1]}			
                return openResult
            if(extremeFareFlag):
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Unable to Match Mkt - B2C allocation at extreme value'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
                return openResult
            if(backstopFlag):
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Matched and allocated - Backstop Hit'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
                return openResult
            if(openingFaresFlag):
                if(ndocounter == 0):
                    howDetermined = str(currenttime) + '_Load Factor_' + str(plf) + '_' + howDetermined
                else:
                    howDetermined = str(currenttime) + '_' + str(startEndOfDayDplfBand) + '_' + str(endOfDayDplfBand) + '_' + howDetermined
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':[howDetermined],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
                return openResult
            if historicFareFlag:
                openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':[howDetermined],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
                return openResult
            openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Matched and allocated'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
            return openResult
                            
    def ownFare(self, anchorFare, channel, backstop, currFltDate, dplfBand, ndocounter, plf, currFltDepartureTime, ndoBand, dPLF, origin, destin, expectedEndOfDayDplfBand, strategyFare, startEndOfDayDplfBand, openResult, openingFaresFlag, historicFareFlag,currenttime,state):
        self.logger.info("Calucating Historic fares")
        self.logger.info("Anchor fare is : " + str(anchorFare))    
                #Get past historic fares from table
        currentDate = datetime.strptime(currFltDate, '%Y-%m-%d')
        month = currentDate.strftime('%B')  # Full month name
        day_of_week = currentDate.strftime('%A')  # Full day name
        self.logger.info("Month is: " + month)
        self.logger.info("Day of week is is: " + day_of_week)

        sector = origin + destin
        time_object = datetime.strptime(currFltDepartureTime, "%H:%M").time()
        deptTime = f"{time_object.hour:02d}:{time_object.minute:02d}"
        hour = f"{time_object.hour:02d}"

        hourlyData = self.rmdatabasehelper.getHourlyOwnFareData(ndocounter, hour)
        gridData = self.rmdatabasehelper.getGridOwnFareData(ndoBand, dplfBand, channel)
        historicData = self.rmdatabasehelper.getFareHistoryData(ndocounter, sector, month, day_of_week, channel, deptTime)

        timeSectorDrop = hourlyData['value'][0]
        gridDrop = gridData['value'][0]

        historicOwnFare = 0
        if len(historicData) != 0:
            historicOwnFare = historicData['price'][0]
        
        if(strategyFare == 'NA'):
            anchorFare = historicOwnFare
        else:
            anchorFare, state = self.getHistoricFare(dPLF, strategyFare, timeSectorDrop, gridDrop, historicOwnFare, anchorFare)

        self.logger.info("New anchor fare is: " + str(anchorFare))

        offset = 0
        if(state != 'NA'):
            historicFareFlag = True
            howDetermined = 'HistFare_' + state
            openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult)
                    
            if(rbddata.shape[0] == 0):
                openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop, openResult)
            else:
                openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop, openResult, rbddata, openingFaresFlag, howDetermined, expectedEndOfDayDplfBand, startEndOfDayDplfBand, ndocounter, plf, historicFareFlag,currenttime)
        return historicOwnFare,state,timeSectorDrop,gridDrop,openResult,historicFareFlag
    
    def getHistoricFare(self, dPLF, strategyFare, timeSectorDrop, gridDrop, historicOwnFare, anchorFare):
        state = 'NA'
        self.logger.info("Historic Own fare is: " + str(historicOwnFare))
        if(historicOwnFare != 0):
            if(dPLF>=0):
                if(float(strategyFare) >= float(historicOwnFare)):
                    anchorFare = strategyFare
                    state = 'Adh_StrategyFare'
                else:
                    anchorFare = historicOwnFare
                    state = 'Adh_OwnFare'
            else:
                if(float(strategyFare) < float(historicOwnFare)):
                    anchorFare = strategyFare
                    state = 'Bdh_StrategyFare'
                if(str(timeSectorDrop).upper() == 'YES' and str(gridDrop).upper() == 'YES'):
                    state = 'Bdh_ModOwnFare'
                    delta = float(strategyFare) - float(historicOwnFare)
                    if(float(historicOwnFare) <= float(strategyFare)):
                        anchorFare = historicOwnFare + (delta / self.constantsutils.OWN_FARE_DELTA) * (dPLF + self.constantsutils.OWN_FARE_DELTA)
                    if(dPLF <= (float(self.constantsutils.OWN_FARE_DELTA) * -1)):
                        anchorFare = historicOwnFare
        
        return anchorFare, state
    
    def getFares(self, currBookedLoad, currFltDate, currParams, ndocounter, totalCapacity, dplfBand, channel, plf, currFltDepartureTime, tgtBookloadFactor,currenttime):
        lastSoldFare = 0
        if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
            farePrice = self.rmdatabasehelper.getLastSellingFare(currFltDate, currParams)
            
        if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
            farePrice = self.navdatabasehelper.getLastSellingFare(currFltDate, currParams)

        self.logger.info("Last Sold Price fetched")
        if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
            bookings = self.rmdatabasehelper.getNumberOfBookings(currFltDate, currParams)
            
        if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
            bookings = self.navdatabasehelper.getNumberOfBookings(currFltDate, currParams)
        self.logger.info("Number of bookings fetched fetched")
        if(farePrice.shape[0] == 0):
            lastSoldFare = 0
        else:
            if channel == B2C_CHANNEL:
                lastSoldFare = farePrice["B2Cfare"][0]
            if channel ==B2B_CHANNEL:
                lastSoldFare = farePrice["B2Bfare"][0]
        if(bookings.shape[0] == 0):
            lastBookloadTillMidnight = 0
        else:
            lastBookloadTillMidnight = bookings["BookingsCount"][0]

        self.logger.info("Last Selling Fare is "+ str(lastSoldFare))
        self.logger.info("Number of Bookings is "+ str(lastBookloadTillMidnight))
        departureTime = currFltDepartureTime

        curTime = datetime.strptime(str(currenttime), TIME_FORMAT)
        deptTime = datetime.strptime(str(departureTime), TIME_FORMAT)

        area = self.rmdatabasehelper.getArea(curTime.hour)

        self.logger.info("Area is:"+ str(area["sum"][0]))
        self.logger.info("current Run Time is:"+ str(curTime))
        self.logger.info("Flight Departure Time is:"+ str(deptTime))

        bookingToday = currBookedLoad - lastBookloadTillMidnight
        expectedEndOfDayBooking = lastBookloadTillMidnight + ((bookingToday / area["sum"][0]) * 100)
        endOfDayTgtBookedLoadList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(ndocounter - 1, currParams.curve_id)
        endOfDayTgtBookedLoad = float(endOfDayTgtBookedLoadList['LF'][0])
        expectedEndOfDaydPLF = ((expectedEndOfDayBooking/totalCapacity) - endOfDayTgtBookedLoad) * 100
        expectedEndOfDayDplfBand = self.rmdatabasehelper.getdpflBand(expectedEndOfDaydPLF)
        
        startOfdayFactor = ((lastBookloadTillMidnight/totalCapacity) - tgtBookloadFactor) * 100
        startEndOfDayDplfBand = self.rmdatabasehelper.getdpflBand(startOfdayFactor)

        expectedEndOfDayDplfBand = self.rmdatabasehelper.getdpflBand(expectedEndOfDaydPLF)
        self.logger.info("endOfDayTgtBookedLoad is: "+ str(endOfDayTgtBookedLoad))
        self.logger.info("expectedEndOfDaydPLF is: "+ str(expectedEndOfDaydPLF))
        self.logger.info("expectedEndOfDayDplfBand is: "+ str(expectedEndOfDayDplfBand))
        self.logger.info("startEndOfDayDplfBand is: "+ str(startEndOfDayDplfBand))

        if(ndocounter==0):
            noon = time(12, 0)
            if(deptTime.time() > noon):
                hours = (deptTime - timedelta(hours=2)).hour
                areaTillDeparture = self.rmdatabasehelper.getArea(hours)
                expectedEndOfDayBooking = lastBookloadTillMidnight + ((bookingToday * areaTillDeparture["sum"][0]) / area["sum"][0])
                plf = (currBookedLoad/totalCapacity) * 100
        
        self.logger.info("expectedEndOfDayBooking is: "+ str(expectedEndOfDayBooking))

        curTime = curTime.time().strftime(TIME_FORMAT)
        deptTime = deptTime.time().strftime(TIME_FORMAT)

        fares = self.rmdatabasehelper.getOpeningFareValue(dplfBand, expectedEndOfDayDplfBand, curTime, ndocounter, plf, deptTime)

        upsell = 0
        timeValue = 0
        if(random.choice([True, False])):
            upsell = lastSoldFare * 0.05
        if(random.choice([True, False, False, False])):
            timeValue = lastSoldFare * 0.025

        return lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, fares, upsell, timeValue, bookingToday, expectedEndOfDayBooking, startEndOfDayDplfBand
    
    def getFinalAnchorFare(self, anchorValue, anchorFare, upsell, timeValue, lastSoldFare, anchorState):
        anchorFares = 0
        howDetermined = ''
        if(anchorValue == 'A'):
            anchorFares =  max(anchorFare, lastSoldFare)
            if(anchorFare > lastSoldFare):
                howDetermined = 'UpsellN_AllocationY_Fare based from ' + anchorState
                upsell = 'NA'
            else:
                anchorFares = anchorFares + upsell
                howDetermined = 'UpsellY_AllocationY_Fare based from last sold Fare '
        if(anchorValue == 'B'):
            anchorFares =  min(anchorFare, lastSoldFare)
            howDetermined = 'UpsellN_AllocationY_'
            if(anchorFare < lastSoldFare):
                howDetermined = howDetermined + 'Fare based from ' + anchorState
            else:
                howDetermined = howDetermined + 'Fare based from last sold Fare '
            if(lastSoldFare == 0):
                anchorFares = anchorFare
                howDetermined = howDetermined + 'Fare based from ' + anchorState
            upsell = 'NA'
        if(anchorValue == 'C'):
            anchorFares =  anchorFare
            howDetermined = 'UpsellN_AllocationY_Fare based from ' + anchorState
            upsell = 'NA'
        if(anchorValue == 'D'):
            anchorFares =  0
            howDetermined = 'UpsellN_AllocationN_Allocations put as 0 '
            upsell = 'NA'
        if(anchorValue == 'E'):
            anchorFares =  lastSoldFare + upsell
            howDetermined = 'UpsellY_AllocationY_Fare based from last sold Fare '
        if(anchorValue == 'F'):
            anchorFares =  min(anchorFare, lastSoldFare)
            howDetermined = ''
            if(anchorFare < lastSoldFare):
                howDetermined = 'UpsellN_AllocationY_Fare based from ' + anchorState
                upsell = 'NA'
            else:
                anchorFares = anchorFares + timeValue
                howDetermined = 'UpsellY_AllocationY_Fare based from last sold Fare '
                upsell = timeValue
            if(lastSoldFare == 0):
                anchorFares = anchorFare
                howDetermined = howDetermined + 'Fare based from ' + anchorState
                upsell = 'NA'
        if(anchorValue == 'G'):
            anchorFares =  min(anchorFare, lastSoldFare)
            delta=abs(anchorFare-lastSoldFare)
            howDetermined = 'UpsellN_AllocationY_Fare based from ' + anchorState
            if((delta/anchorFares)>0.7):
                anchorFares=anchorFares+0.5*delta
                howDetermined = 'UpsellN_AllocationY_Fare based from modified_last sold Fare'
                upsell = 'NA'
        
        return anchorFares, howDetermined, upsell
