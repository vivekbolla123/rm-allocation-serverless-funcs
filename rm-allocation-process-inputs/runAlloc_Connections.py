import copy
import json
import math
import traceback

import numpy as np
import Constants
import os
import warnings

import pandas as pd
import time as time_sleep

from Constants import *

from helper.rm_mongo_helper import MarketFareMongoDB
from helper.nav_api_helper import NavAPIHelper
from helper.redis_helper import RedisHelper
from helper.rm_database_helper import RMDatabaseHelper

from utilities.utils import Utils
from utilities.math_utils import MathUtils
from utilities.time_utils import TimeUtils
from constants_utils import ConstantsUtils

from models.input_source import InputSource
from models.output_summary_connections import OutputSummaryConnections
from models.au_grid_row import AuGridRow

from ownlogger import OwnLogger
from allocations import RBDAllocations
from determine_rbd import DetermineRBD

from datetime import datetime, timedelta
from models.market_list_connections_row import MarketListConnectiosnsRow
from models.input_source import InputSource
from market_fares_service import MarketFareService
warnings.filterwarnings('ignore')


class DynAllocConnections:

    def __init__(self, inputData, currParams):
        self._initialize_params(inputData, currParams)
        # Start the Allocation
        if (currParams["runSingleMarket"]):
            self.runDynamicAllocationSingleMarket(1)
        else:
            self.runDynamicAllocation()

    def runDynamicAllocation(self):
        print("Starting Dynamic Allocation")
        self.mktList = self.rmdatabasehelper.loadRunParams()
        startTime = self.timeutils.getCurrentDateTime()
        username = os.getlogin()
        self.rmdatabasehelper.insertRunStart(startTime, username)

        # The list of markets to run the allocation
        numMarkets = self.mktList.shape[0]
        print("Market List has " + str(numMarkets) + " Rows")

        if (self.debugMode):
            print("Market List : ")
            print(self.mktList)

        for mktCounter in range(numMarkets):
            self.currParams = MarketListConnectiosnsRow(
                self.mktList.iloc[mktCounter])
            self.runDynamicAllocationSingleMarket(mktCounter)

        self.outputResults()

        endTime = self.timeutils.getCurrentDateTime()
        self.rmdatabasehelper.updateRunEnd(endTime)
        runTime = (endTime - startTime).total_seconds()
        print("Allocation Completed in " + str(runTime) + " seconds")

    def runDynamicAllocationSingleMarket(self, mtkIndex):
        self.qpcurrency="INR"
        self.conversion_data = self.rmdatabasehelper.fetch_all_conversion_data()
        print("**********************************************")
        print("Starting Allocation for row : " + str(mtkIndex))
        self.redishelper.updateRunAuditReady()

        if (self.debugMode):
            print(self.currParams)

        # Convert NDO to run dates as necessary
        currDate = self.timeutils.getCurrentDate()
        startDate = currDate
        endDate = currDate
        startNDO = self.currParams.per_start
        endNDO = self.currParams.per_end
        sector1 = self.currParams.sector1
        sector2 = self.currParams.sector2
        flightNo2 = self.currParams.flight2
        flightNo1 = self.currParams.flight1

        startDate, endDate, startNDO, endNDO = self.getStartEndDate(
            currDate, startNDO, endNDO)

        print("Allocation for " + sector1 + " " + sector2 +
              " from " + str(startDate) + " to " + str(endDate))
        self.wait_for_inputs()
        startDateStr = datetime.strftime(
                    startDate , DATE_FORMAT_2)
        endDateStr = datetime.strftime(
                    endDate , DATE_FORMAT_2)
        
        startDate2Str = self.currParams.SecondFlightDepartureDate
        endDate2Str = self.currParams.SecondFlightDepartureDate
        if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
            startDate2Str = self.rmdatabasehelper.get_second_flight_dep(
                self.currParams.flight1, self.currParams.flight2, self.currParams.sector1[:3], self.currParams.sector2[3:], startDateStr)
            endDate2Str = self.rmdatabasehelper.get_second_flight_dep(
                self.currParams.flight1, self.currParams.flight2, self.currParams.sector1[:3], self.currParams.sector2[3:], endDateStr)
        if startDate2Str=="" or endDate2Str=="":
            self.outputSummaryRow_b2c = OutputSummaryConnections()
            print("Unable to get the DepDate of SecondFlight invalid connection- Aborting")
            self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
            self.outputSummaryRow_b2c.RunId = self.runId
            self.outputSummaryRow_b2c.Sector1 = sector1
            self.outputSummaryRow_b2c.Sector2 = sector2
            self.outputSummaryRow_b2c.FltNum1 = str(flightNo1)
            self.outputSummaryRow_b2c.FltNum2 = str(flightNo2)
            self.outputSummaryRow_b2c.DepDate1 = str(self.currParams.per_start)
            self.outputSummaryRow_b2c.DepDate2 = CONST_NA
            self.outputSummaryRow_b2c.HowDetermined = HD_INVALID_CONNECTION
            self.outputSummaryRow_b2c.analystName = self.currParams.analyst_name
            self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
            self.outputSummaryRow_b2b = copy.deepcopy(
                self.outputSummaryRow_b2c)
            self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
            self.outputSummaryRow_b2b.Channel = B2B_CHANNEL
            self.allocResults.append(self.outputSummaryRow_b2c)
            self.rmdatabasehelper.insertSummaryRow_connections(
                self.allocResults)
            self.allocResults.append(self.outputSummaryRow_b2b)
            self.rmdatabasehelper.insertSummaryRow_connections(
                self.allocResults)
            self.outputSummaryRow_b2c.allocationStatus = 0
            self.outputSummaryRow_b2b.allocationStatus = 0
            self.redishelper.updateRunAudit()
            return
        startDate2=datetime.strptime(startDate2Str,DATE_FORMAT_2).date()
        endDate2=datetime.strptime(endDate2Str,DATE_FORMAT_2).date()
        bookedLoad_sector1 = self.currParams.BookedPax1
        bookedLoad_sector2 = self.currParams.BookedPax2
        

        try:
            for ndocounter in range(startNDO, endNDO+1):
                self.outputSummaryRow_b2c = OutputSummaryConnections()
                currDate = self.timeutils.getCurrentDate()
                tgtDate = datetime.strftime(
                    currDate + timedelta(days=ndocounter), DATE_FORMAT_2)

                bookedLoadSingleRowSector1 = bookedLoad_sector1
                tgdDate2 = self.currParams.SecondFlightDepartureDate
                if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
                    tgdDate2 = self.rmdatabasehelper.get_second_flight_dep(
                        self.currParams.flight1, self.currParams.flight2, self.currParams.sector1[:3], self.currParams.sector2[3:], tgtDate)


                cdf = self.rmdatabasehelper.get_curveids(
                    self.currParams.sector1, self.currParams.sector2, self.currParams.flight1, self.currParams.flight2, tgtDate, tgdDate2)
                if (len(cdf)) < 2:
                    dateFormatted1 = datetime.strptime(
                        str(startDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
                    dateFormatted2 = datetime.strptime(
                        str(startDate2), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
                    print("Unable to get the CurevId - Aborting")
                    self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
                    self.outputSummaryRow_b2c.RunId = self.runId
                    self.outputSummaryRow_b2c.Sector1 = sector1
                    self.outputSummaryRow_b2c.Sector2 = sector2
                    self.outputSummaryRow_b2c.FltNum1 = str(flightNo1)
                    self.outputSummaryRow_b2c.FltNum2 = str(flightNo2)
                    self.outputSummaryRow_b2c.DepDate1 = str(dateFormatted1)
                    self.outputSummaryRow_b2c.DepDate2 = str(dateFormatted2)
                    self.outputSummaryRow_b2c.HowDetermined = HD_NO_REF_CURVE_ID
                    self.outputSummaryRow_b2c.analystName = self.currParams.analyst_name
                    self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
                    self.outputSummaryRow_b2b = copy.deepcopy(
                        self.outputSummaryRow_b2c)
                    self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
                    self.outputSummaryRow_b2b.Channel = B2B_CHANNEL
                    self.allocResults.append(self.outputSummaryRow_b2c)
                    self.rmdatabasehelper.insertSummaryRow_connections(
                        self.allocResults)
                    self.allocResults.append(self.outputSummaryRow_b2b)
                    self.rmdatabasehelper.insertSummaryRow_connections(
                        self.allocResults)
                    self.outputSummaryRow_b2c.allocationStatus = 0
                    self.outputSummaryRow_b2b.allocationStatus = 0
                    self.redishelper.updateRunAudit()
                    break
                else:
                    CurveId1 = cdf["CurveID"][0]
                    CurveId2 = cdf["CurveID"][1]
                    date2=datetime.strptime(tgdDate2, DATE_FORMAT_2).date()
                    tgtBookedLoad1 = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(
                        ndocounter, CurveId1)
                    tgtBookedLoad2 = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(
                        (date2-currDate).days, CurveId2)
                    if (len(tgtBookedLoad1) == 0 or len(tgtBookedLoad2) == 0):
                        dateFormatted1 = datetime.strptime(
                        str(startDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
                        dateFormatted2 = datetime.strptime(
                        str(startDate2), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
                        print("No Reference Curve Found - Aborting")
                        self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
                        self.outputSummaryRow_b2c.RunId = self.runId
                        self.outputSummaryRow_b2c.Sector1 = sector1
                        self.outputSummaryRow_b2c.Sector2 = sector2
                        self.outputSummaryRow_b2c.FltNum1 = str(flightNo1)
                        self.outputSummaryRow_b2c.FltNum2 = str(flightNo2)
                        self.outputSummaryRow_b2c.DepDate1 = str(dateFormatted1)
                        self.outputSummaryRow_b2c.DepDate2 = str(dateFormatted2)
                        self.outputSummaryRow_b2c.HowDetermined = HD_NO_REF_CURVE
                        self.outputSummaryRow_b2c.analystName = self.currParams.analyst_name
                        self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
                        self.outputSummaryRow_b2b = copy.deepcopy(
                            self.outputSummaryRow_b2c)
                        self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
                        self.outputSummaryRow_b2b.Channel = B2B_CHANNEL
                        self.allocResults.append(self.outputSummaryRow_b2c)
                        self.rmdatabasehelper.insertSummaryRow_connections(
                            self.allocResults)
                        self.allocResults.append(self.outputSummaryRow_b2b)
                        self.rmdatabasehelper.insertSummaryRow_connections(
                            self.allocResults)
                        self.outputSummaryRow_b2c.allocationStatus = 0
                        self.outputSummaryRow_b2b.allocationStatus = 0
                        self.redishelper.updateRunAudit()
                        break

                    else:
                        tgtBookedLoad1 = float(tgtBookedLoad1['LF'][0])
                        tgtBookedLoad2 = float(tgtBookedLoad2['LF'][0])

                bookedLoadSingleRowSector2 = bookedLoad_sector2

                if (pd.isna(bookedLoadSingleRowSector1) or pd.isna(bookedLoadSingleRowSector2) ):
                    dateFormatted1 = datetime.strptime(
                        str(startDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
                    dateFormatted2 = datetime.strptime(
                        str(startDate2), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
                    print("Unable to get the Booked Load - Aborting")
                    self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
                    self.outputSummaryRow_b2c.RunId = self.runId
                    self.outputSummaryRow_b2c.Sector1 = sector1
                    self.outputSummaryRow_b2c.Sector2 = sector2
                    self.outputSummaryRow_b2c.FltNum1 = str(flightNo1)
                    self.outputSummaryRow_b2c.FltNum2 = str(flightNo2)
                    self.outputSummaryRow_b2c.DepDate1 = str(dateFormatted1)
                    self.outputSummaryRow_b2c.DepDate2 = str(dateFormatted2)
                    
                    self.outputSummaryRow_b2c.HowDetermined = HD_NO_BOOKLOADS
                    self.outputSummaryRow_b2c.analystName = self.currParams.analyst_name
                    self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
                    self.outputSummaryRow_b2b = copy.deepcopy(
                        self.outputSummaryRow_b2c)
                    self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
                    self.outputSummaryRow_b2b.Channel = B2B_CHANNEL
                    self.allocResults.append(self.outputSummaryRow_b2c)
                    self.rmdatabasehelper.insertSummaryRow_connections(
                        self.allocResults)
                    self.allocResults.append(self.outputSummaryRow_b2b)
                    self.rmdatabasehelper.insertSummaryRow_connections(
                        self.allocResults)
                    self.outputSummaryRow_b2c.allocationStatus = 0
                    self.outputSummaryRow_b2b.allocationStatus = 0
                    self.redishelper.updateRunAudit()

                    break
                else:
                    self.allocateAUforflights(
                        tgtDate,tgdDate2 ,bookedLoadSingleRowSector1, bookedLoadSingleRowSector2, ndocounter, tgtBookedLoad1, tgtBookedLoad2)

        except Exception as e:
            print("Exception occurred")
            traceback.print_exc()
            print(e)

        if not self.inputSource.isUpdateNavitaire:
            self.outputResults()

        if (self.inputSource.isUpdateNavitaire) and (self.NavitaireUpdateMethod == Constants.CONST_SFTP):
            update_query = " UPDATE run_flight_date_audit SET status = %s WHERE runId = %s and status = 'Pending' "
            self.wrconn.execute(update_query, "Ready", self.runId)

    def allocateAUforflights(self, current_date1,current_date2, bookedLoadSingleRowSector1, bookedLoadSingleRowSector2, ndocounter, tgtBookedLoad1, tgtBookedLoad2):
        self.outputSummaryRow_b2b = OutputSummaryConnections()
        self.outputSummaryRow_b2c = OutputSummaryConnections()
        self.outputSummaryRow_b2c.Bookedload1 = bookedLoadSingleRowSector1
        self.outputSummaryRow_b2c.Bookedload2 = bookedLoadSingleRowSector2
        self.outputSummaryRow_b2b.Bookedload1 = bookedLoadSingleRowSector1
        self.outputSummaryRow_b2b.Bookedload2 = bookedLoadSingleRowSector2
        tgtBookedLoad1 = tgtBookedLoad1 * \
            self.currParams.Lid1
        tgtBookedLoad2 = tgtBookedLoad2 * \
            self.currParams.Lid2
        sector1 = self.currParams.sector1
        sector2 = self.currParams.sector2
        flight1 = self.currParams.flight1
        flight2 = self.currParams.flight2
        forecasted_plf_sector1 = self.currParams.forecastPLF1
        forecasted_plf_sector2 = self.currParams.forecastPLF2
        forecasted_varience_sector1 = self.currParams.forecast_variance1
        forecasted_varience_sector2 = self.currParams.forecast_variance2
        is_sector1_domestic, is_sector2_domestic = self.rmdatabasehelper.check_sector_types(sector1, sector2)
        domestic_plf_threshold = self.constantsUtils.DOMESTIC_PLF_THRESHOLD
        international_plf_threshold = self.constantsUtils.INTERNATIONAL_PLF_THRESHOLD
        dow = self.currParams.dow
        B2BBackstop = self.currParams.b2b_backstop
        B2CBackstop = self.currParams.b2c_backstop
        priceStrategy = self.currParams.pricestartegy
        discountValue = self.currParams.discountvalue
        SkippingFactor = self.currParams.skipping_factor
        b2bRunId = self.utils.getUniqueId()
        b2cRunId = self.utils.getUniqueId()
        B2BBackstop_inr = B2BBackstop
        B2CBackstop_inr = B2CBackstop
        if self.currParams.currency!="INR":
            B2BBackstop = self.convert_currency(self.currParams.currency,'INR', self.currParams.b2b_backstop)
            B2BBackstop_inr = B2BBackstop
            B2CBackstop = self.convert_currency(self.currParams.currency,'INR', self.currParams.b2c_backstop)
            B2CBackstop_inr = B2CBackstop
            if int(priceStrategy)==1:
                discountValue=self.convert_currency(self.currParams.currency,'INR', self.currParams.discountvalue)

        print("Determining pricing for NDO " +
              str(ndocounter) + " on  " + str(current_date1))
        dow = int(datetime.strftime(datetime.strptime(
            current_date1, '%Y-%m-%d') , "%w"))
        
        should_allocate_to_y = False
        apply_domestic_fare_logic = False
        use_sector1_fare = False
        use_sector2_fare = False


        if is_sector1_domestic and not is_sector2_domestic:
            # Sector1 is domestic, Sector2 is international
            if forecasted_plf_sector1 >= domestic_plf_threshold and forecasted_plf_sector2 >= international_plf_threshold:
                should_allocate_to_y = True
            elif forecasted_plf_sector1 >= domestic_plf_threshold:
                # Only domestic leg meets PLF threshold - use domestic fare logic
                apply_domestic_fare_logic = True
                use_sector1_fare = True
        elif not is_sector1_domestic and is_sector2_domestic:
            # Sector1 is international, Sector2 is domestic  
            if forecasted_plf_sector1 >= international_plf_threshold and forecasted_plf_sector2 >= domestic_plf_threshold:
                should_allocate_to_y = True
            elif forecasted_plf_sector2 >= domestic_plf_threshold:
                # Only domestic leg meets PLF threshold - use domestic fare logic
                apply_domestic_fare_logic = True
                use_sector2_fare = True
                
        if int(self.currParams.sosFlag) == 1:
            should_allocate_to_y = True
        # elif is_sector1_domestic and is_sector2_domestic:
        #     # Both domestic
        #     if forecasted_plf_sector1 >= domestic_plf_threshold and forecasted_plf_sector2 >= domestic_plf_threshold:
        #         should_allocate_to_y = True
        # else:
        #     # Both international
        #     if forecasted_plf_sector1 >= international_plf_threshold and forecasted_plf_sector2 >= international_plf_threshold:
        #         should_allocate_to_y = True


        if (str(self.currParams.dow)[dow:(dow+1)] == "1" ):
            bookload1 = bookedLoadSingleRowSector1
            bookload2 = bookedLoadSingleRowSector2
            minLid = min(
                self.currParams.Lid1,self.currParams.Lid2)
            bookload = max(
                bookload1 - (self.currParams.Lid1 - minLid), bookload2 - (self.currParams.Lid2 - minLid))
            print("Combined Booked Load for " + str(flight1)+" "+str(
                flight2) + " on " + str(current_date1) + " is :" + str(bookload))
            
            b2b_rbd_sector1 = self.currParams.OpeningRBDB2B1
            b2c_rbd_sector1 = self.currParams.OpeningRBDB2C1
            b2b_rbd_sector2 = self.currParams.OpeningRBDB2B2
            b2c_rbd_sector2 = self.currParams.OpeningRBDB2C2
            origin = sector1[:3]
            destin = sector2[-3:]
            qpcurrency = self.rmdatabasehelper.get_qp_currency(origin,destin)
            if len(qpcurrency)!=0:
                self.qpcurrency = qpcurrency['currency'][0]
            else:
                self.qpcurrency=''

            if (b2c_rbd_sector1 == CONST_NA or pd.isna(b2c_rbd_sector1) or b2c_rbd_sector2 == CONST_NA or pd.isna(b2c_rbd_sector2) or  self.qpcurrency==''):
                # unable to get bookload aborting
                lowFare="NA"
                lowAirlineCode="NA"
                lowFilghtNum="NA"
                hiFare="NA"
                hiAirlineCode="NA"
                hiFilghtNum="NA"
                self.outputSummaryRow_b2c.Backstop = B2CBackstop
                self.outputSummaryRow_b2b.Backstop = B2BBackstop
                self.outputSummaryRow_b2c.Backstop_INR = B2CBackstop_inr
                self.outputSummaryRow_b2b.Backstop_INR = B2BBackstop_inr
                openDataB2C = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': [
                    'BaseLeg B2C SellingFare NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
                openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': [
                    'BaseLeg B2B SellingFare NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
                if b2c_rbd_sector1 != CONST_NA and not pd.isna(b2c_rbd_sector1):
                    self.outputSummaryRow_b2c.OpenRBD1 = b2c_rbd_sector1
                    SellingFare1,currency1 = self.rmdatabasehelper.getSellingPrice(
                        b2c_rbd_sector1, sector1, "B2C", "L")
                    if currency1!='INR':
                        SellingFare1=self.convert_currency(currency1,'INR',SellingFare1)
                    self.outputSummaryRow_b2c.SellingFare1 = SellingFare1
                    if self.outputSummaryRow_b2c.SellingFare1 == 0:
                        openDataB2C['FareResult'] = "QP Fares Not Available"
                if b2c_rbd_sector2 != CONST_NA and not pd.isna(b2c_rbd_sector2):
                    self.outputSummaryRow_b2c.OpenRBD2 = b2c_rbd_sector2
                    SellingFare2,currency2 = self.rmdatabasehelper.getSellingPrice(
                        b2c_rbd_sector2, sector2, "B2C", "L")
                    if currency2!='INR':
                        SellingFare2=self.convert_currency(currency2,'INR',SellingFare2)
                    self.outputSummaryRow_b2c.SellingFare2 = SellingFare2
                    if self.outputSummaryRow_b2c.SellingFare2 == 0:
                        openDataB2C['FareResult'] = "QP Fares Not Available"
            else:
                self.outputSummaryRow_b2c.OpenRBD1 = b2c_rbd_sector1
                self.outputSummaryRow_b2c.OpenRBD2 = b2c_rbd_sector2
                b2cSectorTotal,SellingFare1, SellingFare2,currency1,currency2 = self.rmdatabasehelper.getFinalPrice(
                    b2c_rbd_sector1, b2c_rbd_sector2, sector1, sector2, "B2C")
                if currency1!='INR':
                    SellingFare1=self.convert_currency(currency1,'INR',SellingFare1)
                if currency2!='INR':
                    SellingFare2=self.convert_currency(currency2,'INR',SellingFare2) 
                b2cSectorTotal = SellingFare1+SellingFare2
                    
                self.outputSummaryRow_b2c.SellingFare1 = SellingFare1
                self.outputSummaryRow_b2c.SellingFare2 = SellingFare2
                
                lowFare, hiFare, lowAirlineCode, hiAirlineCode, lowFilghtNum, hiFilghtNum, isMarketFareAvailable, criteria,lowmktFile,himktFile = self.marketfaresservice.getMarketFareAnchorFareConnections(
                    B2C_CHANNEL, self.currParams, current_date1, self.currParams.outboundstop, self.currParams.outbound_duration, self.currParams.fareAnchor,self.inputSource,self.convert_currency)
                self.outputSummaryRow_b2c.MktFare_Min=lowFare
                self.outputSummaryRow_b2c.AirlineCode_Min=lowAirlineCode
                self.outputSummaryRow_b2c.FlightNumber_Min=lowFilghtNum
                self.outputSummaryRow_b2c.MktFare_Max=hiFare
                self.outputSummaryRow_b2c.AirlineCode_Max=hiAirlineCode
                self.outputSummaryRow_b2c.FlightNumber_Max=hiFilghtNum
                self.outputSummaryRow_b2c.mktFile=himktFile
                fare2 = hiFare
                if criteria == MINO_FARE_ANCHOR or criteria == MIN_FARE_ANCHOR:
                    fare2 = lowFare
                    self.outputSummaryRow_b2c.mktFile=lowmktFile
                b2cdiscount = "NA"
                b2cTotal = "NA"
                strategyFare=0
                self.fareStatus = ""
                b2cdiscountedFare, b2cdiscount = self.getTotalPriceForConnections(
                        priceStrategy, discountValue, b2cSectorTotal)
                self.outputSummaryRow_b2c.discountedFare=b2cdiscountedFare
                fare1 = b2cdiscountedFare
                    
                openResult = pd.DataFrame(
            columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
                strfare=fare2 
                self.outputSummaryRow_b2c.variance1=(bookload1-tgtBookedLoad1)*100/self.currParams.Lid1
                self.outputSummaryRow_b2c.variance2=(bookload2-tgtBookedLoad2)*100/self.currParams.Lid2
                self.outputSummaryRow_b2b.variance1=(bookload1-tgtBookedLoad1)*100/self.currParams.Lid1
                self.outputSummaryRow_b2b.variance2=(bookload2-tgtBookedLoad2)*100/self.currParams.Lid2
                
                self.outputSummaryRow_b2c.ImputedForecast1 = self.outputSummaryRow_b2b.ImputedForecast1 = self.getParsedImputedForecast(self.outputSummaryRow_b2c.variance1, forecasted_plf_sector1, forecasted_varience_sector1)
                self.outputSummaryRow_b2c.ImputedForecast2 = self.outputSummaryRow_b2b.ImputedForecast2 = self.getParsedImputedForecast(self.outputSummaryRow_b2c.variance2, forecasted_plf_sector2, forecasted_varience_sector2)
                
                offset=self.currParams.offset
                if self.qpcurrency!='INR':
                    B2CBackstop = self.convert_currency('INR',self.qpcurrency,B2CBackstop)
                    B2BBackstop = self.convert_currency('INR',self.qpcurrency,B2BBackstop)
                if self.qpcurrency!='INR':
                    strfare=self.convert_currency('INR',self.qpcurrency,float(strfare))
                rbddata=self.getStrategyFare(
                    strfare, origin, destin, "B2C", B2CBackstop,offset)
                if rbddata!='NA':
                    self.outputSummaryRow_b2c.conversionRate=0
                    if self.qpcurrency!='INR':
                        self.outputSummaryRow_b2c.strategyfare=self.convert_currency(self.qpcurrency,'INR',float(rbddata))
                    else:
                      self.outputSummaryRow_b2c.strategyfare = rbddata
                if self.outputSummaryRow_b2c.strategyfare != "NA":
                    strategyFare= float(self.outputSummaryRow_b2c.strategyfare)
                if str(self.currParams.discountflag) == "1":
                    self.fareStatus = " - DiscountedFare"
                    b2cTotal=b2cdiscountedFare
                elif isMarketFareAvailable:
                    self.fareStatus = " - MarketFare"
                    if (bookload1 > tgtBookedLoad1 and bookload2 > tgtBookedLoad2):
                        b2cTotal = max(fare1, float(strategyFare))
                    else:
                        b2cTotal = min(fare1, float(strategyFare))
                    if apply_domestic_fare_logic and use_sector1_fare:
                        b2cTotal = max(SellingFare1,float(strategyFare))
                    if apply_domestic_fare_logic and use_sector2_fare:
                        b2cTotal = max(SellingFare2,float(strategyFare))
                self.outputSummaryRow_b2c.anchorFare=b2cTotal
                self.outputSummaryRow_b2c.Discount = b2cdiscount
                self.outputSummaryRow_b2c.currency=self.qpcurrency
                self.outputSummaryRow_b2b.currency=self.qpcurrency
                
                if b2cTotal=="NA":
                    self.outputSummaryRow_b2c.Backstop = B2CBackstop
                    self.outputSummaryRow_b2b.Backstop = B2BBackstop
                    self.outputSummaryRow_b2c.Backstop_INR = B2CBackstop_inr
                    self.outputSummaryRow_b2b.Backstop_INR = B2BBackstop_inr
                    openDataB2C = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': [
                        'Market Fares Not Available'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
                else:
                    openDataB2C = self.determineRBDToOpen(
                    b2cTotal, origin, destin, "B2C", B2CBackstop,0,should_allocate_to_y)
                
                if (b2b_rbd_sector1 == CONST_NA or pd.isna(b2b_rbd_sector1) or b2b_rbd_sector2 == CONST_NA or pd.isna(b2b_rbd_sector2)):
                    # unable to get bookload aborting
                    openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': [
                        'BaseLeg B2B SellingFare NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
                    if b2b_rbd_sector1 != CONST_NA and not pd.isna(b2b_rbd_sector1):
                        self.outputSummaryRow_b2b.OpenRBD1 = b2b_rbd_sector1
                        SellingFare1,currency1 = self.rmdatabasehelper.getSellingPrice(
                            b2b_rbd_sector1, sector1, "B2B", "L")
                        if currency1!='INR':
                            SellingFare1=self.convert_currency(currency1,'INR',SellingFare1)
                        self.outputSummaryRow_b2b.SellingFare1 = SellingFare1
                        if self.outputSummaryRow_b2b.SellingFare1 == 0:
                            openDataB2B['FareResult'] = "QP Fares Not Available"
                    if b2b_rbd_sector2 != CONST_NA and not pd.isna(b2b_rbd_sector2):
                        self.outputSummaryRow_b2b.OpenRBD2 = b2b_rbd_sector2
                        SellingFare2,currency2 = self.rmdatabasehelper.getSellingPrice(
                            b2b_rbd_sector2, sector2, "B2B", "L")
                        if currency2!='INR':
                            SellingFare2=self.convert_currency(currency2,'INR',SellingFare2)
                        self.outputSummaryRow_b2b.SellingFare2 = SellingFare2
                        if self.outputSummaryRow_b2b.SellingFare2 == 0:
                            openDataB2B['FareResult'] = "QP Fares Not Available"
                else:
                    self.outputSummaryRow_b2b.OpenRBD1 = b2b_rbd_sector1
                    self.outputSummaryRow_b2b.OpenRBD2 = b2b_rbd_sector2
                    if (b2cTotal == 0 ):
                        openDataB2C = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': [
                            'B2C Fares NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
                        openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': [
                            'B2B Fares NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
                    elif  b2b_rbd_sector1=='' or b2b_rbd_sector2=='':
                        openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': [
                            'B2B Opening NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
                    else:
                        b2bSectorTotal, SellingFare1, SellingFare2,currency1,currency2 = self.rmdatabasehelper.getFinalPrice(
                            b2b_rbd_sector1, b2b_rbd_sector2, sector1, sector2, "B2B")
                        if currency1!='INR':
                            SellingFare1=self.convert_currency(currency1,'INR',SellingFare1)
                        if currency2!='INR':
                            SellingFare2=self.convert_currency(currency2,'INR',SellingFare2) 
                        b2bSectorTotal = SellingFare1+SellingFare2
                         
                        self.outputSummaryRow_b2b.SellingFare1 = SellingFare1
                        self.outputSummaryRow_b2b.SellingFare2 = SellingFare2
                        lowFare, hiFare, lowAirlineCode, hiAirlineCode, lowFilghtNum, hiFilghtNum, isMarketFareAvailable, criteria,lowmktFile,himktFile = self.marketfaresservice.getMarketFareAnchorFareConnections(
                            B2B_CHANNEL, self.currParams, current_date1, self.currParams.outboundstop, self.currParams.outbound_duration, self.currParams.fareAnchor,self.inputSource,self.convert_currency)
                        self.outputSummaryRow_b2b.MktFare_Min=lowFare
                        self.outputSummaryRow_b2b.AirlineCode_Min=lowAirlineCode
                        self.outputSummaryRow_b2b.FlightNumber_Min=lowFilghtNum
                        self.outputSummaryRow_b2b.MktFare_Max=hiFare
                        self.outputSummaryRow_b2b.AirlineCode_Max=hiAirlineCode
                        self.outputSummaryRow_b2b.FlightNumber_Max=hiFilghtNum
                        self.outputSummaryRow_b2b.mktFile=himktFile
                        b2bTotal="NA"
                        b2bdiscount="NA"
                        strategyFare=0
                        b2bdiscountedFare, b2bdiscount = self.getTotalPriceForConnections(
                                priceStrategy, discountValue, b2bSectorTotal)
                        fare1=b2bdiscountedFare
                        fare2 = hiFare
                        if criteria == MINO_FARE_ANCHOR or criteria == MIN_FARE_ANCHOR:
                            fare2 = lowFare
                            self.outputSummaryRow_b2b.mktFile=lowmktFile
                        self.outputSummaryRow_b2b.discountedFare=b2bdiscountedFare   
                        strfare=fare2
                        if self.qpcurrency!='INR':
                            strfare=self.convert_currency('INR',self.qpcurrency,strfare)
                        openResult = pd.DataFrame(
                        columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
                        if self.qpcurrency!='INR':
                            strfare=self.convert_currency('INR',self.qpcurrency,strfare)
                        rbddata=self.getStrategyFare(
                            strfare, origin, destin, "B2B", B2BBackstop,offset)
                        if rbddata!="NA":
                            self.outputSummaryRow_b2b.conversionRate=0
                            if self.qpcurrency!='INR':
                                self.outputSummaryRow_b2b.strategyfare=self.convert_currency(self.qpcurrency,'INR',float(rbddata))
                            else:
                                self.outputSummaryRow_b2b.strategyfare = rbddata   
                        if self.outputSummaryRow_b2b.strategyfare != "NA":
                            strategyFare= float(self.outputSummaryRow_b2b.strategyfare)
                        if str(self.currParams.discountflag) == "1":
                            self.fareStatus = " - DiscountedFare"
                            b2bTotal=b2bdiscountedFare
                        else:
                            self.fareStatus = " - MarketFare"
                            if (bookload1 > tgtBookedLoad1 and bookload2 > tgtBookedLoad2):
                                b2bTotal = max(fare1, float(strategyFare))
                            else:
                                b2bTotal = min(fare1, float(strategyFare))
                            if apply_domestic_fare_logic and use_sector1_fare:
                                b2bTotal = max(SellingFare1,float(strategyFare))
                            if apply_domestic_fare_logic and use_sector2_fare:
                                b2bTotal = max(SellingFare2,float(strategyFare))
                        self.outputSummaryRow_b2b.anchorFare=b2bTotal
                        if b2bTotal=="NA":
                            self.outputSummaryRow_b2c.Backstop = B2CBackstop
                            self.outputSummaryRow_b2b.Backstop = B2BBackstop
                            self.outputSummaryRow_b2c.Backstop_INR = B2CBackstop_inr
                            self.outputSummaryRow_b2b.Backstop_INR = B2BBackstop_inr
                            openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': [
                                'Market Fares Not Available'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
                        else:
                            openDataB2B = self.determineRBDToOpen(
                            b2bTotal, origin, destin, "B2B", B2BBackstop,0,should_allocate_to_y)
                        self.outputSummaryRow_b2b.Discount = b2bdiscount

            if (openDataB2C['statusInd'][0] != 1):
                # Legit Allocation - Write to AU File
                currRec = AuGridRow(self.AU_COLUMN_LENGTH_CONNECTIONS)
                currRec.type = "R"  # Type
                currRec.flight = ' QP' + \
                    str(flight1) + ' ' + sector1+'/' + \
                    ' QP' + str(flight2) + ' ' + sector2
                currRec.start_date = datetime.strftime(
                    datetime.strptime(current_date1, '%Y-%m-%d'), '%m-%d-%Y')
                currRec.end_date = datetime.strftime(
                    datetime.strptime(current_date1, '%Y-%m-%d'), '%m-%d-%Y')
                currRec.day_of_week = 'Daily'
                # Condition Added since we dont need to update the AU Grid in Local but we need the Run Id's when we update Navitare via Server Run
                if self.inputSource.isUpdateNavitaire:
                    currRec.b2crunid = self.outputSummaryRow_b2c.Id
                    currRec.b2brunid = self.outputSummaryRow_b2b.Id

                # First - Adding all B2C RBD's
                b2cAllocationsUnits, brd_counter = self.allocations.b2CAllocation_connections(
                    openDataB2C, currRec, minLid, bookload, SkippingFactor, self.currParams,should_allocate_to_y)

                # Now add all the Z RBDs
                brd_counter, currRec = self.allocations.b2BAllocation_connections(
                    openDataB2B, currRec, b2cAllocationsUnits, brd_counter, self.currParams)

                self.AUFileData.append(currRec)
                if self.inputSource.isUpdateNavitaire:
                    self.updateNavSFTP(currRec)
            current_date1 = datetime.strptime(
                str(current_date1), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
            current_date2 = datetime.strptime(
                str(current_date2), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
            self.outputSummaryRow_b2c.criteria = self.currParams.fareAnchor
            self.outputSummaryRow_b2c.outBoundStop = self.currParams.outboundstop
            self.outputSummaryRow_b2c.outboundDuration = self.currParams.outbound_duration
            self.outputSummaryRow_b2c.fareOffset = self.currParams.offset
            self.outputSummaryRow_b2c.Id = b2cRunId
            self.outputSummaryRow_b2c.RunId = self.runId
            self.outputSummaryRow_b2c.Sector1 = sector1
            self.outputSummaryRow_b2c.Sector2 = sector2
            self.outputSummaryRow_b2c.FltNum1 = str(flight1)
            self.outputSummaryRow_b2c.FltNum2 = str(flight2)
            self.outputSummaryRow_b2c.DepDate1 = str(current_date1)
            self.outputSummaryRow_b2c.DepDate2 = str(current_date2)
            self.outputSummaryRow_b2c.BookedLoad = str(bookload)
            self.outputSummaryRow_b2c.allocationStatus = str(
                openDataB2C["allocationStatus"][0])
            self.outputSummaryRow_b2c.OpenRBD = str(openDataB2C['RBD'][0])
            self.outputSummaryRow_b2c.SellingFare = str(openDataB2C["Fare"][0])
            self.outputSummaryRow_b2c.Channel = 'B2C'
            self.outputSummaryRow_b2c.HowDetermined = str(
                openDataB2C['FareResult'][0])
            self.outputSummaryRow_b2c.Backstop = B2CBackstop
            self.outputSummaryRow_b2c.Backstop_INR = B2CBackstop_inr
            
            self.outputSummaryRow_b2c.analystName = self.currParams.analyst_name
            self.outputSummaryRow_b2c.firstrbd = self.currParams.first_rbd_alloc
            self.outputSummaryRow_b2c.otherrbd = self.currParams.other_rbd_alloc
            
            self.allocResults.append(self.outputSummaryRow_b2c)
            self.rmdatabasehelper.insertSummaryRow_connections(
                self.allocResults)
            
            self.outputSummaryRow_b2b.criteria = self.currParams.fareAnchor
            self.outputSummaryRow_b2b.outBoundStop = self.currParams.outboundstop
            self.outputSummaryRow_b2b.outboundDuration = self.currParams.outbound_duration
            self.outputSummaryRow_b2b.fareOffset = self.currParams.offset
            self.outputSummaryRow_b2b.Id = b2bRunId
            self.outputSummaryRow_b2b.RunId = self.runId
            self.outputSummaryRow_b2b.Sector1 = sector1
            self.outputSummaryRow_b2b.Sector2 = sector2
            self.outputSummaryRow_b2b.FltNum1 = str(flight1)
            self.outputSummaryRow_b2b.FltNum2 = str(flight2)
            self.outputSummaryRow_b2b.DepDate1 = str(current_date1)
            self.outputSummaryRow_b2b.DepDate2 = str(current_date2)
            self.outputSummaryRow_b2b.BookedLoad = str(bookload)
            self.outputSummaryRow_b2b.allocationStatus = str(
                openDataB2B["allocationStatus"][0])
            self.outputSummaryRow_b2b.OpenRBD = str(openDataB2B['RBD'][0])
            self.outputSummaryRow_b2b.SellingFare = str(openDataB2B["Fare"][0])
            self.outputSummaryRow_b2b.Channel = 'B2B'
            self.outputSummaryRow_b2b.HowDetermined = str(
                openDataB2B['FareResult'][0])
            self.outputSummaryRow_b2b.Backstop = B2BBackstop
            self.outputSummaryRow_b2b.Backstop_INR = B2BBackstop_inr
            self.outputSummaryRow_b2b.analystName = self.currParams.analyst_name
            self.outputSummaryRow_b2b.firstrbd = self.currParams.first_rbd_alloc
            self.outputSummaryRow_b2b.otherrbd = self.currParams.other_rbd_alloc
            self.allocResults.append(self.outputSummaryRow_b2b)
            self.rmdatabasehelper.insertSummaryRow_connections(
                self.allocResults)
        else:
            print("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")

    def getTotalPriceForConnections(self, priceStrategy, discountValue, sectorTotal):
        if (priceStrategy == 1):
            discount = discountValue
            total = (sectorTotal) - discount
        if (priceStrategy == 2):
            discount = (sectorTotal) * (discountValue / 100)
            total = (sectorTotal) - discount
        return total, discount

    def updateNavSFTP(self, rec):
        # QP1111 BOMHYD/ QP2222 HYDBLR
        parts = rec.flight.replace('/', '').strip().split()
        flightNo1 = parts[0][2:]
        flightNo2 = parts[2][2:]
        sector1 = parts[1]
        sector2 = parts[3]
        flightDate = datetime.strptime(
            str(rec.start_date), "%m-%d-%Y").date().strftime(Constants.DATE_FORMAT_2)
        auData = {"type": rec.type, "sector1": sector1, "flightNo1": flightNo1,
                  "sector2": sector2, "flightNo2": flightNo2, "departureDate": flightDate, "auClasses": {}}
        auClass = {}

        for i in range(self.AU_COLUMN_LENGTH_CONNECTIONS):
            auClass[getattr(
                rec, f"class_of_service_{i+1}", None)] = int(getattr(rec, f"class_au_{i+1}", None))

        auData['auClasses'] = auClass
        auDataStr = json.dumps(auData)
        self.rmdatabasehelper.insert_run_fltDate_audit(
            self.runId, flightNo1+"/"+flightNo2, flightDate, auDataStr, "Ready", "Ready")
    def getStrategyFare(self, finalprice, origin, destin, channel, backstop,offset):
        anchorFare = finalprice
        howDetermined=""    
        openResult = pd.DataFrame(
            columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
        openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(
            origin, destin, anchorFare, offset, channel, openResult, 'R')

        if (rbddata.shape[0] == 0):
            openResult = self.determinerbd.extremeFaresSkipping(
                origin, destin, anchorFare, offset, channel, backstop, openResult, 'R')
        else:
            openResult = self.determinerbd.getFares(
                origin, destin, anchorFare, offset, channel, openResult, rbddata, howDetermined, 'R')
            
        strategyFare = openResult['Fare'][0]
        print(f"strategy fare is {strategyFare}")
        return strategyFare
    
    def determineRBDToOpen(self, finalprice, origin, destin, channel, backstop,offset,should_allocate_to_y):
        anchorFare = finalprice
        if self.qpcurrency!='INR':
            anchorFare = self.convert_currency('INR',self.qpcurrency,anchorFare)
        openResult = pd.DataFrame(
            columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
        print("Anchor Fare is " + str(anchorFare))
        openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(
            origin, destin, anchorFare, offset, channel, openResult, 'R')

        if (rbddata.shape[0] == 0) or should_allocate_to_y:
            openResult = self.determinerbd.extremeFaresSkipping(
                origin, destin, anchorFare, offset, channel, backstop, openResult, 'R')
            if should_allocate_to_y:
                openResult['FareResult'][0] = 'Matched and allocating at y level'
        else:
            howDetermined = 'Matched and allocated'+self.fareStatus
            openResult = self.determinerbd.getFaresWithBackstop(
                origin, destin, anchorFare, offset, channel, backstop,  openResult, rbddata, howDetermined, 'R')
        
        return openResult

    def outputResults(self):
        if self.inputSource.output[CONST_AU_GRID] == FILE and len(self.AUFileData) != 0:
            AUlist = ['Type', 'Flight', 'StartDate', 'EndDate', 'DayOfWeek',
                      'Aircraft Type', 'Aircraft Suffix', 'Lid', 'Capacity', 'Status']
            length = Constants.AU_COLUMN_LENGTH_CONNECTIONS
            for i in range(length):
                AUlist.append(f'Class Of Service AU Class {i+1}')
                AUlist.append(f'Class Type AU Class {i+1}')
                AUlist.append(f'Class Nest AU Class {i+1}')
                AUlist.append(f'Class Rank AU Class {i+1}')
                AUlist.append(f'Class AU AU Class {i+1}')
                AUlist.append(f'Class Allotted AU Class {i+1}')
                AUlist.append(f'AP Restriction AU Class {i+1}')

            AUFrame = pd.DataFrame([vars(row) for row in self.AUFileData])
            AUFrame = AUFrame.drop(
                columns=["b2brunid", "b2crunid", "au_column_length"])
            AUFrame.columns = AUlist
            AUFrame.to_csv(CONST_AU_GRID_CONNECTIONS_FILE_PATH, index=False)
        if self.inputSource.output[CONST_SUMMARY_FILE] == FILE:
            df = pd.DataFrame([vars(row) for row in self.allocResults])
            df = df.drop(columns=["Id"])
            df.to_csv(CONST_SUMMARY_CONNECTIONS_FILE_PATH, index=False)

    def getStartEndDate(self, currDate, startNDO, endNDO):
        startDate = datetime.strptime(startNDO, Constants.DATE_FORMAT).date()
        endDate = datetime.strptime(endNDO, Constants.DATE_FORMAT).date()

        # Reset the startNDO and endNDO to actually NDO
        startNDO = (startDate - currDate).days
        endNDO = (endDate - currDate).days

        return startDate, endDate, startNDO, endNDO

    def _initialize_params(self, inputData, currParams):
        self.currParams = MarketListConnectiosnsRow(currParams)
        self.log_time = self.currParams.time_logger
        if self.log_time is not None:
            self.log_time.update_run_id(self.currParams.run_id)
        self.inputSource = InputSource(inputData)
        self.debugMode = inputData["debugMode"]
        self.NavitaireUpdateMethod = self.currParams.navitaire_update_method
        self.cache_client = self.currParams.cache_client
        self.NAV_DB_NAME = inputData["NAV_DB_NAME"]
        self.NAV_DB_DW_NAME = inputData["NAV_DB_DW_NAME"]
        self.NAV_DB_WB_NAME = inputData["NAV_DB_WB_NAME"]
        self.NAV_DB_REF_NAME = inputData["NAV_DB_REF_NAME"]
        self.NAV_BASE_API_URL = inputData["NAV_BASE_API_URL"]
        self.NAV_OCP_SUBSCRIPTION_KEY = inputData["NAV_OCP_SUBSCRIPTION_KEY"]
        self.NAV_USERNAME = inputData["NAV_USERNAME"]
        self.NAV_PWD = inputData["NAV_PWD"]
        self.NAV_DOMAIN = inputData["NAV_DOMAIN"]
        self.conn = self.currParams.dbconn
        self.wrconn = self.currParams.wrconn
        self.dwconn = self.currParams.dwconn
        self.mongoclient = self.currParams.mongodbclient
        self.starttime = datetime.now()
        self.allocResults = []
        self.AUFileData = []
        if self.currParams.run_id:
            self.runId = self.currParams.run_id
        else:
            self.runId = self.utils.getUniqueId()
        self.redishelper = RedisHelper(
            self.runId, self.cache_client, self.inputSource)
        self.logger = OwnLogger(self.runId, self.inputSource)
        self.rmdatabasehelper = RMDatabaseHelper(
            self.conn, self.wrconn,self.dwconn, self.runId, self.inputSource, self.debugMode, self.cache_client, self.currParams)
        self.constantsUtils = ConstantsUtils(self.conn)
        self.utils = Utils(self.inputSource)
        self.mathutils = MathUtils(self.inputSource)
        if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
            self.navapihelper = NavAPIHelper(self.NAV_BASE_API_URL, self.NAV_OCP_SUBSCRIPTION_KEY,
                                             self.NAV_USERNAME, self.NAV_PWD, self.NAV_DOMAIN, self.runId, self.inputSource)
        self.timeutils = TimeUtils(self.inputSource)
        # self.navdatabasehelper = NavDatabaseHelper(self.NavitaireConnection, self.conn, self.inputSource,
        #                                            self.NAV_DB_NAME,self.NAV_DB_REF_NAME, self.runId, self.debugMode, self.NAV_DB_WB_NAME, self.currParams,self.log_time)
        self.mongohelper=MarketFareMongoDB(self.wrconn,self.mongoclient,self.inputSource)
        self.marketfaresservice = MarketFareService(
            self.rmdatabasehelper, self.constantsUtils, self.timeutils,self.mongohelper)
        self.determinerbd = DetermineRBD(self.rmdatabasehelper, self.logger, self.inputSource,
                                         self.constantsUtils, self.mathutils, self.redishelper)
        self.allocations = RBDAllocations(
            self.rmdatabasehelper, self.logger, self.constantsUtils, self.mathutils)
        self.AU_COLUMN_LENGTH_CONNECTIONS = self.constantsUtils.AU_COLUMN_LENGTH_CONNECTIONS

        if inputData["latestCode"]:
            code_version = self.rmdatabasehelper.getLatestCodeVersion()
            if code_version != Constants.LATEST_VERSION:
                self.logger.info(
                    "Please use latest code! This is an old version!")
                exit()

    def wait_for_inputs(self):
        """Waits until all inputs are ready."""
        can_copy = False
        while can_copy == False:
            can_copy = self.rmdatabasehelper.areAllInputsReady()
            if can_copy == False:
                self.logger.info('Waiting as inputs are busy...')
                time_sleep.sleep(WAIT_TIME)
                
    def convert_currency(self, from_currency, to_currency, value):
        conversion = self.conversion_data[
            (self.conversion_data['FromCurrencyCode'] == from_currency) &
            (self.conversion_data['ToCurrencyCode'] == to_currency)
        ]
        
        if conversion.empty:
            raise ValueError(f"No conversion rate found for {from_currency} to {to_currency}")
        
        conversion_rate = conversion['ConversionRate'].values[0]
        converted_value = float(value) * conversion_rate
        if self.qpcurrency!='INR':
            self.outputSummaryRow_b2b.conversionRate=conversion_rate
            self.outputSummaryRow_b2c.conversionRate=conversion_rate
            
        
        return converted_value

    def getParsedImputedForecast(self,variance, forecastPLF, forecast_variance):
        try:
            forecastPLF = float(forecastPLF)
            forecast_variance = float(forecast_variance)
            if not math.isnan(forecastPLF) and not math.isnan(forecast_variance):
                return forecastPLF+variance-forecast_variance
            return CONST_NA
        except (ValueError, TypeError):
            return CONST_NA