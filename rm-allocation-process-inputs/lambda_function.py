from datetime import timedelta
from datetime import date
from datetime import datetime
import sys
import logging
import uuid
import time
from dateutil import parser

from sqlalchemy import create_engine
import pandas as pd
import pymssql

import json
import boto3



# sqs setting
sqs = boto3.client('sqs')  # client is required to interact with

TABLE_MARKET_FARES = "market_fares"
WAIT_TIME = 30 # in seconds
NAV_DB_NAME = "[10.154.12.13,52900].[REZ0KOD01]."
RM_ALLOC_DB = "mysql+mysqldb://dbadmin_prod:cxvyFv^^e4bd7b33@p-esb.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"

LOGS_ON=False

username = 'SYS2'
# rds settings
DB_CONN_STRING = "mysql+mysqldb://dbadmin_prod:cxvyFv^^e4bd7b33@p-esb.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"
MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-market-list"
RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-rbds"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = create_engine(DB_CONN_STRING)
    NavitaireConnection = pymssql.connect(
        host="43.204.99.181:1433",
        user="crm_navods_read",
        password="P3WjeMw4463nwm8"
        # database="[10.154.12.13,52900][REZ0KDW01]"
    )
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


def lambda_handler(event, context):
    """
    This function fetches the market_list and inserts it into the queue
    """
    records = event['Records']

    for record in records:
        content = record['body']
        
        runId = getUniqueId()
        startTime = datetime.now()
        insertRunStart(runId, startTime)
        
        allocResults = pd.DataFrame(columns=['Id', 'RunId', 'Origin','Destin','FltNum','DepDate','BookedLoad','TgtLoad','MktFare','OpenRBD','SellingFare', 'Channel', 'HowDetermined'])

        data = json.loads(content)
        logger.info(data)

        runDynamicAllocationSingleMarket(data, runId, allocResults)

        endTime = datetime.now()
        updateRunEnd(runId, endTime)

    return {
        'statusCode': 200,
        'body': 'success'
    }

def runDynamicAllocationSingleMarket(currParams, runId, allocResults):
    """
    Convert NDO to run dates as necessary
    """

    logger.info(currParams)

    currDate = date.today()
    startDate = currDate
    endDate = currDate
    startNDO = currParams['PerStart']
    endNDO = currParams['PerEnd']

    if(currParams['PerType'] == 'D'):
        startDate = datetime.strptime(startNDO, "%m/%d/%Y").date()
        endDate = datetime.strptime(endNDO, "%m/%d/%Y").date()

        #Reset the startNDO and endNDO to actually NDO
        startNDO = (startDate - currDate).days
        endNDO = (endDate - currDate).days
    else:
        startDate = currDate + timedelta(days=int(startNDO))
        endDate = currDate + timedelta(days=int(endNDO))
    
    print("Allocation for " + currParams['Origin'] + currParams['Destin'] + " from " + str(startDate) + " to " + str(endDate))
    
    #Fetch the current booked load from Navitaire
    bookedLoad = getCurrentBookedLoads(currParams['Origin'] ,currParams['Destin'], startDate, endDate)

    tgtBookedLoad = None
    # If for whatever reason - unable to get the booked Load - Abort
    if (bookedLoad.shape[0] == 0):
        print("Unable to get the Booked Load - Aborting")
        allocResults = allocResults.append({"Id": getUniqueId(), "RunId": runId, "Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":"NA","DepDate":str(startDate), "BookedLoad":"NA", "TgtLoad":str(tgtBookedLoad), "MktFare":"NA", "OpenRBD":"NA", "SellingFare":"NA", 'Channel':"NA", 'HowDetermined':"unable to get Booked loads from " + str(startDate) + " to " + str(endDate)}, ignore_index = True)
        insertSummaryRow(allocResults)
    else:
        # check if inputs are available to be copied else wait and retry
        canCopy = False
        while canCopy == False:
            canCopy = areAllInputsReady()
            if canCopy == False:
                print('Waiting as inputs are busy...')
                time.sleep(WAIT_TIME)

        print('Inputs (market fares, channel_map etc) are ready. Copying inputs...')
        # create a market_fares_extract based on org/dest/timerange
        marketFaresExtractName = createTableExtract(currParams['Origin'], currParams['Destin'], startDate, endDate, currParams['TimeWindowStart'], currParams['TimeWindowEnd'])
        print('Inputs Copy completed.')

        # try: 
        # For each NDO
        for ndocounter in range(startNDO, endNDO+1):
            currDate = date.today()
            tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),"%Y-%m-%d")
            tgtBookedLoad = getTargetBookedLoadFromBenchmarkCurve(ndocounter, currParams['CurveID'])

            if (tgtBookedLoad >= 0):
                runDynamicAllocationSingleMarketSingleDay(runId, ndocounter, bookedLoad, tgtBookedLoad, currParams, marketFaresExtractName, allocResults)
            else:
                #Cannot get a reference curve - Abort
                print("Unable to get a Target - Check the booking curves")
                allocResults = allocResults.append({"Id": getUniqueId(), "RunId": runId, "Origin": currParams['Origin'],"Destin":currParams['Destin'],"FltNum":"NA","DepDate":str(tgtDate), "BookedLoad":"NA", "TgtLoad":str(tgtBookedLoad), "MktFare":"NA", "OpenRBD":"NA", "SellingFare":"NA", 'Channel':"NA", 'HowDetermined':"No Reference Curve Found"}, ignore_index = True)
                insertSummaryRow(allocResults)

        # except Exception as e:
        #     print("Exception occurred")
        #     print(e)
        # finally:
        deleteTableExtract(marketFaresExtractName)


def runDynamicAllocationSingleMarketSingleDay(runId, ndocounter, bookedLoad, tgtBookedLoad, currParams, marketFaresExtractName, allocResults):
    currDate = date.today()
    tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),"%Y-%m-%d")
    dow = int(datetime.strftime(currDate + timedelta(days=ndocounter),"%w"))
    
    print("Determining pricing for NDO " + str(ndocounter) + " on  " + str(tgtDate))
    print("Target Pax Load is :" + str(tgtBookedLoad))
    
    ndoBookedLoad = bookedLoad[bookedLoad['DepartureDate'].astype(str).str.contains(str(tgtDate))]
    ndoBookedLoad = ndoBookedLoad.reset_index(drop=True)
    
    numFlts = ndoBookedLoad.shape[0]

    for fltCounter in range(numFlts):
        currFlt = ndoBookedLoad['FlightNumber'][fltCounter]
        currBookedLoad = ndoBookedLoad["bookedPax"][fltCounter]
        currCapacity = ndoBookedLoad['capacity'][fltCounter]
        currFltDate = datetime.strftime(ndoBookedLoad['DepartureDate'][fltCounter],"%m/%d/%Y")	

        runDynamicAllocationSingleFlight(runId, currFlt, currBookedLoad, currCapacity, currFltDate, tgtBookedLoad, dow, currParams, marketFaresExtractName, allocResults)


def runDynamicAllocationSingleFlight(runId, currFlt, currBookedLoad,currCapacity, currFltDate, tgtBookedLoad, dow, currParams, marketFaresExtractName, allocResults):

    calcDeficit = tgtBookedLoad - currBookedLoad
    calcBookStatus = "M"
    if (calcDeficit > 0):
        calcBookStatus = "B"
    if (calcDeficit <= 0):
        calcBookStatus = "A"

    if ((str(currFlt) == str(currParams['FlightNumber'])) or (currParams['FlightNumber'] == '*')) and (calcBookStatus == currParams['bookedStatus']) and (str(currParams['DOW'])[dow:(dow+1)] == "1"):
        print("Actual Booked Load for " + str(currParams['FlightNumber']) + " on " + str(currFltDate) + " is :" + str(currBookedLoad))

        #Actual Work Happens here
        #Get the B2C and B2B Fare ranges
        mktFareRangeB2C = getMarketFareRange('B2C',currFltDate, currParams, marketFaresExtractName)
        mktFareRangeB2B = getMarketFareRange('B2B',currFltDate, currParams, marketFaresExtractName)

        # Does competetive fare exist
        # We only proceed if a B2C fare exists - If B2C does not exist, we will skip the whole thing even with a B2B fare
        # This is because, when we prepare the AU Grid, if we donr push inventory on to B2C RBD, then the flight will not be visible 
        # on OTA - Flag the error for correction here

        if (float(mktFareRangeB2C['LowFare'][0]) > 0):
            lowFareB2C = mktFareRangeB2C['LowFare'][0]
            hiFareB2C = mktFareRangeB2C['HiFare'][0]

            lowFareB2B = mktFareRangeB2B['LowFare'][0]
            hiFareB2B = mktFareRangeB2B['HiFare'][0]

            print("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2C) + " to " + str(hiFareB2C))
            print("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(lowFareB2B) + " to " + str(hiFareB2B))

            openDataB2C = determineRBDToOpen(currParams['Origin'], currParams['Destin'], calcDeficit, lowFareB2C,hiFareB2C, currParams['fareAnchor'],currParams['fareOffset'],'B2C')
            openDataB2B = determineRBDToOpen(currParams['Origin'], currParams['Destin'], calcDeficit, lowFareB2B,hiFareB2B, currParams['fareAnchor'],currParams['fareOffset'],'B2B')

            allocResults = allocResults.append({"Id": getUniqueId(), "RunId": runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), "MktFare":str(round(float(lowFareB2C))), "OpenRBD":str(openDataB2C['RBD'][0]), "SellingFare":str(openDataB2C["Fare"][0]), 'Channel':'B2C', 'HowDetermined':str(openDataB2C['FareResult'][0])}, ignore_index = True)
            insertSummaryRow(allocResults)
            
            allocResults = allocResults.append({"Id": getUniqueId(), "RunId": runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), "MktFare":str(round(float(lowFareB2B))), "OpenRBD":str(openDataB2B['RBD'][0]), "SellingFare":str(openDataB2B["Fare"][0]), 'Channel':'B2B', 'HowDetermined':str(openDataB2B['FareResult'][0])}, ignore_index = True)
            insertSummaryRow(allocResults)

            # statusInd
            # 0 - Allocation went as planned
            # 1 - Could not find market fares 
            # 2 - Ran out of RBDs before the offset could be achieved

            if (openDataB2C['statusInd'][0] == 0) or (openDataB2C['statusInd'][0] == 2):
                #Legit Allocation - Write to AU File
                currRec = []
                currRec.append('L') #Type
                currRec.append(' QP' + str(currFlt) + ' ' + currParams['Origin'] + currParams['Destin']) 
                currRec.append(datetime.strftime(datetime.strptime(currFltDate,'%m/%d/%Y'), '%m-%d-%Y'))
                currRec.append(datetime.strftime(datetime.strptime(currFltDate,'%m/%d/%Y'), '%m-%d-%Y'))
                currRec.append('Daily')
                currRec.append('') #AcftType
                currRec.append('') #Suffix
                currRec.append('') # Lid
                currRec.append('') #Cap
                currRec.append('') #Status


                allocatedInventoryB2C = currBookedLoad
                allocatedInventoryB2B = currBookedLoad
                totalCapacity = currCapacity
                incremental = -1
                bookedDeficit = round(tgtBookedLoad - currBookedLoad)

                #First - Fill in the Z B2B Levels
                #get all the Z RBDs
                RBDQuery = "select RBD, Total from Fares where Org = '" + currParams['Origin'] + "' and Dst = '" + currParams['Destin'] + "' "
                RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' order by Total asc"
                B2BRBDs = pd.read_sql(RBDQuery, conn)

                #Loop Thru
                
                openbuketFlag = False
                for b2bctr in range(B2BRBDs.shape[0]):
                    RBDLabel = B2BRBDs['RBD'][b2bctr]
                    currRec.append(RBDLabel)
                    currRec.append('')
                    currRec.append(-1)
                    currRec.append(-1)
                    if B2BRBDs['RBD'][b2bctr] == openDataB2B['RBD'][0]:
                        openbuketFlag = True

                    if (openbuketFlag):
                        # Allocation
                        if((openDataB2B['statusInd'][0] == 0) or (openDataB2B['statusInd'][0] == 2)):
                            #Valid B2B Allocation
                            if(incremental < 0):
                                # First Alloctation
                                if(currParams['FirstRBDAlloc'] >= 0):
                                    # allocate the FirstRBD 
                                    incremental = currBookedLoad + int(round(currParams['FirstRBDAlloc']/2,0))
                                else:
                                    # negtive number means allocate the full deficit - Only if the deficit is positive - Lagging
                                    if(bookedDeficit > 0):
                                        incremental = currBookedLoad + bookedDeficit
                                    else:
                                        incremental = currBookedLoad + int(round(currParams['OtherRBDAlloc']/2,0))
                            else:
                                incremental = incremental + int(round(currParams['OtherRBDAlloc']/2,0))

                            if (incremental > totalCapacity):
                                incremental = totalCapacity
                            currRec.append(incremental)
                        else:
                            # No B2B Allocaion - Leave it to B2C Fare
                            currRec.append(0)
                    else:
                        if openDataB2B['RBD'][0] == 'NA':
                            # This means B2B market fare was missing. Add -1 to continue with previous value
                            currRec.append(-1)
                        else:
                            currRec.append(0)

                    currRec.append(-1)
                    currRec.append(-1)
                
                #Now Add All RBD Levels
                
                RBDQuery = "select RBD, Total from Fares where Org = '" + currParams['Origin'] + "' and Dst = '" + currParams['Destin'] + "' "
                RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' order by Total asc"
                B2CRBDs = pd.read_sql(RBDQuery, conn)

                # reset
                # Loop Thru
                openbuketFlag = False
                
                incremental = -1

                for b2cctr in range(B2CRBDs.shape[0]):
                    RBDLabel = B2CRBDs['RBD'][b2cctr]
                    currRec.append(RBDLabel)
                    currRec.append('')
                    currRec.append(-1)
                    currRec.append(-1)
                    if B2CRBDs['RBD'][b2cctr] == openDataB2C['RBD'][0]:
                        openbuketFlag = True

                    # Allocation
                    if(openbuketFlag):
                        # Valid B2C Allocation
                        if(incremental < 0):
                            # First Alloctation
                            if(currParams['FirstRBDAlloc'] >= 0):
                                # allocate the FirstRBD 
                                incremental = currBookedLoad +  currParams['FirstRBDAlloc']
                            else:
                                # negtive number means allocate the full deficit - Only if the deficit is positive - Lagging
                                if(bookedDeficit > 0):
                                    incremental = currBookedLoad + bookedDeficit
                                else:
                                    incremental = currBookedLoad + currParams['OtherRBDAlloc']
                        else:
                            incremental = incremental + currParams['OtherRBDAlloc']

                        if (incremental >= totalCapacity):
                            incremental = totalCapacity
                        if (B2CRBDs['RBD'][b2cctr] == 'Y'):
                            incremental = totalCapacity
                        currRec.append(incremental)
                    else:
                        currRec.append(0)
                        
                    currRec.append(-1)
                    currRec.append(-1)
                # End the B2C counter
                
                updateNavitaire(runId, currRec)
        else:
            print("unable to get a Market Fare - Skipping")
    
            allocResults = allocResults.append({"Id": getUniqueId(), "RunId": runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), "MktFare":"NA", "OpenRBD":"NA", "SellingFare":"NA", 'Channel':'B2C', 'HowDetermined':"Market Fare B2C Not Available - Skipping entire allocation"}, ignore_index = True)
            insertSummaryRow(allocResults)
            
            allocResults = allocResults.append({"Id": getUniqueId(), "RunId": runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), "MktFare":"NA", "OpenRBD":"NA", "SellingFare":"NA", 'Channel':'B2B', 'HowDetermined':"Market Fare B2C Not Available - Skipping entire allocation"}, ignore_index = True)
            insertSummaryRow(allocResults)
            
    else:
        print("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")
        
def getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveID):
    #Fetch the Target Booked Load from the curve
    bookingCurveQuery = "Select Pax as tgtPax from Curves where NDO = " + str(ndocounter) + " and CurveID = '" + curveID + "'"
    tgtBookedLoad = -1
    benchmark = pd.read_sql(bookingCurveQuery, conn)
    if(benchmark.shape[0] > 0):
        tgtBookedLoad = benchmark['tgtPax'][0]

    return tgtBookedLoad


def getMarketFareRange(channel, currFltDateStr, currParams, marketFaresExtractName):
    currFltDate = datetime.strptime(currFltDateStr, "%m/%d/%Y").date()		
    mktLowestQuery = "Select ifnull(min(gross_fare),0) as LowFare, ifnull(max(gross_fare),0) as HiFare from " + marketFaresExtractName + " where source_airport = '" 
    mktLowestQuery = mktLowestQuery + currParams['Origin'] + "' and destination_airport = '" + currParams['Destin'] + "'"

    mktLowestQuery = mktLowestQuery + " and departure_date = '" + str(currFltDate) + "'"
    mktLowestQuery = mktLowestQuery + " and str_to_date(outbound_departuretime,'%%T') >= str_to_date('" + currParams['TimeWindowStart'] + "','%%T')"
    mktLowestQuery = mktLowestQuery + " and str_to_date(outbound_departuretime,'%%T') <= str_to_date('" + currParams['TimeWindowEnd'] + "','%%T')"

    mktLowestQuery = mktLowestQuery + " and channel = '" + channel + "' "
    mktLowestQuery = mktLowestQuery + " and outbound_airline_code <> '" + currParams['CarrExlusion'] + "'"

    mktFareRange = pd.read_sql(mktLowestQuery, conn)
    
    # print("Market Fare Range for " + str(currFltDate) + " in the " + channel + " Channel is :")
    # print(mktFareRange) 
    return mktFareRange


def getCurrentBookedLoads(origin, destin, startDate, endDate):
    NavitaireQuery = "select Origin, Destination, FlightNumber, DepartureDate, count(PassengerID) as bookedPax, max(Lid) as capacity from ("
    NavitaireQuery = NavitaireQuery + "select BP.PassengerID, IL.DepartureStation as Origin, IL.ArrivalStation as Destination, IL.DepartureDate,IL.FlightNumber, IL.AdjustedCapacity, IL.Lid "
    NavitaireQuery = NavitaireQuery + " from "+NAV_DB_NAME+"[rez].InventoryLeg IL "
    NavitaireQuery = NavitaireQuery + " left outer join "+NAV_DB_NAME+"[rez].PassengerJourneyLeg PJL on  PJL.InventoryLegID = IL.InventoryLegID "
    NavitaireQuery = NavitaireQuery + " left outer join "+NAV_DB_NAME+"[rez].PassengerJourneySegment PJS on PJS.PassengerID = PJL.PassengerID and PJS.SegmentID = PJL.SegmentID  "
    NavitaireQuery = NavitaireQuery + " left outer join "+NAV_DB_NAME+"[rez].BookingPassenger BP on  BP.PassengerID = PJS.PassengerID "
    NavitaireQuery = NavitaireQuery + " where IL.DepartureDate >= '" + str(startDate) + "' and IL.DepartureDate <= '" +str(endDate) + "' and (PJS.FareBasis != 'SO7NBSOL' or PJS.FareBasis IS NULL)"
    NavitaireQuery = NavitaireQuery + " and IL.DepartureStation = '" + origin + "' and IL.ArrivalStation = '" + destin + "'"
    NavitaireQuery = NavitaireQuery + ") A group by Origin, Destination, FlightNumber, DepartureDate order by Origin, Destination, DepartureDate "

    bookedData = pd.read_sql(NavitaireQuery, NavitaireConnection)
    return bookedData


def determineRBDToOpen(origin, destin, bookedDeficit, lowFare, hiFare, strategy, offset, channel):

    openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult','statusInd'])

    anchorFare = lowFare
    if (strategy == 'MAX'):
        anchorFare = hiFare
    
    print("Strategy applied for " + channel + " Channel is " + strategy )
    print("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))

    RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' "
    
    if channel == 'B2C':
        RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z'"
    if channel == 'B2B':
        RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z'"

    sortorder = ' desc '

    if (offset < 0):
        RBDQuery = RBDQuery + " and Total < " + str(anchorFare)
        sortorder = ' desc '
    if (offset >= 0):
        anchorFare = float(anchorFare)
        if (anchorFare > 0):
            RBDQuery = RBDQuery + " and Total >= " + str(anchorFare)
        else:
            # Below condition forces zero records to be returned
            RBDQuery = RBDQuery + " and Total >= 1000000"
        sortorder = ' asc '

    RBDQuery = RBDQuery + " order by Total " + sortorder

    rbddata = pd.read_sql(RBDQuery, conn)

    if(rbddata.shape[0] == 0):
        print("Market Fare is below all published fares")
        #Fetch the Lowest available fare

        defFareData = fetchExtremePublishedFare(origin, destin, channel, 'LOW')
        if (offset >= 0):
            defFareData = fetchExtremePublishedFare(origin, destin, channel, "HI")

        if (channel == 'B2C'):
            openResult = openResult.append({'RBD':defFareData['RBD'][0], 'Fare':defFareData['Total'][0], 'FareResult':'Unable to Match Mkt - Revisit Fare Ladder','statusInd':2}, ignore_index = True)
        if (channel == 'B2B'):
            openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'B2B Matching Fare Not Found','statusInd':1}, ignore_index = True)
    else:
        #We got back data
        offsetToFetch = min(abs(offset), rbddata.shape[0] - 1) 
        if(offset < 0):
            offsetToFetch = min(abs(offset+1), rbddata.shape[0] - 1) 

        RBDtoOpen = rbddata['RBD'][offsetToFetch]
        FareToOpen = rbddata['Total'][offsetToFetch]
        print('Determining ' + channel + ' Fare ' + str(FareToOpen) + " " + str(RBDtoOpen) )

        if (channel == 'B2B'):
            #Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
            limitB2BFare_Data = fetchExtremePublishedFare(origin, destin, channel, 'HI')
            limitB2BFare = float(limitB2BFare_Data['Total'][0])

            if (float(anchorFare) > limitB2BFare):
                #Outside of B2B Levels - stick with B2C
                openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'Market Fare higher than Z0 levels - skipping B2B allocation','statusInd':1}, ignore_index = True)
            else:
                if(offsetToFetch == rbddata.shape[0]):
                    openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Not Enough Levels available - taking the Max/Min Available','statusInd':2}, ignore_index = True)
                else:
                    openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated','statusInd':0}, ignore_index = True)
        else:
            if(offsetToFetch == rbddata.shape[0]):
                openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Not Enough Levels available - taking the Max/Min Available','statusInd':2}, ignore_index = True)
            else:
                openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated','statusInd':0}, ignore_index = True)

    return openResult


def fetchExtremePublishedFare(origin, destin, channel, type):
    RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "'" 
    if channel == 'B2C':
        if type == 'LOW':
            RBDQuery = RBDQuery + " and RBD = 'V9'"
        else:
            RBDQuery = RBDQuery + " and RBD = 'Y'"
    else:
        if type == 'LOW':
            RBDQuery = RBDQuery + " and RBD = 'Z9'"
        else:
            RBDQuery = RBDQuery + " and RBD = 'Z0'"

    defFareData = pd.read_sql(RBDQuery, conn)
    return defFareData


def updateNavitaire(runId, rec):
    auData = {"runId": runId, "type": rec[0], "flight": rec[1], "startDate": rec[2], "endDate": rec[3], "dayOfWeek": rec[4],
        "aircraftType": rec[5], "aircraftSuffix": rec[6], "lid": rec[7], "capacity": rec[8], "status": rec[9], "auClasses": {}}

    gap = 9
    for i in range(0, 64):
        if (int(rec[gap + i*7 + 5]) != -1):
            auClass = {"classOfService": rec[gap + i*7 + 1], "classType": rec[gap + i*7 + 2], "classNest": int(rec[gap + i*7 + 3]),
                "classRank": int(rec[gap + i*7 + 4]), "classAU": int(rec[gap + i*7 + 5]), "classAllotted": int(rec[gap + i*7 + 6]), "apRestriction": int(rec[gap + i*7 + 7])}
            auData["auClasses"][auClass["classOfService"]] = auClass

    auDataStr = str(auData)
    # Send to queue for the market
    sqs.send_message(
        QueueUrl=RBD_QUEUE_URL,
        MessageBody=auDataStr
    )

def createTableExtract(org, dest, perstart, perend, starttime, endtime):
    name = str(uuid.uuid4())
    name = name.replace("-", "_")
    tableName = "ext_" + name

    dropQuery = "DROP TABLE IF EXISTS " + tableName
    createQuery = "CREATE TABLE " + tableName + " LIKE " + TABLE_MARKET_FARES
    insertQuery = "INSERT INTO " + tableName + " SELECT * FROM " + TABLE_MARKET_FARES + \
        " WHERE source_airport = '" + org + "' AND destination_airport = '" + dest + "' AND departure_date >= '" + \
            perstart.strftime("%Y-%m-%d") + "' AND departure_date <= '" + perend.strftime("%Y-%m-%d") + \
                "' AND outbound_departuretime >= '" + parser.parse(starttime).strftime("%H:%M") + \
                    "' AND outbound_departuretime <= '" + parser.parse(endtime).strftime("%H:%M") + \
                        "' AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' AND created_at > NOW() - INTERVAL 2 DAY"
    # print(insertQuery)
    conn.execute(dropQuery)
    conn.execute(createQuery)
    conn.execute(insertQuery)

    return tableName

def insertSummaryRow(allocResults):
    # latest_row = allocResults.iloc[-1]
    insertQuery = " INSERT INTO run_summary ( Id, Origin, Destin, FltNum, DepDate, BookedLoad, TgtLoad, MktFare, OpenRBD, SellingFare, Channel, HowDetermined, RunId, CreatedAt) "		
    # insertQuery = insertQuery + " VALUES ('"+ latest_row['Id'] + "', '"+latest_row['Origin'] + "', '"+latest_row['Destin'] + "', '"+ latest_row['FltNum'] + "', '"+latest_row['DepDate'] + "', '"+latest_row['BookedLoad'] + "', '"+latest_row['TgtLoad'] + "', '"+latest_row['MktFare'] + "', '"+latest_row['OpenRBD'] + "', '"+latest_row['SellingFare'] + "', '"+latest_row['Channel'] + "', '"+latest_row['HowDetermined'] + "', '"+latest_row['RunId'] + "', '" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') +"')"
    # conn.execute(insertQuery)


def deleteTableExtract(tableName):
    dropQuery = "DROP TABLE IF EXISTS " + tableName
    conn.execute(dropQuery)

def getUniqueId():
    return str(uuid.uuid4())

def insertRunStart(runId, startTime):
    insertQuery = " INSERT INTO allocation_run_audit ( run_id, start_time, user) VALUES (%s, %s, %s)"
    conn.execute(insertQuery, 
        (runId, startTime.strftime('%Y-%m-%d %H:%M:%S'), username))

def updateRunEnd(runId, endTime):
    updateQuery = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "		
    conn.execute(updateQuery, 
        (endTime.strftime('%Y-%m-%d %H:%M:%S'), runId))

def areAllInputsReady():
    countQuery = "select count(*) as busyInputs from inputs_status WHERE is_running = b'1'"
    inputsData = pd.read_sql(countQuery, conn)

    return inputsData['busyInputs'][0] == 0
