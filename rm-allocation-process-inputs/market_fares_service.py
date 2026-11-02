import json
import math
from Constants import *
import numpy as np
import pandas as pd
import logging


class MarketFareService:
    # Assuming other parts of the class are defined elsewhere
    def __init__(self,rm_database_helper,constantsutils,timeutils,mongohelper):
        self.rmdatabasehelper=rm_database_helper
        self.constantsutils=constantsutils
        self.timeutils=timeutils
        self.mongohelper=mongohelper
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    def _isValidSector(self, origin, destin):
        """Returns False if origin and destination are the same station."""
        return origin is not None and destin is not None and origin != destin
    
    def getMarketFareAnchorFare(self, currParams,currFltDate, channel, startTime, endTime, DaySpan, criteria, flightTime,hardcriteria,hardfare,hardstartTime,hardendTime, imp_forecast=CONST_NA):
        # Extract origin and destin from currParams
        origin = currParams.origin
        destin = currParams.destin
        
        # Apply routing logic (getroutedDestination)
        routedSector = self.rmdatabasehelper.getroutedDestination(origin, destin)
        
        if len(routedSector) > 0:
            origin = routedSector['routedSector'][0][:3]
            destin = routedSector['routedSector'][0][3:]
        
        # Get fares from PRIMARY sector (with routed origin/destin)
        startTime, endTime, criteria, mktFareRange, mktfareRange_minomaxo = self.getMarketFares(currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime, origin, destin)
        _, _, _, hardmktFareRange, hardmktfareRange_minomaxo= self.getMarketFares(currFltDate, currParams, channel, hardstartTime, hardendTime, 0, criteria, flightTime, origin, destin)

        isMarketFareAvailable = False
        lowFare = 0
        hiFare = 0
        lowAirlineCode = "Market Fare not found"
        hiAirlineCode = "Market Fare not found"
        lowFilghtNum = "Market Fare not found"
        hiFilghtNum = "Market Fare not found"
        lowmktFile = "Market Fare not found"
        himktFile = "Market Fare not found"

        # If feature flag is enabled and no market fare sector rerouting is present
        try:
            if int(currParams.market_station_mapping) == 1 and routedSector.empty:
                MARKET_FARE_FORECAST_THRESHOLD = self.constantsutils.MARKET_FARE_FORECAST_THRESHOLD

                ndo = 999
                try:
                    ndo = self.timeutils.calculate_ndo(currFltDate)
                except Exception:
                    pass

                # WADD sectors have a forecast across the full 0-180 NDO range.
                _fc_ndo_max = (int(getattr(self.constantsutils, 'WADD_FORECAST_NDO_MAX', 180))
                               if str(getattr(currParams, 'forecast_model', '') or '').strip().upper() == 'WADD'
                               else int(getattr(self.constantsutils, 'FORECAST_ALLOC_NDO_MAX', 30)))
                is_high_forecast = False
                if ndo <= _fc_ndo_max and imp_forecast != CONST_NA and not pd.isna(imp_forecast):
                    try:
                        is_high_forecast = float(imp_forecast) >= MARKET_FARE_FORECAST_THRESHOLD
                    except (ValueError, TypeError):
                        is_high_forecast = False

                mapped_origin = self.rmdatabasehelper.getMappedStation(currParams.origin)
                mapped_destin = self.rmdatabasehelper.getMappedStation(currParams.destin)

                has_mapped_origin = mapped_origin is not None and mapped_origin != currParams.origin
                has_mapped_destin = mapped_destin is not None and mapped_destin != currParams.destin

                # Special case: both mapped_origin and mapped_destin exist
                # Fetch fares from all 4 origin-destin combos and pick based on forecast
                if has_mapped_origin and has_mapped_destin:
                    # Combo 1: original origin - original destin (already fetched as mktFareRange / mktfareRange_minomaxo)
                    # Combo 2: mapped_origin - original destin
                    _, _, _, combo2_mktFareRange, combo2_minomaxo = self.getMarketFares(
                        currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime, mapped_origin, destin
                    )
                    # Combo 3: original origin - mapped_destin
                    _, _, _, combo3_mktFareRange, combo3_minomaxo = self.getMarketFares(
                        currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime, origin, mapped_destin
                    )
                    # Combo 4: mapped_origin - mapped_destin
                    _, _, _, combo4_mktFareRange, combo4_minomaxo = self.getMarketFares(
                        currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime, mapped_origin, mapped_destin
                    )

                    all_combos = [mktFareRange, combo2_mktFareRange, combo3_mktFareRange, combo4_mktFareRange]
                    all_combos_minomaxo = [mktfareRange_minomaxo, combo2_minomaxo, combo3_minomaxo, combo4_minomaxo]

                    # Collect min and max fare rows from each non-empty combo for mktFareRange
                    min_fare_rows = []
                    max_fare_rows = []
                    for combo in all_combos:
                        if not combo.empty:
                            min_fare_rows.append(combo.iloc[[0]])
                            max_fare_rows.append(combo.iloc[[-1]])

                    # Collect min and max fare rows from each non-empty combo for minomaxo
                    min_fare_rows_minomaxo = []
                    max_fare_rows_minomaxo = []
                    for combo in all_combos_minomaxo:
                        if not combo.empty:
                            min_fare_rows_minomaxo.append(combo.iloc[[0]])
                            max_fare_rows_minomaxo.append(combo.iloc[[-1]])

                    # Pick based on forecast performance
                    # Good forecast (>= threshold): take max of min fares and max of max fares
                    # Bad forecast (< threshold): take min of min fares and min of max fares
                    if min_fare_rows and max_fare_rows:
                        all_min_fares = pd.concat(min_fare_rows, ignore_index=True).sort_values(by='gross_fare', ascending=True)
                        all_max_fares = pd.concat(max_fare_rows, ignore_index=True).sort_values(by='gross_fare', ascending=True)

                        if is_high_forecast:
                            picked_min = all_min_fares.iloc[[-1]]  # max of min fares
                            picked_max = all_max_fares.iloc[[-1]]  # max of max fares
                        else:
                            picked_min = all_min_fares.iloc[[0]]   # min of min fares
                            picked_max = all_max_fares.iloc[[0]]   # min of max fares

                        mktFareRange = pd.concat([picked_min, picked_max], ignore_index=True).sort_values(by='gross_fare', ascending=True)

                    if min_fare_rows_minomaxo and max_fare_rows_minomaxo:
                        all_min_fares_mo = pd.concat(min_fare_rows_minomaxo, ignore_index=True).sort_values(by='gross_fare', ascending=True)
                        all_max_fares_mo = pd.concat(max_fare_rows_minomaxo, ignore_index=True).sort_values(by='gross_fare', ascending=True)

                        if is_high_forecast:
                            picked_min_mo = all_min_fares_mo.iloc[[-1]]  # max of min fares
                            picked_max_mo = all_max_fares_mo.iloc[[-1]]  # max of max fares
                        else:
                            picked_min_mo = all_min_fares_mo.iloc[[0]]   # min of min fares
                            picked_max_mo = all_max_fares_mo.iloc[[0]]   # min of max fares

                        mktfareRange_minomaxo = pd.concat([picked_min_mo, picked_max_mo], ignore_index=True).sort_values(by='gross_fare', ascending=True)

                else:
                    # Original logic: only one of mapped_origin or mapped_destin exists
                    mapped_mktFareRange = pd.DataFrame()
                    mapped_mktFareRange_minomaxo = pd.DataFrame()

                    if has_mapped_origin:
                        _, _, _, mapped_mktFareRange, mapped_mktFareRange_minomaxo = self.getMarketFares(
                            currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime, mapped_origin, destin
                        )

                    if has_mapped_destin:
                        _, _, _, mapped_mktFareRange, mapped_mktFareRange_minomaxo = self.getMarketFares(
                            currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime, origin, mapped_destin
                        )

                    if mktFareRange.empty or mapped_mktFareRange.empty:
                        if mktFareRange.empty:
                            mktFareRange = mapped_mktFareRange
                    else:
                        allFares = pd.concat([mktFareRange.iloc[[0, -1]], mapped_mktFareRange.iloc[[0, -1]]], ignore_index=True).sort_values(by='gross_fare', ascending=True)

                        if is_high_forecast:
                            allFares = allFares.iloc[1:]
                        else:
                            allFares = allFares.iloc[:-1]

                        mktFareRange = allFares

                    if mktfareRange_minomaxo.empty or mapped_mktFareRange_minomaxo.empty:
                        if mktfareRange_minomaxo.empty:
                            mktfareRange_minomaxo = mapped_mktFareRange_minomaxo
                    else:
                        allFaresMinoMaxo = pd.concat([mktfareRange_minomaxo.iloc[[0, -1]], mapped_mktFareRange_minomaxo.iloc[[0, -1]]], ignore_index=True).sort_values(by='gross_fare', ascending=True)

                        if is_high_forecast:
                            allFaresMinoMaxo = allFaresMinoMaxo.iloc[1:]
                        else:
                            allFaresMinoMaxo = allFaresMinoMaxo.iloc[:-1]

                        mktfareRange_minomaxo = allFaresMinoMaxo
        
        except Exception as e:
            self.logger.error(f"Error comparing dual sector market fares due to reason {e}")
        
        if(len(mktFareRange) > 0):
            lowFare = mktFareRange['gross_fare'].iloc[0]
            hiFare = mktFareRange['gross_fare'].iloc[-1]
            lowAirlineCode = mktFareRange['outbound_airline_code'].iloc[0]
            hiAirlineCode = mktFareRange['outbound_airline_code'].iloc[-1]
            lowFilghtNum = mktFareRange['outbound_flight'].iloc[0]
            hiFilghtNum = mktFareRange['outbound_flight'].iloc[-1]
            lowmktFile = mktFareRange['mktFile'].iloc[0]
            himktFile = mktFareRange['mktFile'].iloc[-1]
            isMarketFareAvailable = True

            usesOutlierFilteredFares = (criteria == MINO_FARE_ANCHOR or criteria == MAXO_FARE_ANCHOR or criteria == MINOC_FARE_ANCHOR) and (len(mktFareRange) > 1)
            hasOutlierFilteredFares = (not mktfareRange_minomaxo.empty) and ('gross_fare' in mktfareRange_minomaxo.columns)

            # mktfareRange_minomaxo can be empty even when mktFareRange has >1 rows (e.g. a single
            # primary-sector fare merged with mapped-station fares). Don't fall back to raw min/max;
            # treat the market fare as not found.
            if usesOutlierFilteredFares and not hasOutlierFilteredFares:
                self.logger.info(f"Outlier-filtered market fares unavailable for criteria {criteria}; defaulting to Market Fare not found")
                isMarketFareAvailable = False
                lowFare = 0
                hiFare = 0
                lowAirlineCode = "Market Fare not found"
                hiAirlineCode = "Market Fare not found"
                lowFilghtNum = "Market Fare not found"
                hiFilghtNum = "Market Fare not found"
                lowmktFile = "Market Fare not found"
                himktFile = "Market Fare not found"
            elif usesOutlierFilteredFares:
                lowFare = mktfareRange_minomaxo['gross_fare'].iloc[0]
                hiFare = mktfareRange_minomaxo['gross_fare'].iloc[-1]
                lowAirlineCode = mktfareRange_minomaxo['outbound_airline_code'].iloc[0]
                hiAirlineCode = mktfareRange_minomaxo['outbound_airline_code'].iloc[-1]
                lowFilghtNum = mktfareRange_minomaxo['outbound_flight'].iloc[0]
                hiFilghtNum = mktfareRange_minomaxo['outbound_flight'].iloc[-1]
                lowmktFile = mktfareRange_minomaxo['mktFile'].iloc[0]
                himktFile = mktfareRange_minomaxo['mktFile'].iloc[-1]
            if hardcriteria !=CONST_NA:
                if ((hardcriteria == MINO_FARE_ANCHOR ) and (len(hardmktfareRange_minomaxo)>0)):
                    hardfare = hardmktfareRange_minomaxo['gross_fare'][0]
                if ((hardcriteria == MAXO_FARE_ANCHOR ) and (len(hardmktfareRange_minomaxo)>0)):
                    hardfare = hardmktfareRange_minomaxo['gross_fare'].iloc[-1]
                if ((hardcriteria == MIN_FARE_ANCHOR ) and (len(hardmktFareRange)>0)):
                    hardfare = hardmktFareRange['gross_fare'][0]
                if ((hardcriteria == MAX_FARE_ANCHOR ) and (len(hardmktFareRange)>0)):
                    hardfare = hardmktFareRange['gross_fare'].iloc[-1]

            # MIN2/MIN2C -> second lowest market fare; MED/MEDC -> median (averaged) market fare.
            # These use the raw sorted market fare list (no outlier filtering) and are carried in the
            # low fare fields (MktFare_Min) so they feed the MIN anchor branch downstream.
            if criteria == MIN2_FARE_ANCHOR or criteria == MIN2C_FARE_ANCHOR:
                if len(mktFareRange) > 1:
                    lowFare = mktFareRange['gross_fare'].iloc[1]
                    lowAirlineCode = mktFareRange['outbound_airline_code'].iloc[1]
                    lowFilghtNum = mktFareRange['outbound_flight'].iloc[1]
                    lowmktFile = mktFareRange['mktFile'].iloc[1]
            elif criteria == MED_FARE_ANCHOR or criteria == MEDC_FARE_ANCHOR:
                medianFare = float(np.median(mktFareRange['gross_fare']))
                lowFare = medianFare
                # Use the market fare row closest to the (possibly averaged) median for reporting metadata
                closest_idx = (mktFareRange['gross_fare'] - medianFare).abs().idxmin()
                lowAirlineCode = mktFareRange['outbound_airline_code'].loc[closest_idx]
                lowFilghtNum = mktFareRange['outbound_flight'].loc[closest_idx]
                lowmktFile = mktFareRange['mktFile'].loc[closest_idx]

        # Apply market fare adjustments
        try:
            mf_adjustments = self.rmdatabasehelper.getMarketFareAdjustments(currParams.origin, currParams.destin)

            if not mf_adjustments.empty:
                low_fare_adj = mf_adjustments[mf_adjustments['airlineCode'] == lowAirlineCode]['adjustment']
                hi_fare_adj = mf_adjustments[mf_adjustments['airlineCode'] == hiAirlineCode]['adjustment']

                if not low_fare_adj.empty:
                    low_fare_adj_value = int(low_fare_adj.values[0])
                    lowFare = max(0, lowFare + low_fare_adj_value)

                if not hi_fare_adj.empty:
                    hi_fare_adj_value = int(hi_fare_adj.values[0])
                    hiFare = max(0, hiFare + hi_fare_adj_value)

        except Exception as e:
            self.logger.error(f"Market Fare Adjustment not applied due to reason {e}")
        
        return int(lowFare),int(hiFare),lowAirlineCode,hiAirlineCode,lowFilghtNum,hiFilghtNum,isMarketFareAvailable, criteria, startTime, endTime,hardfare,lowmktFile,himktFile

    def getMarketFares(self, currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime, origin, destin):
        # Skip DB call if origin and destination are the same
        if not self._isValidSector(origin, destin):
            self.logger.info(f"Skipping market fare fetch: origin '{origin}' and destin '{destin}' are the same")
            return startTime, endTime, criteria, pd.DataFrame(), pd.DataFrame()

        # Fetch market fares for the sector with provided origin/destin
        mktFareRange = self.fetchMarketFareRange(
            channel, currFltDate, currParams, DaySpan, startTime, endTime, origin, destin, includeWeekend=False,
        )
            
        return self.prepareResponse(startTime, endTime, criteria, mktFareRange)
    
    def is_international(self, params):
        """Checks if flight/market is international based on params.region."""
        return str(getattr(params, 'region', '')).strip().lower() == 'international'

    def fetchMarketFareRange(self, channel, flightDate, params, daySpan, start_time, end_time, origin, destin, includeWeekend):
        ndo = self.timeutils.calculate_ndo(flightDate)
        threshold_json = self.constantsutils.MARKET_FARES_THRESHOLD
        if self.is_international(params):
            threshold_json = getattr(self.constantsutils, 'MARKET_FARES_THRESHOLD_INTERNATIONAL', None) or threshold_json
        hrs = self.get_value_for_ndo(threshold_json, ndo)
        if hrs is None:
            hrs = self.get_value_for_ndo(self.constantsutils.MARKET_FARES_THRESHOLD, ndo)
        if params.mongodb == '1':
            return self.mongohelper.getMarketFareRange(channel, params, flightDate, daySpan, start_time, end_time, includeWeekend, hrs)
        else:
            return self.rmdatabasehelper.getMarketFareRange(
                channel, params, flightDate, daySpan, start_time, end_time, includeWeekend, hrs, origin, destin
            )

    def get_value_for_ndo(self, json_string, ndo):
        if not json_string:
            return None
        if isinstance(json_string, list):
            ranges = json_string
        elif isinstance(json_string, str):
            try:
                ranges = json.loads(json_string)
            except Exception as e:
                self.logger.error(f"Error parsing NDO threshold JSON: {e}")
                return None
        else:
            return None

        for range_dict in ranges:
            try:
                if int(range_dict['startNdo']) <= ndo <= int(range_dict['endNdo']):
                    return range_dict['value']
            except (KeyError, ValueError, TypeError):
                continue

        return None 
    
    def getMarketFareRangeminomaxo(self, mktFareRange):
        data = pd.DataFrame(mktFareRange)
        fare_l1 = data["gross_fare"].tolist()
        fare_list = []
        if len(fare_l1)>1:
            coveragevalue = int(self.constantsutils.COVERAGE_PARAMETER)
            MED = np.median(fare_l1)
            MAD = np.median(np.abs(fare_l1- np.median(fare_l1)))
            if MAD==0:
                MAD=0.15*MED
            for fare in fare_l1:
                MZS = np.abs(fare - MED)/MAD
                if MZS < coveragevalue:
                    fare_list.append(fare)
            min_fare = min(fare_list)
            max_fare = max(fare_list)
            min_fare_row = data[data["gross_fare"] == min_fare].iloc[[0]]
            max_fare_row = data[data["gross_fare"] == max_fare].iloc[[0]]
        elif len(fare_l1)==1:
            # Single fare: nothing to filter as outlier, so it is both the min and max fare
            min_fare_row = data.iloc[[0]]
            max_fare_row = data.iloc[[0]]
        elif len(fare_list)==0:
            return pd.DataFrame()
        else:
            min_fare=fare_list[0]
            max_fare=fare_list[0]
            min_fare_row = data[data["gross_fare"] == min_fare].iloc[[0]]
            max_fare_row = data[data["gross_fare"] == max_fare].iloc[[0]]
            
        mktFareRange = pd.concat([min_fare_row, max_fare_row], ignore_index=True)
        return mktFareRange

    def prepareResponse(self, startTime, endTime, criteria, marketFareRange):
        minMaxFare = self.getMarketFareRangeminomaxo(marketFareRange)
        return startTime, endTime, criteria, marketFareRange, minMaxFare
    
    def fetchMarketFareRangeConnections(self, channel, flightDate, params, outBoundStop,outboundDuration):
        if  params.mongodb=='1':
            self.logger.info("using dynamo db for market fares")
            return self.mongohelper.getMarketFareRangeConnections(
                channel, params,flightDate, outBoundStop,outboundDuration
            )
        else:
            self.logger.info("using mysql db for market fares")
            return self.rmdatabasehelper.getMarketFareRangeConnections(
                channel, params,flightDate, outBoundStop,outboundDuration
            )
    
    def getMarketFareAnchorFareConnections(self,channel, params,flightDate, outBoundStop,outboundDuration,criteria,inputSource,convert_currency):
        
        mktFareRange = self.fetchMarketFareRangeConnections(channel,flightDate,params, outBoundStop,outboundDuration)
        isMarketFareAvailable = False
        lowFare = 0
        hiFare = 0
        lowAirlineCode = "Market Fare not found"
        hiAirlineCode = "Market Fare not found"
        lowFilghtNum = "Market Fare not found"
        hiFilghtNum = "Market Fare not found"
        lowmktFile = "Market Fare not found"
        himktFile = "Market Fare not found"
        
        if len(mktFareRange)!=0:
            isMarketFareAvailable = True
            for index, row in mktFareRange.iterrows():
                if row['currency'] != 'INR':
                    converted_fare = convert_currency(row['currency'], 'INR', row['gross_fare'])
                    mktFareRange.at[index, 'gross_fare'] = converted_fare
                    mktFareRange.at[index, 'currency'] = 'INR'
            mktFareRange=mktFareRange.sort_values(by='gross_fare', ascending=True)
            mktfareRange_minomaxo = self.getMarketFareRangeminomaxo(mktFareRange)
            lowFare = mktFareRange['gross_fare'][0]
            hiFare = mktFareRange['gross_fare'].iloc[-1]
            lowAirlineCode = mktFareRange['outbound_airline_code'][0]
            hiAirlineCode = mktFareRange['outbound_airline_code'].iloc[-1]
            lowFilghtNum = mktFareRange['outbound_flight'][0]
            hiFilghtNum = mktFareRange['outbound_flight'].iloc[-1]
            lowmktFile = mktFareRange['mktFile'][0]
            himktFile = mktFareRange['mktFile'].iloc[-1]
            if ((criteria == MINO_FARE_ANCHOR or criteria == MAXO_FARE_ANCHOR or criteria == MINOC_FARE_ANCHOR) and (len(mktFareRange)>1)):
                lowFare = mktfareRange_minomaxo['gross_fare'].iloc[0]
                hiFare = mktfareRange_minomaxo['gross_fare'].iloc[-1]
                lowAirlineCode = mktfareRange_minomaxo['outbound_airline_code'].iloc[0]
                hiAirlineCode = mktfareRange_minomaxo['outbound_airline_code'].iloc[-1]
                lowFilghtNum = mktfareRange_minomaxo['outbound_flight'].iloc[0]
                hiFilghtNum = mktfareRange_minomaxo['outbound_flight'].iloc[-1]
                lowmktFile = mktfareRange_minomaxo['mktFile'].iloc[0]
                himktFile = mktfareRange_minomaxo['mktFile'].iloc[-1]
        
        return lowFare,hiFare,lowAirlineCode,hiAirlineCode,lowFilghtNum,hiFilghtNum,isMarketFareAvailable, criteria,lowmktFile,himktFile


    def getMFinRange(self, currParams,currFltDate, channel, startTime, endTime, DaySpan, criteria, flightTime, dplfBand):
        startTime, endTime, criteria, mktFareRange, mktfareRange_minomaxo = self.getMarketFares(currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime, currParams.origin, currParams.destin)
        min_10 = 0
        if(dplfBand == 0):
            if(len(mktFareRange) > 0):
                min_10 = mktFareRange['gross_fare'][self.constantsutils.MFMIN0]
        elif(dplfBand == 1 or dplfBand == 2):
            if(len(mktFareRange) > 1):
                min_10 = mktFareRange['gross_fare'][self.constantsutils.MFMIN1]
            elif(len(mktFareRange) == 1):
                min_10 = mktFareRange['gross_fare'][self.constantsutils.MFMIN0]
        return min_10
