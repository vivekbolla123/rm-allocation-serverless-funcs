import pandas as pd
from datetime import datetime, timedelta
from Constants import *
import logging

class MarketFareMongoDB:
    def __init__(self, conn,db,inputSource):
        self.db = db
        self.logger = logging.getLogger(__name__)
        self.gsi_name = 'ChannelDepartureDateGSI'
        self.conn=conn
        self.inputSource=inputSource

    

    def getMarketFareRange(self, channel, currParams, currFltDateStr, DaySpan, startTime, endtime, Day3Flag, hrs):
        
        try:
            currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()
            updateDate = currFltDate + timedelta(days=int(DaySpan))
 
            if Day3Flag == False:
                startDateTime = datetime.combine(currFltDate, datetime.strptime(startTime, "%H:%M").time())
                enddateTime = datetime.combine(currFltDate, datetime.strptime(endtime, "%H:%M").time())
                updateDate = currFltDate + timedelta(days=int(DaySpan))
                
                if currFltDate > updateDate:
                    startDateTime = datetime.combine(updateDate, datetime.strptime(startTime, "%H:%M").time())
                    enddateTime = datetime.combine(currFltDate, datetime.strptime(endtime, "%H:%M").time())
                elif currFltDate < updateDate:
                    startDateTime = datetime.combine(currFltDate, datetime.strptime(startTime, "%H:%M").time())
                    enddateTime = datetime.combine(updateDate, datetime.strptime(endtime, "%H:%M").time())
 
            if channel == B2B_CHANNEL:
                carr_exclusion = currParams.carr_exclusion_b2b
                flightExclusion = currParams.flightExclusionB2B
            else:
                carr_exclusion = currParams.carr_exclusion_b2c
                flightExclusion = currParams.flightExclusionB2C
 
            carr_exclusion_list = carr_exclusion.split('/') if carr_exclusion else []
 
            origin = currParams.origin
            destination = currParams.destin
            routedSector = self.getRoutedDestination(origin, destination)
 
            if len(routedSector) > 0:
                origin = routedSector['routedSector'][0][:3]
                destination = routedSector['routedSector'][0][3:]
 
            # Log the search parameters
            self.logger.info(f"Searching market fares: {origin}-{destination}, Channel: {channel}, Date: {currFltDateStr}")
 
            # Check if collections exist
            collection_names = self.db.list_collection_names()
            ag_exists = 'market_fares_ag' in collection_names
            rg_exists = 'market_fares_rg' in collection_names
            
            if not ag_exists and not rg_exists:
                self.logger.warning("Neither market_fares_ag nor market_fares_rg collections found")
                return pd.DataFrame()
 
            # Build match conditions
            base_match_conditions = {
                "source_airport": origin,
                "destination_airport": destination,
                "channel": channel,
                "outbound_airline_code": {
                    "$nin": carr_exclusion_list + ["QP", "G8"]
                },
                "outbound_stop": 0
            }
            
            if flightExclusion:
                base_match_conditions["outbound_flight"] = {"$ne": flightExclusion}
 
            # Add time interval condition for non-test data
            if hasattr(self, 'inputSource') and self.inputSource.input_source.get(CONST_MARKET_FARES) != TEST_DATA:
                hrs_ago = datetime.now() - timedelta(hours=hrs)
                base_match_conditions["created_at"] = {"$gt": hrs_ago}
 
            # Date and time conditions
            if Day3Flag:
                base_match_conditions.update({
                    "departure_date": {
                        "$gte": datetime.combine(currFltDate, datetime.min.time()),
                        "$lte": datetime.combine(updateDate, datetime.max.time())
                    }
                })
                
                # Add time filtering
                start_time_parts = [int(x) for x in startTime.split(':')]
                end_time_parts = [int(x) for x in endtime.split(':')]
                
                base_match_conditions["$expr"] = {
                    "$and": [
                        {"$gte": [{"$hour": "$outbound_departuretime"}, start_time_parts[0]]},
                        {"$lte": [{"$hour": "$outbound_departuretime"}, end_time_parts[0]]}
                    ]
                }
            else:
                base_match_conditions["$expr"] = {
                    "$and": [
                        {
                            "$gte": [
                                {"$dateFromParts": {
                                    "year": {"$year": "$departure_date"},
                                    "month": {"$month": "$departure_date"},
                                    "day": {"$dayOfMonth": "$departure_date"},
                                    "hour": {"$hour": "$outbound_departuretime"},
                                    "minute": {"$minute": "$outbound_departuretime"},
                                    "second": {"$second": "$outbound_departuretime"}
                                }},
                                startDateTime
                            ]
                        },
                        {
                            "$lte": [
                                {"$dateFromParts": {
                                    "year": {"$year": "$departure_date"},
                                    "month": {"$month": "$departure_date"},
                                    "day": {"$dayOfMonth": "$departure_date"},
                                    "hour": {"$hour": "$outbound_departuretime"},
                                    "minute": {"$minute": "$outbound_departuretime"},
                                    "second": {"$second": "$outbound_departuretime"}
                                }},
                                enddateTime
                            ]
                        }
                    ]
                }
 
            all_results = []
 
            # Query AG collection if it exists
            if ag_exists:
                try:
                    ag_pipeline = [
                        {
    "$set": {
      "outbound_departuretime": {
        "$dateFromString": {
          "dateString": {
            "$concat": ["1970-01-01T", "$outbound_departuretime"]
          },
          "format": "%Y-%m-%dT%H:%M"
        }
      }
    }
  },
                        {"$match": base_match_conditions},
                        {"$addFields": {
                            "source_table": "AG",
                            "mktFile": {"$concat": ["$scrape_file_name", "_AG"]}
                        }},
                        {"$project": {
                            "gross_fare": 1, "outbound_airline_code": 1, "outbound_flight": 1,
                            "departure_date": 1, "destination_airport": 1, "source_airport": 1,
                            "channel": 1, "outbound_departuretime": 1, "created_at": 1,
                            "mktFile": 1, "source_table": 1
                        }}
                    ]
                    
                    ag_cursor = self.db.market_fares_ag.aggregate(ag_pipeline)
                    ag_results = list(ag_cursor)
                    all_results.extend(ag_results)
                    self.logger.info(f"Found {len(ag_results)} records from AG collection")
                    
                except Exception as e:
                    self.logger.error(f"Error querying AG collection: {e}")
 
            # Query RG collection if it exists
            if rg_exists:
                try:
                    rg_pipeline = [
                        {
    "$set": {
      "outbound_departuretime": {
        "$dateFromString": {
          "dateString": {
            "$concat": ["1970-01-01T", "$outbound_departuretime"]
          },
          "format": "%Y-%m-%dT%H:%M"
        }
      }
    }
  },
                        {"$match": base_match_conditions},
                        {"$addFields": {
                            "source_table": "RG",
                            "mktFile": {"$concat": ["$scrape_file_name", "_RG"]}
                        }},
                        {"$project": {
                            "gross_fare": 1, "outbound_airline_code": 1, "outbound_flight": 1,
                            "departure_date": 1, "destination_airport": 1, "source_airport": 1,
                            "channel": 1, "outbound_departuretime": 1, "created_at": 1,
                            "mktFile": 1, "source_table": 1
                        }}
                    ]
                    
                    rg_cursor = self.db.market_fares_rg.aggregate(rg_pipeline)
                    rg_results = list(rg_cursor)
                    all_results.extend(rg_results)
                    self.logger.info(f"Found {len(rg_results)} records from RG collection")
                    
                except Exception as e:
                    self.logger.error(f"Error querying RG collection: {e}")
 
            if not all_results:
                self.logger.warning("No market fare records found")
                return pd.DataFrame(columns=['gross_fare', 'outbound_airline_code', 'outbound_flight', 'mktFile'])
 
 
            # Convert to DataFrame and process
            df = pd.DataFrame(all_results)
            
            # Sort by partition columns and created_at descending
            df = df.sort_values([
                'channel', 'departure_date', 'destination_airport',
                'source_airport', 'outbound_flight', 'outbound_departuretime',
                'created_at'
            ], ascending=[True, True, True, True, True, True, False])
            
            # Remove duplicates keeping the first (latest) record
            df_dedup = df.drop_duplicates(
                subset=['channel', 'departure_date', 'destination_airport',
                    'source_airport', 'outbound_flight', 'outbound_departuretime'],
                keep='first'
            )
            
            # Select required columns and sort by gross_fare
            mktFareRange = df_dedup[['gross_fare', 'outbound_airline_code', 'outbound_flight', 'mktFile']].copy()
            mktFareRange = mktFareRange.sort_values('gross_fare', ascending=True).reset_index(drop=True)
            
            self.logger.info(f"Returning {len(mktFareRange)} unique market fare records")
            
            return mktFareRange
            
        except Exception as e:
            self.logger.error(f"Error in getMarketFareRange: {e}")
            return pd.DataFrame()

    def getMarketFareRangeConnections(self, channel, currParams, currFltDateStr, outBoundStop, outboundDuration):
        currFltDate = currFltDateStr
        origin = currParams.sector1[:3]
        destination = currParams.sector2[3:]
        
        if channel == B2B_CHANNEL:
            carr_exclusion = currParams.carrierExclusionB2B
        else:
            carr_exclusion = currParams.carrierExclusionB2C
        
        # Convert carrier exclusion to list for MongoDB $nin operator
        carr_exclusion_list = carr_exclusion.split('/')
        
        # Common query conditions for both collections
        base_query = {
            "source_airport": origin,
            "destination_airport": destination,
            "channel": channel,
            "outbound_airline_code": {
                "$nin": carr_exclusion_list + ["QP", "G8"]
            },
            "outbound_stop": {"$lte": int(outBoundStop)},
            "outbound_duration": {"$lte": int(outboundDuration)},
            "departure_date": currFltDate
        }
        
        # Add time interval condition for non-test data
        if hasattr(self, 'inputSource') and self.inputSource.input_source.get(CONST_MARKET_FARES) != TEST_DATA:
            two_days_ago = datetime.now() - timedelta(days=2)
            base_query["created_at"] = {"$gt": two_days_ago}
        
        try:
            # MongoDB aggregation pipeline to replicate the SQL logic
            pipeline = [
                # Stage 1: Match documents from market_fares_ag collection
                {
                    "$match": base_query
                },
                # Stage 2: Add source_table field and transform mktFile
                {
                    "$addFields": {
                        "source_table": "AG",
                        "mktFile": {"$concat": ["$scrape_file_name", "_AG"]}
                    }
                },
                # Stage 3: Union with market_fares_rg collection
                {
                    "$unionWith": {
                        "coll": "market_fares_rg",
                        "pipeline": [
                            {"$match": base_query},
                            {
                                "$addFields": {
                                    "source_table": "RG",
                                    "mktFile": {"$concat": ["$scrape_file_name", "_RG"]}
                                }
                            }
                        ]
                    }
                },
                # Stage 4: Sort by created_at descending to get latest records first
                {
                    "$sort": {
                        "channel": 1,
                        "departure_date": 1,
                        "destination_airport": 1,
                        "source_airport": 1,
                        "outbound_flight": 1,
                        "outbound_departuretime": 1,
                        "created_at": -1
                    }
                },
                # Stage 5: Group by the partition fields and take the first (latest) document
                {
                    "$group": {
                        "_id": {
                            "channel": "$channel",
                            "departure_date": "$departure_date",
                            "destination_airport": "$destination_airport",
                            "source_airport": "$source_airport",
                            "outbound_flight": "$outbound_flight",
                            "outbound_departuretime": "$outbound_departuretime"
                        },
                        "gross_fare": {"$first": "$gross_fare"},
                        "outbound_airline_code": {"$first": "$outbound_airline_code"},
                        "outbound_flight": {"$first": "$outbound_flight"},
                        "currency": {"$first": "$currency"},
                        "mktFile": {"$first": "$mktFile"}
                    }
                },
                # Stage 6: Project only the required fields
                {
                    "$project": {
                        "_id": 0,
                        "gross_fare": 1,
                        "outbound_airline_code": 1,
                        "outbound_flight": 1,
                        "currency": 1,
                        "mktFile": 1
                    }
                },
                # Stage 7: Sort by gross_fare ascending
                {
                    "$sort": {"gross_fare": 1}
                }
            ]
            
            # Execute the aggregation pipeline
            # Assuming self.db is your MongoDB database connection
            result = list(self.db.market_fares_ag.aggregate(pipeline))
            
            # Convert to pandas DataFrame
            mktFareRange = pd.DataFrame(result)
            
        except Exception as e:
            self.logger.error(e)
            mktFareRange = pd.DataFrame()  # Return empty DataFrame on error
        
        return mktFareRange

    def getRoutedDestination(self, origin, destination):
        query = f'''
            SELECT routedSector 
            FROM config_marketFares_rerouting 
            WHERE sector = concat('{origin}','{destination}')
        '''
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()
