import pandas as pd
from datetime import datetime, timedelta
from Constants import *
import logging

class MarketFareMongoDB:
    def __init__(self,db,inputSource):
        self.db = db
        self.logger = logging.getLogger(__name__)
        self.gsi_name = 'ChannelDepartureDateGSI'
        self.inputSource=inputSource

    def _convert_gross_fare_to_int(self, df):
        """Helper method to safely convert gross_fare column to int"""
        if 'gross_fare' in df.columns and not df.empty:
            try:
                # Convert to numeric, replacing any non-numeric values with NaN
                df['gross_fare'] = pd.to_numeric(df['gross_fare'], errors='coerce')
                
                # Drop rows where gross_fare is NaN (was non-numeric)
                df = df.dropna(subset=['gross_fare'])
                
                # Convert to int
                df['gross_fare'] = df['gross_fare'].astype(int)
                
                self.logger.info(f"Successfully converted gross_fare to int for {len(df)} records")
                
            except Exception as e:
                self.logger.error(f"Error converting gross_fare to int: {e}")
                # Return empty DataFrame with correct columns if conversion fails completely
                return pd.DataFrame(columns=df.columns)
        
        return df

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
                "outbound_stop": '0'
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
        "gross_fare": {
            "$convert": {
                "input": "$gross_fare",
                "to": "double",
                "onError": None,
                "onNull": None
            }
        }
    }},
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
            mktFareRange = self._convert_gross_fare_to_int(mktFareRange)
            mktFareRange = mktFareRange.sort_values('gross_fare', ascending=True).reset_index(drop=True)
            
            self.logger.info(f"Returning {len(mktFareRange)} unique market fare records")
            
            return mktFareRange
            
        except Exception as e:
            self.logger.error(f"Error in getMarketFareRange: {e}")
            return pd.DataFrame()

    def getMarketFareRangeConnections(self, channel, currParams, currFltDateStr, outBoundStop, outboundDuration):
        """
        Fetches market fare data for flights with connections using a PyMongo aggregation pipeline.
        This is a direct conversion of the original SQL-based function.

        - Converts 'created_at' string to a date for filtering.
        - Handles deduplication by keeping the most recent record for each unique flight.
        """
        try:
            # --- 1. Parameter Setup (Matches original SQL function) ---
            origin = currParams.sector1[:3]
            destination = currParams.sector2[3:]
            
            if channel == B2B_CHANNEL:
                carr_exclusion = currParams.carrierExclusionB2B
            else:
                carr_exclusion = currParams.carrierExclusionB2C
    
            carr_exclusion_list = carr_exclusion.split('/') if carr_exclusion else []
            
            # Convert the flight date string to datetime objects for the start and end of that day.
            # This is the robust way to match "departure_date = 'YYYY-MM-DD'" in MongoDB.
            fltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()
            start_of_day = datetime.combine(fltDate, datetime.min.time())
            end_of_day = datetime.combine(fltDate, datetime.max.time())

            self.logger.info(
                f"Searching connection fares: {origin}-{destination}, Channel: {channel}, "
                f"Date: {currFltDateStr}, Max Stops: {outBoundStop}, Max Duration: {outboundDuration}"
            )

            # --- 2. Check for Collection Existence ---
            collection_names = self.db.list_collection_names()
            ag_exists = 'market_fares_ag' in collection_names
            rg_exists = 'market_fares_rg' in collection_names

            if not ag_exists and not rg_exists:
                self.logger.warning("Neither market_fares_ag nor market_fares_rg collections found.")
                return pd.DataFrame()

            # --- 3. Build the MongoDB Aggregation Pipeline ---
            
            # Stage to convert the 'created_at' string to a date object.
        
            date_conversion_stage = {
                "$set": {
                    "created_at": {
                        "$dateFromString": {
                            "dateString": "$created_at",
                            "format": "%Y-%m-%d %H:%M:%S"  # <-- EXAMPLE: "2023-10-27 15:45:00"
                        }
                    }
                }
            }

            # Build the main matching conditions
            match_conditions = {
                "source_airport": origin,
                "destination_airport": destination,
                "channel": channel,
                "outbound_airline_code": {"$nin": carr_exclusion_list + ["QP", "G8"]},
                "outbound_stop": {"$lte": f"{outBoundStop}"},
                "outbound_duration": {"$lte": int(outboundDuration)},
                "departure_date": {
                    "$gte": start_of_day,
                    "$lte": end_of_day
                }
            }
            
            # Add time interval condition for non-test data (matches SQL's INTERVAL 2 DAY)
            if hasattr(self, 'inputSource') and self.inputSource.input_source.get(CONST_MARKET_FARES) != TEST_DATA:
                two_days_ago = datetime.now() - timedelta(days=2)
                match_conditions["created_at"] = {"$gt": two_days_ago}

            # Define the final fields to be returned from the database
            projection = {
                "_id": 0,
                "gross_fare": 1, "outbound_airline_code": 1, "outbound_flight": 1,
                "currency": 1, "destination_airport": 1, "source_airport": 1,
                "channel": 1, "outbound_departuretime": 1, "departure_date": 1,
                "created_at": 1, "mktFile": 1, "source_table": 1
            }
            
            # --- 4. Query Collections and Combine ---
            all_results = []

            # Query AG collection
            if ag_exists:
                try:
                    ag_pipeline = [
                        date_conversion_stage,
                        {"$match": match_conditions},
                         {"$addFields": {
        "gross_fare": {
            "$convert": {
                "input": "$gross_fare",
                "to": "double",
                "onError": None,
                "onNull": None
            }
        }
    }},
                        {"$addFields": {
                            "source_table": "AG",
                            "mktFile": {"$concat": ["$scrape_file_name", "_AG"]}
                        }},
                        {"$project": projection}
                    ]
                    ag_cursor = self.db.market_fares_ag.aggregate(ag_pipeline)
                    ag_results = list(ag_cursor)
                    all_results.extend(ag_results)
                    self.logger.info(f"Found {len(ag_results)} connection records from AG collection.")
                except Exception as e:
                    self.logger.error(f"Error querying AG collection for connections: {e}")

            # Query RG collection
            if rg_exists:
                try:
                    rg_pipeline = [
                        date_conversion_stage,
                        {"$match": match_conditions},
                        {"$addFields": {
                            "source_table": "RG",
                            "mktFile": {"$concat": ["$scrape_file_name", "_RG"]}
                        }},
                        {"$project": projection}
                    ]
                    rg_cursor = self.db.market_fares_rg.aggregate(rg_pipeline)
                    rg_results = list(rg_cursor)
                    all_results.extend(rg_results)
                    self.logger.info(f"Found {len(rg_results)} connection records from RG collection.")
                except Exception as e:
                    self.logger.error(f"Error querying RG collection for connections: {e}")
            
            if not all_results:
                self.logger.warning("No market fare connection records found for the given criteria.")
                return pd.DataFrame(columns=['gross_fare', 'outbound_airline_code', 'outbound_flight', 'currency', 'mktFile'])

            # --- 5. Deduplicate and Sort in Pandas (Replicates SQL Window Function) ---
            df = pd.DataFrame(all_results)
            
            # These columns define a unique flight, as in the SQL PARTITION BY clause
            partition_cols = [
                'channel', 'departure_date', 'destination_airport',
                'source_airport', 'outbound_flight', 'outbound_departuretime'
            ]
            
            # Sort to bring the latest record for each flight to the top
            df = df.sort_values(by=partition_cols + ['created_at'], ascending=False)
            
            # Keep only the first (latest) record for each unique flight
            df_dedup = df.drop_duplicates(subset=partition_cols, keep='first')
            
            # Select final columns and sort by price, as in the final SQL ORDER BY
            mktFareRange = df_dedup[['gross_fare', 'outbound_airline_code', 'outbound_flight', 'currency', 'mktFile']].copy()
            mktFareRange = self._convert_gross_fare_to_int(mktFareRange)
            mktFareRange = mktFareRange.sort_values('gross_fare', ascending=True).reset_index(drop=True)
            
            self.logger.info(f"Returning {len(mktFareRange)} unique market fare connection records.")
            
            return mktFareRange

        except Exception as e:
            self.logger.error(f"An unexpected error occurred in getMarketFareRangeConnections: {e}")
            return pd.DataFrame()

    def getRoutedDestination(self, origin, destination):
    
        try:
            collection = self.db['config_marketFares_rerouting']
            
            # Concatenate origin and destination
            sector = f'{origin}{destination}'
            
            # Query MongoDB
            results = collection.find(
                {'sector': sector},
                {'_id': 0, 'routedSector': 1}  # Only return routedSector field
            )
            
            # Convert to DataFrame
            df = pd.DataFrame(list(results))
            
            # If no results, return empty DataFrame with proper column
            if df.empty:
                df = pd.DataFrame(columns=['routedSector'])
            
            return df
            
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame(columns=['routedSector'])
