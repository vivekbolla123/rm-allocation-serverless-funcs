import pandas as pd
from datetime import datetime, timedelta, date

class LadderEsbHelper:
    """Handles market fare data collection from MongoDB collections."""

    def __init__(self, db_fares, logger):
        self.db_fares = db_fares
        self.logger = logger
        self.failure_reason = None

    def collect_combined_market_fare_data(self, ndo, sector, channel):
        """Query market_fares_rg and market_fares_ag for fare data within NDO days from today.
        
        Args:
            ndo: Number of days out (e.g. 30 or 60) — departure_date range is today to today+ndo.
            sector: 6-char sector string e.g. 'BOMDEL'.
            channel: 'B2C' or 'B2B'.
            
        Returns:
            DataFrame with columns: Fare, departure_date, source_airport, destination_airport, data_source
            sorted by Fare ascending.
        """
        try:
            origin = sector[:3]
            destination = sector[3:]

            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = today + timedelta(days=int(ndo))

            # Build match conditions
            match_conditions = {
                "source_airport": origin,
                "destination_airport": destination,
                "channel": channel,
                "departure_date": {
                    "$gte": today,
                    "$lte": end_date
                },
                "outbound_stop": "0",
                "outbound_airline_code": {
                    "$in": ["6E", "AI", "IX", "XY", "FZ", "GF", "J9", "KU", "FD", "SG", "TG", "WY", "VZ", "SV", "F3"]
                }
            }

            # Projection — keep only what we need
            projection = {
                "_id": 0,
                "gross_fare": 1,
                "departure_date": 1,
                "source_airport": 1,
                "destination_airport": 1
            }

            all_results = []

            # Check which collections exist
            collection_names = self.db_fares.list_collection_names()
            ag_exists = 'market_fares_ag' in collection_names
            rg_exists = 'market_fares_rg' in collection_names

            if not ag_exists and not rg_exists:
                self.logger.warning("Neither market_fares_ag nor market_fares_rg collections found")
                return pd.DataFrame(columns=['Fare', 'departure_date', 'source_airport', 'destination_airport', 'data_source'])

            # Query AG collection
            if ag_exists:
                try:
                    ag_pipeline = [
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
                        {"$addFields": {"data_source": "AG"}},
                        {"$project": {**projection, "data_source": 1}}
                    ]
                    ag_cursor = self.db_fares.market_fares_ag.aggregate(ag_pipeline, allowDiskUse=True)
                    ag_results = list(ag_cursor)
                    all_results.extend(ag_results)
                    self.logger.info(f"Found {len(ag_results)} records from market_fares_ag")
                except Exception as e:
                    self.logger.error(f"Error querying market_fares_ag: {e}")

            # Query RG collection
            if rg_exists:
                try:
                    rg_pipeline = [
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
                        {"$addFields": {"data_source": "RG"}},
                        {"$project": {**projection, "data_source": 1}}
                    ]
                    rg_cursor = self.db_fares.market_fares_rg.aggregate(rg_pipeline, allowDiskUse=True)
                    rg_results = list(rg_cursor)
                    all_results.extend(rg_results)
                    self.logger.info(f"Found {len(rg_results)} records from market_fares_rg")
                except Exception as e:
                    self.logger.error(f"Error querying market_fares_rg: {e}")

            if not all_results:
                self.logger.warning(f"No market fare records found for {sector}, channel={channel}, NDO={ndo}")
                return pd.DataFrame(columns=['Fare', 'departure_date', 'source_airport', 'destination_airport', 'data_source'])

            # Convert to DataFrame
            df = pd.DataFrame(all_results)

            # Rename gross_fare -> Fare to keep downstream compatibility
            df = df.rename(columns={'gross_fare': 'Fare'})

            # Drop rows where Fare conversion failed (None values)
            df = df.dropna(subset=['Fare'])

            # Sort by Fare ascending
            df = df.sort_values('Fare', ascending=True).reset_index(drop=True)

            self.logger.info(f"Returning {len(df)} market fare records for {sector}, channel={channel}, NDO={ndo}")
            return df

        except Exception as e:
            self.logger.error(f"Error collecting market fare data: {str(e)}")
            self.failure_reason = f"Market fare data collection failed: {str(e)}"
            raise