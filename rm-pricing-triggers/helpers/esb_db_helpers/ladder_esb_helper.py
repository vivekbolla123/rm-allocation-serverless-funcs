import pandas as pd
from datetime import datetime, timedelta, date

class LadderEsbHelper:
    """Handles Navitaire-related database operations and market fare collection."""

    def __init__(self, rm_rd_esb_conn, db_fares, logger):
        self.rm_rd_esb_conn = rm_rd_esb_conn
        self.db_fares = db_fares
        self.logger = logger
        self.failure_reason = None

    def collect_combined_market_fare_data(self, ndo, sector, channel, max_fare_threshold=None, layer_type='direct', carrier_inclusion=None):
        """Query market_fares_rg and market_fares_ag for fare data within NDO days from today.
        
        Args:
            ndo: Number of days out (e.g. 30 or 60) — departure_date range is today to today+ndo.
            sector: 6-char sector string e.g. 'BOMDEL'.
            channel: 'B2C' or 'B2B'.
            max_fare_threshold: Maximum fare threshold. If provided, only fares <= this value are returned.
            
        Returns:
            DataFrame with columns: Fare, departure_date, source_airport, destination_airport, data_source
            sorted by Fare ascending.
        """
        if carrier_inclusion is None:
            carrier_inclusion = []
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
                "outbound_stop": {"$in": ["0", "1"]} if layer_type == 'connection' else "0",
                "outbound_airline_code": {
                    "$in": carrier_inclusion if (layer_type == 'connection' and carrier_inclusion) else ["6E", "AI", "IX", "XY", "FZ", "GF", "J9", "KU", "FD", "SG", "TG", "WY", "VZ", "SV", "F3"]
                }
            }

            # Log fare threshold if provided (applied after type conversion in pipeline)
            if max_fare_threshold is not None:
                self.logger.info(f"Applying max fare threshold: gross_fare <= {max_fare_threshold}")

            # Projection — keep only what we need
            projection = {
                "_id": 0,
                "gross_fare": 1,
                "departure_date": 1,
                "source_airport": 1,
                "destination_airport": 1,
                "outbound_flight_number": 1
            }

            all_results = []

            # Check which collections exist
            collection_names = self.db_fares.list_collection_names()
            ag_exists = 'market_fares_ag' in collection_names
            rg_exists = 'market_fares_rg' in collection_names

            if not ag_exists and not rg_exists:
                self.logger.warning("Neither market_fares_ag nor market_fares_rg collections found")
                return pd.DataFrame(columns=['Fare', 'departure_date', 'source_airport', 'destination_airport', 'outbound_flight_number', 'data_source'])

            # Build fare threshold match stage (applied after $convert so gross_fare is numeric)
            fare_threshold_stage = []
            if max_fare_threshold is not None:
                fare_threshold_stage = [{"$match": {"gross_fare": {"$lte": max_fare_threshold}}}]

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
                        *fare_threshold_stage,
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
                        *fare_threshold_stage,
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
                return pd.DataFrame(columns=['Fare', 'departure_date', 'source_airport', 'destination_airport', 'outbound_flight_number', 'data_source'])

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

    def collect_combined_market_fare_data_bidirectional(self, ndo, sector, reverse_sector, channel, max_fare_threshold=None, conversion_rate=1.0, layer_type='direct', carrier_inclusion=None):
        """Fetch fares for both sector directions and normalize all to INR.
        
        Args:
            ndo: Number of days out.
            sector: Forward sector e.g. 'BOMHKT' (fares assumed in INR).
            reverse_sector: Reverse sector e.g. 'HKTBOM' (fares in foreign currency).
            channel: 'B2C' or 'B2B'.
            max_fare_threshold: Max fare in INR. Applied after currency conversion.
            conversion_rate: INR -> foreign currency rate (e.g. INR -> THB).
                             Reverse fares are converted to INR by dividing by this rate.
        
        Returns:
            DataFrame with all fares normalized to INR, sorted ascending.
        """
        try:
            # 1. Forward sector fares (already in INR) — no threshold at query level,
            #    we apply it after since forward fares are already in INR
            forward_df = self.collect_combined_market_fare_data(ndo, sector, channel, layer_type=layer_type, carrier_inclusion=carrier_inclusion)
            forward_count = len(forward_df) if not forward_df.empty else 0
            self.logger.info(f"Forward sector {sector}: {forward_count} records")

            # 2. Reverse sector fares (in foreign currency)
            reverse_df = self.collect_combined_market_fare_data(ndo, reverse_sector, channel, layer_type=layer_type, carrier_inclusion=carrier_inclusion)
            reverse_count = len(reverse_df) if not reverse_df.empty else 0
            self.logger.info(f"Reverse sector {reverse_sector}: {reverse_count} records")

            # 3. Convert reverse fares to INR
            if not reverse_df.empty and conversion_rate != 1.0:
                reverse_df['Fare'] = reverse_df['Fare'] / conversion_rate
                self.logger.info(f"Converted {reverse_count} reverse fares to INR (÷ {conversion_rate})")

            # 4. Combine both directions
            if forward_df.empty and reverse_df.empty:
                self.logger.warning(f"No market fare records found for {sector} or {reverse_sector}, channel={channel}, NDO={ndo}")
                return pd.DataFrame(columns=['Fare', 'departure_date', 'source_airport', 'destination_airport', 'outbound_flight_number', 'data_source'])

            combined_df = pd.concat([forward_df, reverse_df], ignore_index=True)

            # 5. Apply fare threshold in INR on the combined data
            if max_fare_threshold is not None:
                before_count = len(combined_df)
                combined_df = combined_df[combined_df['Fare'] <= max_fare_threshold]
                after_count = len(combined_df)
                self.logger.info(f"Fare threshold {max_fare_threshold} INR: {before_count} -> {after_count} records")

            # 6. Drop nulls and sort
            combined_df = combined_df.dropna(subset=['Fare'])
            combined_df = combined_df.sort_values('Fare', ascending=True).reset_index(drop=True)

            self.logger.info(f"Bidirectional total: {len(combined_df)} records for {sector}+{reverse_sector}, channel={channel}, NDO={ndo}")
            return combined_df

        except Exception as e:
            self.logger.error(f"Error in bidirectional market fare collection: {str(e)}")
            self.failure_reason = f"Bidirectional market fare collection failed: {str(e)}"
            raise

    def collect_combined_market_fare_data_by_dates(self, start_date, end_date, sector, channel, max_fare_threshold=None, layer_type='direct', carrier_inclusion=None, carrier_exclusion=None):
        """Query market_fares_rg and market_fares_ag using explicit departure_date range.

        Args:
            start_date: Start date string 'YYYY-MM-DD' for departure_date range.
            end_date: End date string 'YYYY-MM-DD' for departure_date range.
            sector: 6-char sector string e.g. 'BOMDEL'.
            channel: 'B2C' or 'B2B'.
            max_fare_threshold: Maximum fare threshold.
            carrier_inclusion: List of airline codes to include (used if provided).
            carrier_exclusion: List of airline codes to exclude (used if provided).

        Returns:
            DataFrame with columns: Fare, departure_date, source_airport, destination_airport, data_source
        """
        if carrier_inclusion is None:
            carrier_inclusion = []
        if carrier_exclusion is None:
            carrier_exclusion = []
        try:
            origin = sector[:3]
            destination = sector[3:]

            start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59, microsecond=999999)

            # Build match conditions
            match_conditions = {
                "source_airport": origin,
                "destination_airport": destination,
                "channel": channel,
                "departure_date": {
                    "$gte": start_dt,
                    "$lte": end_dt
                },
                "outbound_stop": {"$in": ["0", "1"]} if layer_type == 'connection' else "0",
            }

            # Apply carrier filters dynamically
            if carrier_inclusion:
                match_conditions["outbound_airline_code"] = {"$in": carrier_inclusion}
            elif carrier_exclusion:
                match_conditions["outbound_airline_code"] = {"$nin": carrier_exclusion}

            projection = {
                "_id": 0,
                "gross_fare": 1,
                "departure_date": 1,
                "source_airport": 1,
                "destination_airport": 1,
                "outbound_flight_number": 1
            }

            all_results = []
            collection_names = self.db_fares.list_collection_names()
            ag_exists = 'market_fares_ag' in collection_names
            rg_exists = 'market_fares_rg' in collection_names

            if not ag_exists and not rg_exists:
                self.logger.warning("Neither market_fares_ag nor market_fares_rg collections found")
                return pd.DataFrame(columns=['Fare', 'departure_date', 'source_airport', 'destination_airport', 'outbound_flight_number', 'data_source'])

            fare_threshold_stage = []
            if max_fare_threshold is not None:
                fare_threshold_stage = [{"$match": {"gross_fare": {"$lte": max_fare_threshold}}}]

            if ag_exists:
                try:
                    ag_pipeline = [
                        {"$match": match_conditions},
                        {"$addFields": {"gross_fare": {"$convert": {"input": "$gross_fare", "to": "double", "onError": None, "onNull": None}}}},
                        *fare_threshold_stage,
                        {"$addFields": {"data_source": "AG"}},
                        {"$project": {**projection, "data_source": 1}}
                    ]
                    ag_results = list(self.db_fares.market_fares_ag.aggregate(ag_pipeline, allowDiskUse=True))
                    all_results.extend(ag_results)
                    self.logger.info(f"Found {len(ag_results)} records from market_fares_ag")
                except Exception as e:
                    self.logger.error(f"Error querying market_fares_ag: {e}")

            if rg_exists:
                try:
                    rg_pipeline = [
                        {"$match": match_conditions},
                        {"$addFields": {"gross_fare": {"$convert": {"input": "$gross_fare", "to": "double", "onError": None, "onNull": None}}}},
                        *fare_threshold_stage,
                        {"$addFields": {"data_source": "RG"}},
                        {"$project": {**projection, "data_source": 1}}
                    ]
                    rg_results = list(self.db_fares.market_fares_rg.aggregate(rg_pipeline, allowDiskUse=True))
                    all_results.extend(rg_results)
                    self.logger.info(f"Found {len(rg_results)} records from market_fares_rg")
                except Exception as e:
                    self.logger.error(f"Error querying market_fares_rg: {e}")

            if not all_results:
                self.logger.warning(f"No market fare records found for {sector}, channel={channel}, dates={start_date} to {end_date}")
                return pd.DataFrame(columns=['Fare', 'departure_date', 'source_airport', 'destination_airport', 'outbound_flight_number', 'data_source'])

            df = pd.DataFrame(all_results)
            df = df.rename(columns={'gross_fare': 'Fare'})
            df = df.dropna(subset=['Fare'])
            df = df.sort_values('Fare', ascending=True).reset_index(drop=True)

            self.logger.info(f"Returning {len(df)} market fare records for {sector}, channel={channel}, dates={start_date} to {end_date}")
            return df

        except Exception as e:
            self.logger.error(f"Error collecting market fare data by dates: {str(e)}")
            self.failure_reason = f"Market fare data collection by dates failed: {str(e)}"
            raise

    def collect_combined_market_fare_data_bidirectional_by_dates(self, start_date, end_date, sector, reverse_sector, channel, max_fare_threshold=None, conversion_rate=1.0, layer_type='direct', carrier_inclusion=None, carrier_exclusion=None):
        """Fetch fares for both sector directions using explicit date range and normalize to INR.

        Args:
            start_date: Start date string 'YYYY-MM-DD'.
            end_date: End date string 'YYYY-MM-DD'.
            sector: Forward sector e.g. 'BOMHKT'.
            reverse_sector: Reverse sector e.g. 'HKTBOM'.
            channel: 'B2C' or 'B2B'.
            max_fare_threshold: Max fare in INR.
            conversion_rate: INR -> foreign currency rate.
            carrier_inclusion: List of airline codes to include.
            carrier_exclusion: List of airline codes to exclude.

        Returns:
            DataFrame with all fares normalized to INR, sorted ascending.
        """
        try:
            forward_df = self.collect_combined_market_fare_data_by_dates(
                start_date, end_date, sector, channel,
                layer_type=layer_type, carrier_inclusion=carrier_inclusion, carrier_exclusion=carrier_exclusion
            )
            forward_count = len(forward_df) if not forward_df.empty else 0
            self.logger.info(f"Forward sector {sector}: {forward_count} records")

            reverse_df = self.collect_combined_market_fare_data_by_dates(
                start_date, end_date, reverse_sector, channel,
                layer_type=layer_type, carrier_inclusion=carrier_inclusion, carrier_exclusion=carrier_exclusion
            )
            reverse_count = len(reverse_df) if not reverse_df.empty else 0
            self.logger.info(f"Reverse sector {reverse_sector}: {reverse_count} records")

            if not reverse_df.empty and conversion_rate != 1.0:
                reverse_df['Fare'] = reverse_df['Fare'] / conversion_rate
                self.logger.info(f"Converted {reverse_count} reverse fares to INR (÷ {conversion_rate})")

            if forward_df.empty and reverse_df.empty:
                self.logger.warning(f"No market fare records found for {sector} or {reverse_sector}, channel={channel}, dates={start_date} to {end_date}")
                return pd.DataFrame(columns=['Fare', 'departure_date', 'source_airport', 'destination_airport', 'outbound_flight_number', 'data_source'])

            combined_df = pd.concat([forward_df, reverse_df], ignore_index=True)

            if max_fare_threshold is not None:
                before_count = len(combined_df)
                combined_df = combined_df[combined_df['Fare'] <= max_fare_threshold]
                after_count = len(combined_df)
                self.logger.info(f"Fare threshold {max_fare_threshold} INR: {before_count} -> {after_count} records")

            combined_df = combined_df.dropna(subset=['Fare'])
            combined_df = combined_df.sort_values('Fare', ascending=True).reset_index(drop=True)

            self.logger.info(f"Bidirectional total: {len(combined_df)} records for {sector}+{reverse_sector}, channel={channel}, dates={start_date} to {end_date}")
            return combined_df

        except Exception as e:
            self.logger.error(f"Error in bidirectional market fare collection by dates: {str(e)}")
            self.failure_reason = f"Bidirectional market fare collection by dates failed: {str(e)}"
            raise

    def collect_combined_booking_data(self, start_date, end_date, sector, channel):
        """Optimized single query for both flown and booked data."""
        if(channel == "B2B"):
            fareBasis = f"""AND LEFT(PX.FareBasis, 1) = 'Z'"""
        else:
            fareBasis = f"""AND SUBSTRING(PX.FareBasis, 5, 1) = 'R'"""
        
        try:
            wherecondition = f"""WHERE PX.BookingDate >= '{start_date}' AND PX.BookingDate < '{end_date}'
                    {fareBasis}
                    AND LEFT(PX.FareBasis, 1) NOT IN ('S', 'W')
                    AND SUBSTRING(PX.FareBasis, 3, 1) NOT IN ('C')
                    AND CONCAT(PX.Origin, PX.Destination) = '{sector}'
                    AND PX.BookingID IS NOT NULL
                    AND IL.FlightNumber < 7000"""
                    
            query = f"""
            SELECT Fare, FareBasis, BookingDate, Origin, Destination, data_source
            FROM (
                SELECT
                    PX.DiscountedBaseFare_Pro AS Fare,
                    PX.FareBasis, PX.BookingDate, PX.Origin, PX.Destination, 'FLOWN' as data_source
                FROM QP_DL_NAVDATA.QP_DL_FLOWNDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {wherecondition}
                    
                UNION ALL
                
                SELECT
                    PX.DiscountedBaseFare_Pro AS Fare,
                    PX.FareBasis, PX.BookingDate, PX.Origin, PX.Destination, 'BOOKED' as data_source
                FROM QP_DL_NAVDATA.QP_DL_BOOKEDDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {wherecondition}
            ) combined_bookings
            ORDER BY Fare ASC
            """
            
            combined_data = pd.read_sql(query, self.rm_rd_esb_conn)
            
            return combined_data
            
        except Exception as e:
            self.logger.error(f"Error collecting booking data: {str(e)}")
            self.failure_reason = f"Booking data collection failed: {str(e)}"
            raise
    
    def collect_combined_booking_data_bidirectional(self, start_date, end_date, sector, reverse_sector, channel):
        """Collect booking data for both directions and combine them."""
        if(channel == "B2B"):
            fareBasis = f"""AND LEFT(PX.FareBasis, 1) = 'Z'"""
        else:
            fareBasis = f"""AND SUBSTRING(PX.FareBasis, 5, 1) = 'R'"""
        
        try:
            # Collect data for both directions
            wherecondition = f"""WHERE PX.BookingDate >= '{start_date}' AND PX.BookingDate < '{end_date}'
                    {fareBasis}
                    AND LEFT(PX.FareBasis, 1) NOT IN ('S', 'W')
                    AND SUBSTRING(PX.FareBasis, 3, 1) NOT IN ('C')
                    AND CONCAT(PX.Origin, PX.Destination) IN ('{sector}', '{reverse_sector}')
                    AND PX.BookingID IS NOT NULL
                    AND IL.FlightNumber < 7000"""
                    
            query = f"""
            SELECT Fare, FareBasis, BookingDate, Origin, Destination, data_source
            FROM (
                SELECT
                    PX.DiscountedBaseFare_Pro AS Fare,
                    PX.FareBasis, PX.BookingDate, PX.Origin, PX.Destination, 'FLOWN' as data_source
                FROM QP_DL_NAVDATA.QP_DL_FLOWNDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {wherecondition}
                    
                UNION ALL
                
                SELECT
                    PX.DiscountedBaseFare_Pro AS Fare,
                    PX.FareBasis, PX.BookingDate, PX.Origin, PX.Destination, 'BOOKED' as data_source
                FROM QP_DL_NAVDATA.QP_DL_BOOKEDDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {wherecondition}
            ) combined_bookings
            ORDER BY Fare ASC
            """
            
            combined_data = pd.read_sql(query, self.rm_rd_esb_conn)
            
            self.logger.info(f"Collected bidirectional booking data: {len(combined_data)} records for {sector} + {reverse_sector}")
            
            return combined_data
            
        except Exception as e:
            self.logger.error(f"Error collecting bidirectional booking data: {str(e)}")
            self.failure_reason = f"Bidirectional booking data collection failed: {str(e)}"
            raise