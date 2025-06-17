import boto3
import pandas as pd
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import logging
import time
import random
from botocore.exceptions import ClientError

class MarketFareDynamoDB:
    def __init__(self, conn, dynamodb):
        self.dynamodb = dynamodb
        self.market_fares_ag = self.dynamodb.Table('MarketFaresRG')
        self.market_fares_rg = self.dynamodb.Table('MarketFaresAGInt')
        self.logger = logging.getLogger(__name__)
        self.conn = conn
        
    def retry_with_backoff(self, func, max_retries=5, base_delay=1, max_delay=60):
        """
        Retry function with exponential backoff for DynamoDB throttling
        """
        for attempt in range(max_retries):
            try:
                return func()
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['ThrottlingException', 'ProvisionedThroughputExceededException']:
                    if attempt == max_retries - 1:
                        self.logger.error(f"Max retries exceeded for DynamoDB operation: {e}")
                        raise e
                    
                    # Calculate delay with jitter
                    delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                    self.logger.warning(f"DynamoDB throttling detected, retrying in {delay:.2f} seconds... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    raise e
            except Exception as e:
                self.logger.error(f"Non-retryable error in DynamoDB operation: {e}")
                raise e
        
        raise Exception(f"Max retries ({max_retries}) exceeded")

    def getMarketFareRange(self, channel, currParams, currFltDateStr, DaySpan, startTime, endtime, Day3Flag, hrs):
        """
        DynamoDB version of getMarketFareRange method with optimized querying
        """
        try:
            currFltDate = datetime.strptime(currFltDateStr, '%m/%d/%Y').date()
            updateDate = currFltDate + timedelta(days=int(DaySpan))

            if not Day3Flag:
                startDateTime = f"{currFltDate} {startTime}"
                enddateTime = f"{currFltDate} {endtime}"
                updateDate = currFltDate + timedelta(days=int(DaySpan))
                if currFltDate > updateDate:
                    startDateTime = f"{updateDate} {startTime}"
                    enddateTime = f"{currFltDate} {endtime}"
                elif currFltDate < updateDate:
                    startDateTime = f"{currFltDate} {startTime}"
                    enddateTime = f"{updateDate} {endtime}"

            # Get carrier exclusions based on channel
            if channel == 'B2B':
                carr_exclusion = currParams.carr_exclusion_b2b
                flightExclusion = currParams.flightExclusionB2B
            else:
                carr_exclusion = currParams.carr_exclusion_b2c
                flightExclusion = currParams.flightExclusionB2C

            carr_exclusion_list = carr_exclusion.split('/')

            origin = currParams.origin
            destination = currParams.destin
            
            # Handle routed sector
            routedSector = self.getRoutedDestination(origin, destination)
            if len(routedSector) > 0:
                origin = routedSector['routedSector'][0][:3]
                destination = routedSector['routedSector'][0][3:]

            # Query both tables with retry logic
            self.logger.info(f"Querying market fares for {origin}-{destination}, channel: {channel}")
            
            ag_data = self._queryMarketFareTable(
                self.market_fares_ag, 
                origin, destination, channel, currFltDate, updateDate,
                startTime, endtime, Day3Flag, startDateTime, enddateTime,
                carr_exclusion_list, flightExclusion, hrs, 'AG'
            )
            
            rg_data = self._queryMarketFareTable(
                self.market_fares_rg,
                origin, destination, channel, currFltDate, updateDate,
                startTime, endtime, Day3Flag, startDateTime, enddateTime,
                carr_exclusion_list, flightExclusion, hrs, 'RG'
            )

            # Combine and process data
            combined_data = ag_data + rg_data
            self.logger.info(f"Combined data count: {len(combined_data)}")
            
            # Remove duplicates by keeping latest created_at for each unique flight
            latest_records = self._getLatestRecords(combined_data)
            self.logger.info(f"Latest records count after deduplication: {len(latest_records)}")
            
            # Convert to DataFrame and sort by fare
            df_data = []
            for record in latest_records:
                try:
                    df_data.append({
                        'gross_fare': float(record['gross_fare']),
                        'outbound_airline_code': record['outbound_airline_code'],
                        'outbound_flight': record['outbound_flight'],
                        'mktFile': record['mktFile']
                    })
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Skipping record due to data issue: {e}")
                    continue
            
            mktFareRange = pd.DataFrame(df_data)
            if not mktFareRange.empty:
                mktFareRange = mktFareRange.sort_values('gross_fare')
                self.logger.info(f"Returning {len(mktFareRange)} fare records")
            else:
                self.logger.info("No fare records found")
            
            return mktFareRange

        except Exception as e:
            self.logger.error(f"Error in getMarketFareRange: {e}")
            return pd.DataFrame()

    def getMarketFareRangeConnections(self, channel, currParams, currFltDateStr, outBoundStop, outboundDuration):
        """
        DynamoDB version of getMarketFareRangeConnections method with optimized querying
        """
        try:
            currFltDate = currFltDateStr
            origin = currParams.sector1[:3]
            destination = currParams.sector2[3:]
            
            if channel == 'B2B':
                carr_exclusion = currParams.carrierExclusionB2B
            else:
                carr_exclusion = currParams.carrierExclusionB2C
        
            carr_exclusion_list = carr_exclusion.split('/')

            self.logger.info(f"Querying connection fares for {origin}-{destination}, channel: {channel}")

            # Query both tables for connections with retry logic
            ag_data = self._queryMarketFareConnectionsTable(
                self.market_fares_ag,
                origin, destination, channel, currFltDate,
                carr_exclusion_list, outBoundStop, outboundDuration, 'AG'
            )
            
            rg_data = self._queryMarketFareConnectionsTable(
                self.market_fares_rg,
                origin, destination, channel, currFltDate,
                carr_exclusion_list, outBoundStop, outboundDuration, 'RG'
            )

            # Combine and process data
            combined_data = ag_data + rg_data
            self.logger.info(f"Combined connection data count: {len(combined_data)}")
            
            # Remove duplicates by keeping latest created_at for each unique flight
            latest_records = self._getLatestRecords(combined_data)
            self.logger.info(f"Latest connection records count after deduplication: {len(latest_records)}")
            
            # Convert to DataFrame and sort by fare
            df_data = []
            for record in latest_records:
                try:
                    df_data.append({
                        'gross_fare': float(record['gross_fare']),
                        'outbound_airline_code': record['outbound_airline_code'],
                        'outbound_flight': record['outbound_flight'],
                        'currency': record['currency'],
                        'mktFile': record['mktFile']
                    })
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Skipping connection record due to data issue: {e}")
                    continue
            
            mktFareRange = pd.DataFrame(df_data)
            if not mktFareRange.empty:
                mktFareRange = mktFareRange.sort_values('gross_fare')
                self.logger.info(f"Returning {len(mktFareRange)} connection fare records")
            else:
                self.logger.info("No connection fare records found")
            
            return mktFareRange

        except Exception as e:
            self.logger.error(f"Error in getMarketFareRangeConnections: {e}")
            return pd.DataFrame()

    def _queryMarketFareTable(self, table, origin, destination, channel, currFltDate, updateDate,
                             startTime, endtime, Day3Flag, startDateTime, enddateTime,
                             carr_exclusion_list, flightExclusion, hrs, source_suffix):
        """
        Optimized helper method to query market fare table with pagination and retry logic
        """
        try:
            # Build filter expression
            filter_expr = (
                Attr('source_airport').eq(origin) &
                Attr('destination_airport').eq(destination) &
                Attr('channel').eq(channel) &
                Attr('outbound_stop').eq('0') &
                ~Attr('outbound_airline_code').is_in(carr_exclusion_list) &
                Attr('outbound_airline_code').ne('QP') &
                Attr('outbound_airline_code').ne('G8') &
                Attr('outbound_flight').ne(flightExclusion)
            )

            # Add time-based filters
            if Day3Flag:
                filter_expr = filter_expr & (
                    Attr('departure_date').gte(currFltDate.isoformat()) &
                    Attr('departure_date').lte(updateDate.isoformat()) &
                    Attr('outbound_departuretime').gte(startTime) &
                    Attr('outbound_departuretime').lte(endtime)
                )
            else:
                filter_expr = filter_expr & (
                    Attr('departure_date').gte(currFltDate.isoformat()) &
                    Attr('departure_date').lte(updateDate.isoformat())
                )

            # Add time interval filter for non-test data
            if hasattr(self, 'inputSource') and self.inputSource.input_source.get('CONST_MARKET_FARES') != 'TEST_DATA':
                cutoff_time = datetime.now() - timedelta(hours=hrs)
                filter_expr = filter_expr & Attr('created_at').gt(cutoff_time.isoformat())

            all_items = []
            last_evaluated_key = None
            page_count = 0
            max_pages = 100  # Safety limit to prevent infinite loops
            
            self.logger.info(f"Starting scan of {source_suffix} table for {origin}-{destination}")

            while page_count < max_pages:
                def scan_page():
                    scan_kwargs = {
                        'FilterExpression': filter_expr,
                        'ProjectionExpression': 'gross_fare, outbound_airline_code, outbound_flight, departure_date, destination_airport, source_airport, channel, outbound_departuretime, created_at_iso, scrape_file_name',
                        'Limit': 50  # Smaller page size to reduce throttling risk
                    }
                    
                    if last_evaluated_key:
                        scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
                        
                    return table.scan(**scan_kwargs)

                try:
                    # Use retry logic for each page
                    response = self.retry_with_backoff(scan_page)
                    
                    items = response['Items']
                    all_items.extend(items)
                    page_count += 1
                    
                    # Log progress periodically
                    if page_count % 10 == 0:
                        self.logger.info(f"Scanned {page_count} pages from {source_suffix}, items so far: {len(all_items)}")
                    
                    if 'LastEvaluatedKey' not in response:
                        break
                        
                    last_evaluated_key = response['LastEvaluatedKey']
                    
                    # Small delay between pages to be respectful to DynamoDB
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"Error scanning page {page_count} from {source_suffix}: {e}")
                    break

            # Add source information and clean up data
            for item in all_items:
                item['mktFile'] = f"{item.get('scrape_file_name', 'unknown')}_{source_suffix}"
                item['source_table'] = source_suffix
                if 'created_at_iso' in item:
                    item['created_at'] = item.pop('created_at_iso')
                elif 'created_at' not in item:
                    item['created_at'] = datetime.now().isoformat()

            self.logger.info(f"Retrieved {len(all_items)} items from {source_suffix} table in {page_count} pages")
            return all_items

        except Exception as e:
            self.logger.error(f"Error querying {source_suffix} table: {e}")
            return []

    def _queryMarketFareConnectionsTable(self, table, origin, destination, channel, currFltDate,
                                       carr_exclusion_list, outBoundStop, outboundDuration, source_suffix):
        """
        Optimized helper method to query market fare connections table with pagination and retry logic
        """
        try:
            # Build filter expression
            filter_expr = (
                Attr('source_airport').eq(origin) &
                Attr('destination_airport').eq(destination) &
                Attr('channel').eq(channel) &
                Attr('outbound_stop').lte(int(outBoundStop)) &
                Attr('outbound_duration').lte(int(outboundDuration)) &
                ~Attr('outbound_airline_code').is_in(carr_exclusion_list) &
                Attr('outbound_airline_code').ne('QP') &
                Attr('outbound_airline_code').ne('G8') &
                Attr('departure_date').eq(currFltDate)
            )

            # Add time interval filter for non-test data
            if hasattr(self, 'inputSource') and self.inputSource.input_source.get('CONST_MARKET_FARES') != 'TEST_DATA':
                cutoff_time = datetime.now() - timedelta(days=2)
                filter_expr = filter_expr & Attr('created_at').gt(cutoff_time.isoformat())

            all_items = []
            last_evaluated_key = None
            page_count = 0
            max_pages = 100  # Safety limit
            
            self.logger.info(f"Starting connections scan of {source_suffix} table for {origin}-{destination}")

            while page_count < max_pages:
                def scan_page():
                    scan_kwargs = {
                        'FilterExpression': filter_expr,
                        'ProjectionExpression': 'gross_fare, outbound_airline_code, outbound_flight, destination_airport, source_airport, outbound_departuretime, channel, currency, departure_date, created_at, scrape_file_name',
                        'Limit': 50  # Smaller page size
                    }
                    
                    if last_evaluated_key:
                        scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
                        
                    return table.scan(**scan_kwargs)

                try:
                    # Use retry logic for each page
                    response = self.retry_with_backoff(scan_page)
                    
                    items = response['Items']
                    all_items.extend(items)
                    page_count += 1
                    
                    # Log progress periodically
                    if page_count % 10 == 0:
                        self.logger.info(f"Scanned {page_count} connection pages from {source_suffix}, items so far: {len(all_items)}")
                    
                    if 'LastEvaluatedKey' not in response:
                        break
                        
                    last_evaluated_key = response['LastEvaluatedKey']
                    
                    # Small delay between pages
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"Error scanning connections page {page_count} from {source_suffix}: {e}")
                    break

            # Add source information and clean up data
            for item in all_items:
                item['mktFile'] = f"{item.get('scrape_file_name', 'unknown')}_{source_suffix}"
                item['source_table'] = source_suffix
                if 'created_at' not in item:
                    item['created_at'] = datetime.now().isoformat()

            self.logger.info(f"Retrieved {len(all_items)} connection items from {source_suffix} table in {page_count} pages")
            return all_items

        except Exception as e:
            self.logger.error(f"Error querying {source_suffix} connections table: {e}")
            return []

    def _getLatestRecords(self, combined_data):
        """
        Helper method to get latest records for duplicate flights
        Equivalent to the ROW_NUMBER() OVER() logic in SQL
        """
        if not combined_data:
            return []
            
        # Group by unique flight identifier
        flight_groups = {}
        
        for record in combined_data:
            try:
                # Create unique key for grouping
                key = (
                    record.get('channel', ''),
                    record.get('departure_date', ''),
                    record.get('destination_airport', ''),
                    record.get('source_airport', ''),
                    record.get('outbound_flight', ''),
                    record.get('outbound_departuretime', '')
                )
                
                if key not in flight_groups:
                    flight_groups[key] = []
                flight_groups[key].append(record)
            except Exception as e:
                self.logger.warning(f"Skipping record in deduplication due to missing fields: {e}")
                continue
        
        # Keep only the latest record for each group
        latest_records = []
        for group in flight_groups.values():
            try:
                # Sort by created_at descending and take the first (latest)
                latest_record = max(group, key=lambda x: x.get('created_at', ''))
                latest_records.append(latest_record)
            except Exception as e:
                self.logger.warning(f"Error finding latest record in group: {e}")
                # If there's an issue with created_at comparison, just take the first record
                if group:
                    latest_records.append(group[0])
        
        self.logger.info(f"Deduplicated {len(combined_data)} records to {len(latest_records)} latest records")
        return latest_records

    def getRoutedDestination(self, origin, destination):
        """
        Get routed destination from configuration
        """
        query = f"SELECT routedSector FROM config_marketFares_rerouting WHERE sector = CONCAT('{origin}','{destination}')"
        try:
            routedData = pd.read_sql(query, self.conn)
            return routedData
        except Exception as e:
            self.logger.error(f"Error getting routed destination: {e}")
            return pd.DataFrame()

    def _queryMarketFareTable_with_gsi(self, table, origin, destination, channel, currFltDate, updateDate,
                                      startTime, endtime, Day3Flag, startDateTime, enddateTime,
                                      carr_exclusion_list, flightExclusion, hrs, source_suffix, gsi_name='RouteDate-index'):
        """
        Query approach using GSI instead of scan (requires GSI to be created first)
        This method should be used when GSI is available for better performance
        """
        try:
            all_items = []
            current_date = currFltDate
            
            self.logger.info(f"Querying {source_suffix} table using GSI: {gsi_name}")
            
            while current_date <= updateDate:
                # Build key condition for GSI query
                key_condition = Key('source_airport').eq(origin) & Key('destination_airport').eq(destination)
                
                # Build filter expression for additional conditions
                filter_expr = (
                    Attr('channel').eq(channel) &
                    Attr('outbound_stop').eq('0') &
                    ~Attr('outbound_airline_code').is_in(carr_exclusion_list) &
                    Attr('outbound_airline_code').ne('QP') &
                    Attr('outbound_airline_code').ne('G8') &
                    Attr('outbound_flight').ne(flightExclusion) &
                    Attr('departure_date').eq(current_date.isoformat())
                )

                # Add time-based filters
                if Day3Flag:
                    filter_expr = filter_expr & (
                        Attr('outbound_departuretime').gte(startTime) &
                        Attr('outbound_departuretime').lte(endtime)
                    )

                # Add time interval filter for non-test data
                if hasattr(self, 'inputSource') and self.inputSource.input_source.get('CONST_MARKET_FARES') != 'TEST_DATA':
                    cutoff_time = datetime.now() - timedelta(hours=hrs)
                    filter_expr = filter_expr & Attr('created_at').gt(cutoff_time.isoformat())

                def query_date():
                    return table.query(
                        IndexName=gsi_name,
                        KeyConditionExpression=key_condition,
                        FilterExpression=filter_expr,
                        ProjectionExpression='gross_fare, outbound_airline_code, outbound_flight, departure_date, destination_airport, source_airport, channel, outbound_departuretime, created_at_iso, scrape_file_name'
                    )

                try:
                    # Use retry logic for query
                    response = self.retry_with_backoff(query_date)
                    all_items.extend(response['Items'])
                    
                    # Handle pagination for this date
                    while 'LastEvaluatedKey' in response:
                        last_key = response['LastEvaluatedKey']
                        
                        def query_next_page():
                            return table.query(
                                IndexName=gsi_name,
                                KeyConditionExpression=key_condition,
                                FilterExpression=filter_expr,
                                ProjectionExpression='gross_fare, outbound_airline_code, outbound_flight, departure_date, destination_airport, source_airport, channel, outbound_departuretime, created_at_iso, scrape_file_name',
                                ExclusiveStartKey=last_key
                            )
                        
                        response = self.retry_with_backoff(query_next_page)
                        all_items.extend(response['Items'])
                        
                except Exception as e:
                    self.logger.error(f"Error querying {source_suffix} with GSI for date {current_date}: {e}")
                    # Continue with next date instead of failing completely
                
                # Move to next date
                current_date += timedelta(days=1)
                
                # Small delay between date queries
                time.sleep(0.05)

            # Add source information
            for item in all_items:
                item['mktFile'] = f"{item.get('scrape_file_name', 'unknown')}_{source_suffix}"
                item['source_table'] = source_suffix
                if 'created_at_iso' in item:
                    item['created_at'] = item.pop('created_at_iso')

            self.logger.info(f"Retrieved {len(all_items)} items from {source_suffix} via GSI")
            return all_items

        except Exception as e:
            self.logger.error(f"Error querying {source_suffix} table with GSI: {e}")
            # Fallback to scan if GSI query fails
            self.logger.info(f"Falling back to scan method for {source_suffix}")
            return self._queryMarketFareTable(table, origin, destination, channel, currFltDate, updateDate,
                                            startTime, endtime, Day3Flag, startDateTime, enddateTime,
                                            carr_exclusion_list, flightExclusion, hrs, source_suffix)