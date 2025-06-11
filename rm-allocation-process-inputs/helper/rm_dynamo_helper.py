import boto3
import pandas as pd
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import logging

class MarketFareDynamoDB:
    def __init__(self,conn,dynamodb):
        self.dynamodb = dynamodb
        self.market_fares_ag = self.dynamodb.Table('MarketFaresRG')
        self.market_fares_rg = self.dynamodb.Table('MarketFaresAGInt')
        self.logger = logging.getLogger(__name__)
        self.conn=conn
        
    def getMarketFareRange(self, channel, currParams, currFltDateStr, DaySpan, startTime, endtime, Day3Flag, hrs):
        """
        DynamoDB version of getMarketFareRange method
        Note: For optimal performance, ensure your DynamoDB tables have appropriate GSIs for:
        - source_airport-destination_airport-departure_date
        - channel-departure_date
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
            if channel == 'B2B':  # Assuming B2B_CHANNEL constant
                carr_exclusion = currParams.carr_exclusion_b2b
                flightExclusion = currParams.flightExclusionB2B
            else:
                carr_exclusion = currParams.carr_exclusion_b2c
                flightExclusion = currParams.flightExclusionB2C

            carr_exclusion_list = carr_exclusion.split('/')

            origin = currParams.origin
            destination = currParams.destin
            
            # Handle routed sector (assuming you have this method implemented)
            routedSector = self.getRoutedDestination(origin, destination)
            if len(routedSector) > 0:
                origin = routedSector['routedSector'][0][:3]
                destination = routedSector['routedSector'][0][3:]

            # Query both tables
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
            
            # Remove duplicates by keeping latest created_at for each unique flight
            latest_records = self._getLatestRecords(combined_data)
            
            # Convert to DataFrame and sort by fare
            df_data = []
            for record in latest_records:
                df_data.append({
                    'gross_fare': float(record['gross_fare']),
                    'outbound_airline_code': record['outbound_airline_code'],
                    'outbound_flight': record['outbound_flight'],
                    'mktFile': record['mktFile']
                })
            
            mktFareRange = pd.DataFrame(df_data)
            if not mktFareRange.empty:
                mktFareRange = mktFareRange.sort_values('gross_fare')
            
            return mktFareRange

        except Exception as e:
            self.logger.error(f"Error in getMarketFareRange: {e}")
            return pd.DataFrame()

    def getMarketFareRangeConnections(self, channel, currParams, currFltDateStr, outBoundStop, outboundDuration):
        """
        DynamoDB version of getMarketFareRangeConnections method
        """
        try:
            currFltDate = currFltDateStr
            origin = currParams.sector1[:3]
            destination = currParams.sector2[3:]
            
            if channel == 'B2B':  # Assuming B2B_CHANNEL constant
                carr_exclusion = currParams.carrierExclusionB2B
            else:
                carr_exclusion = currParams.carrierExclusionB2C
        
            carr_exclusion_list = carr_exclusion.split('/')

            # Query both tables for connections
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
            
            # Remove duplicates by keeping latest created_at for each unique flight
            latest_records = self._getLatestRecords(combined_data)
            
            # Convert to DataFrame and sort by fare
            df_data = []
            for record in latest_records:
                df_data.append({
                    'gross_fare': float(record['gross_fare']),
                    'outbound_airline_code': record['outbound_airline_code'],
                    'outbound_flight': record['outbound_flight'],
                    'currency': record['currency'],
                    'mktFile': record['mktFile']
                })
            
            mktFareRange = pd.DataFrame(df_data)
            if not mktFareRange.empty:
                mktFareRange = mktFareRange.sort_values('gross_fare')
            
            return mktFareRange

        except Exception as e:
            self.logger.error(f"Error in getMarketFareRangeConnections: {e}")
            return pd.DataFrame()

    def _queryMarketFareTable(self, table, origin, destination, channel, currFltDate, updateDate,
                             startTime, endtime, Day3Flag, startDateTime, enddateTime,
                             carr_exclusion_list, flightExclusion, hrs, source_suffix):
        """
        Helper method to query market fare table with filters
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
                # For datetime comparison in DynamoDB, we need to handle this differently
                # This is a simplified approach - you might need to adjust based on your data format
                filter_expr = filter_expr & (
                    Attr('departure_date').gte(currFltDate.isoformat()) &
                    Attr('departure_date').lte(updateDate.isoformat())
                )

            # Add time interval filter for non-test data
            if hasattr(self, 'inputSource') and self.inputSource.input_source.get('CONST_MARKET_FARES') != 'TEST_DATA':
                cutoff_time = datetime.now() - timedelta(hours=hrs)
                filter_expr = filter_expr & Attr('created_at').gt(cutoff_time.isoformat())

            # Perform scan with filter (Note: for better performance, use Query with GSI if available)
            response = table.scan(
                FilterExpression=filter_expr,
                ProjectionExpression='gross_fare, outbound_airline_code, outbound_flight, departure_date, destination_airport, source_airport, channel, outbound_departuretime, created_at_iso, scrape_file_name'
            )

            items = response['Items']
            
            # Handle pagination if needed
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    FilterExpression=filter_expr,
                    ProjectionExpression='gross_fare, outbound_airline_code, outbound_flight, departure_date, destination_airport, source_airport, channel, outbound_departuretime, created_at_iso, scrape_file_name',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])

            # Add source information
            for item in items:
                item['mktFile'] = f"{item['scrape_file_name']}_{source_suffix}"
                item['source_table'] = source_suffix
                item['created_at'] = item.pop('created_at_iso')

            return items

        except Exception as e:
            self.logger.error(f"Error querying {source_suffix} table: {e}")
            return []

    def _queryMarketFareConnectionsTable(self, table, origin, destination, channel, currFltDate,
                                       carr_exclusion_list, outBoundStop, outboundDuration, source_suffix):
        """
        Helper method to query market fare connections table with filters
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

            # Perform scan with filter
            response = table.scan(
                FilterExpression=filter_expr,
                ProjectionExpression='gross_fare, outbound_airline_code, outbound_flight, destination_airport, source_airport, outbound_departuretime, channel, currency, departure_date, created_at, scrape_file_name'
            )

            items = response['Items']
            
            # Handle pagination if needed
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    FilterExpression=filter_expr,
                    ProjectionExpression='gross_fare, outbound_airline_code, outbound_flight, destination_airport, source_airport, outbound_departuretime, channel, currency, departure_date, created_at, scrape_file_name',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])

            # Add source information
            for item in items:
                item['mktFile'] = f"{item['scrape_file_name']}_{source_suffix}"
                item['source_table'] = source_suffix

            return items

        except Exception as e:
            self.logger.error(f"Error querying {source_suffix} connections table: {e}")
            return []

    def _getLatestRecords(self, combined_data):
        """
        Helper method to get latest records for duplicate flights
        Equivalent to the ROW_NUMBER() OVER() logic in SQL
        """
        # Group by unique flight identifier
        flight_groups = {}
        
        for record in combined_data:
            # Create unique key for grouping
            key = (
                record['channel'],
                record['departure_date'],
                record['destination_airport'],
                record['source_airport'],
                record['outbound_flight'],
                record['outbound_departuretime']
            )
            
            if key not in flight_groups:
                flight_groups[key] = []
            flight_groups[key].append(record)
        
        # Keep only the latest record for each group
        latest_records = []
        for group in flight_groups.values():
            # Sort by created_at descending and take the first (latest)
            latest_record = max(group, key=lambda x: x['created_at'])
            latest_records.append(latest_record)
        
        return latest_records

    def getRoutedDestination(self, origin, destination):
        query = f''' SELECT routedSector FROM config_marketFares_rerouting where sector = concat('{origin}','{destination}') '''
        try:
            routedData = pd.read_sql(query, self.conn)
            return routedData
        except Exception as e:
            self.logger.error(e)