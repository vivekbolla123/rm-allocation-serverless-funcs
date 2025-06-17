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
        # NEW: Define the GSI name as a constant for easy maintenance
        self.gsi_name = 'ChannelDepartureDateGSI'
        
    def getMarketFareRange(self, channel, currParams, currFltDateStr, DaySpan, startTime, endtime, Day3Flag, hrs):
        """
        DynamoDB version of getMarketFareRange method.
        REFACTORED to use a GSI and Query for optimal performance.
        """
        try:
            currFltDate = datetime.strptime(currFltDateStr, '%m/%d/%Y').date()
            updateDate = currFltDate + timedelta(days=int(DaySpan))

            if not Day3Flag:
                # Note: This logic seems complex and might be simplified.
                # The query below uses a simple date range from currFltDate to updateDate.
                startDateTime = f"{currFltDate} {startTime}"
                enddateTime = f"{currFltDate} {endtime}"
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
            
            routedSector = self.getRoutedDestination(origin, destination)
            if len(routedSector) > 0:
                origin = routedSector['routedSector'][0][:3]
                destination = routedSector['routedSector'][0][3:]

            # Query both tables using the new, efficient method
            ag_data = self._queryMarketFareTable(
                self.market_fares_ag, 
                origin, destination, channel, currFltDate, updateDate,
                startTime, endtime, Day3Flag,
                carr_exclusion_list, flightExclusion, hrs, 'AG'
            )
            
            rg_data = self._queryMarketFareTable(
                self.market_fares_rg,
                origin, destination, channel, currFltDate, updateDate,
                startTime, endtime, Day3Flag,
                carr_exclusion_list, flightExclusion, hrs, 'RG'
            )

            # Combine and process data
            combined_data = ag_data + rg_data
            if not combined_data:
                return pd.DataFrame()
            
            latest_records = self._getLatestRecords(combined_data)
            
            # Convert to DataFrame and sort by fare
            df_data = [{
                'gross_fare': float(record['gross_fare']),
                'outbound_airline_code': record['outbound_airline_code'],
                'outbound_flight': record['outbound_flight'],
                'mktFile': record['mktFile']
            } for record in latest_records]
            
            mktFareRange = pd.DataFrame(df_data)
            if not mktFareRange.empty:
                mktFareRange = mktFareRange.sort_values('gross_fare')
            
            return mktFareRange

        except Exception as e:
            self.logger.error(f"Error in getMarketFareRange: {e}")
            return pd.DataFrame()

    def getMarketFareRangeConnections(self, channel, currParams, currFltDateStr, outBoundStop, outboundDuration):
        """
        DynamoDB version of getMarketFareRangeConnections method.
        REFACTORED to use a GSI and Query for optimal performance.
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

            # Query both tables for connections using the new, efficient method
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
            if not combined_data:
                return pd.DataFrame()

            latest_records = self._getLatestRecords(combined_data)
            
            # Convert to DataFrame and sort by fare
            df_data = [{
                'gross_fare': float(record['gross_fare']),
                'outbound_airline_code': record['outbound_airline_code'],
                'outbound_flight': record['outbound_flight'],
                'currency': record.get('currency', 'N/A'), # Use .get for safety
                'mktFile': record['mktFile']
            } for record in latest_records]
            
            mktFareRange = pd.DataFrame(df_data)
            if not mktFareRange.empty:
                mktFareRange = mktFareRange.sort_values('gross_fare')
            
            return mktFareRange

        except Exception as e:
            self.logger.error(f"Error in getMarketFareRangeConnections: {e}")
            return pd.DataFrame()

    def _queryMarketFareTable(self, table, origin, destination, channel, currFltDate, updateDate,
                             startTime, endtime, Day3Flag, carr_exclusion_list, flightExclusion, hrs, source_suffix):
        """
        REFACTORED Helper method to use Query on a GSI instead of Scan.
        """
        items = []
        try:
            # 1. Define the GSI Key Condition for an efficient lookup
            route_channel_pk = f"{origin}-{destination}-{channel}"
            key_expr = (
                Key('route_channel').eq(route_channel_pk) & 
                Key('departure_date').between(currFltDate.isoformat(), updateDate.isoformat())
            )

            # 2. Build the Filter Expression for attributes NOT in the GSI key
            filter_expr = (
                Attr('outbound_stop').eq('0') &
                ~Attr('outbound_airline_code').is_in(carr_exclusion_list) &
                Attr('outbound_airline_code').ne('QP') &
                Attr('outbound_airline_code').ne('G8') &
                Attr('outbound_flight').ne(flightExclusion)
            )

            if Day3Flag:
                filter_expr = filter_expr & Attr('outbound_departuretime').between(startTime, endtime)
            
            if hasattr(self, 'inputSource') and self.inputSource.input_source.get('CONST_MARKET_FARES') != 'TEST_DATA':
                cutoff_time = (datetime.now() - timedelta(hours=hrs)).isoformat()
                filter_expr = filter_expr & Attr('created_at').gt(cutoff_time)

            # 3. Perform the efficient QUERY operation
            response = table.query(
                IndexName=self.gsi_name,
                KeyConditionExpression=key_expr,
                FilterExpression=filter_expr
            )
            items.extend(response['Items'])
            
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    IndexName=self.gsi_name,
                    KeyConditionExpression=key_expr,
                    FilterExpression=filter_expr,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])

            for item in items:
                item['mktFile'] = f"{item.get('scrape_file_name', 'unknown')}_{source_suffix}"
                item['source_table'] = source_suffix
                # Ensure created_at exists for sorting
                if 'created_at_iso' in item:
                    item['created_at'] = item.pop('created_at_iso')

            return items

        except Exception as e:
            # Catching specific ThrottlingException can be useful
            if e.response['Error']['Code'] == 'ThrottlingException':
                 self.logger.error(f"ThrottlingException while querying {source_suffix} table. Check GSI capacity.")
            self.logger.error(f"Error querying {source_suffix} table: {e}")
            return items

    def _queryMarketFareConnectionsTable(self, table, origin, destination, channel, currFltDate,
                                       carr_exclusion_list, outBoundStop, outboundDuration, source_suffix):
        """
        REFACTORED Helper method to use Query on a GSI instead of Scan.
        """
        items = []
        try:
            # 1. Define the GSI Key Condition
            route_channel_pk = f"{origin}-{destination}-{channel}"
            key_expr = (
                Key('route_channel').eq(route_channel_pk) & 
                Key('departure_date').eq(currFltDate)
            )
            
            # 2. Build the Filter Expression
            filter_expr = (
                Attr('outbound_stop').lte(int(outBoundStop)) &
                Attr('outbound_duration').lte(int(outboundDuration)) &
                ~Attr('outbound_airline_code').is_in(carr_exclusion_list) &
                Attr('outbound_airline_code').ne('QP') &
                Attr('outbound_airline_code').ne('G8')
            )

            if hasattr(self, 'inputSource') and self.inputSource.input_source.get('CONST_MARKET_FARES') != 'TEST_DATA':
                cutoff_time = (datetime.now() - timedelta(days=2)).isoformat()
                filter_expr = filter_expr & Attr('created_at').gt(cutoff_time)

            # 3. Perform the efficient QUERY operation
            response = table.query(
                IndexName=self.gsi_name,
                KeyConditionExpression=key_expr,
                FilterExpression=filter_expr
            )
            items.extend(response['Items'])
            
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    IndexName=self.gsi_name,
                    KeyConditionExpression=key_expr,
                    FilterExpression=filter_expr,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])

            for item in items:
                item['mktFile'] = f"{item.get('scrape_file_name', 'unknown')}_{source_suffix}"
                item['source_table'] = source_suffix

            return items

        except Exception as e:
            self.logger.error(f"Error querying {source_suffix} connections table: {e}")
            return items

    def _getLatestRecords(self, combined_data):
        flight_groups = {}
        for record in combined_data:
            key = (
                record.get('channel'),
                record.get('departure_date'),
                record.get('destination_airport'),
                record.get('source_airport'),
                record.get('outbound_flight'),
                record.get('outbound_departuretime')
            )
            
            # Use record's created_at for comparison
            created_at = record.get('created_at')
            if not created_at:
                continue # Skip records without a timestamp

            if key not in flight_groups or created_at > flight_groups[key].get('created_at'):
                flight_groups[key] = record
        
        return list(flight_groups.values())

    def getRoutedDestination(self, origin, destination):
        query = f''' SELECT routedSector FROM config_marketFares_rerouting where sector = concat('{origin}','{destination}') '''
        try:
            routedData = pd.read_sql(query, self.conn)
            return routedData
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()