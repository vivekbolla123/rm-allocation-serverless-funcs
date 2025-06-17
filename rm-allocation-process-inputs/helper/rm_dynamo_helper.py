import pandas as pd
from datetime import datetime, timedelta
from amazondax import AmazonDaxClient
from boto3.dynamodb.conditions import Key, Attr
import logging

class MarketFareDynamoDB:
    def __init__(self, conn, region_name, dax_endpoints):
        self.conn = conn
        self.logger = logging.getLogger(__name__)
        self.gsi_name = 'ChannelDepartureDateGSI'

        # Initialize DAX client
        dax = dax_endpoints

        # Access tables via DAX
        self.market_fares_ag = dax.Table('MarketFaresRG')
        self.market_fares_rg = dax.Table('MarketFaresAGInt')

    def getMarketFareRange(self, channel, currParams, currFltDateStr, DaySpan, startTime, endtime, Day3Flag, hrs):
        try:
            currFltDate = datetime.strptime(currFltDateStr, '%m/%d/%Y').date()
            updateDate = currFltDate + timedelta(days=int(DaySpan))

            if not Day3Flag:
                startDateTime = f"{currFltDate} {startTime}"
                enddateTime = f"{updateDate} {endtime}"

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

            ag_data = self._queryMarketFareTable(
                self.market_fares_ag, origin, destination, channel,
                currFltDate, updateDate, startTime, endtime, Day3Flag,
                carr_exclusion_list, flightExclusion, hrs, 'AG'
            )

            rg_data = self._queryMarketFareTable(
                self.market_fares_rg, origin, destination, channel,
                currFltDate, updateDate, startTime, endtime, Day3Flag,
                carr_exclusion_list, flightExclusion, hrs, 'RG'
            )

            combined_data = ag_data + rg_data
            if not combined_data:
                return pd.DataFrame()

            latest_records = self._getLatestRecords(combined_data)
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
        try:
            currFltDate = currFltDateStr
            origin = currParams.sector1[:3]
            destination = currParams.sector2[3:]

            if channel == 'B2B':
                carr_exclusion = currParams.carrierExclusionB2B
            else:
                carr_exclusion = currParams.carrierExclusionB2C

            carr_exclusion_list = carr_exclusion.split('/')

            ag_data = self._queryMarketFareConnectionsTable(
                self.market_fares_ag, origin, destination, channel,
                currFltDate, carr_exclusion_list, outBoundStop, outboundDuration, 'AG'
            )

            rg_data = self._queryMarketFareConnectionsTable(
                self.market_fares_rg, origin, destination, channel,
                currFltDate, carr_exclusion_list, outBoundStop, outboundDuration, 'RG'
            )

            combined_data = ag_data + rg_data
            if not combined_data:
                return pd.DataFrame()

            latest_records = self._getLatestRecords(combined_data)
            df_data = [{
                'gross_fare': float(record['gross_fare']),
                'outbound_airline_code': record['outbound_airline_code'],
                'outbound_flight': record['outbound_flight'],
                'currency': record.get('currency', 'N/A'),
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
        items = []
        try:
            route_channel_pk = f"{origin}-{destination}-{channel}"
            key_expr = (
                Key('route_channel').eq(route_channel_pk) &
                Key('departure_date').between(currFltDate.isoformat(), updateDate.isoformat())
            )

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
                if 'created_at_iso' in item:
                    item['created_at'] = item.pop('created_at_iso')

            return items

        except Exception as e:
            self.logger.error(f"Error querying {source_suffix} table: {e}")
            return items

    def _queryMarketFareConnectionsTable(self, table, origin, destination, channel, currFltDate,
                                       carr_exclusion_list, outBoundStop, outboundDuration, source_suffix):
        items = []
        try:
            route_channel_pk = f"{origin}-{destination}-{channel}"
            key_expr = (
                Key('route_channel').eq(route_channel_pk) &
                Key('departure_date').eq(currFltDate)
            )

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

            created_at = record.get('created_at')
            if not created_at:
                continue

            if key not in flight_groups or created_at > flight_groups[key].get('created_at'):
                flight_groups[key] = record

        return list(flight_groups.values())

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
