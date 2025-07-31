import json
import configsettings
import pandas as pd
from datetime import datetime, timezone, timedelta
from constants_utils import ConstantsUtils
import traceback
import requests
import uuid
import warnings
warnings.filterwarnings('ignore')

class defaultMarketlist:
    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.navconn = params['navconn']
        self.rm_wr_conn = params['wrconn']
        self.cache_client = params['cache_client']
        self.constants = ConstantsUtils(self.rm_wr_conn)
        self.default_list = json.loads(self.constants.DEFAULT_MARKET_LIST)
        self.max_days = self.constants.DEFAULT_MARKET_LIST_DAYS
        self.session = requests.Session()
        self.session.headers.update(
            {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
        )

    def run(self):
        try:
            self.logger.info("Starting the run method.")
            currDate = datetime.now(timezone.utc)
            endDate = currDate + timedelta(days=self.max_days)
            endDate = endDate.date()

            NavitaireQuery = f"""
                SELECT
                    IL.DepartureStation as Origin,
                    IL.ArrivalStation as Destination,
                    IL.DepartureDate,
                    TRIM(IL.FlightNumber) as FlightNumber,
                    FORMAT(IL.STD, 'HH:mm') as DepartureTime,
                    FORMAT(IL.STDUTC, 'HH:mm') as DepartureTimeUTC
                FROM
                    {configsettings.NAV_DB_NAME}[rez].InventoryLeg IL
                WHERE IL.Lid > 0
                    AND IL.Status NOT IN (2)
                    AND IL.DepartureDate >= CAST(GETDATE() AS DATE)
                    AND IL.DepartureDate <= '{endDate}'
            """
            navFlights = pd.read_sql(NavitaireQuery, self.navconn)

            if navFlights.empty or len(navFlights) == 0:
                raise ValueError("No flights found in NavODS")

            MLQuery = f"""
                SELECT Origin, Destin AS Destination,
                    STR_TO_DATE(PerStart, '%%m/%%d/%%Y') as DepartureDate,
                    TRIM(FlightNumber) as FlightNumber
                FROM QP_DW_RMALLOC.market_list
                WHERE STR_TO_DATE(PerStart, '%%m/%%d/%%Y') >= CURDATE()
                    AND STR_TO_DATE(PerStart, '%%m/%%d/%%Y') <= '{endDate}'

                UNION

                SELECT Origin, Destin AS Destination,
                    STR_TO_DATE(PerStart, '%%m/%%d/%%Y') as DepartureDate,
                    TRIM(FlightNumber) as FlightNumber
                FROM QP_DW_RMALLOC.market_list_international
                WHERE STR_TO_DATE(PerStart, '%%m/%%d/%%Y') >= CURDATE()
                    AND STR_TO_DATE(PerStart, '%%m/%%d/%%Y') <= '{endDate}';
            """
            mlFlights = pd.read_sql(MLQuery, self.rm_wr_conn)

            if mlFlights.empty or len(mlFlights) == 0:
                raise ValueError("No flights found in Marketlist")

            navFlights['DepartureDate'] = pd.to_datetime(navFlights['DepartureDate'])
            mlFlights['DepartureDate'] = pd.to_datetime(mlFlights['DepartureDate'])

            # Get unmatched records - Fix: Use indicator merge to properly identify unmatched records
            merge_indicator = navFlights.merge(
                mlFlights[['Origin', 'Destination', 'FlightNumber', 'DepartureDate']],
                on=['Origin', 'Destination', 'FlightNumber', 'DepartureDate'],
                how='left',
                indicator=True
            )
            
            # Filter for records that exist only in navFlights (left_only)
            unmatched_navFlights = merge_indicator[merge_indicator['_merge'] == 'left_only'].drop('_merge', axis=1)
            unmatched_navFlights['sector'] = unmatched_navFlights['Origin'] + unmatched_navFlights['Destination']
            
            analyst_query = "SELECT sector, user1 FROM QP_DW_RMALLOC.config_user_sector_map;"
            analyst_sector_map = pd.read_sql(analyst_query, self.rm_wr_conn)
            if analyst_sector_map.empty:
                raise ValueError("No columns found for market_list table")
            
            station_master_query = "SELECT * FROM QP_DW_RMALLOC.config_station_master;"
            station_master_map = pd.read_sql(station_master_query, self.rm_wr_conn)
            if station_master_map.empty:
                raise ValueError("No columns found for market_list table")
            
            unmatched_navFlights = unmatched_navFlights.merge(
                analyst_sector_map[['sector','user1']],
                on=['sector'],
                how='left'
            )

            # Get default values
            default_list = self.default_list
            if len(default_list)==0:
                raise ValueError("No default values found for market_list")
            column_names = list(default_list.keys())

            # Prepare data for insertion
            insert_data = []
            for _, row in unmatched_navFlights.iterrows():
                record = default_list.copy()
                record['Origin'] = row['Origin']
                record['Destin'] = row['Destination']
                record['PerStart'] = row['DepartureDate'].strftime('%m/%d/%Y')
                record['PerEnd'] = row['DepartureDate'].strftime('%m/%d/%Y')
                record['FlightNumber'] = row['FlightNumber']
                record['analystName'] = row['user1']
                record['UUID'] = uuid.uuid4()
                record['defaultMlFlag'] = "1"
                weekday_num = row['DepartureDate'].weekday()

                sunday_based_day = (weekday_num + 1) % 7
                dow_base = list('9999999')
                dow_base[sunday_based_day] = '1'
                record['DOW'] = ''.join(dow_base)
                insert_data.append([record.get(col, '') for col in column_names])

            # Create DataFrame for insertion
            insert_df = pd.DataFrame(insert_data, columns=column_names)

            origin_routes = insert_df.merge(
                station_master_map, 
                left_on='Origin', 
                right_on='station', 
                how='left'
            ).rename(columns={'route': 'origin_route'})

            final_df = origin_routes.merge(
                station_master_map, 
                left_on='Destin', 
                right_on='station', 
                how='left'
            ).rename(columns={'route': 'destin_route'})

            international_df = final_df[
                (final_df['origin_route'] == 'I') | (final_df['destin_route'] == 'I')
            ]

            # Domestic: rest of them (everything that's not international)
            domestic_df = final_df[
                ~((final_df['origin_route'] == 'I') | (final_df['destin_route'] == 'I'))
            ]

            international_df = international_df.drop(columns=['origin_route', 'destin_route', 'station_x', 'station_y'], errors='ignore')
            domestic_df = domestic_df.drop(columns=['origin_route', 'destin_route', 'station_x', 'station_y'], errors='ignore')


            # Insert unmatched records into market_list
            domestic_df.to_sql(
                name='market_list',
                con=self.rm_wr_conn,
                schema='QP_DW_RMALLOC',
                if_exists='append',
                index=False
            )
            international_df.to_sql(
                name='market_list_international',
                con=self.rm_wr_conn,
                schema='QP_DW_RMALLOC',
                if_exists='append',
                index=False
            )

            self.logger.info(f"Inserted {len(domestic_df)} unmatched records into market_list.")
            self.logger.info(f"Inserted {len(international_df)} unmatched records into market_list_international.")
            self.logger.info("Run method completed.")
        except Exception as e:
            self.logger.error(f"An error occurred in the run method: {traceback.format_exc()}")