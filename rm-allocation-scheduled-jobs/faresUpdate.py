import json
import configsettings
import pandas as pd


class Fares:

    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']
        self.rm_rd_esb_conn = params['rdEsbConn']
        self.sqs = params['sqs']
        self.s3 = params['s3']
        self.S3_RM_BUCKET_NAME = configsettings.S3_RM_BUCKET_NAME
        self.S3_FILE_PATH = configsettings.S3_PARQUET_FILE_PATH
        self.queue_name = configsettings.CALCULATE_COMPONENTS_QUEUE_URL

    def run(self):
        """Helper values to be generated"""

        # Initiliaze other variables
        previous_month = (pd.Timestamp("today") - pd.DateOffset(months=1)).replace(day=1).strftime("%b%y")
        current_month = pd.Timestamp("today").strftime("%b%y")
        self.monthList = [previous_month, current_month]

        sectors = self.load_sectors()
        print('Loaded all Sectors from Database, shape:', sectors.shape)

        ndo_bands = self.load_ndobands()
        print('Loaded NDO Bands from Database, shape:', ndo_bands.shape)

        rg_paths = self.load_rg_paths()
        print('Loaded RateGain Paths.....')

        cluster = self.load_timebands()
        print('Loaded Datetime clusters from Database, shape:', cluster.shape)

        ##############################################

        for i in range(sectors.shape[0]):
            data = {
                "overrides": {
                    "ndo_bands": ndo_bands.tolist(),
                    "cluster": cluster.to_json(),
                    "sector": sectors["Sector"][i],
                }
            }
            for path in rg_paths:
                data['overrides']['rg_paths'] = path

                # Call Historic Fares
                data['modules'] = {'historicFares' : 1}
                self.sqs.send_message(
                    QueueUrl=self.queue_name,
                    MessageBody=json.dumps(data)
                )
                # print(data)

                # Call Profile Fares
                data['modules'] = {'profileFares' : 1}
                self.sqs.send_message(
                    QueueUrl=self.queue_name,
                    MessageBody=json.dumps(data)
                )
                # print(data)
                
        print("All data sent in queue")

    def load_sectors(self):
        return pd.read_sql(
            """ SELECT DISTINCT CONCAT(Origin,Destination) AS Sector
            FROM QP_DL_FLOWNDATA
            WHERE DepartureDate >= '2023-10-01' AND
            CAST(TRIM(FlightNumber) AS UNSIGNED) < 4000
            """,
            self.rm_rd_esb_conn,
        )

    def load_ndobands(self):
        """Load the NDOBands"""
        sql_query = "SELECT * FROM QP_DW_RMALLOC.ndo_bands"

        ndo_bands = pd.read_sql(sql_query, self.rm_rd_conn)
        ndo_bands = ndo_bands.sort_values(by='ndo_band', ascending=False)
        ndo_bands = ndo_bands[['ndo_band', 'start']].values
        return ndo_bands

    def load_timebands(self):
        """Loads Time Bands appending dcluster and time_ranges"""
        sql_query = """
            SELECT cluster, startTime, endTime
            FROM QP_DW_RMALLOC.config_clusters
            """
        cluster = pd.read_sql(sql_query, self.rm_rd_conn)

        cluster['startTime'] = cluster['startTime'].map(lambda x: str(x).split(' ')[-1])
        cluster['endTime'] = cluster['endTime'].map(lambda x: str(x).split(' ')[-1])
        return cluster

    def load_rg_paths(self):

        response =  self.s3.list_objects_v2(Bucket=self.S3_RM_BUCKET_NAME, Prefix=self.S3_FILE_PATH)
        paths = []
        for obj in response.get('Contents', []):
            # Get the key (path) of the object
            key = obj['Key']
            # Retrieve the object from the bucket
            if any(month in key for month in self.monthList):
                paths.append(key)
                print(f"Retrieved object: {key}")

        # paths = paths[1:]
        print(paths)

        return paths
