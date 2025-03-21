import datetime
import io
import configsettings
from outliers import Outliers
import json
import pandas as pd
import numpy as np
from boto3 import client


class ProfileFaresModule:

    def __init__(self, logger, params, overrides, context):
        """Initialize all Variables, only input is the Engine Key Profile"""
        self.context = context
        self.logger = logger
        self.rm_rd_conn = params['wrconn']
        self.rm_rd_esb_conn = params['rdEsbConn']
        self.sqs = params['sqs']
        self.s3 = params['s3']
        
        self.S3_RM_BUCKET_NAME = configsettings.S3_RM_BUCKET_NAME
        self.S3_FILE_PATH = configsettings.S3_FILE_PATH


        self.time_ranges = [
            (datetime.time(2, 30), datetime.time(7, 30), 0),
            (datetime.time(7, 30), datetime.time(11, 0), 1),
            (datetime.time(11, 0), datetime.time(14, 30), 2),
            (datetime.time(14, 30), datetime.time(18, 0), 3),
            (datetime.time(18, 0), datetime.time(21, 0), 4),
            (datetime.time(21, 0), datetime.time(23, 59), 5),
            (datetime.time(0, 0), datetime.time(2, 30), 5)
        ]
        
        # Initiliaze other variables
        self.dsectors = overrides['sector']
        self.NDBs = overrides['ndo_bands']
        self.dcluster = pd.DataFrame(json.loads(overrides['cluster']))
        self.RGpaths = overrides['rg_paths']

        # This is where we will store our Dataframes
        self.dfProf = None
        self.full_RG = None
        self.Profile_Config = None

    def run(self):
        print("profile fares started.....")
        self.prevmdate = (pd.Timestamp('today') - pd.DateOffset(months=1)).replace(day=1).strftime('%Y-%m-%d')
        # Call Outliers
        outliers = Outliers(self.rm_rd_esb_conn)
        self.OutliersPath = outliers.outliers(self.dcluster, self.dsectors, self.prevmdate)
        print("Outliers data fetched")
        
        print("******* Now Fetching Profile Fares ********")
        self.fetch_all_profile()
        print("******* Now Fetching Profile RateGain Fares ********")
        self.fetch_all_RG()
        print("******* Now Performing Interpolations ********")
        self.L2interp()

        self.Profile_Config = self.Profile_Config.dropna()

        qs = ["Q" + str(i) for i in range(21)]
        col_order = ["Sector", "cluster", "Month", "DOW", *qs, "startTime", "endTime"]
        self.Profile_Config = self.Profile_Config[col_order]
        self.Profile_Config = self.Profile_Config.dropna()

        for index, value in self.Profile_Config.iterrows():
            query = f'''
            	INSERT INTO QP_DW_RMALLOC.config_profile_fares (sector, startTime, endTime, month, dow, cluster, `Q0`,`Q1`,`Q2`,`Q3`,`Q4`,`Q5`,`Q6`,`Q7`,`Q8`,`Q9`,`Q10`,`Q11`,`Q12`,`Q13`,`Q14`,`Q15`,`Q16`,`Q17`,`Q18`,`Q19`,`Q20`)
                VALUES ('{value['Sector']}', '{value['startTime']}', '{value['endTime']}', '{value['Month']}', '{value['DOW']}', '{value['cluster']}', 
                        {value['Q0']}, {value['Q1']}, {value['Q2']}, {value['Q3']}, {value['Q4']}, {value['Q5']}, {value['Q6']}, {value['Q7']},
                        {value['Q8']}, {value['Q9']}, {value['Q10']}, {value['Q11']}, {value['Q12']}, {value['Q13']}, {value['Q14']}, {value['Q15']},
                        {value['Q16']}, {value['Q17']}, {value['Q18']}, {value['Q19']}, {value['Q20']})
                ON DUPLICATE KEY UPDATE
                    startTime = VALUES(startTime),
                    endTime = VALUES(endTime),
                    month = VALUES(month),
                    dow = VALUES(dow),
                    cluster = VALUES(cluster),
                    `Q0` = VALUES(`Q0`),
                    `Q1` = VALUES(`Q1`),
                    `Q2` = VALUES(`Q2`),
                    `Q3` = VALUES(`Q3`),
                    `Q4` = VALUES(`Q4`),
                    `Q5` = VALUES(`Q5`),
                    `Q6` = VALUES(`Q6`),
                    `Q7` = VALUES(`Q7`),
                    `Q8` = VALUES(`Q8`),
                    `Q9` = VALUES(`Q9`),
                    `Q10` = VALUES(`Q10`),
                    `Q11` = VALUES(`Q11`),
                    `Q12` = VALUES(`Q12`),
                    `Q13` = VALUES(`Q13`),
                    `Q14` = VALUES(`Q14`),
                    `Q15` = VALUES(`Q15`),
                    `Q16` = VALUES(`Q16`),
                    `Q17` = VALUES(`Q17`),
                    `Q18` = VALUES(`Q18`),
                    `Q19` = VALUES(`Q19`),
                    `Q20` = VALUES(`Q20`);
            '''
            self.rm_rd_conn.execute(query)

        self.logger.info("Profile fares completed for:" + self.dsectors)

    def query_writer_pax(self, sector):

        sql_query = f"""
        SELECT
            CONCAT(qdf.Origin, qdf.Destination) AS Sector,
            qdf.DepartureDate,
            CAST(TRIM(qdf.FlightNumber) AS UNSIGNED) as FltNo,
            (qdf.DiscountedBaseFare_Pro + qdf.RCS_Pro + qdf.CUTE_Pro + qdf.YQ_Pro + qdf.SGST_Pro + qdf.UGST_Pro + qdf.IGST_Pro + qdf.CGST_Pro + qdf.AirportCharges_Pro + qdf.DUDF_Pro) AS ALLIN        FROM
            QP_DL_FLOWNDATAPAX qdf
        WHERE
            CONCAT(qdf.Origin, qdf.Destination) = '{sector}' AND
            qdf.DepartureDate >= '{self.prevmdate}' AND 
            SUBSTRING(qdf.farebasis, 5, 1) IN ('R', 'M') AND
            CAST(TRIM(qdf.FlightNumber) AS UNSIGNED) < 4000
        """

        df = pd.read_sql(sql_query, self.rm_rd_esb_conn)
        df["DepartureDate"] = pd.to_datetime(df["DepartureDate"]).dt.strftime("%Y-%m-%d")

        # Get the Time Variables Next:
        dcap = self.query_get_cluster(sector)
        df = pd.merge(df, dcap, how="left", on=["FltNo", "DepartureDate"])

        try:
            # Fetch the Outliers and Remove them:
            if self.OutliersPath is not None and not self.OutliersPath.empty:
                dout = self.get_sector_bad_dates(sector)
                df = pd.merge(df, dout, how="left", on=["Sector", "cluster", "DepartureDate"])
                df = df[df.Outlier.ne(1)]
                del df["Outlier"]
        except:
            pass

        # Ensure that your ALLIN is greater than  100 atleast.
        df = df[df.ALLIN.gt(350)].dropna()

        qs = np.array(
            [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9,
             0.95, 0.98, ])

        df = (
            df.groupby(["Sector", "cluster", "Month", "DOW"])["ALLIN"]
            .quantile(qs)
            .unstack()
        )

        # Rename columns to reflect quantile values
        cols_names = []
        for i in range(21):
            cols_names.append(f"Q_{i}")

        df.columns = cols_names

        # Reset index to turn multi-index into columns
        df.reset_index(inplace=True)

        return df

    def query_get_cluster(self, sector):

        starts = self.dcluster['startTime']
        ends = self.dcluster['endTime']
        cl = self.dcluster['cluster']

        sql_query = f"""
        SELECT
            qdf.DepartureDate,
            CAST(TRIM(qdf.FlightNumber) AS UNSIGNED) as FltNo,
            MONTHNAME(qdf.DepartureDate) AS Month,
            DAYNAME(qdf.DepartureDate) AS DOW,
            CASE
                WHEN TIME(il.STD) >= '{starts['0']}' AND TIME(il.STD) < '{ends['0']}' THEN {cl['0']}
                WHEN TIME(il.STD) >= '{starts['1']}' AND TIME(il.STD) < '{ends['1']}' THEN {cl['1']}
                WHEN TIME(il.STD) >= '{starts['2']}' AND TIME(il.STD) < '{ends['2']}' THEN {cl['2']}
                WHEN TIME(il.STD) >= '{starts['3']}' AND TIME(il.STD) < '{ends['3']}' THEN {cl['3']}
                WHEN TIME(il.STD) >= '{starts['4']}' AND TIME(il.STD) < '{ends['4']}' THEN {cl['4']}
                ELSE 5
            END AS cluster
        FROM
            QP_DL_FLOWNDATA qdf
        JOIN 
            QP_DL_NAVDATA.QP_DL_InventoryLeg il 
            ON qdf.InventoryLegID = il.InventoryLegID
        WHERE
            CONCAT(qdf.Origin, qdf.Destination) = '{sector}' AND
            qdf.DepartureDate >= '{self.prevmdate}' AND
            CAST(TRIM(qdf.FlightNumber) AS UNSIGNED) < 4000
        """
        dk = pd.read_sql(sql_query, self.rm_rd_esb_conn)
        dk["DepartureDate"] = pd.to_datetime(dk["DepartureDate"]).dt.strftime("%Y-%m-%d")

        return dk

    def get_sector_bad_dates(self, sector):
        """Get Sector/cluster wise Breakdown of Outliers"""
        cols = ["Sector", "cluster", "DepartureDate", "Outlier"]
        dt = self.OutliersPath[cols].query("Sector==@sector and Outlier==1")
        return dt

    def fetch_all_profile(self):

        # Initialize an empty frame
        df = pd.DataFrame()
        try:
            dfx = self.query_writer_pax(self.dsectors)
            df = pd.concat([df, dfx])

            # Ensure they are the correct shape, throw errors otherwise
            assert dfx.shape[0] > 0
            assert dfx.shape[1] == 25
            print(f"{self.dsectors} data fetched successfully. Shape: {dfx.shape}.")
        except:
            print(f"{self.dsectors} data unable to fetch. Pushed to Failed Log...")
        # Save data to required destination
        df = df.merge(self.dcluster, how="left", on=["cluster"])

        self.dfProf = df

    def get_cluster(self, dep_time):
        """Takes in a departuretime and returns the relevant cluster"""
        for start, end, cluster in self.time_ranges:
            if start <= end:
                if start <= dep_time < end:
                    return int(cluster)
            else:
                if start <= dep_time or dep_time < end:
                    return int(cluster)
        return None

    def fetch_RG_pathwise(self, path):
        usecols = ["AirlineCode", "Sector", "Stops", "Dep_Time", "DepartureDate", "AvgFare", "NDO", ]

        response = self.s3.get_object(Bucket=self.S3_RM_BUCKET_NAME, Key=path)
        parquet_file = response["Body"].read()
        dfx = pd.read_parquet(io.BytesIO(parquet_file), columns=usecols)

        dfx = dfx[dfx.NDO.between(30, 40)]

        dfx = dfx[dfx.AirlineCode.isin(["6E", "AI", "UK", "SG", "I5", "QP"]) & dfx.Stops.eq("0")]

        dfx["cluster"] = dfx["Dep_Time"].apply(self.get_cluster)
        dfx["DOW"] = pd.to_datetime(dfx["DepartureDate"]).dt.day_name()
        dfx["Month"] = pd.to_datetime(dfx["DepartureDate"]).dt.month_name()

        dfx.drop(columns=["AirlineCode", "Stops", "Dep_Time", "DepartureDate", "NDO"], inplace=True)

        dfx = dfx.groupby(["Sector", "Month", "DOW", "cluster"]).agg(Mu=("AvgFare", "mean"))

        dfx.reset_index(inplace=True)

        return dfx

    def fetch_all_RG(self):
        full_RG = pd.DataFrame()

        print("Working:", self.RGpaths)
        dfx = self.fetch_RG_pathwise(self.RGpaths)
        full_RG = pd.concat([full_RG, dfx])

        full_RG["cluster"] = full_RG["cluster"].astype(int)

        self.full_RG = full_RG

    def L2interp(self):
        """Interpolation Sequences"""

        # Filter out those sequences which we have actually flown and drop nan
        dfx = self.dfProf[["Sector", "Month", "DOW"]].drop_duplicates()
        full_RG = pd.merge(self.full_RG, dfx, how="right", on=["Sector", "Month", "DOW"]).dropna()

        # Now we create the dataframe that has all the remainder sequences to interp
        dfm = pd.merge(full_RG, self.dfProf, how="left", on=["Sector", "cluster", "Month", "DOW"])

        # Now we create the dataframe that has the aggreagtes of all present sequences
        dt = (
            dfm.dropna()
            .groupby(["Sector", "Month", "DOW"], as_index=False)
            .agg("mean")
            .drop(columns=["cluster"])
        )

        # Now, merge these aggregates with the original full sequence dataframe
        dfMerged = pd.merge(dfm, dt, how="left", on=["Sector", "Month", "DOW"], suffixes=("", "_qp"))

        # Finally, with all values at hand, perform the interpolation
        for i in np.arange(21):
            idx = dfMerged[dfMerged[f"Q_{i}"].isna()].index

            dfMerged.loc[idx, f"Q_{i}"] = (
                    dfMerged.loc[idx, f"Q_{i}_qp"]
                    * dfMerged.loc[idx, "Mu"]
                    / dfMerged.loc[idx, "Mu_qp"]
            )

        # Filter only those columns that you want to keep
        dfMerged = dfMerged[self.dfProf.columns]

        for i in range(21):
            dfMerged = dfMerged.rename(columns={f"Q_{i}": f"Q{i}"})

        # Add the Start and End columns again
        dfMerged = pd.merge(dfMerged.drop(columns=["startTime", "endTime"]),
                            self.dcluster,
                            on=["cluster"],
                            how="left",
                            )

        self.Profile_Config = dfMerged
