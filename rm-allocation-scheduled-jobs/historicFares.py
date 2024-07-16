import datetime
import io
import sys
import json
import configsettings
import pandas as pd
from outliers import Outliers

class HistoricFaresModule:

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
        
        # Fetch Time Ranges
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

    def run(self):
        print("Historic Fares started.....")
        self.prevmdate = (pd.Timestamp('today') - pd.DateOffset(months=1)).replace(day=1).strftime('%Y-%m-%d')
        # Call Outliers
        outliers = Outliers(self.rm_rd_esb_conn)
        self.OutliersPath = outliers.outliers(self.dcluster, self.dsectors, self.prevmdate)
        
        print("Outliers data fetched")
        
        print("******* Now Fetching Historic Fares ********")
        self.fetch_all_historic()
        print("******* Now Fetching Historic RateGain Fares ********")
        self.fetch_all_RG()
        print("******* Now Performing Interpolations ********")
        self.L2interp()
        ###################################################################

        self.Historic_Config = self.Historic_Config.dropna()
        print("Historic fares completed for:" + self.dsectors)

        for index, value in self.Historic_Config.iterrows():
            query = f'''
            	INSERT INTO QP_DW_RMALLOC.config_ownFares (sector, startTime, endTime, Month, DOW, channel, `0`,`1`,`2`,`3`,`4`,`5`,`6`,`7`)
                VALUES ('{value['Sector']}', '{value['startTime']}', '{value['endTime']}', '{value['Month']}', '{value['DOW']}', '{value['Channel']}', 
                {value['0_QP']}, {value['1_QP']}, {value['2_QP']}, {value['3_QP']}, {value['4_QP']}, {value['5_QP']}, {value['6_QP']}, {value['7_QP']})
                ON DUPLICATE KEY UPDATE
                startTime = VALUES(startTime),
                endTime = VALUES(endTime),
                Month = VALUES(Month),
                DOW = VALUES(DOW),
                channel = VALUES(channel),
                `0` = VALUES(`0`),
                `1` = VALUES(`1`),
                `2` = VALUES(`2`),
                `3` = VALUES(`3`),
                `4` = VALUES(`4`),
                `5` = VALUES(`5`),
                `6` = VALUES(`6`),
                `7` = VALUES(`7`);
            '''
            self.rm_rd_conn.execute(query)

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

    def BandMaker(self, ndo):
        """Returns an NDOBand for a given NDO value"""
        for el in self.NDBs:
            if ndo >= el[1]:
                return str(int(el[0]))

    def get_sector_bad_dates(self, sector):
        """Get Sector/cluster wise Breakdown of Outliers"""
        cols = ["Sector", "cluster", "DepartureDate", "Outlier"]
        dt = self.OutliersPath[cols].query("Sector==@sector and Outlier==1")
        return dt

    def query_get_cluster(self, sector):
        """Get Time clusters For Flights"""

        starts = self.dcluster.startTime.values
        ends = self.dcluster.endTime.values
        cl = self.dcluster.cluster.values

        sql_query = f"""
                SELECT
                    qdf.DepartureDate,
                    CAST(TRIM(qdf.FlightNumber) AS UNSIGNED) as FltNo,
                    MONTHNAME(qdf.DepartureDate) AS Month,
                    DAYNAME(qdf.DepartureDate) AS DOW,
                    CASE
                        WHEN TIME(qil.STD) >= '{starts[0]}' AND TIME(qil.STD) < '{ends[0]}' THEN {cl[0]}
                        WHEN TIME(qil.STD) >= '{starts[1]}' AND TIME(qil.STD) < '{ends[1]}' THEN {cl[1]}
                        WHEN TIME(qil.STD) >= '{starts[2]}' AND TIME(qil.STD) < '{ends[2]}' THEN {cl[2]}
                        WHEN TIME(qil.STD) >= '{starts[3]}' AND TIME(qil.STD) < '{ends[3]}' THEN {cl[3]}
                        WHEN TIME(qil.STD) >= '{starts[4]}' AND TIME(qil.STD) < '{ends[4]}' THEN {cl[4]}
                        ELSE 5
                    END AS cluster
                FROM
                    QP_DL_FLOWNDATA qdf
                JOIN 
                    QP_DL_NAVDATA.QP_DL_InventoryLeg qil 
                    ON qdf.InventoryLegID = qil.InventoryLegID
                WHERE
                    CONCAT(qdf.Origin, qdf.Destination) = '{sector}' AND
                    qdf.DepartureDate >= '{self.prevmdate}' AND
                    CAST(TRIM(qdf.FlightNumber) AS UNSIGNED) < 4000
        """
        dk = pd.read_sql(sql_query, self.rm_rd_esb_conn)
        dk["DepartureDate"] = pd.to_datetime(dk["DepartureDate"]).dt.strftime("%Y-%m-%d")

        return dk

    def query_writer_pax(self, sector):

        sql_query = f"""
        SELECT
            CONCAT(qdf.Origin, qdf.Destination) AS Sector,
            DATEDIFF(qdf.DepartureDate, qdf.BookingDate) AS NDO,
            qdf.DepartureDate,
            CAST(TRIM(qdf.FlightNumber) AS UNSIGNED) as FltNo,
            (qdf.DiscountedBaseFare_Pro + qdf.RCS_Pro + qdf.CUTE_Pro + qdf.YQ_Pro + qdf.SGST_Pro + qdf.UGST_Pro + qdf.IGST_Pro + qdf.CGST_Pro + qdf.AirportCharges_Pro + qdf.DUDF_Pro) AS ALLIN,
            CASE 
                WHEN SUBSTRING(qdf.farebasis, 5, 1) = 'M' THEN 'B2B'
                WHEN SUBSTRING(qdf.farebasis, 5, 1) = 'R' THEN 'B2C'
            END AS Channel
        FROM
            QP_DL_FLOWNDATAPAX qdf
        WHERE
            CONCAT(qdf.Origin, qdf.Destination) = '{sector}' AND
            qdf.DepartureDate >= '{self.prevmdate}' AND
            SUBSTRING(qdf.farebasis, 5, 1) IN ('R', 'M') AND
            CAST(TRIM(qdf.FlightNumber) AS UNSIGNED) < 4000
        """
        df = pd.read_sql(sql_query, self.rm_rd_esb_conn)
        df["DepartureDate"] = pd.to_datetime(df["DepartureDate"]).dt.strftime("%Y-%m-%d")

        # Call the Time Variables here and merge them
        dcols = self.query_get_cluster(sector)
        df = pd.merge(dcols, df, how="left", on=["DepartureDate", "FltNo"])
        try:
            # Fetch the Outliers and Remove them:
            if self.OutliersPath is not None and not self.OutliersPath.empty:
                dout = self.get_sector_bad_dates(sector)
                df = pd.merge(df, dout, how="left", on=["Sector", "cluster", "DepartureDate"])
                df = df[df.Outlier.ne(1)]
                del df["Outlier"]
        except:
            pass

        # Drop redundnat values and then
        df = df[df.ALLIN.gt(300)].dropna()

        # Create NDO Bands
        df["NDO"] = df["NDO"].astype(int)
        df["NDOBand"] = df["NDO"].apply(self.BandMaker)

        # Aggregate on the basis of require column order & make a pivot
        col_order = ["Sector", "Month", "DOW", "Channel", "cluster", "NDOBand"]
        df = (df.groupby(col_order).agg(Fare=("ALLIN", "median")).reset_index())  # Median instead of mean

        df = df.pivot_table(index=col_order[:-1], columns="NDOBand", values="Fare").reset_index()
        df.Name = None

        return df

    def fetch_all_historic(self):

        # Initialize an empty frame
        df = pd.DataFrame()

        try:
            dfx = self.query_writer_pax(self.dsectors)
            df = pd.concat([df, dfx])

            # Ensure they are the correct shape, throw errors otherwise
            assert dfx.shape[0] > 0
            assert dfx.shape[1] == 13
            print(f"{self.dsectors} data fetched successfully. Shape: {dfx.shape}.")
        except:
            print(f"{self.dsectors} data unable to fetch. Pushed to Failed Log...")
        # Save data to required destination
        df = df.merge(self.dcluster, how="left", on=["cluster"])

        self.dfHist = df

    def fetch_RG_pathwise(self, path):
        usecols = ["Channel", "AirlineCode", "SourceAirport", "DestinationAirport", "Stops", "Dep_Time",
                   "DepartureDate", "AvgFare", "NDO", ]
        response = self.s3.get_object(Bucket=self.S3_RM_BUCKET_NAME, Key=path)
        parquet_file = response["Body"].read()
        dfx = pd.read_parquet(io.BytesIO(parquet_file), columns=usecols).rename(
            columns={
                "SourceAirport": "Origin",
                "DestinationAirport": "Destination",
            }
        )

        dfx["Sector"] = dfx["Origin"] + dfx["Destination"]
        dfx = dfx[dfx.AirlineCode.isin(["QP", "6E", "AI", "UK", "SG", "I5"]) & dfx.Stops.eq("0")]
        dfx.drop(columns=["AirlineCode", "Stops", "Origin", "Destination"], inplace=True)

        dfx["cluster"] = dfx["Dep_Time"].apply(self.get_cluster)
        dfx["NDOBand"] = dfx["NDO"].apply(self.BandMaker)
        dfx["DOW"] = pd.to_datetime(dfx["DepartureDate"]).dt.day_name()
        dfx["Month"] = pd.to_datetime(dfx["DepartureDate"]).dt.month_name()
        dfx = pd.pivot_table(dfx,
                             index=["Sector", "Month", "DOW", "Channel", "cluster"],
                             values="AvgFare",
                             columns="NDOBand",
                             ).reset_index()
        return dfx

    def fetch_all_RG(self):
        full_RG = pd.DataFrame()

        print("Working:", self.RGpaths)
        dfx = self.fetch_RG_pathwise(self.RGpaths)
        full_RG = pd.concat([full_RG, dfx])

        full_RG["cluster"] = full_RG["cluster"].astype(int)

        self.full_RG = full_RG

    def L1interp(self, full_df, full_RG):

        ons = ["Sector", "Month", "DOW", "Channel", "cluster"]
        qpcs = [str(x) + "_QP" for x in range(8)]
        rgs = [str(x) + "_RG" for x in range(8)]

        dfz = pd.merge(full_df.round(1), full_RG.round(1), "left", on=ons, suffixes=("_QP", "_RG"))
        try:
            dfz["Mean_RG"] = dfz[rgs].mean(axis=1)
            dfz["Mean_QP"] = dfz[qpcs].mean(axis=1)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            sys.exit(1)

        for i in range(8):
            idx = dfz[dfz[f"{i}_QP"].isna()].index

            dfz.loc[idx, f"{i}_QP"] = (dfz.loc[idx, f"{i}_RG"] * dfz.loc[idx, "Mean_QP"] / dfz.loc[idx, "Mean_RG"])

        dfz[qpcs] = dfz[qpcs].interpolate(method="linear", axis=1, limit_direction="both")
        full = [*ons, *qpcs]
        dfz = dfz[full]
        return dfz

    def L2interp(self):

        # perform L1-interpolation here
        full_df = self.dfHist
        full_RG = self.full_RG
        dfz = self.L1interp(full_df=full_df, full_RG=full_RG)

        # Make a DataFrame that has all the Sequences:
        ons = ["Sector", "Month", "DOW", "Channel", "cluster"]
        qpcs = [str(x) + "_QP" for x in range(8)]
        colsn = ons + qpcs

        # Create a cross join containing all possibilities:
        df1 = full_df[["Sector", "Month"]].drop_duplicates()
        df2 = full_RG[["DOW", "Channel", "cluster"]].drop_duplicates()
        df1 = pd.merge(df1, df2, how="cross")

        df1 = df1.merge(dfz.round(1), how="left", on=ons)
        df1 = df1.merge(full_RG.round(1), how="left", on=ons)

        # Get the inter-cluster mean
        dft = (df1.groupby(ons[:-1], as_index=False).agg("mean").drop(columns=["cluster"]))

        df1 = df1.merge(dft, how="left", on=ons[:-1], suffixes=("", "_mu"))

        # Now Perform L2 Interpolations:
        for i in range(8):
            idx = df1[df1[f"{i}_QP"].isna()].index

            df1.loc[idx, f"{i}_QP"] = (df1.loc[idx, f"{i}"] * df1.loc[idx, f"{i}_QP_mu"] / df1.loc[idx, f"{i}_mu"])

        df1[qpcs] = df1[qpcs].interpolate(method="linear", axis=1, limit_direction="both")

        # Finally, clean the dataframe, add the start and end data
        df2 = df1[colsn]
        df2 = pd.merge(df2, self.dcluster, on=["cluster"], how="left")
        self.Historic_Config = df2
