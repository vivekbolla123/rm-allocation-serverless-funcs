import traceback
import numpy as np
import pandas as pd
from sklearn.neighbors import LocalOutlierFactor


class Outliers:

    def __init__(self, rm_rd_esb_conn):
        self.rm_rd_esb_conn = rm_rd_esb_conn

    def outliers(self, clusters, sector, date):
        self.Outliers = None
        self.dcluster = clusters
        self.prevmdate = date
        self.process_AF(sector)
        if self.Outliers is not None:
            if not self.Outliers.empty:
                return self.Outliers.dropna()
        # dfAF.to_csv('OutliersTest.csv', index=False)
        return None

    def process_AF(self, sector):
        try:
            self.df = self.load_data(sector)
            dhist = self.df
            colx = 'cluster'

            # Get 28 Day Rolling Median and create upper and lower bounds for Outlier Rejection
            print('-- -- Getting Rolling Median')
            dhist = dhist.groupby(['Sector', colx], as_index=False).apply(self.roll_med).reset_index(drop=True)

            # Run the LocalOutlierFactor Algorithm for Outlier Detection with Time Vars over 30 Neighbours
            print('-- -- Getting Global Outlier Factors')
            dhist = dhist.groupby(['Sector', colx], as_index=False).apply(self.LofNORM).reset_index(drop=True)

            # Run the Local Outlier Factor Algorithm for Outlier Detection locally with no time vars
            print('-- -- Getting Local Outlier Factors')
            for n in [7, 14]:
                dhist = dhist.groupby(['Sector', colx], as_index=False).apply(lambda x: self.Lof_m_s(x, n=n)).reset_index(drop=True)

            # Run the Local Outlier Factor Algorithm for Outlier Detection with weeks
            print('-- -- Getting Weekly Outlier Factors')
            dhist = dhist.groupby(['Sector', colx], as_index=False).apply(self.Lof_w).reset_index(drop=True)

            # Now, we will finally make assignments:
            # Initially all points are normal
            dhist['Outlier'] = 0
            
            if dhist.shape[0] != 0:

                # Now those points with Score < -1.6 is guaranteed to be an outlier initially
                c1 = dhist['LOF'].le(-1.6) | dhist['LOF_s'].le(-1.6) | dhist['LOF_m'].le(-1.6) | dhist['LOF_w'].le(-2)
                dhist.loc[c1, 'Outlier'] = 1

                # If Average of any three is less than -1.5 then also outlier
                a1 = (dhist['LOF'] + dhist['LOF_s'] + dhist['LOF_m']).lt(-4.5)
                a2 = (dhist['LOF'] + dhist['LOF_s'] - np.power(-1 * dhist['LOF_w'], 0.5)).lt(-5)
                a3 = (dhist['LOF'] + dhist['LOF_m'] - np.power(-1 * dhist['LOF_w'], 0.5)).lt(-5)
                a4 = (dhist['LOF_s'] + dhist['LOF_m'] - np.power(-1 * dhist['LOF_w'], 0.5)).lt(-5)
                c2 = a1 | a2 | a3 | a4
                dhist.loc[c2, 'Outlier'] = 1

                # Now those points that fall under rolling median are not outliers
                c5 = (dhist['AF'] <= dhist['AFMu']) & (dhist['AF'] >= dhist['AFMl'])
                dhist.loc[c5, 'Outlier'] = 0

                # ALso those with either value less than -1.2 are not outliers
                a1 = dhist['LOF'].gt(-1.4)
                a2 = dhist['LOF_s'].gt(-1.4)
                a3 = dhist['LOF_m'].gt(-1.4)
                a4 = np.power(-1 * dhist['LOF_w'], 0.333333).lt(1.5)
                c3 = (a1 & a2) | (a2 & a3) | (a3 & a1) | (a4 & a3) | (a4 & a2) | (a4 & a1)
                dhist.loc[c3, 'Outlier'] = 0

                # IF LOF_S is less than <-1.2 then reject as well
                dhist.loc[dhist['LOF_s'].gt(-1.15) | dhist['LOF'].gt(-1.15), 'Outlier'] = 0

                # Clean the Dataframe
                dhist = dhist.merge(self.dcluster, on=['cluster'])

                self.Outliers = dhist
        except Exception as e:
            print("Error occurred: ",e)
            traceback.print_exc()
            

    def load_data(self, sector):

        starts = self.dcluster.startTime.values
        ends = self.dcluster.endTime.values
        cl = self.dcluster.cluster.values

        sql_query = f"""
            SELECT
                CONCAT(qf.Origin, qf.Destination) AS Sector,
                qf.DepartureDate, qf.RevenuePax AS Seats,
                (qf.Taxes + qf.CUTE + qf.DiscountedBaseFare + qf.YQ + qf.RCS + qf.AirportCharges) AS Revenue,
                CASE
                    WHEN TIME(il.STD) >= '{starts[0]}' AND TIME(il.STD) < '{ends[0]}' THEN {cl[0]}
                    WHEN TIME(il.STD) >= '{starts[1]}' AND TIME(il.STD) < '{ends[1]}' THEN {cl[1]}
                    WHEN TIME(il.STD) >= '{starts[2]}' AND TIME(il.STD) < '{ends[2]}' THEN {cl[2]}
                    WHEN TIME(il.STD) >= '{starts[3]}' AND TIME(il.STD) < '{ends[3]}' THEN {cl[3]}
                    WHEN TIME(il.STD) >= '{starts[4]}' AND TIME(il.STD) < '{ends[4]}' THEN {cl[4]}
                    ELSE 5
                END AS cluster
            FROM
                QP_DL_NAVDATA.QP_DL_FLOWNDATA qf
            LEFT JOIN
                QP_DL_NAVDATA.QP_DL_InventoryLeg il 
                ON qf.InventoryLegID = il.InventoryLegID
            WHERE
                CONCAT(qf.Origin, qf.Destination) = '{sector}' AND
                qf.DepartureDate >= '{self.prevmdate}' 
                AND qf.RevenuePax > 0
                AND CAST(TRIM(qf.FlightNumber) AS UNSIGNED) < 4000
            """
        df = pd.read_sql(sql_query, self.rm_rd_esb_conn)
        print("Data fetched")

        df = df.groupby(['Sector', 'cluster', 'DepartureDate'], as_index=False).agg({'Seats': 'sum', 'Revenue': 'sum'})

        # Get Average Fare and RASK:
        df['AF'] = df['Revenue'] / df['Seats']

        # Create Time Variables:
        df['DOW'] = pd.to_datetime(df['DepartureDate']).dt.weekday
        df['s_DOW'] = np.sin((2 * np.pi * df['DOW']) / 7)
        df['c_DOW'] = np.cos((2 * np.pi * df['DOW']) / 7)
        df['ord'] = pd.to_datetime(df['DepartureDate']).apply(lambda x: x.toordinal())
        valid_ord = df['ord'].dropna()
        if len(valid_ord) > 0:
            maxord, minord = valid_ord.max(), valid_ord.min()
            if maxord != minord:  # Prevent division by zero
                df['ord'] = (df['ord'] - minord) / (maxord - minord)
            else:
                df['ord'] = 0
        else:
            # Handle the case where all values are NaT
            df['ord'] = np.nan
            
        # maxord, minord = df['ord'].max(), df['ord'].min()
        # df['ord'] = (df['ord'] - minord) / (maxord - minord)
        df.drop(columns=['Seats', 'Revenue'], inplace=True)

        # ensure that only those groups that have more than 60 points are considered:
        df = df.groupby(['Sector', 'cluster'], as_index=False).filter(lambda x: len(x) >= 90)

        self.df = df
        return df

    def Lof_m_s(self, group, n):
        """Will be called twice, with n = 7 and 14"""

        X = group['AF'].values.reshape(-1, 1)
        try:
            lof = LocalOutlierFactor(n_neighbors=n, contamination='auto')
            lof.fit_predict(X)

            if n == 7:
                group['LOF_s'] = lof.negative_outlier_factor_

            if n == 14:
                group['LOF_m'] = lof.negative_outlier_factor_

        except:
            group['LOF_m'] = -1.3

        return group

    def Lof_w(self, group):
        """Weekly LocalOutlierFactor"""

        group['LOF_w'] = -1

        for i in group.DOW.unique():
            ci = group.DOW.eq(i)

            X = group.loc[ci]['AF'].values.reshape(-1, 1)

            try:
                lof = LocalOutlierFactor(n_neighbors=4, contamination='auto')
                outliers = lof.fit_predict(X)
                group.loc[ci, 'LOF_w'] = lof.negative_outlier_factor_
            except:
                pass

        return group

    def roll_med(self, group, cl='AF'):
        """Takes the rolling median to define outlier rejection range"""
        tol = 0.22
        afm = cl + 'M'
        afmu = cl + 'Mu'
        afml = cl + 'Ml'
        group[afm] = group[cl].rolling(window=28, min_periods=7).median()
        group[afmu] = (1 + tol) * group[afm]
        group[afml] = (1 - tol) * group[afm]

        return group

    def LofNORM(self, group, n_neighbors=30, cl='AF'):
        """Takes a global outlier detection approach"""

        # Normalize Fares now:
        afmax, afmin = group[cl].max(), group[cl].min()
        group['AFN'] = (group[cl] - afmin) / (afmax - afmin)

        # Let's create weights -- Fare has the majority precedence so it should be 60
        # Dates are equally important so 20 and DOW 10, 10 each
        weights = np.array([40, 3, 3, 3])
        X = group[['AFN', 'c_DOW', 's_DOW', 'ord']].values * weights

        # Fit & Predict outliers
        try:
            lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination='auto')
            lof.fit_predict(X)

            # let's get their scores
            # Generally LOF < -1.5 means that a given point is an outlier
            group['LOF'] = lof.negative_outlier_factor_

        except:
            group['LOF'] = -1.3

        del group['AFN']

        return group
