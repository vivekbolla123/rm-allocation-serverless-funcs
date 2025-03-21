import pandas as pd
from scipy.stats import poisson


class NoShowProbs:
    def __init__(self, logger, params, overrides, context) -> None:
        self.context = context
        self.logger = logger
        self.rm_wr_conn = params['wrconn']
        self.rm_rd_conn = params['rdconn']
        self.rm_rd_esb_conn = params['rdEsbConn']
        self.load_timebands()

    def load_timebands(self):
        sql_query = """
            SELECT cluster, startTime, endTime
            FROM QP_DW_RMALLOC.config_clusters
        """
        dcluster = pd.read_sql(sql_query, self.rm_rd_conn)
        dcluster['startTime'] = dcluster['startTime'].astype(
            str).str.split(' ').str[-1]
        dcluster['endTime'] = dcluster['endTime'].astype(
            str).str.split(' ').str[-1]
        self.dcluster = dcluster

    def fetch_data(self):
        starts = self.dcluster.startTime.values
        ends = self.dcluster.endTime.values
        cl = self.dcluster.cluster.values

        sql_query = f"""
            SELECT
                CONCAT(qdf.Origin, qdf.Destination) AS Sector,
                qdf.DepartureDate, qdf.NoShowPax,
                CASE
                    WHEN TIME(qil.STD) >= '{starts[0]}' AND TIME(qil.STD) < '{ends[0]}' THEN {cl[0]}
                    WHEN TIME(qil.STD) >= '{starts[1]}' AND TIME(qil.STD) < '{ends[1]}' THEN {cl[1]}
                    WHEN TIME(qil.STD) >= '{starts[2]}' AND TIME(qil.STD) < '{ends[2]}' THEN {cl[2]}
                    WHEN TIME(qil.STD) >= '{starts[3]}' AND TIME(qil.STD) < '{ends[3]}' THEN {cl[3]}
                    WHEN TIME(qil.STD) >= '{starts[4]}' AND TIME(qil.STD) < '{ends[4]}' THEN {cl[4]}
                    ELSE 5
                END AS cluster
            FROM QP_DL_FLOWNDATA qdf
            LEFT JOIN QP_DL_InventoryLeg qil ON qil.InventoryLegID = qdf.InventoryLegID
            WHERE qdf.DepartureDate >= '2023-10-01' AND qdf.Capacity > 0 AND qdf.NoShowPax <= 7
        """

        df = pd.read_sql(sql_query, self.rm_rd_esb_conn)
        df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
        df['Month'] = df['DepartureDate'].dt.month_name()
        df['DOW'] = df['DepartureDate'].dt.day_name()
        self.df = df

    def poisson_dist(self, group, level):
        lambda_grp = group['NoShowPax'].mean()
        n_grp = len(group['NoShowPax'])

        probs = [1 - poisson.cdf(x, lambda_grp) for x in range(2)]
        for i, prob in enumerate(probs):
            group[f'P{i}'] = prob

        c1 = group.filter(like='P').columns
        c2 = [*level, *c1]
        group = group[c2].drop_duplicates()

        group['N'] = n_grp
        return group

    def postprocess(self, df):
        column_order = ['Sector', 'Month', 'cluster', 'N', 'P0', 'P1']
        df = df[column_order]

        df = df.melt(id_vars=['Sector', 'Month', 'cluster',
                     'N'], var_name='OBCount', value_name='Prob')
        df['OBCount'] = df['OBCount'].str.extract('(\d+)').astype(int) + 1

        df = df[df['OBCount'] <= 2]
        df = df.sort_values(by=['Sector', 'Month', 'cluster'])
        return df

    def run(self):
        level = ['Sector', 'Month', 'cluster']

        self.logger.info("Fetching Raw Data ...")
        self.fetch_data()
        self.logger.info("Data Fetch Complete...")

        self.logger.info("Processing Data...")
        probdf = self.df.groupby(level, as_index=False).apply(
            lambda x: self.poisson_dist(x, level)).reset_index(drop=True)
        self.logger.info("Data Processing Complete...")
        probdf = pd.merge(probdf, self.dcluster, how = 'left', on = 'cluster')
        probdf = self.postprocess(probdf)
        probdf=probdf.drop_duplicates()
        self.logger.info("Data retrieved in required column order")
        self.rm_wr_conn.execute("TRUNCATE no_show_probabilities")
        probdf.to_sql("no_show_probabilities", self.rm_wr_conn,
                      if_exists='append', index=False)
