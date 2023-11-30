## THIS FILE IS DEPRECATED AND SHOULD BE DELETED IN SUBSEQUENT RELEASES

import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime


class Schema:
    def __init__(self):

        # Establish a connection to the DataLake
        self.conn = create_engine(
            "mysql+mysqldb://rmscriptuser:8BkNw9JEOHoxD6h@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC")

        # Read the Paramaters
        self.parameters = pd.read_csv("params/loadParams.csv")
        self.channelMap = pd.read_csv("params/channelMaps.csv")

        self.load_MarketFareScrapes()

    def load_RateGainScrapeFile(self, RGFile):
        # Load a single RateGain scrapes file

        print(self.channelMap)

        channelMap = {}
        for i in range(len(self.channelMap.index)):
            currSurc = self.channelMap['source'][i]
            currChannel = self.channelMap['channel'][i]
            channelMap[currSurc] = currChannel

        # channelMapParams = self.channelMap.values.tolist()
        # print(channelMapParams)

        # channelMap  = {
        # 	"EaseMyTripDomestic": "B2C",
        # 	"EaseMyTripDomestic_C1": "B2C",
        # 	"EaseMyTripDomestic_C2": "B2C",
        # 	"EaseMyTripDomestic_C3": "B2C",
        # 	"TravelboutiqueonlineCom":"B2B"
        # }
        # print(channelMap)
        print("Scrapes is " + RGFile)

        scrapesdata = pd.read_csv("data/" + RGFile)
        # keep only columns we need
        scrapesdata = scrapesdata[['SiteName', 'DepartureDate', 'SourceAirport', 'DestinationAirport', 'Status',
                                   'GrossFare', 'Outbound_Airline_Code', 'Outbound_Stop', 'Outbound_Flight', 'Outbound_Departuretime']]
        # Map the Channel for use in setting inventory by Channel
        scrapesdata['Channel'] = scrapesdata['SiteName'].map(channelMap)

        print("Rategain data has " + str(scrapesdata.shape[0]) + " records")
        # Filter out NonStops, Available fares only, and remove Akasa fares
        scrapesdata = scrapesdata.query('Outbound_Stop == 0')
        scrapesdata = scrapesdata.query('Status == "Available"')
        scrapesdata = scrapesdata.query('GrossFare > 0')
        scrapesdata = scrapesdata.query('Outbound_Airline_Code != "QP"')

        print("Rategain data has " +
              str(scrapesdata.shape[0]) + " records for Non Akasa nonstops and open flights")
        return scrapesdata

    def dropTable(self, tablename):
        self.conn.execute("DROP TABLE if exists " + tablename)

    def load_MarketFareScrapes(self):
        # Loads all RateGain fare scrapes, and eliminated overlaps

        # Start with the 365 day file
        RGFile = "RateGain0-365File"
        filename = self.parameters.query(
            'Parameter == "' + RGFile + '"')["Value"].values[0]
        scrapesdata = self.load_RateGainScrapeFile(filename)

        self.dropTable("MarketFares")
        scrapesdata.to_sql(name='MarketFares', con=self.conn,
                           if_exists='replace', index=False)
        self.writeLoadLog(filename)

        countQuery = "SELECT count(*) as numRecs from MarketFares"
        nodeData = pd.read_sql(countQuery, self.conn)
        print("Complete Market Fares now has " +
              str(nodeData['numRecs'].values[0]) + " records")

        # Now the 90 day file
        RGFile = "RateGain0-90File"
        filename = self.parameters.query(
            'Parameter == "' + RGFile + '"')["Value"].values[0]
        scrapesdata = self.load_RateGainScrapeFile(filename)

        # Eliminate overlaps
        self.dropTable("tempMarketFares")
        scrapesdata.to_sql(name='tempMarketFares',
                           con=self.conn, if_exists='replace', index=False)
        self.writeLoadLog(filename)

        print("Deleting stale data from Market Fare Scrapes")
        self.conn.execute(
            "Delete from MarketFares where DepartureDate in (select distinct DepartureDate from tempMarketFares)")

        print("Adding fresh data to Market Fare Scrapes")
        self.conn.execute(
            "Insert into MarketFares select * from tempMarketFares")

        countQuery = "SELECT count(*) as numRecs from MarketFares"
        nodeData = pd.read_sql(countQuery, self.conn)
        print("Complete Market Fares now has " +
              str(nodeData['numRecs'].values[0]) + " records")

        # Now the Hourly File
        RGFile = "RateGainHourlyFile"
        filename = self.parameters.query(
            'Parameter == "' + RGFile + '"')["Value"].values[0]
        scrapesdata = self.load_RateGainScrapeFile(filename)

        # Eliminate dups
        self.dropTable("tempMarketFares")
        scrapesdata.to_sql(name='tempMarketFares',
                           con=self.conn, if_exists='replace', index=False)
        self.writeLoadLog(filename)

        print("Deleting stale data from Market Fare Scrapes")
        self.conn.execute(
            "Delete from MarketFares where DepartureDate in (select distinct DepartureDate from tempMarketFares)")

        print("Adding fresh data to Market Fare Scrapes")
        self.conn.execute(
            "Insert into MarketFares select * from tempMarketFares")
        self.conn.execute('commit')

        countQuery = "SELECT count(*) as numRecs from MarketFares"
        nodeData = pd.read_sql(countQuery, self.conn)
        print("Complete Market Fares now has " +
              str(nodeData['numRecs'].values[0]) + " records")

        self.dropTable("tempMarketFares")

    def writeLoadLog(self, filename):
        logQuery = "INSERT INTO QP_DW_RMALLOC.DynAllocLoadLog (`user`,`runTime`,`Filename`)"
        logQuery = logQuery + \
            " VALUES ( '" + os.getlogin() + "','" + \
            str(datetime.now()) + "','" + filename + "')"

        self.conn.execute(logQuery)


def __del__(self):
    # body of destructor
    self.conn.close()


if __name__ == "__main__":
    Schema()
