import sys
import pandas as pd
from datetime import datetime


class Schema:
    def __init__(self, mongo_db):
        # Establish a connection to MongoDB
        self.db = mongo_db
        self.dw_db = mongo_db

    def initializTables(self):
        self.load_generic_collection("curves", "Curves.csv")
        self.load_generic_collection("fares", "Fares.csv")
        self.load_generic_collection("market_fares_rg", "MarketFares.csv")
        self.load_generic_collection("market_fares_ag", "MarketFares.csv")
        self.load_generic_collection("bookload", "Bookload.csv")
        self.load_generic_collection("market_list", "MarketList.csv")
        self.load_generic_collection("market_list_connections", "MarketList_Connections.csv")
        self.load_generic_collection("d1_d2_strategies", "d1d2Strategy.csv")
        self.load_generic_collection("allocation_acceptable_range_d1", "acceptableRange.csv")
        self.load_generic_collection("dplf_bands", "dplf_bands.csv")
        self.load_generic_collection("ndo_bands", "ndo_bands.csv")
        self.load_generic_collection("time_ranges", "time_ranges.csv")
        self.load_generic_collection("rm_parameter_values", "rm_parameter_values.csv")
        self.load_generic_collection("config_pfl_threshold", "config_pfl_threshold.csv")
        self.load_generic_collection("departure_time_ranges", "departure_time_ranges.csv")
        self.load_generic_collection("booking_fare_data", "booking_fare_data.csv")
        self.load_generic_collection("config_share_of_booking", "config_share_of_booking.csv")
        self.load_generic_collection("config_runtime", "config_runTime.csv")
        self.load_generic_collection("no_show_probabilities", "no_show_probabilities.csv")
        self.load_generic_collection("config_ndo_range", "config_ndo_range.csv")
        self.load_generic_collection("config_marketfares_rerouting", "config_marketFares_rerouting.csv")
        self.load_generic_collection("config_profile_fares", "config_profile_fares.csv")
        self.load_generic_collection("config_profile_fare_station_upsell", "config_profile_fare_station_upsell.csv")
        self.load_generic_collection("config_date_event", "config_date_event.csv")
        self.load_generic_collection("config_determine_fares", "config_determine_fares.csv")
        self.load_generic_collection("current_opening_rbd", "current_opening_rbd.csv")
        self.load_generic_collection("default_distress_inventory", "default_distress_inventory.csv")
        self.load_generic_collection("distress_inventory_strategy", "distress_inventory_strategy.csv")
        self.load_generic_collection("current_rbd", "current_rbd.csv")
        self.load_generic_collection("config_clusters", "config_clusters.csv")
        self.load_generic_collection("series_blocking", "series_blocking.csv")
        self.load_generic_collection("series_booked", "series_booked.csv")
        
        # Load to data warehouse database
        self.load_generic_collection("currency_conversion", "config_conversion_rate.csv")
        
        self.load_generic_collection("connections_details", "connections_details.csv")
        self.load_generic_collection("market_list_international", "Marketlist_international.csv")
        self.load_generic_collection("tbf_discount_grid", "tbf_discount_grid.csv")
        self.load_generic_collection("expected_no_shows", "expected_no_shows.csv")
        self.load_generic_collection("sales_inventory", "sales_inventory.csv")
        self.load_generic_collection("salesinv_rbd_distribution", "salesinv_rbd_distribution.csv")
        self.load_generic_collection("distress_calendar", "distress_calendar.csv")
        self.load_generic_collection("forecast_results", "forecast_results.csv")
        self.load_generic_collection("group_potential_seats", "group_potential_seats.csv")
        self.load_generic_collection("auto_b2b_factor", "auto_b2b_factor.csv")
        self.load_generic_collection("forecast_bands", "forecast_bands.csv")
        self.load_generic_collection("imputed_forecast_bands", "imp_forecast_bands.csv")
        self.load_generic_collection("config_station_master", "config_station_master.csv")
        self.load_generic_collection("market_fare_adjustments", "market_fare_adjustments.csv")
        self.load_generic_collection("city_pair", "city_pair.csv")
        self.load_generic_collection("market_fare_station_map", "market_fare_station_map.csv")
        self.load_generic_collection("active_sectors", "active_sectors.csv")
        
        self.transformCollections()

    def load_generic_collection(self, collectionName, dataFile, use_dw=False):
        """Load a CSV file into a MongoDB Collection"""
        rawdata = pd.read_csv(f"{sys.path[0]}/testcases/input/" + dataFile)
        
        if collectionName in ["market_fares_ag", "market_fares_rg"]:
            # Convert departure_date to full datetime (MongoDB compatible)
            if "departure_date" in rawdata.columns:
                rawdata["departure_date"] = pd.to_datetime(
                    rawdata["departure_date"], errors="coerce", format="%Y-%m-%d"
                )

            # Convert outbound_departuretime (e.g., "5:45") to datetime
            if "outbound_stop" in rawdata.columns:
                def parse_time(t):
                    try:
                        # Pad single-digit hour with 0
                        t_str = str(t).strip()
                        return t_str
                    except:
                        return None

                rawdata["outbound_stop"] = rawdata["outbound_stop"].apply(parse_time)

        # Select the appropriate database
        db = self.dw_db if use_dw else self.db
        
        # Drop the collection if it exists
        self.dropCollection(collectionName, use_dw)
        
        # Convert DataFrame to list of dictionaries
        records = rawdata.to_dict('records')
        
        # Insert documents into MongoDB collection
        if records:
            db[collectionName].insert_many(records)
        
        print(f"Loaded {collectionName}")

    def dropCollection(self, collectionName, use_dw=False):
        """Drop a MongoDB collection if it exists"""
        db = self.dw_db if use_dw else self.db
        db[collectionName].drop()

    def transformCollections(self):
        """
        Transform data types and values in MongoDB collections
        MongoDB is schemaless, but we can transform data during loading
        """
        
        # Transform bookload - convert DepartureDate strings to dates
        self.transform_bookload()
        
        
        # Transform numeric fields in various collections
        # self.transform_numeric_fields()
        
        # Transform time fields
        # self.transform_time_fields()
        
        print("Transformations completed")
    def transform_bookload(self):
        """Transform bookload collection date and flight number fields"""
        try:
            # Get all documents
            docs = list(self.db.bookload.find())
            
            for doc in docs:
                updates = {}
                
                # Convert DepartureDate to datetime (MongoDB doesn't support date-only objects)
                # if 'DepartureDate' in doc and isinstance(doc['DepartureDate'], str):
                #     try:
                #         updates['DepartureDate'] = datetime.strptime(doc['DepartureDate'], '%Y-%m-%d')
                #     except:
                #         try:
                #             updates['DepartureDate'] = datetime.strptime(doc['DepartureDate'], '%m/%d/%Y')
                #         except:
                #             pass
                
                # Convert FlightNumber to string
                # if 'FlightNumber' in doc:
                #     updates['FlightNumber'] = str(doc['FlightNumber'])
                
                # Update document if there are changes
                if updates:
                    self.db.bookload.update_one(
                        {'_id': doc['_id']},
                        {'$set': updates}
                    )
        except Exception as e:
            print(f"Error transforming bookload: {e}")


    def __del__(self):
        # MongoDB connections are typically managed by the client
        # No explicit close needed for database objects
        pass

