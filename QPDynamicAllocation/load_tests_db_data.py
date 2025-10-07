import sys
import pandas as pd
from datetime import datetime


class Schema:
    def __init__(self, mongo_db):
        # Establish a connection to MongoDB
        self.db = mongo_db
        self.dw_db = mongo_db

    def initializTables(self):
        self.load_generic_collection("Curves", "Curves.csv")
        self.load_generic_collection("Fares", "Fares.csv")
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
        self.load_generic_collection("config_runTime", "config_runTime.csv")
        self.load_generic_collection("no_show_probabilities", "no_show_probabilities.csv")
        self.load_generic_collection("config_ndo_range", "config_ndo_range.csv")
        self.load_generic_collection("config_marketFares_rerouting", "config_marketFares_rerouting.csv")
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
        
        self.transformCollections()

    def load_generic_collection(self, collectionName, dataFile, use_dw=False):
        """Load a CSV file into a MongoDB Collection"""
        rawdata = pd.read_csv(f"{sys.path[0]}/testcases/input/" + dataFile)
        
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
        self.transform_numeric_fields()
        
        # Transform time fields
        self.transform_time_fields()
        
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

    def transform_numeric_fields(self):
        """Transform numeric fields to proper types"""
        
        # d1_d2_strategies: convert to integers
        try:
            docs = list(self.db.d1_d2_strategies.find())
            for doc in docs:
                updates = {}
                if 'ndo_band' in doc:
                    updates['ndo_band'] = int(doc['ndo_band']) if doc['ndo_band'] else None
                if 'dplf_band' in doc:
                    updates['dplf_band'] = int(doc['dplf_band']) if doc['dplf_band'] else None
                if 'offset' in doc:
                    updates['offset'] = int(doc['offset']) if doc['offset'] else None
                
                if updates:
                    self.db.d1_d2_strategies.update_one(
                        {'_id': doc['_id']},
                        {'$set': updates}
                    )
        except Exception as e:
            print(f"Error transforming d1_d2_strategies: {e}")
        
        # dplf_bands: convert to integers
        try:
            docs = list(self.db.dplf_bands.find())
            for doc in docs:
                updates = {}
                if 'dplf_band' in doc:
                    updates['dplf_band'] = int(doc['dplf_band']) if doc['dplf_band'] else None
                if 'start' in doc:
                    updates['start'] = int(doc['start']) if doc['start'] else None
                if 'end' in doc:
                    updates['end'] = int(doc['end']) if doc['end'] else None
                
                if updates:
                    self.db.dplf_bands.update_one(
                        {'_id': doc['_id']},
                        {'$set': updates}
                    )
        except Exception as e:
            print(f"Error transforming dplf_bands: {e}")
        
        # ndo_bands: convert to integers
        try:
            docs = list(self.db.ndo_bands.find())
            for doc in docs:
                updates = {}
                if 'ndo_band' in doc:
                    updates['ndo_band'] = int(doc['ndo_band']) if doc['ndo_band'] else None
                if 'start' in doc:
                    updates['start'] = int(doc['start']) if doc['start'] else None
                if 'end' in doc:
                    updates['end'] = int(doc['end']) if doc['end'] else None
                
                if updates:
                    self.db.ndo_bands.update_one(
                        {'_id': doc['_id']},
                        {'$set': updates}
                    )
        except Exception as e:
            print(f"Error transforming ndo_bands: {e}")

    def transform_time_fields(self):
        """Transform time fields in config_ownFares"""
        try:
            docs = list(self.db.config_ownFares.find())
            for doc in docs:
                updates = {}
                
                # Convert startTime and endTime to proper time format
                if 'startTime' in doc and isinstance(doc['startTime'], str):
                    try:
                        time_obj = datetime.strptime(doc['startTime'], '%H:%M:%S').time()
                        updates['startTime'] = time_obj
                    except:
                        pass
                
                if 'endTime' in doc and isinstance(doc['endTime'], str):
                    try:
                        time_obj = datetime.strptime(doc['endTime'], '%H:%M:%S').time()
                        updates['endTime'] = time_obj
                    except:
                        pass
                
                # Convert numeric columns (0-6) to integers
                for i in range(7):
                    col = str(i)
                    if col in doc:
                        try:
                            updates[col] = int(doc[col]) if doc[col] else None
                        except:
                            pass
                
                if updates:
                    self.db.config_ownFares.update_one(
                        {'_id': doc['_id']},
                        {'$set': updates}
                    )
        except Exception as e:
            print(f"Error transforming config_ownFares: {e}")

    def __del__(self):
        # MongoDB connections are typically managed by the client
        # No explicit close needed for database objects
        pass

