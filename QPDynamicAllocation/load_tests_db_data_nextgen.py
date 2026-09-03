import os
import sys
import pandas as pd
from datetime import datetime


class SchemaNextGen:
    """
    Dedicated MongoDB test fixture loader for NextGen Optimization.
    Loads fixtures from testcases/inputnextgen/ (or inputnewzen / inputnewgen).
    Slices and initializes the MongoDB `bookload` collection directly from
    `add_model_fare_bucket_level_weighted.csv` (using Current_Bookings as bookedPax),
    ensuring no dependency on Bookload.csv.
    """

    # Collections that belong to the master db
    MASTER_COLLECTIONS = {
        "config_station_master", "active_sectors",
        "market_fares_ag", "market_fares_rg", "tax_mast_dom", "tax_mast_intl",
        "active_flight_connections", "active_flight_legs", "cleanup_config",
    }

    def __init__(self, mongo_db, master_db=None, input_dir=None):
        self.db = mongo_db
        self.dw_db = mongo_db
        self.master_db = master_db

        base_dir = os.path.dirname(os.path.abspath(__file__))
        if input_dir:
            self.input_dir = input_dir
        else:
            candidates = [
                os.path.join(base_dir, "testcases", "inputnexgen"),
                os.path.join(base_dir, "testcases", "inputnextgen"),
            ]
            self.input_dir = next((c for c in candidates if os.path.exists(c)), candidates[0])

    def initializTables(self):
        # 1. Essential allocation configuration collections
        self.load_generic_collection("curves", "Curves.csv")
        self.load_generic_collection("fares", "Fares.csv")
        self.load_generic_collection("market_fares_rg", "MarketFares.csv")
        self.load_generic_collection("market_fares_ag", "MarketFares.csv")
        self.load_generic_collection("market_list", "MarketList.csv")
        self.load_generic_collection("d1_d2_strategies", "d1d2Strategy.csv")
        self.load_generic_collection("dplf_bands", "dplf_bands.csv")
        self.load_generic_collection("ndo_bands", "ndo_bands.csv")
        self.load_generic_collection("time_ranges", "time_ranges.csv")
        self.load_generic_collection("rm_parameter_values", "rm_parameter_values.csv")
        self.load_generic_collection("config_pfl_threshold", "config_pfl_threshold.csv")
        self.load_generic_collection("config_profile_fares", "config_profile_fares.csv")
        self.load_generic_collection("config_date_event", "config_date_event.csv")
        self.load_generic_collection("config_determine_fares", "config_determine_fares.csv")
        self.load_generic_collection("config_station_master", "config_station_master.csv")
        self.load_generic_collection("active_sectors", "active_sectors.csv")
        self.load_generic_collection("tax_mast_dom", "tax_mast_dom.csv")
        self.load_generic_collection("tax_mast_intl", "tax_mast_intl.csv")

        # 2. NextGen AU Optimization & Competition Response Collections
        self.load_generic_collection("comp_response_plf_bands", "comp_response_plf_bands.csv")
        self.load_generic_collection("comp_response_grid", "comp_response_grid.csv")
        self.load_generic_collection("comp_fare_recalibration", "comp_fare_recalibration.csv")
        self.load_generic_collection("add_model_fare_bucket_level_weighted", "add_model_fare_bucket_level_weighted.csv")

        # 3. Sourcing bookload, capacity, departure time directly from weighted file
        self.load_bookload_from_weighted()

        self.transformCollections()

    def load_bookload_from_weighted(self):
        """
        Creates the MongoDB `bookload` collection directly from
        `add_model_fare_bucket_level_weighted.csv` using `Current_Bookings` as `bookedPax`.
        This completely removes the dependency on Bookload.csv.
        """
        weighted_file = os.path.join(self.input_dir, "add_model_fare_bucket_level_weighted.csv")
        if not os.path.exists(weighted_file):
            print(f"Warning: Weighted forecast file not found: {weighted_file}")
            return

        print(f"Loading bookload collection directly from weighted file: {weighted_file}")
        df = pd.read_csv(weighted_file)
        if df.empty:
            return

        sec_col = 'Sector' if 'Sector' in df.columns else 'sector'
        dep_col = 'DepartureDate' if 'DepartureDate' in df.columns else ('DepartureDate_future' if 'DepartureDate_future' in df.columns else 'date')
        flt_col = 'FlightNumber' if 'FlightNumber' in df.columns else ('FlightNumber_future' if 'FlightNumber_future' in df.columns else 'flight')
        cb_col = next((c for c in ['Current_Bookings', 'Total_Bookings', 'Total Bookings', 'Current Bookings'] if c in df.columns), None)

        # Drop existing bookload collection
        self.dropCollection("bookload")

        # Group by flight to get 1 bookload record per flight
        unique_flights = df.groupby([sec_col, dep_col, flt_col], as_index=False).first()
        docs = []

        for _, row in unique_flights.iterrows():
            sec = str(row[sec_col]).replace('/', '').strip()
            org = sec[:3] if len(sec) >= 3 else sec
            dst = sec[3:] if len(sec) >= 6 else ''
            flt_num = int(row[flt_col]) if pd.notna(row[flt_col]) else 0
            raw_date = row[dep_col]
            dep_date_str = str(pd.to_datetime(raw_date).strftime('%Y-%m-%d')) if pd.notna(raw_date) else ''

            cb_val = float(row[cb_col]) if cb_col and pd.notna(row[cb_col]) else 0.0
            cap_val = float(row.get('Capacity', 189.0)) if pd.notna(row.get('Capacity')) else 189.0
            std_val = str(row.get('STD', '10:00'))[:5]

            docs.append({
                "Origin": org,
                "Destination": dst,
                "FlightNumber": flt_num,
                "DepartureDate": dep_date_str,
                "bookedPax": cb_val,
                "capacity": cap_val,
                "Lid": cap_val,
                "adjustedCapacity": cap_val,
                "DepartureTime": std_val,
                "tbf_bookings": 0.0,
                "group_bookings": 0.0,
                "WA_bookings": 0.0,
                "WB_bookings": 0.0,
                "WC_bookings": 0.0,
                "WD_bookings": 0.0,
                "WE_bookings": 0.0,
            })

        if docs:
            self.db.bookload.insert_many(docs)
            print(f"Successfully loaded {len(docs)} flight bookload records from weighted file into MongoDB.")

    def load_generic_collection(self, collectionName, dataFile, use_dw=False):
        """Load a CSV file into MongoDB from self.input_dir"""
        csv_path = os.path.join(self.input_dir, dataFile)
        if not os.path.exists(csv_path):
            return

        rawdata = pd.read_csv(csv_path)

        if collectionName in ["market_fares_ag", "market_fares_rg"]:
            if "departure_date" in rawdata.columns:
                rawdata["departure_date"] = pd.to_datetime(
                    rawdata["departure_date"], errors="coerce", format="%Y-%m-%d"
                )
            if "outbound_stop" in rawdata.columns:
                rawdata["outbound_stop"] = rawdata["outbound_stop"].apply(
                    lambda t: str(t).strip() if pd.notna(t) else None
                )

        if collectionName == "expected_no_shows":
            if "FlightNumber" in rawdata.columns:
                rawdata["FlightNumber"] = rawdata["FlightNumber"].astype(str)

        db = self.master_db if collectionName in self.MASTER_COLLECTIONS else (self.dw_db if use_dw else self.db)
        self.dropCollection(collectionName, use_dw)
        records = rawdata.to_dict('records')

        import json
        for record in records:
            if 'history' in record and isinstance(record['history'], str):
                try:
                    record['history'] = json.loads(record['history'])
                except Exception:
                    pass

        if records:
            db[collectionName].insert_many(records)

    def dropCollection(self, collectionName, use_dw=False):
        db = self.master_db if collectionName in self.MASTER_COLLECTIONS else (self.dw_db if use_dw else self.db)
        try:
            db[collectionName].drop()
        except:
            pass

    def transformCollections(self):
        try:
            self.db.config_pfl_threshold.update_many(
                {},
                [
                    {"$set": {"plfThreshold": {"$toDouble": "$plfThreshold"}}},
                    {"$set": {"Day": {"$toInt": "$Day"}}},
                    {"$set": {"Hour": {"$toInt": "$Hour"}}},
                ]
            )
        except Exception:
            pass

        try:
            self.db.market_list.update_many(
                {},
                [
                    {"$set": {"perStartFormatted": {"$dateFromString": {"dateString": "$PerStart", "format": "%m/%d/%Y"}}}},
                    {"$set": {"perEndFormatted": {"$dateFromString": {"dateString": "$PerEnd", "format": "%m/%d/%Y"}}}}
                ]
            )
        except Exception:
            pass
