from datetime import datetime, timedelta
import json
import pandas as pd
import pytz
from ownlogger import OwnLogger
from Constants import *
from utilities.time_utils import TimeUtils 
from helper.redis_helper import RedisHelper
from constants_utils import ConstantsUtils

class RMDatabaseHelper:
    def __init__(self, mongo_db, dwconn, runId, inputSource, debugmode, cache_client, currParams):
        self.db = mongo_db  # MongoDB database instance
        self.master_db = getattr(currParams, 'mongo_master_db', mongo_db) if hasattr(currParams, 'mongo_master_db') else mongo_db
        self.dwconn = dwconn  # Data warehouse MongoDB instance
        self.runId = runId
        self.logger = OwnLogger(runId, inputSource)
        self.currParams = currParams
        self.inputSource = inputSource
        self.debugMode = debugmode
        self.timeutils = TimeUtils(inputSource)
        self.redishelper = RedisHelper(self.runId, cache_client, inputSource)
        self.constants_utils = ConstantsUtils(mongo_db)

    def getLatestCodeVersion(self):
        try:
            result = self.db.config_code_version.find_one({"isActive": True})
            return result['code_version'] if result else None
        except Exception as e:
            self.logger.error(e)
            return None

    def loadRunParams(self):
        if self.inputSource.input_source[CONST_MARKET_LIST] == FILE:
            if self.currParams.route_type == "direct":
                self.mktList = pd.read_csv(CONST_MARKET_LIST_FILE_PATH)
            else:
                self.mktList = pd.read_csv(CONST_MARKET_LIST_CONNECTIONS_FILE_PATH)
        else:
            if self.currParams.route_type == "direct":
                try:
                    # Aggregate pipeline for market_list with joins
                    pipeline = [
                        # First, convert the date format from MM/DD/YYYY to YYYY-MM-DD
                        {
                            "$addFields": {
                                "perStartFormatted": {
                                    "$concat": [
                                        {"$substr": ["$PerStart", 6, 4]},  # Year (2023)
                                        "-",
                                        {"$substr": ["$PerStart", 0, 2]},  # Month (08)
                                        "-",
                                        {"$substr": ["$PerStart", 3, 2]}   # Day (01)
                                    ]
                                }
                            }
                        },
                        # Lookup booking_fare_data
                        {
                            "$lookup": {
                                "from": "booking_fare_data",
                                "let": {
                                    "org": "$Origin",
                                    "dst": "$Destin",
                                    "flt": "$FlightNumber",
                                    "date": "$perStartFormatted"
                                },
                                "pipeline": [
                                    {
                                        "$match": {
                                            "$expr": {
                                                "$and": [
                                                    {"$eq": ["$Origin", "$$org"]},
                                                    {"$eq": ["$Destin", "$$dst"]},
                                                    {"$eq": ["$FlightNumber", "$$flt"]},
                                                    {"$eq": ["$FlightDate", "$$date"]}
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "as": "bfd"
                            }
                        },
                        {"$unwind": {"path": "$bfd", "preserveNullAndEmptyArrays": True}},
                        # Lookup Bookload
                        {
                            "$lookup": {
                                "from": "bookload",
                                "let": {
                                    "org": "$Origin",
                                    "dst": "$Destin",
                                    "flt": "$FlightNumber",
                                    "date": "$perStartFormatted"
                                },
                                "pipeline": [
                                    {
                                        "$match": {
                                            "$expr": {
                                                "$and": [
                                                    {"$eq": ["$Origin", "$$org"]},
                                                    {"$eq": ["$Destination", "$$dst"]},
                                                    {"$eq": ["$FlightNumber", "$$flt"]},
                                                    {"$eq": ["$DepartureDate", "$$date"]}
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "as": "bl"
                            }
                        },
                        {"$unwind": {"path": "$bl", "preserveNullAndEmptyArrays": True}},
                        # Lookup current_rbd with aggregation
                        {
                            "$lookup": {
                                "from": "current_rbd",
                                "let": {
                                    "org": "$Origin",
                                    "dst": "$Destin",
                                    "flt": "$FlightNumber",
                                    "date": "$perStartFormatted"
                                },
                                "pipeline": [
                                    {
                                        "$match": {
                                            "$expr": {
                                                "$and": [
                                                    {"$eq": ["$Origin", "$$org"]},
                                                    {"$eq": ["$Destin", "$$dst"]},
                                                    {"$eq": ["$FlightNumber", "$$flt"]},
                                                    {"$eq": ["$DepartureDate", "$$date"]}
                                                ]
                                            }
                                        }
                                    },
                                    {
                                        "$group": {
                                            "_id": None,
                                            "seats_sold_L": {
                                                "$sum": {
                                                    "$cond": [
                                                        {"$eq": ["$ClassOfService", "L"]},
                                                        "$ClassSold",
                                                        0
                                                    ]
                                                }
                                            },
                                            "seats_sold_P": {
                                                "$sum": {
                                                    "$cond": [
                                                        {"$eq": ["$ClassOfService", "P"]},
                                                        "$ClassSold",
                                                        0
                                                    ]
                                                }
                                            },
                                            "current_rbd_L": {
                                                "$sum": {
                                                    "$cond": [
                                                        {"$eq": ["$ClassOfService", "L"]},
                                                        "$ClassAu",
                                                        0
                                                    ]
                                                }
                                            },
                                            "current_rbd_P": {
                                                "$sum": {
                                                    "$cond": [
                                                        {"$eq": ["$ClassOfService", "P"]},
                                                        "$ClassAu",
                                                        0
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                ],
                                "as": "cr"
                            }
                        },
                        {"$unwind": {"path": "$cr", "preserveNullAndEmptyArrays": True}},
                        # Lookup forecast_results
                        {
                            "$lookup": {
                                "from": "forecast_results",
                                "let": {
                                    "org": "$Origin",
                                    "dst": "$Destin",
                                    "flt": "$FlightNumber",
                                    "date": "$perStartFormatted"
                                },
                                "pipeline": [
                                    {
                                        "$match": {
                                            "$expr": {
                                                "$and": [
                                                    {"$eq": ["$Origin", "$$org"]},
                                                    {"$eq": ["$Destination", "$$dst"]},
                                                    {"$eq": ["$FlightNumber", "$$flt"]},
                                                    {"$eq": ["$DepartureDate", "$$date"]}
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "as": "fr"
                            }
                        },
                        {"$unwind": {"path": "$fr", "preserveNullAndEmptyArrays": True}},
                        # Lookup series_booked with aggregation
                        {
                            "$lookup": {
                                "from": "series_booked",
                                "let": {
                                    "org": "$Origin",
                                    "dst": "$Destin",
                                    "flt": "$FlightNumber",
                                    "date": "$perStartFormatted"
                                },
                                "pipeline": [
                                    {
                                        "$match": {
                                            "$expr": {
                                                "$and": [
                                                    {"$eq": ["$Origin", "$$org"]},
                                                    {"$eq": ["$Destination", "$$dst"]},
                                                    {"$eq": ["$FlightNumber", "$$flt"]},
                                                    {"$eq": ["$DepartureDate", "$$date"]}
                                                ]
                                            }
                                        }
                                    },
                                    {
                                        "$group": {
                                            "_id": None,
                                            "total_series_seats": {"$sum": "$bookedPax"}
                                        }
                                    }
                                ],
                                "as": "sb"
                            }
                        },
                        {"$unwind": {"path": "$sb", "preserveNullAndEmptyArrays": True}},
                        # Add fields from joined collections with proper null handling
                        {
                            "$addFields": {
    "BookingsCount": "$bfd.BookingsCount",
    "BookingToday": "$bfd.BookingToday",
    "BookingsInHour": "$bfd.BookingsInHour",
    "B2B_last3avg_fare": "$bfd.B2B_last3avg_fare",
    "B2C_last3avg_fare": "$bfd.B2C_last3avg_fare",
    "last7avg": "$bfd.last7avg",
    "STD": "$bfd.STD",
    "bookedPax": "$bl.bookedPax",
    "DepartureTime": "$bl.DepartureTime",
    "capacity": "$bl.Lid",
    "adjustedCapacity": "$bl.adjustedCapacity",
    "tbf_bookings": "$bl.tbf_bookings",
    "group_bookings": "$bl.group_bookings",
    "WA_bookings": "$bl.WA_bookings",
    "WB_bookings": "$bl.WB_bookings",
    "WC_bookings": "$bl.WC_bookings",
    "WD_bookings": "$bl.WD_bookings",
    "WE_bookings": "$bl.WE_bookings",
    "forecastPLF": "$fr.ForecastPLF",
    "forecast_variance": "$fr.variance",
    "ALLIN_AF": "$fr.ALLIN_AF",
    "ForecastAF_Allin": "$fr.ForecastAF_Allin",
    "ALLIN_AF_NB_LM": "$fr.ALLIN_AF_NB_LM",
    "ALLIN_AF_NB_LY": "$fr.ALLIN_AF_NB_LY",
    "Adjustment_Factor": "$fr.Adjustment_Factor",
    "Expected_PLF": "$fr.Expected_PLF",
    "Flown_Target_Load": "$fr.Flown_Target_Load",
    "Imputed_Forecast_6hrs": "$fr.Imputed_Forecast_6hrs",
    "forecast_run_plf": "$fr.forecast_run_plf",
    "seats_sold_L": {"$ifNull": ["$cr.seats_sold_L", 0]},
"seats_sold_P": {"$ifNull": ["$cr.seats_sold_P", 0]},
"current_rbd_L": {"$ifNull": ["$cr.current_rbd_L", 0]},
"current_rbd_P": {"$ifNull": ["$cr.current_rbd_P", 0]},
"total_series_seats": {"$ifNull": ["$sb.total_series_seats", 0]},
    "NextFlight": 0
}
                        },
                        # Remove the temporary fields and nested objects
                        {
                            "$project": {
                                "perStartFormatted": 0,
                                "bfd": 0,
                                "bl": 0,
                                "cr": 0,
                                "fr": 0,
                                "sb": 0
                            }
                        }
                    ]
                    
                    self.logger.info("Executing direct route aggregation pipeline")
                    results = list(self.db.market_list.aggregate(pipeline))
                    self.logger.info(f"Retrieved {len(results)} records from aggregation")
                    
                    if len(results) == 0:
                        self.logger.warning("No results returned from aggregation pipeline")
                    
                    self.mktList = pd.DataFrame(results)
                    
                    # Enrich with active_sectors data from master db (cross-db join)
                    if not self.mktList.empty and 'Origin' in self.mktList.columns and 'Destin' in self.mktList.columns:
                        try:
                            sectors = (self.mktList['Origin'] + self.mktList['Destin']).unique().tolist()
                            act_sec_data = list(self.master_db.active_sectors.find(
                                {"Sector": {"$in": sectors}},
                                {"Sector": 1, "Forecast_model": 1, "Farecap": 1, "_id": 0}
                            ))
                            if act_sec_data:
                                act_sec_df = pd.DataFrame(act_sec_data)
                                self.mktList['_sector_key'] = self.mktList['Origin'] + self.mktList['Destin']
                                self.mktList = self.mktList.merge(
                                    act_sec_df.rename(columns={"Sector": "_sector_key", "Forecast_model": "forecast_model", "Farecap": "farecap"}),
                                    on='_sector_key', how='left'
                                )
                                self.mktList.drop(columns=['_sector_key'], inplace=True)
                            else:
                                self.mktList['forecast_model'] = None
                                self.mktList['farecap'] = None
                        except Exception as e:
                            self.logger.warning(f"Error enriching with active_sectors: {e}")
                            self.mktList['forecast_model'] = None
                            self.mktList['farecap'] = None
                    
                except Exception as e:
                    self.logger.error(f"Error in direct route aggregation: {str(e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    self.mktList = pd.DataFrame()
            else:
                # Connections query
                try:
                    pipeline = [
    {
        "$addFields": {
            # Convert PerStart from MM/DD/YYYY to YYYY-MM-DD string format to match other collections
            "perStartDateStr": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {
                        "$dateFromString": {
                            "dateString": "$PerStart",
                            "format": "%m/%d/%Y"
                        }
                    }
                }
            },
            # Extract origin and destination from Sector1 and Sector2
            "sector1Origin": {"$substr": ["$Sector1", 0, 3]},
            "sector1Dest": {"$substr": ["$Sector1", 3, 3]},
            "sector2Origin": {"$substr": ["$Sector2", 0, 3]},
            "sector2Dest": {"$substr": ["$Sector2", 3, 3]},
            # Convert Flight numbers to integers for matching
            "flight1Int": {"$toInt": "$Flight1"},
            "flight2Int": {"$toInt": "$Flight2"}
        }
    },
    {
        "$lookup": {
            "from": "connections_details",
            "let": {
                "s1Origin": "$sector1Origin",
                "s1Dest": "$sector1Dest",
                "flight1": "$flight1Int",
                "perStart": "$perStartDateStr",
                "s2Origin": "$sector2Origin",
                "s2Dest": "$sector2Dest",
                "flight2": "$flight2Int"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin1", "$$s1Origin"]},
                                {"$eq": ["$Destnination1", "$$s1Dest"]},
                                {"$eq": ["$FlightNo1", "$$flight1"]},
                                {"$eq": ["$Departuredate1", "$$perStart"]},
                                {"$eq": ["$Origin2", "$$s2Origin"]},
                                {"$eq": ["$Destnination2", "$$s2Dest"]},
                                {"$eq": ["$FlightNo2", "$$flight2"]}
                            ]
                        }
                    }
                }
            ],
            "as": "connections_details"
        }
    },
    {
        "$unwind": {
            "path": "$connections_details",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "bookload",
            "let": {
                "origin": "$sector1Origin",
                "dest": "$sector1Dest",
                "flight": "$flight1Int",
                "depDate": "$perStartDateStr"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin", "$$origin"]},
                                {"$eq": ["$Destination", "$$dest"]},
                                {"$eq": ["$FlightNumber", "$$flight"]},
                                {"$eq": ["$DepartureDate", "$$depDate"]}
                            ]
                        }
                    }
                }
            ],
            "as": "bookload1"
        }
    },
    {
        "$lookup": {
            "from": "forecast_results",
            "let": {
                "origin": "$sector1Origin",
                "dest": "$sector1Dest",
                "flight": "$flight1Int",
                "depDate": "$perStartDateStr"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin", "$$origin"]},
                                {"$eq": ["$Destination", "$$dest"]},
                                {"$eq": ["$FlightNumber", "$$flight"]},
                                {"$eq": ["$DepartureDate", "$$depDate"]}
                            ]
                        }
                    }
                }
            ],
            "as": "forecast1"
        }
    },
    {
        "$lookup": {
            "from": "forecast_results",
            "let": {
                "origin": "$sector2Origin",
                "dest": "$sector2Dest",
                "flight": "$flight2Int",
                "depDate": "$connections_details.Departuredate2"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin", "$$origin"]},
                                {"$eq": ["$Destination", "$$dest"]},
                                {"$eq": ["$FlightNumber", "$$flight"]},
                                {"$eq": ["$DepartureDate", "$$depDate"]}
                            ]
                        }
                    }
                }
            ],
            "as": "forecast2"
        }
    },
    {
        "$lookup": {
            "from": "bookload",
            "let": {
                "origin": "$sector2Origin",
                "dest": "$sector2Dest",
                "flight": "$flight2Int",
                "depDate": "$connections_details.Departuredate2"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin", "$$origin"]},
                                {"$eq": ["$Destination", "$$dest"]},
                                {"$eq": ["$FlightNumber", "$$flight"]},
                                {"$eq": ["$DepartureDate", "$$depDate"]}
                            ]
                        }
                    }
                }
            ],
            "as": "bookload2"
        }
    },
    {
        "$lookup": {
            "from": "current_opening_rbd",
            "let": {
                "origin": "$sector1Origin",
                "dest": "$sector1Dest",
                "flight": "$flight1Int",
                "depDate": "$perStartDateStr"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin", "$$origin"]},
                                {"$eq": ["$Destination", "$$dest"]},
                                {"$eq": ["$flightno", "$$flight"]},
                                {"$eq": ["$DepartureDate", "$$depDate"]},
                                {"$eq": ["$Channel", "B2B"]}
                            ]
                        }
                    }
                }
            ],
            "as": "crb1_b2b"
        }
    },
    {
        "$lookup": {
            "from": "current_opening_rbd",
            "let": {
                "origin": "$sector1Origin",
                "dest": "$sector1Dest",
                "flight": "$flight1Int",
                "depDate": "$perStartDateStr"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin", "$$origin"]},
                                {"$eq": ["$Destination", "$$dest"]},
                                {"$eq": ["$flightno", "$$flight"]},
                                {"$eq": ["$DepartureDate", "$$depDate"]},
                                {"$eq": ["$Channel", "B2C"]}
                            ]
                        }
                    }
                }
            ],
            "as": "crb1_b2c"
        }
    },
    {
        "$lookup": {
            "from": "current_opening_rbd",
            "let": {
                "origin": "$sector2Origin",
                "dest": "$sector2Dest",
                "flight": "$flight2Int",
                "depDate": "$connections_details.Departuredate2"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin", "$$origin"]},
                                {"$eq": ["$Destination", "$$dest"]},
                                {"$eq": ["$flightno", "$$flight"]},
                                {"$eq": ["$DepartureDate", "$$depDate"]},
                                {"$eq": ["$Channel", "B2B"]}
                            ]
                        }
                    }
                }
            ],
            "as": "crb2_b2b"
        }
    },
    {
        "$lookup": {
            "from": "current_opening_rbd",
            "let": {
                "origin": "$sector2Origin",
                "dest": "$sector2Dest",
                "flight": "$flight2Int",
                "depDate": "$connections_details.Departuredate2"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$Origin", "$$origin"]},
                                {"$eq": ["$Destination", "$$dest"]},
                                {"$eq": ["$flightno", "$$flight"]},
                                {"$eq": ["$DepartureDate", "$$depDate"]},
                                {"$eq": ["$Channel", "B2C"]}
                            ]
                        }
                    }
                }
            ],
            "as": "crb2_b2c"
        }
    },
    {
        "$replaceRoot": {
            "newRoot": {
                "$mergeObjects": [
                    "$$ROOT",
                    {
                        # bookload 1
                        "BookedPax1": {"$arrayElemAt": ["$bookload1.bookedPax", 0]},
                        "Lid1": {"$arrayElemAt": ["$bookload1.Lid", 0]},
                        
                        # Forecast 1
                        "forecastPLF1": {"$arrayElemAt": ["$forecast1.ForecastPLF", 0]},
                        "forecast_variance1": {"$arrayElemAt": ["$forecast1.variance", 0]},
                        
                        # bookload 2
                        "BookedPax2": {"$arrayElemAt": ["$bookload2.bookedPax", 0]},
                        "Lid2": {"$arrayElemAt": ["$bookload2.Lid", 0]},
                        
                        # Forecast 2
                        "forecastPLF2": {"$arrayElemAt": ["$forecast2.ForecastPLF", 0]},
                        "forecast_variance2": {"$arrayElemAt": ["$forecast2.variance", 0]},
                        
                        # Current Opening RBD
                        "OpeningRBDB2B1": {"$arrayElemAt": ["$crb1_b2b.OpeningRBD", 0]},
                        "OpeningRBDB2C1": {"$arrayElemAt": ["$crb1_b2c.OpeningRBD", 0]},
                        "OpeningRBDB2B2": {"$arrayElemAt": ["$crb2_b2b.OpeningRBD", 0]},
                        "OpeningRBDB2C2": {"$arrayElemAt": ["$crb2_b2c.OpeningRBD", 0]},
                        
                        # Second Flight Departure Date
                        "SecondFlightDepartureDate": "$connections_details.Departuredate2",
                        
                        # Direct Flight Data for Scenario 4 (Connection OD)
                        "DirectFlightNumber": "$connections_details.DirectFlightNumber",
                        "DirectFlightBookedPax": "$connections_details.DirectFlightBookedPax",
                        "DirectFlightLid": "$connections_details.DirectFlightLid",
                        "DirectFlightForecast": "$connections_details.DirectFlightForecast",
                        "DirectFlightPLF": "$connections_details.DirectFlightPLF",
                        "DirectFlightDepTime": "$connections_details.DirectFlightDepTime",
                        "DirectFlightOpeningRBDB2B": "$connections_details.DirectFlightOpeningRBDB2B",
                        "DirectFlightOpeningRBDB2C": "$connections_details.DirectFlightOpeningRBDB2C",
                        
                        # Map SOSFlag to sosFlag
                        "sosFlag": "$SOSFlag"
                    }
                ]
            }
        }
    },
    {
        "$project": {
            # Remove the temporary fields and lookup arrays
            "perStartDateStr": 0,
            "sector1Origin": 0,
            "sector1Dest": 0,
            "sector2Origin": 0,
            "sector2Dest": 0,
            "flight1Int": 0,
            "flight2Int": 0,
            "bookload1": 0,
            "bookload2": 0,
            "forecast1": 0,
            "forecast2": 0,
            "crb1_b2b": 0,
            "crb1_b2c": 0,
            "crb2_b2b": 0,
            "crb2_b2c": 0,
            "connections_details": 0
        }
    }
]
                    
                    self.logger.info("Executing connections route aggregation pipeline")
                    results = list(self.db.market_list_connections.aggregate(pipeline))
                    self.logger.info(f"Retrieved {len(results)} records from aggregation")
                    
                    if len(results) == 0:
                        self.logger.warning("No results returned from connections aggregation pipeline")
                    
                    self.mktList = pd.DataFrame(results)
                    
                    # Enrich with active_sectors data from master db (cross-db join)
                    if not self.mktList.empty and 'Sector1' in self.mktList.columns and 'Sector2' in self.mktList.columns:
                        try:
                            sectors = (self.mktList['Sector1'].str[:3] + self.mktList['Sector2'].str[3:]).unique().tolist()
                            act_sec_data = list(self.master_db.active_sectors.find(
                                {"Sector": {"$in": sectors}},
                                {"Sector": 1, "Forecast_model": 1, "Farecap": 1, "_id": 0}
                            ))
                            if act_sec_data:
                                act_sec_df = pd.DataFrame(act_sec_data)
                                self.mktList['_sector_key'] = self.mktList['Sector1'].str[:3] + self.mktList['Sector2'].str[3:]
                                self.mktList = self.mktList.merge(
                                    act_sec_df.rename(columns={"Sector": "_sector_key", "Forecast_model": "forecast_model", "Farecap": "farecap"}),
                                    on='_sector_key', how='left'
                                )
                                self.mktList.drop(columns=['_sector_key'], inplace=True)
                            else:
                                self.mktList['forecast_model'] = None
                                self.mktList['farecap'] = None
                        except Exception as e:
                            self.logger.warning(f"Error enriching connections with active_sectors: {e}")
                            self.mktList['forecast_model'] = None
                            self.mktList['farecap'] = None
                    
                except Exception as e:
                    self.logger.error(f"Error in connections route aggregation: {str(e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    self.mktList = pd.DataFrame()

        if self.debugMode:
            self.logger.info("Loaded Market List")
            self.logger.info(f"Market List shape: {self.mktList.shape}")
            if not self.mktList.empty:
                self.logger.info(f"Market List columns: {self.mktList.columns.tolist()}")
                self.logger.info(f"First few rows:\n{self.mktList.head()}")
            else:
                self.logger.warning("Market List is empty!")
        
        return self.mktList

    def insertRunStart(self, startTime, username):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            try:
                curr_version_docs = list(self.db.currentversion.find())
                table_versions = {doc['tableName']: doc['curr_version'] for doc in curr_version_docs}

                type_val = "Local Run"

                self.db.allocation_run_audit.insert_one({
                    "run_id": self.runId,
                    "start_time": startTime.strftime(DATE_TIME_FORMAT),
                    "user": username,
                    "dtd_start": 0,
                    "dtd_end": 365,
                    "Curves": table_versions.get('Curves'),
                    "d1_d2_strategies": table_versions.get('d1_d2_strategies'),
                    "market_list": type_val,
                    "Fares": table_versions.get('Fares'),
                    "type": type_val,
                    "is_sftp_pushed": '0',
                    "is_connections_required": '0',
                    "update_navitaire_method": 'file'
                })
            except Exception as e:
                self.logger.error(e)

    def updateRunEnd(self, endTime):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            try:
                self.db.allocation_run_audit.update_one(
                    {"run_id": self.runId},
                    {"$set": {"end_time": endTime.strftime(DATE_TIME_FORMAT)}}
                )
            except Exception as e:
                self.logger.error(e)

    def areAllInputsReady(self):
        if self.inputSource.input_source[CONST_INPUT_STATUS] == DB:
            try:
                count = self.db.inputs_status.count_documents({
                    "is_running": True,
                    "name": {"$not": {"$regex": "^market_list"}}
                })
                return count == 0
            except Exception as e:
                self.logger.error(e)
                return False

    def format_date_to_mm_dd_yyyy(self, date_val):
        try:
            if date_val is None or pd.isna(date_val):
                return date_val
            date_str = str(date_val)
            if len(date_str) == 10 and date_str[2] == '/' and date_str[5] == '/':
                return date_str
            return pd.to_datetime(date_str).strftime('%m/%d/%Y')
        except Exception:
            return date_val

    def insertSummaryRow(self, allocResults):
        if self.inputSource.logs[CONST_RUN_SUMMARY] == DB:
            latest_row = allocResults[-1]

            doc = {
                "Id": latest_row.Id,
                "Origin": latest_row.Origin,
                "Destin": latest_row.Destin,
                "FltNum": latest_row.FltNum,
                "DepDate": self.format_date_to_mm_dd_yyyy(latest_row.DepDate),
                "BookedLoad": latest_row.BookedLoad,
                "TgtLoad": latest_row.TgtLoad,
                "MktFare_Min": str(latest_row.MktFare_Min),
                "CurveId": str(latest_row.CurveId),
                "ExpectedNoShows": str(latest_row.ExpectedNoShows),
                "OpenRBD": latest_row.OpenRBD,
                "SellingFare": str(latest_row.SellingFare),
                "Channel": latest_row.Channel,
                "HowDetermined": latest_row.HowDetermined,
                "RunId": latest_row.RunId,
                "CreatedAt": self.timeutils.getCurrentDateTime().strftime(DATE_TIME_FORMAT),
                "MktFare_Max": str(latest_row.MktFare_Max),
                "fareAnchor": str(latest_row.anchorFare),
                "fareOffset": str(latest_row.fareOffset),
                "TimeRange": str(latest_row.TimeRange),
                "dlfBand": str(latest_row.dlfBand),
                "ndoBand": str(latest_row.ndoBand),
                "criteria": str(latest_row.criteria),
                "AirlineCode_Max": str(latest_row.AirlineCode_Max),
                "AirlineCode_Min": str(latest_row.AirlineCode_Min),
                "FlightNumber_Max": str(latest_row.FlightNumber_Max),
                "FlightNumber_Min": str(latest_row.FlightNumber_Min),
                "backstop": str(latest_row.Backstop),
                "autoBackstop": str(latest_row.autoBackstop),
                "Variance": str(latest_row.Variance),
                "StrategyReference": str(latest_row.StrategyReference),
                "analystName": str(latest_row.analystName),
                "bookedPlf": str(latest_row.bookedPlf),
                "actualbookedPlf": str(latest_row.actualbookedPlf),
                "startTime": str(latest_row.startTime),
                "endTime": str(latest_row.endTime),
                "allocationStatus": str(latest_row.allocationStatus),
                "endOfDaydlfBand": str(latest_row.endOfDaydlfBand),
                "endOfDayTgtLoad": str(latest_row.endOfDayTgtLoad),
                "strategyFare": str(latest_row.strategyFare),
                "lastSellingFare": str(latest_row.lastSellingFare),
                "lastSevenAvgFare": str(latest_row.lastSevenAvgFare),
                "hardAnchorFare": str(latest_row.hardAnchorFare),
                "upsell": str(latest_row.upsell),
                "bookingToday": str(latest_row.bookingToday),
                "bookingInHour": str(latest_row.bookingLastHour),
                "currentTgtLoad": str(latest_row.currentTgtLoad),
                "overBookingCount": str(latest_row.overBookingCount),
                "profileFare": str(latest_row.profileFare),
                "status": str(latest_row.status),
                "w0Flag": str(latest_row.w0Flag),
                "mktFile": str(latest_row.mktFile),
                "ForecastAF_Allin": str(latest_row.ForecastAF_Allin),
                "AF_Allin": str(latest_row.AF_Allin),
                "AF_NB_Allin": str(latest_row.AF_NB_Allin),
                "ALLIN_AF_NB_LM": str(latest_row.ALLIN_AF_NB_LM),
                "ALLIN_AF_NB_LY": str(latest_row.ALLIN_AF_NB_LY),
                "FirstRBDAlloc": str(latest_row.FirstRBDAlloc),
                "OtherRBDAlloc": str(latest_row.OtherRBDAlloc),
                "forecastPLF": str(latest_row.forecastPLF),
                "ImputedForecastPLF": str(latest_row.ImputedForecastPLF),
                "B2BFactor": str(latest_row.B2BFactor),
                "toleranceValue": str(latest_row.toleranceValue),
                "anchorFareBeforeTolerance": str(latest_row.anchorFareBeforeTolerance),
                "grpAdjProfileFare": str(latest_row.grpAdjProfileFare),
                "grpAdjPLF": str(latest_row.grpAdjPLF),
                "grpFareAnchor": str(latest_row.grpFareAnchor),
                # Portfolio anchor split (R9) — direct-route rows get real
                # values; connections / sales-inventory rows leave them as
                # ``CONST_NA``.
                "z_score_composite": str(latest_row.z_score_composite),
                "Portfolio_Anchor": str(latest_row.Portfolio_Anchor),
                "z_score88": str(latest_row.z_score88),
                "Portfolio88_Anchor": str(latest_row.Portfolio88_Anchor),
                "portfolio_count": str(latest_row.portfolio_count),
                "conversionRate": str(latest_row.conversionRate),
                "currency": str(latest_row.currency)
            }

            try:
                self.db.run_summary.insert_one(doc)
            except Exception as e:
                self.logger.error(e)

    def getTargetBookedLoadFromBenchmarkCurve(self, ndocounter, curveID):
        if ndocounter < 0:
            ndocounter = 0
        try:
            result = self.db.curves.find_one({"NDO": ndocounter, "CurveID": curveID})
            if result:
                return pd.DataFrame([{"LF": result.get("LF")}])
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getNdoBand(self, ndo):
        try:
            result = self.db.ndo_bands.find_one({"start": {"$lte": ndo}, "end": {"$gt": ndo}})
            return result['ndo_band'] if result else None
        except Exception as e:
            self.logger.error(e)
            return None

    def getdpflBand(self, dPLF):
        try:
            result = self.db.dplf_bands.find_one({"start": {"$lte": dPLF}, "end": {"$gt": dPLF}})
            return result['dplf_band'] if result else None
        except Exception as e:
            self.logger.error(e)
            return None

    def get_international_bookload_threshold(self, ndo):
        """
        Get the International Bookload Threshold (PLF) for a given NDO.
        Uses the NDO Bands defined in ConstantsUtils.
        """
        try:
            for (start, end), threshold in self.constants_utils.INTERNATIONAL_BOOKLOAD_THRESHOLDS.items():
                if start <= ndo <= end:
                    return threshold
            # Default to 0 if no band matches (should cover all cases if 365 is max)
            return 0
        except Exception as e:
            self.logger.error(f"Error getting international bookload threshold for NDO {ndo}: {e}")
            return 0

    def getForecastBand(self, forecast):
        try:
            result = self.db.forecast_bands.find_one({"start": {"$lte": int(forecast)}, "end": {"$gt": int(forecast)}})
            return result['forecast_band'] if result else CONST_NA
        except Exception as e:
            self.logger.error(e)
            return CONST_NA

    def get_imputed_forecast_band(self, imp_forecast):
        rounded_imp_fcst = round(imp_forecast, 2)
        try:
            result = self.db.imputed_forecast_bands.find_one(
                {"start": {"$lte": rounded_imp_fcst}, "end": {"$gte": rounded_imp_fcst}}
            )
            return int(result['band']) if result else CONST_NA
        except Exception as e:
            self.logger.error(e)
            return CONST_NA
    
    def get_af_forecast_band(self, af_forecast):
        rounded_af_fcst = round(af_forecast, 2)
        try:
            result = self.db.imputed_forecast_bands.find_one(
                {"start_af": {"$lte": rounded_af_fcst}, "end_af": {"$gte": rounded_af_fcst}}
            )
            return int(result['band']) if result else CONST_NA
        except Exception as e:
            self.logger.error(e)
            return CONST_NA

    def getPlfThreshhold(self, ndocounter, flightNumber):
        rbds = json.loads(self.constants_utils.SOFTBLOCK_RBDS)
        
        try:
            # Try to find specific flight/NDO combination
            result = self.db.config_pfl_threshold.find_one({
                "flightNumber": int(flightNumber),
                "NDO": int(ndocounter)
            })
            
            # If not found, try wildcard
            if not result:
                result = self.db.config_pfl_threshold.find_one({
                    "flightNumber": "*",
                    "NDO": int(ndocounter)
                })
            
            if result:
                return pd.DataFrame([result])
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getDepartureTimeBands(self):
        try:
            results = list(self.db.departure_time_ranges.find())
            time_bands = [(self.timeutils.parse_time(r['StartTime']), 
                          self.timeutils.parse_time(r['EndTime']), 
                          r['TimeBand']) for r in results]
            
            # Get start/end range
            first = self.db.departure_time_ranges.find_one(sort=[("TimeBand", 1)])
            second = self.db.departure_time_ranges.find_one(sort=[("TimeBand", 1)], skip=1)
            
            start_end_range = [(first['EndTime'], second['StartTime'])] if first and second else []
            
            return time_bands, start_end_range
        except Exception as e:
            self.logger.error(e)
            return [], []

    def getd1d2StrategyValue(self, strategy, dplfBand, ndoBand, forecastBand, channel):
        try:
            results1 = list(self.db.d1_d2_strategies.find({
                "ndo_band": ndoBand,
                "dplf_band": dplfBand,
                "strategy": strategy,
                "channel": channel
            }))
            results2 = list(self.db.forecast_strategy.find({
                "ndo_band": ndoBand,
                "forecast_band": forecastBand,
                "strategy": strategy,
                "channel": channel
            }))
            return pd.DataFrame(results1 + results2)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getB2BStrategyValue(self, ndoBand, forecastBand, name):
        try:
            result = self.db.auto_b2b_factor.find_one({
                "ndo_band": ndoBand,
                "forecast_band": forecastBand,
                "name": name
            })
            return result["b2b_factor"] if result else None
        except Exception as e:
            self.logger.error(e)
            return None

    def getTimeRange(self, time_range):
        try:
            results = list(self.db.time_ranges.find({"time_range": time_range}))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getTimeReferenceValue(self, currParams):
        try:
            results = list(self.db.allocation_acceptable_range_d1.find({"airports": currParams.origin}))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def _convert_gross_fare_to_int(self, df):
        """Helper method to safely convert gross_fare column to int"""
        if 'gross_fare' in df.columns and not df.empty:
            try:
                # Convert to numeric, replacing any non-numeric values with NaN
                df['gross_fare'] = pd.to_numeric(df['gross_fare'], errors='coerce')
                
                # Drop rows where gross_fare is NaN (was non-numeric)
                df = df.dropna(subset=['gross_fare'])
                
                # Convert to int
                df['gross_fare'] = df['gross_fare'].astype(int)
                
                self.logger.info(f"Successfully converted gross_fare to int for {len(df)} records")
                
            except Exception as e:
                self.logger.error(f"Error converting gross_fare to int: {e}")
                # Return empty DataFrame with correct columns if conversion fails completely
                return pd.DataFrame(columns=df.columns)
        
        return df

    def getMarketFareAdjustments(self, origin, destin):
        try:
            results = list(self.db.market_fare_adjustments.find({ "sector": origin + destin }))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(f"Error getting Market fare adjustments: {e}")
            return pd.DataFrame()
    
    def getMappedStation(self, station):
        try:
            doc = self.db.market_fare_station_map.find_one(
                {"$or": [{"station1": station}, {"station2": station}]}
            )

            if not doc:
                return None

            if station == doc.get("station1"):
                return doc.get("station2")
            elif station == doc.get("station2"):
                return doc.get("station1")

            return None
        except Exception as e:
            self.logger.error(f"Error getting mapped Market fare station: {e}")
            return None

    def getMarketFareRange(self, channel, currParams, currFltDateStr, DaySpan, startTime, endtime, Day3Flag, hrs, origin, destination):
        
        try:
            currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()
            updateDate = currFltDate + timedelta(days=int(DaySpan))
 
            if Day3Flag == False:
                startDateTime = datetime.combine(currFltDate, datetime.strptime(startTime, "%H:%M").time())
                enddateTime = datetime.combine(currFltDate, datetime.strptime(endtime, "%H:%M").time())
                updateDate = currFltDate + timedelta(days=int(DaySpan))
                
                if currFltDate > updateDate:
                    startDateTime = datetime.combine(updateDate, datetime.strptime(startTime, "%H:%M").time())
                    enddateTime = datetime.combine(currFltDate, datetime.strptime(endtime, "%H:%M").time())
                elif currFltDate < updateDate:
                    startDateTime = datetime.combine(currFltDate, datetime.strptime(startTime, "%H:%M").time())
                    enddateTime = datetime.combine(updateDate, datetime.strptime(endtime, "%H:%M").time())
 
            if channel == B2B_CHANNEL:
                carr_exclusion = currParams.carr_exclusion_b2b
                flightExclusion = currParams.flightExclusionB2B
            else:
                carr_exclusion = currParams.carr_exclusion_b2c
                flightExclusion = currParams.flightExclusionB2C
 
            carr_exclusion_list = carr_exclusion.split('/') if isinstance(carr_exclusion, str) and carr_exclusion else []
            flight_exclusion_list = flightExclusion.split('/') if isinstance(flightExclusion, str) and flightExclusion else []
 
            # Log the search parameters
            self.logger.info(f"Searching market fares: {origin}-{destination}, Channel: {channel}, Date: {currFltDateStr}")
 
            # Check if collections exist
            collection_names = self.master_db.list_collection_names()
            ag_exists = 'market_fares_ag' in collection_names
            rg_exists = 'market_fares_rg' in collection_names
            
            if not ag_exists and not rg_exists:
                self.logger.warning("Neither market_fares_ag nor market_fares_rg collections found")
                return pd.DataFrame()
 
            # Build match conditions
            base_match_conditions = {
                "source_airport": origin,
                "destination_airport": destination,
                "channel": channel,
                "outbound_airline_code": {
                    "$nin": carr_exclusion_list + ["QP", "G8"]
                },
                "outbound_stop": '0'
            }
            
            if flight_exclusion_list:
                base_match_conditions["outbound_flight"] = {"$nin": flight_exclusion_list}
 
            # Add time interval condition for non-test data
            if hasattr(self, 'inputSource') and self.inputSource.input_source.get(CONST_MARKET_FARES) != TEST_DATA:
                hrs_ago = datetime.now() - timedelta(hours=hrs)
                base_match_conditions["created_at"] = {"$gt": hrs_ago}
 
            # Date and time conditions
            if Day3Flag:
                base_match_conditions.update({
                    "departure_date": {
                        "$gte": datetime.combine(currFltDate, datetime.min.time()),
                        "$lte": datetime.combine(updateDate, datetime.max.time())
                    }
                })
                
                # Add time filtering
                start_time_parts = [int(x) for x in startTime.split(':')]
                end_time_parts = [int(x) for x in endtime.split(':')]
                
                base_match_conditions["$expr"] = {
                    "$and": [
                        {"$gte": [{"$hour": "$outbound_departuretime"}, start_time_parts[0]]},
                        {"$lte": [{"$hour": "$outbound_departuretime"}, end_time_parts[0]]}
                    ]
                }
            else:
                base_match_conditions["$expr"] = {
                    "$and": [
                        {
                            "$gte": [
                                {"$dateFromParts": {
                                    "year": {"$year": "$departure_date"},
                                    "month": {"$month": "$departure_date"},
                                    "day": {"$dayOfMonth": "$departure_date"},
                                    "hour": {"$hour": "$outbound_departuretime"},
                                    "minute": {"$minute": "$outbound_departuretime"},
                                    "second": {"$second": "$outbound_departuretime"}
                                }},
                                startDateTime
                            ]
                        },
                        {
                            "$lte": [
                                {"$dateFromParts": {
                                    "year": {"$year": "$departure_date"},
                                    "month": {"$month": "$departure_date"},
                                    "day": {"$dayOfMonth": "$departure_date"},
                                    "hour": {"$hour": "$outbound_departuretime"},
                                    "minute": {"$minute": "$outbound_departuretime"},
                                    "second": {"$second": "$outbound_departuretime"}
                                }},
                                enddateTime
                            ]
                        }
                    ]
                }
 
            all_results = []
 
            # Query AG collection if it exists
            if ag_exists:
                try:
                    ag_pipeline = [
                        {
    "$set": {
      "outbound_departuretime": {
        "$dateFromString": {
          "dateString": {
            "$concat": ["1970-01-01T", "$outbound_departuretime"]
          },
          "format": "%Y-%m-%dT%H:%M"
        }
      }
    }
  },
                        {"$match": base_match_conditions},
                        {"$addFields": {
                            "source_table": "AG",
                            "mktFile": {"$concat": [{"$toString": "$scrape_file_name"}, "_AG"]}
                        }},
                        {"$project": {
                            "gross_fare": 1, "outbound_airline_code": 1, "outbound_flight": 1,
                            "departure_date": 1, "destination_airport": 1, "source_airport": 1,
                            "channel": 1, "outbound_departuretime": 1, "created_at": 1,
                            "mktFile": 1, "source_table": 1
                        }}
                    ]
                    
                    ag_cursor = self.master_db.market_fares_ag.aggregate(ag_pipeline)
                    ag_results = list(ag_cursor)
                    all_results.extend(ag_results)
                    self.logger.info(f"Found {len(ag_results)} records from AG collection")
                    
                except Exception as e:
                    self.logger.error(f"Error querying AG collection: {e}")
 
            # Query RG collection if it exists
            if rg_exists:
                try:
                    rg_pipeline = [
                        {
    "$set": {
      "outbound_departuretime": {
        "$dateFromString": {
          "dateString": {
            "$concat": ["1970-01-01T", "$outbound_departuretime"]
          },
          "format": "%Y-%m-%dT%H:%M"
        }
      }
    }
  },
                        {"$match": base_match_conditions},
                         {"$addFields": {
        "gross_fare": {
            "$convert": {
                "input": "$gross_fare",
                "to": "double",
                "onError": None,
                "onNull": None
            }
        }
    }},
                        {"$addFields": {
                            "source_table": "RG",
                            "mktFile": {"$concat": [{"$toString": "$scrape_file_name"}, "_RG"]}
                        }},
                        {"$project": {
                            "gross_fare": 1, "outbound_airline_code": 1, "outbound_flight": 1,
                            "departure_date": 1, "destination_airport": 1, "source_airport": 1,
                            "channel": 1, "outbound_departuretime": 1, "created_at": 1,
                            "mktFile": 1, "source_table": 1
                        }}
                    ]
                    
                    rg_cursor = self.master_db.market_fares_rg.aggregate(rg_pipeline)
                    rg_results = list(rg_cursor)
                    all_results.extend(rg_results)
                    self.logger.info(f"Found {len(rg_results)} records from RG collection")
                    
                except Exception as e:
                    self.logger.error(f"Error querying RG collection: {e}")
 
            if not all_results:
                self.logger.info("No market fare records found")
                return pd.DataFrame(columns=['gross_fare', 'outbound_airline_code', 'outbound_flight', 'mktFile'])
 
 
            # Convert to DataFrame and process
            df = pd.DataFrame(all_results)
            
            # Sort by partition columns and created_at descending
            df = df.sort_values([
                'channel', 'departure_date', 'destination_airport',
                'source_airport', 'outbound_flight', 'outbound_departuretime',
                'created_at'
            ], ascending=[True, True, True, True, True, True, False])
            
            # Remove duplicates keeping the first (latest) record
            df_dedup = df.drop_duplicates(
                subset=['channel', 'departure_date', 'destination_airport',
                    'source_airport', 'outbound_flight', 'outbound_departuretime'],
                keep='first'
            )
            
            # Select required columns and sort by gross_fare
            mktFareRange = df_dedup[['gross_fare', 'outbound_airline_code', 'outbound_flight', 'mktFile']].copy()
            mktFareRange = self._convert_gross_fare_to_int(mktFareRange)
            mktFareRange = mktFareRange.sort_values('gross_fare', ascending=True).reset_index(drop=True)
            
            self.logger.info(f"Returning {len(mktFareRange)} unique market fare records")
            
            return mktFareRange
            
        except Exception as e:
            self.logger.error(f"Error in getMarketFareRange: {e}")
            return  pd.DataFrame(columns=['gross_fare', 'outbound_airline_code', 'outbound_flight', 'mktFile'])

    def getMarketFareRangeConnections(self, channel, currParams, currFltDateStr, outBoundStop, outboundDuration):
        """
        Fetches market fare data for flights with connections using a PyMongo aggregation pipeline.
        This is a direct conversion of the original SQL-based function.

        - Converts 'created_at' string to a date for filtering.
        - Handles deduplication by keeping the most recent record for each unique flight.
        """
        try:
            # --- 1. Parameter Setup (Matches original SQL function) ---
            origin = currParams.sector1[:3]
            destination = currParams.sector2[3:]
            
            if channel == B2B_CHANNEL:
                carr_exclusion = currParams.carrierExclusionB2B
            else:
                carr_exclusion = currParams.carrierExclusionB2C
    
            carr_exclusion_list = carr_exclusion.split('/') if isinstance(carr_exclusion, str) and carr_exclusion else []
            
            # Convert the flight date string to datetime objects for the start and end of that day.
            # This is the robust way to match "departure_date = 'YYYY-MM-DD'" in MongoDB.
            fltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()
            start_of_day = datetime.combine(fltDate, datetime.min.time())
            end_of_day = datetime.combine(fltDate, datetime.max.time())

            self.logger.info(
                f"Searching connection fares: {origin}-{destination}, Channel: {channel}, "
                f"Date: {currFltDateStr}, Max Stops: {outBoundStop}, Max Duration: {outboundDuration}"
            )

            # --- 2. Check for Collection Existence ---
            collection_names = self.master_db.list_collection_names()
            ag_exists = 'market_fares_ag' in collection_names
            rg_exists = 'market_fares_rg' in collection_names

            if not ag_exists and not rg_exists:
                self.logger.warning("Neither market_fares_ag nor market_fares_rg collections found.")
                return pd.DataFrame()

            # --- 3. Build the MongoDB Aggregation Pipeline ---
            
            # Stage to convert the 'created_at' string to a date object.
        
            # date_conversion_stage = {
            #     "$set": {
            #         "created_at": {
            #             "$dateFromString": {
            #                 "dateString": "$created_at",
            #                 "format": "%Y-%m-%d %H:%M:%S"  # <-- EXAMPLE: "2023-10-27 15:45:00"
            #             }
            #         }
            #     }
            # }

            # Build the main matching conditions
            match_conditions = {
    "source_airport": origin,
    "destination_airport": destination,
    "channel": channel,
    "outbound_airline_code": {"$nin": carr_exclusion_list + ["QP", "G8"]},
    "departure_date": {
        "$gte": start_of_day,
        "$lte": end_of_day
    },
    "$expr": {
        "$and": [
            {
                "$lte": [
                    {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$outbound_duration"}, "string"]},
                            "then": {"$toDouble": "$outbound_duration"},
                            "else": "$outbound_duration"
                        }
                    },
                    float(outboundDuration)
                ]
            },
            {
                "$lte": [
                    {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$outbound_stop"}, "string"]},
                            "then": {"$toInt": "$outbound_stop"},
                            "else": "$outbound_stop"
                        }
                    },
                    int(outBoundStop)
                ]
            }
        ]
    }
}
            
            # Add time interval condition for non-test data (matches SQL's INTERVAL 2 DAY)
            if hasattr(self, 'inputSource') and self.inputSource.input_source.get(CONST_MARKET_FARES) != TEST_DATA:
                two_days_ago = datetime.now() - timedelta(days=2)
                match_conditions["created_at"] = {"$gt": two_days_ago}

            # Define the final fields to be returned from the database
            projection = {
                "_id": 0,
                "gross_fare": 1, "outbound_airline_code": 1, "outbound_flight": 1,
                "currency": 1, "destination_airport": 1, "source_airport": 1,
                "channel": 1, "outbound_departuretime": 1, "departure_date": 1,
                "created_at": 1, "mktFile": 1, "source_table": 1
            }
            
           
            
            # --- 4. Query Collections and Combine ---
            all_results = []

            # Query AG collection
            if ag_exists:
                try:
                    ag_pipeline = [
                        
                        {"$match": match_conditions},
                        {"$addFields": {
                            "source_table": "AG",
                            "mktFile": {"$concat": [{"$toString": "$scrape_file_name"}, "_AG"]}
                        }},
                        {"$project": projection}
                    ]
                    ag_cursor = self.master_db.market_fares_ag.aggregate(ag_pipeline)
                    ag_results = list(ag_cursor)
                    all_results.extend(ag_results)
                    self.logger.info(f"Found {len(ag_results)} connection records from AG collection.")
                except Exception as e:
                    self.logger.error(f"Error querying AG collection for connections: {e}")

            # Query RG collection
            if rg_exists:
                try:
                    rg_pipeline = [
                        
                        {"$match": match_conditions},
                        {"$addFields": {
                            "source_table": "RG",
                            "mktFile": {"$concat": [{"$toString": "$scrape_file_name"}, "_RG"]}
                        }},
                        {"$project": projection}
                    ]
                    rg_cursor = self.master_db.market_fares_rg.aggregate(rg_pipeline)
                    rg_results = list(rg_cursor)
                    all_results.extend(rg_results)
                    self.logger.info(f"Found {len(rg_results)} connection records from RG collection.")
                except Exception as e:
                    self.logger.error(f"Error querying RG collection for connections: {e}")
            
            if not all_results:
                self.logger.info("No market fare connection records found for the given criteria.")
                return pd.DataFrame(columns=['gross_fare', 'outbound_airline_code', 'outbound_flight', 'currency', 'mktFile'])

        

            # --- 5. Deduplicate and Sort in Pandas (Replicates SQL Window Function) ---
            df = pd.DataFrame(all_results)
            
            # These columns define a unique flight, as in the SQL PARTITION BY clause
            partition_cols = [
                'channel', 'departure_date', 'destination_airport',
                'source_airport', 'outbound_flight', 'outbound_departuretime'
            ]
            
            # Sort to bring the latest record for each flight to the top
            df = df.sort_values(by=partition_cols + ['created_at'], ascending=False)
            
            # Keep only the first (latest) record for each unique flight
            df_dedup = df.drop_duplicates(subset=partition_cols, keep='first')
            
            # Select final columns and sort by price, as in the final SQL ORDER BY
            mktFareRange = df_dedup[['gross_fare', 'outbound_airline_code', 'outbound_flight', 'currency', 'mktFile']].copy()
            mktFareRange = self._convert_gross_fare_to_int(mktFareRange)
            mktFareRange = mktFareRange.sort_values('gross_fare', ascending=True).reset_index(drop=True)
            
            self.logger.info(f"Returning {len(mktFareRange)} unique market fare connection records.")
            
            return mktFareRange

        except Exception as e:
            self.logger.error(f"An unexpected error occurred in getMarketFareRangeConnections: {e}")
            return pd.DataFrame(columns=['gross_fare', 'outbound_airline_code', 'outbound_flight', 'currency', 'mktFile'])


    def getCurrentTime(self, currentTimeFlag, currParams, currFltDate, starttime):
        if currentTimeFlag:
            currFltDate = datetime.strptime(currFltDate, "%m/%d/%Y").strftime("%Y-%m-%d")
            try:
                result = self.db.config_runtime.find_one({
                    "FlightNumber": int(currParams.flight_number),
                    "Origin": currParams.origin,
                    "Destin": currParams.destin,
                    "FlightDate": currFltDate
                })
                self.currentTime = result["runTime"] if result else None
            except Exception as e:
                self.logger.error(e)
        else:
            ist_timezone = pytz.timezone('Asia/Kolkata')
            cur_time = starttime.astimezone(ist_timezone).time().strftime("%H:%M")
            self.currentTime = cur_time
        return self.currentTime

    def get_qp_currency(self, origin, destin):
        try:
            results = list(self.db.fares.find({"Org": origin, "Dst": destin}))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_qp_fares(self, origin, destin):
        try:
            results = list(self.db.fares.find({"Org": origin, "Dst": destin}))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_TBF_fares(self, origin, destin):
        is_valid = False
        tbf_rbds = TBF_RBD.split(',')
        try:
            results = list(self.db.fares.find({
                "Org": origin,
                "Dst": destin,
                "RBD": {"$in": tbf_rbds},
                "TOTAL": {"$ne": 1}
            }))
            rbddata = pd.DataFrame(results)
            is_valid = (
                len(rbddata) >= TBF_COUNT 
                and not (rbddata['TOTAL'] <= 0).any() 
                and not rbddata['TOTAL'].isnull().any()
            )
        except Exception as e:
            self.logger.error(e)
        return is_valid

    def rbd_list_builder(self, channel):
        include_rbd_list = []

        if channel == 'B2C':
            include_rbd_list = [rbd.strip() for rbd in self.constants_utils.B2C_RBD_VALUES.split(',')]
        elif channel == 'B2B':
            include_rbd_list = [rbd.strip() for rbd in self.constants_utils.B2B_RBD_VALUES.split(',')]
        elif channel == 'TBF':
            include_rbd_list = [rbd.strip() for rbd in self.constants_utils.TBF_RBD.split(',')]
        elif channel == 'CUG':
            include_rbd_list = [rbd.strip() for rbd in self.constants_utils.CUG_RBD.split(',')]
        elif channel == 'GROUP':        
            include_rbd_list = [rbd.strip() for rbd in self.constants_utils.GROUP_RBD_VALUES.split(',')]
        elif channel == 'ALL':
            include_rbd_list = [rbd.strip() for rbd in self.constants_utils.B2C_RBD_VALUES.split(',')]
            include_rbd_list += [rbd.strip() for rbd in self.constants_utils.B2B_RBD_VALUES.split(',')]
            include_rbd_list += [rbd.strip() for rbd in self.constants_utils.TBF_RBD.split(',')]
            include_rbd_list += [rbd.strip() for rbd in self.constants_utils.CUG_RBD.split(',')]
            include_rbd_list += [rbd.strip() for rbd in self.constants_utils.GROUP_RBD_VALUES.split(',')]

        return include_rbd_list

    def getValueWithOffset(self, origin, destin, anchorFare, offset, channel, openResult, route, strategy_criteria = None):
        self.logger.info("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))

        if (not anchorFare) or anchorFare == CONST_NA or pd.isna(anchorFare) or anchorFare == 0:
            self.logger.info('Strategy Fare not available.')
            openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['Strategy Fare NA'],
                         'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}
            self.redishelper.updateRunAudit()
            return openResult, pd.DataFrame()
        
        try:
            results = list(self.db.fares.find({"Org": origin, "Dst": destin, "route": route, "TOTAL": {"$ne": 1}}))
            rbddata = pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            rbddata = pd.DataFrame()

        if rbddata.empty:
            self.logger.info('QP Fare not available. Please validate Fares table')
            openResult = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['QP Fares not available'],
                         'anchorFare': [anchorFare], 'statusInd': [1], 'allocationStatus': [1]}
            self.redishelper.updateRunAudit()
            return openResult, rbddata

        # exclude_rbd_list = TBF_RBD.split(',') + CUG_RBD.split(',')
        # exclude_rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))

        query = {
            "Org": origin,
            "Dst": destin,
            "route": route,
            "RBD": {"$in": self.rbd_list_builder(channel)},
            "TOTAL": {"$ne": 1}
        }

        sortorder = -1  # desc
        use_pipeline = False

        if int(offset) < 0:
            if strategy_criteria and (strategy_criteria == MINC_FARE_ANCHOR or strategy_criteria == MINOC_FARE_ANCHOR):
                pipeline = [
                    {"$match": query},
                    {
                        "$addFields": {
                            "fare_diff": {"$abs": {"$subtract": ["$TOTAL", float(anchorFare)]}}
                        }
                    },
                    {"$sort": {"fare_diff": 1}},
                    {
                        "$group": {
                            "_id": None,
                            "closest_total": {"$first": "$TOTAL"},
                            "all_docs": {"$push": "$$ROOT"}
                        }
                    },
                     {"$unwind": "$all_docs"},
                    {
                        "$addFields": {
                            "all_docs.closest_total": "$closest_total"
                        }
                    },
                    {"$replaceRoot": {"newRoot": "$all_docs"}},
                    {"$match": {"$expr": {"$lte": ["$TOTAL", "$$ROOT.closest_total"]}}},
                    {"$sort": {"TOTAL": -1}}
                ]
                use_pipeline = True
            else:
                query["TOTAL"] = {"$lt": int(anchorFare), "$ne": 1}
            sortorder = -1

        if int(offset) >= 0:
            anchorFare = float(anchorFare)
            if anchorFare > 0:
                if strategy_criteria and (strategy_criteria == MINC_FARE_ANCHOR or strategy_criteria == MINOC_FARE_ANCHOR):
                    pipeline = [
                        {"$match": query},
                        {
                            "$addFields": {
                                "fare_diff": {"$abs": {"$subtract": ["$TOTAL", anchorFare]}}
                            }
                        },
                        {"$sort": {"fare_diff": 1}},
                        {
                            "$group": {
                                "_id": None,
                                "closest_total": {"$first": "$TOTAL"},
                                "all_docs": {"$push": "$$ROOT"}
                            }
                        },
                        {"$unwind": "$all_docs"},
                        {
                            "$addFields": {
                                "all_docs.closest_total": "$closest_total"
                            }
                        },
                        {"$replaceRoot": {"newRoot": "$all_docs"}},
                        {"$match": {"$expr": {"$gte": ["$TOTAL", "$$ROOT.closest_total"]}}},
                        {"$sort": {"TOTAL": 1}}
                    ]
                    use_pipeline = True
                else:
                    query["TOTAL"] = {"$gte": int(anchorFare), "$ne": 1}
            else:
                query["TOTAL"] = {"$gte": 1000000, "$ne": 1}
            sortorder = 1

        if self.debugMode:
            self.logger.info(query)

        try:
            if use_pipeline:
                results = list(self.db.fares.aggregate(pipeline))
            else:
                results = list(self.db.fares.find(query).sort("TOTAL", sortorder))
            rbddata = pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            rbddata = pd.DataFrame()

        return openResult, rbddata

    def fetchExtremePublishedFare(self, origin, destin, channel, type, backstop, route):
        # exclude_rbd_list = TBF_RBD.split(',') + CUG_RBD.split(',')
        # exclude_rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))

        # Base query
        query = {
            "Org": origin,
            "Dst": destin,
            "route": route,
            "RBD": {"$in": self.rbd_list_builder(channel)},
            "TOTAL": {"$ne": 1}
        }

        # Add channel-specific regex filter
        # if channel == 'B2C':
        #     query["$and"].append({"RBD": {"$not": {"$regex": "^Z"}}})
        # else:  # B2B
        #     query["$and"].append({"RBD": {"$regex": "^Z"}})

        # Add TOTAL/backstop and sort order
        query["TOTAL"] = {"$gte": float(backstop), "$ne": 1}
        sortorder = 1 if type == 'LOW' else -1

        try:
            results = list(self.db.fares.find(query).sort("TOTAL", sortorder))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getHourlyOwnFareData(self, ndo, hour):
        try:
            result = self.db.config_ownfares_hourly.find_one({
                "ndoStart": {"$lte": ndo},
                "ndoEnd": {"$gte": ndo},
                "hour": int(hour)
            })
            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getGridOwnFareData(self, ndoBand, dlfBand, channel):
        try:
            result = self.db.config_ownfare_grid.find_one({
                "ndoBand": ndoBand,
                "dlfBand": dlfBand,
                "channel": channel
            })
            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getFareHistoryData(self, ndo, sector, month, dow, channel, currTime):
        try:
            ndo_result = self.db.config_ndo_range.find_one({"start": {"$lte": ndo}, "end": {"$gt": ndo}})
            if not ndo_result:
                return pd.DataFrame()

            ndo_value = ndo_result['value']
            currTime = currTime + ':00'

            # Determine cluster based on time
            time_obj = datetime.strptime(currTime, '%H:%M').time()
            cluster = None
            if datetime.strptime('02:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('07:30:00', '%H:%M:%S').time():
                cluster = 0
            elif datetime.strptime('07:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('11:00:00', '%H:%M:%S').time():
                cluster = 1
            elif datetime.strptime('11:00:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('14:30:00', '%H:%M:%S').time():
                cluster = 2
            elif datetime.strptime('14:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('18:00:00', '%H:%M:%S').time():
                cluster = 3
            elif datetime.strptime('18:00:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('21:00:00', '%H:%M:%S').time():
                cluster = 4
            else:
                cluster = 5

            pipeline = [
                {
                    "$match": {
                        "sector": sector,
                        "Month": month,
                        "channel": channel,
                        "DOW": dow
                    }
                },
                {
                    "$addFields": {
                        "Cluster": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$and": [
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$startTime"}}, "02:30:00"]},
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$endTime"}}, "07:30:00"]}
                                    ]}, "then": 0},
                                    {"case": {"$and": [
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$startTime"}}, "07:30:00"]},
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$endTime"}}, "11:00:00"]}
                                    ]}, "then": 1},
                                    {"case": {"$and": [
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$startTime"}}, "11:00:00"]},
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$endTime"}}, "14:30:00"]}
                                    ]}, "then": 2},
                                    {"case": {"$and": [
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$startTime"}}, "14:30:00"]},
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$endTime"}}, "18:00:00"]}
                                    ]}, "then": 3},
                                    {"case": {"$and": [
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$startTime"}}, "18:00:00"]},
                                        {"$eq": [{"$dateToString": {"format": "%H:%M:%S", "date": "$endTime"}}, "21:00:00"]}
                                    ]}, "then": 4}
                                ],
                                "default": 5
                            }
                        }
                    }
                },
                {"$match": {"Cluster": cluster}},
                {"$project": {"price": f"${ndo_value}"}}
            ]

            results = list(self.db.config_ownfares.aggregate(pipeline))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getProfileFares(self, sector, month, dow, currTime, percentile, incremented_profile_fare_flag=0):
        try:
            time_obj = datetime.strptime(currTime, '%H:%M').time()
            cluster = None
            if datetime.strptime('02:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('07:30:00', '%H:%M:%S').time():
                cluster = 0
            elif datetime.strptime('07:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('11:00:00', '%H:%M:%S').time():
                cluster = 1
            elif datetime.strptime('11:00:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('14:30:00', '%H:%M:%S').time():
                cluster = 2
            elif datetime.strptime('14:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('18:00:00', '%H:%M:%S').time():
                cluster = 3
            elif datetime.strptime('18:00:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('21:00:00', '%H:%M:%S').time():
                cluster = 4
            else:
                cluster = 5

            # Choose collection based on flag
            if int(incremented_profile_fare_flag) == 1:
                collection = self.db.group_incremented_profile_fares
            else:
                collection = self.db.config_profile_fares

            result = collection.find_one({
                "sector": sector,
                "month": month,
                "dow": dow,
                "cluster": cluster
            })

            if result:
                return pd.DataFrame([{"price": result.get(percentile)}])
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()
    
    def getCluster(self, time_obj):
        cluster = None
        if datetime.strptime('02:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('07:30:00', '%H:%M:%S').time():
            cluster = 0
        elif datetime.strptime('07:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('11:00:00', '%H:%M:%S').time():
            cluster = 1
        elif datetime.strptime('11:00:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('14:30:00', '%H:%M:%S').time():
            cluster = 2
        elif datetime.strptime('14:30:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('18:00:00', '%H:%M:%S').time():
            cluster = 3
        elif datetime.strptime('18:00:00', '%H:%M:%S').time() <= time_obj < datetime.strptime('21:00:00', '%H:%M:%S').time():
            cluster = 4
        else:
            cluster = 5
        return cluster

    def profileFaresExtract(self, origin, destin, month, dow, currTime, incremented_profile_fare_flag=0):
        try:
            sector = origin + destin
            time_obj = datetime.strptime(currTime, '%H:%M').time()
            cluster = self.getCluster(time_obj)

            # Choose collection based on flag
            if int(incremented_profile_fare_flag) == 1:
                collection = self.db.group_incremented_profile_fares
            else:
                collection = self.db.config_profile_fares

            result = collection.find_one(
    {
        "sector": sector,
        "month": month,
        "dow": dow,
        "cluster": cluster
    },
    {
        "_id": 0,  # exclude _id
        "Q0": 1, "Q1": 1, "Q2": 1, "Q3": 1, "Q4": 1,
        "Q5": 1, "Q6": 1, "Q7": 1, "Q8": 1, "Q9": 1,
        "Q10": 1, "Q11": 1, "Q12": 1, "Q13": 1, "Q14": 1,
        "Q15": 1, "Q16": 1, "Q17": 1, "Q18": 1, "Q19": 1,
        "Q20": 1  # include only Q0 to Q20
    }
)

            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getMappedMonth(self, currFltDate):
        try:
            result = self.db.config_date_event.find_one({"date": currFltDate})
            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getUpsellValue(self, currFltDate, sector):
        try:
            result = self.db.config_profile_fare_station_upsell.find_one({
                "date": currFltDate,
                "sector": sector
            })
            if result:
                val = result.get('value')
                if pd.isna(val) or (isinstance(val, float) and __import__('math').isnan(val)):
                    return pd.DataFrame()
                value = str(val).replace('%', '')
                return pd.DataFrame([{"percentage": value}])
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getArea(self, currentHour):
        try:
            pipeline = [
    {"$match": {"hours": {"$lte": currentHour}}},
    {
        "$group": {
            "_id": None, 
            "sum": {"$sum": {"$toDouble": "$shareOfBooking"}}
        }
    }
]
            results = list(self.db.config_share_of_booking.aggregate(pipeline))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getRDBTable(self, dplfBand, endOfDayDplfBand, currentTime, ndoBand):
        try:
            docs = list(self.db.config_determine_fares.find({}))
            matching_docs = []

            for doc in docs:
                cond = json.loads(doc.get("conditions", "{}"))

                # Check ndo_band first
                if "ndo_band" in cond:
                    if cond["ndo_band"] != str(ndoBand):
                        continue  # Skip if ndo_band does not match
                else:
                    # Only check time if ndo_band is not present
                    start_time = cond.get("start_time", "00:00")
                    end_time = cond.get("end_time", "23:59")
                    if not (start_time <= currentTime <= end_time):
                        continue

                # Check start and end DLF bands
                start_band_ok = str(dplfBand) in cond.get("start_of_day_dlf_band", "")
                end_band_ok = str(endOfDayDplfBand) in cond.get("end_of_day_dlf_band", "")

                if start_band_ok and end_band_ok:
                    matching_docs.append(doc)

            return pd.DataFrame(matching_docs)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def fareExtract(self, origin, destination, channel, openingRBD, route):
        query = {"Org": origin, "Dst": destination, "route": route}

        if isinstance(channel, str):
            channel_list = [channel]
        else:
            channel_list = channel

        rbd_list = []
        for ch in channel_list:
            rbd_list.extend(self.rbd_list_builder(ch.strip()))

        query["RBD"] = {"$in": rbd_list}
        if openingRBD:
            query["RBD"] = openingRBD

        try:
            results = list(self.db.fares.find(query).sort("TOTAL", 1))
            df = pd.DataFrame(results)
            if not df.empty:
                df['id'] = range(1, len(df) + 1)
            return df
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def insert_run_fltDate_audit(self, run_id, flightNumber,sector, flightDate, result, b2cstatus, b2bstatus):
        try:
            self.db.run_flight_date_audit.insert_one({
                "runId": run_id,
                "flightNumber": flightNumber,
                "sector":sector,
                "flightDate": flightDate,
                "result": result,
                "b2cstatus": b2cstatus,
                "b2bstatus": b2bstatus,
                "createdAt": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        except Exception as e:
            self.logger.error(e)

    def getCurrentBookedLoads(self, origin, destin, startDate, endDate):
        try:
            # Convert dates to datetime if they're date objects
            if isinstance(startDate, datetime):
                start_dt = startDate
            else:
                start_dt = datetime.combine(startDate, datetime.min.time())
            
            if isinstance(endDate, datetime):
                end_dt = endDate
            else:
                end_dt = datetime.combine(endDate, datetime.min.time())
            
            results = list(self.db.bookload.find({
                "DepartureDate": {"$gte": start_dt, "$lte": end_dt},
                "Origin": origin,
                "Destination": destin
            }))
            
            if self.debugMode:
                self.logger.info(f"Query: origin={origin}, destin={destin}, dates={startDate} to {endDate}")
            
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getLid(self, origin, destin, startDate, endDate, FltNumber):
        try:
            results = list(self.db.bookload.find({
                "DepartureDate": {"$gte": startDate, "$lte": endDate},
                "Origin": origin,
                "Destination": destin,
                "FlightNumber": int(FltNumber)
            }))

            if self.debugMode:
                self.logger.info(f"Query: {origin}-{destin}, Flight {FltNumber}")

            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def checkForNextFLight(self, currFltDateStr, currParams):
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT)

        try:
            results = list(self.db.bookload.find({
                "DepartureDate": currFltDate,
                "Origin": currParams.origin,
                "Destination": currParams.destin
            }).sort("DepartureTime", 1))

            df = pd.DataFrame(results)
            if df.empty:
                return False

            df['FlightNumber'] = df['FlightNumber'].astype(str)
            row_index = df[
                (df['FlightNumber'] == str(currParams.flight_number)) &
                (df['Origin'] == currParams.origin) &
                (df['Destination'] == currParams.destin)
            ].index

            return len(row_index) > 0 and row_index[0] < len(df) - 1
        except Exception as e:
            self.logger.error(e)
            return False

    def getLastSellingFare(self, currFltDateStr, currParams):
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).strftime(DATE_FORMAT_2)

        try:
            result = self.db.booking_fare_data.find_one({
                "FlightDate": currFltDate,
                "FlightNumber": int(currParams.flight_number),
                "Origin": currParams.origin,
                "Destin": currParams.destin
            })
            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getNumberOfBookings(self, currFltDateStr, currParams):
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2)

        try:
            result = self.db.booking_fare_data.find_one({
                "FlightDate": currFltDate,
                "FlightNumber": currParams.flight_number,
                "Origin": currParams.origin,
                "Destin": currParams.destin
            })
            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_parameters(self):
        try:
            results = list(self.db.rm_parameter_values.find())
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getroutedDestination(self, origin, destination):
        try:
            sector = origin + destination
            results = list(self.db.config_marketfares_rerouting.find({"sector": sector}))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getFinalPrice(self, rbd_sector1, rbd_sector2, sector1, sector2, channel):

        try:
            sector1_result = self.db.fares.find_one({
                "SECTOR": sector1,
                "route": "L",
                "RBD": rbd_sector1,
                "TOTAL": {"$ne": 1}
            })

            sector2_result = self.db.fares.find_one({
                "SECTOR": sector2,
                "route": "L",
                "RBD": rbd_sector2,
                "TOTAL": {"$ne": 1}
            })

            if not sector1_result or not sector2_result:
                return 0, 0, 0, "", ""

            sector1_total = sector1_result["TOTAL"]
            sector2_total = sector2_result["TOTAL"]
            currency1 = sector1_result["currency"]
            currency2 = sector2_result["currency"]
            total = sector1_total + sector2_total

            return total, sector1_total, sector2_total, currency1, currency2
        except Exception as e:
            self.logger.error(e)
            return 0, 0, 0, "", ""

    def getSellingPrice(self, rbd, sector, channel, route):
        try:
            result = self.db.fares.find_one({
                "$and": [
                    {"SECTOR": sector},
                    {"RBD": rbd},
                    {"route": route},
                    {"TOTAL": {"$ne": 1}}
                ]
            })

            if not result:
                return 0, ""

            return result["TOTAL"], result["currency"]
        except Exception as e:
            self.logger.error(e)
            return 0, ""

    def insertSummaryRow_connections(self, allocresults):
        if self.inputSource.logs[CONST_RUN_SUMMARY] == DB:
            latest_row = allocresults[-1]

            doc = {
                "Id": latest_row.Id,
                "RunId": latest_row.RunId,
                "Sector1": latest_row.Sector1,
                "Sector2": latest_row.Sector2,
                "FltNum1": str(latest_row.FltNum1),
                "FltNum2": str(latest_row.FltNum2),
                "DepDate1": self.format_date_to_mm_dd_yyyy(latest_row.DepDate1),
                "DepDate2": self.format_date_to_mm_dd_yyyy(latest_row.DepDate2),
                "BookedLoad": str(latest_row.BookedLoad),
                "BookedLoad1": str(latest_row.Bookedload1),
                "SellingFare1": str(latest_row.SellingFare1),
                "SellingFare2": str(latest_row.SellingFare2),
                "BookedLoad2": str(latest_row.Bookedload2),
                "OpenRBD1": str(latest_row.OpenRBD1),
                "OpenRBD2": str(latest_row.OpenRBD2),
                "Discount": str(latest_row.Discount),
                "anchorFare": latest_row.anchorFare,
                "anchorFareBeforeTolerance": latest_row.anchorFareBeforeTolerance,
                "OpenRBD": str(latest_row.OpenRBD),
                "SellingFare": str(latest_row.SellingFare),
                "Channel": latest_row.Channel,
                "HowDetermined": latest_row.HowDetermined,
                "allocationStatus": latest_row.allocationStatus,
                "Backstop": str(latest_row.Backstop),
                "CreatedAt": self.timeutils.getCurrentDateTime().strftime(DATE_TIME_FORMAT),
                "analystName": str(latest_row.analystName),
                "Variance1": str(latest_row.variance1),
                "Variance2": str(latest_row.variance2),
                "strategyFare": str(latest_row.strategyfare),
                "criteria": str(latest_row.criteria),
                "fareOffset": str(latest_row.fareOffset),
                "MktFare_Min": str(latest_row.MktFare_Min),
                "AirlineCode_Min": str(latest_row.AirlineCode_Min),
                "FlightNumber_Min": str(latest_row.FlightNumber_Min),
                "MktFare_Max": str(latest_row.MktFare_Max),
                "AirlineCode_Max": str(latest_row.AirlineCode_Max),
                "FlightNumber_Max": str(latest_row.FlightNumber_Max),
                "outBoundStop": str(latest_row.outBoundStop),
                "outboundDuration": str(latest_row.outboundDuration),
                "DiscountedFare": str(latest_row.discountedFare),
                "Currency": str(latest_row.currency),
                "conversionRate": str(latest_row.conversionRate),
                "mktFile": str(latest_row.mktFile),
                "firstrbd": str(latest_row.firstrbd),
                "otherrbd": str(latest_row.otherrbd),
                "Backstop_INR": str(latest_row.Backstop_INR),
                "ImputedForecast1": str(latest_row.ImputedForecast1),
                "ImputedForecast2": str(latest_row.ImputedForecast2)
            }

            try:
                self.db.run_summary_connections.insert_one(doc)
            except Exception as e:
                self.logger.error(e)

    def getcurrentRBD(self, current_date, flight, channel, sector, bookload):
        try:
            result = self.db.current_opening_rbd.find_one({
                "DepartureDate": str(current_date),
                "$expr": {"$eq": [{"$concat": ["$Origin", "$Destination"]}, sector]},
                "flightno": str(flight),
                "Channel": channel
            })
            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getDefaultAu(self, departure_date):
        month_name = datetime.strptime(departure_date, '%Y-%m-%d').strftime('%B')
        try:
            result = self.db.default_distress_inventory.find_one({"month": month_name})
            if result:
                return result["default_au"], result["au_cap"]
            return None, None
        except Exception as e:
            self.logger.error(e)
            return None, None

    def getDistressAu(self, ndo_band, dplf_band):
        try:
            result = self.db.distress_inventory_strategy.find_one({
                "ndo_band": ndo_band,
                "dplf_band": dplf_band
            })
            return result["au_value"] if result else None
        except Exception as e:
            self.logger.error(e)
            return None

    def get_seats_sold_by_class(self, flight_number, origin, destination, departure_date):
        try:
            results = list(self.db.current_rbd.find({
                "FlightNumber": str(flight_number),
                "DepartureDate": str(departure_date),
                "Origin": origin,
                "Destin": destination,
                "ClassOfService": {"$in": ["P", "L"]}
            }))

            if not results:
                return -1, -1

            df = pd.DataFrame(results)
            l_class_seats_sold = df[df['ClassOfService'] == 'L']['ClassSold'].values[0] if 'L' in df['ClassOfService'].values else -1
            p_class_seats_sold = df[df['ClassOfService'] == 'P']['ClassSold'].values[0] if 'P' in df['ClassOfService'].values else -1

            return l_class_seats_sold, p_class_seats_sold
        except Exception as e:
            self.logger.error(e)
            return -1, -1

    def get_current_rbds_by_class(self, departure_date, flight_number, sector):
        try:
            results = list(self.db.current_rbd.find({
                "DepartureDate": departure_date,
                "FlightNumber": str(flight_number),
                "$expr": {"$eq": [{"$concat": ["$Origin", "$Destin"]}, str(sector)]},
                "ClassOfService": {"$in": ["P", "L"]}
            }))

            if not results:
                return -1, -1

            df = pd.DataFrame(results)
            l_class_rbd = df[df['ClassOfService'] == 'L']['ClassAu'].values[0] if 'L' in df['ClassOfService'].values else -1
            p_class_rbd = df[df['ClassOfService'] == 'P']['ClassAu'].values[0] if 'P' in df['ClassOfService'].values else -1

            return l_class_rbd, p_class_rbd
        except Exception as e:
            self.logger.error(e)
            return -1, -1

    # def insertSummaryRow_connections(self, summary_list):
    #     try:
    #         print(f"DEBUG: insertSummaryRow_connections called with {len(summary_list)} items")
    #         if len(summary_list) > 0:
    #             print(f"DEBUG: First item RunId: {summary_list[0].RunId}")
            
    #         data_list = []
    #         for summary in summary_list:
    #             # This part of the code was incomplete in the instruction.
    #             # Assuming it was meant to construct a document for insertion.
    #             # For now, it's left as a placeholder to maintain syntactical correctness.
    #             pass # Placeholder for the actual logic that was intended to be here.
    #         # The original function had logic to insert a single doc based on allocresults[-1]
    #         # The new function signature suggests processing a list.
    #         # The instruction provided an incomplete code block for the loop.
    #         # To ensure syntactical correctness, the loop body is replaced with 'pass'.
    #         # The rest of the original function's logic for insertion is commented out
    #         # as it's unclear how it should integrate with the new 'summary_list' parameter.

    #         # if self.inputSource.logs[CONST_RUN_SUMMARY] == DB:
    #         #     latest_row = allocresults[-1]
    #         #     doc = { ... }
    #         #     try:
    #         #         self.db.run_summary_connections.insert_one(doc)
    #         #     except Exception as e:
    #         #         self.logger.error(e)
    #     except Exception as e:
    #         self.logger.error(e)

    def getSeriesBlockedSeats(self, departure_date):
        try:
            pipeline = [
                {
                    "$match": {
                        "DepartureDate": departure_date,
                        "OrgCode": {"$ne": self.constants_utils.TBF_ORG_CODE}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "blocked": {"$sum": "$SeatsBlocked"}
                    }
                }
            ]
            results = list(self.db.series_blocking.aggregate(pipeline))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame(columns=['blocked'])

    def get_series_seats(self, departure_date, flight_number, sector):
        try:
            results = list(self.db.series_booked.find({
                "FlightNumber": flight_number,
                "DepartureDate": departure_date,
                "$expr": {"$eq": [{"$concat": ["$Origin", "$Destination"]}, sector]}
            }))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    # def fetch_all_conversion_data(self):
    #     try:
    #         results = list(self.dwdb.currency_conversion.find())
    #         return pd.DataFrame(results)
    #     except Exception as e:
    #         self.logger.error(e)
    #         return pd.DataFrame()
    
    def fetch_all_conversion_data(self):
        if  self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            query = f"SELECT FromCurrencyCode, ToCurrencyCode, ConversionRate FROM currency_conversion"
            df = pd.read_sql(query, self.dwconn)
        else:
            collection = self.db['currency_conversion']

            # Using find() to get all documents with only specific fields
            cursor = collection.find(
                {},  # Empty filter means select all documents
                {
                    'FromCurrencyCode': 1,
                    'ToCurrencyCode': 1,
                    'ConversionRate': 1,
                    '_id': 0  # Exclude the _id field
                }
            )

            # Convert to pandas DataFrame
            df = pd.DataFrame(list(cursor))
        return df

    def get_curveids(self, sector1, sector2, fltno1, fltno2, dep1, dep2):
        try:
            dep1 = datetime.strptime(dep1, "%Y-%m-%d")  # Converts "2025-01-15" to datetime
            dep2 = datetime.strptime(dep2, "%Y-%m-%d")
            # Query both market_list and market_list_international
            query1 = {
    "$or": [
        {
            "$expr": {
                "$and": [
                    {"$eq": [{"$concat": ["$Origin", "$Destin"]}, sector1]},
                    {"$eq": [{"$dateFromString": {"dateString": "$PerStart", "format": "%m/%d/%Y"}}, dep1]}
                ]
            },
            "FlightNumber": int(fltno1)
        },
        {
            "$expr": {
                "$and": [
                    {"$eq": [{"$concat": ["$Origin", "$Destin"]}, sector2]},
                    {"$eq": [{"$dateFromString": {"dateString": "$PerStart", "format": "%m/%d/%Y"}}, dep2]}
                ]
            },
            "FlightNumber": int(fltno2)
        }
    ]
}

            results1 = list(self.db.market_list.find(query1))
            results2 = list(self.db.market_list_international.find(query1))

            combined = results1 + results2
            return pd.DataFrame(combined)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_second_flight_dep(self, flightno1, flightno2, org, destin, depDate):
        try:
            result = self.db.connections_details.find_one({
                "FlightNo1": int(flightno1),
                "FlightNo2": int(flightno2),
                "Origin1": org,
                "Destnination2": destin,
                "Departuredate1": depDate
            })
            return result['Departuredate2'] if result else ""
        except Exception as e:
            self.logger.error(e)
            return ""

    def get_tbf_discount(self, month, sector):
        try:
            # First try to find exact match
            result = self.db.tbf_discount_grid.find_one({
                "Sector": sector,
                "Month": month
            })

            # If not found, try with wildcards in priority order
            if not result:
                result = self.db.tbf_discount_grid.find_one({
                    "Sector": sector,
                    "Month": "*"
                })

            if not result:
                result = self.db.tbf_discount_grid.find_one({
                    "Sector": "*",
                    "Month": month
                })

            if not result:
                result = self.db.tbf_discount_grid.find_one({
                    "Sector": "*",
                    "Month": "*"
                })

            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getOverbookedSeats(self, origin, destin, FltNumber, currFltDate):
        sector = origin + destin
        try:
            result = self.db.expected_no_shows.find_one({
                "DepartureDate": str(currFltDate),
                "Sector": sector,
                "FlightNumber": str(FltNumber)
            })

            if self.debugMode:
                self.logger.info(f"Query: {sector}, Flight {FltNumber}, Date {currFltDate}")

            return result["ExpectedNoShows"] if result else CONST_NA
        except Exception as e:
            self.logger.error(e)
            return CONST_NA

    def get_farther_rbd_distribution(self, seats):
        try:
            results = list(self.db.salesinv_rbd_distribution.find({
                "seats": seats,
                "type": "farther"
            }))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_closer_rbd_distribution(self, seats):
        try:
            results = list(self.db.salesinv_rbd_distribution.find({
                "seats": seats,
                "type": "closer"
            }))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_sales_inventory(self, date):
        try:
            results = list(self.db.sales_inventory.find({"date": date}))
            return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_distress_calendar_value(self, dep_date, sector):
        try:
            result = self.db.distress_calendar.find_one({
                "date": dep_date,
                "sector": sector
            })
            return pd.DataFrame([result]) if result else pd.DataFrame()
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_CUG_fares(self, origin, destin):
        is_valid = False
        cug_rbds = CUG_RBD.split(',')
        try:
            results = list(self.db.fares.find({
                "Org": origin,
                "Dst": destin,
                "RBD": {"$in": cug_rbds},
                "TOTAL": {"$ne": 1}
            }))
            rbddata = pd.DataFrame(results)
            is_valid = (
                len(rbddata) >= CUG_COUNT 
                and not (rbddata['TOTAL'] <= 0).any() 
                and not rbddata['TOTAL'].isnull().any()
            )
        except Exception as e:
            self.logger.error(e)
        return is_valid

    def get_group_potential_seats(self, sector, month):
        try:
            result = self.db.group_potential_seats.find_one({
                "sector": sector,
                "month": month
            })
            return result['value'] if result else 0
        except Exception as e:
            self.logger.error(f"Error fetching group potential seats for sector {sector}, month {month}: {e}")
            return 0

    def get_group_profile_fare_increments(self, sector, year, month_str, dow, cluster):
        """
        Fetch group profile fare increment factors from group_profile_fare_increments collection.
        Returns a dict with keys I0, I1, ... I20 or None if not found.
        """
        try:
            result = self.db.group_profile_fare_increments.find_one({
                "sector": sector,
                "year": int(year),
                "month": month_str,
                "dow": dow,
                "cluster": int(cluster)
            })
            return result if result else None
        except Exception as e:
            self.logger.error(f"Error fetching group profile fare increments: {e}")
            return None

    def get_all_group_forecast_bands(self):
        """
        Fetch all documents from group_forecast_bands collection at once.
        Returns a list of band dicts sorted by 'start' ascending.
        Each dict contains: band, start, end, b2c_discount, fare_anchor_strategy,
        b2c_fare_weight, profile_fare_weight.
        """
        try:
            results = list(self.db.group_forecast_bands.find({}).sort("start", 1))
            bands = []
            for r in results:
                bands.append({
                    'band': int(r.get('band', 0)),
                    'start': r.get('start', 0),
                    'end': r.get('end', 0),
                    'b2c_discount': r.get('b2c_discount', 0),
                    'fare_anchor_strategy': r.get('fare_anchor_strategy', 'min'),
                    'b2c_fare_weight': r.get('b2c_fare_weight', 0),
                    'profile_fare_weight': r.get('profile_fare_weight', 1),
                })
            return bands
        except Exception as e:
            self.logger.error(f"Error fetching all group forecast bands: {e}")
            return []

    def get_group_forecast_band(self, imp_forecast):
        """
        Fetch group forecast band, b2c_discount, fare_anchor_strategy, and weight fields
        from group_forecast_bands collection.
        Returns a dict with 'band', 'b2c_discount', 'fare_anchor_strategy',
        'b2c_fare_weight', and 'profile_fare_weight' keys, or defaults.
        """
        try:
            result = self.db.group_forecast_bands.find_one({
                "start": {"$lte": float(imp_forecast)},
                "end": {"$gte": float(imp_forecast)}
            })
            if result:
                return {
                    'band': int(result['band']),
                    'b2c_discount': result.get('b2c_discount', 0),
                    'fare_anchor_strategy': result.get('fare_anchor_strategy', 'min'),
                    'b2c_fare_weight': result.get('b2c_fare_weight', 0),
                    'profile_fare_weight': result.get('profile_fare_weight', 1),
                }
            return {'band': 0, 'b2c_discount': 0, 'fare_anchor_strategy': 'min', 'b2c_fare_weight': 0, 'profile_fare_weight': 1}
        except Exception as e:
            self.logger.error(f"Error fetching group forecast band for forecast {imp_forecast}: {e}")
            return {'band': 0, 'b2c_discount': 0, 'fare_anchor_strategy': 'min', 'b2c_fare_weight': 0, 'profile_fare_weight': 1}

    def get_tbf_seats_blocked(self, orgcode, departuredate):
        try:
            pipeline = [
                {
                    "$match": {
                        "OrgCode": orgcode,
                        "DepartureDate": departuredate
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_blocked": {"$sum": "$seatsblocked"}
                    }
                }
            ]
            results = list(self.db.series_blocking.aggregate(pipeline))
            if results and results[0].get('total_blocked') is not None:
                return results[0]['total_blocked']
            return 0
        except Exception as e:
            self.logger.error(f"Error fetching TBF seats blocked for OrgCode {orgcode}, Date {departuredate}: {e}")
            return 0

    def check_sector_types(self, sector1, sector2):
        """
        Check if sectors are domestic or international using single query
        Returns tuple: (is_sector1_domestic, is_sector2_domestic)
        Uses config_station_master table: if any station has route='I', sector is international
        """
        try:
            sector1_origin = sector1[:3]
            sector1_dest = sector1[3:]
            sector2_origin = sector2[:3]
            sector2_dest = sector2[3:]

            # Query all 4 stations at once
            results = list(self.master_db.config_station_master.find({
                "station": {"$in": [sector1_origin, sector1_dest, sector2_origin, sector2_dest]}
            }))

            station_df = pd.DataFrame(results)

            # Check if sector1 is international (any station has route='I')
            sector1_stations = station_df[station_df['station'].isin([sector1_origin, sector1_dest])]
            is_sector1_domestic = not (sector1_stations['route'] == 'I').any() if not sector1_stations.empty else True

            # Check if sector2 is international (any station has route='I')
            sector2_stations = station_df[station_df['station'].isin([sector2_origin, sector2_dest])]
            is_sector2_domestic = not (sector2_stations['route'] == 'I').any() if not sector2_stations.empty else True

            return is_sector1_domestic, is_sector2_domestic

        except Exception as e:
            print(f"Error checking sector types for {sector1}, {sector2}: {e}")
            # Default to domestic if there's an error
            return True, True

    def getAutoToleranceConfig(self):
        try:
            ndo_cursor = self.db["auto_tolerance_ndo_config"].find({}, {"_id": 0})
            ndo_config = pd.DataFrame(list(ndo_cursor))
            
            delta_cursor = self.db["auto_tolerance_delta_bands"].find({}, {"_id": 0})
            delta_bands = pd.DataFrame(list(delta_cursor))
            
            grid_cursor = self.db["auto_tolerance_grid_values"].find({}, {"_id": 0})
            grid_values = pd.DataFrame(list(grid_cursor))
            
            return {
                "ndo_config": ndo_config,
                "delta_bands": delta_bands,
                "grid_values": grid_values
            }
        except Exception as e:
            self.logger.error("Error in getAutoToleranceConfig: " + str(e))
            return None

    def get_stage_length(self, departure_station, arrival_station):
        """
        Get the ActualDistance (stage length) from MySQL CityPair table or MongoDB city_pair collection.
        
        Args:
            departure_station: 3-letter departure station code (e.g., 'DEL')
            arrival_station: 3-letter arrival station code (e.g., 'BOM')
            
        Returns:
            float: ActualDistance in km, or 0 if not found
        """
        try:
            if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
                # Fetch from MySQL
                query = f"""
                    SELECT ActualDistance 
                    FROM QP_DWH_MASTER.CityPair 
                    WHERE DepartureStation = '{departure_station}' 
                    AND ArrivalStation = '{arrival_station}'
                    AND InActive = 0
                    LIMIT 1
                """
                result = pd.read_sql(query, self.dwconn)
                
                if not result.empty and result['ActualDistance'].iloc[0] is not None:
                    return float(result['ActualDistance'].iloc[0])
                else:
                    self.logger.info(f"No stage length found for {departure_station}-{arrival_station}")
                    return 0
            else:
                # Fetch from MongoDB city_pair collection
                result = self.db.city_pair.find_one({
                    "DepartureStation": departure_station,
                    "ArrivalStation": arrival_station,
                    "InActive": 0
                })
                
                if result and result.get('ActualDistance') is not None:
                    return float(result['ActualDistance'])
                else:
                    self.logger.info(f"No stage length found for {departure_station}-{arrival_station}")
                    return 0
        except Exception as e:
            self.logger.error(f"Error fetching stage length for {departure_station}-{arrival_station}: {e}")
            return 0

    def getAutoToleranceState(self, redis_key):
        try:
            result = self.db.auto_tolerance_redis_state.find_one({"redis_key": redis_key})
            if result:
                return result
            
            # Fallback for old format if migrating from auto_tolerance_
            if redis_key.startswith("run_history:"):
                old_key = redis_key.replace("run_history:", "auto_tolerance_", 1)
                old_result = self.db.auto_tolerance_redis_state.find_one({"redis_key": old_key})
                if old_result:
                    return old_result
        except Exception as e:
            self.logger.error(f"Error fetching auto_tolerance_redis_state from Mongo: {e}")
        return None

    def setAutoToleranceState(self, redis_key, state_dict):
        try:
            state_dict["redis_key"] = redis_key
            self.db.auto_tolerance_redis_state.update_one(
                {"redis_key": redis_key},
                {"$set": state_dict},
                upsert=True
            )
        except Exception as e:
            self.logger.error(f"Error setting auto_tolerance_redis_state to Mongo: {e}")