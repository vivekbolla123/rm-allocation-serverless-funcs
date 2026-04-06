"""
Market List Handler — simple model for per-sector ladder generation config.

Reads from MongoDB collections where field names are already in camelCase.
Collections:
    market_list              — Domestic sectors
    market_list_international — International sectors
    market_list_connections   — International connection sectors
"""

from datetime import datetime, date


# Route -> MongoDB collection name
COLLECTION_MAP = {
    'D':   'market_list',
    'I':   'market_list_international',
    'CNX': 'market_list_connections',
}


class MarketListConfig:
    """One row from a market_list collection. Fields match the DB camelCase names."""

    def __init__(self, params):
        self.sector = params.get("Sector")
        self.date_range_start = self._to_date(params.get("DateRangeStart"))
        self.date_range_end = self._to_date(params.get("DateRangeEnd"))
        self.type = params.get("Type")  # 'Bookings' or 'Market'
        self.min_threshold_b2c = float(params.get("MinThreshold_B2C", 0))
        self.max_threshold_b2c = float(params.get("MaxThreshold_B2C", 999999))
        self.min_threshold_b2b = float(params.get("MinThreshold_B2B", 0))
        self.max_threshold_b2b = float(params.get("MaxThreshold_B2B", 999999))
        self.carrier_inclusion = params.get("CarrierInclusion") or []
        self.carrier_exclusion = params.get("CarrierExclusion") or []
        self.b2c_distinct_fare_count = int(params.get("B2C_Distinct_Fare_Count", 60))
        self.coverage_parameter = float(params.get("CoverageParameter", 2))
        self.market_flag = str(params.get("MarketFlag", "TRUE")).strip().upper()
        self.up_b2c = int(params.get("UP_B2C", 5))
        self.up_b2b = int(params.get("UP_B2B", 4))
        self.b2b_threshold = float(params.get("B2B_Threshold", 100))
        self.yield_threshold = float(params.get("Yield_Threshold", 1.4))

        # Derived: map Type to internal fare_source_flag
        raw_type = str(self.type or 'market').strip().lower()
        self.fare_source_flag = 'esb' if raw_type == 'bookings' else 'market'

    @staticmethod
    def _to_date(val):
        """Convert datetime/date/string to date object."""
        if val is None:
            return None
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
        try:
            return datetime.strptime(str(val).strip(), '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None


class MarketListHandler:
    """Loads market list documents from MongoDB."""

    def __init__(self, mongo_db, logger, constants):
        self.db = mongo_db
        self.logger = logger
        self.constants = constants

    def load_sector_configs(self, route):
        """Load all sector rows from the appropriate collection.

        Returns:
            dict mapping sector code (str) -> MarketListConfig.
        """
        collection_name = COLLECTION_MAP.get(route)
        if not collection_name:
            raise ValueError(
                f"Unknown route '{route}'. Expected one of: {list(COLLECTION_MAP.keys())}"
            )

        raw_docs = list(self.db[collection_name].find({}, {'_id': 0}))
        if not raw_docs:
            self.logger.warning(f"No documents found in collection '{collection_name}'")
            return {}

        configs = {}
        for doc in raw_docs:
            cfg = MarketListConfig(doc)
            sector = cfg.sector
            if not sector or len(str(sector).strip()) != 6:
                self.logger.warning(f"Skipping invalid sector in {collection_name}: {sector}")
                continue
            sector = str(sector).strip().upper()
            cfg.sector = sector

            if sector in configs:
                self.logger.warning(f"Duplicate sector {sector} in {collection_name}, keeping first")
                continue

            configs[sector] = cfg

        self.logger.info(f"Loaded {len(configs)} sectors from '{collection_name}'")
        return configs
