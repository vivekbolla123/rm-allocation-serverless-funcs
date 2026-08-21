"""
localtest/schema.py
=============================================================================
The contract between the mock CSVs and the two local stores.

Every dataset the additive pickup model reads is declared here exactly once:
which CSV holds it, whether it lands in MySQL or MongoDB, its columns, and
which of those columns are dates.

MySQL side (the ESB schema the production SQL queries verbatim):

    hist_flown_data.csv  -> QP_DL_FLOWNDATAPAX    historical flown pax
    booked_data.csv      -> QP_DL_BOOKEDDATAPAX   forward bookings
    inventory_leg.csv    -> QP_DL_InventoryLeg    flight master / schedule

MongoDB side:

    fares.csv               -> <db>.fares                              (fare buckets)
    rm_parameter_values.csv -> <db>.rm_parameter_values                 (pricing constants)
    active_sectors.csv      -> <master_db>.active_sectors
    tax_mast_dom.csv        -> <master_db>.tax_mast_dom
    tax_mast_intl.csv       -> <master_db>.tax_mast_intl
    demand_multiplier.csv   -> <master_db>.config_sector_demand_multiplier
    wtp.csv                 -> <master_db>.config_quarter_wtp

Optional CSV overrides (absent by default — the model computes them):

    fare_buckets.csv     replaces step 1  (GP fare bucket computation)
    tagging.csv          replaces step 4  (historical Z-score lean/peak tagging)

Model outputs — exactly two, each one file covering every sector. Only the
weighted branch is kept; the normal branch is still computed, just not persisted.
The EMSR-b results (protection limits, AU, promo row) ride along on the
fare-bucket file rather than being a third output.

    add_model_flight_level_weighted           flight level, weighted
    add_model_fare_bucket_level_weighted      fare bucket level, weighted,
                                              including the EMSR-b AU ladder
"""

# =============================================================================
# MySQL: table names (unqualified — the schema comes from LocalConfig)
# =============================================================================

TBL_FLOWN = 'QP_DL_FLOWNDATAPAX'
TBL_BOOKED = 'QP_DL_BOOKEDDATAPAX'
TBL_INVENTORY_LEG = 'QP_DL_InventoryLeg'


# `InventoryLegID` is the PRIMARY KEY of QP_DL_InventoryLeg on purpose: the
# production forward query selects fm.AdjustedCapacity while grouping on
# fm.Capacity, which MySQL 8 only accepts under ONLY_FULL_GROUP_BY when the
# grouped set is functionally dependent on a primary key.
DDL = {
    TBL_FLOWN: f"""
        CREATE TABLE IF NOT EXISTS `{TBL_FLOWN}` (
            `Origin`                 CHAR(3)        NOT NULL,
            `Destination`            CHAR(3)        NOT NULL,
            `DepartureDate`          DATE           NOT NULL,
            `BookingDate`            DATE           NOT NULL,
            `FlightNumber`           INT            NOT NULL,
            `InventoryLegID`         BIGINT         NOT NULL,
            `PNR`                    VARCHAR(12)    NOT NULL,
            `RBD`                    VARCHAR(4)     NOT NULL,
            `FareBasis`              VARCHAR(24)    NOT NULL,
            `FB_Net`                 DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `YQ_Pro`                 DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `RCS`                    DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `CUTE`                   DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `DiscountedBaseFare_Pro` DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `AdjustedCapacity`       INT            NOT NULL DEFAULT 0,
            `Distance`               INT            NOT NULL DEFAULT 0,
            KEY `ix_flown_sector_date` (`Origin`, `Destination`, `DepartureDate`),
            KEY `ix_flown_leg` (`InventoryLegID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    TBL_BOOKED: f"""
        CREATE TABLE IF NOT EXISTS `{TBL_BOOKED}` (
            `InventoryLegID`         BIGINT         NOT NULL,
            `DepartureDate`          DATE           NOT NULL,
            `BookingDate`            DATE           NOT NULL,
            `PNR`                    VARCHAR(12)    NOT NULL,
            `RBD`                    VARCHAR(4)     NOT NULL,
            `FareBasis`              VARCHAR(24)    NOT NULL,
            `FB_Net`                 DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `YQ_Pro`                 DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `RCS`                    DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `CUTE`                   DECIMAL(12,2)  NOT NULL DEFAULT 0,
            `DiscountedBaseFare_Pro` DECIMAL(12,2)  NOT NULL DEFAULT 0,
            KEY `ix_booked_leg_bdate` (`InventoryLegID`, `BookingDate`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    TBL_INVENTORY_LEG: f"""
        CREATE TABLE IF NOT EXISTS `{TBL_INVENTORY_LEG}` (
            `InventoryLegID`    BIGINT      NOT NULL,
            `DepartureStation`  CHAR(3)     NOT NULL,
            `ArrivalStation`    CHAR(3)     NOT NULL,
            `STD`               DATETIME    NOT NULL,
            `FlightNumber`      INT         NOT NULL,
            `AdjustedCapacity`  INT         NOT NULL DEFAULT 0,
            `Capacity`          INT         NOT NULL DEFAULT 0,
            `DepartureDate`     DATE        NOT NULL,
            `CarrierCode`       CHAR(2)     NOT NULL DEFAULT 'QP',
            `Status`            INT         NOT NULL DEFAULT 1,
            PRIMARY KEY (`InventoryLegID`),
            KEY `ix_leg_station_date` (`DepartureStation`, `ArrivalStation`, `DepartureDate`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
}


# csv file -> (mysql table, date columns, datetime columns)
MYSQL_DATASETS = {
    'hist_flown_data.csv': (TBL_FLOWN, ['DepartureDate', 'BookingDate'], []),
    'booked_data.csv': (TBL_BOOKED, ['DepartureDate', 'BookingDate'], []),
    'inventory_leg.csv': (TBL_INVENTORY_LEG, ['DepartureDate'], ['STD']),
}


# =============================================================================
# MongoDB: csv file -> (which db, collection, date columns)
#   'dw'     -> LocalConfig.mongo_db          (qp_dw_rmalloc equivalent)
#   'master' -> LocalConfig.mongo_master_db   (qp_rm_master equivalent)
# =============================================================================

MONGO_DATASETS = {
    'fares.csv': ('dw', 'fares', []),
    'rm_parameter_values.csv': ('dw', 'rm_parameter_values', []),
    'active_sectors.csv': ('master', 'active_sectors', []),
    'tax_mast_dom.csv': ('master', 'tax_mast_dom', []),
    'tax_mast_intl.csv': ('master', 'tax_mast_intl', []),
    # DepartureDate is intentionally left as the raw string: the production
    # normaliser parses it with dayfirst=True, so we must not reformat it here.
    'demand_multiplier.csv': ('master', 'config_sector_demand_multiplier', []),
    'wtp.csv': ('master', 'config_quarter_wtp', []),
}


# =============================================================================
# Optional CSV overrides for computed steps
# =============================================================================

OVERRIDE_FARE_BUCKETS_CSV = 'fare_buckets.csv'   # cols: Sector, FareBasis
OVERRIDE_TAGGING_CSV = 'tagging.csv'             # cols: Sector, DepartureDate,
                                                 #       FlightNumber, Time Cluster, Tagging


# =============================================================================
# Output datasets
#
# The local flow emits exactly two: the weighted flight level and the weighted
# fare bucket level, each a single file covering every sector.
#
# The EMSR-b frame is NOT a separate output. It is the weighted fare-bucket frame
# plus the optimization results (OptimizationDemand, ProtectionLimit, AU, RBD,
# OpeningFare*) and one promo row per flight — same grain, superset of columns. So
# the optimized frame IS the fare-bucket output, and the pre-optimization copy is
# dropped.
#
# Keys match the `s3_dataset` values in
# AdditivePickupModelProcessor._save_results. Anything absent from these maps is
# skipped, which is how the normal (unweighted) branch stays unpersisted.
# =============================================================================

OUTPUT_COLLECTIONS = {
    'flight_level_weighted': 'add_model_flight_level_weighted',
    'fare_bucket_weighted': 'add_model_fare_bucket_level_weighted',
}

# Emitted under one name, persisted under another.
DATASET_ALIASES = {
    'inventory_optimization': 'fare_bucket_weighted',
}

# Emitted but discarded, because an aliased dataset supersedes it. Checked before
# the aliases so the un-optimized fare-bucket frame is dropped while the optimized
# one takes its place.
SUPERSEDED_DATASETS = {
    'fare_bucket_weighted',
}

# Prefix used by every output collection. clear_outputs() drops the whole family
# so collections left behind by an earlier configuration get cleaned up too.
OUTPUT_COLLECTION_PREFIX = 'add_model_'


def all_input_csvs():
    """Every CSV the seeder knows how to load, in seed order."""
    return list(MYSQL_DATASETS) + list(MONGO_DATASETS)
