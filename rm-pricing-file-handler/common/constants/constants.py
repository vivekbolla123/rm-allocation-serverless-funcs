# normal constants
UPLOAD_QUEUE_STATUS_IN_PROGRESS = "IN-PROGRESS"
UPLOAD_QUEUE_STATUS_DONE = "DONE"
UPLOAD_QUEUE_STATUS_FAILED = "FAILED"
UPLOAD_QUEUE_STATUS_PARTIALLY_UPLOADED = "PARTIALLY UPLOADED"

COMMA = ','


# table/column name constants
SALE_DISCOUNTS_TABLE = "config_sale_discounts"
SALE_DATES_TABLE = "config_sale_dates"
SAVER_TABLE = "saver"
PRODUCT_UPSELL_TABLE = "product_upsell"
MARKET_LIST_TABLE = "market_list"
MARKET_LIST_INTERNATIONAL_TABLE = "market_list_international"
MARKET_LIST_CONNECTIONS_TABLE = "market_list_connections"
FOOTNOTE_MASTER_TABLE = "config_footnote_master"
MARKET_LIST_TABLES = [MARKET_LIST_TABLE, MARKET_LIST_INTERNATIONAL_TABLE, MARKET_LIST_CONNECTIONS_TABLE]

#error constants
DATABASE_ACCESS_ERROR_MESSAGE = 'Database access error'
UPLOAD_INFO_NOT_FOUND_ERROR_MESSAGE = 'Data not found in file_upload_audit'

LIVE_TARIFF_SHEET_TABLE = "live_tariff_sheet"

# Define tables that should be fully replaced vs incrementally updated
FULL_REPLACE_TABLES = [
    "config_station_master",
    "config_inputs",
    "rm_parameter_values"
]