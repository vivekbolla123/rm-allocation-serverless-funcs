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

#error constants
DATABASE_ACCESS_ERROR_MESSAGE = 'Database access error'
UPLOAD_INFO_NOT_FOUND_ERROR_MESSAGE = 'Data not found in file_upload_audit'

# Define tables that should be fully replaced vs incrementally updated
FULL_REPLACE_TABLES = [
    "config_station_master",
    "config_inputs",
    "rm_parameter_values"
]