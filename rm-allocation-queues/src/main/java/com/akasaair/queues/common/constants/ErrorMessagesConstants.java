package com.akasaair.queues.common.constants;

public interface ErrorMessagesConstants {
    String INVALID_DATA_ERROR_MESSAGE = "Invalid Data has been inserted";
    String INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE = "Invalid Number of Rows or columns";
    String INVALID_TABLE_NAME_ERROR_MESSAGE = "Invalid table Name";
    String DATABASE_ACCESS_ERROR_MESSAGE = "Database access error";
    String OBJECT_ID_NOT_FOUND = "Object ID not found in DB, Stopping upload.";
    String FILE_UPLOAD_ERROR_MESSAGE = "Error in the uploaded file";
    public static final String INVALID_SALES_DATE_RANGE_ERROR = "'end_date' cannot be before 'start_date'."; // Existing
    public static final String INVALID_SALES_DATE_FORMAT_ERROR = "Invalid date format. Expected MM/dd/yyyy."; // Existing
    public static final String INVALID_SALES_NUMBER_FORMAT_ERROR = "must be a valid number."; // Existing
    public static final String INVALID_SALES_MONTH_YEAR_FORMAT_ERROR = "Invalid 'month' format. Expected MMMM_yyyy (e.g., March_2025)."; // New
    public static final String INSUFFICIENT_COLUMNS_ERROR_MESSAGE = "Insufficient columns for the detected format."; // New/Refined
    public static final String UNKNOWN_SALES_INVENTORY_CSV_FORMAT_ERROR = "Unable to determine Sales Inventory CSV format from headers. Please check column names."; // New
    public static final String MISSING_SALES_HEADER_ERROR_PREFIX = "CSV is missing one or more required headers for sales inventory: ";
    public static final String MISSING_SI_RBD_HEADERS_ERROR = "Required headers 'seats' (at column 1) and 'type' (at column 2) not found in Sales RBD Distribution CSV.";
    public static final String NO_RBD_COLUMNS_FOUND_ERROR = "No RBD columns found after 'seats' and 'type' (from column 3 onwards) in Sales RBD Distribution CSV.";
    public static final String INVALID_SI_RBD_SEATS_ERROR = "Invalid 'seats' value. Must be a non-negative integer.";
    public static final String EMPTY_SI_RBD_TYPE_ERROR = "The 'type' value cannot be empty.";
    public static final String INVALID_SI_RBD_TYPE_VALUE_ERROR_PREFIX = "Invalid 'type' value: ";
    public static final String DUPLICATE_PK_SI_RBD_ERROR_PREFIX = "Duplicate primary key (seats, type, rbd) detected in CSV data for: ";// Append actual value and allowed list
    public static final String INVALID_SI_RBD_RBD_VALUE_ERROR = "Invalid value for RBD column. Must be a non-negative integer.";
    public static final String ERROR_PROCESSING_FILE = "Error Processing File. Please check if all required columns are added";
}
