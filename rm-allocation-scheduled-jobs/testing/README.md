# Testing Setup for RM Allocation Scheduled Jobs

This testing directory contains optimized mock data and test configurations for local testing of the four main scheduled job scripts:

1. `defaultMarketlist.py` - Creates default market list entries for unmatched flights
2. `updateImpForecast.py` - Updates imputed forecast PLF values
3. `schedule30.py` - Manages flight booking threshold monitoring
4. `heuristicCalc.py` - Holiday-based fare projections and upsell calculations

**Key Approach**: Uses the actual scripts with only a modified `lambda_function.py` for testing. All configuration files are used as-is from `rm-allocation-configurations`. Mock data has been optimized to include only columns actually used by the code.

## Directory Structure

```
testing/
├── README.md                          # This file
├── inputs/                            # Optimized CSV files with only used columns
│   ├── navitaire_flights.csv         # 6 columns: Origin, Destination, DepartureDate, FlightNumber, DepartureTime, DepartureTimeUTC
│   ├── market_list.csv               # 52 columns: Full MongoDB structure with all used fields
│   ├── market_list_international.csv # 52 columns: Full MongoDB structure with all used fields
│   ├── config_user_sector_map.csv    # 2 columns: sector, user1
│   ├── config_station_master.csv     # 2 columns: station, route
│   ├── forecast_results.csv          # 6 columns: forecastPLF, Variance, Origin, Destination, FlightNumber, DepartureDate
│   ├── curves.csv                    # 3 columns: CurveID, NDO, LF
│   ├── config_share_of_booking.csv   # 2 columns: hours, shareOfBooking
│   ├── booked_loads.csv              # 7 columns: Origin, Destination, FlightNumber, DepartureDate, adjustedCapacity, bookedPax, DepartureTime
│   ├── flight_bookings.csv           # 5 columns: FlightNumber, DepartureStation, ArrivalStation, DepartureDate, TotalBookings
│   ├── config_column_names.csv       # 2 columns: tableName, columns
│   ├── calendar_af.csv               # 3 columns: Sector, DepartureDate, AverageFare (for heuristicCalc)
│   ├── config_holiday_list.csv       # 2 columns: Holiday, Date (for heuristicCalc)
│   ├── config_date_event.csv         # 4 columns: date, DOW, mappedMonth, event (for heuristicCalc)
│   ├── config_profile_fares.csv      # 26 columns: sector, month, dow, cluster, Q0-Q20, startTime, endTime, currency (for heuristicCalc)
│   ├── config_profile_fare_station_upsell.csv # 4 columns: date, sector, value, mode (for heuristicCalc)
│   └── flight_tracker_cache.csv      # 6 columns: flight_key, last_check_time, total_bookings, previous_bookings, last_threshold_date, last_threshold_bookings (for schedule30)
├── output/                           # Output CSV files (auto-created)
├── test_lambda_function.py          # Test runner with local MongoDB (ONLY modified file)
├── setup_test_data.py               # Script to load mock data into local MongoDB
├── mock_handlers.py                 # Mock classes for database and S3 operations
└── analyze_results.py               # Script to analyze test results and export data
```

## Mock Data Optimization

All mock data files have been analyzed and optimized to include **only the columns actually used** in the code:

### Key Optimizations:
- **config_station_master.csv**: Reduced from 5 columns to 2 (only `station` and `route` are used)
- **config_user_sector_map.csv**: Reduced from 4 columns to 2 (only `sector` and `user1` are used)
- **curves.csv**: Uses 3 columns (`CurveID`, `NDO`, `LF`) with realistic curve IDs like `C17440_40`
- **config_share_of_booking.csv**: Uses 2 columns (`hours`, `shareOfBooking`) for time-based distribution
- **Market list files**: Include all 52 columns matching the actual MongoDB structure you provided
- **Data consistency**: Flight numbers and routes are consistent across all related files
- **Flight tracker cache**: `flight_tracker_cache.csv` provides realistic Redis cache data for schedule30 testing

### Data Types and Values:
- **Flight numbers**: Realistic values (1146, 1147, 1001, 1002, etc.)
- **Curve IDs**: Match actual patterns (`C17440_40`, `C17441_41`, etc.)
- **Date formats**: Proper MM/DD/YYYY format for PerStart/PerEnd fields
- **Numeric values**: Appropriate data types (integers for counts, floats for percentages)
- **String values**: Proper analyst names, station codes, and UUIDs

## Setup Instructions

1. **Install Dependencies**:
   ```bash
   # Install required packages
   pip install -r requirements.txt
   
   # Or install individually
   pip install pymongo pandas numpy pytz redis requests
   ```

2. **Start Local MongoDB**:
   ```bash
   # Using Docker
   docker run -d -p 27017:27017 --name test-mongo mongo:latest
   
   # Or install MongoDB locally and start the service
   ```

3. **Load Mock Data**:
   ```bash
   cd rm-allocation-scheduled-jobs/testing
   python setup_test_data.py
   ```

4. **Run All Tests**:
   ```bash
   # Option 1: Use the run_tests.py script (recommended)
   cd rm-allocation-scheduled-jobs/testing
   python run_tests.py
   
   # Option 2: Run directly from the testing directory
   cd rm-allocation-scheduled-jobs/testing
   python test_lambda_function.py
   ```

5. **Run Individual Tests**:
   ```bash
   # Using run_tests.py (recommended)
   cd rm-allocation-scheduled-jobs/testing
   python run_tests.py --module default_ml
   python run_tests.py --module imputed_forecast
   python run_tests.py --module schedule_thirty
   python run_tests.py --module heuristic_calc
   
   # Or run directly
   cd rm-allocation-scheduled-jobs/testing
   python test_lambda_function.py --module default_ml
   python test_lambda_function.py --module imputed_forecast
   python test_lambda_function.py --module schedule_thirty
   python test_lambda_function.py --module heuristic_calc
   ```

6. **Analyze Results**:
   ```bash
   # Export MongoDB data and analyze output files
   python analyze_results.py
   ```

## Key Features

### Uses Actual Scripts
- **No code duplication**: Uses the original `defaultMarketlist.py`, `updateImpForecast.py`, and `schedule30.py`
- **Real configuration**: Uses actual `configsettings.py` and `Constants.py` from `rm-allocation-configurations`
- **Only modification**: Custom `test_lambda_function.py` that mocks external connections

### Smart Mocking
- **MongoDB**: Connects to local MongoDB instance
- **SQL Connections**: Mocked to return data from CSV files
- **Redis Cache**: In-memory dictionary for testing
- **S3 Operations**: Results saved to local CSV files in `output/` directory
- **Lambda Invocations**: Mocked with local function calls

### Optimized Mock Data
- **Column analysis**: Each CSV file contains only columns actually used in the code
- **Realistic values**: Data matches actual MongoDB document structure and types
- **Consistent relationships**: Flight numbers and routes are consistent across files
- **Proper data types**: Integers, floats, and strings match expected MongoDB types

### Output Files
The scripts will generate output files in the `output/` directory:
- Results from `defaultMarketlist.py` → MongoDB collections (can be exported)
- Results from `updateImpForecast.py` → CSV file with calculated PLF values
- Results from `schedule30.py` → CSV file with market list entries and flight tracking data
- Results from `heuristicCalc.py` → CSV file with holiday-based fare projections and upsell calculations

## Column Usage Analysis

Based on code analysis, here's what columns are actually used:

### config_station_master.csv (2/5 columns used)
- ✅ `station` - Used for route matching
- ✅ `route` - Used to determine domestic vs international
- ❌ `city`, `country`, `timezone` - Not used in any script

### config_user_sector_map.csv (2/4 columns used)
- ✅ `sector` - Used for analyst assignment
- ✅ `user1` - Used as primary analyst
- ❌ `user2`, `user3` - Not used in any script

### curves.csv (3/3 columns used)
- ✅ `CurveID` - Used for curve matching
- ✅ `NDO` - Used for days-out calculations
- ✅ `LF` - Used for load factor calculations

### config_share_of_booking.csv (2/2 columns used)
- ✅ `hours` - Used for time-based calculations
- ✅ `shareOfBooking` - Used for booking distribution

## Testing Individual Scripts

Each script can be tested independently:

```bash
# Change to the testing directory first
cd rm-allocation-scheduled-jobs/testing

# Test defaultMarketlist only
python run_tests.py --module default_ml
# or
python test_lambda_function.py --module default_ml

# Test updateImpForecast only  
python run_tests.py --module imputed_forecast
# or
python test_lambda_function.py --module imputed_forecast

# Test schedule30 only
python run_tests.py --module schedule_thirty
# or
python test_lambda_function.py --module schedule_thirty

# Test heuristicCalc only
python run_tests.py --module heuristic_calc
# or
python test_lambda_function.py --module heuristic_calc

# Test all modules
python run_tests.py --module all
# or simply
python run_tests.py
```

## Data Validation

The optimized mock data ensures:
- **Consistency**: Flight numbers match across all related files
- **Realism**: Uses actual curve ID patterns and realistic values
- **Completeness**: All required fields for each script are present
- **Efficiency**: No unused columns to reduce memory usage and improve performance