# QPDynamicAllocation Testing Framework

This testing framework provides comprehensive testing for the `QPDynamicAllocation/update_last_sold_fare.py` script with smart connection mocking and CSV output generation.

## Overview

The `update_last_sold_fare.py` script is challenging to test because:
1. All connection strings are hardcoded in the same file
2. It connects to multiple external systems (Redis, SQL Server, MongoDB)
3. The main logic runs in the constructor
4. It processes real-time booking data

## Solution

This framework uses **runtime patching** to:
- Replace all external connections with mock objects
- Capture the final output data that would be stored in Redis
- Save the results as CSV files for analysis
- Test the core business logic without external dependencies

## Directory Structure

```
QPDynamicAllocation/testing/
├── README.md                     # This file
├── test_last_sold_fare.py       # Main test framework (run this)
├── mock_handlers.py             # Mock classes for all connections
├── inputs/                      # Mock input data
│   ├── last_sold_bookings.csv   # Mock booking data from Navitaire
│   └── rm_parameter_values.csv  # Mock filter configurations
└── output/                      # Generated test results (auto-created)
    └── last_sold_fare_output.csv # Final Redis data as CSV
```

## Setup

1. **Install Dependencies**:
   ```bash
   pip install pandas pymongo redis pymssql boto3
   ```

2. **Run Test**:
   ```bash
   cd QPDynamicAllocation/testing
   python test_last_sold_fare.py
   ```

## Input Data Files

### last_sold_bookings.csv
Mock booking data that simulates the complex SQL query results from Navitaire:
- **BookingID**: Unique booking identifier
- **BookingUTC**: Booking timestamp
- **PassengerID**: Passenger identifier
- **DepartureStation**: Origin airport code
- **ArrivalStation**: Destination airport code
- **DepartureDate**: Flight departure date
- **FlightNumber**: Flight number
- **FareBasis**: Fare basis code (M, R, C, V, Z)
- **ClassOfService**: Service class
- **Fare**: Fare amount

### rm_parameter_values.csv
Filter configurations for different fare types:
- **fare_filter_allfares**: General fare filtering rules
- **fare_filter_b2bfares**: B2B-specific filtering 
- **fare_filter_b2cfares**: B2C-specific filtering 

## Mock Framework Features

### Smart Connection Patching
- **Redis**: `MockRedisClient` captures all SET operations and simulates GET/EXISTS
- **SQL Server**: `MockSQLConnection` returns mock booking data
- **MongoDB**: `MockMongoClient` provides filter configurations
- **AWS SSM**: Mock parameter store for credentials

### Runtime Patching Strategy
```python
# Patches applied before importing the actual script
patches = [
    patch('update_last_sold_fare.client', return_value=mock_ssm_client),
    patch('update_last_sold_fare.pymssql.connect', return_value=mock_nav_connection),
    patch('update_last_sold_fare.Redis', return_value=mock_redis_client),
    patch('update_last_sold_fare.MongoClient', return_value=mock_mongo_client),
]
```

### Output Capture
The framework captures the final Redis data structure:
```json
{
  "allfares": [...],     // All qualifying fares (max 7)
  "b2bfares": [...],     // B2B fares only (max 3)
  "b2cfares": [...]      // B2C fares only (max 3)
}
```

## Output Files

### last_sold_fare_output.csv
Contains the processed fare data that would be stored in Redis:
- **redis_key**: The Redis key (e.g., "LastSoldFare:DELAMD1146_2024-01-20")
- **all_fares_count**: Number of all fares
- **b2b_fares_count**: Number of B2B fares
- **b2c_fares_count**: Number of C2C fares
- **all_fares_data**: JSON array of all fare records
- **b2b_fares_data**: JSON array of B2B fare records
- **b2c_fares_data**: JSON array of B2C fare records

## Key Testing Features

### 1. Connection Isolation
- No external dependencies required
- All connections are mocked at runtime
- Original script runs unchanged

### 2. Business Logic Testing
- Tests fare filtering logic
- Tests fare basis categorization (B2B vs B2C)
- Tests Redis key generation
- Tests data merging and deduplication

### 3. Data Validation
- Verifies fare filtering rules are applied correctly
- Ensures proper JSON structure in Redis
- Validates fare limits (7 all, 3 B2B, 3 B2C)

### 4. Comprehensive Logging
- Tracks all Redis operations
- Logs SQL query execution
- Shows data processing steps

## Fare Filtering Logic

The script applies different filters based on fare type:

### All Fares Filter
- Excludes class prefixes: ['G']
- Excludes specific classes: ['G7']
- No fare basis restrictions

### B2B Fares Filter
- Excludes class prefixes: ['G']
- Excludes specific classes: ['G7']
- Only includes fare basis: ['M', 'R']

### B2C Fares Filter
- Excludes class prefixes: ['G']
- Excludes specific classes: ['G7']
- Only includes fare basis: ['C', 'V', 'Z']

## Redis Key Structure

Keys follow the pattern: `LastSoldFare:{DepartureStation}{ArrivalStation}{FlightNumber}_{DepartureDate}`

Example: `LastSoldFare:DELAMD1146_2024-01-20`

## Troubleshooting

### Import Errors
- Ensure you're running from the `QPDynamicAllocation/testing/` directory
- Check that the parent directory contains `update_last_sold_fare.py`

### Missing Data
- Verify input CSV files exist in the `inputs/` directory
- Check CSV file formats match expected columns

### Connection Errors
- All connections are mocked - no real external systems needed
- If you see connection errors, the patching may not be working correctly

## Extending the Tests

### Adding More Test Data
1. Add more records to `last_sold_bookings.csv`
2. Include different fare basis codes
3. Add various flight routes and dates

### Testing Different Scenarios
1. Modify filter configurations in `rm_parameter_values.csv`
2. Test with empty booking data
3. Test with duplicate bookings

### Custom Assertions
Add validation logic to verify:
- Correct fare categorization
- Proper data deduplication
- Expected Redis key formats

This framework provides a complete testing environment for the LastSoldFare functionality without requiring any external system dependencies.