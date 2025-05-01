# Table-Based Query Optimization Summary

## Overview

This document summarizes the changes made to optimize database queries by implementing table-level caching throughout the RM Allocation serverless functions. Instead of repeatedly querying the same tables with different filter conditions, we now load entire tables into memory once and filter them in-memory for subsequent operations.

## Key Components Implemented

1. **TableCache Utility Class**

   - Located at `utilities/table_cache_utils.py`
   - Manages loading and caching of entire database tables
   - Provides filtering operations on cached tables
   - Includes performance monitoring capabilities

2. **Integration with Database Helpers**

   - Updated `RMDatabaseHelper` to use table caching
   - Updated `NavDatabaseHelper` to use table caching when available
   - Added fallback mechanisms for backward compatibility

3. **Performance Monitoring**
   - Added tracking of cache hits and misses
   - Implemented time saved estimation
   - Created performance reporting methods

## Optimized Methods

The following methods were modified to use table caching instead of repeated queries:

### RMDatabaseHelper

- `getNdoBand` - Filters the cached ndo_bands table
- `getdpflBand` - Filters the cached dplf_bands table
- `getTimeRange` - Filters the cached time_ranges table
- `get_current_rbds_by_class` - Filters the cached current_rbd table
- `get_series_seats` - Filters the cached series_booked table
- `getSeriesBlockedSeats` - Filters the cached series_blocking table
- `get_tbf_discount` - Filters the cached tbf_discount_grid table
- `fareExtract` - Filters the cached Fares table
- `getLastSellingFare` - Filters the cached booking_fare_data table
- `getNumberOfBookings` - Filters the cached booking_fare_data table
- `getRDBTable` - Processes and filters the cached config_determine_fares table
- `fetch_all_conversion_data` - Returns the cached currency_conversion table

### NavDatabaseHelper

- `getCurrentBookedLoads` - Filters the cached bookload table
- Added methods to track and report performance statistics

## Automatic Table Preloading

To maximize performance benefits, commonly accessed tables are automatically loaded at initialization:

```python
# Common reference tables
self.table_cache.load_table("ndo_bands", self.conn)
self.table_cache.load_table("dplf_bands", self.conn)
self.table_cache.load_table("time_ranges", self.conn)
self.table_cache.load_table("config_profile_fares", self.conn)
self.table_cache.load_table("currency_conversion", self.dwconn)
self.table_cache.load_table("tbf_discount_grid", self.conn)
self.table_cache.load_table("series_blocking", self.conn)
self.table_cache.load_table("config_ownFares", self.conn)
self.table_cache.load_table("config_determine_fares", self.conn)
```

## Performance Benefits

The table caching system provides several benefits:

1. **Reduced Database I/O**: Each table is loaded only once per execution
2. **Improved Response Time**: In-memory filtering is much faster than database queries
3. **Lower Database Load**: Fewer queries means less load on the database server
4. **Consistent Data View**: All operations use the same dataset, avoiding timing inconsistencies
5. **Memory Efficiency**: Tables are stored as pandas DataFrames which are optimized for memory usage

## Performance Monitoring

Performance statistics are logged at the end of each execution, including:

- Number of cache hits and misses
- Cache hit ratio
- Estimated time saved
- Memory usage statistics
- Detailed information on table sizes and load times

## Implementation Approach

1. **Incremental Adoption**: Existing query-level caching can coexist with table-level caching
2. **Fallback Mechanisms**: If a table is too large, conditional loading is used
3. **Backward Compatibility**: Systems without the new utils can still use the older caching
4. **Smart Preloading**: Only load tables that are likely to be needed based on input parameters

## Best Practices For Using Table Cache

1. Only cache tables that are small to medium-sized
2. For larger tables, use conditions to load only necessary subsets
3. Clear the cache when appropriate to free memory
4. Use the performance reporting to identify which tables benefit most from caching
5. Balance between memory usage and query optimization
