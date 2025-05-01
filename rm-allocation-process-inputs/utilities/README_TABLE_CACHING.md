# Table-Based Caching System

This document describes the table-based caching system implemented to optimize database access across the RM Allocation serverless functions.

## Purpose

The table-based caching system is designed to:

1. Load entire tables into memory once and filter them in-memory for subsequent queries
2. Eliminate redundant queries to the same tables
3. Improve performance by avoiding database I/O operations
4. Minimize latency when accessing the same data multiple times

## Implementation

The system consists of:

1. A central `TableCache` utility class in `utilities/table_cache_utils.py`
2. Integration with database helper classes
3. Preloading of commonly accessed tables at initialization

### TableCache

The `TableCache` class provides the core functionality:

- Load entire tables into memory when first needed
- Filter cached tables using pandas DataFrame operations
- Support custom query functions for complex filtering
- Manage the lifecycle of cached data

### Integration with Database Helpers

The table caching mechanism has been integrated with:

`RMDatabaseHelper` in `rm-allocation-process-inputs/helper/rm_database_helper.py`

### How It Works

Instead of querying the database each time data is needed, the system follows this pattern:

1. When a table is first needed, load it entirely into memory
2. For subsequent requests, filter the in-memory table using pandas operations
3. Return only the needed rows/columns without hitting the database

For example:

```python
# Instead of running this query multiple times:
# SELECT * FROM ndo_bands WHERE start <= {ndo} AND end >= {ndo}

# We load the table once:
ndo_bands_df = table_cache.load_table("ndo_bands", conn)

# Then filter it in-memory for each request:
result = ndo_bands_df[(ndo_bands_df['start'] <= ndo) & (ndo_bands_df['end'] >= ndo)]
```

## Benefits

- **Performance**: Eliminates database round-trips after initial load
- **Consistency**: Ensures all code uses the same dataset without stale reads
- **Readability**: Simplifies code by separating data loading from filtering
- **Flexibility**: Allows complex filtering operations not easily expressed in SQL

## Best Practices

When working with the table cache:

1. Only cache tables that are reasonably sized
2. For large tables, consider loading only relevant subsets (using the `condition` parameter)
3. Be mindful of memory usage in serverless environments
4. Consider the freshness requirements of your data
5. Clear the cache at appropriate times to free memory

## Example Usage

```python
from utilities.table_cache_utils import TableCache

# Initialize the cache manager
table_cache = TableCache(logger)

# Load a table (happens only once)
table_cache.load_table("time_ranges", connection)

# Filter the table without hitting the database
time_range_df = table_cache.filter_table("time_ranges", {"time_range": "TR"})

# Use a custom filtering function
result = table_cache.query_table("time_ranges",
    lambda df: df[(df['start'] <= current_time) & (df['end'] >= current_time)])

# Get statistics
stats = table_cache.get_cache_stats()
logger.info(f"Cache contains {stats['total_tables']} tables using {stats['memory_usage']} bytes")
```

## Preloading Common Tables

For best performance, common reference tables are preloaded at initialization time:

```python
# Preload commonly used tables
table_cache.load_table("ndo_bands", conn)
table_cache.load_table("dplf_bands", conn)
table_cache.load_table("time_ranges", conn)
```

## Memory Considerations

Table caching works best for:

1. Small to medium-sized reference tables
2. Tables that don't change during the execution
3. Tables that are accessed frequently

For very large tables, consider:

1. Loading only relevant subsets using the `condition` parameter
2. Using aggressive filtering to keep the memory footprint small
3. Clearing the cache when the data is no longer needed
