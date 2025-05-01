# Query Caching System

This document describes the query caching system implemented to optimize database access across the RM Allocation serverless functions.

## Purpose

The query caching system is designed to:

1. Reduce redundant database queries by caching the results in memory
2. Improve performance of the allocation functions
3. Minimize database load by reusing data that has already been fetched
4. Provide a consistent caching mechanism across all components

## Implementation

The system consists of:

1. A central `QueryCacheManager` utility class
2. Integration with existing database helper classes
3. Consistent caching mechanism across different modules

### QueryCacheManager

The `QueryCacheManager` class (in `utilities/query_cache_utils.py`) provides the core functionality:

- Cache database query results in memory
- Track and manage cached data
- Provide utilities for cache management
- Offer statistics about cache usage

### Integration with Database Helpers

The caching mechanism has been integrated with:

1. `RMDatabaseHelper` in `rm-allocation-process-inputs/helper/rm_database_helper.py`
2. `NavDatabaseHelper` in `rm-allocation-fetch-inputs/lambda_function.py`
3. `QCProcess` in `rm-allocation-scheduled-jobs/qcTableGen.py`

### How It Works

Instead of directly calling `pd.read_sql()` or similar functions, the code now uses:

```python
result_df = self.execute_cached_query(query, connection, cache_key)
```

The system will:

1. Check if the result is already in the cache using the cache_key
2. If found, return the cached result without hitting the database
3. If not found, execute the query, cache the result, and return it

## Benefits

- **Performance**: Reduces redundant database calls, improving performance
- **Resource Usage**: Minimizes database load by reusing fetched data
- **Consistency**: Ensures consistent data across different parts of the application using the same queries
- **Memory Management**: The cache can be cleared or managed as needed

## Best Practices

When working with the query cache:

1. Always use a specific cache key for each unique query
2. For parameterized queries, include the parameters in the cache key
3. Consider the memory implications of caching large datasets
4. Clear the cache when appropriate (e.g., after processing is complete)
5. Use the cache statistics to monitor memory usage

## Example Usage

```python
from utilities.query_cache_utils import QueryCacheManager

# Initialize the cache manager
cache_manager = QueryCacheManager(logger)

# Execute a query with caching
results = cache_manager.execute_cached_query(
    "SELECT * FROM table WHERE condition = 'value'",
    database_connection,
    "table_with_condition_value"
)

# Cache a processed dataframe
processed_df = process_data(results)
cache_manager.cache_dataframe(processed_df, "processed_table_with_condition_value")

# Get cache statistics
stats = cache_manager.get_cache_stats()
logger.info(f"Cache contains {stats['total_items']} items using {stats['memory_usage']} bytes")
```

## Maintenance and Troubleshooting

If you encounter memory issues:

1. Check the cache statistics to see memory usage
2. Clear the cache after processing large datasets
3. Consider segmenting the cache by removing specific items when no longer needed
4. For Lambda functions, consider clearing the cache before returning to free memory
