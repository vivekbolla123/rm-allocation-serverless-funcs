import pandas as pd
import logging
import time

class TableCache:
    """
    A utility class for caching entire database tables and providing methods to filter the cached data.
    This reduces the need for multiple queries to the same table.
    """
    
    def __init__(self, logger=None):
        """
        Initialize the table cache.
        
        Args:
            logger: Optional logger instance. If not provided, a new one will be created.
        """
        self.table_cache = {}
        self.logger = logger if logger else logging.getLogger(__name__)
        
        # Performance monitoring
        self.cache_hits = 0
        self.cache_misses = 0
        self.total_query_time_saved = 0  # Estimated time saved in seconds
        self.query_times = {}  # Track initial load time for each table
    
    def load_table(self, table_name, connection, schema=None, condition=None, force_reload=False):
        """
        Load an entire table into the cache if it doesn't exist or force_reload is True.
        
        Args:
            table_name: The name of the table to load
            connection: Database connection to use
            schema: Optional schema name
            condition: Optional WHERE condition for the query
            force_reload: Force reload of the table even if it's already cached
            
        Returns:
            DataFrame containing the table data
        """
        cache_key = f"{schema}.{table_name}" if schema else table_name
        
        # Return cached table if available and not forcing reload
        if cache_key in self.table_cache and not force_reload:
            self.logger.info(f"Using cached table: {cache_key}")
            self.cache_hits += 1
            return self.table_cache[cache_key]
        
        # Build the query
        if schema:
            query = f"SELECT * FROM {schema}.{table_name}"
        else:
            query = f"SELECT * FROM {table_name}"
            
        if condition:
            query += f" WHERE {condition}"
        
        # Execute query and cache the result
        try:
            self.logger.info(f"Loading table {cache_key} into cache")
            self.cache_misses += 1
            
            # Measure loading time for performance monitoring
            start_time = time.time()
            df = pd.read_sql(query, connection)
            load_time = time.time() - start_time
            
            # Store the load time for future reference
            self.query_times[cache_key] = load_time
            
            self.table_cache[cache_key] = df
            self.logger.info(f"Loaded {len(df)} rows from {cache_key} in {load_time:.2f} seconds")
            return df
        except Exception as e:
            self.logger.error(f"Error loading table {cache_key}: {e}")
            raise
    
    def filter_table(self, table_name, filters=None, schema=None):
        """
        Filter cached table data based on conditions.
        
        Args:
            table_name: The name of the table to filter
            filters: Dictionary of column-value pairs to filter on (multiple conditions are ANDed)
            schema: Optional schema name
            
        Returns:
            Filtered DataFrame or None if table not in cache
        """
        cache_key = f"{schema}.{table_name}" if schema else table_name
        
        if cache_key not in self.table_cache:
            self.logger.warning(f"Table {cache_key} not found in cache")
            return None
        
        # Increment cache hit counter
        self.cache_hits += 1
        
        # Update estimated time saved (assume filtering is instant compared to DB query)
        if cache_key in self.query_times:
            self.total_query_time_saved += self.query_times[cache_key]
        
        df = self.table_cache[cache_key]
        
        if not filters:
            return df
        
        # Apply all filters
        filtered_df = df.copy()
        for column, value in filters.items():
            if isinstance(value, (list, tuple)):
                filtered_df = filtered_df[filtered_df[column].isin(value)]
            else:
                filtered_df = filtered_df[filtered_df[column] == value]
        
        return filtered_df
    
    def query_table(self, table_name, query_func, schema=None):
        """
        Apply a custom query function to the cached table.
        
        Args:
            table_name: The name of the table to query
            query_func: Function that takes a DataFrame and returns a filtered/transformed DataFrame
            schema: Optional schema name
            
        Returns:
            Resulting DataFrame or None if table not in cache
        """
        cache_key = f"{schema}.{table_name}" if schema else table_name
        
        if cache_key not in self.table_cache:
            self.logger.warning(f"Table {cache_key} not found in cache")
            return None
        
        # Increment cache hit counter
        self.cache_hits += 1
        
        # Update estimated time saved
        if cache_key in self.query_times:
            self.total_query_time_saved += self.query_times[cache_key]
        
        return query_func(self.table_cache[cache_key])
    
    def get_cached_tables(self):
        """
        Get a list of all tables currently in the cache.
        
        Returns:
            List of table names
        """
        return list(self.table_cache.keys())
    
    def clear_cache(self):
        """Clear all cached tables"""
        self.table_cache.clear()
        self.logger.info("Cleared all tables from cache")
    
    def remove_table(self, table_name, schema=None):
        """
        Remove a specific table from the cache.
        
        Args:
            table_name: The name of the table to remove
            schema: Optional schema name
            
        Returns:
            True if the table was removed, False if it wasn't in the cache
        """
        cache_key = f"{schema}.{table_name}" if schema else table_name
        
        if cache_key in self.table_cache:
            del self.table_cache[cache_key]
            self.logger.info(f"Removed table {cache_key} from cache")
            return True
        return False
    
    def get_cache_stats(self):
        """
        Get statistics about the cache contents.
        
        Returns:
            Dictionary with cache statistics
        """
        stats = {
            'total_tables': len(self.table_cache),
            'total_rows': sum(len(df) for df in self.table_cache.values()),
            'memory_usage': sum(df.memory_usage(deep=True).sum() for df in self.table_cache.values()),
            'tables': {name: len(df) for name, df in self.table_cache.items()},
            'performance': {
                'cache_hits': self.cache_hits,
                'cache_misses': self.cache_misses,
                'hit_ratio': self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0,
                'estimated_time_saved': f"{self.total_query_time_saved:.2f} seconds",
                'load_times': {table: f"{time:.2f} seconds" for table, time in self.query_times.items()}
            }
        }
        return stats
    
    def log_performance_report(self):
        """
        Log a performance report showing cache effectiveness.
        """
        stats = self.get_cache_stats()
        performance = stats['performance']
        
        self.logger.info("=" * 60)
        self.logger.info("TABLE CACHE PERFORMANCE REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Total cache hits: {performance['cache_hits']}")
        self.logger.info(f"Total cache misses: {performance['cache_misses']}")
        self.logger.info(f"Cache hit ratio: {performance['hit_ratio']:.2%}")
        self.logger.info(f"Estimated time saved: {performance['estimated_time_saved']}")
        self.logger.info(f"Total tables cached: {stats['total_tables']}")
        self.logger.info(f"Total rows cached: {stats['total_rows']}")
        self.logger.info(f"Total memory usage: {stats['memory_usage'] / (1024*1024):.2f} MB")
        
        if stats['tables']:
            self.logger.info("-" * 60)
            self.logger.info("TABLE SIZES:")
            for table, size in sorted(stats['tables'].items(), key=lambda x: x[1], reverse=True):
                self.logger.info(f"  {table}: {size} rows, loaded in {self.query_times.get(table, 0):.2f}s")
        
        self.logger.info("=" * 60) 