import pandas as pd
import logging

class QueryCacheManager:
    """
    A utility class for caching database query results.
    This helps avoid repeated queries to the same tables by storing results in memory.
    """
    
    def __init__(self, logger=None):
        """
        Initialize the cache manager.
        
        Args:
            logger: Optional logger instance. If not provided, a new one will be created.
        """
        self.query_cache = {}
        self.logger = logger if logger else logging.getLogger(__name__)
    
    def execute_cached_query(self, query, connection, query_key=None):
        """
        Execute a query and cache the result in a dataframe.
        If the query has been previously executed (based on query_key), return the cached result.
        
        Args:
            query: SQL query string to execute
            connection: Database connection to use
            query_key: Optional key to identify the query in the cache. If not provided, the query itself is used.
            
        Returns:
            DataFrame with query results
        """
        cache_key = query_key if query_key else query
        
        # Return cached result if available
        if cache_key in self.query_cache:
            self.logger.info(f"Using cached result for query: {cache_key[:50]}...")
            return self.query_cache[cache_key]
        
        # Execute query and cache the result
        try:
            self.logger.info(f"Executing new query: {cache_key[:50]}...")
            df = pd.read_sql(query, connection)
            self.query_cache[cache_key] = df
            return df
        except Exception as e:
            self.logger.error(f"Error executing query {cache_key[:50]}...: {e}")
            raise
    
    def cache_dataframe(self, df, key):
        """
        Directly cache a dataframe without executing a query.
        Useful for storing processed dataframes that might be reused.
        
        Args:
            df: DataFrame to cache
            key: Key to identify the dataframe in the cache
            
        Returns:
            The cached dataframe (for convenience)
        """
        self.query_cache[key] = df
        return df
    
    def get_cached_item(self, key):
        """
        Retrieve a cached item by key.
        
        Args:
            key: The cache key to lookup
            
        Returns:
            The cached item if found, None otherwise
        """
        return self.query_cache.get(key, None)
    
    def clear_cache(self):
        """Clear all cached items"""
        self.query_cache.clear()
    
    def remove_item(self, key):
        """
        Remove a specific item from the cache.
        
        Args:
            key: The cache key to remove
            
        Returns:
            True if the item was removed, False if it wasn't in the cache
        """
        if key in self.query_cache:
            del self.query_cache[key]
            return True
        return False
    
    def get_cache_keys(self):
        """
        Get all keys currently in the cache.
        
        Returns:
            List of all cache keys
        """
        return list(self.query_cache.keys())
    
    def get_cache_stats(self):
        """
        Get statistics about the cache contents.
        
        Returns:
            Dictionary with cache statistics
        """
        stats = {
            'total_items': len(self.query_cache),
            'memory_usage': sum(df.memory_usage(deep=True).sum() for df in self.query_cache.values() if isinstance(df, pd.DataFrame)),
            'dataframe_count': sum(1 for item in self.query_cache.values() if isinstance(item, pd.DataFrame)),
        }
        return stats 