# Query Caching System

The Snowflake Cost Optimizer includes a sophisticated caching system to improve performance by avoiding expensive re-execution of queries.

## 🚀 **How It Works**

The caching system automatically caches query results based on:
- **Query content** (normalized SQL)
- **Query parameters** (days, limits, etc.)
- **Time-to-Live (TTL)** settings per query type

## ⏰ **Cache TTL by Query Type**

Different types of queries have different cache durations based on how frequently the data changes:

| Query Type | TTL | Reason |
|------------|-----|--------|
| **Warehouse Usage** | 30 minutes | Changes frequently throughout the day |
| **Query History** | 1 hour | Moderately dynamic, new queries added regularly |
| **Storage Usage** | 6 hours | Changes slowly, table sizes don't change often |
| **User Access Patterns** | 24 hours | Very stable, permissions change infrequently |
| **Cost Analysis** | 2 hours | Important for accuracy but not real-time |

## 💾 **Cache Storage**

- **Location**: `.cache/query_cache/` directory
- **Format**: Pickled pandas DataFrames (`.pkl` files)
- **Index**: JSON index file tracking metadata
- **Automatic Cleanup**: Expired entries cleaned on startup

## 🎯 **Benefits**

### **Performance Improvements**
- **Warehouse Usage**: ~10-30 seconds → instant
- **Storage Analysis**: ~30-60 seconds → instant  
- **Query History**: ~15-45 seconds → instant
- **User Access**: ~20-40 seconds → instant

### **Cost Savings**
- Reduces Snowflake compute credits usage
- Fewer queries against account_usage views
- Lower data transfer costs

### **User Experience**
- Faster dashboard loading
- Responsive navigation between pages
- Better perceived performance

## 🔧 **Cache Management**

### **Automatic Management**
- Expired entries cleared on app startup
- Invalid cache files automatically removed
- TTL-based expiration per query type

### **Manual Controls** (in Sidebar)
- **Cache Statistics**: View current cache status
- **Clear Expired**: Remove only expired entries
- **Clear All**: Remove all cached data
- **Refresh Data**: Force refresh current page

### **Cache Statistics**
- Total cached queries
- Valid vs expired entries  
- Total cache size in MB
- Cache directory location

## 🔄 **Cache Key Generation**

Cache keys are generated using:
```python
# Query normalization
normalized_query = ' '.join(query.lower().strip().split())

# Parameters serialization  
params_str = json.dumps(params, sort_keys=True)

# MD5 hash for unique key
cache_key = hashlib.md5(f"{normalized_query}:{params_str}".encode()).hexdigest()
```

## ⚙️ **Configuration**

### **Enable/Disable Caching**
```python
# Enable caching (default)
connector = SnowflakeConnector(settings, enable_cache=True)

# Disable caching
connector = SnowflakeConnector(settings, enable_cache=False)
```

### **Force Refresh**
```python
# Bypass cache for this query
result = connector.execute_query(
    query, 
    force_refresh=True
)
```

### **Custom TTL**
```python
# Use custom cache TTL
result = connector.execute_query(
    query,
    cache_key_type='custom',
    cache_params={'custom_param': 'value'}
)
```

## 📊 **Monitoring**

### **Cache Hit Rate**
Monitor cache effectiveness through:
- Streamlit sidebar cache statistics
- Log messages showing cache hits/misses
- Query execution time comparisons

### **Log Messages**
```
DEBUG: Cache hit for query (key: a1b2c3d4...)
DEBUG: Cached query result (key: a1b2c3d4..., size: 150 rows)
DEBUG: Query executed in 23.45s, returned 150 rows
INFO: Cleared 5 expired cache entries
```

## 🛠️ **Troubleshooting**

### **Cache Not Working**
1. Check if caching is enabled in connector
2. Verify cache directory is writable
3. Check log messages for errors

### **Stale Data**
1. Use "Clear All" to force refresh
2. Check TTL settings for query type
3. Use force_refresh parameter

### **Cache Size Issues**
1. Monitor cache size in statistics
2. Clear expired entries regularly
3. Adjust TTL settings if needed

### **Permission Errors**
1. Ensure cache directory is writable
2. Check file system permissions
3. Clear cache directory manually if needed

## 🚀 **Best Practices**

1. **Let TTL work**: Don't clear cache unnecessarily
2. **Monitor size**: Large result sets use more cache space
3. **Use force_refresh**: When you need guaranteed fresh data
4. **Regular cleanup**: Clear expired entries periodically
5. **Monitor performance**: Watch for cache hit rates

## 🔮 **Future Enhancements**

- Cache hit rate metrics
- Configurable TTL per query type
- Memory-based caching option
- Distributed cache support
- Cache warming strategies
- Intelligent cache invalidation 