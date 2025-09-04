/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <functional>
#include <optional>

// Forward declarations
struct pg_result;

namespace sonet {
namespace database {

// Cache entry configuration
struct CacheConfig {
    size_t max_cache_size = 1000;           // Maximum number of cached queries
    size_t max_result_size = 1024 * 1024;  // Maximum size of cached result in bytes
    std::chrono::minutes default_ttl{30};   // Default time-to-live for cache entries
    bool enable_compression = true;         // Enable result compression
    double hit_rate_threshold = 0.8;        // Minimum hit rate to keep cache enabled
    size_t cleanup_interval_seconds = 300;  // Cache cleanup interval
};

// Cache entry metadata
struct CacheEntry {
    std::string query_hash;
    std::string query_type;
    std::string table_name;
    std::vector<std::string> parameters;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_accessed;
    std::chrono::system_clock::time_point expires_at;
    size_t access_count;
    size_t result_size;
    bool is_valid;
    
    // Constructor
    CacheEntry() : access_count(0), result_size(0), is_valid(true) {}
    
    // Check if entry is expired
    bool is_expired() const {
        return std::chrono::system_clock::now() > expires_at;
    }
    
    // Check if entry should be evicted (LRU + TTL)
    bool should_evict() const {
        return is_expired() || !is_valid;
    }
    
    // Update access time and count
    void touch() {
        last_accessed = std::chrono::system_clock::now();
        access_count++;
    }
};

// Query cache interface
class QueryCache {
public:
    explicit QueryCache(const CacheConfig& config = {});
    ~QueryCache();
    
    // Prevent copying
    QueryCache(const QueryCache&) = delete;
    QueryCache& operator=(const QueryCache&) = delete;
    
    // Cache operations
    void put(const std::string& query_hash,
             const std::string& query_type,
             const std::string& table_name,
             const std::vector<std::string>& parameters,
             std::unique_ptr<pg_result> result,
             std::chrono::minutes ttl = std::chrono::minutes{0});
    
    std::optional<std::unique_ptr<pg_result>> get(const std::string& query_hash,
                                                  const std::vector<std::string>& parameters);
    
    void invalidate(const std::string& query_hash);
    void invalidate_by_table(const std::string& table_name);
    void invalidate_by_pattern(const std::string& pattern);
    void clear();
    
    // Cache management
    void set_ttl(const std::string& query_hash, std::chrono::minutes ttl);
    void enable_caching(bool enable);
    void set_max_size(size_t max_size);
    void set_compression(bool enable);
    
    // Statistics and monitoring
    size_t get_cache_size() const;
    size_t get_hit_count() const;
    size_t get_miss_count() const;
    double get_hit_rate() const;
    size_t get_memory_usage() const;
    
    // Cache health
    bool is_healthy() const;
    void cleanup_expired_entries();
    void optimize_cache();
    
    // Configuration
    CacheConfig get_config() const;
    void update_config(const CacheConfig& config);

private:
    // Internal cache structure
    struct CacheData {
        std::unique_ptr<pg_result> result;
        CacheEntry metadata;
    };
    
    // Member variables
    std::unordered_map<std::string, CacheData> cache_;
    CacheConfig config_;
    
    mutable std::mutex cache_mutex_;
    std::atomic<bool> caching_enabled_{true};
    std::atomic<size_t> hit_count_{0};
    std::atomic<size_t> miss_count_{0};
    std::atomic<size_t> memory_usage_{0};
    
    // Internal methods
    std::string generate_cache_key(const std::string& query_hash,
                                  const std::vector<std::string>& parameters) const;
    bool should_cache_query(const std::string& query_type,
                           const std::string& table_name) const;
    void evict_entries_if_needed();
    void update_memory_usage(const CacheData& data, bool adding);
    std::chrono::minutes calculate_ttl(const std::string& query_type,
                                       const std::string& table_name) const;
    
    // Compression utilities
    std::vector<uint8_t> compress_result(const pg_result* result) const;
    std::unique_ptr<pg_result> decompress_result(const std::vector<uint8_t>& compressed) const;
    
    // Cache optimization
    void update_access_patterns();
    void adjust_ttl_based_on_usage();
};

// Cache-aware query executor
class CachedQueryExecutor {
public:
    explicit CachedQueryExecutor(std::shared_ptr<QueryCache> cache);
    
    // Execute query with caching
    std::unique_ptr<pg_result> execute_cached(
        const std::string& query,
        const std::vector<std::string>& parameters,
        const std::string& query_type,
        const std::string& table_name,
        std::function<std::unique_ptr<pg_result>()> executor);
    
    // Execute query without caching
    std::unique_ptr<pg_result> execute_uncached(
        const std::string& query,
        const std::vector<std::string>& parameters,
        std::function<std::unique_ptr<pg_result>()> executor);
    
    // Cache management
    void invalidate_cache(const std::string& query_hash);
    void invalidate_table_cache(const std::string& table_name);
    void clear_cache();
    
    // Statistics
    double get_cache_hit_rate() const;
    size_t get_cache_size() const;

private:
    std::shared_ptr<QueryCache> cache_;
    std::hash<std::string> query_hasher_;
    
    std::string hash_query(const std::string& query) const;
};

// Cache warming utilities
class CacheWarmer {
public:
    explicit CacheWarmer(std::shared_ptr<QueryCache> cache);
    
    // Warm cache with common queries
    void warm_cache_with_common_queries();
    void warm_cache_for_table(const std::string& table_name);
    void warm_cache_for_user(const std::string& user_id);
    
    // Predictive cache warming
    void warm_cache_based_on_patterns();
    void warm_cache_based_on_time();

private:
    std::shared_ptr<QueryCache> cache_;
    
    std::vector<std::string> get_common_queries() const;
    std::vector<std::string> get_table_queries(const std::string& table_name) const;
    std::vector<std::string> get_user_queries(const std::string& user_id) const;
};

} // namespace database
} // namespace sonet