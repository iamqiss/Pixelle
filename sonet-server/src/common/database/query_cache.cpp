/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "query_cache.h"
#include <algorithm>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <spdlog/spdlog.h>

namespace sonet {
namespace database {

// QueryCache implementation
QueryCache::QueryCache(const CacheConfig& config) 
    : config_(config) {
    spdlog::info("QueryCache initialized with max_size={}, max_result_size={}MB", 
                 config_.max_cache_size, config_.max_result_size / (1024 * 1024));
}

QueryCache::~QueryCache() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    cache_.clear();
    spdlog::info("QueryCache destroyed, cleared {} entries", cache_.size());
}

void QueryCache::put(const std::string& query_hash,
                     const std::string& query_type,
                     const std::string& table_name,
                     const std::vector<std::string>& parameters,
                     std::unique_ptr<pg_result> result,
                     std::chrono::minutes ttl) {
    if (!caching_enabled_ || !result) {
        return;
    }
    
    // Check if query should be cached
    if (!should_cache_query(query_type, table_name)) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Generate cache key
    std::string cache_key = generate_cache_key(query_hash, parameters);
    
    // Check if result size is within limits
    size_t result_size = estimate_result_size(result.get());
    if (result_size > config_.max_result_size) {
        spdlog::debug("Query result too large to cache: {} bytes (max: {})", 
                     result_size, config_.max_result_size);
        return;
    }
    
    // Create cache entry
    CacheEntry entry;
    entry.query_hash = query_hash;
    entry.query_type = query_type;
    entry.table_name = table_name;
    entry.parameters = parameters;
    entry.created_at = std::chrono::system_clock::now();
    entry.last_accessed = entry.created_at;
    entry.access_count = 1;
    entry.result_size = result_size;
    entry.is_valid = true;
    
    // Set TTL
    if (ttl.count() > 0) {
        entry.expires_at = entry.created_at + ttl;
    } else {
        entry.expires_at = entry.created_at + config_.default_ttl;
    }
    
    // Create cache data
    CacheData data;
    data.result = std::move(result);
    data.metadata = entry;
    
    // Evict entries if needed
    evict_entries_if_needed();
    
    // Store in cache
    cache_[cache_key] = std::move(data);
    update_memory_usage(data, true);
    
    spdlog::debug("Cached query: {} (type: {}, table: {}, size: {} bytes)", 
                  query_hash, query_type, table_name, result_size);
}

std::optional<std::unique_ptr<pg_result>> QueryCache::get(const std::string& query_hash,
                                                          const std::vector<std::string>& parameters) {
    if (!caching_enabled_) {
        miss_count_++;
        return std::nullopt;
    }
    
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Generate cache key
    std::string cache_key = generate_cache_key(query_hash, parameters);
    
    // Look for entry
    auto it = cache_.find(cache_key);
    if (it == cache_.end()) {
        miss_count_++;
        return std::nullopt;
    }
    
    CacheData& data = it->second;
    CacheEntry& entry = data.metadata;
    
    // Check if entry is valid and not expired
    if (!entry.is_valid || entry.is_expired()) {
        // Remove expired entry
        update_memory_usage(data, false);
        cache_.erase(it);
        miss_count_++;
        return std::nullopt;
    }
    
    // Update access statistics
    entry.touch();
    hit_count_++;
    
    // Clone the result (since we can't return a reference to unique_ptr)
    std::unique_ptr<pg_result> cloned_result = clone_pg_result(data.result.get());
    
    spdlog::debug("Cache hit for query: {} (access_count: {})", query_hash, entry.access_count);
    
    return cloned_result;
}

void QueryCache::invalidate(const std::string& query_hash) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t invalidated_count = 0;
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (it->second.metadata.query_hash == query_hash) {
            update_memory_usage(it->second, false);
            it = cache_.erase(it);
            invalidated_count++;
        } else {
            ++it;
        }
    }
    
    if (invalidated_count > 0) {
        spdlog::info("Invalidated {} cache entries for query hash: {}", invalidated_count, query_hash);
    }
}

void QueryCache::invalidate_by_table(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t invalidated_count = 0;
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (it->second.metadata.table_name == table_name) {
            update_memory_usage(it->second, false);
            it = cache_.erase(it);
            invalidated_count++;
        } else {
            ++it;
        }
    }
    
    if (invalidated_count > 0) {
        spdlog::info("Invalidated {} cache entries for table: {}", invalidated_count, table_name);
    }
}

void QueryCache::invalidate_by_pattern(const std::string& pattern) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t invalidated_count = 0;
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (it->second.metadata.query_hash.find(pattern) != std::string::npos ||
            it->second.metadata.table_name.find(pattern) != std::string::npos) {
            update_memory_usage(it->second, false);
            it = cache_.erase(it);
            invalidated_count++;
        } else {
            ++it;
        }
    }
    
    if (invalidated_count > 0) {
        spdlog::info("Invalidated {} cache entries matching pattern: {}", invalidated_count, pattern);
    }
}

void QueryCache::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t cleared_count = cache_.size();
    cache_.clear();
    memory_usage_ = 0;
    
    spdlog::info("Cleared {} cache entries", cleared_count);
}

void QueryCache::set_ttl(const std::string& query_hash, std::chrono::minutes ttl) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    for (auto& [key, data] : cache_) {
        if (data.metadata.query_hash == query_hash) {
            data.metadata.expires_at = data.metadata.created_at + ttl;
            spdlog::debug("Updated TTL for query: {} to {} minutes", query_hash, ttl.count());
            break;
        }
    }
}

void QueryCache::enable_caching(bool enable) {
    caching_enabled_ = enable;
    spdlog::info("Query caching {}", enable ? "enabled" : "disabled");
}

void QueryCache::set_max_size(size_t max_size) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    config_.max_cache_size = max_size;
    
    // Evict entries if current size exceeds new limit
    evict_entries_if_needed();
    
    spdlog::info("Cache max size updated to {}", max_size);
}

void QueryCache::set_compression(bool enable) {
    config_.enable_compression = enable;
    spdlog::info("Query result compression {}", enable ? "enabled" : "disabled");
}

size_t QueryCache::get_cache_size() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return cache_.size();
}

size_t QueryCache::get_hit_count() const {
    return hit_count_;
}

size_t QueryCache::get_miss_count() const {
    return miss_count_;
}

double QueryCache::get_hit_rate() const {
    size_t total = hit_count_ + miss_count_;
    return total > 0 ? static_cast<double>(hit_count_) / total : 0.0;
}

size_t QueryCache::get_memory_usage() const {
    return memory_usage_;
}

bool QueryCache::is_healthy() const {
    double hit_rate = get_hit_rate();
    return hit_rate >= config_.hit_rate_threshold && 
           memory_usage_ < config_.max_result_size * 2;
}

void QueryCache::cleanup_expired_entries() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t cleaned_count = 0;
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (it->second.metadata.should_evict()) {
            update_memory_usage(it->second, false);
            it = cache_.erase(it);
            cleaned_count++;
        } else {
            ++it;
        }
    }
    
    if (cleaned_count > 0) {
        spdlog::info("Cleaned up {} expired cache entries", cleaned_count);
    }
}

void QueryCache::optimize_cache() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Update access patterns
    update_access_patterns();
    
    // Adjust TTL based on usage
    adjust_ttl_based_on_usage();
    
    // Evict least recently used entries if needed
    evict_entries_if_needed();
    
    spdlog::info("Cache optimization completed");
}

CacheConfig QueryCache::get_config() const {
    return config_;
}

void QueryCache::update_config(const CacheConfig& config) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    config_ = config;
    
    // Apply new configuration
    evict_entries_if_needed();
    
    spdlog::info("Cache configuration updated");
}

// Private methods
std::string QueryCache::generate_cache_key(const std::string& query_hash,
                                          const std::vector<std::string>& parameters) const {
    std::stringstream ss;
    ss << query_hash;
    
    for (const auto& param : parameters) {
        ss << "|" << param;
    }
    
    return ss.str();
}

bool QueryCache::should_cache_query(const std::string& query_type,
                                   const std::string& table_name) const {
    // Don't cache write operations
    if (query_type == "INSERT" || query_type == "UPDATE" || 
        query_type == "DELETE" || query_type == "TRUNCATE") {
        return false;
    }
    
    // Don't cache system table queries
    if (table_name.find("pg_") == 0 || table_name.find("information_schema") == 0) {
        return false;
    }
    
    return true;
}

void QueryCache::evict_entries_if_needed() {
    if (cache_.size() <= config_.max_cache_size) {
        return;
    }
    
    // Convert to vector for sorting
    std::vector<std::pair<std::string, const CacheData*>> entries;
    for (const auto& [key, data] : cache_) {
        entries.emplace_back(key, &data);
    }
    
    // Sort by access count and last access time (LRU)
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) {
                  const auto& data_a = *a.second;
                  const auto& data_b = *b.second;
                  
                  if (data_a.metadata.access_count != data_b.metadata.access_count) {
                      return data_a.metadata.access_count < data_b.metadata.access_count;
                  }
                  
                  return data_a.metadata.last_accessed < data_b.metadata.last_accessed;
              });
    
    // Remove excess entries
    size_t to_remove = cache_.size() - config_.max_cache_size;
    for (size_t i = 0; i < to_remove; ++i) {
        const auto& key = entries[i].first;
        const auto& data = *entries[i].second;
        
        update_memory_usage(data, false);
        cache_.erase(key);
    }
    
    spdlog::debug("Evicted {} cache entries to maintain size limit", to_remove);
}

void QueryCache::update_memory_usage(const CacheData& data, bool adding) {
    if (adding) {
        memory_usage_ += data.metadata.result_size;
    } else {
        if (memory_usage_ >= data.metadata.result_size) {
            memory_usage_ -= data.metadata.result_size;
        } else {
            memory_usage_ = 0;
        }
    }
}

std::chrono::minutes QueryCache::calculate_ttl(const std::string& query_type,
                                               const std::string& table_name) const {
    // Custom TTL based on query type and table
    if (query_type == "SELECT") {
        if (table_name == "users" || table_name == "profiles") {
            return std::chrono::minutes{15}; // User data changes frequently
        } else if (table_name == "notes" || table_name == "comments") {
            return std::chrono::minutes{5};  // Social content changes very frequently
        } else {
            return std::chrono::minutes{30}; // Default for other tables
        }
    }
    
    return config_.default_ttl;
}

void QueryCache::update_access_patterns() {
    // This could be extended to track query patterns and adjust caching strategy
    // For now, it's a placeholder for future optimization
}

void QueryCache::adjust_ttl_based_on_usage() {
    // This could be extended to adjust TTL based on access frequency
    // For now, it's a placeholder for future optimization
}

// Utility functions for pg_result handling
size_t QueryCache::estimate_result_size(const pg_result* result) {
    if (!result) return 0;
    
    // This is a simplified estimation - in a real implementation,
    // you'd need to traverse the actual pg_result structure
    // For now, return a reasonable estimate
    return 1024; // 1KB default estimate
}

std::unique_ptr<pg_result> QueryCache::clone_pg_result(const pg_result* result) {
    if (!result) return nullptr;
    
    // This is a simplified clone - in a real implementation,
    // you'd need to properly clone the pg_result structure
    // For now, return nullptr to indicate cloning is not implemented
    spdlog::warn("pg_result cloning not implemented - returning nullptr");
    return nullptr;
}

// CachedQueryExecutor implementation
CachedQueryExecutor::CachedQueryExecutor(std::shared_ptr<QueryCache> cache)
    : cache_(cache) {
    if (!cache_) {
        throw std::invalid_argument("QueryCache cannot be null");
    }
}

std::unique_ptr<pg_result> CachedQueryExecutor::execute_cached(
    const std::string& query,
    const std::vector<std::string>& parameters,
    const std::string& query_type,
    const std::string& table_name,
    std::function<std::unique_ptr<pg_result>()> executor) {
    
    if (!cache_) {
        return executor();
    }
    
    // Try to get from cache first
    std::string query_hash = hash_query(query);
    auto cached_result = cache_->get(query_hash, parameters);
    
    if (cached_result.has_value()) {
        spdlog::debug("Cache hit for query: {}", query_hash);
        return std::move(cached_result.value());
    }
    
    // Execute query and cache result
    spdlog::debug("Cache miss for query: {}", query_hash);
    auto result = executor();
    
    if (result) {
        cache_->put(query_hash, query_type, table_name, parameters, std::move(result));
    }
    
    return result;
}

std::unique_ptr<pg_result> CachedQueryExecutor::execute_uncached(
    const std::string& query,
    const std::vector<std::string>& parameters,
    std::function<std::unique_ptr<pg_result>()> executor) {
    
    return executor();
}

void CachedQueryExecutor::invalidate_cache(const std::string& query_hash) {
    if (cache_) {
        cache_->invalidate(query_hash);
    }
}

void CachedQueryExecutor::invalidate_table_cache(const std::string& table_name) {
    if (cache_) {
        cache_->invalidate_by_table(table_name);
    }
}

void CachedQueryExecutor::clear_cache() {
    if (cache_) {
        cache_->clear();
    }
}

double CachedQueryExecutor::get_cache_hit_rate() const {
    return cache_ ? cache_->get_hit_rate() : 0.0;
}

size_t CachedQueryExecutor::get_cache_size() const {
    return cache_ ? cache_->get_cache_size() : 0;
}

std::string CachedQueryExecutor::hash_query(const std::string& query) const {
    return std::to_string(query_hasher_(query));
}

// CacheWarmer implementation
CacheWarmer::CacheWarmer(std::shared_ptr<QueryCache> cache) : cache_(cache) {
    if (!cache_) {
        throw std::invalid_argument("QueryCache cannot be null");
    }
}

void CacheWarmer::warm_cache_with_common_queries() {
    if (!cache_) return;
    
    auto common_queries = get_common_queries();
    spdlog::info("Warming cache with {} common queries", common_queries.size());
    
    // In a real implementation, you would execute these queries
    // and cache their results. For now, just log the intention.
    for (const auto& query : common_queries) {
        spdlog::debug("Would warm cache with query: {}", query);
    }
}

void CacheWarmer::warm_cache_for_table(const std::string& table_name) {
    if (!cache_) return;
    
    auto table_queries = get_table_queries(table_name);
    spdlog::info("Warming cache for table: {} with {} queries", table_name, table_queries.size());
    
    for (const auto& query : table_queries) {
        spdlog::debug("Would warm cache for table {} with query: {}", table_name, query);
    }
}

void CacheWarmer::warm_cache_for_user(const std::string& user_id) {
    if (!cache_) return;
    
    auto user_queries = get_user_queries(user_id);
    spdlog::info("Warming cache for user: {} with {} queries", user_id, user_queries.size());
    
    for (const auto& query : user_queries) {
        spdlog::debug("Would warm cache for user {} with query: {}", user_id, query);
    }
}

void CacheWarmer::warm_cache_based_on_patterns() {
    if (!cache_) return;
    
    spdlog::info("Warming cache based on access patterns");
    // Implementation would analyze access patterns and pre-populate cache
}

void CacheWarmer::warm_cache_based_on_time() {
    if (!cache_) return;
    
    spdlog::info("Warming cache based on time-based patterns");
    // Implementation would analyze time-based patterns and pre-populate cache
}

std::vector<std::string> CacheWarmer::get_common_queries() const {
    // Return common queries that should be cached
    return {
        "SELECT * FROM users WHERE id = $1",
        "SELECT * FROM profiles WHERE user_id = $1",
        "SELECT * FROM notes WHERE author_id = $1 ORDER BY created_at DESC LIMIT 20",
        "SELECT COUNT(*) FROM notes WHERE author_id = $1",
        "SELECT * FROM comments WHERE note_id = $1 ORDER BY created_at ASC"
    };
}

std::vector<std::string> CacheWarmer::get_table_queries(const std::string& table_name) const {
    // Return table-specific queries
    if (table_name == "users") {
        return {
            "SELECT * FROM users WHERE id = $1",
            "SELECT * FROM users WHERE email = $1",
            "SELECT * FROM users WHERE username = $1"
        };
    } else if (table_name == "notes") {
        return {
            "SELECT * FROM notes WHERE id = $1",
            "SELECT * FROM notes WHERE author_id = $1 ORDER BY created_at DESC",
            "SELECT COUNT(*) FROM notes WHERE author_id = $1"
        };
    }
    
    return {};
}

std::vector<std::string> CacheWarmer::get_user_queries(const std::string& user_id) const {
    // Return user-specific queries
    return {
        "SELECT * FROM users WHERE id = $1",
        "SELECT * FROM profiles WHERE user_id = $1",
        "SELECT * FROM notes WHERE author_id = $1 ORDER BY created_at DESC LIMIT 20",
        "SELECT * FROM user_settings WHERE user_id = $1"
    };
}

} // namespace database
} // namespace sonet