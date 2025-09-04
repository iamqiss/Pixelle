#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>
#include <functional>
#include <future>
#include "crypto_engine.hpp"

namespace sonet::messaging::crypto {

// Forward declarations
class CryptoEngine;
class CryptoKey;

// Performance optimization structures
struct CacheEntry {
    CryptoKey key;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_accessed;
    uint32_t access_count;
    bool is_dirty;
    
    bool is_expired(const std::chrono::seconds& ttl) const;
    void update_access();
};

struct BatchOperation {
    std::string operation_id;
    std::vector<std::string> target_ids;
    std::function<bool(const std::string&)> operation;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point deadline;
    uint32_t priority;
    bool is_completed;
    
    bool is_expired() const;
    bool is_high_priority() const;
};

struct PerformanceMetrics {
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t batch_operations_completed;
    uint64_t async_operations_completed;
    uint64_t total_operations;
    std::chrono::milliseconds average_operation_time;
    std::chrono::milliseconds cache_lookup_time;
    std::chrono::milliseconds batch_processing_time;
    
    void reset();
    void update_average_time(const std::chrono::milliseconds& operation_time);
};

class PerformanceOptimizer {
public:
    explicit PerformanceOptimizer(std::shared_ptr<CryptoEngine> crypto_engine);
    ~PerformanceOptimizer();

    // Key Caching
    std::optional<CryptoKey> get_cached_key(const std::string& key_id);
    void cache_key(const std::string& key_id, const CryptoKey& key, 
                   const std::chrono::seconds& ttl = std::chrono::hours(1));
    void invalidate_cache(const std::string& key_id);
    void clear_expired_cache();
    void set_cache_ttl(const std::chrono::seconds& ttl);
    void set_max_cache_size(size_t max_size);
    
    // Batch Operations
    std::string queue_batch_operation(const std::vector<std::string>& target_ids,
                                     std::function<bool(const std::string&)> operation,
                                     uint32_t priority = 0,
                                     const std::chrono::seconds& deadline = std::chrono::minutes(5));
    bool cancel_batch_operation(const std::string& operation_id);
    std::vector<std::string> get_pending_batch_operations();
    bool process_batch_operations();
    void set_batch_size_limit(size_t max_batch_size);
    void set_batch_processing_interval(const std::chrono::milliseconds& interval);
    
    // Async Processing
    template<typename T>
    std::future<T> execute_async(std::function<T()> task, uint32_t priority = 0);
    
    template<typename T>
    std::future<T> execute_async_with_timeout(std::function<T()> task,
                                             const std::chrono::milliseconds& timeout,
                                             uint32_t priority = 0);
    
    void wait_for_all_async_operations();
    void cancel_async_operation(const std::string& operation_id);
    std::vector<std::string> get_running_async_operations();
    
    // Memory Optimization
    void optimize_memory_usage();
    void set_memory_limit(size_t max_memory_bytes);
    void enable_compression(bool enable);
    void set_compression_level(int level);
    
    // Performance Monitoring
    PerformanceMetrics get_performance_metrics() const;
    void reset_performance_metrics();
    void enable_performance_logging(bool enable);
    void set_performance_logging_interval(const std::chrono::seconds& interval);
    
    // Configuration
    void set_optimization_level(int level); // 0=disabled, 1=basic, 2=aggressive, 3=maximum
    void enable_adaptive_optimization(bool enable);
    void set_adaptive_thresholds(const std::unordered_map<std::string, double>& thresholds);

private:
    std::shared_ptr<CryptoEngine> crypto_engine_;
    
    // Key caching
    std::unordered_map<std::string, CacheEntry> key_cache_;
    std::chrono::seconds cache_ttl_;
    size_t max_cache_size_;
    std::mutex cache_mutex_;
    
    // Batch operations
    std::queue<BatchOperation> batch_queue_;
    std::unordered_map<std::string, BatchOperation> pending_batches_;
    size_t max_batch_size_;
    std::chrono::milliseconds batch_processing_interval_;
    std::mutex batch_mutex_;
    
    // Async operations
    std::unordered_map<std::string, std::future<void>> async_operations_;
    std::mutex async_mutex_;
    
    // Performance metrics
    mutable PerformanceMetrics metrics_;
    mutable std::mutex metrics_mutex_;
    
    // Configuration
    int optimization_level_;
    bool adaptive_optimization_enabled_;
    std::unordered_map<std::string, double> adaptive_thresholds_;
    bool performance_logging_enabled_;
    std::chrono::seconds performance_logging_interval_;
    
    // Memory management
    size_t max_memory_bytes_;
    bool compression_enabled_;
    int compression_level_;
    
    // Threading
    std::thread cache_cleanup_thread_;
    std::thread batch_processing_thread_;
    std::thread performance_monitoring_thread_;
    std::atomic<bool> running_{true};
    
    // Helper methods
    void background_cache_cleanup();
    void background_batch_processing();
    void background_performance_monitoring();
    void log_performance_metrics();
    void adaptive_optimization();
    size_t estimate_memory_usage() const;
    void compress_cache_entry(CacheEntry& entry);
    void decompress_cache_entry(CacheEntry& entry);
    
    // Internal utilities
    std::string generate_operation_id();
    bool should_compress(const CacheEntry& entry) const;
    void update_metrics(const std::string& operation, const std::chrono::milliseconds& duration);
};

// Template implementations
template<typename T>
std::future<T> PerformanceOptimizer::execute_async(std::function<T()> task, uint32_t priority) {
    std::lock_guard<std::mutex> lock(async_mutex_);
    
    std::string operation_id = generate_operation_id();
    
    auto future = std::async(std::launch::async, [this, operation_id, task]() {
        auto start = std::chrono::high_resolution_clock::now();
        
        try {
            auto result = task();
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            update_metrics("async_operation", duration);
            
            return result;
        } catch (...) {
            // Log error and rethrow
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            update_metrics("async_operation_error", duration);
            throw;
        }
    });
    
    async_operations_[operation_id] = future.share();
    return future;
}

template<typename T>
std::future<T> PerformanceOptimizer::execute_async_with_timeout(std::function<T()> task,
                                                              const std::chrono::milliseconds& timeout,
                                                              uint32_t priority) {
    return execute_async<T>([task, timeout]() -> T {
        auto future = std::async(std::launch::async, task);
        
        if (future.wait_for(timeout) == std::future_status::timeout) {
            throw std::runtime_error("Async operation timed out");
        }
        
        return future.get();
    }, priority);
}

} // namespace sonet::messaging::crypto