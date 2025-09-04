/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>
#include <memory>
#include <functional>

namespace sonet {
namespace database {

// Performance metrics for a single query
struct QueryMetrics {
    std::string query_hash;
    std::string query_type; // "SELECT", "INSERT", "UPDATE", "DELETE"
    std::string table_name;
    std::chrono::microseconds execution_time;
    std::chrono::microseconds preparation_time;
    std::chrono::microseconds connection_wait_time;
    size_t rows_affected;
    size_t rows_returned;
    size_t parameters_count;
    bool used_prepared_statement;
    bool used_index;
    std::string execution_plan;
    std::chrono::system_clock::time_point timestamp;
    std::string error_message;
    bool success;
    
    // Constructor with default values
    QueryMetrics() 
        : execution_time(0), preparation_time(0), connection_wait_time(0),
          rows_affected(0), rows_returned(0), parameters_count(0),
          used_prepared_statement(false), used_index(false), success(true) {}
};

// Performance statistics for query patterns
struct QueryStats {
    size_t total_executions;
    size_t successful_executions;
    size_t failed_executions;
    std::chrono::microseconds total_execution_time;
    std::chrono::microseconds min_execution_time;
    std::chrono::microseconds max_execution_time;
    std::chrono::microseconds avg_execution_time;
    std::chrono::microseconds p95_execution_time;
    std::chrono::microseconds p99_execution_time;
    
    // Constructor
    QueryStats() 
        : total_executions(0), successful_executions(0), failed_executions(0),
          total_execution_time(0), min_execution_time(std::chrono::microseconds::max()),
          max_execution_time(0), avg_execution_time(0), p95_execution_time(0), p99_execution_time(0) {}
    
    // Update statistics with new metrics
    void update(const QueryMetrics& metrics);
    
    // Calculate percentiles
    void calculate_percentiles(const std::vector<std::chrono::microseconds>& times);
};

// Connection pool performance metrics
struct ConnectionPoolMetrics {
    size_t total_connections_created;
    size_t total_connections_destroyed;
    size_t current_active_connections;
    size_t current_idle_connections;
    size_t max_concurrent_connections;
    std::chrono::microseconds avg_connection_wait_time;
    std::chrono::microseconds max_connection_wait_time;
    size_t connection_timeouts;
    size_t connection_errors;
    std::chrono::system_clock::time_point last_updated;
    
    // Constructor
    ConnectionPoolMetrics() 
        : total_connections_created(0), total_connections_destroyed(0),
          current_active_connections(0), current_idle_connections(0),
          max_concurrent_connections(0), avg_connection_wait_time(0),
          max_connection_wait_time(0), connection_timeouts(0), connection_errors(0) {}
};

// Performance alert thresholds
struct PerformanceThresholds {
    std::chrono::microseconds slow_query_threshold{100000}; // 100ms
    std::chrono::microseconds very_slow_query_threshold{1000000}; // 1s
    size_t max_connection_wait_time_ms{5000}; // 5s
    size_t max_failed_queries_percent{5}; // 5%
    size_t max_connection_pool_utilization{80}; // 80%
    
    // Constructor
    PerformanceThresholds() = default;
    
    // Constructor with custom values
    PerformanceThresholds(std::chrono::microseconds slow_threshold,
                         std::chrono::microseconds very_slow_threshold,
                         size_t max_wait_time,
                         size_t max_failed_percent,
                         size_t max_pool_utilization)
        : slow_query_threshold(slow_threshold),
          very_slow_query_threshold(very_slow_threshold),
          max_connection_wait_time_ms(max_wait_time),
          max_failed_queries_percent(max_failed_percent),
          max_connection_pool_utilization(max_pool_utilization) {}
};

// Performance alert callback
using PerformanceAlertCallback = std::function<void(const std::string& alert_type, 
                                                   const std::string& message, 
                                                   const QueryMetrics& metrics)>;

// Main performance monitor class
class PerformanceMonitor {
public:
    static PerformanceMonitor& get_instance();
    
    // Prevent copying
    PerformanceMonitor(const PerformanceMonitor&) = delete;
    PerformanceMonitor& operator=(const PerformanceMonitor&) = delete;
    
    // Configuration
    void set_thresholds(const PerformanceThresholds& thresholds);
    void set_alert_callback(PerformanceAlertCallback callback);
    void enable_monitoring(bool enable);
    void set_sampling_rate(double rate); // 0.0 to 1.0
    
    // Query monitoring
    void start_query_monitoring(const std::string& query_hash, 
                               const std::string& query_type,
                               const std::string& table_name);
    void end_query_monitoring(const std::string& query_hash, 
                             bool success,
                             size_t rows_affected = 0,
                             size_t rows_returned = 0,
                             const std::string& error_message = "");
    
    // Connection monitoring
    void record_connection_created();
    void record_connection_destroyed();
    void record_connection_acquired();
    void record_connection_released();
    void record_connection_wait_time(std::chrono::microseconds wait_time);
    void record_connection_timeout();
    void record_connection_error();
    
    // Metrics retrieval
    QueryStats get_query_stats(const std::string& query_hash) const;
    ConnectionPoolMetrics get_connection_pool_metrics() const;
    std::vector<QueryMetrics> get_slow_queries(size_t limit = 100) const;
    std::vector<QueryMetrics> get_recent_queries(size_t limit = 100) const;
    
    // Performance analysis
    std::vector<std::string> get_performance_recommendations() const;
    std::string generate_performance_report() const;
    
    // Cleanup
    void clear_old_metrics(std::chrono::hours max_age);
    void reset_metrics();
    
    // Health check
    bool is_performance_healthy() const;

private:
    PerformanceMonitor();
    ~PerformanceMonitor();
    
    // Internal methods
    void update_query_stats(const QueryMetrics& metrics);
    void check_performance_thresholds(const QueryMetrics& metrics);
    void add_query_metrics(const QueryMetrics& metrics);
    std::string hash_query(const std::string& query) const;
    
    // Member variables
    std::unordered_map<std::string, QueryMetrics> active_queries_;
    std::unordered_map<std::string, QueryStats> query_statistics_;
    std::vector<QueryMetrics> recent_queries_;
    std::vector<QueryMetrics> slow_queries_;
    
    ConnectionPoolMetrics connection_pool_metrics_;
    PerformanceThresholds thresholds_;
    PerformanceAlertCallback alert_callback_;
    
    mutable std::mutex metrics_mutex_;
    mutable std::mutex active_queries_mutex_;
    
    std::atomic<bool> monitoring_enabled_{true};
    std::atomic<double> sampling_rate_{1.0};
    std::atomic<size_t> max_recent_queries_{1000};
    std::atomic<size_t> max_slow_queries_{1000};
    
    // Timing utilities
    std::chrono::steady_clock::time_point get_current_time() const;
};

// RAII wrapper for automatic query monitoring
class QueryMonitorScope {
public:
    QueryMonitorScope(const std::string& query_hash,
                     const std::string& query_type,
                     const std::string& table_name);
    ~QueryMonitorScope();
    
    // Mark query as successful
    void mark_success(size_t rows_affected = 0, size_t rows_returned = 0);
    
    // Mark query as failed
    void mark_failure(const std::string& error_message);
    
    // Prevent copying
    QueryMonitorScope(const QueryMonitorScope&) = delete;
    QueryMonitorScope& operator=(const QueryMonitorScope&) = delete;
    
    // Allow moving
    QueryMonitorScope(QueryMonitorScope&& other) noexcept;
    QueryMonitorScope& operator=(QueryMonitorScope&& other) noexcept;

private:
    std::string query_hash_;
    std::chrono::steady_clock::time_point start_time_;
    bool completed_;
    bool success_;
    size_t rows_affected_;
    size_t rows_returned_;
    std::string error_message_;
};

// Utility macros for easy monitoring
#define MONITOR_QUERY(query_type, table_name) \
    sonet::database::QueryMonitorScope query_monitor( \
        sonet::database::PerformanceMonitor::get_instance().hash_query(__PRETTY_FUNCTION__), \
        query_type, table_name)

#define MONITOR_SELECT(table_name) MONITOR_QUERY("SELECT", table_name)
#define MONITOR_INSERT(table_name) MONITOR_QUERY("INSERT", table_name)
#define MONITOR_UPDATE(table_name) MONITOR_QUERY("UPDATE", table_name)
#define MONITOR_DELETE(table_name) MONITOR_QUERY("DELETE", table_name)

} // namespace database
} // namespace sonet