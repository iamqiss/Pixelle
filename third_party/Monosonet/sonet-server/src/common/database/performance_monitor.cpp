/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "performance_monitor.h"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <functional>
#include <cstring>

namespace sonet {
namespace database {

// QueryStats implementation
void QueryStats::update(const QueryMetrics& metrics) {
    total_executions++;
    
    if (metrics.success) {
        successful_executions++;
    } else {
        failed_executions++;
    }
    
    total_execution_time += metrics.execution_time;
    
    if (metrics.execution_time < min_execution_time) {
        min_execution_time = metrics.execution_time;
    }
    
    if (metrics.execution_time > max_execution_time) {
        max_execution_time = metrics.execution_time;
    }
    
    // Calculate average
    avg_execution_time = total_execution_time / total_executions;
}

void QueryStats::calculate_percentiles(const std::vector<std::chrono::microseconds>& times) {
    if (times.empty()) return;
    
    auto sorted_times = times;
    std::sort(sorted_times.begin(), sorted_times.end());
    
    size_t p95_index = static_cast<size_t>(sorted_times.size() * 0.95);
    size_t p99_index = static_cast<size_t>(sorted_times.size() * 0.99);
    
    if (p95_index < sorted_times.size()) {
        p95_execution_time = sorted_times[p95_index];
    }
    
    if (p99_index < sorted_times.size()) {
        p99_execution_time = sorted_times[p99_index];
    }
}

// PerformanceMonitor implementation
PerformanceMonitor::PerformanceMonitor() {
    spdlog::info("PerformanceMonitor initialized");
}

PerformanceMonitor::~PerformanceMonitor() {
    spdlog::info("PerformanceMonitor shutting down");
}

PerformanceMonitor& PerformanceMonitor::get_instance() {
    static PerformanceMonitor instance;
    return instance;
}

void PerformanceMonitor::set_thresholds(const PerformanceThresholds& thresholds) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    thresholds_ = thresholds;
    spdlog::info("Performance thresholds updated");
}

void PerformanceMonitor::set_alert_callback(PerformanceAlertCallback callback) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    alert_callback_ = callback;
    spdlog::info("Performance alert callback set");
}

void PerformanceMonitor::enable_monitoring(bool enable) {
    monitoring_enabled_ = enable;
    spdlog::info("Performance monitoring {}", enable ? "enabled" : "disabled");
}

void PerformanceMonitor::set_sampling_rate(double rate) {
    if (rate < 0.0 || rate > 1.0) {
        spdlog::warn("Invalid sampling rate: {}. Must be between 0.0 and 1.0", rate);
        return;
    }
    sampling_rate_ = rate;
    spdlog::info("Performance monitoring sampling rate set to {}", rate);
}

void PerformanceMonitor::start_query_monitoring(const std::string& query_hash, 
                                               const std::string& query_type,
                                               const std::string& table_name) {
    if (!monitoring_enabled_ || 
        (sampling_rate_ < 1.0 && 
         static_cast<double>(rand()) / RAND_MAX > sampling_rate_)) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(active_queries_mutex_);
    
    QueryMetrics metrics;
    metrics.query_hash = query_hash;
    metrics.query_type = query_type;
    metrics.table_name = table_name;
    metrics.timestamp = std::chrono::system_clock::now();
    
    active_queries_[query_hash] = metrics;
}

void PerformanceMonitor::end_query_monitoring(const std::string& query_hash, 
                                             bool success,
                                             size_t rows_affected,
                                             size_t rows_returned,
                                             const std::string& error_message) {
    if (!monitoring_enabled_) return;
    
    std::lock_guard<std::mutex> lock(active_queries_mutex_);
    
    auto it = active_queries_.find(query_hash);
    if (it == active_queries_.end()) {
        return; // Query wasn't being monitored
    }
    
    auto& metrics = it->second;
    auto end_time = get_current_time();
    
    metrics.execution_time = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - std::chrono::steady_clock::time_point(metrics.timestamp.time_since_epoch()));
    metrics.success = success;
    metrics.rows_affected = rows_affected;
    metrics.rows_returned = rows_returned;
    metrics.error_message = error_message;
    
    // Add to recent queries and slow queries if applicable
    add_query_metrics(metrics);
    
    // Update statistics
    update_query_stats(metrics);
    
    // Check thresholds and trigger alerts
    check_performance_thresholds(metrics);
    
    // Remove from active queries
    active_queries_.erase(it);
}

void PerformanceMonitor::record_connection_created() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    connection_pool_metrics_.total_connections_created++;
    connection_pool_metrics_.last_updated = std::chrono::system_clock::now();
}

void PerformanceMonitor::record_connection_destroyed() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    connection_pool_metrics_.total_connections_destroyed++;
    connection_pool_metrics_.last_updated = std::chrono::system_clock::now();
}

void PerformanceMonitor::record_connection_acquired() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    connection_pool_metrics_.current_active_connections++;
    if (connection_pool_metrics_.current_active_connections > 
        connection_pool_metrics_.max_concurrent_connections) {
        connection_pool_metrics_.max_concurrent_connections = 
            connection_pool_metrics_.current_active_connections;
    }
    connection_pool_metrics_.last_updated = std::chrono::system_clock::now();
}

void PerformanceMonitor::record_connection_released() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    if (connection_pool_metrics_.current_active_connections > 0) {
        connection_pool_metrics_.current_active_connections--;
    }
    connection_pool_metrics_.last_updated = std::chrono::system_clock::now();
}

void PerformanceMonitor::record_connection_wait_time(std::chrono::microseconds wait_time) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    // Update average wait time
    if (connection_pool_metrics_.total_connections_created > 0) {
        auto total_wait = connection_pool_metrics_.avg_connection_wait_time * 
                          (connection_pool_metrics_.total_connections_created - 1);
        total_wait += wait_time;
        connection_pool_metrics_.avg_connection_wait_time = 
            total_wait / connection_pool_metrics_.total_connections_created;
    } else {
        connection_pool_metrics_.avg_connection_wait_time = wait_time;
    }
    
    if (wait_time > connection_pool_metrics_.max_connection_wait_time) {
        connection_pool_metrics_.max_connection_wait_time = wait_time;
    }
    
    connection_pool_metrics_.last_updated = std::chrono::system_clock::now();
}

void PerformanceMonitor::record_connection_timeout() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    connection_pool_metrics_.connection_timeouts++;
    connection_pool_metrics_.last_updated = std::chrono::system_clock::now();
}

void PerformanceMonitor::record_connection_error() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    connection_pool_metrics_.connection_errors++;
    connection_pool_metrics_.last_updated = std::chrono::system_clock::now();
}

QueryStats PerformanceMonitor::get_query_stats(const std::string& query_hash) const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    auto it = query_statistics_.find(query_hash);
    return (it != query_statistics_.end()) ? it->second : QueryStats{};
}

ConnectionPoolMetrics PerformanceMonitor::get_connection_pool_metrics() const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    return connection_pool_metrics_;
}

std::vector<QueryMetrics> PerformanceMonitor::get_slow_queries(size_t limit) const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    auto queries = slow_queries_;
    if (queries.size() > limit) {
        queries.resize(limit);
    }
    return queries;
}

std::vector<QueryMetrics> PerformanceMonitor::get_recent_queries(size_t limit) const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    auto queries = recent_queries_;
    if (queries.size() > limit) {
        queries.resize(limit);
    }
    return queries;
}

std::vector<std::string> PerformanceMonitor::get_performance_recommendations() const {
    std::vector<std::string> recommendations;
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    // Check for slow queries
    if (!slow_queries_.empty()) {
        recommendations.push_back("Consider adding database indexes for slow queries");
        recommendations.push_back("Review query execution plans for optimization opportunities");
    }
    
    // Check connection pool utilization
    if (connection_pool_metrics_.current_active_connections > 0) {
        double utilization = static_cast<double>(connection_pool_metrics_.current_active_connections) /
                           (connection_pool_metrics_.current_active_connections + 
                            connection_pool_metrics_.current_idle_connections) * 100.0;
        
        if (utilization > thresholds_.max_connection_pool_utilization) {
            recommendations.push_back("Consider increasing connection pool size");
        }
    }
    
    // Check for connection timeouts
    if (connection_pool_metrics_.connection_timeouts > 0) {
        recommendations.push_back("High connection timeout rate - check database health");
    }
    
    // Check for failed queries
    size_t total_queries = 0;
    size_t failed_queries = 0;
    
    for (const auto& stat : query_statistics_) {
        total_queries += stat.second.total_executions;
        failed_queries += stat.second.failed_executions;
    }
    
    if (total_queries > 0) {
        double failure_rate = static_cast<double>(failed_queries) / total_queries * 100.0;
        if (failure_rate > thresholds_.max_failed_queries_percent) {
            recommendations.push_back("High query failure rate - investigate database issues");
        }
    }
    
    return recommendations;
}

std::string PerformanceMonitor::generate_performance_report() const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    std::ostringstream report;
    report << "=== Database Performance Report ===\n\n";
    
    // Query statistics
    report << "Query Statistics:\n";
    report << "  Total unique queries: " << query_statistics_.size() << "\n";
    
    size_t total_executions = 0;
    size_t total_failures = 0;
    std::chrono::microseconds total_time{0};
    
    for (const auto& stat : query_statistics_) {
        total_executions += stat.second.total_executions;
        total_failures += stat.second.failed_executions;
        total_time += stat.second.total_execution_time;
    }
    
    report << "  Total executions: " << total_executions << "\n";
    report << "  Total failures: " << total_failures << "\n";
    report << "  Success rate: " << std::fixed << std::setprecision(2)
           << (total_executions > 0 ? (1.0 - static_cast<double>(total_failures) / total_executions) * 100.0 : 0.0)
           << "%\n";
    
    if (total_executions > 0) {
        auto avg_time = total_time / total_executions;
        report << "  Average execution time: " << avg_time.count() << " μs\n";
    }
    
    // Connection pool statistics
    report << "\nConnection Pool Statistics:\n";
    report << "  Total connections created: " << connection_pool_metrics_.total_connections_created << "\n";
    report << "  Current active connections: " << connection_pool_metrics_.current_active_connections << "\n";
    report << "  Current idle connections: " << connection_pool_metrics_.current_idle_connections << "\n";
    report << "  Max concurrent connections: " << connection_pool_metrics_.max_concurrent_connections << "\n";
    report << "  Connection timeouts: " << connection_pool_metrics_.connection_timeouts << "\n";
    report << "  Connection errors: " << connection_pool_metrics_.connection_errors << "\n";
    
    // Slow queries
    if (!slow_queries_.empty()) {
        report << "\nSlow Queries (last " << slow_queries_.size() << "):\n";
        for (const auto& query : slow_queries_) {
            report << "  " << query.query_type << " on " << query.table_name 
                   << " - " << query.execution_time.count() << " μs\n";
        }
    }
    
    // Performance recommendations
    auto recommendations = get_performance_recommendations();
    if (!recommendations.empty()) {
        report << "\nPerformance Recommendations:\n";
        for (const auto& rec : recommendations) {
            report << "  - " << rec << "\n";
        }
    }
    
    report << "\nReport generated at: " 
           << std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) << "\n";
    
    return report.str();
}

void PerformanceMonitor::clear_old_metrics(std::chrono::hours max_age) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    auto cutoff_time = std::chrono::system_clock::now() - max_age;
    
    // Clear old recent queries
    recent_queries_.erase(
        std::remove_if(recent_queries_.begin(), recent_queries_.end(),
            [cutoff_time](const QueryMetrics& metrics) {
                return metrics.timestamp < cutoff_time;
            }),
        recent_queries_.end()
    );
    
    // Clear old slow queries
    slow_queries_.erase(
        std::remove_if(slow_queries_.begin(), slow_queries_.end(),
            [cutoff_time](const QueryMetrics& metrics) {
                return metrics.timestamp < cutoff_time;
            }),
        slow_queries_.end()
    );
    
    spdlog::info("Cleared metrics older than {} hours", max_age.count());
}

void PerformanceMonitor::reset_metrics() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    query_statistics_.clear();
    recent_queries_.clear();
    slow_queries_.clear();
    connection_pool_metrics_ = ConnectionPoolMetrics{};
    
    spdlog::info("Performance metrics reset");
}

bool PerformanceMonitor::is_performance_healthy() const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    // Check for high failure rate
    size_t total_queries = 0;
    size_t failed_queries = 0;
    
    for (const auto& stat : query_statistics_) {
        total_queries += stat.second.total_executions;
        failed_queries += stat.second.failed_executions;
    }
    
    if (total_queries > 0) {
        double failure_rate = static_cast<double>(failed_queries) / total_queries * 100.0;
        if (failure_rate > thresholds_.max_failed_queries_percent) {
            return false;
        }
    }
    
    // Check for connection pool issues
    if (connection_pool_metrics_.connection_timeouts > 10) {
        return false;
    }
    
    if (connection_pool_metrics_.connection_errors > 10) {
        return false;
    }
    
    return true;
}

void PerformanceMonitor::update_query_stats(const QueryMetrics& metrics) {
    auto& stats = query_statistics_[metrics.query_hash];
    stats.update(metrics);
}

void PerformanceMonitor::check_performance_thresholds(const QueryMetrics& metrics) {
    if (!alert_callback_) return;
    
    std::string alert_type;
    std::string message;
    
    if (metrics.execution_time > thresholds_.very_slow_query_threshold) {
        alert_type = "VERY_SLOW_QUERY";
        message = "Query execution time exceeded very slow threshold";
        alert_callback_(alert_type, message, metrics);
    } else if (metrics.execution_time > thresholds_.slow_query_threshold) {
        alert_type = "SLOW_QUERY";
        message = "Query execution time exceeded slow threshold";
        alert_callback_(alert_type, message, metrics);
    }
    
    if (!metrics.success) {
        alert_type = "QUERY_FAILURE";
        message = "Query execution failed";
        alert_callback_(alert_type, message, metrics);
    }
}

void PerformanceMonitor::add_query_metrics(const QueryMetrics& metrics) {
    // Add to recent queries
    recent_queries_.push_back(metrics);
    if (recent_queries_.size() > max_recent_queries_) {
        recent_queries_.erase(recent_queries_.begin());
    }
    
    // Add to slow queries if applicable
    if (metrics.execution_time > thresholds_.slow_query_threshold) {
        slow_queries_.push_back(metrics);
        if (slow_queries_.size() > max_slow_queries_) {
            slow_queries_.erase(slow_queries_.begin());
        }
    }
}

std::string PerformanceMonitor::hash_query(const std::string& query) const {
    // Simple hash function for query identification
    std::hash<std::string> hasher;
    return std::to_string(hasher(query));
}

std::chrono::steady_clock::time_point PerformanceMonitor::get_current_time() const {
    return std::chrono::steady_clock::now();
}

// QueryMonitorScope implementation
QueryMonitorScope::QueryMonitorScope(const std::string& query_hash,
                                     const std::string& query_type,
                                     const std::string& table_name)
    : query_hash_(query_hash), completed_(false), success_(false),
      rows_affected_(0), rows_returned_(0) {
    
    PerformanceMonitor::get_instance().start_query_monitoring(
        query_hash_, query_type, table_name);
}

QueryMonitorScope::~QueryMonitorScope() {
    if (!completed_) {
        mark_failure("Query monitoring scope destroyed without completion");
    }
}

void QueryMonitorScope::mark_success(size_t rows_affected, size_t rows_returned) {
    if (completed_) return;
    
    success_ = true;
    rows_affected_ = rows_affected;
    rows_returned_ = rows_returned;
    completed_ = true;
    
    PerformanceMonitor::get_instance().end_query_monitoring(
        query_hash_, true, rows_affected_, rows_returned_);
}

void QueryMonitorScope::mark_failure(const std::string& error_message) {
    if (completed_) return;
    
    success_ = false;
    error_message_ = error_message;
    completed_ = true;
    
    PerformanceMonitor::get_instance().end_query_monitoring(
        query_hash_, false, 0, 0, error_message_);
}

QueryMonitorScope::QueryMonitorScope(QueryMonitorScope&& other) noexcept
    : query_hash_(std::move(other.query_hash_)),
      start_time_(other.start_time_),
      completed_(other.completed_),
      success_(other.success_),
      rows_affected_(other.rows_affected_),
      rows_returned_(other.rows_returned_),
      error_message_(std::move(other.error_message_)) {
    other.completed_ = true; // Prevent other from calling end_query_monitoring
}

QueryMonitorScope& QueryMonitorScope::operator=(QueryMonitorScope&& other) noexcept {
    if (this != &other) {
        if (!completed_) {
            mark_failure("Query monitoring scope moved without completion");
        }
        
        query_hash_ = std::move(other.query_hash_);
        start_time_ = other.start_time_;
        completed_ = other.completed_;
        success_ = other.success_;
        rows_affected_ = other.rows_affected_;
        rows_returned_ = other.rows_returned_;
        error_message_ = std::move(other.error_message_);
        
        other.completed_ = true;
    }
    return *this;
}

} // namespace database
} // namespace sonet