/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "performance_monitor.h"
#include <memory>
#include <chrono>
#include <atomic>
#include <thread>
#include <functional>

namespace sonet {
namespace database {

// Connection pool optimization configuration
struct PoolOptimizationConfig {
    // Dynamic sizing
    bool enable_dynamic_sizing = true;
    size_t min_connections = 5;
    size_t max_connections = 50;
    size_t target_utilization = 70; // 70% target utilization
    
    // Connection lifecycle
    std::chrono::seconds max_connection_age{3600}; // 1 hour
    std::chrono::seconds connection_idle_timeout{300}; // 5 minutes
    std::chrono::seconds connection_test_interval{60}; // 1 minute
    
    // Health monitoring
    bool enable_health_checks = true;
    std::chrono::seconds health_check_interval{30}; // 30 seconds
    size_t max_failed_health_checks = 3;
    
    // Performance thresholds
    std::chrono::milliseconds max_connection_wait_time{5000}; // 5 seconds
    double max_error_rate = 0.05; // 5% max error rate
    size_t max_concurrent_connections = 100;
    
    // Adaptive behavior
    bool enable_adaptive_timeouts = true;
    bool enable_connection_recycling = true;
    bool enable_load_balancing = false;
    
    // Constructor
    PoolOptimizationConfig() = default;
    
    // Constructor with custom values
    PoolOptimizationConfig(size_t min_conn, size_t max_conn, size_t target_util)
        : min_connections(min_conn), max_connections(max_conn), target_utilization(target_util) {}
};

// Connection health status
enum class ConnectionHealth {
    HEALTHY,
    DEGRADED,
    UNHEALTHY,
    CRITICAL
};

// Connection pool health metrics
struct PoolHealthMetrics {
    ConnectionHealth overall_health;
    size_t healthy_connections;
    size_t degraded_connections;
    size_t unhealthy_connections;
    size_t total_connections;
    double health_score; // 0.0 to 1.0
    
    // Performance indicators
    double avg_response_time;
    double error_rate;
    double utilization_rate;
    size_t connection_wait_time_avg;
    size_t connection_wait_time_max;
    
    // Constructor
    PoolHealthMetrics() 
        : overall_health(ConnectionHealth::HEALTHY),
          healthy_connections(0), degraded_connections(0), unhealthy_connections(0),
          total_connections(0), health_score(1.0), avg_response_time(0.0),
          error_rate(0.0), utilization_rate(0.0), connection_wait_time_avg(0),
          connection_wait_time_max(0) {}
};

// Main connection pool optimizer
class ConnectionPoolOptimizer {
public:
    explicit ConnectionPoolOptimizer(const PoolOptimizationConfig& config = {});
    ~ConnectionPoolOptimizer();
    
    // Prevent copying
    ConnectionPoolOptimizer(const ConnectionPoolOptimizer&) = delete;
    ConnectionPoolOptimizer& operator=(const ConnectionPoolOptimizer&) = delete;
    
    // Configuration
    void set_config(const PoolOptimizationConfig& config);
    PoolOptimizationConfig get_config() const;
    
    // Optimization control
    void start_optimization();
    void stop_optimization();
    void pause_optimization();
    void resume_optimization();
    
    // Health monitoring
    PoolHealthMetrics get_pool_health() const;
    bool is_pool_healthy() const;
    void perform_health_check();
    
    // Dynamic optimization
    void optimize_pool_size();
    void optimize_connection_timeouts();
    void optimize_connection_recycling();
    
    // Performance analysis
    std::vector<std::string> get_optimization_recommendations() const;
    std::string generate_optimization_report() const;
    
    // Event callbacks
    using HealthAlertCallback = std::function<void(ConnectionHealth, const std::string&)>;
    void set_health_alert_callback(HealthAlertCallback callback);
    
    // Statistics
    size_t get_optimization_count() const;
    std::chrono::system_clock::time_point get_last_optimization() const;
    double get_optimization_effectiveness() const;

private:
    // Member variables
    PoolOptimizationConfig config_;
    PoolHealthMetrics current_health_;
    PerformanceMonitor& performance_monitor_;
    
    std::atomic<bool> optimization_running_{false};
    std::atomic<bool> optimization_paused_{false};
    std::atomic<size_t> optimization_count_{0};
    std::atomic<double> optimization_effectiveness_{0.0};
    
    std::chrono::system_clock::time_point last_optimization_;
    std::chrono::system_clock::time_point last_health_check_;
    
    // Background threads
    std::thread optimization_thread_;
    std::thread health_monitor_thread_;
    
    // Callbacks
    HealthAlertCallback health_alert_callback_;
    
    // Internal methods
    void optimization_loop();
    void health_monitor_loop();
    void update_pool_health();
    ConnectionHealth calculate_health_score() const;
    void trigger_health_alert(ConnectionHealth health, const std::string& message);
    
    // Optimization algorithms
    size_t calculate_optimal_pool_size() const;
    std::chrono::seconds calculate_optimal_timeout() const;
    bool should_recycle_connection(size_t connection_age, size_t error_count) const;
    
    // Performance analysis
    double calculate_utilization_rate() const;
    double calculate_error_rate() const;
    double calculate_response_time() const;
    
    // Utility methods
    void log_optimization_event(const std::string& event, const std::string& details);
    bool should_perform_optimization() const;
};

// Connection load balancer
class ConnectionLoadBalancer {
public:
    explicit ConnectionLoadBalancer(size_t pool_count = 1);
    
    // Load balancing strategies
    enum class Strategy {
        ROUND_ROBIN,
        LEAST_CONNECTIONS,
        WEIGHTED_ROUND_ROBIN,
        ADAPTIVE_LOAD_BALANCING
    };
    
    // Configuration
    void set_strategy(Strategy strategy);
    void set_pool_weights(const std::vector<double>& weights);
    void enable_health_aware_routing(bool enable);
    
    // Pool selection
    size_t select_pool(const std::string& query_type = "");
    size_t select_pool_for_user(const std::string& user_id);
    size_t select_pool_for_table(const std::string& table_name);
    
    // Pool management
    void add_pool(size_t pool_id, double weight = 1.0);
    void remove_pool(size_t pool_id);
    void update_pool_health(size_t pool_id, ConnectionHealth health);
    
    // Statistics
    std::vector<size_t> get_pool_selection_counts() const;
    double get_pool_utilization(size_t pool_id) const;
    Strategy get_current_strategy() const;

private:
    // Member variables
    Strategy current_strategy_;
    std::vector<double> pool_weights_;
    std::vector<ConnectionHealth> pool_health_;
    std::vector<size_t> selection_counts_;
    std::vector<double> pool_utilization_;
    
    std::atomic<size_t> current_pool_index_{0};
    std::atomic<size_t> total_selections_{0};
    
    bool health_aware_routing_;
    
    // Internal methods
    size_t round_robin_selection();
    size_t least_connections_selection();
    size_t weighted_round_robin_selection();
    size_t adaptive_load_balancing_selection(const std::string& query_type);
    
    void update_pool_statistics(size_t pool_id);
    double calculate_pool_score(size_t pool_id) const;
};

// Connection performance analyzer
class ConnectionPerformanceAnalyzer {
public:
    explicit ConnectionPerformanceAnalyzer();
    
    // Performance analysis
    struct PerformanceAnalysis {
        double throughput;           // Queries per second
        double latency_p50;          // 50th percentile latency
        double latency_p95;          // 95th percentile latency
        double latency_p99;          // 99th percentile latency
        double error_rate;           // Error rate percentage
        double connection_efficiency; // Connection utilization efficiency
        double query_efficiency;     // Query execution efficiency
        
        // Constructor
        PerformanceAnalysis() 
            : throughput(0.0), latency_p50(0.0), latency_p95(0.0), latency_p99(0.0),
              error_rate(0.0), connection_efficiency(0.0), query_efficiency(0.0) {}
    };
    
    // Analysis methods
    PerformanceAnalysis analyze_pool_performance(const std::vector<QueryMetrics>& metrics) const;
    PerformanceAnalysis analyze_connection_performance(const ConnectionPoolMetrics& metrics) const;
    
    // Performance recommendations
    std::vector<std::string> get_performance_recommendations(const PerformanceAnalysis& analysis) const;
    std::string generate_performance_analysis_report(const PerformanceAnalysis& analysis) const;
    
    // Trend analysis
    bool is_performance_improving(const std::vector<PerformanceAnalysis>& historical_data) const;
    double calculate_performance_trend(const std::vector<PerformanceAnalysis>& historical_data) const;
    
    // Threshold analysis
    bool meets_performance_targets(const PerformanceAnalysis& analysis, 
                                  const PoolOptimizationConfig& config) const;

private:
    // Internal methods
    double calculate_throughput(const std::vector<QueryMetrics>& metrics) const;
    std::vector<double> calculate_latency_percentiles(const std::vector<QueryMetrics>& metrics) const;
    double calculate_error_rate(const std::vector<QueryMetrics>& metrics) const;
    double calculate_connection_efficiency(const ConnectionPoolMetrics& metrics) const;
    double calculate_query_efficiency(const std::vector<QueryMetrics>& metrics) const;
    
    // Statistical utilities
    double calculate_percentile(const std::vector<double>& values, double percentile) const;
    double calculate_average(const std::vector<double>& values) const;
    double calculate_standard_deviation(const std::vector<double>& values) const;
};

} // namespace database
} // namespace sonet