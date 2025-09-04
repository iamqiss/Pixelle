/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "performance_monitor.h"
#include "query_cache.h"
#include "connection_pool_optimizer.h"
#include <memory>
#include <vector>
#include <string>
#include <chrono>
#include <functional>
#include <atomic>
#include <thread>
#include <random>

namespace sonet {
namespace database {

// Load test configuration
struct LoadTestConfig {
    // Test parameters
    size_t concurrent_users = 10;
    size_t total_requests = 1000;
    std::chrono::seconds test_duration{300}; // 5 minutes
    std::chrono::milliseconds think_time{100}; // 100ms between requests
    
    // Ramp-up and ramp-down
    std::chrono::seconds ramp_up_time{60};   // 1 minute to reach full load
    std::chrono::seconds ramp_down_time{60}; // 1 minute to reduce load
    
    // Performance targets
    double target_throughput = 100.0;        // requests per second
    std::chrono::milliseconds target_latency_p95{100}; // 95th percentile latency
    double target_error_rate = 0.01;         // 1% max error rate
    
    // Test scenarios
    bool enable_stress_testing = false;
    bool enable_endurance_testing = false;
    bool enable_spike_testing = false;
    
    // Constructor
    LoadTestConfig() = default;
    
    // Constructor with custom values
    LoadTestConfig(size_t users, size_t requests, std::chrono::seconds duration)
        : concurrent_users(users), total_requests(requests), test_duration(duration) {}
};

// Load test result metrics
struct LoadTestMetrics {
    // Overall test results
    std::chrono::system_clock::time_point test_start_time;
    std::chrono::system_clock::time_point test_end_time;
    std::chrono::seconds total_duration;
    
    // Request statistics
    size_t total_requests;
    size_t successful_requests;
    size_t failed_requests;
    size_t timeout_requests;
    
    // Performance metrics
    double throughput;                    // requests per second
    double avg_response_time;             // average response time
    double min_response_time;             // minimum response time
    double max_response_time;             // maximum response time
    double p50_response_time;             // 50th percentile response time
    double p90_response_time;             // 90th percentile response time
    double p95_response_time;             // 95th percentile response time
    double p99_response_time;             // 99th percentile response time
    
    // Error metrics
    double error_rate;                    // percentage of failed requests
    std::vector<std::string> error_types; // types of errors encountered
    
    // Resource utilization
    double cpu_utilization;               // CPU usage percentage
    double memory_utilization;            // Memory usage percentage
    double connection_pool_utilization;   // Connection pool usage percentage
    
    // Constructor
    LoadTestMetrics() 
        : total_requests(0), successful_requests(0), failed_requests(0), timeout_requests(0),
          throughput(0.0), avg_response_time(0.0), min_response_time(0.0), max_response_time(0.0),
          p50_response_time(0.0), p90_response_time(0.0), p95_response_time(0.0), p99_response_time(0.0),
          error_rate(0.0), cpu_utilization(0.0), memory_utilization(0.0), connection_pool_utilization(0.0) {}
};

// Individual request result
struct RequestResult {
    size_t request_id;
    std::string request_type;
    std::chrono::microseconds response_time;
    bool success;
    std::string error_message;
    std::chrono::system_clock::time_point timestamp;
    
    // Constructor
    RequestResult() : request_id(0), response_time(0), success(false) {}
};

// Load test scenario definition
struct LoadTestScenario {
    std::string name;
    std::string description;
    std::function<std::unique_ptr<pg_result>()> query_executor;
    double weight; // relative frequency of this scenario
    
    // Constructor
    LoadTestScenario() : weight(1.0) {}
    
    // Constructor with custom values
    LoadTestScenario(const std::string& n, const std::string& desc, 
                     std::function<std::unique_ptr<pg_result>()> executor, double w = 1.0)
        : name(n), description(desc), query_executor(executor), weight(w) {}
};

// Main load tester class
class LoadTester {
public:
    explicit LoadTester(const LoadTestConfig& config = {});
    ~LoadTester();
    
    // Prevent copying
    LoadTester(const LoadTester&) = delete;
    LoadTester& operator=(const LoadTester&) = delete;
    
    // Configuration
    void set_config(const LoadTestConfig& config);
    LoadTestConfig get_config() const;
    
    // Test scenarios
    void add_scenario(const LoadTestScenario& scenario);
    void remove_scenario(const std::string& scenario_name);
    void clear_scenarios();
    
    // Test execution
    LoadTestMetrics run_load_test();
    LoadTestMetrics run_stress_test();
    LoadTestMetrics run_endurance_test();
    LoadTestMetrics run_spike_test();
    
    // Test control
    void start_test();
    void stop_test();
    void pause_test();
    void resume_test();
    
    // Real-time monitoring
    LoadTestMetrics get_current_metrics() const;
    bool is_test_running() const;
    double get_test_progress() const;
    
    // Event callbacks
    using TestProgressCallback = std::function<void(double progress, const LoadTestMetrics& metrics)>;
    using TestCompleteCallback = std::function<void(const LoadTestMetrics& metrics)>;
    
    void set_progress_callback(TestProgressCallback callback);
    void set_complete_callback(TestCompleteCallback callback);

private:
    // Member variables
    LoadTestConfig config_;
    std::vector<LoadTestScenario> scenarios_;
    LoadTestMetrics current_metrics_;
    
    std::atomic<bool> test_running_{false};
    std::atomic<bool> test_paused_{false};
    std::atomic<size_t> completed_requests_{0};
    std::atomic<size_t> failed_requests_{0};
    
    std::chrono::system_clock::time_point test_start_time_;
    std::vector<RequestResult> request_results_;
    
    // Callbacks
    TestProgressCallback progress_callback_;
    TestCompleteCallback complete_callback_;
    
    // Internal methods
    void execute_test_scenario(const LoadTestScenario& scenario, size_t user_id);
    void update_metrics(const RequestResult& result);
    void calculate_final_metrics();
    void generate_test_report();
    
    // Test execution helpers
    void ramp_up_users();
    void ramp_down_users();
    void execute_user_workload(size_t user_id);
    void wait_for_think_time();
    
    // Utility methods
    double calculate_percentile(const std::vector<double>& values, double percentile) const;
    void log_test_event(const std::string& event, const std::string& details);
};

// Benchmark suite for specific database operations
class DatabaseBenchmark {
public:
    explicit DatabaseBenchmark();
    
    // Benchmark types
    enum class BenchmarkType {
        READ_PERFORMANCE,
        WRITE_PERFORMANCE,
        MIXED_WORKLOAD,
        CONNECTION_POOL,
        QUERY_CACHE,
        TRANSACTION_PERFORMANCE,
        CONCURRENT_ACCESS
    };
    
    // Benchmark configuration
    struct BenchmarkConfig {
        BenchmarkType type;
        size_t iterations;
        size_t warmup_iterations;
        std::chrono::seconds duration;
        bool enable_metrics_collection;
        
        // Constructor
        BenchmarkConfig() 
            : type(BenchmarkType::READ_PERFORMANCE), iterations(1000), 
              warmup_iterations(100), duration(std::chrono::seconds{60}), 
              enable_metrics_collection(true) {}
    };
    
    // Benchmark results
    struct BenchmarkResult {
        std::string benchmark_name;
        BenchmarkType type;
        size_t iterations;
        std::chrono::microseconds total_time;
        std::chrono::microseconds avg_time;
        std::chrono::microseconds min_time;
        std::chrono::microseconds max_time;
        std::chrono::microseconds p95_time;
        std::chrono::microseconds p99_time;
        double throughput; // operations per second
        size_t errors;
        double error_rate;
        
        // Constructor
        BenchmarkResult() 
            : iterations(0), total_time(0), avg_time(0), min_time(0), max_time(0),
              p95_time(0), p99_time(0), throughput(0.0), errors(0), error_rate(0.0) {}
    };
    
    // Benchmark execution
    BenchmarkResult run_benchmark(const BenchmarkConfig& config);
    std::vector<BenchmarkResult> run_benchmark_suite();
    
    // Specific benchmarks
    BenchmarkResult benchmark_read_performance(size_t iterations = 1000);
    BenchmarkResult benchmark_write_performance(size_t iterations = 1000);
    BenchmarkResult benchmark_mixed_workload(size_t iterations = 1000);
    BenchmarkResult benchmark_connection_pool(size_t iterations = 1000);
    BenchmarkResult benchmark_query_cache(size_t iterations = 1000);
    BenchmarkResult benchmark_transactions(size_t iterations = 1000);
    BenchmarkResult benchmark_concurrent_access(size_t iterations = 1000);
    
    // Results analysis
    std::string generate_benchmark_report(const std::vector<BenchmarkResult>& results) const;
    std::vector<std::string> get_benchmark_recommendations(const BenchmarkResult& result) const;
    bool meets_performance_targets(const BenchmarkResult& result, 
                                  const std::vector<double>& targets) const;

private:
    // Member variables
    std::vector<BenchmarkResult> benchmark_results_;
    
    // Internal methods
    void warmup_benchmark(const BenchmarkConfig& config);
    void collect_metrics(const BenchmarkConfig& config);
    double calculate_throughput(size_t operations, std::chrono::microseconds total_time) const;
    std::vector<double> calculate_percentiles(const std::vector<std::chrono::microseconds>& times) const;
    
    // Benchmark implementations
    void execute_read_benchmark(size_t iterations, std::vector<std::chrono::microseconds>& times);
    void execute_write_benchmark(size_t iterations, std::vector<std::chrono::microseconds>& times);
    void execute_mixed_benchmark(size_t iterations, std::vector<std::chrono::microseconds>& times);
    void execute_connection_pool_benchmark(size_t iterations, std::vector<std::chrono::microseconds>& times);
    void execute_query_cache_benchmark(size_t iterations, std::vector<std::chrono::microseconds>& times);
    void execute_transaction_benchmark(size_t iterations, std::vector<std::chrono::microseconds>& times);
    void execute_concurrent_benchmark(size_t iterations, std::vector<std::chrono::microseconds>& times);
};

// Performance regression detector
class PerformanceRegressionDetector {
public:
    explicit PerformanceRegressionDetector();
    
    // Regression detection configuration
    struct RegressionConfig {
        double threshold_percentage = 10.0;  // 10% performance degradation threshold
        size_t min_data_points = 10;         // minimum data points for detection
        bool enable_trend_analysis = true;   // enable trend-based detection
        double confidence_level = 0.95;      // statistical confidence level
        
        // Constructor
        RegressionConfig() = default;
    };
    
    // Regression analysis result
    struct RegressionAnalysis {
        bool regression_detected;
        double degradation_percentage;
        std::string metric_name;
        std::string confidence_level;
        std::vector<std::string> recommendations;
        
        // Constructor
        RegressionAnalysis() : regression_detected(false), degradation_percentage(0.0) {}
    };
    
    // Analysis methods
    RegressionAnalysis detect_regression(const std::vector<LoadTestMetrics>& historical_data,
                                       const LoadTestMetrics& current_data);
    RegressionAnalysis detect_regression(const std::vector<BenchmarkResult>& historical_data,
                                       const BenchmarkResult& current_data);
    
    // Trend analysis
    bool is_performance_declining(const std::vector<double>& metrics) const;
    double calculate_degradation_rate(const std::vector<double>& metrics) const;
    
    // Statistical analysis
    double calculate_statistical_significance(const std::vector<double>& baseline,
                                            const std::vector<double>& current) const;
    bool is_change_significant(double p_value, double confidence_level) const;

private:
    // Member variables
    RegressionConfig config_;
    
    // Internal methods
    double calculate_percentage_change(double baseline, double current) const;
    std::vector<double> extract_metric_values(const std::vector<LoadTestMetrics>& data,
                                             const std::string& metric_name) const;
    std::vector<double> extract_metric_values(const std::vector<BenchmarkResult>& data,
                                             const std::string& metric_name) const;
};

} // namespace database
} // namespace sonet