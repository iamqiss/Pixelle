/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "connection_pool_optimizer.h"
#include <algorithm>
#include <chrono>
#include <thread>
#include <spdlog/spdlog.h>

namespace sonet {
namespace database {

// ConnectionPoolOptimizer implementation
ConnectionPoolOptimizer::ConnectionPoolOptimizer(const PoolOptimizationConfig& config)
    : config_(config), performance_monitor_(PerformanceMonitor::get_instance()) {
    spdlog::info("ConnectionPoolOptimizer initialized with min_connections={}, max_connections={}", 
                 config_.min_connections, config_.max_connections);
}

ConnectionPoolOptimizer::~ConnectionPoolOptimizer() {
    stop_optimization();
    
    if (optimization_thread_.joinable()) {
        optimization_thread_.join();
    }
    
    if (health_monitor_thread_.joinable()) {
        health_monitor_thread_.join();
    }
    
    spdlog::info("ConnectionPoolOptimizer destroyed");
}

void ConnectionPoolOptimizer::set_config(const PoolOptimizationConfig& config) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    config_ = config;
    spdlog::info("ConnectionPoolOptimizer configuration updated");
}

PoolOptimizationConfig ConnectionPoolOptimizer::get_config() const {
    return config_;
}

void ConnectionPoolOptimizer::start_optimization() {
    if (optimization_running_) {
        spdlog::warn("Optimization already running");
        return;
    }
    
    optimization_running_ = true;
    optimization_paused_ = false;
    
    // Start background threads
    optimization_thread_ = std::thread(&ConnectionPoolOptimizer::optimization_loop, this);
    health_monitor_thread_ = std::thread(&ConnectionPoolOptimizer::health_monitor_loop, this);
    
    spdlog::info("Connection pool optimization started");
}

void ConnectionPoolOptimizer::stop_optimization() {
    if (!optimization_running_) {
        return;
    }
    
    optimization_running_ = false;
    
    if (optimization_thread_.joinable()) {
        optimization_thread_.join();
    }
    
    if (health_monitor_thread_.joinable()) {
        health_monitor_thread_.join();
    }
    
    spdlog::info("Connection pool optimization stopped");
}

void ConnectionPoolOptimizer::pause_optimization() {
    optimization_paused_ = true;
    spdlog::info("Connection pool optimization paused");
}

void ConnectionPoolOptimizer::resume_optimization() {
    optimization_paused_ = false;
    spdlog::info("Connection pool optimization resumed");
}

PoolHealthMetrics ConnectionPoolOptimizer::get_pool_health() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return current_health_;
}

bool ConnectionPoolOptimizer::is_pool_healthy() const {
    auto health = get_pool_health();
    return health.overall_health == ConnectionHealth::HEALTHY || 
           health.overall_health == ConnectionHealth::DEGRADED;
}

void ConnectionPoolOptimizer::perform_health_check() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    update_pool_health();
    
    if (health_alert_callback_) {
        trigger_health_alert(current_health_.overall_health, 
                           "Health check completed");
    }
}

void ConnectionPoolOptimizer::optimize_pool_size() {
    if (!optimization_running_ || optimization_paused_) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t optimal_size = calculate_optimal_pool_size();
    if (optimal_size != current_health_.total_connections) {
        spdlog::info("Pool size optimization: current={}, optimal={}", 
                     current_health_.total_connections, optimal_size);
        
        // In a real implementation, you would resize the actual connection pool
        // For now, just log the recommendation
        log_optimization_event("pool_size_optimization", 
                             "Recommended pool size: " + std::to_string(optimal_size));
    }
}

void ConnectionPoolOptimizer::optimize_connection_timeouts() {
    if (!optimization_running_ || optimization_paused_) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto optimal_timeout = calculate_optimal_timeout();
    if (optimal_timeout != config_.connection_idle_timeout) {
        spdlog::info("Connection timeout optimization: current={}s, optimal={}s", 
                     config_.connection_idle_timeout.count(), optimal_timeout.count());
        
        log_optimization_event("timeout_optimization", 
                             "Recommended timeout: " + std::to_string(optimal_timeout.count()) + "s");
    }
}

void ConnectionPoolOptimizer::optimize_connection_recycling() {
    if (!optimization_running_ || optimization_paused_) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Analyze connection health and recommend recycling
    if (current_health_.unhealthy_connections > 0) {
        spdlog::info("Connection recycling recommended: {} unhealthy connections detected", 
                     current_health_.unhealthy_connections);
        
        log_optimization_event("connection_recycling", 
                             "Unhealthy connections: " + std::to_string(current_health_.unhealthy_connections));
    }
}

std::vector<std::string> ConnectionPoolOptimizer::get_optimization_recommendations() const {
    std::vector<std::string> recommendations;
    
    auto health = get_pool_health();
    
    if (health.overall_health == ConnectionHealth::CRITICAL) {
        recommendations.push_back("Immediate attention required: Connection pool is in critical state");
    }
    
    if (health.error_rate > config_.max_error_rate) {
        recommendations.push_back("Error rate exceeds threshold: Investigate connection issues");
    }
    
    if (health.utilization_rate > 0.9) {
        recommendations.push_back("High utilization: Consider increasing pool size");
    }
    
    if (health.connection_wait_time_avg > config_.max_connection_wait_time.count()) {
        recommendations.push_back("Connection wait time too high: Optimize pool configuration");
    }
    
    return recommendations;
}

std::string ConnectionPoolOptimizer::generate_optimization_report() const {
    auto health = get_pool_health();
    auto recommendations = get_optimization_recommendations();
    
    std::stringstream report;
    report << "Connection Pool Optimization Report\n";
    report << "==================================\n\n";
    
    report << "Overall Health: ";
    switch (health.overall_health) {
        case ConnectionHealth::HEALTHY: report << "HEALTHY"; break;
        case ConnectionHealth::DEGRADED: report << "DEGRADED"; break;
        case ConnectionHealth::UNHEALTHY: report << "UNHEALTHY"; break;
        case ConnectionHealth::CRITICAL: report << "CRITICAL"; break;
    }
    report << " (Score: " << std::fixed << std::setprecision(2) << health.health_score << ")\n\n";
    
    report << "Connection Statistics:\n";
    report << "  Total: " << health.total_connections << "\n";
    report << "  Healthy: " << health.healthy_connections << "\n";
    report << "  Degraded: " << health.degraded_connections << "\n";
    report << "  Unhealthy: " << health.unhealthy_connections << "\n\n";
    
    report << "Performance Metrics:\n";
    report << "  Avg Response Time: " << health.avg_response_time << "ms\n";
    report << "  Error Rate: " << (health.error_rate * 100) << "%\n";
    report << "  Utilization: " << (health.utilization_rate * 100) << "%\n";
    report << "  Avg Wait Time: " << health.connection_wait_time_avg << "ms\n";
    report << "  Max Wait Time: " << health.connection_wait_time_max << "ms\n\n";
    
    if (!recommendations.empty()) {
        report << "Recommendations:\n";
        for (const auto& rec : recommendations) {
            report << "  - " << rec << "\n";
        }
    }
    
    return report.str();
}

void ConnectionPoolOptimizer::set_health_alert_callback(HealthAlertCallback callback) {
    health_alert_callback_ = callback;
}

size_t ConnectionPoolOptimizer::get_optimization_count() const {
    return optimization_count_;
}

std::chrono::system_clock::time_point ConnectionPoolOptimizer::get_last_optimization() const {
    return last_optimization_;
}

double ConnectionPoolOptimizer::get_optimization_effectiveness() const {
    return optimization_effectiveness_;
}

// Private methods
void ConnectionPoolOptimizer::optimization_loop() {
    while (optimization_running_) {
        if (!optimization_paused_) {
            try {
                // Perform optimizations
                optimize_pool_size();
                optimize_connection_timeouts();
                optimize_connection_recycling();
                
                optimization_count_++;
                last_optimization_ = std::chrono::system_clock::now();
                
                // Calculate effectiveness based on health improvement
                auto previous_health = current_health_.health_score;
                update_pool_health();
                auto current_health = current_health_.health_score;
                
                if (current_health > previous_health) {
                    optimization_effectiveness_ = std::min(1.0, optimization_effectiveness_ + 0.1);
                } else if (current_health < previous_health) {
                    optimization_effectiveness_ = std::max(0.0, optimization_effectiveness_ - 0.05);
                }
                
            } catch (const std::exception& e) {
                spdlog::error("Error in optimization loop: {}", e.what());
            }
        }
        
        // Wait before next optimization cycle
        std::this_thread::sleep_for(std::chrono::seconds(30));
    }
}

void ConnectionPoolOptimizer::health_monitor_loop() {
    while (optimization_running_) {
        try {
            perform_health_check();
        } catch (const std::exception& e) {
            spdlog::error("Error in health monitor loop: {}", e.what());
        }
        
        // Wait before next health check
        std::this_thread::sleep_for(config_.health_check_interval);
    }
}

void ConnectionPoolOptimizer::update_pool_health() {
    // Get current metrics from performance monitor
    auto pool_metrics = performance_monitor_.get_connection_pool_metrics();
    
    // Update health metrics
    current_health_.total_connections = pool_metrics.total_connections;
    current_health_.healthy_connections = pool_metrics.healthy_connections;
    current_health_.degraded_connections = pool_metrics.degraded_connections;
    current_health_.unhealthy_connections = pool_metrics.unhealthy_connections;
    
    // Calculate performance indicators
    current_health_.avg_response_time = calculate_response_time();
    current_health_.error_rate = calculate_error_rate();
    current_health_.utilization_rate = calculate_utilization_rate();
    current_health_.connection_wait_time_avg = pool_metrics.avg_wait_time_ms;
    current_health_.connection_wait_time_max = pool_metrics.max_wait_time_ms;
    
    // Calculate overall health score
    current_health_.health_score = calculate_health_score();
    current_health_.overall_health = calculate_health_score();
}

ConnectionHealth ConnectionPoolOptimizer::calculate_health_score() const {
    double score = 1.0;
    
    // Deduct points for various issues
    if (current_health_.error_rate > config_.max_error_rate) {
        score -= 0.3;
    }
    
    if (current_health_.utilization_rate > 0.9) {
        score -= 0.2;
    }
    
    if (current_health_.connection_wait_time_avg > config_.max_connection_wait_time.count()) {
        score -= 0.2;
    }
    
    if (current_health_.unhealthy_connections > 0) {
        score -= 0.3;
    }
    
    // Determine health status
    if (score >= 0.8) return ConnectionHealth::HEALTHY;
    if (score >= 0.6) return ConnectionHealth::DEGRADED;
    if (score >= 0.4) return ConnectionHealth::UNHEALTHY;
    return ConnectionHealth::CRITICAL;
}

void ConnectionPoolOptimizer::trigger_health_alert(ConnectionHealth health, const std::string& message) {
    if (health_alert_callback_) {
        health_alert_callback_(health, message);
    }
    
    // Log the alert
    std::string health_str;
    switch (health) {
        case ConnectionHealth::HEALTHY: health_str = "HEALTHY"; break;
        case ConnectionHealth::DEGRADED: health_str = "DEGRADED"; break;
        case ConnectionHealth::UNHEALTHY: health_str = "UNHEALTHY"; break;
        case ConnectionHealth::CRITICAL: health_str = "CRITICAL"; break;
    }
    
    spdlog::warn("Health alert: {} - {}", health_str, message);
}

size_t ConnectionPoolOptimizer::calculate_optimal_pool_size() const {
    // Simple algorithm: base size on utilization and error rate
    size_t base_size = config_.min_connections;
    
    if (current_health_.utilization_rate > 0.8) {
        base_size = std::min(config_.max_connections, 
                            static_cast<size_t>(base_size * 1.5));
    }
    
    if (current_health_.error_rate > config_.max_error_rate) {
        base_size = std::min(config_.max_connections, 
                            static_cast<size_t>(base_size * 1.2));
    }
    
    return std::clamp(base_size, config_.min_connections, config_.max_connections);
}

std::chrono::seconds ConnectionPoolOptimizer::calculate_optimal_timeout() const {
    // Adjust timeout based on error rate and response time
    auto base_timeout = config_.connection_idle_timeout;
    
    if (current_health_.error_rate > config_.max_error_rate) {
        // Reduce timeout to recycle connections more frequently
        return std::chrono::seconds{static_cast<long>(base_timeout.count() * 0.7)};
    }
    
    if (current_health_.avg_response_time > 1000) { // > 1 second
        // Increase timeout to reduce connection churn
        return std::chrono::seconds{static_cast<long>(base_timeout.count() * 1.3)};
    }
    
    return base_timeout;
}

bool ConnectionPoolOptimizer::should_recycle_connection(size_t connection_age, size_t error_count) const {
    // Recycle if connection is too old
    if (connection_age > config_.max_connection_age.count()) {
        return true;
    }
    
    // Recycle if too many errors
    if (error_count > config_.max_failed_health_checks) {
        return true;
    }
    
    return false;
}

double ConnectionPoolOptimizer::calculate_utilization_rate() const {
    if (current_health_.total_connections == 0) return 0.0;
    return static_cast<double>(current_health_.healthy_connections + 
                              current_health_.degraded_connections) / 
           current_health_.total_connections;
}

double ConnectionPoolOptimizer::calculate_error_rate() const {
    // This would be calculated from actual error metrics
    // For now, return a placeholder value
    return 0.0;
}

double ConnectionPoolOptimizer::calculate_response_time() const {
    // This would be calculated from actual response time metrics
    // For now, return a placeholder value
    return 0.0;
}

void ConnectionPoolOptimizer::log_optimization_event(const std::string& event, const std::string& details) {
    spdlog::info("Optimization event: {} - {}", event, details);
}

bool ConnectionPoolOptimizer::should_perform_optimization() const {
    auto now = std::chrono::system_clock::now();
    auto time_since_last = std::chrono::duration_cast<std::chrono::minutes>(
        now - last_optimization_).count();
    
    return time_since_last >= 5; // Optimize every 5 minutes
}

// ConnectionLoadBalancer implementation
ConnectionLoadBalancer::ConnectionLoadBalancer(size_t pool_count)
    : current_strategy_(Strategy::ROUND_ROBIN), health_aware_routing_(false) {
    
    pool_weights_.resize(pool_count, 1.0);
    pool_health_.resize(pool_count, ConnectionHealth::HEALTHY);
    selection_counts_.resize(pool_count, 0);
    pool_utilization_.resize(pool_count, 0.0);
    
    spdlog::info("ConnectionLoadBalancer initialized with {} pools", pool_count);
}

void ConnectionLoadBalancer::set_strategy(Strategy strategy) {
    current_strategy_ = strategy;
    spdlog::info("Load balancing strategy changed to {}", static_cast<int>(strategy));
}

void ConnectionLoadBalancer::set_pool_weights(const std::vector<double>& weights) {
    if (weights.size() != pool_weights_.size()) {
        spdlog::warn("Weights size mismatch: expected {}, got {}", 
                     pool_weights_.size(), weights.size());
        return;
    }
    
    pool_weights_ = weights;
    spdlog::info("Pool weights updated");
}

void ConnectionLoadBalancer::enable_health_aware_routing(bool enable) {
    health_aware_routing_ = enable;
    spdlog::info("Health-aware routing {}", enable ? "enabled" : "disabled");
}

size_t ConnectionLoadBalancer::select_pool(const std::string& query_type) {
    size_t selected_pool = 0;
    
    switch (current_strategy_) {
        case Strategy::ROUND_ROBIN:
            selected_pool = round_robin_selection();
            break;
        case Strategy::LEAST_CONNECTIONS:
            selected_pool = least_connections_selection();
            break;
        case Strategy::WEIGHTED_ROUND_ROBIN:
            selected_pool = weighted_round_robin_selection();
            break;
        case Strategy::ADAPTIVE_LOAD_BALANCING:
            selected_pool = adaptive_load_balancing_selection(query_type);
            break;
    }
    
    // Update statistics
    selection_counts_[selected_pool]++;
    total_selections_++;
    update_pool_statistics(selected_pool);
    
    return selected_pool;
}

size_t ConnectionLoadBalancer::select_pool_for_user(const std::string& user_id) {
    // Simple hash-based selection for user affinity
    std::hash<std::string> hasher;
    size_t hash = hasher(user_id);
    return hash % pool_weights_.size();
}

size_t ConnectionLoadBalancer::select_pool_for_table(const std::string& table_name) {
    // Simple hash-based selection for table affinity
    std::hash<std::string> hasher;
    size_t hash = hasher(table_name);
    return hash % pool_weights_.size();
}

void ConnectionLoadBalancer::add_pool(size_t pool_id, double weight) {
    if (pool_id >= pool_weights_.size()) {
        pool_weights_.resize(pool_id + 1, weight);
        pool_health_.resize(pool_id + 1, ConnectionHealth::HEALTHY);
        selection_counts_.resize(pool_id + 1, 0);
        pool_utilization_.resize(pool_id + 1, 0.0);
    } else {
        pool_weights_[pool_id] = weight;
    }
    
    spdlog::info("Added pool {} with weight {}", pool_id, weight);
}

void ConnectionLoadBalancer::remove_pool(size_t pool_id) {
    if (pool_id < pool_weights_.size()) {
        // Note: In a real implementation, you'd need to handle active connections
        spdlog::info("Removed pool {}", pool_id);
    }
}

void ConnectionLoadBalancer::update_pool_health(size_t pool_id, ConnectionHealth health) {
    if (pool_id < pool_health_.size()) {
        pool_health_[pool_id] = health;
        spdlog::debug("Updated pool {} health to {}", pool_id, static_cast<int>(health));
    }
}

std::vector<size_t> ConnectionLoadBalancer::get_pool_selection_counts() const {
    return selection_counts_;
}

double ConnectionLoadBalancer::get_pool_utilization(size_t pool_id) const {
    if (pool_id < pool_utilization_.size()) {
        return pool_utilization_[pool_id];
    }
    return 0.0;
}

ConnectionLoadBalancer::Strategy ConnectionLoadBalancer::get_current_strategy() const {
    return current_strategy_;
}

// Private methods
size_t ConnectionLoadBalancer::round_robin_selection() {
    size_t selected = current_pool_index_ % pool_weights_.size();
    current_pool_index_++;
    return selected;
}

size_t ConnectionLoadBalancer::least_connections_selection() {
    size_t selected = 0;
    size_t min_connections = selection_counts_[0];
    
    for (size_t i = 1; i < selection_counts_.size(); ++i) {
        if (selection_counts_[i] < min_connections) {
            min_connections = selection_counts_[i];
            selected = i;
        }
    }
    
    return selected;
}

size_t ConnectionLoadBalancer::weighted_round_robin_selection() {
    // Simple weighted round-robin implementation
    static size_t current_weight_index = 0;
    static double current_weight = 0.0;
    
    while (true) {
        if (current_weight <= 0) {
            current_weight_index = (current_weight_index + 1) % pool_weights_.size();
            current_weight = pool_weights_[current_weight_index];
        }
        
        current_weight -= 1.0;
        return current_weight_index;
    }
}

size_t ConnectionLoadBalancer::adaptive_load_balancing_selection(const std::string& query_type) {
    // Adaptive selection based on query type and pool health
    size_t best_pool = 0;
    double best_score = 0.0;
    
    for (size_t i = 0; i < pool_weights_.size(); ++i) {
        double score = calculate_pool_score(i);
        
        if (score > best_score) {
            best_score = score;
            best_pool = i;
        }
    }
    
    return best_pool;
}

void ConnectionLoadBalancer::update_pool_statistics(size_t pool_id) {
    if (pool_id < pool_utilization_.size()) {
        // Simple utilization calculation based on selection count
        pool_utilization_[pool_id] = static_cast<double>(selection_counts_[pool_id]) / 
                                    std::max(total_selections_, 1UL);
    }
}

double ConnectionLoadBalancer::calculate_pool_score(size_t pool_id) const {
    if (pool_id >= pool_weights_.size()) return 0.0;
    
    double score = pool_weights_[pool_id];
    
    // Adjust score based on health
    if (health_aware_routing_) {
        switch (pool_health_[pool_id]) {
            case ConnectionHealth::HEALTHY:
                score *= 1.0;
                break;
            case ConnectionHealth::DEGRADED:
                score *= 0.7;
                break;
            case ConnectionHealth::UNHEALTHY:
                score *= 0.3;
                break;
            case ConnectionHealth::CRITICAL:
                score *= 0.1;
                break;
        }
    }
    
    // Adjust score based on utilization (prefer less utilized pools)
    if (pool_id < pool_utilization_.size()) {
        score *= (1.0 - pool_utilization_[pool_id]);
    }
    
    return score;
}

// ConnectionPerformanceAnalyzer implementation
ConnectionPerformanceAnalyzer::ConnectionPerformanceAnalyzer() {
    spdlog::info("ConnectionPerformanceAnalyzer initialized");
}

ConnectionPerformanceAnalyzer::PerformanceAnalysis 
ConnectionPerformanceAnalyzer::analyze_pool_performance(const std::vector<QueryMetrics>& metrics) const {
    PerformanceAnalysis analysis;
    
    if (metrics.empty()) {
        return analysis;
    }
    
    // Calculate throughput
    analysis.throughput = calculate_throughput(metrics);
    
    // Calculate latency percentiles
    auto percentiles = calculate_latency_percentiles(metrics);
    if (percentiles.size() >= 3) {
        analysis.latency_p50 = percentiles[0];
        analysis.latency_p95 = percentiles[1];
        analysis.latency_p99 = percentiles[2];
    }
    
    // Calculate error rate
    analysis.error_rate = calculate_error_rate(metrics);
    
    // Calculate efficiencies
    analysis.connection_efficiency = 0.8; // Placeholder
    analysis.query_efficiency = 0.9;      // Placeholder
    
    return analysis;
}

ConnectionPerformanceAnalyzer::PerformanceAnalysis 
ConnectionPerformanceAnalyzer::analyze_connection_performance(const ConnectionPoolMetrics& metrics) const {
    PerformanceAnalysis analysis;
    
    // Analyze connection pool metrics
    analysis.throughput = static_cast<double>(metrics.total_connections);
    analysis.connection_efficiency = static_cast<double>(metrics.healthy_connections) / 
                                   std::max(metrics.total_connections, 1UL);
    
    return analysis;
}

std::vector<std::string> ConnectionPerformanceAnalyzer::get_performance_recommendations(
    const PerformanceAnalysis& analysis) const {
    std::vector<std::string> recommendations;
    
    if (analysis.latency_p95 > 1000) { // > 1 second
        recommendations.push_back("95th percentile latency is high - investigate slow queries");
    }
    
    if (analysis.error_rate > 0.05) { // > 5%
        recommendations.push_back("Error rate is high - check connection health and configuration");
    }
    
    if (analysis.connection_efficiency < 0.8) { // < 80%
        recommendations.push_back("Connection efficiency is low - optimize pool configuration");
    }
    
    return recommendations;
}

std::string ConnectionPerformanceAnalyzer::generate_performance_analysis_report(
    const PerformanceAnalysis& analysis) const {
    std::stringstream report;
    report << "Performance Analysis Report\n";
    report << "==========================\n\n";
    
    report << "Throughput: " << std::fixed << std::setprecision(2) << analysis.throughput << " ops/sec\n";
    report << "Latency (50th percentile): " << analysis.latency_p50 << " ms\n";
    report << "Latency (95th percentile): " << analysis.latency_p95 << " ms\n";
    report << "Latency (99th percentile): " << analysis.latency_p99 << " ms\n";
    report << "Error Rate: " << (analysis.error_rate * 100) << "%\n";
    report << "Connection Efficiency: " << (analysis.connection_efficiency * 100) << "%\n";
    report << "Query Efficiency: " << (analysis.query_efficiency * 100) << "%\n\n";
    
    auto recommendations = get_performance_recommendations(analysis);
    if (!recommendations.empty()) {
        report << "Recommendations:\n";
        for (const auto& rec : recommendations) {
            report << "  - " << rec << "\n";
        }
    }
    
    return report.str();
}

bool ConnectionPerformanceAnalyzer::is_performance_improving(
    const std::vector<PerformanceAnalysis>& historical_data) const {
    if (historical_data.size() < 2) return false;
    
    // Simple trend analysis: compare latest with previous
    const auto& latest = historical_data.back();
    const auto& previous = historical_data[historical_data.size() - 2];
    
    return latest.throughput > previous.throughput &&
           latest.latency_p95 < previous.latency_p95 &&
           latest.error_rate < previous.error_rate;
}

double ConnectionPerformanceAnalyzer::calculate_performance_trend(
    const std::vector<PerformanceAnalysis>& historical_data) const {
    if (historical_data.size() < 2) return 0.0;
    
    // Calculate average improvement across metrics
    double total_improvement = 0.0;
    int metric_count = 0;
    
    for (size_t i = 1; i < historical_data.size(); ++i) {
        const auto& current = historical_data[i];
        const auto& previous = historical_data[i - 1];
        
        if (previous.throughput > 0) {
            total_improvement += (current.throughput - previous.throughput) / previous.throughput;
            metric_count++;
        }
        
        if (previous.latency_p95 > 0) {
            total_improvement += (previous.latency_p95 - current.latency_p95) / previous.latency_p95;
            metric_count++;
        }
        
        if (previous.error_rate > 0) {
            total_improvement += (previous.error_rate - current.error_rate) / previous.error_rate;
            metric_count++;
        }
    }
    
    return metric_count > 0 ? total_improvement / metric_count : 0.0;
}

bool ConnectionPerformanceAnalyzer::meets_performance_targets(
    const PerformanceAnalysis& analysis, 
    const PoolOptimizationConfig& config) const {
    // Check if performance meets configured targets
    return analysis.latency_p95 < config.max_connection_wait_time.count() &&
           analysis.error_rate < config.max_error_rate;
}

// Private methods
double ConnectionPerformanceAnalyzer::calculate_throughput(const std::vector<QueryMetrics>& metrics) const {
    if (metrics.empty()) return 0.0;
    
    // Calculate average queries per second
    double total_time = 0.0;
    for (const auto& metric : metrics) {
        total_time += std::chrono::duration<double>(metric.execution_time).count();
    }
    
    return total_time > 0 ? metrics.size() / total_time : 0.0;
}

std::vector<double> ConnectionPerformanceAnalyzer::calculate_latency_percentiles(
    const std::vector<QueryMetrics>& metrics) const {
    std::vector<double> latencies;
    latencies.reserve(metrics.size());
    
    for (const auto& metric : metrics) {
        latencies.push_back(std::chrono::duration<double, std::milli>(metric.execution_time).count());
    }
    
    std::sort(latencies.begin(), latencies.end());
    
    std::vector<double> percentiles;
    percentiles.push_back(calculate_percentile(latencies, 0.5));  // P50
    percentiles.push_back(calculate_percentile(latencies, 0.95)); // P95
    percentiles.push_back(calculate_percentile(latencies, 0.99)); // P99
    
    return percentiles;
}

double ConnectionPerformanceAnalyzer::calculate_error_rate(const std::vector<QueryMetrics>& metrics) const {
    if (metrics.empty()) return 0.0;
    
    size_t error_count = 0;
    for (const auto& metric : metrics) {
        if (!metric.error_message.empty()) {
            error_count++;
        }
    }
    
    return static_cast<double>(error_count) / metrics.size();
}

double ConnectionPerformanceAnalyzer::calculate_connection_efficiency(const ConnectionPoolMetrics& metrics) const {
    if (metrics.total_connections == 0) return 0.0;
    return static_cast<double>(metrics.healthy_connections) / metrics.total_connections;
}

double ConnectionPerformanceAnalyzer::calculate_query_efficiency(const std::vector<QueryMetrics>& metrics) const {
    if (metrics.empty()) return 0.0;
    
    size_t successful_queries = 0;
    for (const auto& metric : metrics) {
        if (metric.error_message.empty()) {
            successful_queries++;
        }
    }
    
    return static_cast<double>(successful_queries) / metrics.size();
}

double ConnectionPerformanceAnalyzer::calculate_percentile(const std::vector<double>& values, double percentile) const {
    if (values.empty()) return 0.0;
    
    size_t index = static_cast<size_t>(percentile * (values.size() - 1));
    return values[index];
}

double ConnectionPerformanceAnalyzer::calculate_average(const std::vector<double>& values) const {
    if (values.empty()) return 0.0;
    
    double sum = 0.0;
    for (double value : values) {
        sum += value;
    }
    
    return sum / values.size();
}

double ConnectionPerformanceAnalyzer::calculate_standard_deviation(const std::vector<double>& values) const {
    if (values.size() < 2) return 0.0;
    
    double mean = calculate_average(values);
    double sum_squared_diff = 0.0;
    
    for (double value : values) {
        double diff = value - mean;
        sum_squared_diff += diff * diff;
    }
    
    return std::sqrt(sum_squared_diff / (values.size() - 1));
}

} // namespace database
} // namespace sonet