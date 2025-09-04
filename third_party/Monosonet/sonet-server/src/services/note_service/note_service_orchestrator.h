/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#ifndef SONET_NOTE_SERVICE_ORCHESTRATOR_H
#define SONET_NOTE_SERVICE_ORCHESTRATOR_H

#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>

#include "controllers/note_controller.h"
#include "controllers/attachment_controller.h"
#include "grpc/note_grpc_service.h"
#include "websocket/note_websocket_handler.h"

#include "service.h"
// #include "services/timeline_service.h"
// #include "services/notification_service.h"
// #include "services/analytics_service.h"

#include "../core/network/http_server.h"
#include "../core/network/websocket_server.h"
#include "../core/cache/redis_client.h"
#include "../core/database/notegres_client.h"
#include "../core/security/auth_service.h"
#include "../core/security/rate_limiter.h"
#include "../core/config/service_config.h"
#include "../core/logging/logger.h"
#include "../core/monitoring/health_checker.h"
#include "../core/monitoring/metrics_collector.h"

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

using json = nlohmann::json;

namespace sonet::note {

/**
 * @brief Twitter-Scale Note Service Orchestrator
 * 
 * Orchestrates all components of the note service including:
 * - HTTP REST API server with comprehensive endpoints
 * - gRPC high-performance service for inter-service communication
 * - WebSocket real-time features for live updates
 * - Background services for timeline generation and analytics
 * - Health monitoring and performance metrics
 * - Horizontal scaling and load balancing
 * - Service discovery and configuration management
 * - Graceful shutdown and disaster recovery
 */
class NoteServiceOrchestrator {
public:
    // Constructor
    explicit NoteServiceOrchestrator(std::shared_ptr<core::config::ServiceConfig> config);

    // Destructor
    ~NoteServiceOrchestrator();

    // ========== SERVICE LIFECYCLE ==========
    
    /**
     * @brief Initialize all service components
     * 
     * Initialization order:
     * 1. Database connections and migrations
     * 2. Cache and Redis setup
     * 3. Core business services
     * 4. Security and authentication
     * 5. HTTP/gRPC/WebSocket servers
     * 6. Background tasks and monitoring
     */
    bool initialize();

    /**
     * @brief Start all services in correct order
     * 
     * Startup sequence:
     * 1. Database health checks
     * 2. Cache warming
     * 3. gRPC service startup
     * 4. HTTP API server
     * 5. WebSocket real-time server
     * 6. Background services
     * 7. Health monitoring
     */
    bool start();

    /**
     * @brief Gracefully shutdown all services
     * 
     * Shutdown sequence:
     * 1. Stop accepting new connections
     * 2. Complete in-flight requests
     * 3. Close WebSocket connections
     * 4. Stop background tasks
     * 5. Flush caches and close databases
     * 6. Generate shutdown report
     */
    void shutdown();

    /**
     * @brief Check if service is healthy and ready
     */
    bool is_healthy() const;

    /**
     * @brief Check if service is ready to accept traffic
     */
    bool is_ready() const;

    // ========== SCALING AND PERFORMANCE ==========
    
    /**
     * @brief Get current service performance metrics
     */
    json get_performance_metrics() const;

    /**
     * @brief Get service health status
     */
    json get_health_status() const;

    /**
     * @brief Get real-time service statistics
     */
    json get_service_statistics() const;

    /**
     * @brief Trigger cache warming for better performance
     */
    void warm_caches();

    /**
     * @brief Optimize service for current workload pattern
     */
    void optimize_for_workload(const std::string& workload_type);

    // ========== CONFIGURATION MANAGEMENT ==========
    
    /**
     * @brief Reload configuration without downtime
     */
    bool reload_configuration();

    /**
     * @brief Update rate limiting configuration
     */
    void update_rate_limits(const json& rate_limit_config);

    /**
     * @brief Update cache configuration
     */
    void update_cache_config(const json& cache_config);

    // ========== MONITORING AND ALERTING ==========
    
    /**
     * @brief Register custom health check
     */
    void register_health_check(const std::string& name, std::function<bool()> check_function);

    /**
     * @brief Set performance alert thresholds
     */
    void set_alert_thresholds(const json& thresholds);

    /**
     * @brief Get recent error logs
     */
    json get_error_logs(int count = 100) const;

    /**
     * @brief Get performance trends
     */
    json get_performance_trends(const std::string& timeframe = "24h") const;

private:
    // ========== CONFIGURATION ==========
    std::shared_ptr<core::config::ServiceConfig> config_;

    // ========== CORE INFRASTRUCTURE ==========
    std::shared_ptr<core::database::NotegresClient> notegres_client_;
    std::shared_ptr<core::cache::RedisClient> redis_client_;
    std::shared_ptr<core::security::AuthService> auth_service_;
    std::shared_ptr<core::security::RateLimiter> rate_limiter_;
    std::shared_ptr<core::logging::Logger> logger_;
    std::shared_ptr<core::monitoring::HealthChecker> health_checker_;
    std::shared_ptr<core::monitoring::MetricsCollector> metrics_collector_;

    // ========== BUSINESS SERVICES ==========
    std::shared_ptr<repositories::NoteRepository> note_repository_;
    std::shared_ptr<NoteService> note_service_;
    // std::shared_ptr<services::TimelineService> timeline_service_;
    // std::shared_ptr<services::NotificationService> notification_service_;
    // std::shared_ptr<services::AnalyticsService> analytics_service_;

    // ========== API CONTROLLERS ==========
    std::shared_ptr<controllers::NoteController> note_controller_;
    std::shared_ptr<controllers::AttachmentController> attachment_controller_;

    // ========== NETWORK SERVICES ==========
    std::shared_ptr<core::network::HttpServer> http_server_;
    std::shared_ptr<core::network::WebSocketServer> websocket_server_;
    std::shared_ptr<grpc::NoteGrpcService> grpc_service_;
    std::shared_ptr<websocket::NoteWebSocketHandler> websocket_handler_;

    // ========== SERVICE STATE ==========
    std::atomic<bool> initialized_{false};
    std::atomic<bool> running_{false};
    std::atomic<bool> healthy_{false};
    std::atomic<bool> ready_{false};

    // ========== BACKGROUND TASKS ==========
    std::vector<std::thread> background_threads_;
    std::atomic<bool> background_tasks_running_{false};

    // ========== PERFORMANCE MONITORING ==========
    mutable std::mutex metrics_mutex_;
    std::chrono::steady_clock::time_point start_time_;
    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> successful_requests_{0};
    std::atomic<uint64_t> failed_requests_{0};
    std::atomic<uint64_t> websocket_connections_{0};
    std::atomic<uint64_t> grpc_requests_{0};

    // ========== INITIALIZATION METHODS ==========
    
    bool initialize_database();
    bool initialize_cache();
    bool initialize_security();
    bool initialize_business_services();
    bool initialize_controllers();
    bool initialize_network_services();
    bool initialize_monitoring();

    // ========== STARTUP METHODS ==========
    
    bool start_database_services();
    bool start_cache_services();
    bool start_grpc_service();
    bool start_http_service();
    bool start_websocket_service();
    bool start_background_services();
    bool start_monitoring_services();

    // ========== BACKGROUND TASKS ==========
    
    void start_background_tasks();
    void stop_background_tasks();
    
    // Background task implementations
    void timeline_generation_task();
    void analytics_aggregation_task();
    void cache_maintenance_task();
    void health_monitoring_task();
    void metrics_collection_task();
    void trending_calculation_task();
    void content_moderation_task();
    void database_cleanup_task();

    // ========== HEALTH AND MONITORING ==========
    
    void setup_health_checks();
    void setup_performance_monitoring();
    void setup_alerting();
    
    bool check_database_health();
    bool check_cache_health();
    bool check_service_health();
    bool check_network_health();
    
    void collect_performance_metrics();
    void check_performance_thresholds();
    void generate_health_report();

    // ========== CACHE MANAGEMENT ==========
    
    void setup_cache_warming();
    void warm_timeline_caches();
    void warm_user_caches();
    void warm_trending_caches();
    void warm_analytics_caches();

    // ========== CONFIGURATION HELPERS ==========
    
    void load_service_configuration();
    void validate_configuration();
    void apply_configuration_updates();
    
    // Configuration getters
    int get_http_port() const;
    int get_grpc_port() const;
    int get_websocket_port() const;
    std::string get_redis_url() const;
    std::string get_notegres_url() const;
    json get_rate_limit_config() const;
    json get_cache_config() const;

    // ========== ERROR HANDLING ==========
    
    void handle_initialization_error(const std::string& component, const std::exception& e);
    void handle_runtime_error(const std::string& component, const std::exception& e);
    void handle_shutdown_error(const std::string& component, const std::exception& e);
    
    void log_error(const std::string& message, const json& context = json::object());
    void log_warning(const std::string& message, const json& context = json::object());
    void log_info(const std::string& message, const json& context = json::object());

    // ========== UTILITY METHODS ==========
    
    std::string generate_service_id() const;
    std::string get_service_version() const;
    json get_system_information() const;
    json get_resource_usage() const;
    
    void register_signal_handlers();
    void handle_shutdown_signal(int signal);
    
    // ========== CONSTANTS ==========
    
    // Service configuration
    static constexpr int DEFAULT_HTTP_PORT = 8080;
    static constexpr int DEFAULT_GRPC_PORT = 9090;
    static constexpr int DEFAULT_WEBSOCKET_PORT = 8081;
    
    // Performance thresholds
    static constexpr int64_t REQUEST_TIMEOUT_MS = 30000;        // 30 seconds
    static constexpr int64_t GRPC_TIMEOUT_MS = 10000;          // 10 seconds
    static constexpr int64_t WEBSOCKET_PING_INTERVAL_MS = 30000; // 30 seconds
    
    // Resource limits
    static constexpr int MAX_HTTP_CONNECTIONS = 10000;
    static constexpr int MAX_GRPC_CONNECTIONS = 5000;
    static constexpr int MAX_WEBSOCKET_CONNECTIONS = 50000;
    static constexpr size_t MAX_MEMORY_USAGE_MB = 4096;       // 4GB
    
    // Background task intervals
    static constexpr int TIMELINE_GENERATION_INTERVAL_S = 60;   // 1 minute
    static constexpr int ANALYTICS_AGGREGATION_INTERVAL_S = 300; // 5 minutes
    static constexpr int CACHE_MAINTENANCE_INTERVAL_S = 1800;   // 30 minutes
    static constexpr int HEALTH_CHECK_INTERVAL_S = 30;         // 30 seconds
    static constexpr int METRICS_COLLECTION_INTERVAL_S = 60;   // 1 minute
    static constexpr int TRENDING_CALCULATION_INTERVAL_S = 300; // 5 minutes
    
    // Startup timeouts
    static constexpr int SERVICE_STARTUP_TIMEOUT_S = 300;      // 5 minutes
    static constexpr int GRACEFUL_SHUTDOWN_TIMEOUT_S = 120;    // 2 minutes
    static constexpr int DATABASE_CONNECTION_TIMEOUT_S = 30;    // 30 seconds
    static constexpr int CACHE_CONNECTION_TIMEOUT_S = 10;       // 10 seconds
};

/**
 * @brief Service Builder for Easy Configuration and Setup
 */
class NoteServiceBuilder {
public:
    static std::unique_ptr<NoteServiceOrchestrator> create_production_service(
        const std::string& config_file_path
    );
    
    static std::unique_ptr<NoteServiceOrchestrator> create_development_service(
        const json& dev_config = json::object()
    );
    
    static std::unique_ptr<NoteServiceOrchestrator> create_test_service(
        const json& test_config = json::object()
    );

private:
    static std::shared_ptr<core::config::ServiceConfig> load_production_config(
        const std::string& config_file_path
    );
    
    static std::shared_ptr<core::config::ServiceConfig> create_development_config(
        const json& overrides
    );
    
    static std::shared_ptr<core::config::ServiceConfig> create_test_config(
        const json& overrides
    );
};

/**
 * @brief Main Service Entry Point
 */
class NoteServiceApplication {
public:
    static int run(int argc, char* argv[]);
    
private:
    static void setup_signal_handlers();
    static void handle_graceful_shutdown(int signal);
    static void print_startup_banner();
    static void print_shutdown_message();
    
    static std::unique_ptr<NoteServiceOrchestrator> service_instance_;
};

} // namespace sonet::note

#endif // SONET_NOTE_SERVICE_ORCHESTRATOR_H
