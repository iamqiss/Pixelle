/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This defines the main notification service interface that orchestrates all components.
 * I designed this as the central facade that brings together processors, channels,
 * repositories, and controllers into one cohesive notification system.
 */

#pragma once

#include "models/notification.h"
#include <nlohmann/json.hpp>
#include <future>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>

namespace sonet {
namespace notification_service {

/**
 * Configuration for the NotificationService
 * 
 * I designed this config structure to hold all the settings needed
 * to run the notification service across different environments.
 */
struct NotificationServiceConfig {
    // Database configuration
    std::string database_url = "postgresql://localhost:5432/sonet";
    std::string redis_url = "redis://localhost:6379";
    int database_pool_size = 10;
    bool enable_caching = true;
    
    // SMTP configuration for email notifications
    std::string smtp_host = "localhost";
    int smtp_port = 587;
    std::string smtp_username;
    std::string smtp_password;
    bool smtp_use_tls = true;
    std::string email_from_name = "Sonet";
    std::string email_from_address = "notifications@sonet.app";
    int email_rate_limit_per_minute = 100;
    int email_rate_limit_per_hour = 1000;
    
    // Push notification configuration
    std::string fcm_server_key;
    std::string fcm_project_id;
    std::string apns_key_id;
    std::string apns_team_id;
    std::string apns_bundle_id;
    std::string apns_private_key;
    int push_rate_limit_per_minute = 500;
    int push_rate_limit_per_hour = 10000;
    
    // WebSocket configuration
    std::string websocket_host = "0.0.0.0";
    int websocket_port = 8080;
    int max_websocket_connections = 10000;
    
    // gRPC server configuration
    bool enable_grpc = true;
    std::string grpc_host = "0.0.0.0";
    int grpc_port = 50051;
    
    // HTTP server configuration
    bool enable_http = true;
    std::string http_host = "0.0.0.0";
    int http_port = 8081;
    
    // Processor configuration
    int processor_worker_threads = 4;
    int processor_batch_size = 100;
    int processor_batch_timeout_seconds = 5;
    bool enable_rate_limiting = true;
    bool enable_deduplication = true;
    bool enable_batching = true;
    
    // Authentication configuration
    bool enable_authentication = true;
    std::string jwt_secret;
    
    // API rate limiting
    int api_rate_limit_per_user_per_minute = 60;
    int max_notifications_per_request = 100;
    
    // Service configuration
    std::string service_name = "notification_service";
    std::string service_version = "1.0.0";
    
    /**
     * Load configuration from environment variables
     */
    static NotificationServiceConfig from_environment();
    
    /**
     * Load configuration from JSON file
     */
    static NotificationServiceConfig from_file(const std::string& file_path);
    
    /**
     * Validate the configuration
     */
    bool is_valid() const;
    
    /**
     * Convert to JSON for serialization
     */
    nlohmann::json to_json() const;
    
    /**
     * Load from JSON
     */
    static NotificationServiceConfig from_json(const nlohmann::json& json);
};

/**
 * Main notification service class
 * 
 * This is the central orchestrator that brings together all notification
 * components. I built this as a high-level facade that handles the
 * complexity of coordinating processors, channels, repositories, and controllers.
 * 
 * Key responsibilities:
 * - Service lifecycle management (start/stop)
 * - Component coordination and dependency injection
 * - Health monitoring and metrics collection
 * - API endpoints for notification operations
 * - Resource cleanup and graceful shutdown
 */
class NotificationService {
public:
    using Config = NotificationServiceConfig;
    
    /**
     * Constructor
     * 
     * @param config Service configuration
     */
    explicit NotificationService(const Config& config);
    
    /**
     * Destructor - ensures graceful shutdown
     */
    ~NotificationService();
    
    // Non-copyable and non-movable
    NotificationService(const NotificationService&) = delete;
    NotificationService& operator=(const NotificationService&) = delete;
    NotificationService(NotificationService&&) = delete;
    NotificationService& operator=(NotificationService&&) = delete;
    
    /**
     * Start the notification service
     * 
     * This initializes all components, starts the processor, and begins
     * listening for incoming requests. Returns true if successful.
     */
    bool start();
    
    /**
     * Stop the notification service
     * 
     * This performs a graceful shutdown, stopping all components
     * and cleaning up resources.
     */
    void stop();
    
    /**
     * Check if the service is currently running
     */
    bool is_running() const;
    
    /**
     * Check if the service is healthy
     * 
     * This performs a comprehensive health check across all components
     */
    bool is_healthy() const;
    
    /**
     * Send a single notification
     * 
     * This is the main entry point for sending notifications. The notification
     * will be processed asynchronously through the notification processor.
     * 
     * @param notification The notification to send
     * @return Future that resolves to true if successful
     */
    std::future<bool> send_notification(const models::Notification& notification);
    
    /**
     * Send multiple notifications efficiently
     * 
     * This batches multiple notifications for efficient processing.
     * 
     * @param notifications Vector of notifications to send
     * @return Future that resolves to true if all were successful
     */
    std::future<bool> send_bulk_notifications(const std::vector<models::Notification>& notifications);
    
    /**
     * Get detailed health status
     * 
     * Returns a comprehensive health report including status of all
     * components, database connectivity, and performance metrics.
     */
    nlohmann::json get_health_status() const;
    
    /**
     * Get service metrics
     * 
     * Returns detailed metrics about notification processing, delivery
     * rates, active connections, and performance statistics.
     */
    nlohmann::json get_service_metrics() const;
    
    /**
     * Get count of active WebSocket connections
     */
    int get_active_connection_count() const;
    
    /**
     * Get processor statistics
     * 
     * Returns statistics about notification processing including
     * throughput, queue sizes, and success rates.
     */
    nlohmann::json get_processor_statistics() const;
    
    /**
     * Register a device for push notifications
     * 
     * @param user_id User identifier
     * @param device_token Platform-specific device token
     * @param platform Platform name ("ios", "android", "web")
     * @return True if registration was successful
     */
    bool register_device(const std::string& user_id, 
                        const std::string& device_token,
                        const std::string& platform);
    
    /**
     * Unregister a device from push notifications
     * 
     * @param user_id User identifier
     * @param device_token Device token to remove
     * @return True if unregistration was successful
     */
    bool unregister_device(const std::string& user_id, const std::string& device_token);
    
    /**
     * Perform resource cleanup
     * 
     * This cleans up expired connections, invalid device tokens,
     * and other resources that need periodic maintenance.
     */
    void cleanup_resources();

private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
};

/**
 * Factory class for creating notification services
 * 
 * I created this factory to provide different service configurations
 * for different deployment scenarios (development, testing, production).
 */
class NotificationServiceFactory {
public:
    /**
     * Create a production-ready notification service
     * 
     * This creates a service with all features enabled and production
     * settings for reliability and performance.
     */
    static std::unique_ptr<NotificationService> create_production(const NotificationServiceConfig& config);
    
    /**
     * Create a development notification service
     * 
     * This creates a service with relaxed settings suitable for
     * development and testing, including mock implementations.
     */
    static std::unique_ptr<NotificationService> create_development(const NotificationServiceConfig& config);
    
    /**
     * Create a testing notification service
     * 
     * This creates a service with mock implementations for unit testing.
     */
    static std::unique_ptr<NotificationService> create_testing();
    
    /**
     * Create a minimal notification service
     * 
     * This creates a lightweight service with only essential features
     * enabled, suitable for resource-constrained environments.
     */
    static std::unique_ptr<NotificationService> create_minimal(const NotificationServiceConfig& config);
};

/**
 * Utility functions for service management
 */
namespace service_utils {
    /**
     * Validate service configuration
     */
    bool validate_config(const NotificationServiceConfig& config);
    
    /**
     * Load configuration from multiple sources
     * Priority: environment variables > config file > defaults
     */
    NotificationServiceConfig load_config(const std::string& config_file_path = "");
    
    /**
     * Setup logging for the service
     */
    void setup_logging(const std::string& log_level = "info");
    
    /**
     * Setup signal handlers for graceful shutdown
     */
    void setup_signal_handlers(NotificationService& service);
    
    /**
     * Wait for shutdown signal
     */
    void wait_for_shutdown();
}

} // namespace notification_service
} // namespace sonet