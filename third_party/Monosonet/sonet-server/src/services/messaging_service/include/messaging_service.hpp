/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <memory>
#include <chrono>
#include <atomic>
#include <thread>
#include <json/json.h>
#include <grpcpp/grpcpp.h>
#include "messaging_controller.hpp"
#include "../../core/config/config_manager.hpp"
#include "../../core/logging/logger.hpp"
#include "../../core/database/connection_pool.hpp"
#include "../../core/cache/redis_client.hpp"

namespace sonet::messaging {

struct ServiceConfiguration {
    // Network configuration
    std::string host = "0.0.0.0";
    uint16_t http_port = 8086;
    uint16_t grpc_port = 9090;
    uint16_t websocket_port = 9096;
    
    // Database configuration
    std::string database_host = "localhost";
    uint16_t database_port = 5432;
    std::string database_name = "messaging_service";
    std::string database_user = "messaging_user";
    std::string database_password = "";
    uint32_t database_pool_size = 50;
    
    // Redis configuration
    std::string redis_host = "localhost";
    uint16_t redis_port = 6379;
    uint32_t redis_database = 2;
    std::string redis_password = "";
    uint32_t redis_pool_size = 20;
    
    // Storage configuration
    std::string storage_type = "filesystem"; // filesystem, s3, gcs
    std::string storage_base_path = "/var/lib/sonet/messaging/attachments";
    uint64_t max_attachment_size = 104857600; // 100MB
    uint64_t max_message_size = 10485760;     // 10MB
    
    // Encryption configuration
    bool encryption_enabled = true;
    bool e2e_encryption_enabled = true;
    std::string encryption_algorithm = "AES-256-GCM";
    uint32_t key_rotation_hours = 24;
    bool quantum_resistant_mode = false;
    
    // Performance configuration
    uint32_t max_connections = 10000;
    uint32_t worker_threads = 8;
    uint32_t message_buffer_size = 1000;
    uint32_t websocket_ping_interval_seconds = 30;
    uint32_t connection_timeout_seconds = 300;
    
    // Rate limiting
    uint32_t messages_per_minute_limit = 60;
    uint32_t uploads_per_hour_limit = 50;
    uint32_t api_requests_per_minute = 1000;
    
    // Message retention
    uint32_t message_retention_days = 365;
    uint32_t media_retention_days = 90;
    bool auto_delete_expired = true;
    
    // Features
    bool typing_indicators_enabled = true;
    bool read_receipts_enabled = true;
    bool message_reactions_enabled = true;
    bool disappearing_messages_enabled = true;
    bool file_uploads_enabled = true;
    bool message_search_enabled = true;
    
    // Monitoring
    bool metrics_enabled = true;
    bool health_checks_enabled = true;
    std::string metrics_endpoint = "/metrics";
    std::string health_endpoint = "/health";
    
    Json::Value to_json() const;
    static ServiceConfiguration from_json(const Json::Value& json);
    bool is_valid() const;
};

struct ServiceMetrics {
    // Service uptime
    std::chrono::system_clock::time_point service_start_time;
    std::chrono::milliseconds uptime;
    
    // Request metrics
    uint64_t total_requests = 0;
    uint64_t successful_requests = 0;
    uint64_t failed_requests = 0;
    std::chrono::milliseconds average_response_time;
    uint32_t requests_per_second = 0;
    
    // Message metrics
    uint64_t messages_sent = 0;
    uint64_t messages_received = 0;
    uint64_t messages_encrypted = 0;
    uint64_t messages_failed = 0;
    uint32_t messages_per_second = 0;
    
    // Connection metrics
    uint32_t active_connections = 0;
    uint32_t peak_connections = 0;
    uint32_t total_connections = 0;
    uint32_t failed_connections = 0;
    
    // Storage metrics
    uint64_t storage_used_bytes = 0;
    uint64_t attachments_stored = 0;
    uint64_t messages_stored = 0;
    uint32_t database_connections = 0;
    uint32_t redis_connections = 0;
    
    // Error metrics
    uint32_t encryption_errors = 0;
    uint32_t database_errors = 0;
    uint32_t network_errors = 0;
    uint32_t validation_errors = 0;
    
    Json::Value to_json() const;
    void update_request_metrics(bool success, std::chrono::milliseconds response_time);
    void update_message_metrics(bool sent, bool encrypted, bool success);
    void update_connection_metrics(uint32_t active, bool connection_success);
    void update_storage_metrics(uint64_t bytes_used, bool attachment_stored);
    void increment_error_count(const std::string& error_type);
};

class MessagingService {
private:
    // Core configuration
    ServiceConfiguration config_;
    
    // Core components
    std::unique_ptr<MessagingController> controller_;
    std::unique_ptr<MessagingAPIHandler> api_handler_;
    
    // Infrastructure
    std::shared_ptr<core::config::ConfigManager> config_manager_;
    std::shared_ptr<core::logging::Logger> logger_;
    std::shared_ptr<core::database::ConnectionPool> database_pool_;
    std::shared_ptr<core::cache::RedisClient> redis_client_;
    
    // HTTP/gRPC servers
    std::unique_ptr<grpc::Server> grpc_server_;
    std::thread http_server_thread_;
    std::thread grpc_server_thread_;
    
    // Service state
    std::atomic<bool> running_;
    std::atomic<bool> shutdown_requested_;
    std::chrono::system_clock::time_point start_time_;
    
    // Metrics and monitoring
    ServiceMetrics metrics_;
    std::mutex metrics_mutex_;
    std::thread metrics_update_thread_;
    
    // Background tasks
    std::thread health_check_thread_;
    std::thread cleanup_thread_;
    std::thread key_rotation_thread_;
    
    // Internal methods
    bool initialize_database();
    bool initialize_redis();
    bool initialize_storage();
    bool initialize_encryption();
    bool start_http_server();
    bool start_grpc_server();
    bool start_websocket_server();
    void start_background_tasks();
    void stop_background_tasks();
    void update_metrics();
    void perform_health_checks();
    void perform_cleanup_tasks();
    void rotate_encryption_keys();
    void handle_shutdown_signal();
    
public:
    MessagingService();
    ~MessagingService();
    
    // Service lifecycle
    bool initialize(const std::string& config_file = "");
    bool start();
    void shutdown();
    void wait_for_shutdown();
    bool is_running() const;
    
    // Configuration
    bool load_configuration(const std::string& config_file);
    bool reload_configuration();
    ServiceConfiguration get_configuration() const;
    bool update_configuration(const ServiceConfiguration& new_config);
    
    // Health and monitoring
    Json::Value get_health_status();
    Json::Value get_service_metrics();
    Json::Value get_detailed_metrics();
    ServiceMetrics get_metrics() const;
    bool perform_health_check();
    
    // Component access (for testing and monitoring)
    std::shared_ptr<MessagingController> get_controller() const;
    std::shared_ptr<MessagingAPIHandler> get_api_handler() const;
    std::shared_ptr<core::logging::Logger> get_logger() const;
    
    // Administrative operations
    void force_cleanup();
    void reset_metrics();
    void reload_encryption_keys();
    void rebuild_database_indexes();
    void compact_database();
    void backup_data(const std::string& backup_path);
    bool restore_data(const std::string& backup_path);
    
    // Debugging and diagnostics
    Json::Value get_system_info();
    Json::Value get_connection_info();
    Json::Value get_database_info();
    Json::Value get_redis_info();
    std::vector<std::string> get_active_sessions();
    
    // Signal handling
    void setup_signal_handlers();
    static void signal_handler(int signal);
    
private:
    static MessagingService* instance_;
};

// Service utilities
class ServiceUtils {
public:
    static std::string get_service_version();
    static std::string get_build_info();
    static Json::Value get_system_resources();
    static bool check_port_availability(uint16_t port);
    static std::string format_uptime(std::chrono::milliseconds uptime);
    static std::string format_bytes(uint64_t bytes);
    static std::string format_requests_per_second(uint32_t rps);
    static bool validate_service_configuration(const ServiceConfiguration& config);
    static Json::Value create_health_response(bool healthy, const std::string& details = "");
    static Json::Value create_metrics_response(const ServiceMetrics& metrics);
    static std::vector<std::string> get_required_environment_variables();
    static bool check_environment_variables();
    static std::string get_default_config_path();
    static bool create_default_config_file(const std::string& path);
};

// Service factory for dependency injection
class MessagingServiceFactory {
public:
    static std::unique_ptr<MessagingService> create_service();
    static std::unique_ptr<MessagingService> create_service_with_config(const ServiceConfiguration& config);
    static std::unique_ptr<MessagingController> create_controller(const ServiceConfiguration& config);
    static std::unique_ptr<MessagingAPIHandler> create_api_handler(std::shared_ptr<MessagingController> controller);
    
    // For testing
    static std::unique_ptr<MessagingService> create_test_service();
    static std::unique_ptr<MessagingService> create_minimal_service();
};

// Exception classes for service-specific errors
class MessagingServiceException : public std::exception {
private:
    std::string message_;
    std::string error_code_;
    
public:
    MessagingServiceException(const std::string& message, const std::string& error_code = "");
    const char* what() const noexcept override;
    const std::string& get_error_code() const;
};

class ConfigurationException : public MessagingServiceException {
public:
    ConfigurationException(const std::string& message);
};

class InitializationException : public MessagingServiceException {
public:
    InitializationException(const std::string& message);
};

class DatabaseException : public MessagingServiceException {
public:
    DatabaseException(const std::string& message);
};

class EncryptionException : public MessagingServiceException {
public:
    EncryptionException(const std::string& message);
};

} // namespace sonet::messaging
