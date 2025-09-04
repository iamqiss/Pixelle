/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "include/messaging_service.hpp"
#include <signal.h>
#include <thread>
#include <chrono>
#include <fstream>
#include <sstream>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "grpc/messaging_grpc_service.h"

namespace sonet::messaging {

// ServiceConfig implementation
Json::Value ServiceConfig::to_json() const {
    Json::Value json;
    json["service_name"] = service_name;
    json["version"] = version;
    json["environment"] = environment;
    json["http_port"] = http_port;
    json["websocket_port"] = websocket_port;
    json["grpc_port"] = grpc_port;
    json["database_url"] = database_url;
    json["redis_url"] = redis_url;
    json["max_connections"] = max_connections;
    json["max_file_size"] = static_cast<Json::UInt64>(max_file_size);
    json["enable_encryption"] = enable_encryption;
    json["enable_monitoring"] = enable_monitoring;
    json["log_level"] = log_level;
    json["storage_path"] = storage_path;
    json["metrics_port"] = metrics_port;
    return json;
}

ServiceConfig ServiceConfig::from_json(const Json::Value& json) {
    ServiceConfig config;
    config.service_name = json.get("service_name", "messaging_service").asString();
    config.version = json.get("version", "1.0.0").asString();
    config.environment = json.get("environment", "development").asString();
    config.http_port = json.get("http_port", 8080).asUInt();
    config.websocket_port = json.get("websocket_port", 8081).asUInt();
    config.grpc_port = json.get("grpc_port", 8082).asUInt();
    config.database_url = json.get("database_url", "").asString();
    config.redis_url = json.get("redis_url", "").asString();
    config.max_connections = json.get("max_connections", 10000).asUInt();
    config.max_file_size = json.get("max_file_size", 104857600).asUInt64(); // 100MB
    config.enable_encryption = json.get("enable_encryption", true).asBool();
    config.enable_monitoring = json.get("enable_monitoring", true).asBool();
    config.log_level = json.get("log_level", "INFO").asString();
    config.storage_path = json.get("storage_path", "/tmp/sonet/messaging").asString();
    config.metrics_port = json.get("metrics_port", 9090).asUInt();
    return config;
}

// ServiceMetrics implementation
Json::Value ServiceMetrics::to_json() const {
    Json::Value json;
    json["uptime_seconds"] = static_cast<Json::UInt64>(uptime_seconds);
    json["total_messages_sent"] = static_cast<Json::UInt64>(total_messages_sent);
    json["total_messages_received"] = static_cast<Json::UInt64>(total_messages_received);
    json["active_connections"] = active_connections;
    json["active_chats"] = active_chats;
    json["total_users"] = total_users;
    json["messages_per_second"] = messages_per_second;
    json["cpu_usage_percent"] = cpu_usage_percent;
    json["memory_usage_mb"] = memory_usage_mb;
    json["disk_usage_mb"] = disk_usage_mb;
    json["network_in_mbps"] = network_in_mbps;
    json["network_out_mbps"] = network_out_mbps;
    json["error_rate"] = error_rate;
    json["cache_hit_rate"] = cache_hit_rate;
    json["database_connections"] = database_connections;
    json["queue_size"] = queue_size;
    json["last_updated"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_updated.time_since_epoch()).count();
    return json;
}

void ServiceMetrics::reset() {
    total_messages_sent = 0;
    total_messages_received = 0;
    active_connections = 0;
    active_chats = 0;
    total_users = 0;
    messages_per_second = 0.0;
    cpu_usage_percent = 0.0;
    memory_usage_mb = 0.0;
    disk_usage_mb = 0.0;
    network_in_mbps = 0.0;
    network_out_mbps = 0.0;
    error_rate = 0.0;
    cache_hit_rate = 0.0;
    database_connections = 0;
    queue_size = 0;
    last_updated = std::chrono::system_clock::now();
}

void ServiceMetrics::update_message_stats(bool sent, uint64_t count) {
    if (sent) {
        total_messages_sent += count;
    } else {
        total_messages_received += count;
    }
    last_updated = std::chrono::system_clock::now();
}

// MessagingService implementation
MessagingService::MessagingService() : running_(false), start_time_(std::chrono::system_clock::now()) {
    // Load default configuration
    config_ = load_default_config();
    
    // Initialize components
    init_components();
    
    // Set up signal handlers
    setup_signal_handlers();
}

MessagingService::MessagingService(const ServiceConfig& config) 
    : config_(config), running_(false), start_time_(std::chrono::system_clock::now()) {
    
    // Initialize components
    init_components();
    
    // Set up signal handlers
    setup_signal_handlers();
}

MessagingService::~MessagingService() {
    stop();
}

bool MessagingService::start() {
    if (running_.load()) {
        return false; // Already running
    }
    
    try {
        // Validate configuration
        if (!validate_config()) {
            return false;
        }
        
        // Initialize storage
        if (!init_storage()) {
            return false;
        }
        
        // Start database connections
        if (!init_database()) {
            return false;
        }
        
        // Start Redis cache
        if (!init_cache()) {
            return false;
        }
        
        // Start messaging controller (HTTP + WebSocket)
        messaging_controller_ = std::make_unique<api::MessagingController>(
            config_.http_port, config_.websocket_port);
        
        if (!messaging_controller_->start()) {
            return false;
        }
        
        // Start gRPC server
        if (!start_grpc_server()) {
            messaging_controller_->stop();
            return false;
        }
        
        // Start monitoring if enabled
        if (config_.enable_monitoring) {
            start_monitoring();
        }
        
        // Start background threads
        start_background_threads();
        
        running_ = true;
        start_time_ = std::chrono::system_clock::now();
        
        // Log service start
        log_info("Messaging service started successfully");
        log_info("HTTP server listening on port " + std::to_string(config_.http_port));
        log_info("WebSocket server listening on port " + std::to_string(config_.websocket_port));
        log_info("gRPC server listening on port " + std::to_string(config_.grpc_port));
        
        return true;
        
    } catch (const std::exception& e) {
        log_error("Failed to start messaging service: " + std::string(e.what()));
        return false;
    }
}

void MessagingService::stop() {
    if (!running_.load()) {
        return; // Not running
    }
    
    log_info("Stopping messaging service...");
    
    running_ = false;
    
    // Stop background threads
    stop_background_threads();
    
    // Stop monitoring
    stop_monitoring();
    
    // Stop gRPC server
    stop_grpc_server();
    
    // Stop messaging controller
    if (messaging_controller_) {
        messaging_controller_->stop();
        messaging_controller_.reset();
    }
    
    // Close cache connections
    shutdown_cache();
    
    // Close database connections
    shutdown_database();
    
    log_info("Messaging service stopped");
}

bool MessagingService::is_running() const {
    return running_.load();
}

ServiceConfig MessagingService::get_config() const {
    return config_;
}

ServiceMetrics MessagingService::get_metrics() const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    ServiceMetrics metrics = metrics_;
    
    // Update uptime
    auto now = std::chrono::system_clock::now();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_);
    metrics.uptime_seconds = uptime.count();
    
    return metrics;
}

bool MessagingService::load_config_from_file(const std::string& config_path) {
    try {
        std::ifstream file(config_path);
        if (!file.is_open()) {
            log_error("Failed to open config file: " + config_path);
            return false;
        }
        
        Json::Value config_json;
        Json::Reader reader;
        
        if (!reader.parse(file, config_json)) {
            log_error("Failed to parse config file: " + reader.getFormattedErrorMessages());
            return false;
        }
        
        config_ = ServiceConfig::from_json(config_json);
        
        log_info("Configuration loaded from: " + config_path);
        return true;
        
    } catch (const std::exception& e) {
        log_error("Error loading config: " + std::string(e.what()));
        return false;
    }
}

void MessagingService::wait_for_shutdown() {
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

ServiceConfig MessagingService::load_default_config() {
    ServiceConfig config;
    config.service_name = "messaging_service";
    config.version = "1.0.0";
    config.environment = "development";
    config.http_port = 8080;
    config.websocket_port = 8081;
    config.grpc_port = 8082;
    config.database_url = "postgresql://localhost:5432/sonet_messaging";
    config.redis_url = "redis://localhost:6379/0";
    config.max_connections = 10000;
    config.max_file_size = 100 * 1024 * 1024; // 100MB
    config.enable_encryption = true;
    config.enable_monitoring = true;
    config.log_level = "INFO";
    config.storage_path = "/tmp/sonet/messaging";
    config.metrics_port = 9090;
    return config;
}

void MessagingService::init_components() {
    // Initialize metrics
    metrics_.reset();
    
    // Initialize logging
    init_logging();
}

void MessagingService::setup_signal_handlers() {
    // Set up SIGINT and SIGTERM handlers for graceful shutdown
    signal(SIGINT, [](int signal) {
        // This would be handled by a static instance reference
        // For now, just log
    });
    
    signal(SIGTERM, [](int signal) {
        // This would be handled by a static instance reference
        // For now, just log
    });
}

bool MessagingService::validate_config() {
    if (config_.service_name.empty()) {
        log_error("Service name cannot be empty");
        return false;
    }
    
    if (config_.http_port == 0 || config_.websocket_port == 0 || config_.grpc_port == 0) {
        log_error("All port numbers must be specified");
        return false;
    }
    
    if (config_.http_port == config_.websocket_port || 
        config_.http_port == config_.grpc_port || 
        config_.websocket_port == config_.grpc_port) {
        log_error("All ports must be unique");
        return false;
    }
    
    if (config_.max_connections == 0) {
        log_error("Maximum connections must be greater than 0");
        return false;
    }
    
    if (config_.storage_path.empty()) {
        log_error("Storage path cannot be empty");
        return false;
    }
    
    return true;
}

bool MessagingService::init_storage() {
    try {
        // Create storage directories
        std::string mkdir_cmd = "mkdir -p " + config_.storage_path + "/uploads";
        if (std::system(mkdir_cmd.c_str()) != 0) {
            log_error("Failed to create storage directory: " + config_.storage_path);
            return false;
        }
        
        mkdir_cmd = "mkdir -p " + config_.storage_path + "/temp";
        if (std::system(mkdir_cmd.c_str()) != 0) {
            log_error("Failed to create temp directory");
            return false;
        }
        
        mkdir_cmd = "mkdir -p " + config_.storage_path + "/logs";
        if (std::system(mkdir_cmd.c_str()) != 0) {
            log_error("Failed to create logs directory");
            return false;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        log_error("Storage initialization error: " + std::string(e.what()));
        return false;
    }
}

bool MessagingService::init_database() {
    // Initialize database connection pool
    // This would typically use a connection pool library like libpqxx for postgresql
    
    log_info("Initializing database connection...");
    
    try {
        // TODO: Implement actual database initialization
        // For now, just log success
        log_info("Database connection initialized: " + config_.database_url);
        
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        metrics_.database_connections = 10; // Simulated
        
        return true;
        
    } catch (const std::exception& e) {
        log_error("Database initialization failed: " + std::string(e.what()));
        return false;
    }
}

bool MessagingService::init_cache() {
    // Initialize Redis connection
    log_info("Initializing Redis cache...");
    
    try {
        // TODO: Implement actual Redis initialization
        // For now, just log success
        log_info("Redis cache initialized: " + config_.redis_url);
        
        return true;
        
    } catch (const std::exception& e) {
        log_error("Cache initialization failed: " + std::string(e.what()));
        return false;
    }
}

bool MessagingService::start_grpc_server() {
	try {
		// Initialize gRPC server
		log_info("Starting gRPC server on port " + std::to_string(config_.grpc_port));
		
		grpc::EnableDefaultHealthCheckService(true);
		grpc::reflection::InitProtoReflectionServerBuilderPlugin();
		
		grpc::ServerBuilder builder;
		std::string address = "0.0.0.0:" + std::to_string(config_.grpc_port);
		builder.AddListeningPort(address, grpc::InsecureServerCredentials());
		
		// Register real gRPC messaging service implementation
		static ::sonet::messaging::grpc_impl::MessagingGrpcService service_impl;
		builder.RegisterService(&service_impl);
		
		grpc_server_ = builder.BuildAndStart();
		if (!grpc_server_) {
			log_error("Failed to start gRPC server");
			return false;
		}
		
		grpc_server_thread_ = std::thread([this]() {
			run_grpc_server();
		});
		
		return true;
		
	} catch (const std::exception& e) {
		log_error("gRPC server startup failed: " + std::string(e.what()));
		return false;
	}
}

void MessagingService::stop_grpc_server() {
	if (grpc_server_) {
		grpc_server_->Shutdown();
	}
	if (grpc_server_thread_.joinable()) {
		grpc_server_thread_.join();
	}
	log_info("gRPC server stopped");
}

void MessagingService::run_grpc_server() {
	if (grpc_server_) {
		grpc_server_->Wait();
	}
}

void MessagingService::start_monitoring() {
    log_info("Starting monitoring on port " + std::to_string(config_.metrics_port));
    
    monitoring_thread_ = std::thread([this]() {
        run_monitoring_server();
    });
}

void MessagingService::stop_monitoring() {
    if (monitoring_thread_.joinable()) {
        monitoring_thread_.join();
    }
    
    log_info("Monitoring stopped");
}

void MessagingService::run_monitoring_server() {
    // Run Prometheus metrics server
    while (running_.load()) {
        // TODO: Implement Prometheus metrics endpoint
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void MessagingService::start_background_threads() {
    // Start metrics collection thread
    metrics_thread_ = std::thread([this]() {
        while (running_.load()) {
            collect_metrics();
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    });
    
    // Start health check thread
    health_check_thread_ = std::thread([this]() {
        while (running_.load()) {
            perform_health_checks();
            std::this_thread::sleep_for(std::chrono::seconds(30));
        }
    });
    
    // Start cleanup thread
    cleanup_thread_ = std::thread([this]() {
        while (running_.load()) {
            perform_cleanup();
            std::this_thread::sleep_for(std::chrono::minutes(5));
        }
    });
}

void MessagingService::stop_background_threads() {
    if (metrics_thread_.joinable()) {
        metrics_thread_.join();
    }
    
    if (health_check_thread_.joinable()) {
        health_check_thread_.join();
    }
    
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

void MessagingService::collect_metrics() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    // Update metrics
    metrics_.last_updated = std::chrono::system_clock::now();
    
    // Collect system metrics
    collect_system_metrics();
    
    // Collect application metrics
    collect_application_metrics();
}

void MessagingService::collect_system_metrics() {
    // TODO: Implement system metrics collection
    // This would use system calls or libraries like libproc
    
    metrics_.cpu_usage_percent = 15.5; // Simulated
    metrics_.memory_usage_mb = 256.0; // Simulated
    metrics_.disk_usage_mb = 1024.0; // Simulated
    metrics_.network_in_mbps = 10.0; // Simulated
    metrics_.network_out_mbps = 8.5; // Simulated
}

void MessagingService::collect_application_metrics() {
    // Collect messaging-specific metrics
    if (messaging_controller_) {
        // Get WebSocket metrics
        // auto ws_metrics = messaging_controller_->get_websocket_metrics();
        // metrics_.active_connections = ws_metrics.total_connections;
        
        // Simulated values
        metrics_.active_connections = 1250;
        metrics_.active_chats = 450;
        metrics_.total_users = 5000;
        metrics_.messages_per_second = 25.5;
        metrics_.error_rate = 0.01; // 1%
        metrics_.cache_hit_rate = 0.95; // 95%
        metrics_.queue_size = 15;
    }
}

void MessagingService::perform_health_checks() {
    log_debug("Performing health checks...");
    
    // Check database connectivity
    bool db_healthy = check_database_health();
    
    // Check cache connectivity
    bool cache_healthy = check_cache_health();
    
    // Check storage availability
    bool storage_healthy = check_storage_health();
    
    if (!db_healthy || !cache_healthy || !storage_healthy) {
        log_warning("Health check failed - DB: " + std::to_string(db_healthy) + 
                   ", Cache: " + std::to_string(cache_healthy) + 
                   ", Storage: " + std::to_string(storage_healthy));
    }
}

bool MessagingService::check_database_health() {
    // TODO: Implement database health check
    return true; // Simulated
}

bool MessagingService::check_cache_health() {
    // TODO: Implement cache health check
    return true; // Simulated
}

bool MessagingService::check_storage_health() {
    // Check if storage directory is accessible
    return std::filesystem::exists(config_.storage_path) && 
           std::filesystem::is_directory(config_.storage_path);
}

void MessagingService::perform_cleanup() {
    log_debug("Performing cleanup tasks...");
    
    // Clean up temporary files
    cleanup_temp_files();
    
    // Clean up expired sessions
    cleanup_expired_sessions();
    
    // Clean up old logs
    cleanup_old_logs();
}

void MessagingService::cleanup_temp_files() {
    try {
        std::string temp_dir = config_.storage_path + "/temp";
        
        // TODO: Implement temp file cleanup
        // Delete files older than 1 hour
        
    } catch (const std::exception& e) {
        log_error("Temp file cleanup failed: " + std::string(e.what()));
    }
}

void MessagingService::cleanup_expired_sessions() {
    // TODO: Implement session cleanup
}

void MessagingService::cleanup_old_logs() {
    try {
        std::string logs_dir = config_.storage_path + "/logs";
        
        // TODO: Implement log file cleanup
        // Keep logs for 30 days
        
    } catch (const std::exception& e) {
        log_error("Log cleanup failed: " + std::string(e.what()));
    }
}

void MessagingService::shutdown_database() {
    log_info("Shutting down database connections...");
    
    // TODO: Close database connection pool
    
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    metrics_.database_connections = 0;
}

void MessagingService::shutdown_cache() {
    log_info("Shutting down cache connections...");
    
    // TODO: Close Redis connections
}

void MessagingService::init_logging() {
    // TODO: Initialize structured logging
    // This would typically use a logging library like spdlog
}

void MessagingService::log_info(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cout << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [INFO] " << message << std::endl;
}

void MessagingService::log_warning(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cout << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [WARN] " << message << std::endl;
}

void MessagingService::log_error(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cerr << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [ERROR] " << message << std::endl;
}

void MessagingService::log_debug(const std::string& message) {
    if (config_.log_level == "DEBUG") {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        
        std::cout << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
                  << "] [DEBUG] " << message << std::endl;
    }
}

} // namespace sonet::messaging
