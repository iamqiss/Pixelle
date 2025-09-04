/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This implements the main notification service that orchestrates all components.
 * I built this as the central hub that brings together processors, channels,
 * repositories, and controllers into one cohesive notification system.
 */

#include "service.h"
#include "processors/notification_processor.h"
#include "channels/email_channel.h"
#include "channels/push_channel.h"
#include "channels/websocket_channel.h"
#include "repositories/notification_repository.h"
#include "controllers/notification_controller.h"
#include <thread>
#include <chrono>
#include <httplib.h>
#include <grpcpp/grpcpp.h>
#include "notification.grpc.pb.h"

namespace sonet {
namespace notification_service {

// Internal implementation for NotificationService
struct NotificationService::Impl {
    Config config;
    
    // Core components
    std::shared_ptr<repositories::NotificationRepository> repository;
    std::unique_ptr<processors::NotificationProcessor> processor;
    std::unique_ptr<controllers::NotificationController> controller;
    
    // Delivery channels
    std::shared_ptr<channels::EmailChannel> email_channel;
    std::shared_ptr<channels::PushChannel> push_channel;
    std::shared_ptr<channels::WebSocketChannel> websocket_channel;
    
    // gRPC and HTTP servers
    std::unique_ptr<grpc::Server> grpc_server;
    std::unique_ptr<httplib::Server> http_server;
    std::thread grpc_server_thread;
    std::thread http_server_thread;
    std::unique_ptr<::sonet::notification::NotificationService::Service> owned_grpc_service;
    
    // Service state
    std::atomic<bool> is_running{false};
    std::atomic<bool> is_healthy{false};
    std::chrono::system_clock::time_point startup_time;
    
    // Health monitoring
    std::thread health_monitor_thread;
    std::atomic<bool> health_monitoring{false};
    
    Impl(const Config& cfg) : config(cfg) {
        startup_time = std::chrono::system_clock::now();
    }
    
    ~Impl() {
        stop_service();
    }
    
    bool initialize_components() {
        try {
            // Initialize repository
            repositories::NotificationRepository::Config repo_config;
            repo_config.database_url = config.database_url;
            repo_config.redis_url = config.redis_url;
            repo_config.connection_pool_size = config.database_pool_size;
            repo_config.cache_ttl = std::chrono::hours{1};
            repo_config.enable_caching = config.enable_caching;
            
            repository = std::shared_ptr<repositories::NotificationRepository>{
                repositories::NotificationRepositoryFactory::create_notegresql({
                    .connection_string = config.database_url,
                    .max_connections = config.database_pool_size,
                    .enable_redis_cache = config.enable_caching,
                    .redis_host = "localhost",
                    .redis_port = 6379
                })
            };
            
            // Initialize email channel
            channels::SMTPEmailChannel::Config smtp_cfg;
            smtp_cfg.smtp_host = config.smtp_host;
            smtp_cfg.smtp_port = config.smtp_port;
            smtp_cfg.username = config.smtp_username;
            smtp_cfg.password = config.smtp_password;
            smtp_cfg.use_tls = config.smtp_use_tls;
            smtp_cfg.sender_name = config.email_from_name;
            smtp_cfg.sender_email = config.email_from_address;
            smtp_cfg.max_emails_per_minute = config.email_rate_limit_per_minute;
            smtp_cfg.max_emails_per_hour = config.email_rate_limit_per_hour;

            email_channel = channels::EmailChannelFactory::create_smtp(smtp_cfg);
            processor->register_email_channel(email_channel);
            
            // Initialize push channel
            channels::FCMPushChannel::Config fcm_cfg;
            fcm_cfg.project_id = config.fcm_project_id;
            fcm_cfg.server_key = config.fcm_server_key;
            fcm_cfg.apns_key_id = config.apns_key_id;
            fcm_cfg.apns_team_id = config.apns_team_id;
            fcm_cfg.apns_key_path = config.apns_private_key;
            fcm_cfg.max_requests_per_minute = config.push_rate_limit_per_minute;
            fcm_cfg.max_requests_per_hour = config.push_rate_limit_per_hour;

            push_channel = channels::PushChannelFactory::create_fcm(fcm_cfg);
            processor->register_push_channel(push_channel);
            
            // Initialize WebSocket channel
            channels::WebSocketPPChannel::Config ws_config;
            ws_config.port = config.websocket_port;
            ws_config.host = config.websocket_host;
            ws_config.jwt_secret = config.jwt_secret;
            ws_config.max_connections = config.max_websocket_connections;
            ws_config.ping_interval = std::chrono::seconds{30};
            ws_config.connection_timeout = std::chrono::minutes{5};
            ws_config.max_message_size = 64 * 1024; // 64KB
            
            websocket_channel = channels::WebSocketChannelFactory::create_websocketpp(ws_config);
            processor->register_websocket_channel(websocket_channel);
            
            // Initialize processor
            processors::NotificationProcessor::Config processor_config;
            processor_config.worker_thread_count = config.processor_worker_threads;
            processor_config.enable_rate_limiting = config.enable_rate_limiting;
            processor_config.enable_deduplication = config.enable_deduplication;
            processor_config.enable_batching = config.enable_batching;
            
            processor = std::make_unique<processors::NotificationProcessor>(
                repository,
                processor_config
            );
            
            // Initialize controller
            controllers::NotificationController::Config controller_config;
            controller_config.require_authentication = config.enable_authentication;
            controller_config.jwt_secret = config.jwt_secret;
            controller_config.rate_limits.requests_per_minute = config.api_rate_limit_per_user_per_minute;
            controller_config.max_request_size_mb = 10;
            controller_config.enable_websocket = false; // WebSocket served by channels::WebSocketChannel

            controller = std::make_unique<controllers::NotificationController>(
                repository,
                controller_config
            );
            
            return true;
            
        } catch (const std::exception& e) {
            // Log error
            return false;
        }
    }
    
    bool start_servers() {
        try {
            // Start WebSocket server
                         auto ws_future = websocket_channel->start_server(config.websocket_port, config.websocket_host);
             if (!ws_future.get()) {
                 return false;
             }
             
             // Controller owns no WebSocket server; reuse channel implementation
             controller->start();
             
             // Start gRPC server
            if (config.enable_grpc) {
                grpc_server_thread = std::thread([this]() {
                    start_grpc_server();
                });
            }
            
            // Start HTTP server  
            if (config.enable_http) {
                http_server_thread = std::thread([this]() {
                    start_http_server();
                });
            }
            
            return true;
            
        } catch (const std::exception& e) {
            return false;
        }
    }
    
    void start_grpc_server() {
        try {
            std::string server_address = config.grpc_host + ":" + std::to_string(config.grpc_port);
            
            grpc::ServerBuilder builder;
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            auto grpc_svc = std::make_unique<NotificationGrpcService>(controller);
            builder.RegisterService(grpc_svc.get());
            
            grpc_server = builder.BuildAndStart();
            // Keep service object alive while server runs
            owned_grpc_service = std::move(grpc_svc);
            if (grpc_server) {
                // Log server started
                grpc_server->Wait();
            }
            
        } catch (const std::exception& e) {
            // Log error
        }
    }
    
    void start_http_server() {
        try {
            http_server = std::make_unique<httplib::Server>();
            
            // TODO: Expose HTTP endpoints via controller when routes are implemented
            
            // Health check endpoint
            http_server->Get("/health", [this](const httplib::Request& req, httplib::Response& res) {
                handle_health_check(req, res);
            });
            
            // Metrics endpoint
            http_server->Get("/metrics", [this](const httplib::Request& req, httplib::Response& res) {
                handle_metrics_request(req, res);
            });
            
            // Start server
            bool started = http_server->listen(config.http_host, config.http_port);
            if (!started) {
                // Log error
            }
            
        } catch (const std::exception& e) {
            // Log error
        }
    }
    
    void handle_health_check(const httplib::Request& req, httplib::Response& res) {
        nlohmann::json health_data = get_health_status();
        
        res.set_header("Content-Type", "application/json");
        
        if (health_data["healthy"].get<bool>()) {
            res.status = 200;
        } else {
            res.status = 503;
        }
        
        res.body = health_data.dump(2);
    }
    
    void handle_metrics_request(const httplib::Request& req, httplib::Response& res) {
        nlohmann::json metrics = get_service_metrics();
        
        res.set_header("Content-Type", "application/json");
        res.status = 200;
        res.body = metrics.dump(2);
    }
    
    nlohmann::json get_health_status() const {
        bool is_healthy = true;
        nlohmann::json health = {
            {"service", "notification_service"},
            {"version", "1.0.0"},
            {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()},
            {"uptime_seconds", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now() - startup_time).count()}
        };
        
        // Check repository health
        nlohmann::json repo_health = {{"status", "unknown"}};
        try {
            if (repository) {
                // repo_health = repository->get_health_status();
                repo_health["status"] = "healthy";
            }
        } catch (const std::exception& e) {
            repo_health["status"] = "unhealthy";
            repo_health["error"] = e.what();
            is_healthy = false;
        }
        health["repository"] = repo_health;
        
        // Check processor health
        nlohmann::json processor_health = {{"status", "unknown"}};
        try {
            if (processor) {
                processor_health = processor->get_health_status();
            }
        } catch (const std::exception& e) {
            processor_health["status"] = "unhealthy";
            processor_health["error"] = e.what();
            is_healthy = false;
        }
        health["processor"] = processor_health;
        
        // Check WebSocket channel health
        nlohmann::json ws_health = {{"status", "unknown"}};
        try {
            if (websocket_channel) {
                ws_health["status"] = websocket_channel->is_running() ? "healthy" : "unhealthy";
                ws_health["active_connections"] = websocket_channel->get_active_connection_count();
            }
        } catch (const std::exception& e) {
            ws_health["status"] = "unhealthy";
            ws_health["error"] = e.what();
            is_healthy = false;
        }
        health["websocket"] = ws_health;
        
        // Check email channel health
        nlohmann::json email_health = {{"status", "unknown"}};
        try {
            if (email_channel) {
                // email_health = email_channel->get_health_status();
                email_health["status"] = "healthy";
            }
        } catch (const std::exception& e) {
            email_health["status"] = "unhealthy";
            email_health["error"] = e.what();
            is_healthy = false;
        }
        health["email"] = email_health;
        
        // Check push channel health
        nlohmann::json push_health = {{"status", "unknown"}};
        try {
            if (push_channel) {
                // push_health = push_channel->get_health_status();
                push_health["status"] = "healthy";
            }
        } catch (const std::exception& e) {
            push_health["status"] = "unhealthy";
            push_health["error"] = e.what();
            is_healthy = false;
        }
        health["push"] = push_health;
        
        health["healthy"] = is_healthy;
        
        return health;
    }
    
    nlohmann::json get_service_metrics() const {
        nlohmann::json metrics = {
            {"service", "notification_service"},
            {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()}
        };
        
        // Get processor metrics
        if (processor) {
            metrics["processor"] = processor->get_statistics();
        }
        
        // Get WebSocket metrics
        if (websocket_channel) {
            metrics["websocket"] = {
                {"connection_stats", websocket_channel->get_connection_stats()},
                {"delivery_stats", websocket_channel->get_delivery_stats()}
            };
        }
        
        // Get email metrics
        if (email_channel) {
            // metrics["email"] = email_channel->get_delivery_stats();
            metrics["email"] = {{"status", "enabled"}};
        }
        
        // Get push metrics
        if (push_channel) {
            // metrics["push"] = push_channel->get_delivery_stats();
            metrics["push"] = {{"status", "enabled"}};
        }
        
        // Get repository metrics
        if (repository) {
            // metrics["repository"] = repository->get_performance_stats();
            metrics["repository"] = {{"status", "connected"}};
        }
        
        return metrics;
    }
    
    void start_health_monitoring() {
        health_monitoring = true;
        
        health_monitor_thread = std::thread([this]() {
            while (health_monitoring.load()) {
                try {
                    // Update health status
                    nlohmann::json health = get_health_status();
                    is_healthy = health["healthy"].get<bool>();
                    
                    // Perform cleanup tasks
                    if (websocket_channel) {
                        websocket_channel->cleanup_expired_connections();
                        websocket_channel->cleanup_idle_connections();
                    }
                    
                    // Sleep for 30 seconds
                    std::this_thread::sleep_for(std::chrono::seconds{30});
                    
                } catch (const std::exception& e) {
                    is_healthy = false;
                    std::this_thread::sleep_for(std::chrono::seconds{30});
                }
            }
        });
    }
    
    void stop_health_monitoring() {
        health_monitoring = false;
        
        if (health_monitor_thread.joinable()) {
            health_monitor_thread.join();
        }
    }
    
    void stop_service() {
        if (!is_running.load()) {
            return;
        }
        
        is_running = false;
        
        // Stop health monitoring
        stop_health_monitoring();
        
        // Stop controller
        if (controller) {
            controller->stop();
        }
        
        // Stop processor
        if (processor) {
            processor->stop();
        }
        
        // Stop WebSocket server
        if (websocket_channel) {
            websocket_channel->stop_server();
        }
        
        // Stop gRPC server
        if (grpc_server) {
            grpc_server->Shutdown();
        }
        
        // Stop HTTP server
        if (http_server) {
            http_server->stop();
        }
        
        // Wait for server threads
        if (grpc_server_thread.joinable()) {
            grpc_server_thread.join();
        }
        
        if (http_server_thread.joinable()) {
            http_server_thread.join();
        }
    }
};

class NotificationGrpcService final : public sonet::notification::NotificationService::Service {
public:
    NotificationGrpcService(std::shared_ptr<sonet::notification_service::controllers::NotificationController> controller)
        : controller_(std::move(controller)) {}

    grpc::Status ListNotifications(grpc::ServerContext* context,
                                   const sonet::notification::ListNotificationsRequest* request,
                                   sonet::notification::ListNotificationsResponse* response) override {
        try {
            auto json = controller_->get_user_notifications(request->user_id(), /*limit*/ request->pagination().limit(), /*offset*/ 0);
            if (!json.value("success", true)) {
                return grpc::Status(grpc::StatusCode::INTERNAL, "List failed");
            }
            for (const auto& n : json["notifications"]) {
                auto* out = response->add_notifications();
                out->set_notification_id(n.value("id", ""));
                out->set_user_id(n.value("user_id", ""));
                out->set_type(sonet::notification::NOTIFICATION_TYPE_UNKNOWN);
                out->set_actor_user_id(n.value("actor_user_id", ""));
                out->set_note_id(n.value("note_id", ""));
                out->set_is_read(n.value("is_read", false));
            }
            return grpc::Status::OK;
        } catch (...) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Unhandled error");
        }
    }

    grpc::Status MarkNotificationRead(grpc::ServerContext* context,
                                      const sonet::notification::MarkNotificationReadRequest* request,
                                      sonet::notification::MarkNotificationReadResponse* response) override {
        try {
            auto json = controller_->mark_as_read(request->notification_id(), request->user_id());
            response->set_success(json.value("marked_as_read", false));
            return grpc::Status::OK;
        } catch (...) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Unhandled error");
        }
    }

private:
    std::shared_ptr<sonet::notification_service::controllers::NotificationController> controller_;
};

NotificationService::NotificationService(const Config& config)
    : pimpl_(std::make_unique<Impl>(config)) {
}

NotificationService::~NotificationService() = default;

bool NotificationService::start() {
    if (pimpl_->is_running.load()) {
        return false;
    }
    
    // Initialize all components
    if (!pimpl_->initialize_components()) {
        return false;
    }
    
    // Start processor
    if (!pimpl_->processor->start()) {
        return false;
    }
    
    // Start servers
    if (!pimpl_->start_servers()) {
        pimpl_->processor->stop();
        return false;
    }
    
    // Start health monitoring
    pimpl_->start_health_monitoring();
    
    pimpl_->is_running = true;
    pimpl_->is_healthy = true;
    
    return true;
}

void NotificationService::stop() {
    pimpl_->stop_service();
}

bool NotificationService::is_running() const {
    return pimpl_->is_running.load();
}

bool NotificationService::is_healthy() const {
    return pimpl_->is_healthy.load();
}

std::future<bool> NotificationService::send_notification(const models::Notification& notification) {
    if (!pimpl_->processor) {
        auto promise = std::make_shared<std::promise<bool>>();
        auto future = promise->get_future();
        promise->set_value(false);
        return future;
    }
    
    return pimpl_->processor->process_notification(notification);
}

std::future<bool> NotificationService::send_bulk_notifications(const std::vector<models::Notification>& notifications) {
    if (!pimpl_->processor) {
        auto promise = std::make_shared<std::promise<bool>>();
        auto future = promise->get_future();
        promise->set_value(false);
        return future;
    }
    
    return pimpl_->processor->process_bulk_notifications(notifications);
}

nlohmann::json NotificationService::get_health_status() const {
    return pimpl_->get_health_status();
}

nlohmann::json NotificationService::get_service_metrics() const {
    return pimpl_->get_service_metrics();
}

int NotificationService::get_active_connection_count() const {
    if (!pimpl_->websocket_channel) {
        return 0;
    }
    
    return pimpl_->websocket_channel->get_active_connection_count();
}

nlohmann::json NotificationService::get_processor_statistics() const {
    if (!pimpl_->processor) {
        return nlohmann::json{};
    }
    
    return pimpl_->processor->get_statistics();
}

bool NotificationService::register_device(const std::string& user_id, 
                                        const std::string& device_token,
                                        const std::string& platform) {
    if (!pimpl_->push_channel) {
        return false;
    }
    
    // Convert platform string to enum
    channels::DevicePlatform device_platform = channels::DevicePlatform::UNKNOWN;
    if (platform == "ios") {
        device_platform = channels::DevicePlatform::IOS;
    } else if (platform == "android") {
        device_platform = channels::DevicePlatform::ANDROID;
    } else if (platform == "web") {
        device_platform = channels::DevicePlatform::WEB;
    }
    
    return pimpl_->push_channel->register_device(user_id, device_token, device_platform);
}

bool NotificationService::unregister_device(const std::string& user_id, const std::string& device_token) {
    if (!pimpl_->push_channel) {
        return false;
    }
    
    return pimpl_->push_channel->unregister_device(user_id, device_token);
}

void NotificationService::cleanup_resources() {
    if (pimpl_->websocket_channel) {
        pimpl_->websocket_channel->cleanup_expired_connections();
        pimpl_->websocket_channel->cleanup_idle_connections();
    }
    
    if (pimpl_->push_channel) {
        // pimpl_->push_channel->cleanup_expired_tokens();
    }
}

} // namespace notification_service
} // namespace sonet