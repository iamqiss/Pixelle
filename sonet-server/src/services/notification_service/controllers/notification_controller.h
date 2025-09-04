/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This is the HTTP controller for notification management. I built this to handle
 * both REST API endpoints and WebSocket connections for real-time notifications.
 * It's designed to scale with Sonet's growth while keeping the API simple.
 */

#pragma once

#include "../models/notification.h"
#include "../repositories/notification_repository.h"
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <chrono>
#include <functional>
#include <nlohmann/json.hpp>
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

namespace sonet {
namespace notification_service {
namespace controllers {

/**
 * WebSocket connection manager for real-time notifications
 */
class WebSocketConnectionManager {
public:
    using connection_hdl = websocketpp::connection_hdl;
    using server = websocketpp::server<websocketpp::config::asio>;
    using message_ptr = server::message_ptr;
    
    struct UserConnection {
        connection_hdl hdl;
        std::string user_id;
        std::string session_id;
        std::chrono::system_clock::time_point connected_at;
        std::chrono::system_clock::time_point last_ping;
        bool is_active;
        nlohmann::json client_info; // Device info, app version, etc.
    };
    
    WebSocketConnectionManager();
    ~WebSocketConnectionManager();
    
    // Connection management
    void add_connection(connection_hdl hdl, const std::string& user_id, 
                       const std::string& session_id, const nlohmann::json& client_info = {});
    void remove_connection(connection_hdl hdl);
    void remove_user_connections(const std::string& user_id);
    
    // Message sending
    bool send_to_user(const std::string& user_id, const nlohmann::json& message);
    bool send_to_connection(connection_hdl hdl, const nlohmann::json& message);
    void broadcast_to_users(const std::vector<std::string>& user_ids, 
                           const nlohmann::json& message);
    
    // Connection info
    std::vector<UserConnection> get_user_connections(const std::string& user_id) const;
    int get_active_connection_count() const;
    int get_user_connection_count(const std::string& user_id) const;
    bool is_user_online(const std::string& user_id) const;
    
    // Health management
    void ping_connections();
    void cleanup_stale_connections();
    nlohmann::json get_connection_stats() const;
    
    // Event handlers
    void set_server(std::shared_ptr<server> srv) { server_ = srv; }
    void on_open(connection_hdl hdl);
    void on_close(connection_hdl hdl);
    void on_message(connection_hdl hdl, message_ptr msg);
    void on_pong(connection_hdl hdl, std::string payload);
    
private:
    std::shared_ptr<server> server_;
    mutable std::mutex connections_mutex_;
    std::unordered_map<connection_hdl, std::unique_ptr<UserConnection>, 
                      std::owner_less<connection_hdl>> connections_;
    std::unordered_map<std::string, std::vector<connection_hdl>> user_connections_;
    
    std::atomic<int> total_connections_{0};
    std::atomic<int> active_connections_{0};
    
    void cleanup_user_connections(const std::string& user_id);
    std::string connection_to_string(connection_hdl hdl) const;
};

/**
 * Rate limiter for notification endpoints
 */
class NotificationRateLimiter {
public:
    struct RateLimit {
        int requests_per_minute = 100;
        int requests_per_hour = 1000;
        int burst_limit = 10;
    };
    
    explicit NotificationRateLimiter(const RateLimit& limits = {});
    
    bool check_rate_limit(const std::string& user_id, const std::string& endpoint);
    void reset_rate_limit(const std::string& user_id, const std::string& endpoint = "");
    nlohmann::json get_rate_limit_status(const std::string& user_id, 
                                        const std::string& endpoint) const;
    
private:
    RateLimit limits_;
    mutable std::mutex rate_data_mutex_;
    
    struct RateData {
        std::vector<std::chrono::system_clock::time_point> minute_requests;
        std::vector<std::chrono::system_clock::time_point> hour_requests;
        int burst_count = 0;
        std::chrono::system_clock::time_point last_burst_reset;
    };
    
    std::unordered_map<std::string, RateData> rate_data_;
    
    void cleanup_old_requests(RateData& data) const;
    std::string get_rate_key(const std::string& user_id, const std::string& endpoint) const;
};

/**
 * Main notification controller handling HTTP and WebSocket requests
 * Provides Twitter-scale notification management with real-time delivery
 */
class NotificationController {
public:
    struct Config {
        // HTTP server configuration
        std::string http_host = "0.0.0.0";
        int http_port = 8080;
        int http_thread_pool_size = 10;
        std::chrono::seconds http_timeout{30};
        
        // WebSocket server configuration
        std::string websocket_host = "0.0.0.0";
        int websocket_port = 8081;
        int websocket_thread_pool_size = 5;
        std::chrono::seconds websocket_ping_interval{30};
        std::chrono::seconds websocket_pong_timeout{10};
        
        // Rate limiting
        NotificationRateLimiter::RateLimit rate_limits;
        
        // Caching
        bool enable_response_caching = true;
        std::chrono::seconds cache_ttl{300};
        
        // Security
        std::string jwt_secret;
        bool require_authentication = true;
        bool enable_cors = true;
        std::vector<std::string> allowed_origins = {"*"};
        
        // Performance
        bool enable_compression = true;
        bool enable_request_logging = true;
        bool enable_metrics_collection = true;
        int max_request_size_mb = 10;
        
        // Real-time features
        bool enable_websocket = true;
        bool enable_push_notifications = true;
        bool enable_email_notifications = true;
        
        // Batch processing
        int max_batch_size = 1000;
        std::chrono::seconds batch_processing_interval{5};
    };
    
    explicit NotificationController(
        std::shared_ptr<repositories::NotificationRepository> repository,
        const Config& config = {});
    
    virtual ~NotificationController();
    
    // Lifecycle management
    void start();
    void stop();
    bool is_running() const;
    
    // HTTP API Endpoints
    
    // Notification CRUD operations
    nlohmann::json create_notification(const nlohmann::json& request, 
                                      const std::string& user_id);
    nlohmann::json get_notification(const std::string& notification_id, 
                                   const std::string& user_id);
    nlohmann::json update_notification(const std::string& notification_id, 
                                      const nlohmann::json& request, 
                                      const std::string& user_id);
    nlohmann::json delete_notification(const std::string& notification_id, 
                                      const std::string& user_id);
    
    // User notification operations
    nlohmann::json get_user_notifications(const std::string& user_id, 
                                         const nlohmann::json& query_params);
    nlohmann::json get_unread_notifications(const std::string& user_id, 
                                           const nlohmann::json& query_params);
    nlohmann::json get_unread_count(const std::string& user_id);
    nlohmann::json mark_as_read(const std::string& notification_id, 
                               const std::string& user_id);
    nlohmann::json mark_all_as_read(const std::string& user_id);
    
    // Bulk operations
    nlohmann::json create_notifications_bulk(const nlohmann::json& request, 
                                            const std::string& user_id);
    nlohmann::json update_notifications_bulk(const nlohmann::json& request, 
                                            const std::string& user_id);
    nlohmann::json mark_as_read_bulk(const nlohmann::json& request, 
                                    const std::string& user_id);
    
    // Notification preferences
    nlohmann::json get_user_preferences(const std::string& user_id);
    nlohmann::json update_user_preferences(const nlohmann::json& request, 
                                          const std::string& user_id);
    nlohmann::json delete_user_preferences(const std::string& user_id);
    
    // Analytics and stats
    nlohmann::json get_user_stats(const std::string& user_id);
    nlohmann::json get_notification_analytics(const std::string& notification_id, 
                                             const std::string& user_id);
    nlohmann::json get_delivery_stats(const std::string& user_id, 
                                     const nlohmann::json& query_params);
    
    // Search and filtering
    nlohmann::json search_notifications(const nlohmann::json& query, 
                                       const std::string& user_id);
    nlohmann::json get_notifications_by_type(const std::string& user_id, 
                                            const std::string& type, 
                                            const nlohmann::json& query_params);
    nlohmann::json get_grouped_notifications(const std::string& user_id, 
                                            const std::string& group_key, 
                                            const nlohmann::json& query_params);
    
    // Real-time WebSocket API
    void handle_websocket_connect(const std::string& user_id, 
                                 const std::string& session_id,
                                 WebSocketConnectionManager::connection_hdl hdl,
                                 const nlohmann::json& client_info = {});
    void handle_websocket_disconnect(WebSocketConnectionManager::connection_hdl hdl);
    void handle_websocket_message(WebSocketConnectionManager::connection_hdl hdl, 
                                 const nlohmann::json& message);
    
    // Real-time notification delivery
    bool send_real_time_notification(const models::Notification& notification);
    void broadcast_notification_to_followers(const models::Notification& notification, 
                                            const std::vector<std::string>& follower_ids);
    void send_typing_indicator(const std::string& conversation_id, 
                              const std::string& user_id, bool is_typing);
    void send_presence_update(const std::string& user_id, const std::string& status);
    
    // Push notification integration
    nlohmann::json register_push_token(const std::string& user_id, 
                                      const nlohmann::json& token_info);
    nlohmann::json unregister_push_token(const std::string& user_id, 
                                        const std::string& token);
    nlohmann::json send_push_notification(const models::Notification& notification);
    
    // Email notification integration
    nlohmann::json send_email_notification(const models::Notification& notification);
    nlohmann::json get_email_preferences(const std::string& user_id);
    nlohmann::json update_email_preferences(const std::string& user_id, 
                                           const nlohmann::json& preferences);
    
    // Administrative operations
    nlohmann::json get_system_stats();
    nlohmann::json get_health_status();
    nlohmann::json cleanup_old_notifications(const nlohmann::json& request);
    nlohmann::json force_notification_delivery(const std::string& notification_id);
    nlohmann::json get_delivery_failures(const nlohmann::json& query_params);
    
    // Performance and monitoring
    nlohmann::json get_performance_metrics();
    nlohmann::json get_rate_limit_status(const std::string& user_id);
    nlohmann::json get_connection_stats();
    void reset_performance_metrics();
    
    // Authentication and authorization
    bool authenticate_request(const std::string& token, std::string& user_id);
    bool authorize_notification_access(const std::string& user_id, 
                                     const std::string& notification_id);
    bool authorize_admin_access(const std::string& user_id);
    
    // Request validation
    bool validate_notification_request(const nlohmann::json& request, 
                                     std::vector<std::string>& errors);
    bool validate_bulk_request(const nlohmann::json& request, 
                              std::vector<std::string>& errors);
    bool validate_preferences_request(const nlohmann::json& request, 
                                    std::vector<std::string>& errors);
    
    // Helper methods for notifications
    nlohmann::json notification_to_json(const models::Notification& notification, 
                                       bool include_sensitive = false);
    models::Notification json_to_notification(const nlohmann::json& json);
    nlohmann::json create_response(const std::string& status, 
                                  const nlohmann::json& data = {}, 
                                  const std::string& message = "");
    nlohmann::json create_error_response(const std::string& error, 
                                        int status_code = 400, 
                                        const std::vector<std::string>& details = {});
    
    // Event hooks for extensibility
    using NotificationCreatedCallback = std::function<void(const models::Notification&)>;
    using NotificationDeliveredCallback = std::function<void(const models::Notification&)>;
    using NotificationReadCallback = std::function<void(const models::Notification&)>;
    using UserConnectedCallback = std::function<void(const std::string&)>;
    using UserDisconnectedCallback = std::function<void(const std::string&)>;
    
    void set_notification_created_callback(NotificationCreatedCallback callback);
    void set_notification_delivered_callback(NotificationDeliveredCallback callback);
    void set_notification_read_callback(NotificationReadCallback callback);
    void set_user_connected_callback(UserConnectedCallback callback);
    void set_user_disconnected_callback(UserDisconnectedCallback callback);
    
private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    // Internal helper methods
    void initialize_http_server();
    void initialize_websocket_server();
    void start_background_processors();
    void stop_background_processors();
    
    void process_pending_notifications();
    void process_scheduled_notifications();
    void process_batch_notifications();
    void cleanup_expired_notifications();
    
    std::string extract_user_id_from_token(const std::string& token);
    std::string generate_session_id();
    
    // Performance tracking
    void track_request_start(const std::string& endpoint, const std::string& user_id);
    void track_request_end(const std::string& endpoint, const std::string& user_id, 
                          std::chrono::microseconds duration, bool success);
    void track_websocket_message(const std::string& user_id, const std::string& type);
    void track_notification_delivery(const models::Notification& notification, 
                                   const std::string& channel, bool success);
    
    // Caching helpers
    std::string get_cache_key(const std::string& prefix, const std::string& id);
    std::optional<nlohmann::json> get_cached_response(const std::string& key);
    void cache_response(const std::string& key, const nlohmann::json& response, 
                       std::chrono::seconds ttl = std::chrono::seconds{300});
    void invalidate_user_cache(const std::string& user_id);
    
    // Security helpers
    std::string hash_token(const std::string& token);
    bool validate_cors_origin(const std::string& origin);
    std::string sanitize_input(const std::string& input);
    
    // Notification processing helpers
    void enrich_notification(models::Notification& notification);
    bool should_send_real_time(const models::Notification& notification);
    bool should_send_push(const models::Notification& notification);
    bool should_send_email(const models::Notification& notification);
    
    // Template rendering
    std::string render_notification_template(const models::Notification& notification, 
                                            const std::string& template_type);
    nlohmann::json get_template_context(const models::Notification& notification);
};

/**
 * Factory for creating notification controller instances
 */
class NotificationControllerFactory {
public:
    static std::unique_ptr<NotificationController> create(
        std::shared_ptr<repositories::NotificationRepository> repository,
        const NotificationController::Config& config = {});
    
    static std::unique_ptr<NotificationController> create_from_config(
        std::shared_ptr<repositories::NotificationRepository> repository,
        const nlohmann::json& config_json);
};

} // namespace controllers
} // namespace notification_service
} // namespace sonet
