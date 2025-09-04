/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This is the WebSocket channel for real-time notifications. I built this to
 * deliver instant notifications when users are actively browsing Sonet,
 * making the experience feel live and engaging.
 */

#pragma once

#include "../models/notification.h"
#include <string>
#include <vector>
#include <memory>
#include <future>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <mutex>
#include <atomic>
#include <nlohmann/json.hpp>
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

namespace sonet {
namespace notification_service {
namespace channels {

/**
 * WebSocket connection information
 * I track this to manage user sessions and deliver targeted notifications
 */
struct WebSocketConnection {
    using connection_hdl = websocketpp::connection_hdl;
    
    std::string connection_id;
    std::string user_id;
    std::string session_id;
    connection_hdl handle;
    std::chrono::system_clock::time_point connected_at;
    std::chrono::system_clock::time_point last_ping;
    std::chrono::system_clock::time_point last_activity;
    bool is_authenticated = false;
    bool is_active = true;
    
    // Client information
    std::string user_agent;
    std::string ip_address;
    std::string device_type; // "desktop", "mobile", "tablet"
    std::string browser;
    std::string os;
    nlohmann::json client_capabilities;
    
    // Subscription preferences
    std::unordered_set<models::NotificationType> subscribed_types;
    bool real_time_enabled = true;
    int max_notifications_per_minute = 10;
    
    bool is_expired() const {
        auto now = std::chrono::system_clock::now();
        auto minutes_since_ping = std::chrono::duration_cast<std::chrono::minutes>(
            now - last_ping).count();
        return minutes_since_ping > 5; // 5 minutes timeout
    }
    
    bool is_idle() const {
        auto now = std::chrono::system_clock::now();
        auto minutes_since_activity = std::chrono::duration_cast<std::chrono::minutes>(
            now - last_activity).count();
        return minutes_since_activity > 30; // 30 minutes idle
    }
    
    nlohmann::json to_json() const {
        return nlohmann::json{
            {"connection_id", connection_id},
            {"user_id", user_id},
            {"session_id", session_id},
            {"connected_at", std::chrono::duration_cast<std::chrono::seconds>(
                connected_at.time_since_epoch()).count()},
            {"last_ping", std::chrono::duration_cast<std::chrono::seconds>(
                last_ping.time_since_epoch()).count()},
            {"last_activity", std::chrono::duration_cast<std::chrono::seconds>(
                last_activity.time_since_epoch()).count()},
            {"is_authenticated", is_authenticated},
            {"is_active", is_active},
            {"user_agent", user_agent},
            {"ip_address", ip_address},
            {"device_type", device_type},
            {"browser", browser},
            {"os", os},
            {"client_capabilities", client_capabilities},
            {"real_time_enabled", real_time_enabled},
            {"max_notifications_per_minute", max_notifications_per_minute}
        };
    }
};

/**
 * WebSocket message structure for notifications
 * I designed this to be lightweight but informative
 */
struct WebSocketMessage {
    enum class Type {
        NOTIFICATION,
        BATCH_UPDATE,
        STATUS_UPDATE,
        PING,
        PONG,
        AUTH_REQUEST,
        AUTH_RESPONSE,
        SUBSCRIBE,
        UNSUBSCRIBE,
        ERROR
    };
    
    Type type;
    std::string message_id;
    std::chrono::system_clock::time_point timestamp;
    nlohmann::json payload;
    std::string user_id;
    bool requires_ack = false;
    int retry_count = 0;
    
    std::string to_string() const {
        nlohmann::json msg = {
            {"type", static_cast<int>(type)},
            {"message_id", message_id},
            {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
                timestamp.time_since_epoch()).count()},
            {"payload", payload},
            {"requires_ack", requires_ack},
            {"retry_count", retry_count}
        };
        return msg.dump();
    }
    
    static std::optional<WebSocketMessage> from_string(const std::string& str) {
        try {
            auto json = nlohmann::json::parse(str);
            WebSocketMessage msg;
            msg.type = static_cast<Type>(json["type"].get<int>());
            msg.message_id = json["message_id"].get<std::string>();
            msg.timestamp = std::chrono::system_clock::from_time_t(json["timestamp"].get<time_t>());
            msg.payload = json["payload"];
            msg.requires_ack = json.value("requires_ack", false);
            msg.retry_count = json.value("retry_count", 0);
            return msg;
        } catch (...) {
            return std::nullopt;
        }
    }
};

/**
 * WebSocket delivery result
 * I track this to monitor real-time delivery performance
 */
struct WebSocketDeliveryResult {
    bool success = false;
    std::string connection_id;
    std::string message_id;
    std::string error_message;
    std::chrono::system_clock::time_point sent_at;
    std::chrono::milliseconds delivery_time{0};
    bool connection_dropped = false;
    bool requires_retry = false;
    int retry_count = 0;
    
    nlohmann::json to_json() const {
        return nlohmann::json{
            {"success", success},
            {"connection_id", connection_id},
            {"message_id", message_id},
            {"error_message", error_message},
            {"sent_at", std::chrono::duration_cast<std::chrono::seconds>(
                sent_at.time_since_epoch()).count()},
            {"delivery_time_ms", delivery_time.count()},
            {"connection_dropped", connection_dropped},
            {"requires_retry", requires_retry},
            {"retry_count", retry_count}
        };
    }
};

/**
 * WebSocket notification template
 * I use these to format notifications for real-time delivery
 */
struct WebSocketTemplate {
    models::NotificationType type;
    std::string title_template;
    std::string message_template;
    std::string icon_template;
    std::string action_template;
    bool show_avatar = true;
    bool show_timestamp = true;
    bool auto_dismiss = false;
    std::chrono::seconds dismiss_after{10};
    nlohmann::json custom_data;
    
    bool is_valid() const {
        return !title_template.empty() && !message_template.empty();
    }
};

/**
 * WebSocket channel interface for real-time notifications
 * I built this to handle thousands of concurrent connections efficiently
 */
class WebSocketChannel {
public:
    virtual ~WebSocketChannel() = default;
    
    // Connection management
    virtual std::future<bool> start_server(int port, const std::string& host = "0.0.0.0") = 0;
    virtual void stop_server() = 0;
    virtual bool is_running() const = 0;
    
    // Client connection handling
    virtual std::string add_connection(websocketpp::connection_hdl hdl,
                                     const std::string& user_id,
                                     const std::string& session_id,
                                     const nlohmann::json& client_info = {}) = 0;
    virtual bool remove_connection(const std::string& connection_id) = 0;
    virtual bool authenticate_connection(const std::string& connection_id, const std::string& auth_token) = 0;
    virtual std::vector<WebSocketConnection> get_user_connections(const std::string& user_id) = 0;
    virtual std::optional<WebSocketConnection> get_connection(const std::string& connection_id) = 0;
    virtual int get_active_connection_count() const = 0;
    
    // Notification delivery
    virtual std::future<WebSocketDeliveryResult> send_notification(
        const models::Notification& notification,
        const std::string& connection_id) = 0;
    
    virtual std::future<std::vector<WebSocketDeliveryResult>> send_to_user(
        const models::Notification& notification,
        const std::string& user_id) = 0;
    
    virtual std::future<std::vector<WebSocketDeliveryResult>> send_to_users(
        const models::Notification& notification,
        const std::vector<std::string>& user_ids) = 0;
    
    virtual std::future<std::vector<WebSocketDeliveryResult>> send_batch_notifications(
        const std::vector<models::Notification>& notifications,
        const std::string& user_id) = 0;
    
    virtual std::future<WebSocketDeliveryResult> send_message(
        const WebSocketMessage& message,
        const std::string& connection_id) = 0;
    
    virtual std::future<std::vector<WebSocketDeliveryResult>> broadcast_message(
        const WebSocketMessage& message,
        const std::vector<std::string>& connection_ids) = 0;
    
    // Subscription management
    virtual bool subscribe_to_notification_type(const std::string& connection_id,
                                               models::NotificationType type) = 0;
    virtual bool unsubscribe_from_notification_type(const std::string& connection_id,
                                                   models::NotificationType type) = 0;
    virtual bool set_real_time_enabled(const std::string& connection_id, bool enabled) = 0;
    virtual bool set_rate_limit(const std::string& connection_id, int max_per_minute) = 0;
    
    // Template management
    virtual bool register_template(models::NotificationType type, const WebSocketTemplate& template) = 0;
    virtual bool update_template(models::NotificationType type, const WebSocketTemplate& template) = 0;
    virtual bool remove_template(models::NotificationType type) = 0;
    virtual std::optional<WebSocketTemplate> get_template(models::NotificationType type) const = 0;
    
    // Message rendering
    virtual WebSocketMessage render_notification_message(
        const models::Notification& notification,
        const WebSocketTemplate& template) const = 0;
    
    // Health and monitoring
    virtual nlohmann::json get_connection_stats() const = 0;
    virtual nlohmann::json get_delivery_stats() const = 0;
    virtual nlohmann::json get_health_status() const = 0;
    virtual void reset_stats() = 0;
    
    // Maintenance
    virtual int cleanup_expired_connections() = 0;
    virtual int cleanup_idle_connections() = 0;
    virtual void ping_all_connections() = 0;
    
    // Configuration
    virtual bool configure(const nlohmann::json& config) = 0;
    virtual nlohmann::json get_config() const = 0;
};

/**
 * WebSocketPP-based implementation
 * I use WebSocketPP because it's reliable and handles concurrency well
 */
class WebSocketPPChannel : public WebSocketChannel {
public:
    struct Config {
        int port = 8080;
        std::string host = "0.0.0.0";
        int max_connections = 10000;
        std::chrono::seconds connection_timeout{300}; // 5 minutes
        std::chrono::seconds ping_interval{30};
        std::chrono::seconds idle_timeout{1800}; // 30 minutes
        
        // Message settings
        size_t max_message_size = 1024 * 1024; // 1MB
        int max_messages_per_minute = 60;
        bool enable_message_compression = true;
        
        // Security
        bool require_authentication = true;
        std::string jwt_secret;
        std::chrono::hours token_expiry{24};
        bool enable_rate_limiting = true;
        
        // Performance
        int worker_thread_count = 4;
        int io_thread_count = 2;
        bool enable_keepalive = true;
        size_t receive_buffer_size = 4096;
        size_t send_buffer_size = 4096;
        
        // Monitoring
        bool enable_access_log = false;
        bool enable_error_log = true;
        std::string log_level = "info";
        
        // Features
        bool enable_subprotocols = false;
        std::vector<std::string> allowed_origins;
        std::unordered_map<std::string, std::string> custom_headers;
    };
    
    using server = websocketpp::server<websocketpp::config::asio>;
    using connection_hdl = websocketpp::connection_hdl;
    using message_ptr = server::message_ptr;
    
    explicit WebSocketPPChannel(const Config& config);
    virtual ~WebSocketPPChannel();
    
    // Implement WebSocketChannel interface
    std::future<bool> start_server(int port, const std::string& host = "0.0.0.0") override;
    void stop_server() override;
    bool is_running() const override;
    
    std::string add_connection(websocketpp::connection_hdl hdl,
                             const std::string& user_id,
                             const std::string& session_id,
                             const nlohmann::json& client_info = {}) override;
    bool remove_connection(const std::string& connection_id) override;
    bool authenticate_connection(const std::string& connection_id, const std::string& auth_token) override;
    std::vector<WebSocketConnection> get_user_connections(const std::string& user_id) override;
    std::optional<WebSocketConnection> get_connection(const std::string& connection_id) override;
    int get_active_connection_count() const override;
    
    std::future<WebSocketDeliveryResult> send_notification(
        const models::Notification& notification,
        const std::string& connection_id) override;
    
    std::future<std::vector<WebSocketDeliveryResult>> send_to_user(
        const models::Notification& notification,
        const std::string& user_id) override;
    
    std::future<std::vector<WebSocketDeliveryResult>> send_to_users(
        const models::Notification& notification,
        const std::vector<std::string>& user_ids) override;
    
    std::future<std::vector<WebSocketDeliveryResult>> send_batch_notifications(
        const std::vector<models::Notification>& notifications,
        const std::string& user_id) override;
    
    std::future<WebSocketDeliveryResult> send_message(
        const WebSocketMessage& message,
        const std::string& connection_id) override;
    
    std::future<std::vector<WebSocketDeliveryResult>> broadcast_message(
        const WebSocketMessage& message,
        const std::vector<std::string>& connection_ids) override;
    
    bool subscribe_to_notification_type(const std::string& connection_id,
                                       models::NotificationType type) override;
    bool unsubscribe_from_notification_type(const std::string& connection_id,
                                           models::NotificationType type) override;
    bool set_real_time_enabled(const std::string& connection_id, bool enabled) override;
    bool set_rate_limit(const std::string& connection_id, int max_per_minute) override;
    
    bool register_template(models::NotificationType type, const WebSocketTemplate& template) override;
    bool update_template(models::NotificationType type, const WebSocketTemplate& template) override;
    bool remove_template(models::NotificationType type) override;
    std::optional<WebSocketTemplate> get_template(models::NotificationType type) const override;
    
    WebSocketMessage render_notification_message(
        const models::Notification& notification,
        const WebSocketTemplate& template) const override;
    
    nlohmann::json get_connection_stats() const override;
    nlohmann::json get_delivery_stats() const override;
    nlohmann::json get_health_status() const override;
    void reset_stats() override;
    
    int cleanup_expired_connections() override;
    int cleanup_idle_connections() override;
    void ping_all_connections() override;
    
    bool configure(const nlohmann::json& config) override;
    nlohmann::json get_config() const override;
    
    // WebSocketPP-specific methods
    void set_message_handler(std::function<void(connection_hdl, message_ptr)> handler);
    void set_open_handler(std::function<void(connection_hdl)> handler);
    void set_close_handler(std::function<void(connection_hdl)> handler);
    bool validate_jwt_token(const std::string& token, std::string& user_id);
    
private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    // Event handlers
    void on_open(connection_hdl hdl);
    void on_close(connection_hdl hdl);
    void on_message(connection_hdl hdl, message_ptr msg);
    void on_ping(connection_hdl hdl, std::string payload);
    void on_pong(connection_hdl hdl, std::string payload);
    
    // Internal helper methods
    std::string generate_connection_id() const;
    void update_connection_activity(const std::string& connection_id);
    bool send_raw_message(connection_hdl hdl, const std::string& message);
    std::string replace_template_variables(const std::string& template,
                                         const std::unordered_map<std::string, std::string>& variables) const;
    std::unordered_map<std::string, std::string> extract_template_variables(
        const models::Notification& notification) const;
    void track_message_sent();
    void track_message_failed();
    void track_connection_added();
    void track_connection_removed();
    void cleanup_old_stats();
    
    // Connection management helpers
    void schedule_ping_timer();
    void schedule_cleanup_timer();
    void handle_ping_timer();
    void handle_cleanup_timer();
};

/**
 * Factory for creating WebSocket channels
 * I use this to standardize WebSocket channel creation
 */
class WebSocketChannelFactory {
public:
    enum class ChannelType {
        WEBSOCKETPP,
        UWEBSOCKETS,
        MOCK // For testing
    };
    
    static std::unique_ptr<WebSocketChannel> create(ChannelType type, const nlohmann::json& config);
    static std::unique_ptr<WebSocketChannel> create_websocketpp(const WebSocketPPChannel::Config& config);
    static std::unique_ptr<WebSocketChannel> create_mock(); // For testing
    
    // Template helpers
    static WebSocketTemplate create_like_template();
    static WebSocketTemplate create_comment_template();
    static WebSocketTemplate create_follow_template();
    static WebSocketTemplate create_mention_template();
    static WebSocketTemplate create_renote_template();
    static WebSocketTemplate create_dm_template();
    static std::unordered_map<models::NotificationType, WebSocketTemplate> create_default_templates();
};

} // namespace channels
} // namespace notification_service
} // namespace sonet