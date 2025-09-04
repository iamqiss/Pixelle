/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This implements the WebSocket channel for real-time notifications.
 * I built this to deliver instant notifications when users are actively
 * browsing Sonet, making the experience feel live and engaging like magic.
 */

#include "websocket_channel.h"
#include <jwt-cpp/jwt.h>
#include <thread>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <uuid/uuid.h>

namespace sonet {
namespace notification_service {
namespace channels {

// Internal implementation for WebSocketPP channel
struct WebSocketPPChannel::Impl {
    Config config;
    server websocket_server;
    std::thread server_thread;
    std::atomic<bool> is_running{false};
    
    // Connection management
    std::unordered_map<std::string, std::unique_ptr<WebSocketConnection>> connections;
    std::unordered_map<std::string, std::vector<std::string>> user_connections; // user_id -> connection_ids
    std::unordered_map<connection_hdl, std::string, std::owner_less<connection_hdl>> handle_to_connection_id;
    mutable std::mutex connections_mutex;
    
    // Templates
    std::unordered_map<models::NotificationType, WebSocketTemplate> templates;
    mutable std::mutex templates_mutex;
    
    // Statistics
    std::atomic<int> messages_sent{0};
    std::atomic<int> messages_failed{0};
    std::atomic<int> connections_added{0};
    std::atomic<int> connections_removed{0};
    std::atomic<int> active_connections{0};
    std::chrono::system_clock::time_point stats_start;
    mutable std::mutex stats_mutex;
    
    // Rate limiting per connection
    std::unordered_map<std::string, int> connection_message_counts;
    std::unordered_map<std::string, std::chrono::system_clock::time_point> connection_rate_reset;
    mutable std::mutex rate_limit_mutex;
    
    // Timers
    std::thread ping_timer_thread;
    std::thread cleanup_timer_thread;
    std::atomic<bool> timers_running{false};
    
    Impl(const Config& cfg) : config(cfg) {
        stats_start = std::chrono::system_clock::now();
        initialize_default_templates();
        setup_websocket_server();
    }
    
    ~Impl() {
        stop_server();
    }
    
    void initialize_default_templates() {
        std::lock_guard<std::mutex> lock(templates_mutex);
        
        // Like notification template
        WebSocketTemplate like_template;
        like_template.type = models::NotificationType::LIKE;
        like_template.title_template = "{{sender_name}} liked your note";
        like_template.message_template = "\"{{note_excerpt}}\"";
        like_template.icon_template = "https://sonet.app/icons/like.svg";
        like_template.action_template = "/note/{{note_id}}";
        like_template.show_avatar = true;
        like_template.show_timestamp = true;
        like_template.auto_dismiss = true;
        like_template.dismiss_after = std::chrono::seconds{8};
        templates[models::NotificationType::LIKE] = like_template;
        
        // Comment notification template
        WebSocketTemplate comment_template;
        comment_template.type = models::NotificationType::COMMENT;
        comment_template.title_template = "{{sender_name}} commented";
        comment_template.message_template = "\"{{comment_text}}\"";
        comment_template.icon_template = "https://sonet.app/icons/comment.svg";
        comment_template.action_template = "/note/{{note_id}}";
        comment_template.show_avatar = true;
        comment_template.show_timestamp = true;
        comment_template.auto_dismiss = false; // Keep comments visible
        templates[models::NotificationType::COMMENT] = comment_template;
        
        // Follow notification template
        WebSocketTemplate follow_template;
        follow_template.type = models::NotificationType::FOLLOW;
        follow_template.title_template = "New follower";
        follow_template.message_template = "{{sender_name}} started following you";
        follow_template.icon_template = "https://sonet.app/icons/follow.svg";
        follow_template.action_template = "/profile/{{sender_id}}";
        follow_template.show_avatar = true;
        follow_template.show_timestamp = true;
        follow_template.auto_dismiss = false;
        templates[models::NotificationType::FOLLOW] = follow_template;
        
        // Mention notification template
        WebSocketTemplate mention_template;
        mention_template.type = models::NotificationType::MENTION;
        mention_template.title_template = "{{sender_name}} mentioned you";
        mention_template.message_template = "\"{{note_text}}\"";
        mention_template.icon_template = "https://sonet.app/icons/mention.svg";
        mention_template.action_template = "/note/{{note_id}}";
        mention_template.show_avatar = true;
        mention_template.show_timestamp = true;
        mention_template.auto_dismiss = false; // Mentions are important
        templates[models::NotificationType::MENTION] = mention_template;
        
        // Renote notification template
        WebSocketTemplate renote_template;
        renote_template.type = models::NotificationType::RENOTE;
        renote_template.title_template = "{{sender_name}} renoted your note";
        renote_template.message_template = "\"{{note_excerpt}}\"";
        renote_template.icon_template = "https://sonet.app/icons/renote.svg";
        renote_template.action_template = "/note/{{note_id}}";
        renote_template.show_avatar = true;
        renote_template.show_timestamp = true;
        renote_template.auto_dismiss = true;
        renote_template.dismiss_after = std::chrono::seconds{10};
        templates[models::NotificationType::RENOTE] = renote_template;
        
        // Direct message template
        WebSocketTemplate dm_template;
        dm_template.type = models::NotificationType::DIRECT_MESSAGE;
        dm_template.title_template = "{{sender_name}}";
        dm_template.message_template = "New message";
        dm_template.icon_template = "https://sonet.app/icons/message.svg";
        dm_template.action_template = "/messages/{{conversation_id}}";
        dm_template.show_avatar = true;
        dm_template.show_timestamp = true;
        dm_template.auto_dismiss = false; // Keep DMs visible
        templates[models::NotificationType::DIRECT_MESSAGE] = dm_template;
    }
    
    void setup_websocket_server() {
        try {
            // Set logging levels
            websocket_server.set_access_channels(websocketpp::log::alevel::all);
            websocket_server.clear_access_channels(websocketpp::log::alevel::frame_payload);
            websocket_server.set_error_channels(websocketpp::log::elevel::all);
            
            // Initialize ASIO
            websocket_server.init_asio();
            
            // Set reuse address
            websocket_server.set_reuse_addr(true);
            
            // Configure settings
            websocket_server.set_message_max_size(config.max_message_size);
            
            // Set handlers - using lambdas to capture this
            websocket_server.set_open_handler([this](connection_hdl hdl) {
                this->on_open(hdl);
            });
            
            websocket_server.set_close_handler([this](connection_hdl hdl) {
                this->on_close(hdl);
            });
            
            websocket_server.set_message_handler([this](connection_hdl hdl, server::message_ptr msg) {
                this->on_message(hdl, msg);
            });
            
            websocket_server.set_ping_handler([this](connection_hdl hdl, std::string payload) -> bool {
                this->on_ping(hdl, payload);
                return true;
            });
            
            websocket_server.set_pong_handler([this](connection_hdl hdl, std::string payload) {
                this->on_pong(hdl, payload);
            });
            
        } catch (const std::exception& e) {
            // Log error
        }
    }
    
    void stop_server() {
        if (!is_running.load()) {
            return;
        }
        
        is_running = false;
        timers_running = false;
        
        // Stop the server
        websocket_server.stop();
        
        // Wait for server thread
        if (server_thread.joinable()) {
            server_thread.join();
        }
        
        // Wait for timer threads
        if (ping_timer_thread.joinable()) {
            ping_timer_thread.join();
        }
        
        if (cleanup_timer_thread.joinable()) {
            cleanup_timer_thread.join();
        }
        
        // Clear all connections
        std::lock_guard<std::mutex> lock(connections_mutex);
        connections.clear();
        user_connections.clear();
        handle_to_connection_id.clear();
        active_connections = 0;
    }
    
    void on_open(connection_hdl hdl) {
        // Connection will be properly registered when authentication message is received
        track_connection_added();
    }
    
    void on_close(connection_hdl hdl) {
        std::lock_guard<std::mutex> lock(connections_mutex);
        
        auto hdl_it = handle_to_connection_id.find(hdl);
        if (hdl_it != handle_to_connection_id.end()) {
            const std::string& connection_id = hdl_it->second;
            
            auto conn_it = connections.find(connection_id);
            if (conn_it != connections.end()) {
                const std::string& user_id = conn_it->second->user_id;
                
                // Remove from user connections
                auto user_it = user_connections.find(user_id);
                if (user_it != user_connections.end()) {
                    auto& user_conn_list = user_it->second;
                    user_conn_list.erase(
                        std::remove(user_conn_list.begin(), user_conn_list.end(), connection_id),
                        user_conn_list.end()
                    );
                    
                    if (user_conn_list.empty()) {
                        user_connections.erase(user_it);
                    }
                }
                
                connections.erase(conn_it);
                active_connections--;
            }
            
            handle_to_connection_id.erase(hdl_it);
        }
        
        track_connection_removed();
    }
    
    void on_message(connection_hdl hdl, server::message_ptr msg) {
        try {
            std::string payload = msg->get_payload();
            auto message_opt = WebSocketMessage::from_string(payload);
            
            if (!message_opt) {
                send_error_message(hdl, "Invalid message format");
                return;
            }
            
            auto& message = *message_opt;
            
            // Handle different message types
            switch (message.type) {
                case WebSocketMessage::Type::AUTH_REQUEST:
                    handle_auth_request(hdl, message);
                    break;
                    
                case WebSocketMessage::Type::SUBSCRIBE:
                    handle_subscribe_request(hdl, message);
                    break;
                    
                case WebSocketMessage::Type::UNSUBSCRIBE:
                    handle_unsubscribe_request(hdl, message);
                    break;
                    
                case WebSocketMessage::Type::PING:
                    handle_ping_request(hdl, message);
                    break;
                    
                default:
                    send_error_message(hdl, "Unsupported message type");
                    break;
            }
            
            // Update connection activity
            update_connection_activity_by_handle(hdl);
            
        } catch (const std::exception& e) {
            send_error_message(hdl, "Message processing error");
        }
    }
    
    void on_ping(connection_hdl hdl, std::string payload) {
        update_connection_activity_by_handle(hdl);
    }
    
    void on_pong(connection_hdl hdl, std::string payload) {
        update_connection_activity_by_handle(hdl);
    }
    
    void handle_auth_request(connection_hdl hdl, const WebSocketMessage& message) {
        try {
            std::string token = message.payload.value("token", "");
            std::string session_id = message.payload.value("session_id", "");
            nlohmann::json client_info = message.payload.value("client_info", nlohmann::json{});
            
            if (token.empty()) {
                send_auth_response(hdl, false, "Token required");
                return;
            }
            
            // Validate JWT token and extract user ID
            std::string user_id;
            if (!validate_jwt_token(token, user_id)) {
                send_auth_response(hdl, false, "Invalid token");
                return;
            }
            
            // Create connection
            std::string connection_id = generate_connection_id();
            auto connection = std::make_unique<WebSocketConnection>();
            connection->connection_id = connection_id;
            connection->user_id = user_id;
            connection->session_id = session_id;
            connection->handle = hdl;
            connection->connected_at = std::chrono::system_clock::now();
            connection->last_ping = connection->connected_at;
            connection->last_activity = connection->connected_at;
            connection->is_authenticated = true;
            connection->is_active = true;
            connection->client_capabilities = client_info;
            
            // Extract client info if available
            if (client_info.contains("user_agent")) {
                connection->user_agent = client_info["user_agent"].get<std::string>();
            }
            if (client_info.contains("device_type")) {
                connection->device_type = client_info["device_type"].get<std::string>();
            }
            
            // Subscribe to all notification types by default
            connection->subscribed_types = {
                models::NotificationType::LIKE,
                models::NotificationType::COMMENT,
                models::NotificationType::FOLLOW,
                models::NotificationType::MENTION,
                models::NotificationType::RENOTE,
                models::NotificationType::DIRECT_MESSAGE
            };
            
            {
                std::lock_guard<std::mutex> lock(connections_mutex);
                
                // Store connection
                connections[connection_id] = std::move(connection);
                handle_to_connection_id[hdl] = connection_id;
                user_connections[user_id].push_back(connection_id);
                active_connections++;
            }
            
            send_auth_response(hdl, true, "Authentication successful");
            
        } catch (const std::exception& e) {
            send_auth_response(hdl, false, "Authentication error");
        }
    }
    
    void handle_subscribe_request(connection_hdl hdl, const WebSocketMessage& message) {
        // Implementation for subscription management
        send_status_message(hdl, "Subscription updated");
    }
    
    void handle_unsubscribe_request(connection_hdl hdl, const WebSocketMessage& message) {
        // Implementation for unsubscription management
        send_status_message(hdl, "Unsubscription updated");
    }
    
    void handle_ping_request(connection_hdl hdl, const WebSocketMessage& message) {
        WebSocketMessage pong_message;
        pong_message.type = WebSocketMessage::Type::PONG;
        pong_message.message_id = generate_message_id();
        pong_message.timestamp = std::chrono::system_clock::now();
        pong_message.payload = message.payload;
        
        send_raw_message(hdl, pong_message.to_string());
    }
    
    void send_auth_response(connection_hdl hdl, bool success, const std::string& message) {
        WebSocketMessage response;
        response.type = WebSocketMessage::Type::AUTH_RESPONSE;
        response.message_id = generate_message_id();
        response.timestamp = std::chrono::system_clock::now();
        response.payload = {
            {"success", success},
            {"message", message}
        };
        
        send_raw_message(hdl, response.to_string());
    }
    
    void send_status_message(connection_hdl hdl, const std::string& status) {
        WebSocketMessage message;
        message.type = WebSocketMessage::Type::STATUS_UPDATE;
        message.message_id = generate_message_id();
        message.timestamp = std::chrono::system_clock::now();
        message.payload = {{"status", status}};
        
        send_raw_message(hdl, message.to_string());
    }
    
    void send_error_message(connection_hdl hdl, const std::string& error) {
        WebSocketMessage message;
        message.type = WebSocketMessage::Type::ERROR;
        message.message_id = generate_message_id();
        message.timestamp = std::chrono::system_clock::now();
        message.payload = {{"error", error}};
        
        send_raw_message(hdl, message.to_string());
    }
    
    bool send_raw_message(connection_hdl hdl, const std::string& message) {
        try {
            websocket_server.send(hdl, message, websocketpp::frame::opcode::text);
            track_message_sent();
            return true;
        } catch (const std::exception& e) {
            track_message_failed();
            return false;
        }
    }
    
    std::string generate_connection_id() const {
        uuid_t uuid;
        uuid_generate_random(uuid);
        char uuid_str[37];
        uuid_unparse_lower(uuid, uuid_str);
        return std::string(uuid_str);
    }
    
    std::string generate_message_id() const {
        static std::atomic<uint64_t> counter{0};
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
        return std::to_string(timestamp) + "_" + std::to_string(counter++);
    }
    
    void update_connection_activity_by_handle(connection_hdl hdl) {
        std::lock_guard<std::mutex> lock(connections_mutex);
        
        auto hdl_it = handle_to_connection_id.find(hdl);
        if (hdl_it != handle_to_connection_id.end()) {
            const std::string& connection_id = hdl_it->second;
            auto conn_it = connections.find(connection_id);
            if (conn_it != connections.end()) {
                conn_it->second->last_activity = std::chrono::system_clock::now();
                conn_it->second->last_ping = std::chrono::system_clock::now();
            }
        }
    }
    
    bool validate_jwt_token(const std::string& token, std::string& user_id) {
        try {
            if (config.jwt_secret.empty()) {
                // Reject unauthenticated connections when secret is not set
                return false;
            }
            
            auto verifier = jwt::verify()
                .allow_algorithm(jwt::algorithm::hs256{config.jwt_secret})
                .with_issuer("sonet");
            
            auto decoded = jwt::decode(token);
            verifier.verify(decoded);
            
            user_id = decoded.get_payload_claim("user_id").as_string();
            return !user_id.empty();
            
        } catch (const std::exception& e) {
            return false;
        }
    }
    
    void track_message_sent() { messages_sent++; }
    void track_message_failed() { messages_failed++; }
    void track_connection_added() { connections_added++; }
    void track_connection_removed() { connections_removed++; }
    
    void start_timers() {
        timers_running = true;
        
        ping_timer_thread = std::thread([this]() {
            while (timers_running.load()) {
                handle_ping_timer();
                std::this_thread::sleep_for(config.ping_interval);
            }
        });
        
        cleanup_timer_thread = std::thread([this]() {
            while (timers_running.load()) {
                handle_cleanup_timer();
                std::this_thread::sleep_for(std::chrono::minutes(5)); // Cleanup every 5 minutes
            }
        });
    }
    
    void handle_ping_timer() {
        std::lock_guard<std::mutex> lock(connections_mutex);
        
        for (const auto& [connection_id, connection] : connections) {
            if (connection->is_active) {
                WebSocketMessage ping_message;
                ping_message.type = WebSocketMessage::Type::PING;
                ping_message.message_id = generate_message_id();
                ping_message.timestamp = std::chrono::system_clock::now();
                
                send_raw_message(connection->handle, ping_message.to_string());
            }
        }
    }
    
    void handle_cleanup_timer() {
        cleanup_expired_connections();
        cleanup_idle_connections();
    }
    
    int cleanup_expired_connections() {
        std::lock_guard<std::mutex> lock(connections_mutex);
        
        std::vector<std::string> expired_connections;
        
        for (const auto& [connection_id, connection] : connections) {
            if (connection->is_expired()) {
                expired_connections.push_back(connection_id);
            }
        }
        
        // Remove expired connections
        for (const std::string& connection_id : expired_connections) {
            auto conn_it = connections.find(connection_id);
            if (conn_it != connections.end()) {
                const std::string& user_id = conn_it->second->user_id;
                connection_hdl hdl = conn_it->second->handle;
                
                // Remove from user connections
                auto user_it = user_connections.find(user_id);
                if (user_it != user_connections.end()) {
                    auto& user_conn_list = user_it->second;
                    user_conn_list.erase(
                        std::remove(user_conn_list.begin(), user_conn_list.end(), connection_id),
                        user_conn_list.end()
                    );
                    
                    if (user_conn_list.empty()) {
                        user_connections.erase(user_it);
                    }
                }
                
                handle_to_connection_id.erase(hdl);
                connections.erase(conn_it);
                active_connections--;
                
                // Close the WebSocket connection
                try {
                    websocket_server.close(hdl, websocketpp::close::status::going_away, "Connection expired");
                } catch (const std::exception& e) {
                    // Log error
                }
            }
        }
        
        return static_cast<int>(expired_connections.size());
    }
    
    int cleanup_idle_connections() {
        std::lock_guard<std::mutex> lock(connections_mutex);
        
        std::vector<std::string> idle_connections;
        
        for (const auto& [connection_id, connection] : connections) {
            if (connection->is_idle()) {
                idle_connections.push_back(connection_id);
            }
        }
        
        // Mark idle connections as inactive but don't remove them yet
        for (const std::string& connection_id : idle_connections) {
            auto conn_it = connections.find(connection_id);
            if (conn_it != connections.end()) {
                conn_it->second->is_active = false;
            }
        }
        
        return static_cast<int>(idle_connections.size());
    }
};

WebSocketPPChannel::WebSocketPPChannel(const Config& config)
    : pimpl_(std::make_unique<Impl>(config)) {
}

WebSocketPPChannel::~WebSocketPPChannel() = default;

std::future<bool> WebSocketPPChannel::start_server(int port, const std::string& host) {
    auto promise = std::make_shared<std::promise<bool>>();
    auto future = promise->get_future();
    
    if (pimpl_->is_running.load()) {
        promise->set_value(false);
        return future;
    }
    
    pimpl_->server_thread = std::thread([this, port, host, promise]() {
        try {
            pimpl_->websocket_server.listen(port);
            pimpl_->websocket_server.start_accept();
            
            pimpl_->is_running = true;
            pimpl_->start_timers();
            
            promise->set_value(true);
            
            // Run the server
            pimpl_->websocket_server.run();
            
        } catch (const std::exception& e) {
            promise->set_value(false);
        }
    });
    
    return future;
}

void WebSocketPPChannel::stop_server() {
    pimpl_->stop_server();
}

bool WebSocketPPChannel::is_running() const {
    return pimpl_->is_running.load();
}

std::string WebSocketPPChannel::add_connection(websocketpp::connection_hdl hdl,
                                             const std::string& user_id,
                                             const std::string& session_id,
                                             const nlohmann::json& client_info) {
    // Connection will be added when authentication message is received
    // This method is for backward compatibility
    return "";
}

bool WebSocketPPChannel::remove_connection(const std::string& connection_id) {
    std::lock_guard<std::mutex> lock(pimpl_->connections_mutex);
    
    auto conn_it = pimpl_->connections.find(connection_id);
    if (conn_it == pimpl_->connections.end()) {
        return false;
    }
    
    const std::string& user_id = conn_it->second->user_id;
    connection_hdl hdl = conn_it->second->handle;
    
    // Remove from user connections
    auto user_it = pimpl_->user_connections.find(user_id);
    if (user_it != pimpl_->user_connections.end()) {
        auto& user_conn_list = user_it->second;
        user_conn_list.erase(
            std::remove(user_conn_list.begin(), user_conn_list.end(), connection_id),
            user_conn_list.end()
        );
        
        if (user_conn_list.empty()) {
            pimpl_->user_connections.erase(user_it);
        }
    }
    
    pimpl_->handle_to_connection_id.erase(hdl);
    pimpl_->connections.erase(conn_it);
    pimpl_->active_connections--;
    
    // Close the WebSocket connection
    try {
        pimpl_->websocket_server.close(hdl, websocketpp::close::status::normal, "Connection removed");
    } catch (const std::exception& e) {
        // Log error
    }
    
    return true;
}

std::future<WebSocketDeliveryResult> WebSocketPPChannel::send_to_user(
    const models::Notification& notification,
    const std::string& user_id) {
    
    auto promise = std::make_shared<std::promise<WebSocketDeliveryResult>>();
    auto future = promise->get_future();
    
    std::thread([this, notification, user_id, promise]() {
        WebSocketDeliveryResult result;
        auto start_time = std::chrono::high_resolution_clock::now();
        
        try {
            // Get template for notification type
            std::lock_guard<std::mutex> template_lock(pimpl_->templates_mutex);
            auto template_it = pimpl_->templates.find(notification.type);
            if (template_it == pimpl_->templates.end()) {
                result.success = false;
                result.error_message = "No template found for notification type";
                result.sent_at = std::chrono::system_clock::now();
                promise->set_value(result);
                return;
            }
            
            const auto& ws_template = template_it->second;
            template_lock.~lock_guard();
            
            // Render notification message
            WebSocketMessage message = render_notification_message(notification, ws_template);
            std::string message_str = message.to_string();
            
            // Get user connections
            std::vector<std::string> connection_ids;
            {
                std::lock_guard<std::mutex> conn_lock(pimpl_->connections_mutex);
                auto user_it = pimpl_->user_connections.find(user_id);
                if (user_it != pimpl_->user_connections.end()) {
                    connection_ids = user_it->second;
                }
            }
            
            if (connection_ids.empty()) {
                result.success = false;
                result.error_message = "No active connections for user";
                result.sent_at = std::chrono::system_clock::now();
                promise->set_value(result);
                return;
            }
            
            // Send to all active connections for this user
            bool any_success = false;
            for (const std::string& connection_id : connection_ids) {
                std::lock_guard<std::mutex> conn_lock(pimpl_->connections_mutex);
                auto conn_it = pimpl_->connections.find(connection_id);
                if (conn_it != pimpl_->connections.end() && conn_it->second->is_active) {
                    // Check if user is subscribed to this notification type
                    if (conn_it->second->subscribed_types.find(notification.type) != 
                        conn_it->second->subscribed_types.end()) {
                        
                        bool send_success = pimpl_->send_raw_message(conn_it->second->handle, message_str);
                        if (send_success) {
                            any_success = true;
                            result.connection_id = connection_id;
                        }
                    }
                }
            }
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            result.success = any_success;
            result.message_id = message.message_id;
            result.sent_at = std::chrono::system_clock::now();
            result.delivery_time = duration;
            
            if (!any_success) {
                result.error_message = "Failed to deliver to any connection";
            }
            
        } catch (const std::exception& e) {
            result.success = false;
            result.error_message = e.what();
            result.sent_at = std::chrono::system_clock::now();
        }
        
        promise->set_value(result);
    }).detach();
    
    return future;
}

WebSocketMessage WebSocketPPChannel::render_notification_message(
    const models::Notification& notification,
    const WebSocketTemplate& template) const {
    
    WebSocketMessage message;
    message.type = WebSocketMessage::Type::NOTIFICATION;
    message.message_id = pimpl_->generate_message_id();
    message.timestamp = std::chrono::system_clock::now();
    message.user_id = notification.user_id;
    
    // Extract template variables
    auto variables = extract_template_variables(notification);
    
    // Render template content
    std::string title = replace_template_variables(template.title_template, variables);
    std::string body = replace_template_variables(template.message_template, variables);
    std::string icon = replace_template_variables(template.icon_template, variables);
    std::string action = replace_template_variables(template.action_template, variables);
    
    // Build payload
    message.payload = {
        {"notification_id", notification.id},
        {"type", static_cast<int>(notification.type)},
        {"title", title},
        {"message", body},
        {"icon", icon},
        {"action", action},
        {"show_avatar", template.show_avatar},
        {"show_timestamp", template.show_timestamp},
        {"auto_dismiss", template.auto_dismiss},
        {"dismiss_after", template.dismiss_after.count()},
        {"priority", static_cast<int>(notification.priority)},
        {"custom_data", template.custom_data},
        {"template_data", notification.template_data}
    };
    
    return message;
}

std::unordered_map<std::string, std::string> WebSocketPPChannel::extract_template_variables(
    const models::Notification& notification) const {
    
    std::unordered_map<std::string, std::string> variables;
    
    // Extract basic notification data
    variables["notification_id"] = notification.id;
    variables["user_id"] = notification.user_id;
    variables["sender_id"] = notification.sender_id;
    
    // Extract template data
    for (const auto& [key, value] : notification.template_data.items()) {
        if (value.is_string()) {
            variables[key] = value.get<std::string>();
        } else {
            variables[key] = value.dump();
        }
    }
    
    return variables;
}

std::string WebSocketPPChannel::replace_template_variables(
    const std::string& template_str,
    const std::unordered_map<std::string, std::string>& variables) const {
    
    std::string result = template_str;
    
    for (const auto& [key, value] : variables) {
        std::string placeholder = "{{" + key + "}}";
        size_t pos = 0;
        while ((pos = result.find(placeholder, pos)) != std::string::npos) {
            result.replace(pos, placeholder.length(), value);
            pos += value.length();
        }
    }
    
    return result;
}

bool WebSocketPPChannel::register_template(models::NotificationType type, const WebSocketTemplate& template) {
    if (!template.is_valid()) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
    pimpl_->templates[type] = template;
    return true;
}

std::optional<WebSocketTemplate> WebSocketPPChannel::get_template(models::NotificationType type) const {
    std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
    auto it = pimpl_->templates.find(type);
    if (it != pimpl_->templates.end()) {
        return it->second;
    }
    return std::nullopt;
}

int WebSocketPPChannel::get_active_connection_count() const {
    return pimpl_->active_connections.load();
}

nlohmann::json WebSocketPPChannel::get_connection_stats() const {
    std::lock_guard<std::mutex> lock(pimpl_->connections_mutex);
    
    int authenticated_connections = 0;
    int active_connections = 0;
    std::unordered_map<std::string, int> device_type_counts;
    
    for (const auto& [connection_id, connection] : pimpl_->connections) {
        if (connection->is_authenticated) {
            authenticated_connections++;
        }
        if (connection->is_active) {
            active_connections++;
        }
        device_type_counts[connection->device_type]++;
    }
    
    return nlohmann::json{
        {"total_connections", pimpl_->connections.size()},
        {"authenticated_connections", authenticated_connections},
        {"active_connections", active_connections},
        {"connections_added", pimpl_->connections_added.load()},
        {"connections_removed", pimpl_->connections_removed.load()},
        {"device_type_counts", device_type_counts},
        {"unique_users", pimpl_->user_connections.size()}
    };
}

nlohmann::json WebSocketPPChannel::get_delivery_stats() const {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    
    auto now = std::chrono::system_clock::now();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - pimpl_->stats_start).count();
    
    return nlohmann::json{
        {"messages_sent", pimpl_->messages_sent.load()},
        {"messages_failed", pimpl_->messages_failed.load()},
        {"success_rate", (pimpl_->messages_sent.load() + pimpl_->messages_failed.load()) > 0 ? 
            static_cast<double>(pimpl_->messages_sent.load()) / 
            (pimpl_->messages_sent.load() + pimpl_->messages_failed.load()) : 0.0},
        {"uptime_seconds", uptime},
        {"messages_per_second", uptime > 0 ? 
            static_cast<double>(pimpl_->messages_sent.load()) / uptime : 0.0}
    };
}

int WebSocketPPChannel::cleanup_expired_connections() {
    return pimpl_->cleanup_expired_connections();
}

int WebSocketPPChannel::cleanup_idle_connections() {
    return pimpl_->cleanup_idle_connections();
}

void WebSocketPPChannel::ping_all_connections() {
    pimpl_->handle_ping_timer();
}

// Factory implementations
std::unique_ptr<WebSocketChannel> WebSocketChannelFactory::create(ChannelType type, const nlohmann::json& config) {
    switch (type) {
        case ChannelType::WEBSOCKETPP: {
            WebSocketPPChannel::Config ws_config;
            ws_config.port = config.value("port", 8080);
            ws_config.host = config.value("host", "0.0.0.0");
            ws_config.jwt_secret = config.value("jwt_secret", "");
            return create_websocketpp(ws_config);
        }
        case ChannelType::MOCK:
            return create_mock();
        default:
            return nullptr;
    }
}

std::unique_ptr<WebSocketChannel> WebSocketChannelFactory::create_websocketpp(const WebSocketPPChannel::Config& config) {
    return std::make_unique<WebSocketPPChannel>(config);
}

std::unique_ptr<WebSocketChannel> WebSocketChannelFactory::create_mock() {
    WebSocketPPChannel::Config config;
    config.port = 8080;
    config.host = "localhost";
    return std::make_unique<WebSocketPPChannel>(config);
}

WebSocketTemplate WebSocketChannelFactory::create_like_template() {
    WebSocketTemplate template;
    template.type = models::NotificationType::LIKE;
    template.title_template = "{{sender_name}} liked your note";
    template.message_template = "\"{{note_excerpt}}\"";
    template.icon_template = "https://sonet.app/icons/like.svg";
    template.action_template = "/note/{{note_id}}";
    template.auto_dismiss = true;
    template.dismiss_after = std::chrono::seconds{8};
    return template;
}

} // namespace channels
} // namespace notification_service
} // namespace sonet