/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <mutex>
#include <atomic>
#include <thread>
#include <queue>
#include <json/json.h>
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#include "message.hpp"
#include "chat.hpp"

namespace sonet::messaging::realtime {

enum class ConnectionStatus {
    CONNECTING,
    CONNECTED,
    AUTHENTICATED,
    DISCONNECTING,
    DISCONNECTED,
    FAILED,
    BANNED
};

enum class MessageEventType {
    NEW_MESSAGE,
    MESSAGE_EDITED,
    MESSAGE_DELETED,
    MESSAGE_READ,
    MESSAGE_DELIVERED,
    TYPING_STARTED,
    TYPING_STOPPED,
    USER_JOINED_CHAT,
    USER_LEFT_CHAT,
    CHAT_CREATED,
    CHAT_UPDATED,
    CHAT_DELETED,
    PARTICIPANT_ADDED,
    PARTICIPANT_REMOVED,
    PARTICIPANT_ROLE_CHANGED,
    ONLINE_STATUS_CHANGED,
    CALL_INITIATED,
    CALL_ENDED,
    SYSTEM_NOTIFICATION
};

enum class OnlineStatus {
    ONLINE,
    AWAY,
    BUSY,
    INVISIBLE,
    OFFLINE
};

struct ClientConnection {
    std::string connection_id;
    std::string user_id;
    std::string device_id;
    std::string session_token;
    ConnectionStatus status;
    OnlineStatus online_status;
    
    std::chrono::system_clock::time_point connected_at;
    std::chrono::system_clock::time_point last_activity;
    std::chrono::system_clock::time_point authenticated_at;
    
    std::string ip_address;
    std::string user_agent;
    std::string platform;
    std::string app_version;
    
    std::unordered_set<std::string> subscribed_chats;
    std::queue<Json::Value> pending_messages;
    uint32_t message_count = 0;
    uint64_t bytes_sent = 0;
    uint64_t bytes_received = 0;
    
    // Rate limiting
    std::chrono::system_clock::time_point last_message_time;
    uint32_t messages_in_current_minute = 0;
    uint32_t rate_limit_violations = 0;
    
    Json::Value to_json() const;
    static ClientConnection from_json(const Json::Value& json);
    bool is_authenticated() const;
    bool is_rate_limited() const;
    void update_activity();
    void increment_message_count();
    void add_bytes_sent(uint64_t bytes);
    void add_bytes_received(uint64_t bytes);
};

struct TypingIndicator {
    std::string user_id;
    std::string chat_id;
    std::chrono::system_clock::time_point started_at;
    std::chrono::system_clock::time_point expires_at;
    
    Json::Value to_json() const;
    static TypingIndicator from_json(const Json::Value& json);
    bool is_expired() const;
};

struct RealtimeEvent {
    MessageEventType type;
    std::string chat_id;
    std::string user_id;
    std::string target_user_id;
    Json::Value data;
    std::chrono::system_clock::time_point timestamp;
    std::string event_id;
    uint32_t priority = 1;
    
    Json::Value to_json() const;
    static RealtimeEvent from_json(const Json::Value& json);
    std::vector<std::string> get_recipient_user_ids() const;
};

struct ConnectionMetrics {
    uint32_t total_connections = 0;
    uint32_t authenticated_connections = 0;
    uint32_t messages_sent_per_second = 0;
    uint32_t messages_received_per_second = 0;
    uint64_t total_bytes_sent = 0;
    uint64_t total_bytes_received = 0;
    uint32_t failed_authentications = 0;
    uint32_t rate_limit_violations = 0;
    std::chrono::system_clock::time_point last_reset;
    
    Json::Value to_json() const;
    void reset();
    void update_message_stats(bool sent, uint64_t bytes);
};

class WebSocketManager {
private:
    using server = websocketpp::server<websocketpp::config::asio>;
    using connection_hdl = server::connection_hdl;
    using message_ptr = server::message_ptr;
    
    // WebSocket server
    std::unique_ptr<server> server_;
    std::thread server_thread_;
    std::atomic<bool> running_;
    
    // Connection management
    std::unordered_map<std::string, std::shared_ptr<ClientConnection>> connections_;
    std::unordered_map<connection_hdl, std::string, std::owner_less<connection_hdl>> hdl_to_id_;
    std::unordered_map<std::string, connection_hdl> id_to_hdl_;
    std::unordered_map<std::string, std::unordered_set<std::string>> user_connections_;
    std::mutex connections_mutex_;
    
    // Chat subscriptions
    std::unordered_map<std::string, std::unordered_set<std::string>> chat_subscribers_;
    std::mutex subscriptions_mutex_;
    
    // Typing indicators
    std::unordered_map<std::string, std::vector<TypingIndicator>> typing_indicators_;
    std::mutex typing_mutex_;
    std::thread typing_cleanup_thread_;
    
    // Event queue and broadcasting
    std::queue<RealtimeEvent> event_queue_;
    std::mutex event_queue_mutex_;
    std::thread event_processor_thread_;
    
    // Metrics and monitoring
    ConnectionMetrics metrics_;
    std::mutex metrics_mutex_;
    
    // Configuration
    uint16_t port_;
    uint32_t max_connections_;
    uint32_t message_rate_limit_;
    std::chrono::seconds ping_interval_;
    std::chrono::seconds connection_timeout_;
    std::chrono::seconds typing_timeout_;
    
    // Security configuration
    std::unordered_set<std::string> allowed_origins_;
    bool require_tls_header_ = false; // if true, check X-Forwarded-Proto == https
    
    // Authentication callback
    std::function<bool(const std::string&, const std::string&)> auth_callback_;
    
    // Event handlers
    void on_open(connection_hdl hdl);
    void on_close(connection_hdl hdl);
    void on_message(connection_hdl hdl, message_ptr msg);
    void on_fail(connection_hdl hdl);
    
    // Message processing
    void process_client_message(const std::string& connection_id, const Json::Value& message);
    void handle_authentication(const std::string& connection_id, const Json::Value& auth_data);
    void handle_subscribe_chat(const std::string& connection_id, const Json::Value& data);
    void handle_unsubscribe_chat(const std::string& connection_id, const Json::Value& data);
    void handle_typing_indicator(const std::string& connection_id, const Json::Value& data);
    void handle_status_update(const std::string& connection_id, const Json::Value& data);
    void handle_ping(const std::string& connection_id, const Json::Value& data);
    
    // Internal utilities
    void process_event_queue();
    void cleanup_typing_indicators();
    void cleanup_expired_connections();
    void update_connection_metrics();
    bool is_connection_rate_limited(const std::string& connection_id);
    void send_to_connection(const std::string& connection_id, const Json::Value& message);
    std::vector<std::string> get_chat_connection_ids(const std::string& chat_id);
    
public:
    WebSocketManager(uint16_t port = 9096);
    ~WebSocketManager();
    
    // Server lifecycle
    bool start();
    void stop();
    bool is_running() const;
    
    // Configuration
    void set_max_connections(uint32_t max_connections);
    void set_message_rate_limit(uint32_t messages_per_minute);
    void set_ping_interval(std::chrono::seconds interval);
    void set_connection_timeout(std::chrono::seconds timeout);
    void set_typing_timeout(std::chrono::seconds timeout);
    void set_authentication_callback(std::function<bool(const std::string&, const std::string&)> callback);
    void set_allowed_origins(const std::vector<std::string>& origins);
    void set_require_tls_header(bool require_tls);
    
    // Connection management
    std::vector<std::shared_ptr<ClientConnection>> get_all_connections();
    std::shared_ptr<ClientConnection> get_connection(const std::string& connection_id);
    std::vector<std::shared_ptr<ClientConnection>> get_user_connections(const std::string& user_id);
    bool disconnect_connection(const std::string& connection_id, const std::string& reason = "");
    bool disconnect_user(const std::string& user_id, const std::string& reason = "");
    uint32_t get_connection_count();
    uint32_t get_authenticated_connection_count();
    
    // Message broadcasting
    void broadcast_to_chat(const std::string& chat_id, const RealtimeEvent& event);
    void broadcast_to_user(const std::string& user_id, const RealtimeEvent& event);
    void broadcast_to_users(const std::vector<std::string>& user_ids, const RealtimeEvent& event);
    void send_to_connection(const std::string& connection_id, const RealtimeEvent& event);
    
    // Event publishing
    void publish_message_event(const Message& message, MessageEventType event_type);
    void publish_chat_event(const Chat& chat, MessageEventType event_type, 
                           const std::string& actor_user_id = "");
    void publish_typing_event(const std::string& user_id, const std::string& chat_id, bool is_typing);
    void publish_status_event(const std::string& user_id, OnlineStatus status);
    void publish_system_notification(const std::string& chat_id, const std::string& message);
    
    // Chat subscriptions
    bool subscribe_to_chat(const std::string& connection_id, const std::string& chat_id);
    bool unsubscribe_from_chat(const std::string& connection_id, const std::string& chat_id);
    std::vector<std::string> get_user_subscribed_chats(const std::string& user_id);
    std::vector<std::string> get_chat_subscribers(const std::string& chat_id);
    
    // Typing indicators
    void start_typing(const std::string& user_id, const std::string& chat_id);
    void stop_typing(const std::string& user_id, const std::string& chat_id);
    std::vector<TypingIndicator> get_typing_users(const std::string& chat_id);
    
    // Online status
    void set_user_status(const std::string& user_id, OnlineStatus status);
    OnlineStatus get_user_status(const std::string& user_id);
    std::vector<std::string> get_online_users();
    std::vector<std::string> get_online_users_in_chat(const std::string& chat_id);
    
    // Monitoring and metrics
    ConnectionMetrics get_metrics();
    Json::Value get_detailed_metrics();
    void reset_metrics();
    std::vector<std::string> get_active_connection_ids();
    
    // Security and moderation
    bool ban_connection(const std::string& connection_id, const std::string& reason = "");
    bool ban_user(const std::string& user_id, const std::string& reason = "");
    bool unban_user(const std::string& user_id);
    bool is_user_banned(const std::string& user_id);
    std::vector<std::string> get_banned_users();
    
    // Rate limiting
    bool is_user_rate_limited(const std::string& user_id);
    void reset_user_rate_limit(const std::string& user_id);
    uint32_t get_user_message_count(const std::string& user_id, std::chrono::minutes within);
    
    // Health and diagnostics
    Json::Value get_health_status();
    Json::Value get_connection_diagnostics(const std::string& connection_id);
    void force_ping_all_connections();
    void cleanup_stale_connections();
    
    // Event system
    void add_event_to_queue(const RealtimeEvent& event);
    uint32_t get_pending_event_count();
    void clear_event_queue();
    
    // Utilities
    std::string generate_connection_id();
    std::string generate_event_id();
    bool is_valid_connection_id(const std::string& connection_id);
    std::chrono::milliseconds calculate_latency(const std::string& connection_id);
};

// WebSocket utilities
class WebSocketUtils {
public:
    static Json::Value create_message_event(MessageEventType type, const Json::Value& data);
    static Json::Value create_error_response(const std::string& error, const std::string& details = "");
    static Json::Value create_success_response(const Json::Value& data = Json::Value::null);
    static Json::Value create_ping_message();
    static Json::Value create_pong_message(const std::string& ping_id = "");
    static bool is_valid_json_message(const std::string& message);
    static MessageEventType parse_event_type(const std::string& type_str);
    static std::string event_type_to_string(MessageEventType type);
    static OnlineStatus parse_online_status(const std::string& status_str);
    static std::string online_status_to_string(OnlineStatus status);
    static std::string format_connection_info(const ClientConnection& connection);
    static size_t calculate_message_size(const Json::Value& message);
    static bool should_compress_message(const Json::Value& message);
    static std::string compress_message(const std::string& message);
    static std::string decompress_message(const std::string& compressed_message);
};

} // namespace sonet::messaging::realtime
