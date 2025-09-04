/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "include/websocket_manager.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/strand.hpp>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>

namespace sonet::messaging::realtime {

// ClientConnection implementation
Json::Value ClientConnection::to_json() const {
    Json::Value json;
    json["connection_id"] = connection_id;
    json["user_id"] = user_id;
    json["device_id"] = device_id;
    json["status"] = static_cast<int>(status);
    json["online_status"] = static_cast<int>(online_status);
    
    json["connected_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        connected_at.time_since_epoch()).count();
    json["last_activity"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_activity.time_since_epoch()).count();
    json["authenticated_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        authenticated_at.time_since_epoch()).count();
    
    json["ip_address"] = ip_address;
    json["user_agent"] = user_agent;
    json["platform"] = platform;
    json["app_version"] = app_version;
    
    Json::Value subscribed_chats_json(Json::arrayValue);
    for (const auto& chat_id : subscribed_chats) {
        subscribed_chats_json.append(chat_id);
    }
    json["subscribed_chats"] = subscribed_chats_json;
    
    json["message_count"] = message_count;
    json["bytes_sent"] = static_cast<Json::UInt64>(bytes_sent);
    json["bytes_received"] = static_cast<Json::UInt64>(bytes_received);
    json["rate_limit_violations"] = rate_limit_violations;
    
    return json;
}

bool ClientConnection::is_authenticated() const {
    return status == ConnectionStatus::AUTHENTICATED && !session_token.empty();
}

bool ClientConnection::is_rate_limited() const {
    auto now = std::chrono::system_clock::now();
    auto minute_start = std::chrono::time_point_cast<std::chrono::minutes>(now);
    auto last_minute_start = std::chrono::time_point_cast<std::chrono::minutes>(last_message_time);
    
    if (minute_start != last_minute_start) {
        // Reset counter for new minute
        const_cast<ClientConnection*>(this)->messages_in_current_minute = 0;
    }
    
    return messages_in_current_minute >= 60; // 60 messages per minute limit
}

void ClientConnection::update_activity() {
    last_activity = std::chrono::system_clock::now();
}

void ClientConnection::increment_message_count() {
    message_count++;
    auto now = std::chrono::system_clock::now();
    auto minute_start = std::chrono::time_point_cast<std::chrono::minutes>(now);
    auto last_minute_start = std::chrono::time_point_cast<std::chrono::minutes>(last_message_time);
    
    if (minute_start != last_minute_start) {
        messages_in_current_minute = 1;
    } else {
        messages_in_current_minute++;
    }
    
    last_message_time = now;
}

void ClientConnection::add_bytes_sent(uint64_t bytes) {
    bytes_sent += bytes;
}

void ClientConnection::add_bytes_received(uint64_t bytes) {
    bytes_received += bytes;
}

// TypingIndicator implementation
Json::Value TypingIndicator::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["chat_id"] = chat_id;
    json["started_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        started_at.time_since_epoch()).count();
    json["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        expires_at.time_since_epoch()).count();
    return json;
}

TypingIndicator TypingIndicator::from_json(const Json::Value& json) {
    TypingIndicator indicator;
    indicator.user_id = json["user_id"].asString();
    indicator.chat_id = json["chat_id"].asString();
    
    auto started_ms = json["started_at"].asInt64();
    indicator.started_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(started_ms));
    
    auto expires_ms = json["expires_at"].asInt64();
    indicator.expires_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(expires_ms));
    
    return indicator;
}

bool TypingIndicator::is_expired() const {
    return std::chrono::system_clock::now() > expires_at;
}

// RealtimeEvent implementation
Json::Value RealtimeEvent::to_json() const {
    Json::Value json;
    json["type"] = static_cast<int>(type);
    json["chat_id"] = chat_id;
    json["user_id"] = user_id;
    json["target_user_id"] = target_user_id;
    json["data"] = data;
    json["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        timestamp.time_since_epoch()).count();
    json["event_id"] = event_id;
    json["priority"] = priority;
    return json;
}

RealtimeEvent RealtimeEvent::from_json(const Json::Value& json) {
    RealtimeEvent event;
    event.type = static_cast<MessageEventType>(json["type"].asInt());
    event.chat_id = json["chat_id"].asString();
    event.user_id = json["user_id"].asString();
    event.target_user_id = json["target_user_id"].asString();
    event.data = json["data"];
    
    auto timestamp_ms = json["timestamp"].asInt64();
    event.timestamp = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(timestamp_ms));
    
    event.event_id = json["event_id"].asString();
    event.priority = json["priority"].asUInt();
    
    return event;
}

// ConnectionMetrics implementation
Json::Value ConnectionMetrics::to_json() const {
    Json::Value json;
    json["total_connections"] = total_connections;
    json["authenticated_connections"] = authenticated_connections;
    json["messages_sent_per_second"] = messages_sent_per_second;
    json["messages_received_per_second"] = messages_received_per_second;
    json["total_bytes_sent"] = static_cast<Json::UInt64>(total_bytes_sent);
    json["total_bytes_received"] = static_cast<Json::UInt64>(total_bytes_received);
    json["failed_authentications"] = failed_authentications;
    json["rate_limit_violations"] = rate_limit_violations;
    json["last_reset"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_reset.time_since_epoch()).count();
    return json;
}

void ConnectionMetrics::reset() {
    total_connections = 0;
    authenticated_connections = 0;
    messages_sent_per_second = 0;
    messages_received_per_second = 0;
    failed_authentications = 0;
    rate_limit_violations = 0;
    last_reset = std::chrono::system_clock::now();
}

void ConnectionMetrics::update_message_stats(bool sent, uint64_t bytes) {
    if (sent) {
        messages_sent_per_second++;
        total_bytes_sent += bytes;
    } else {
        messages_received_per_second++;
        total_bytes_received += bytes;
    }
}

// WebSocketManager implementation
WebSocketManager::WebSocketManager(uint16_t port)
    : port_(port), max_connections_(10000), message_rate_limit_(60),
      ping_interval_(std::chrono::seconds(30)), connection_timeout_(std::chrono::seconds(300)),
      typing_timeout_(std::chrono::seconds(10)), running_(false) {
    
    // Initialize WebSocket server
    server_ = std::make_unique<server>();
    
    // Configure server
    server_->set_access_channels(websocketpp::log::alevel::all);
    server_->clear_access_channels(websocketpp::log::alevel::frame_payload);
    server_->set_error_channels(websocketpp::log::elevel::all);
    
    // Initialize Asio
    server_->init_asio();
    
    // Set message handlers
    server_->set_message_handler([this](connection_hdl hdl, message_ptr msg) {
        on_message(hdl, msg);
    });
    
    server_->set_open_handler([this](connection_hdl hdl) {
        on_open(hdl);
    });
    
    server_->set_close_handler([this](connection_hdl hdl) {
        on_close(hdl);
    });
    
    server_->set_fail_handler([this](connection_hdl hdl) {
        on_fail(hdl);
    });
    
    // Set up periodic tasks
    server_->set_reuse_addr(true);
}

WebSocketManager::~WebSocketManager() {
    stop();
}

bool WebSocketManager::start() {
    try {
        server_->listen(port_);
        server_->start_accept();
        
        running_ = true;
        
        // Start server thread
        server_thread_ = std::thread([this]() {
            try {
                server_->run();
            } catch (const std::exception& e) {
                // Log error
                running_ = false;
            }
        });
        
        // Start background threads
        event_processor_thread_ = std::thread([this]() {
            process_event_queue();
        });
        
        typing_cleanup_thread_ = std::thread([this]() {
            while (running_) {
                cleanup_typing_indicators();
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        });
        
        return true;
        
    } catch (const std::exception& e) {
        running_ = false;
        return false;
    }
}

void WebSocketManager::stop() {
    running_ = false;
    
    if (server_) {
        server_->stop();
    }
    
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    
    if (event_processor_thread_.joinable()) {
        event_processor_thread_.join();
    }
    
    if (typing_cleanup_thread_.joinable()) {
        typing_cleanup_thread_.join();
    }
    
    // Clear all connections
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_.clear();
    hdl_to_id_.clear();
    id_to_hdl_.clear();
    user_connections_.clear();
}

bool WebSocketManager::is_running() const {
    return running_.load();
}

void WebSocketManager::set_allowed_origins(const std::vector<std::string>& origins) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    allowed_origins_.clear();
    for (const auto& o : origins) allowed_origins_.insert(o);
}

void WebSocketManager::set_require_tls_header(bool require_tls) {
    require_tls_header_ = require_tls;
}

void WebSocketManager::on_open(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(connections_mutex_);

    if (connections_.size() >= max_connections_) {
        try {
            server_->close(hdl, websocketpp::close::status::policy_violation, "Server full");
        } catch (...) {}
        return;
    }

    std::string connection_id = generate_connection_id();

    auto connection = std::make_shared<ClientConnection>();
    connection->connection_id = connection_id;
    connection->status = ConnectionStatus::CONNECTED;
    connection->online_status = OnlineStatus::ONLINE;
    connection->connected_at = std::chrono::system_clock::now();
    connection->last_activity = connection->connected_at;

    try {
        auto con = server_->get_con_from_hdl(hdl);
        connection->ip_address = con->get_remote_endpoint();

        // Reject tokens in URL to avoid leakage via logs and intermediaries
        std::string resource = con->get_resource();
        if (resource.find("token=") != std::string::npos) {
            server_->close(hdl, websocketpp::close::status::policy_violation, "Token in URL not allowed");
            return;
        }

        // Enforce TLS if behind proxy
        if (require_tls_header_) {
            auto xfproto = con->get_request_header("X-Forwarded-Proto");
            if (xfproto != "https") {
                server_->close(hdl, websocketpp::close::status::policy_violation, "HTTPS required");
                return;
            }
        }

        // Origin allowlist
        auto origin = con->get_request_header("Origin");
        if (!allowed_origins_.empty()) {
            if (origin.empty() || allowed_origins_.find(origin) == allowed_origins_.end()) {
                server_->close(hdl, websocketpp::close::status::policy_violation, "Origin not allowed");
                return;
            }
        }

        auto ua = con->get_request_header("User-Agent");
        if (!ua.empty()) connection->user_agent = ua;
    } catch (...) {
        // ignore
    }

    connections_[connection_id] = connection;
    hdl_to_id_[hdl] = connection_id;
    id_to_hdl_[connection_id] = hdl;

    metrics_.total_connections++;

    Json::Value welcome;
    welcome["type"] = "connection_established";
    welcome["connection_id"] = connection_id;
    welcome["server_version"] = "1.0.0";
    welcome["encryption_supported"] = true;
    welcome["features"] = Json::arrayValue;
    welcome["features"].append("e2e_encryption");
    welcome["features"].append("typing_indicators");
    welcome["features"].append("read_receipts");

    send_to_connection(connection_id, welcome);
}

void WebSocketManager::on_close(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto hdl_it = hdl_to_id_.find(hdl);
    if (hdl_it == hdl_to_id_.end()) {
        return;
    }
    
    std::string connection_id = hdl_it->second;
    
    auto conn_it = connections_.find(connection_id);
    if (conn_it != connections_.end()) {
        auto connection = conn_it->second;
        
        // Remove from user connections
        if (!connection->user_id.empty()) {
            auto user_it = user_connections_.find(connection->user_id);
            if (user_it != user_connections_.end()) {
                user_it->second.erase(connection_id);
                if (user_it->second.empty()) {
                    user_connections_.erase(user_it);
                }
            }
        }
        
        // Clean up subscriptions
        std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
        for (const auto& chat_id : connection->subscribed_chats) {
            auto chat_it = chat_subscribers_.find(chat_id);
            if (chat_it != chat_subscribers_.end()) {
                chat_it->second.erase(connection_id);
                if (chat_it->second.empty()) {
                    chat_subscribers_.erase(chat_it);
                }
            }
        }
        
        connections_.erase(conn_it);
        if (connection->is_authenticated()) {
            metrics_.authenticated_connections--;
        }
    }
    
    hdl_to_id_.erase(hdl_it);
    id_to_hdl_.erase(connection_id);
}

void WebSocketManager::on_fail(connection_hdl hdl) {
    // Handle the same as close
    on_close(hdl);
}

void WebSocketManager::on_message(connection_hdl hdl, message_ptr msg) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto hdl_it = hdl_to_id_.find(hdl);
    if (hdl_it == hdl_to_id_.end()) {
        return;
    }
    
    std::string connection_id = hdl_it->second;
    auto conn_it = connections_.find(connection_id);
    if (conn_it == connections_.end()) {
        return;
    }
    
    auto connection = conn_it->second;
    
    // Check rate limiting
    if (connection->is_rate_limited()) {
        connection->rate_limit_violations++;
        metrics_.rate_limit_violations++;
        
        Json::Value error;
        error["type"] = "error";
        error["error"] = "rate_limit_exceeded";
        error["message"] = "Too many messages per minute";
        error["retry_after"] = 60;
        
        send_to_connection(connection_id, error);
        return;
    }
    
    connection->update_activity();
    connection->increment_message_count();
    connection->add_bytes_received(msg->get_payload().length());
    
    // Parse message
    try {
        Json::Value message_json;
        Json::Reader reader;
        if (!reader.parse(msg->get_payload(), message_json)) {
            Json::Value error;
            error["type"] = "error";
            error["error"] = "invalid_json";
            error["message"] = "Invalid JSON format";
            
            send_to_connection(connection_id, error);
            return;
        }
        
        process_client_message(connection_id, message_json);
        
    } catch (const std::exception& e) {
        Json::Value error;
        error["type"] = "error";
        error["error"] = "processing_error";
        error["message"] = "Failed to process message";
        
        send_to_connection(connection_id, error);
    }
}

void WebSocketManager::process_client_message(const std::string& connection_id, 
                                            const Json::Value& message) {
    
    if (!message.isMember("type")) {
        Json::Value error;
        error["type"] = "error";
        error["error"] = "missing_type";
        error["message"] = "Message type is required";
        
        send_to_connection(connection_id, error);
        return;
    }
    
    std::string msg_type = message["type"].asString();
    
    if (msg_type == "authenticate") {
        handle_authentication(connection_id, message);
    } else if (msg_type == "subscribe_chat") {
        handle_subscribe_chat(connection_id, message);
    } else if (msg_type == "unsubscribe_chat") {
        handle_unsubscribe_chat(connection_id, message);
    } else if (msg_type == "typing") {
        handle_typing_indicator(connection_id, message);
    } else if (msg_type == "status_update") {
        handle_status_update(connection_id, message);
    } else if (msg_type == "ping") {
        handle_ping(connection_id, message);
    } else {
        Json::Value error;
        error["type"] = "error";
        error["error"] = "unknown_message_type";
        error["message"] = "Unknown message type: " + msg_type;
        
        send_to_connection(connection_id, error);
    }
}

void WebSocketManager::handle_authentication(const std::string& connection_id, 
                                           const Json::Value& auth_data) {

    auto connection = get_connection(connection_id);
    if (!connection) {
        return;
    }

    // Simple per-connection auth attempt rate limiting
    static thread_local std::unordered_map<std::string, std::pair<std::chrono::system_clock::time_point,int>> auth_attempts;
    auto& entry = auth_attempts[connection_id];
    auto now = std::chrono::system_clock::now();
    if (entry.first + std::chrono::seconds(10) < now) {
        entry = {now, 0};
    }
    entry.second++;
    if (entry.second > 5) {
        Json::Value error;
        error["type"] = "auth_error";
        error["error"] = "too_many_attempts";
        error["message"] = "Too many authentication attempts";
        send_to_connection(connection_id, error);
        return;
    }

    if (!auth_data.isMember("token") || !auth_data.isMember("user_id")) {
        Json::Value error;
        error["type"] = "auth_error";
        error["error"] = "missing_credentials";
        error["message"] = "Token and user_id are required";
        send_to_connection(connection_id, error);
        return;
    }

    std::string token = auth_data["token"].asString();
    std::string user_id = auth_data["user_id"].asString();

    bool auth_success = false;
    if (auth_callback_) {
        auth_success = auth_callback_(user_id, token);
    }

    if (auth_success) {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connection->user_id = user_id;
        connection->session_token = token;
        connection->status = ConnectionStatus::AUTHENTICATED;
        connection->authenticated_at = std::chrono::system_clock::now();

        if (auth_data.isMember("device_id")) {
            connection->device_id = auth_data["device_id"].asString();
        }
        if (auth_data.isMember("platform")) {
            connection->platform = auth_data["platform"].asString();
        }
        if (auth_data.isMember("app_version")) {
            connection->app_version = auth_data["app_version"].asString();
        }

        user_connections_[user_id].insert(connection_id);
        metrics_.authenticated_connections++;

        Json::Value success;
        success["type"] = "auth_success";
        success["user_id"] = user_id;
        success["connection_id"] = connection_id;
        success["features"] = Json::arrayValue;
        success["features"].append("e2e_encryption");
        success["features"].append("typing_indicators");
        success["features"].append("read_receipts");
        send_to_connection(connection_id, success);

    } else {
        metrics_.failed_authentications++;
        Json::Value error;
        error["type"] = "auth_error";
        error["error"] = "invalid_credentials";
        error["message"] = "Authentication failed";
        send_to_connection(connection_id, error);
    }
}

void WebSocketManager::handle_subscribe_chat(const std::string& connection_id, 
                                           const Json::Value& data) {
    
    auto connection = get_connection(connection_id);
    if (!connection || !connection->is_authenticated()) {
        Json::Value error;
        error["type"] = "error";
        error["error"] = "not_authenticated";
        error["message"] = "Authentication required";
        
        send_to_connection(connection_id, error);
        return;
    }
    
    if (!data.isMember("chat_id")) {
        Json::Value error;
        error["type"] = "error";
        error["error"] = "missing_chat_id";
        error["message"] = "Chat ID is required";
        
        send_to_connection(connection_id, error);
        return;
    }
    
    std::string chat_id = data["chat_id"].asString();
    
    if (subscribe_to_chat(connection_id, chat_id)) {
        Json::Value success;
        success["type"] = "chat_subscribed";
        success["chat_id"] = chat_id;
        
        send_to_connection(connection_id, success);
    } else {
        Json::Value error;
        error["type"] = "error";
        error["error"] = "subscription_failed";
        error["message"] = "Failed to subscribe to chat";
        
        send_to_connection(connection_id, error);
    }
}

void WebSocketManager::handle_typing_indicator(const std::string& connection_id, 
                                             const Json::Value& data) {
    
    auto connection = get_connection(connection_id);
    if (!connection || !connection->is_authenticated()) {
        return;
    }
    
    if (!data.isMember("chat_id") || !data.isMember("is_typing")) {
        return;
    }
    
    std::string chat_id = data["chat_id"].asString();
    bool is_typing = data["is_typing"].asBool();
    
    if (is_typing) {
        start_typing(connection->user_id, chat_id);
    } else {
        stop_typing(connection->user_id, chat_id);
    }
}

void WebSocketManager::broadcast_to_chat(const std::string& chat_id, const RealtimeEvent& event) {
	std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
	
	auto chat_it = chat_subscribers_.find(chat_id);
	if (chat_it == chat_subscribers_.end()) {
		// Fallback: no explicit subscribers; broadcast to all authenticated connections
		Json::Value event_json = event.to_json();
		{
			std::lock_guard<std::mutex> lock(connections_mutex_);
			for (const auto& entry : connections_) {
				const auto& connection_id = entry.first;
				const auto& connection = entry.second;
				if (connection && connection->is_authenticated()) {
					send_to_connection(connection_id, event_json);
				}
			}
		}
		return;
	}
	
	Json::Value event_json = event.to_json();
	
	for (const auto& connection_id : chat_it->second) {
		send_to_connection(connection_id, event_json);
	}
}

void WebSocketManager::send_to_connection(const std::string& connection_id, 
                                        const Json::Value& message) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto conn_it = connections_.find(connection_id);
    if (conn_it == connections_.end()) {
        return;
    }
    
    auto hdl_it = id_to_hdl_.find(connection_id);
    if (hdl_it == id_to_hdl_.end()) {
        return;
    }
    
    try {
        Json::StreamWriterBuilder builder;
        std::string message_str = Json::writeString(builder, message);
        
        server_->send(hdl_it->second, message_str, websocketpp::frame::opcode::text);
        
        conn_it->second->add_bytes_sent(message_str.length());
        metrics_.update_message_stats(true, message_str.length());
        
    } catch (const std::exception& e) {
        // Handle send error - connection might be closed
        on_close(hdl_it->second);
    }
}

bool WebSocketManager::subscribe_to_chat(const std::string& connection_id, 
                                        const std::string& chat_id) {
    auto connection = get_connection(connection_id);
    if (!connection || !connection->is_authenticated()) {
        return false;
    }
    
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    
    connection->subscribed_chats.insert(chat_id);
    chat_subscribers_[chat_id].insert(connection_id);
    
    return true;
}

void WebSocketManager::start_typing(const std::string& user_id, const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(typing_mutex_);
    
    auto now = std::chrono::system_clock::now();
    
    TypingIndicator indicator;
    indicator.user_id = user_id;
    indicator.chat_id = chat_id;
    indicator.started_at = now;
    indicator.expires_at = now + typing_timeout_;
    
    // Remove existing typing indicator for this user in this chat
    auto& indicators = typing_indicators_[chat_id];
    indicators.erase(
        std::remove_if(indicators.begin(), indicators.end(),
            [&user_id](const TypingIndicator& ind) {
                return ind.user_id == user_id;
            }),
        indicators.end()
    );
    
    indicators.push_back(indicator);
    
    // Broadcast typing event
    RealtimeEvent event;
    event.type = MessageEventType::TYPING_STARTED;
    event.chat_id = chat_id;
    event.user_id = user_id;
    event.timestamp = now;
    event.event_id = generate_event_id();
    
    broadcast_to_chat(chat_id, event);
}

void WebSocketManager::stop_typing(const std::string& user_id, const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(typing_mutex_);
    
    auto it = typing_indicators_.find(chat_id);
    if (it == typing_indicators_.end()) {
        return;
    }
    
    auto& indicators = it->second;
    auto removed = std::remove_if(indicators.begin(), indicators.end(),
        [&user_id](const TypingIndicator& ind) {
            return ind.user_id == user_id;
        });
    
    if (removed != indicators.end()) {
        indicators.erase(removed, indicators.end());
        
        // Broadcast typing stopped event
        RealtimeEvent event;
        event.type = MessageEventType::TYPING_STOPPED;
        event.chat_id = chat_id;
        event.user_id = user_id;
        event.timestamp = std::chrono::system_clock::now();
        event.event_id = generate_event_id();
        
        broadcast_to_chat(chat_id, event);
    }
}

void WebSocketManager::cleanup_typing_indicators() {
    std::lock_guard<std::mutex> lock(typing_mutex_);
    
    auto now = std::chrono::system_clock::now();
    
    for (auto it = typing_indicators_.begin(); it != typing_indicators_.end();) {
        auto& indicators = it->second;
        
        // Remove expired indicators
        auto removed = std::remove_if(indicators.begin(), indicators.end(),
            [now](const TypingIndicator& ind) {
                return ind.is_expired();
            });
        
        if (removed != indicators.end()) {
            // Send stop typing events for expired indicators
            for (auto expired_it = removed; expired_it != indicators.end(); ++expired_it) {
                RealtimeEvent event;
                event.type = MessageEventType::TYPING_STOPPED;
                event.chat_id = expired_it->chat_id;
                event.user_id = expired_it->user_id;
                event.timestamp = now;
                event.event_id = generate_event_id();
                
                broadcast_to_chat(expired_it->chat_id, event);
            }
            
            indicators.erase(removed, indicators.end());
        }
        
        if (indicators.empty()) {
            it = typing_indicators_.erase(it);
        } else {
            ++it;
        }
    }
}

std::shared_ptr<ClientConnection> WebSocketManager::get_connection(const std::string& connection_id) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto it = connections_.find(connection_id);
    return (it != connections_.end()) ? it->second : nullptr;
}

std::string WebSocketManager::generate_connection_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    
    uint64_t high = dis(gen);
    uint64_t low = dis(gen);
    
    std::stringstream ss;
    ss << "conn_" << std::hex << high << low;
    return ss.str();
}

std::string WebSocketManager::generate_event_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    
    uint64_t high = dis(gen);
    uint64_t low = dis(gen);
    
    std::stringstream ss;
    ss << "evt_" << std::hex << high << low;
    return ss.str();
}

void WebSocketManager::process_event_queue() {
    while (running_) {
        std::unique_lock<std::mutex> lock(event_queue_mutex_);
        
        if (event_queue_.empty()) {
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        
        auto event = event_queue_.front();
        event_queue_.pop();
        lock.unlock();
        
        // Process the event
        if (!event.chat_id.empty()) {
            broadcast_to_chat(event.chat_id, event);
        } else if (!event.target_user_id.empty()) {
            broadcast_to_user(event.target_user_id, event);
        }
    }
}

void WebSocketManager::broadcast_to_user(const std::string& user_id, const RealtimeEvent& event) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto user_it = user_connections_.find(user_id);
    if (user_it == user_connections_.end()) {
        return;
    }
    
    Json::Value event_json = event.to_json();
    
    for (const auto& connection_id : user_it->second) {
        send_to_connection(connection_id, event_json);
    }
}

ConnectionMetrics WebSocketManager::get_metrics() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    return metrics_;
}

void WebSocketManager::set_authentication_callback(
    std::function<bool(const std::string&, const std::string&)> callback) {
    auth_callback_ = callback;
}

} // namespace sonet::messaging::realtime
