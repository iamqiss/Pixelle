/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "note_websocket_handler.h"
#include <spdlog/spdlog.h>

namespace sonet::note::websocket {

NoteWebSocketHandler::NoteWebSocketHandler() {
    spdlog::info("NoteWebSocketHandler initialized");
    
    // Initialize Redis connection pool
    initialize_redis_pool();
    
    // Start background cleanup task
    start_cleanup_task();
}

NoteWebSocketHandler::~NoteWebSocketHandler() {
    stop_cleanup_task();
    cleanup_redis_pool();
}

bool NoteWebSocketHandler::handle_connection(websocketpp::connection_hdl hdl, const std::string& user_id) {
    try {
        std::lock_guard<std::shared_mutex> lock(connections_mutex_);
        
        ConnectionInfo info;
        info.user_id = user_id;
        info.connection_hdl = hdl;
        info.connected_at = std::chrono::steady_clock::now();
        info.last_activity = info.connected_at;
        info.subscriptions.insert("timeline"); // Default subscription
        
        connections_[hdl] = info;
        user_connections_[user_id].insert(hdl);
        
        // Send welcome message
        nlohmann::json welcome_msg = {
            {"type", "connection_established"},
            {"user_id", user_id},
            {"server_time", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()}
        };
        
        send_message(hdl, welcome_msg);
        
        spdlog::info("WebSocket connection established for user: {}", user_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Error handling WebSocket connection: {}", e.what());
        return false;
    }
}

void NoteWebSocketHandler::handle_disconnection(websocketpp::connection_hdl hdl) {
    try {
        std::lock_guard<std::shared_mutex> lock(connections_mutex_);
        
        auto it = connections_.find(hdl);
        if (it != connections_.end()) {
            const std::string& user_id = it->second.user_id;
            
            // Remove from user connections
            auto user_it = user_connections_.find(user_id);
            if (user_it != user_connections_.end()) {
                user_it->second.erase(hdl);
                if (user_it->second.empty()) {
                    user_connections_.erase(user_it);
                }
            }
            
            // Remove from connections
            connections_.erase(it);
            
            spdlog::info("WebSocket connection closed for user: {}", user_id);
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error handling WebSocket disconnection: {}", e.what());
    }
}

void NoteWebSocketHandler::handle_message(websocketpp::connection_hdl hdl, const std::string& message) {
    try {
        auto json_msg = nlohmann::json::parse(message);
        std::string msg_type = json_msg.value("type", "");
        
        // Update last activity
        {
            std::shared_lock<std::shared_mutex> lock(connections_mutex_);
            auto it = connections_.find(hdl);
            if (it != connections_.end()) {
                it->second.last_activity = std::chrono::steady_clock::now();
            }
        }
        
        if (msg_type == "subscribe") {
            handle_subscribe_request(hdl, json_msg);
        } else if (msg_type == "unsubscribe") {
            handle_unsubscribe_request(hdl, json_msg);
        } else if (msg_type == "ping") {
            handle_ping_request(hdl, json_msg);
        } else if (msg_type == "typing_start") {
            handle_typing_indicator(hdl, json_msg, true);
        } else if (msg_type == "typing_stop") {
            handle_typing_indicator(hdl, json_msg, false);
        } else {
            spdlog::warn("Unknown WebSocket message type: {}", msg_type);
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error handling WebSocket message: {}", e.what());
        
        nlohmann::json error_msg = {
            {"type", "error"},
            {"message", "Invalid message format"}
        };
        send_message(hdl, error_msg);
    }
}

void NoteWebSocketHandler::broadcast_note_created(const nlohmann::json& note_data) {
    try {
        nlohmann::json broadcast_msg = {
            {"type", "note_created"},
            {"note", note_data},
            {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()}
        };
        
        // Broadcast to timeline subscribers
        broadcast_to_subscribers("timeline", broadcast_msg);
        
        // Broadcast to author's followers
        std::string author_id = note_data.value("author_id", "");
        if (!author_id.empty()) {
            broadcast_to_followers(author_id, broadcast_msg);
        }
        
        spdlog::debug("Broadcasted note creation: {}", note_data.value("note_id", ""));
        
    } catch (const std::exception& e) {
        spdlog::error("Error broadcasting note creation: {}", e.what());
    }
}

void NoteWebSocketHandler::broadcast_note_updated(const nlohmann::json& note_data) {
    try {
        nlohmann::json broadcast_msg = {
            {"type", "note_updated"},
            {"note", note_data},
            {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()}
        };
        
        broadcast_to_subscribers("timeline", broadcast_msg);
        
    } catch (const std::exception& e) {
        spdlog::error("Error broadcasting note update: {}", e.what());
    }
}

void NoteWebSocketHandler::broadcast_engagement_update(const nlohmann::json& engagement_data) {
    try {
        nlohmann::json broadcast_msg = {
            {"type", "engagement_update"},
            {"data", engagement_data},
            {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()}
        };
        
        // Broadcast to engagement subscribers
        broadcast_to_subscribers("engagement", broadcast_msg);
        
    } catch (const std::exception& e) {
        spdlog::error("Error broadcasting engagement update: {}", e.what());
    }
}

// Private implementation methods (stubs)

void NoteWebSocketHandler::initialize_redis_pool() {
    // Placeholder for Redis connection pool initialization
    spdlog::info("Redis connection pool initialized (placeholder)");
}

void NoteWebSocketHandler::cleanup_redis_pool() {
    // Placeholder for Redis cleanup
    spdlog::info("Redis connection pool cleaned up (placeholder)");
}

void NoteWebSocketHandler::start_cleanup_task() {
    cleanup_running_ = true;
    cleanup_thread_ = std::thread([this] {
        while (cleanup_running_) {
            cleanup_stale_connections();
            std::this_thread::sleep_for(std::chrono::minutes(1));
        }
    });
}

void NoteWebSocketHandler::stop_cleanup_task() {
    cleanup_running_ = false;
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

void NoteWebSocketHandler::cleanup_stale_connections() {
    try {
        std::lock_guard<std::shared_mutex> lock(connections_mutex_);
        
        auto now = std::chrono::steady_clock::now();
        std::vector<websocketpp::connection_hdl> stale_connections;
        
        for (const auto& [hdl, info] : connections_) {
            auto idle_time = std::chrono::duration_cast<std::chrono::minutes>(
                now - info.last_activity);
            
            if (idle_time.count() > 30) { // 30 minutes idle
                stale_connections.push_back(hdl);
            }
        }
        
        for (const auto& hdl : stale_connections) {
            handle_disconnection(hdl);
        }
        
        if (!stale_connections.empty()) {
            spdlog::info("Cleaned up {} stale WebSocket connections", stale_connections.size());
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error during connection cleanup: {}", e.what());
    }
}

void NoteWebSocketHandler::send_message(websocketpp::connection_hdl hdl, const nlohmann::json& message) {
    try {
        // Placeholder implementation - would use actual WebSocket server
        spdlog::debug("Sending WebSocket message: {}", message.dump());
        
    } catch (const std::exception& e) {
        spdlog::error("Error sending WebSocket message: {}", e.what());
    }
}

void NoteWebSocketHandler::broadcast_to_subscribers(const std::string& channel, const nlohmann::json& message) {
    try {
        std::shared_lock<std::shared_mutex> lock(connections_mutex_);
        
        for (const auto& [hdl, info] : connections_) {
            if (info.subscriptions.count(channel) > 0) {
                send_message(hdl, message);
            }
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error broadcasting to subscribers: {}", e.what());
    }
}

void NoteWebSocketHandler::broadcast_to_followers(const std::string& user_id, const nlohmann::json& message) {
    // Placeholder implementation - would fetch follower list and broadcast
    spdlog::debug("Broadcasting to followers of user: {}", user_id);
}

void NoteWebSocketHandler::handle_subscribe_request(websocketpp::connection_hdl hdl, const nlohmann::json& request) {
    try {
        std::string channel = request.value("channel", "");
        if (channel.empty()) return;
        
        std::lock_guard<std::shared_mutex> lock(connections_mutex_);
        auto it = connections_.find(hdl);
        if (it != connections_.end()) {
            it->second.subscriptions.insert(channel);
            
            nlohmann::json response = {
                {"type", "subscription_confirmed"},
                {"channel", channel}
            };
            send_message(hdl, response);
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error handling subscribe request: {}", e.what());
    }
}

void NoteWebSocketHandler::handle_unsubscribe_request(websocketpp::connection_hdl hdl, const nlohmann::json& request) {
    try {
        std::string channel = request.value("channel", "");
        if (channel.empty()) return;
        
        std::lock_guard<std::shared_mutex> lock(connections_mutex_);
        auto it = connections_.find(hdl);
        if (it != connections_.end()) {
            it->second.subscriptions.erase(channel);
            
            nlohmann::json response = {
                {"type", "unsubscription_confirmed"},
                {"channel", channel}
            };
            send_message(hdl, response);
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error handling unsubscribe request: {}", e.what());
    }
}

void NoteWebSocketHandler::handle_ping_request(websocketpp::connection_hdl hdl, const nlohmann::json& request) {
    nlohmann::json pong_response = {
        {"type", "pong"},
        {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count()}
    };
    send_message(hdl, pong_response);
}

void NoteWebSocketHandler::handle_typing_indicator(websocketpp::connection_hdl hdl, const nlohmann::json& request, bool is_typing) {
    try {
        std::string target_user_id = request.value("target_user_id", "");
        if (target_user_id.empty()) return;
        
        std::shared_lock<std::shared_mutex> lock(connections_mutex_);
        auto conn_it = connections_.find(hdl);
        if (conn_it == connections_.end()) return;
        
        const std::string& user_id = conn_it->second.user_id;
        
        nlohmann::json typing_msg = {
            {"type", is_typing ? "user_typing" : "user_stopped_typing"},
            {"user_id", user_id},
            {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()}
        };
        
        // Send to target user's connections
        auto target_it = user_connections_.find(target_user_id);
        if (target_it != user_connections_.end()) {
            for (const auto& target_hdl : target_it->second) {
                send_message(target_hdl, typing_msg);
            }
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error handling typing indicator: {}", e.what());
    }
}

} // namespace sonet::note::websocket
