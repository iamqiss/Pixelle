//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "implementations.h"
#include <iostream>
#include <sstream>
#include <thread>
#include <random>
#include <algorithm>

namespace sonet::timeline {

namespace {
    // Helper to convert system_clock::time_point to protobuf timestamp
    ::sonet::common::Timestamp ToProtoTimestamp(std::chrono::system_clock::time_point tp) {
        ::sonet::common::Timestamp result;
        auto duration = tp.time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);
        
        result.set_seconds(seconds.count());
        result.set_nanos(static_cast<int32_t>(nanos.count()));
        return result;
    }

    // Generate connection ID
    std::string GenerateConnectionId() {
        static std::random_device rd;
        static std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        std::ostringstream oss;
        oss << "conn_" << std::hex << dis(gen);
        return oss.str();
    }

    // Escape string for JSON
    std::string JsonEscape(const std::string& s) {
        std::string out;
        out.reserve(s.size() + 16);
        for (unsigned char c : s) {
            switch (c) {
                case '"': out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\b': out += "\\b"; break;
                case '\f': out += "\\f"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                default:
                    if (c < 0x20) {
                        char buf[7];
                        std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                        out += buf;
                    } else {
                        out.push_back(static_cast<char>(c));
                    }
            }
        }
        return out;
    }

    // Simple JSON message formatting
    std::string FormatJsonMessage(const std::string& type, const std::string& data) {
        std::ostringstream oss;
        oss << "{"
            << "\"type\":\"" << JsonEscape(type) << "\"," 
            << "\"timestamp\":" << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch()).count() << ","
            << "\"data\":" << data
            << "}";
        return oss.str();
    }
}

// ============= WEBSOCKET REALTIME NOTIFIER IMPLEMENTATION =============

WebSocketRealtimeNotifier::WebSocketRealtimeNotifier(int port) : port_(port) {
    std::cout << "WebSocket Realtime Notifier initialized on port " << port_ << std::endl;
}

WebSocketRealtimeNotifier::~WebSocketRealtimeNotifier() {
    Stop();
}

void WebSocketRealtimeNotifier::NotifyNewItems(
    const std::string& user_id,
    const std::vector<RankedTimelineItem>& items
) {
    if (items.empty()) return;
    
    std::ostringstream data_stream;
    data_stream << "{\"user_id\":\"" << JsonEscape(user_id) << "\",\"new_items\":[";
    
    for (size_t i = 0; i < items.size(); ++i) {
        if (i > 0) data_stream << ",";
        
        const auto& item = items[i];
        data_stream << "{"
                   << "\"note_id\":\"" << JsonEscape(item.note.id()) << "\"," 
                   << "\"author_id\":\"" << JsonEscape(item.note.author_id()) << "\"," 
                   << "\"content\":\"" << JsonEscape(item.note.content()) << "\"," 
                   << "\"final_score\":" << item.final_score
                   << "}";
    }
    
    data_stream << "]}";
    
    std::string message = FormatJsonMessage("new_items", data_stream.str());
    SendToUser(user_id, message);
    
    std::cout << "Notified user " << user_id << " of " << items.size() << " new timeline items" << std::endl;
}

void WebSocketRealtimeNotifier::NotifyItemUpdate(
    const std::string& user_id,
    const std::string& item_id,
    const ::sonet::timeline::TimelineUpdate& update
) {
    std::ostringstream data_stream;
    data_stream << "{"
               << "\"user_id\":\"" << JsonEscape(user_id) << "\"," 
               << "\"item_id\":\"" << JsonEscape(item_id) << "\"," 
               << "\"update_type\":\"" << static_cast<int>(update.update_type()) << "\"" 
               << "}";
    
    std::string message = FormatJsonMessage("item_update", data_stream.str());
    SendToUser(user_id, message);
    
    std::cout << "Notified user " << user_id << " of item update: " << item_id << std::endl;
}

void WebSocketRealtimeNotifier::NotifyItemDeleted(
    const std::string& user_id,
    const std::string& item_id
) {
    std::ostringstream data_stream;
    data_stream << "{"
               << "\"user_id\":\"" << JsonEscape(user_id) << "\"," 
               << "\"item_id\":\"" << JsonEscape(item_id) << "\"" 
               << "}";
    
    std::string message = FormatJsonMessage("item_deleted", data_stream.str());
    
    if (user_id == "*") {
        BroadcastToAll(message);
        std::cout << "Broadcast item deletion: " << item_id << std::endl;
    } else {
        SendToUser(user_id, message);
        std::cout << "Notified user " << user_id << " of item deletion: " << item_id << std::endl;
    }
}

void WebSocketRealtimeNotifier::Subscribe(const std::string& user_id, const std::string& connection_id) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    Connection conn;
    conn.connection_id = connection_id;
    conn.user_id = user_id;
    conn.last_activity = std::chrono::system_clock::now();
    conn.is_active = true;
    
    connections_[connection_id] = conn;
    user_connections_[user_id].push_back(connection_id);
    
    std::cout << "User " << user_id << " subscribed with connection " << connection_id << std::endl;
}

void WebSocketRealtimeNotifier::Unsubscribe(const std::string& user_id, const std::string& connection_id) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    // Remove from connections
    connections_.erase(connection_id);
    
    // Remove from user connections
    auto user_it = user_connections_.find(user_id);
    if (user_it != user_connections_.end()) {
        auto& user_conns = user_it->second;
        user_conns.erase(
            std::remove(user_conns.begin(), user_conns.end(), connection_id),
            user_conns.end()
        );
        
        if (user_conns.empty()) {
            user_connections_.erase(user_it);
        }
    }
    
    std::cout << "User " << user_id << " unsubscribed connection " << connection_id << std::endl;
}

void WebSocketRealtimeNotifier::Start() {
    if (running_) return;
    
    running_ = true;
    
    // In a real implementation, this would start a WebSocket server
    // For now, just simulate the server running
    server_thread_ = std::thread([this]() {
        std::cout << "WebSocket server started on port " << port_ << std::endl;
        
        while (running_) {
            // Simulate server activity and clean up stale connections
            std::this_thread::sleep_for(std::chrono::seconds(30));
            
            // Clean up inactive connections
            std::lock_guard<std::mutex> lock(connections_mutex_);
            auto now = std::chrono::system_clock::now();
            
            for (auto it = connections_.begin(); it != connections_.end();) {
                auto age = std::chrono::duration_cast<std::chrono::minutes>(now - it->second.last_activity);
                if (age.count() > 30) { // 30 minutes timeout
                    std::string user_id = it->second.user_id;
                    std::string conn_id = it->first;
                    
                    it = connections_.erase(it);
                    
                    // Remove from user connections
                    auto user_it = user_connections_.find(user_id);
                    if (user_it != user_connections_.end()) {
                        auto& user_conns = user_it->second;
                        user_conns.erase(
                            std::remove(user_conns.begin(), user_conns.end(), conn_id),
                            user_conns.end()
                        );
                        
                        if (user_conns.empty()) {
                            user_connections_.erase(user_it);
                        }
                    }
                    
                    std::cout << "Cleaned up stale connection: " << conn_id << " for user " << user_id << std::endl;
                } else {
                    ++it;
                }
            }
        }
        
        std::cout << "WebSocket server stopped" << std::endl;
    });
}

void WebSocketRealtimeNotifier::Stop() {
    if (!running_) return;
    
    running_ = false;
    
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_.clear();
    user_connections_.clear();
    
    std::cout << "WebSocket server stopped and connections cleared" << std::endl;
}

void WebSocketRealtimeNotifier::SendToUser(const std::string& user_id, const std::string& message) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto user_it = user_connections_.find(user_id);
    if (user_it != user_connections_.end()) {
        int sent_count = 0;
        
        for (const auto& conn_id : user_it->second) {
            auto conn_it = connections_.find(conn_id);
            if (conn_it != connections_.end() && conn_it->second.is_active) {
                // In real implementation, this would send via WebSocket
                // For now, just log the message
                std::cout << "SEND [" << conn_id << "]: " << message << std::endl;
                
                // Update last activity
                conn_it->second.last_activity = std::chrono::system_clock::now();
                sent_count++;
            }
        }
        
        if (sent_count == 0) {
            std::cout << "No active connections for user " << user_id << std::endl;
        }
    } else {
        std::cout << "No connections found for user " << user_id << std::endl;
    }
}

void WebSocketRealtimeNotifier::BroadcastToAll(const std::string& message) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    int sent_count = 0;
    
    for (auto& [conn_id, connection] : connections_) {
        if (connection.is_active) {
            // In real implementation, this would send via WebSocket
            std::cout << "BROADCAST [" << conn_id << "]: " << message << std::endl;
            
            connection.last_activity = std::chrono::system_clock::now();
            sent_count++;
        }
    }
    
    std::cout << "Broadcast message sent to " << sent_count << " connections" << std::endl;
}

} // namespace sonet::timeline
