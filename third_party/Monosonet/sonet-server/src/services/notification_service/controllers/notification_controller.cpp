/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This implements the notification controller with WebSocket support for real-time
 * notifications. I focused on making this fast and reliable for mobile users
 * who expect instant notifications about their notes and interactions.
 */

#include "notification_controller.h"
#include <jwt-cpp/jwt.h>
#include <thread>
#include <queue>
#include <condition_variable>
#include <regex>
#include <sstream>
#include <random>
#include <iomanip>
#include <uuid/uuid.h>

namespace sonet {
namespace notification_service {
namespace controllers {

// WebSocketConnectionManager Implementation

WebSocketConnectionManager::WebSocketConnectionManager() = default;

WebSocketConnectionManager::~WebSocketConnectionManager() {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_.clear();
    user_connections_.clear();
}

void WebSocketConnectionManager::add_connection(connection_hdl hdl, const std::string& user_id, 
                                               const std::string& session_id, 
                                               const nlohmann::json& client_info) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto connection = std::make_unique<UserConnection>();
    connection->hdl = hdl;
    connection->user_id = user_id;
    connection->session_id = session_id;
    connection->connected_at = std::chrono::system_clock::now();
    connection->last_ping = connection->connected_at;
    connection->is_active = true;
    connection->client_info = client_info;
    
    connections_[hdl] = std::move(connection);
    user_connections_[user_id].push_back(hdl);
    
    total_connections_++;
    active_connections_++;
}

void WebSocketConnectionManager::remove_connection(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto it = connections_.find(hdl);
    if (it != connections_.end()) {
        std::string user_id = it->second->user_id;
        connections_.erase(it);
        
        // Remove from user connections
        auto user_it = user_connections_.find(user_id);
        if (user_it != user_connections_.end()) {
            auto& user_conns = user_it->second;
            user_conns.erase(
                std::remove_if(user_conns.begin(), user_conns.end(),
                    [&hdl](const connection_hdl& conn_hdl) {
                        return !hdl.owner_before(conn_hdl) && !conn_hdl.owner_before(hdl);
                    }),
                user_conns.end()
            );
            
            if (user_conns.empty()) {
                user_connections_.erase(user_it);
            }
        }
        
        active_connections_--;
    }
}

void WebSocketConnectionManager::remove_user_connections(const std::string& user_id) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    cleanup_user_connections(user_id);
}

bool WebSocketConnectionManager::send_to_user(const std::string& user_id, 
                                              const nlohmann::json& message) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto user_it = user_connections_.find(user_id);
    if (user_it == user_connections_.end()) {
        return false;
    }
    
    bool sent_any = false;
    for (const auto& hdl : user_it->second) {
        auto conn_it = connections_.find(hdl);
        if (conn_it != connections_.end() && conn_it->second->is_active) {
            if (send_to_connection(hdl, message)) {
                sent_any = true;
            }
        }
    }
    
    return sent_any;
}

bool WebSocketConnectionManager::send_to_connection(connection_hdl hdl, 
                                                   const nlohmann::json& message) {
    if (!server_) {
        return false;
    }
    
    try {
        auto con = server_->get_con_from_hdl(hdl);
        if (con) {
            server_->send(hdl, message.dump(), websocketpp::frame::opcode::text);
            return true;
        }
    } catch (const std::exception& e) {
        // Connection might be closed, mark as inactive
        auto it = connections_.find(hdl);
        if (it != connections_.end()) {
            it->second->is_active = false;
        }
    }
    
    return false;
}

void WebSocketConnectionManager::broadcast_to_users(const std::vector<std::string>& user_ids, 
                                                   const nlohmann::json& message) {
    for (const auto& user_id : user_ids) {
        send_to_user(user_id, message);
    }
}

std::vector<WebSocketConnectionManager::UserConnection> 
WebSocketConnectionManager::get_user_connections(const std::string& user_id) const {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    std::vector<UserConnection> result;
    auto user_it = user_connections_.find(user_id);
    if (user_it != user_connections_.end()) {
        for (const auto& hdl : user_it->second) {
            auto conn_it = connections_.find(hdl);
            if (conn_it != connections_.end() && conn_it->second->is_active) {
                result.push_back(*conn_it->second);
            }
        }
    }
    
    return result;
}

int WebSocketConnectionManager::get_active_connection_count() const {
    return active_connections_.load();
}

int WebSocketConnectionManager::get_user_connection_count(const std::string& user_id) const {
    return get_user_connections(user_id).size();
}

bool WebSocketConnectionManager::is_user_online(const std::string& user_id) const {
    return get_user_connection_count(user_id) > 0;
}

void WebSocketConnectionManager::ping_connections() {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto now = std::chrono::system_clock::now();
    
    for (auto& [hdl, connection] : connections_) {
        if (connection->is_active) {
            try {
                if (server_) {
                    server_->ping(hdl, "ping");
                    connection->last_ping = now;
                }
            } catch (const std::exception& e) {
                connection->is_active = false;
            }
        }
    }
}

void WebSocketConnectionManager::cleanup_stale_connections() {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto now = std::chrono::system_clock::now();
    auto timeout = std::chrono::seconds{60}; // 1 minute timeout
    
    std::vector<connection_hdl> to_remove;
    
    for (auto& [hdl, connection] : connections_) {
        if (!connection->is_active || 
            (now - connection->last_ping) > timeout) {
            to_remove.push_back(hdl);
        }
    }
    
    for (const auto& hdl : to_remove) {
        auto it = connections_.find(hdl);
        if (it != connections_.end()) {
            std::string user_id = it->second->user_id;
            connections_.erase(it);
            cleanup_user_connections(user_id);
            active_connections_--;
        }
    }
}

nlohmann::json WebSocketConnectionManager::get_connection_stats() const {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    nlohmann::json stats;
    stats["total_connections"] = total_connections_.load();
    stats["active_connections"] = active_connections_.load();
    stats["unique_users"] = user_connections_.size();
    
    std::unordered_map<std::string, int> connections_per_user;
    for (const auto& [user_id, conns] : user_connections_) {
        connections_per_user[user_id] = conns.size();
    }
    stats["connections_per_user"] = connections_per_user;
    
    return stats;
}

void WebSocketConnectionManager::cleanup_user_connections(const std::string& user_id) {
    auto user_it = user_connections_.find(user_id);
    if (user_it != user_connections_.end()) {
        // Remove all connections for this user
        for (const auto& hdl : user_it->second) {
            connections_.erase(hdl);
        }
        user_connections_.erase(user_it);
    }
}

std::string WebSocketConnectionManager::connection_to_string(connection_hdl hdl) const {
    try {
        if (server_) {
            auto con = server_->get_con_from_hdl(hdl);
            if (con) {
                return con->get_remote_endpoint();
            }
        }
    } catch (const std::exception& e) {
        // Ignore
    }
    return "unknown";
}

// NotificationRateLimiter Implementation

NotificationRateLimiter::NotificationRateLimiter(const RateLimit& limits) 
    : limits_(limits) {}

bool NotificationRateLimiter::check_rate_limit(const std::string& user_id, 
                                              const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(rate_data_mutex_);
    
    std::string key = get_rate_key(user_id, endpoint);
    auto& data = rate_data_[key];
    
    cleanup_old_requests(data);
    
    auto now = std::chrono::system_clock::now();
    
    // Check burst limit
    if (data.burst_count >= limits_.burst_limit) {
        auto since_reset = now - data.last_burst_reset;
        if (since_reset < std::chrono::seconds{60}) {
            return false; // Burst limit exceeded
        } else {
            data.burst_count = 0;
            data.last_burst_reset = now;
        }
    }
    
    // Check per-minute limit
    if (data.minute_requests.size() >= limits_.requests_per_minute) {
        return false;
    }
    
    // Check per-hour limit
    if (data.hour_requests.size() >= limits_.requests_per_hour) {
        return false;
    }
    
    // Record this request
    data.minute_requests.push_back(now);
    data.hour_requests.push_back(now);
    data.burst_count++;
    
    return true;
}

void NotificationRateLimiter::reset_rate_limit(const std::string& user_id, 
                                              const std::string& endpoint) {
    std::lock_guard<std::mutex> lock(rate_data_mutex_);
    
    if (endpoint.empty()) {
        // Reset all endpoints for user
        auto it = rate_data_.begin();
        while (it != rate_data_.end()) {
            if (it->first.find(user_id + ":") == 0) {
                it = rate_data_.erase(it);
            } else {
                ++it;
            }
        }
    } else {
        std::string key = get_rate_key(user_id, endpoint);
        rate_data_.erase(key);
    }
}

nlohmann::json NotificationRateLimiter::get_rate_limit_status(const std::string& user_id, 
                                                             const std::string& endpoint) const {
    std::lock_guard<std::mutex> lock(rate_data_mutex_);
    
    std::string key = get_rate_key(user_id, endpoint);
    auto it = rate_data_.find(key);
    
    nlohmann::json status;
    status["user_id"] = user_id;
    status["endpoint"] = endpoint;
    status["limits"]["requests_per_minute"] = limits_.requests_per_minute;
    status["limits"]["requests_per_hour"] = limits_.requests_per_hour;
    status["limits"]["burst_limit"] = limits_.burst_limit;
    
    if (it != rate_data_.end()) {
        const auto& data = it->second;
        status["current"]["minute_requests"] = data.minute_requests.size();
        status["current"]["hour_requests"] = data.hour_requests.size();
        status["current"]["burst_count"] = data.burst_count;
        
        // Calculate remaining
        status["remaining"]["minute_requests"] = 
            std::max(0, limits_.requests_per_minute - static_cast<int>(data.minute_requests.size()));
        status["remaining"]["hour_requests"] = 
            std::max(0, limits_.requests_per_hour - static_cast<int>(data.hour_requests.size()));
        status["remaining"]["burst_requests"] = 
            std::max(0, limits_.burst_limit - data.burst_count);
    } else {
        status["current"]["minute_requests"] = 0;
        status["current"]["hour_requests"] = 0;
        status["current"]["burst_count"] = 0;
        status["remaining"]["minute_requests"] = limits_.requests_per_minute;
        status["remaining"]["hour_requests"] = limits_.requests_per_hour;
        status["remaining"]["burst_requests"] = limits_.burst_limit;
    }
    
    return status;
}

void NotificationRateLimiter::cleanup_old_requests(RateData& data) const {
    auto now = std::chrono::system_clock::now();
    auto minute_ago = now - std::chrono::minutes{1};
    auto hour_ago = now - std::chrono::hours{1};
    
    // Remove minute requests older than 1 minute
    data.minute_requests.erase(
        std::remove_if(data.minute_requests.begin(), data.minute_requests.end(),
            [minute_ago](const auto& time) { return time < minute_ago; }),
        data.minute_requests.end()
    );
    
    // Remove hour requests older than 1 hour
    data.hour_requests.erase(
        std::remove_if(data.hour_requests.begin(), data.hour_requests.end(),
            [hour_ago](const auto& time) { return time < hour_ago; }),
        data.hour_requests.end()
    );
}

std::string NotificationRateLimiter::get_rate_key(const std::string& user_id, 
                                                 const std::string& endpoint) const {
    return user_id + ":" + endpoint;
}

// NotificationController Implementation Details

struct NotificationController::Impl {
    Config config;
    std::shared_ptr<repositories::NotificationRepository> repository;
    std::unique_ptr<WebSocketConnectionManager> ws_manager;
    std::unique_ptr<NotificationRateLimiter> rate_limiter;
    
    // Server instances
    std::shared_ptr<WebSocketConnectionManager::server> ws_server;
    
    // Background processing
    std::atomic<bool> is_running{false};
    std::vector<std::thread> background_threads;
    std::mutex background_mutex;
    std::condition_variable background_cv;
    
    // Performance tracking
    struct PerformanceMetrics {
        std::atomic<int> total_requests{0};
        std::atomic<int> successful_requests{0};
        std::atomic<int> failed_requests{0};
        std::atomic<int> notifications_created{0};
        std::atomic<int> notifications_delivered{0};
        std::atomic<int> websocket_messages{0};
        std::chrono::system_clock::time_point start_time;
        std::mutex duration_mutex;
        std::vector<std::chrono::microseconds> request_durations;
    } metrics;
    
    // Event callbacks
    NotificationController::NotificationCreatedCallback notification_created_cb;
    NotificationController::NotificationDeliveredCallback notification_delivered_cb;
    NotificationController::NotificationReadCallback notification_read_cb;
    NotificationController::UserConnectedCallback user_connected_cb;
    NotificationController::UserDisconnectedCallback user_disconnected_cb;
    
    // Cache for frequently accessed data
    mutable std::mutex cache_mutex;
    std::unordered_map<std::string, std::pair<nlohmann::json, std::chrono::system_clock::time_point>> response_cache;
    
    Impl(std::shared_ptr<repositories::NotificationRepository> repo, const Config& cfg)
        : config(cfg), repository(repo) {
        
        ws_manager = std::make_unique<WebSocketConnectionManager>();
        rate_limiter = std::make_unique<NotificationRateLimiter>(config.rate_limits);
        metrics.start_time = std::chrono::system_clock::now();
    }
    
    ~Impl() {
        stop_background_processors();
    }
    
    std::string generate_uuid() const {
        uuid_t uuid;
        uuid_generate_random(uuid);
        char uuid_str[37];
        uuid_unparse(uuid, uuid_str);
        return std::string(uuid_str);
    }
    
    void start_background_processors() {
        if (is_running.load()) return;
        
        std::lock_guard<std::mutex> lock(background_mutex);
        is_running.store(true);
        
        // Start notification processing thread
        background_threads.emplace_back([this]() {
            while (is_running.load()) {
                try {
                    process_pending_notifications();
                    process_scheduled_notifications();
                    cleanup_expired_notifications();
                    
                    std::unique_lock<std::mutex> lock(background_mutex);
                    background_cv.wait_for(lock, config.batch_processing_interval);
                } catch (const std::exception& e) {
                    // Log error and continue
                }
            }
        });
        
        // Start WebSocket maintenance thread
        if (config.enable_websocket) {
            background_threads.emplace_back([this]() {
                while (is_running.load()) {
                    try {
                        ws_manager->ping_connections();
                        ws_manager->cleanup_stale_connections();
                        
                        std::unique_lock<std::mutex> lock(background_mutex);
                        background_cv.wait_for(lock, config.websocket_ping_interval);
                    } catch (const std::exception& e) {
                        // Log error and continue
                    }
                }
            });
        }
        
        // Start cache cleanup thread
        background_threads.emplace_back([this]() {
            while (is_running.load()) {
                try {
                    cleanup_response_cache();
                    
                    std::unique_lock<std::mutex> lock(background_mutex);
                    background_cv.wait_for(lock, std::chrono::minutes{5});
                } catch (const std::exception& e) {
                    // Log error and continue
                }
            }
        });
    }
    
    void stop_background_processors() {
        is_running.store(false);
        background_cv.notify_all();
        
        for (auto& thread : background_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        background_threads.clear();
    }
    
    void process_pending_notifications() {
        auto pending_future = repository->get_pending_notifications(100);
        auto pending_notifications = pending_future.get();
        
        for (const auto& notification : pending_notifications) {
            try {
                // Process based on delivery channels
                bool delivered = false;
                
                if (notification.has_delivery_channel(models::DeliveryChannel::IN_APP)) {
                    if (send_real_time_notification(notification)) {
                        delivered = true;
                    }
                }
                
                if (notification.has_delivery_channel(models::DeliveryChannel::PUSH_NOTIFICATION)) {
                    // Implement push notification sending
                    delivered = true;
                }
                
                if (notification.has_delivery_channel(models::DeliveryChannel::EMAIL)) {
                    // Implement email notification sending
                    delivered = true;
                }
                
                // Update delivery status
                if (delivered) {
                    auto updated_notification = notification;
                    updated_notification.mark_as_delivered();
                    repository->update_notification(updated_notification);
                    
                    metrics.notifications_delivered++;
                    
                    if (notification_delivered_cb) {
                        notification_delivered_cb(notification);
                    }
                }
                
            } catch (const std::exception& e) {
                // Mark as failed
                auto failed_notification = notification;
                failed_notification.mark_as_failed(e.what());
                repository->update_notification(failed_notification);
            }
        }
    }
    
    void process_scheduled_notifications() {
        auto now = std::chrono::system_clock::now();
        auto scheduled_future = repository->get_scheduled_notifications(now, 50);
        auto scheduled_notifications = scheduled_future.get();
        
        for (auto& notification : scheduled_notifications) {
            if (notification.should_send_now()) {
                // Move to pending status for processing
                notification.status = models::DeliveryStatus::PENDING;
                repository->update_notification(notification);
            }
        }
    }
    
    void cleanup_expired_notifications() {
        repository->cleanup_expired_notifications();
    }
    
    bool send_real_time_notification(const models::Notification& notification) {
        if (!ws_manager->is_user_online(notification.user_id)) {
            return false;
        }
        
        nlohmann::json message;
        message["type"] = "notification";
        message["data"] = notification_to_json(notification);
        message["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        return ws_manager->send_to_user(notification.user_id, message);
    }
    
    nlohmann::json notification_to_json(const models::Notification& notification, 
                                       bool include_sensitive = false) const {
        nlohmann::json json = notification.to_json();
        
        if (!include_sensitive) {
            // Remove sensitive fields
            json.erase("tracking_id");
            json.erase("analytics_data");
            json.erase("template_data");
        }
        
        // Add computed fields
        json["age_ms"] = notification.get_age().count();
        json["display_text"] = notification.get_display_text();
        json["is_expired"] = notification.is_expired();
        
        return json;
    }
    
    void cleanup_response_cache() {
        std::lock_guard<std::mutex> lock(cache_mutex);
        
        auto now = std::chrono::system_clock::now();
        auto it = response_cache.begin();
        
        while (it != response_cache.end()) {
            if (now - it->second.second > config.cache_ttl) {
                it = response_cache.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    std::optional<nlohmann::json> get_cached_response(const std::string& key) const {
        if (!config.enable_response_caching) {
            return std::nullopt;
        }
        
        std::lock_guard<std::mutex> lock(cache_mutex);
        auto it = response_cache.find(key);
        
        if (it != response_cache.end()) {
            auto now = std::chrono::system_clock::now();
            if (now - it->second.second <= config.cache_ttl) {
                return it->second.first;
            } else {
                response_cache.erase(it);
            }
        }
        
        return std::nullopt;
    }
    
    void cache_response(const std::string& key, const nlohmann::json& response,
                       std::chrono::seconds ttl) {
        if (!config.enable_response_caching) {
            return;
        }
        
        std::lock_guard<std::mutex> lock(cache_mutex);
        response_cache[key] = {response, std::chrono::system_clock::now()};
    }
};

// NotificationController Implementation

NotificationController::NotificationController(
    std::shared_ptr<repositories::NotificationRepository> repository,
    const Config& config) 
    : pimpl_(std::make_unique<Impl>(repository, config)) {}

NotificationController::~NotificationController() = default;

void NotificationController::start() {
    // WebSocket server is provided by channels::WebSocketChannel; do not start another here
    pimpl_->start_background_processors();
}

void NotificationController::stop() {
    pimpl_->stop_background_processors();
    
    // No local ws_server to stop; rely on external WebSocket channel
    (void)0;
}

bool NotificationController::is_running() const {
    return pimpl_->is_running.load();
}

nlohmann::json NotificationController::create_notification(const nlohmann::json& request, 
                                                          const std::string& user_id) {
    auto start_time = std::chrono::steady_clock::now();
    track_request_start("create_notification", user_id);
    
    try {
        // Rate limiting
        if (!pimpl_->rate_limiter->check_rate_limit(user_id, "create_notification")) {
            return create_error_response("Rate limit exceeded", 429);
        }
        
        // Validate request
        std::vector<std::string> errors;
        if (!validate_notification_request(request, errors)) {
            return create_error_response("Validation failed", 400, errors);
        }
        
        // Convert JSON to notification
        auto notification = json_to_notification(request);
        notification.sender_id = user_id; // Override sender to authenticated user
        
        // Enrich notification with additional data
        enrich_notification(notification);
        
        // Create in repository
        auto create_future = pimpl_->repository->create_notification(notification);
        std::string notification_id = create_future.get();
        
        notification.id = notification_id;
        
        // Track metrics
        pimpl_->metrics.notifications_created++;
        
        // Trigger callback
        if (pimpl_->notification_created_cb) {
            pimpl_->notification_created_cb(notification);
        }
        
        // Send real-time if appropriate
        if (should_send_real_time(notification)) {
            send_real_time_notification(notification);
        }
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time);
        track_request_end("create_notification", user_id, duration, true);
        
        return create_response("success", {
            {"notification_id", notification_id},
            {"notification", notification_to_json(notification)}
        });
        
    } catch (const std::exception& e) {
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time);
        track_request_end("create_notification", user_id, duration, false);
        
        return create_error_response("Failed to create notification: " + std::string(e.what()), 500);
    }
}

nlohmann::json NotificationController::get_user_notifications(const std::string& user_id, 
                                                             const nlohmann::json& query_params) {
    auto start_time = std::chrono::steady_clock::now();
    track_request_start("get_user_notifications", user_id);
    
    try {
        // Check cache first
        std::string cache_key = "user_notifs:" + user_id + ":" + query_params.dump();
        auto cached_response = pimpl_->get_cached_response(cache_key);
        if (cached_response) {
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time);
            track_request_end("get_user_notifications", user_id, duration, true);
            return *cached_response;
        }
        
        // Parse query parameters
        int limit = query_params.value("limit", 50);
        int offset = query_params.value("offset", 0);
        
        limit = std::min(limit, 100); // Cap at 100
        
        // Get notifications from repository
        auto notifications_future = pimpl_->repository->get_user_notifications(user_id, limit, offset);
        auto notifications = notifications_future.get();
        
        // Convert to JSON
        nlohmann::json notification_array = nlohmann::json::array();
        for (const auto& notification : notifications) {
            notification_array.push_back(notification_to_json(notification));
        }
        
        // Get additional stats
        auto unread_future = pimpl_->repository->get_unread_count(user_id);
        int unread_count = unread_future.get();
        
        auto response = create_response("success", {
            {"notifications", notification_array},
            {"unread_count", unread_count},
            {"limit", limit},
            {"offset", offset},
            {"total_returned", notifications.size()}
        });
        
        // Cache the response
        pimpl_->cache_response(cache_key, response);
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time);
        track_request_end("get_user_notifications", user_id, duration, true);
        
        return response;
        
    } catch (const std::exception& e) {
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time);
        track_request_end("get_user_notifications", user_id, duration, false);
        
        return create_error_response("Failed to get notifications: " + std::string(e.what()), 500);
    }
}

nlohmann::json NotificationController::get_unread_count(const std::string& user_id) {
    auto start_time = std::chrono::steady_clock::now();
    track_request_start("get_unread_count", user_id);
    
    try {
        auto unread_future = pimpl_->repository->get_unread_count(user_id);
        int unread_count = unread_future.get();
        
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time);
        track_request_end("get_unread_count", user_id, duration, true);
        
        return create_response("success", {
            {"unread_count", unread_count}
        });
        
    } catch (const std::exception& e) {
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time);
        track_request_end("get_unread_count", user_id, duration, false);
        
        return create_error_response("Failed to get unread count: " + std::string(e.what()), 500);
    }
}

nlohmann::json NotificationController::mark_as_read(const std::string& notification_id, 
                                                   const std::string& user_id) {
    auto start_time = std::chrono::steady_clock::now();
    track_request_start("mark_as_read", user_id);
    
    try {
        // Authorize access
        if (!authorize_notification_access(user_id, notification_id)) {
            return create_error_response("Unauthorized", 403);
        }
        
        auto success_future = pimpl_->repository->mark_notification_as_read(notification_id, user_id);
        bool success = success_future.get();
        
        if (success) {
            // Invalidate user cache
            invalidate_user_cache(user_id);
            
            // Get the updated notification for callback
            auto notification_future = pimpl_->repository->get_notification(notification_id);
            auto notification_opt = notification_future.get();
            
            if (notification_opt && pimpl_->notification_read_cb) {
                pimpl_->notification_read_cb(*notification_opt);
            }
            
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time);
            track_request_end("mark_as_read", user_id, duration, true);
            
            return create_response("success", {
                {"marked_as_read", true}
            });
        } else {
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time);
            track_request_end("mark_as_read", user_id, duration, false);
            
            return create_error_response("Notification not found or already read", 404);
        }
        
    } catch (const std::exception& e) {
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time);
        track_request_end("mark_as_read", user_id, duration, false);
        
        return create_error_response("Failed to mark as read: " + std::string(e.what()), 500);
    }
}

bool NotificationController::send_real_time_notification(const models::Notification& notification) {
    return pimpl_->send_real_time_notification(notification);
}

void NotificationController::handle_websocket_connect(const std::string& user_id, 
                                                     const std::string& session_id,
                                                     WebSocketConnectionManager::connection_hdl hdl,
                                                     const nlohmann::json& client_info) {
    pimpl_->ws_manager->add_connection(hdl, user_id, session_id, client_info);
    
    if (pimpl_->user_connected_cb) {
        pimpl_->user_connected_cb(user_id);
    }
    
    // Send initial unread count
    auto unread_future = pimpl_->repository->get_unread_count(user_id);
    int unread_count = unread_future.get();
    
    nlohmann::json welcome_message;
    welcome_message["type"] = "welcome";
    welcome_message["data"]["unread_count"] = unread_count;
    welcome_message["data"]["session_id"] = session_id;
    
    pimpl_->ws_manager->send_to_connection(hdl, welcome_message);
}

void NotificationController::handle_websocket_disconnect(WebSocketConnectionManager::connection_hdl hdl) {
    auto connections = pimpl_->ws_manager->get_user_connections("");
    
    // Find the user for this connection before removing
    std::string user_id;
    for (const auto& conn : connections) {
        if (!conn.hdl.owner_before(hdl) && !hdl.owner_before(conn.hdl)) {
            user_id = conn.user_id;
            break;
        }
    }
    
    pimpl_->ws_manager->remove_connection(hdl);
    
    if (!user_id.empty() && pimpl_->user_disconnected_cb) {
        pimpl_->user_disconnected_cb(user_id);
    }
}

// Utility method implementations

nlohmann::json NotificationController::notification_to_json(const models::Notification& notification, 
                                                           bool include_sensitive) {
    return pimpl_->notification_to_json(notification, include_sensitive);
}

models::Notification NotificationController::json_to_notification(const nlohmann::json& json) {
    models::Notification notification;
    notification.from_json(json);
    return notification;
}

nlohmann::json NotificationController::create_response(const std::string& status, 
                                                      const nlohmann::json& data, 
                                                      const std::string& message) {
    nlohmann::json response;
    response["status"] = status;
    response["data"] = data;
    response["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    if (!message.empty()) {
        response["message"] = message;
    }
    
    return response;
}

nlohmann::json NotificationController::create_error_response(const std::string& error, 
                                                            int status_code, 
                                                            const std::vector<std::string>& details) {
    nlohmann::json response;
    response["status"] = "error";
    response["error"] = error;
    response["status_code"] = status_code;
    response["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    if (!details.empty()) {
        response["details"] = details;
    }
    
    return response;
}

bool NotificationController::validate_notification_request(const nlohmann::json& request, 
                                                          std::vector<std::string>& errors) {
    errors.clear();
    
    if (!request.contains("user_id") || !request["user_id"].is_string()) {
        errors.push_back("user_id is required and must be a string");
    }
    
    if (!request.contains("title") || !request["title"].is_string()) {
        errors.push_back("title is required and must be a string");
    }
    
    if (!request.contains("message") || !request["message"].is_string()) {
        errors.push_back("message is required and must be a string");
    }
    
    if (request.contains("type")) {
        std::string type_str = request["type"];
        try {
            models::string_to_notification_type(type_str);
        } catch (const std::exception& e) {
            errors.push_back("invalid notification type: " + type_str);
        }
    }
    
    return errors.empty();
}

bool NotificationController::authenticate_request(const std::string& token, std::string& user_id) {
    if (!pimpl_->config.require_authentication) {
        user_id = "anonymous";
        return true;
    }
    
    try {
        auto decoded_token = jwt::decode(token);
        auto verifier = jwt::verify()
            .allow_algorithm(jwt::algorithm::hs256{pimpl_->config.jwt_secret})
            .with_issuer("sonet")
            .leeway(5);
        
        verifier.verify(decoded_token);
        
        user_id = decoded_token.get_payload_claim("user_id").as_string();
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool NotificationController::authorize_notification_access(const std::string& user_id, 
                                                          const std::string& notification_id) {
    try {
        auto notification_future = pimpl_->repository->get_notification(notification_id);
        auto notification_opt = notification_future.get();
        
        if (!notification_opt) {
            return false;
        }
        
        return notification_opt->user_id == user_id;
        
    } catch (const std::exception& e) {
        return false;
    }
}

void NotificationController::enrich_notification(models::Notification& notification) {
    if (notification.id.empty()) {
        notification.id = pimpl_->generate_uuid();
    }
    
    if (notification.tracking_id.empty()) {
        notification.tracking_id = "track_" + pimpl_->generate_uuid();
    }
    
    // Set default expiration if not set
    if (notification.expires_at <= notification.created_at) {
        notification.expires_at = notification.created_at + std::chrono::hours(24 * 30); // 30 days
    }
}

bool NotificationController::should_send_real_time(const models::Notification& notification) {
    return notification.has_delivery_channel(models::DeliveryChannel::IN_APP) &&
           notification.priority >= models::NotificationPriority::NORMAL;
}

void NotificationController::track_request_start(const std::string& endpoint, const std::string& user_id) {
    pimpl_->metrics.total_requests++;
}

void NotificationController::track_request_end(const std::string& endpoint, const std::string& user_id, 
                                              std::chrono::microseconds duration, bool success) {
    if (success) {
        pimpl_->metrics.successful_requests++;
    } else {
        pimpl_->metrics.failed_requests++;
    }
    
    if (pimpl_->config.enable_metrics_collection) {
        std::lock_guard<std::mutex> lock(pimpl_->metrics.duration_mutex);
        pimpl_->metrics.request_durations.push_back(duration);
        
        // Keep only last 1000 durations for memory efficiency
        if (pimpl_->metrics.request_durations.size() > 1000) {
            pimpl_->metrics.request_durations.erase(
                pimpl_->metrics.request_durations.begin(),
                pimpl_->metrics.request_durations.begin() + 500
            );
        }
    }
}

void NotificationController::invalidate_user_cache(const std::string& user_id) {
    pimpl_->repository->invalidate_user_cache(user_id);
    
    // Also invalidate response cache
    std::lock_guard<std::mutex> lock(pimpl_->cache_mutex);
    auto it = pimpl_->response_cache.begin();
    while (it != pimpl_->response_cache.end()) {
        if (it->first.find("user_notifs:" + user_id) == 0) {
            it = pimpl_->response_cache.erase(it);
        } else {
            ++it;
        }
    }
}

void NotificationController::initialize_websocket_server() {
    // WebSocket server is managed by channels::WebSocketChannel; no-op here.
}

// Factory Implementation

std::unique_ptr<NotificationController> NotificationControllerFactory::create(
    std::shared_ptr<repositories::NotificationRepository> repository,
    const NotificationController::Config& config) {
    return std::make_unique<NotificationController>(repository, config);
}

std::unique_ptr<NotificationController> NotificationControllerFactory::create_from_config(
    std::shared_ptr<repositories::NotificationRepository> repository,
    const nlohmann::json& config_json) {
    
    NotificationController::Config config;
    
    if (config_json.contains("http_host")) {
        config.http_host = config_json["http_host"];
    }
    if (config_json.contains("http_port")) {
        config.http_port = config_json["http_port"];
    }
    if (config_json.contains("websocket_port")) {
        config.websocket_port = config_json["websocket_port"];
    }
    if (config_json.contains("jwt_secret")) {
        config.jwt_secret = config_json["jwt_secret"];
    }
    if (config_json.contains("enable_websocket")) {
        config.enable_websocket = config_json["enable_websocket"];
    }
    
    return create(repository, config);
}

} // namespace controllers
} // namespace notification_service
} // namespace sonet
