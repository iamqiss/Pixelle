/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This is the notification processor that handles the business logic for creating,
 * routing, and batching notifications. I designed this to be the central hub
 * where all notification decisions are made before sending to channels.
 */

#pragma once

#include "../models/notification.h"
#include "../repositories/notification_repository.h"
#include "../channels/email_channel.h"
#include "../channels/push_channel.h"
#include "../channels/websocket_channel.h"
#include <memory>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <future>

namespace sonet {
namespace notification_service {
namespace processors {

/**
 * Processing rules for different notification types
 * I configure these to control how each type of notification is handled
 */
struct NotificationProcessingRule {
    models::NotificationType type;
    bool enable_batching = true;
    std::chrono::minutes batch_window{5}; // How long to collect similar notifications
    int max_batch_size = 10;
    bool deduplicate = true;
    std::chrono::minutes deduplication_window{60};
    bool rate_limit = true;
    int max_per_hour = 50;
    int max_per_day = 500;
    std::vector<models::DeliveryChannel> allowed_channels;
    models::NotificationPriority default_priority = models::NotificationPriority::NORMAL;
    std::chrono::minutes expiry_time{24 * 60}; // 24 hours default
    
    // Template settings
    std::string email_template;
    std::string push_template;
    std::string websocket_template;
    bool use_rich_content = true;
    bool include_actions = true;
};

/**
 * Rate limiting state for users
 * I track this to prevent notification spam
 */
struct UserRateLimit {
    std::string user_id;
    std::unordered_map<models::NotificationType, int> hourly_counts;
    std::unordered_map<models::NotificationType, int> daily_counts;
    std::chrono::system_clock::time_point hour_reset_time;
    std::chrono::system_clock::time_point day_reset_time;
    bool is_throttled = false;
    std::chrono::system_clock::time_point throttled_until;
};

/**
 * Batch state for grouping similar notifications
 * I use this to avoid overwhelming users with individual notifications
 */
struct NotificationBatch {
    std::string batch_id;
    models::NotificationType type;
    std::string primary_user_id;
    std::vector<std::string> notification_ids;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point scheduled_for;
    bool is_ready_to_send = false;
    nlohmann::json aggregated_data;
    
    bool should_send() const {
        return is_ready_to_send || std::chrono::system_clock::now() >= scheduled_for;
    }
};

/**
 * Processing statistics for monitoring
 * I track these to understand system performance
 */
struct ProcessingStats {
    std::atomic<int> notifications_processed{0};
    std::atomic<int> notifications_batched{0};
    std::atomic<int> notifications_deduplicated{0};
    std::atomic<int> notifications_rate_limited{0};
    std::atomic<int> notifications_failed{0};
    std::atomic<int> batches_created{0};
    std::atomic<int> batches_sent{0};
    std::chrono::system_clock::time_point start_time;
    
    void reset() {
        notifications_processed = 0;
        notifications_batched = 0;
        notifications_deduplicated = 0;
        notifications_rate_limited = 0;
        notifications_failed = 0;
        batches_created = 0;
        batches_sent = 0;
        start_time = std::chrono::system_clock::now();
    }
    
    nlohmann::json to_json() const {
        auto now = std::chrono::system_clock::now();
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
        
        return nlohmann::json{
            {"notifications_processed", notifications_processed.load()},
            {"notifications_batched", notifications_batched.load()},
            {"notifications_deduplicated", notifications_deduplicated.load()},
            {"notifications_rate_limited", notifications_rate_limited.load()},
            {"notifications_failed", notifications_failed.load()},
            {"batches_created", batches_created.load()},
            {"batches_sent", batches_sent.load()},
            {"uptime_seconds", uptime},
            {"processing_rate", uptime > 0 ? notifications_processed.load() / static_cast<double>(uptime) : 0.0}
        };
    }
};

/**
 * Main notification processor that handles all business logic
 * I built this to be the brain of the notification system
 */
class NotificationProcessor {
public:
    struct Config {
        // Processing settings
        int worker_thread_count = 4;
        int max_queue_size = 10000;
        std::chrono::milliseconds processing_interval{100};
        std::chrono::milliseconds batch_check_interval{1000};
        
        // Rate limiting
        bool enable_rate_limiting = true;
        int default_hourly_limit = 50;
        int default_daily_limit = 500;
        std::chrono::minutes rate_limit_window{60};
        
        // Batching
        bool enable_batching = true;
        std::chrono::minutes default_batch_window{5};
        int default_batch_size = 10;
        
        // Deduplication
        bool enable_deduplication = true;
        std::chrono::minutes deduplication_window{60};
        
        // Performance
        bool enable_metrics = true;
        std::chrono::seconds metrics_flush_interval{30};
        bool enable_health_checks = true;
    };
    
    explicit NotificationProcessor(
        std::shared_ptr<repositories::NotificationRepository> repository,
        const Config& config = Config{});
    
    ~NotificationProcessor();
    
    // Core processing methods
    std::future<bool> process_notification(const models::Notification& notification);
    std::future<std::vector<bool>> process_notifications_bulk(
        const std::vector<models::Notification>& notifications);
    
    // Immediate delivery (bypass batching and rate limiting)
    std::future<bool> send_immediate(const models::Notification& notification);
    
    // Batch management
    std::future<bool> force_send_batch(const std::string& batch_id);
    std::future<bool> force_send_user_batches(const std::string& user_id);
    std::future<std::vector<NotificationBatch>> get_pending_batches();
    
    // Channel registration
    void register_email_channel(std::shared_ptr<channels::EmailChannel> channel);
    void register_push_channel(std::shared_ptr<channels::PushChannel> channel);
    void register_websocket_channel(std::shared_ptr<channels::WebSocketChannel> channel);
    
    // Rule management
    void add_processing_rule(const NotificationProcessingRule& rule);
    void update_processing_rule(models::NotificationType type, const NotificationProcessingRule& rule);
    void remove_processing_rule(models::NotificationType type);
    std::optional<NotificationProcessingRule> get_processing_rule(models::NotificationType type) const;
    
    // User management
    std::future<bool> set_user_rate_limit(const std::string& user_id, 
                                          models::NotificationType type,
                                          int hourly_limit, int daily_limit);
    std::future<bool> throttle_user(const std::string& user_id, 
                                   std::chrono::minutes duration);
    std::future<bool> unthrottle_user(const std::string& user_id);
    std::future<bool> reset_user_rate_limits(const std::string& user_id);
    
    // Analytics and monitoring
    ProcessingStats get_processing_stats() const;
    void reset_processing_stats();
    std::future<nlohmann::json> get_health_status();
    std::future<std::unordered_map<std::string, nlohmann::json>> get_detailed_metrics();
    
    // Control methods
    void start();
    void stop();
    void pause();
    void resume();
    bool is_running() const;
    bool is_paused() const;
    
    // Queue management
    int get_queue_size() const;
    void clear_queue();
    bool is_queue_full() const;
    
private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    // Internal processing methods
    void worker_thread();
    void batch_processor_thread();
    void metrics_thread();
    void health_check_thread();
    
    bool should_process_notification(const models::Notification& notification);
    bool check_rate_limits(const models::Notification& notification);
    bool check_deduplication(const models::Notification& notification);
    std::optional<std::string> find_or_create_batch(const models::Notification& notification);
    bool add_to_batch(const std::string& batch_id, const models::Notification& notification);
    bool send_notification_to_channels(const models::Notification& notification);
    bool send_batch_to_channels(const NotificationBatch& batch);
    
    // Helper methods
    std::string generate_batch_id() const;
    std::string generate_deduplication_key(const models::Notification& notification) const;
    bool is_similar_notification(const models::Notification& a, const models::Notification& b) const;
    models::Notification create_batch_notification(const NotificationBatch& batch) const;
    
    // Rate limiting helpers
    void update_rate_limits(const std::string& user_id, models::NotificationType type);
    bool is_user_throttled(const std::string& user_id) const;
    void cleanup_expired_rate_limits();
    
    // Batch helpers
    void check_ready_batches();
    void cleanup_expired_batches();
    bool is_batch_ready(const NotificationBatch& batch) const;
    
    // Metrics helpers
    void track_notification_processed();
    void track_notification_batched();
    void track_notification_deduplicated();
    void track_notification_rate_limited();
    void track_notification_failed();
    void track_batch_created();
    void track_batch_sent();
    void flush_metrics();
};

/**
 * Factory for creating notification processors
 * I use this to standardize processor creation across the system
 */
class NotificationProcessorFactory {
public:
    static std::unique_ptr<NotificationProcessor> create(
        std::shared_ptr<repositories::NotificationRepository> repository,
        const NotificationProcessor::Config& config = NotificationProcessor::Config{});
    
    static std::unique_ptr<NotificationProcessor> create_with_channels(
        std::shared_ptr<repositories::NotificationRepository> repository,
        std::shared_ptr<channels::EmailChannel> email_channel,
        std::shared_ptr<channels::PushChannel> push_channel,
        std::shared_ptr<channels::WebSocketChannel> websocket_channel,
        const NotificationProcessor::Config& config = NotificationProcessor::Config{});
    
    static NotificationProcessor::Config create_default_config();
    static NotificationProcessor::Config create_high_volume_config();
    static NotificationProcessor::Config create_low_latency_config();
};

} // namespace processors
} // namespace notification_service
} // namespace sonet