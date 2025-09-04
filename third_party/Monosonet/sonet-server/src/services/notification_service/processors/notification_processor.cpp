/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This implements the notification processor - the brain of the notification system.
 * I built this to intelligently batch, rate limit, and route notifications so users
 * get timely updates without being overwhelmed. It's designed to handle millions.
 */

#include "notification_processor.h"
#include <thread>
#include <queue>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <uuid/uuid.h>

namespace sonet {
namespace notification_service {
namespace processors {

// Internal implementation details
struct NotificationProcessor::Impl {
    Config config;
    std::shared_ptr<repositories::NotificationRepository> repository;
    std::shared_ptr<channels::EmailChannel> email_channel;
    std::shared_ptr<channels::PushChannel> push_channel;
    std::shared_ptr<channels::WebSocketChannel> websocket_channel;
    
    // Processing state
    std::atomic<bool> is_running{false};
    std::atomic<bool> is_paused{false};
    std::vector<std::thread> worker_threads;
    std::thread batch_processor_thread;
    std::thread metrics_thread;
    std::thread health_check_thread;
    
    // Queues and synchronization
    std::queue<models::Notification> notification_queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    
    // Rate limiting and batching
    std::unordered_map<std::string, UserRateLimit> user_rate_limits;
    std::unordered_map<std::string, NotificationBatch> active_batches;
    std::unordered_map<models::NotificationType, NotificationProcessingRule> processing_rules;
    std::unordered_set<std::string> deduplication_cache;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> dedup_expirations;
    size_t dedup_cleanup_counter = 0;
    
    mutable std::mutex rate_limit_mutex;
    mutable std::mutex batch_mutex;
    mutable std::mutex rules_mutex;
    mutable std::mutex dedup_mutex;
    
    // Statistics
    ProcessingStats stats;
    mutable std::mutex stats_mutex;
    
    // Helper for generating IDs
    std::random_device rd;
    std::mt19937 gen{rd()};
    
    Impl(std::shared_ptr<repositories::NotificationRepository> repo, const Config& cfg)
        : config(cfg), repository(repo) {
        stats.start_time = std::chrono::system_clock::now();
        initialize_default_rules();
    }
    
    void initialize_default_rules() {
        // I set up sensible defaults for each notification type
        std::lock_guard<std::mutex> lock(rules_mutex);
        
        // Like notifications - batch these to avoid spam
        NotificationProcessingRule like_rule;
        like_rule.type = models::NotificationType::LIKE;
        like_rule.enable_batching = true;
        like_rule.batch_window = std::chrono::minutes{10};
        like_rule.max_batch_size = 20;
        like_rule.deduplicate = true;
        like_rule.deduplication_window = std::chrono::minutes{30};
        like_rule.rate_limit = true;
        like_rule.max_per_hour = 20;
        like_rule.max_per_day = 100;
        like_rule.allowed_channels = {models::DeliveryChannel::WEBSOCKET, models::DeliveryChannel::PUSH};
        like_rule.default_priority = models::NotificationPriority::LOW;
        processing_rules[models::NotificationType::LIKE] = like_rule;
        
        // Comment notifications - more important, less batching
        NotificationProcessingRule comment_rule;
        comment_rule.type = models::NotificationType::COMMENT;
        comment_rule.enable_batching = true;
        comment_rule.batch_window = std::chrono::minutes{5};
        comment_rule.max_batch_size = 5;
        comment_rule.deduplicate = false; // Each comment is unique
        comment_rule.rate_limit = true;
        comment_rule.max_per_hour = 30;
        comment_rule.max_per_day = 200;
        comment_rule.allowed_channels = {
            models::DeliveryChannel::WEBSOCKET, 
            models::DeliveryChannel::PUSH, 
            models::DeliveryChannel::EMAIL
        };
        comment_rule.default_priority = models::NotificationPriority::NORMAL;
        processing_rules[models::NotificationType::COMMENT] = comment_rule;
        
        // Follow notifications - immediate delivery
        NotificationProcessingRule follow_rule;
        follow_rule.type = models::NotificationType::FOLLOW;
        follow_rule.enable_batching = false; // Send immediately
        follow_rule.deduplicate = true;
        follow_rule.deduplication_window = std::chrono::hours{24};
        follow_rule.rate_limit = true;
        follow_rule.max_per_hour = 10;
        follow_rule.max_per_day = 50;
        follow_rule.allowed_channels = {
            models::DeliveryChannel::WEBSOCKET, 
            models::DeliveryChannel::PUSH, 
            models::DeliveryChannel::EMAIL
        };
        follow_rule.default_priority = models::NotificationPriority::HIGH;
        processing_rules[models::NotificationType::FOLLOW] = follow_rule;
        
        // Mention notifications - highest priority
        NotificationProcessingRule mention_rule;
        mention_rule.type = models::NotificationType::MENTION;
        mention_rule.enable_batching = false;
        mention_rule.deduplicate = false;
        mention_rule.rate_limit = true;
        mention_rule.max_per_hour = 15;
        mention_rule.max_per_day = 100;
        mention_rule.allowed_channels = {
            models::DeliveryChannel::WEBSOCKET, 
            models::DeliveryChannel::PUSH, 
            models::DeliveryChannel::EMAIL
        };
        mention_rule.default_priority = models::NotificationPriority::URGENT;
        processing_rules[models::NotificationType::MENTION] = mention_rule;
        
        // Renote notifications - similar to likes but less frequent
        NotificationProcessingRule renote_rule;
        renote_rule.type = models::NotificationType::RENOTE;
        renote_rule.enable_batching = true;
        renote_rule.batch_window = std::chrono::minutes{15};
        renote_rule.max_batch_size = 10;
        renote_rule.deduplicate = true;
        renote_rule.deduplication_window = std::chrono::hours{1};
        renote_rule.rate_limit = true;
        renote_rule.max_per_hour = 25;
        renote_rule.max_per_day = 150;
        renote_rule.allowed_channels = {
            models::DeliveryChannel::WEBSOCKET, 
            models::DeliveryChannel::PUSH, 
            models::DeliveryChannel::EMAIL
        };
        renote_rule.default_priority = models::NotificationPriority::NORMAL;
        processing_rules[models::NotificationType::RENOTE] = renote_rule;
        
        // DM notifications - always immediate and high priority
        NotificationProcessingRule dm_rule;
        dm_rule.type = models::NotificationType::DIRECT_MESSAGE;
        dm_rule.enable_batching = false;
        dm_rule.deduplicate = false;
        dm_rule.rate_limit = false; // Don't rate limit DMs
        dm_rule.allowed_channels = {
            models::DeliveryChannel::WEBSOCKET, 
            models::DeliveryChannel::PUSH, 
            models::DeliveryChannel::EMAIL
        };
        dm_rule.default_priority = models::NotificationPriority::URGENT;
        processing_rules[models::NotificationType::DIRECT_MESSAGE] = dm_rule;
    }
};

NotificationProcessor::NotificationProcessor(
    std::shared_ptr<repositories::NotificationRepository> repository,
    const Config& config)
    : pimpl_(std::make_unique<Impl>(repository, config)) {
}

NotificationProcessor::~NotificationProcessor() {
    stop();
}

std::future<bool> NotificationProcessor::process_notification(const models::Notification& notification) {
    auto promise = std::make_shared<std::promise<bool>>();
    auto future = promise->get_future();
    
    // Check if we should process this notification
    if (!should_process_notification(notification)) {
        promise->set_value(false);
        return future;
    }
    
    // Add to queue for processing
    {
        std::lock_guard<std::mutex> lock(pimpl_->queue_mutex);
        
        if (pimpl_->notification_queue.size() >= static_cast<size_t>(pimpl_->config.max_queue_size)) {
            promise->set_value(false);
            return future;
        }
        
        pimpl_->notification_queue.push(notification);
    }
    
    pimpl_->queue_cv.notify_one();
    promise->set_value(true);
    
    track_notification_processed();
    return future;
}

std::future<std::vector<bool>> NotificationProcessor::process_notifications_bulk(
    const std::vector<models::Notification>& notifications) {
    
    auto promise = std::make_shared<std::promise<std::vector<bool>>>();
    auto future = promise->get_future();
    
    std::vector<bool> results;
    results.reserve(notifications.size());
    
    {
        std::lock_guard<std::mutex> lock(pimpl_->queue_mutex);
        
        for (const auto& notification : notifications) {
            if (!should_process_notification(notification)) {
                results.push_back(false);
                continue;
            }
            
            if (pimpl_->notification_queue.size() >= static_cast<size_t>(pimpl_->config.max_queue_size)) {
                results.push_back(false);
                continue;
            }
            
            pimpl_->notification_queue.push(notification);
            results.push_back(true);
            track_notification_processed();
        }
    }
    
    pimpl_->queue_cv.notify_all();
    promise->set_value(results);
    
    return future;
}

std::future<bool> NotificationProcessor::send_immediate(const models::Notification& notification) {
    auto promise = std::make_shared<std::promise<bool>>();
    auto future = promise->get_future();
    
    // Bypass all batching and rate limiting for immediate delivery
    std::thread([this, notification, promise]() {
        bool success = send_notification_to_channels(notification);
        promise->set_value(success);
    }).detach();
    
    return future;
}

void NotificationProcessor::register_email_channel(std::shared_ptr<channels::EmailChannel> channel) {
    pimpl_->email_channel = channel;
}

void NotificationProcessor::register_push_channel(std::shared_ptr<channels::PushChannel> channel) {
    pimpl_->push_channel = channel;
}

void NotificationProcessor::register_websocket_channel(std::shared_ptr<channels::WebSocketChannel> channel) {
    pimpl_->websocket_channel = channel;
}

void NotificationProcessor::add_processing_rule(const NotificationProcessingRule& rule) {
    std::lock_guard<std::mutex> lock(pimpl_->rules_mutex);
    pimpl_->processing_rules[rule.type] = rule;
}

void NotificationProcessor::start() {
    if (pimpl_->is_running.load()) {
        return;
    }
    
    pimpl_->is_running = true;
    pimpl_->is_paused = false;
    
    // Start worker threads
    for (int i = 0; i < pimpl_->config.worker_thread_count; ++i) {
        pimpl_->worker_threads.emplace_back(&NotificationProcessor::worker_thread, this);
    }
    
    // Start specialized threads
    pimpl_->batch_processor_thread = std::thread(&NotificationProcessor::batch_processor_thread, this);
    pimpl_->metrics_thread = std::thread(&NotificationProcessor::metrics_thread, this);
    pimpl_->health_check_thread = std::thread(&NotificationProcessor::health_check_thread, this);
}

void NotificationProcessor::stop() {
    if (!pimpl_->is_running.load()) {
        return;
    }
    
    pimpl_->is_running = false;
    pimpl_->queue_cv.notify_all();
    
    // Wait for worker threads to finish
    for (auto& thread : pimpl_->worker_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    pimpl_->worker_threads.clear();
    
    // Wait for specialized threads
    if (pimpl_->batch_processor_thread.joinable()) {
        pimpl_->batch_processor_thread.join();
    }
    if (pimpl_->metrics_thread.joinable()) {
        pimpl_->metrics_thread.join();
    }
    if (pimpl_->health_check_thread.joinable()) {
        pimpl_->health_check_thread.join();
    }
}

bool NotificationProcessor::is_running() const {
    return pimpl_->is_running.load();
}

bool NotificationProcessor::is_paused() const {
    return pimpl_->is_paused.load();
}

int NotificationProcessor::get_queue_size() const {
    std::lock_guard<std::mutex> lock(pimpl_->queue_mutex);
    return static_cast<int>(pimpl_->notification_queue.size());
}

ProcessingStats NotificationProcessor::get_processing_stats() const {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    return pimpl_->stats;
}

// Worker thread implementation
void NotificationProcessor::worker_thread() {
    while (pimpl_->is_running.load()) {
        std::unique_lock<std::mutex> lock(pimpl_->queue_mutex);
        
        // Wait for notifications or shutdown
        pimpl_->queue_cv.wait(lock, [this] {
            return !pimpl_->notification_queue.empty() || !pimpl_->is_running.load();
        });
        
        if (!pimpl_->is_running.load()) {
            break;
        }
        
        if (pimpl_->is_paused.load()) {
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        
        // Get notification from queue
        models::Notification notification = pimpl_->notification_queue.front();
        pimpl_->notification_queue.pop();
        lock.unlock();
        
        try {
            // Process the notification
            if (!check_rate_limits(notification)) {
                track_notification_rate_limited();
                continue;
            }
            
            if (pimpl_->config.enable_deduplication && check_deduplication(notification)) {
                track_notification_deduplicated();
                continue;
            }
            
            // Check if we should batch this notification
            std::lock_guard<std::mutex> rules_lock(pimpl_->rules_mutex);
            auto rule_it = pimpl_->processing_rules.find(notification.type);
            if (rule_it != pimpl_->processing_rules.end() && rule_it->second.enable_batching) {
                auto batch_id = find_or_create_batch(notification);
                if (batch_id && add_to_batch(*batch_id, notification)) {
                    track_notification_batched();
                    continue;
                }
            }
            
            // Send immediately if not batched
            if (send_notification_to_channels(notification)) {
                // Update delivery status in repository
                pimpl_->repository->update_delivery_status(
                    notification.id, models::DeliveryStatus::DELIVERED);
            } else {
                track_notification_failed();
                pimpl_->repository->update_delivery_status(
                    notification.id, models::DeliveryStatus::FAILED, "Channel delivery failed");
            }
            
        } catch (const std::exception& e) {
            track_notification_failed();
            // Log error and update repository
            pimpl_->repository->update_delivery_status(
                notification.id, models::DeliveryStatus::FAILED, e.what());
        }
    }
}

// Batch processor thread
void NotificationProcessor::batch_processor_thread() {
    while (pimpl_->is_running.load()) {
        try {
            check_ready_batches();
            cleanup_expired_batches();
            
            std::this_thread::sleep_for(pimpl_->config.batch_check_interval);
        } catch (const std::exception& e) {
            // Log error but continue processing
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

// Metrics thread
void NotificationProcessor::metrics_thread() {
    while (pimpl_->is_running.load()) {
        try {
            if (pimpl_->config.enable_metrics) {
                flush_metrics();
            }
            
            std::this_thread::sleep_for(pimpl_->config.metrics_flush_interval);
        } catch (const std::exception& e) {
            // Log error but continue
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    }
}

// Health check thread
void NotificationProcessor::health_check_thread() {
    while (pimpl_->is_running.load()) {
        try {
            if (pimpl_->config.enable_health_checks) {
                cleanup_expired_rate_limits();
                // Additional health checks can go here
            }
            
            std::this_thread::sleep_for(std::chrono::minutes(1));
        } catch (const std::exception& e) {
            // Log error but continue
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}

// Helper method implementations
bool NotificationProcessor::should_process_notification(const models::Notification& notification) {
    // Basic validation
    if (notification.user_id.empty() || notification.id.empty()) {
        return false;
    }
    
    // Check expiry
    auto now = std::chrono::system_clock::now();
    if (notification.expires_at && now > *notification.expires_at) {
        return false;
    }
    
    return true;
}

bool NotificationProcessor::check_rate_limits(const models::Notification& notification) {
    if (!pimpl_->config.enable_rate_limiting) {
        return true;
    }
    
    std::lock_guard<std::mutex> lock(pimpl_->rate_limit_mutex);
    
    auto& user_limits = pimpl_->user_rate_limits[notification.user_id];
    auto now = std::chrono::system_clock::now();
    
    // Reset counters if time windows have passed
    if (now >= user_limits.hour_reset_time) {
        user_limits.hourly_counts.clear();
        user_limits.hour_reset_time = now + std::chrono::hours(1);
    }
    
    if (now >= user_limits.day_reset_time) {
        user_limits.daily_counts.clear();
        user_limits.day_reset_time = now + std::chrono::hours(24);
    }
    
    // Check if user is throttled
    if (user_limits.is_throttled && now < user_limits.throttled_until) {
        return false;
    } else if (user_limits.is_throttled) {
        user_limits.is_throttled = false;
    }
    
    // Get rate limits for this notification type
    std::lock_guard<std::mutex> rules_lock(pimpl_->rules_mutex);
    auto rule_it = pimpl_->processing_rules.find(notification.type);
    if (rule_it == pimpl_->processing_rules.end() || !rule_it->second.rate_limit) {
        return true;
    }
    
    const auto& rule = rule_it->second;
    
    // Check hourly limit
    int hourly_count = user_limits.hourly_counts[notification.type];
    if (hourly_count >= rule.max_per_hour) {
        return false;
    }
    
    // Check daily limit
    int daily_count = user_limits.daily_counts[notification.type];
    if (daily_count >= rule.max_per_day) {
        return false;
    }
    
    // Update counters
    user_limits.hourly_counts[notification.type]++;
    user_limits.daily_counts[notification.type]++;
    
    return true;
}

bool NotificationProcessor::check_deduplication(const models::Notification& notification) {
    std::lock_guard<std::mutex> lock(pimpl_->dedup_mutex);
    
    std::string dedup_key = generate_deduplication_key(notification);
    
    // Determine TTL window based on rule for this notification type
    auto rule_it = pimpl_->processing_rules.find(notification.type);
    auto now = std::chrono::steady_clock::now();
    auto ttl = std::chrono::minutes{30};
    if (rule_it != pimpl_->processing_rules.end() && rule_it->second.deduplication_window.count() > 0) {
        ttl = rule_it->second.deduplication_window;
    }
    
    // Cleanup a few expired entries opportunistically
    if ((++pimpl_->dedup_cleanup_counter % 64) == 0) {
        for (auto it = pimpl_->dedup_expirations.begin(); it != pimpl_->dedup_expirations.end(); ) {
            if (it->second <= now) {
                pimpl_->deduplication_cache.erase(it->first);
                it = pimpl_->dedup_expirations.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    // Check if we've seen this notification recently
    if (pimpl_->deduplication_cache.find(dedup_key) != pimpl_->deduplication_cache.end()) {
        auto exp_it = pimpl_->dedup_expirations.find(dedup_key);
        if (exp_it != pimpl_->dedup_expirations.end() && exp_it->second > now) {
            return true; // Duplicate within TTL window
        } else {
            // Expired entry; remove and proceed
            pimpl_->deduplication_cache.erase(dedup_key);
            if (exp_it != pimpl_->dedup_expirations.end()) pimpl_->dedup_expirations.erase(exp_it);
        }
    }
    
    // Add to cache with expiration
    pimpl_->deduplication_cache.insert(dedup_key);
    pimpl_->dedup_expirations[dedup_key] = now + ttl;
    
    return false; // Not a duplicate
}

std::string NotificationProcessor::generate_deduplication_key(const models::Notification& notification) const {
    // Create a key based on notification type, user, and relevant data
    std::ostringstream key;
    key << static_cast<int>(notification.type) << ":"
        << notification.user_id << ":"
        << notification.sender_id << ":";
    
    // Add type-specific data for better deduplication
    if (notification.template_data.contains("note_id")) {
        key << notification.template_data["note_id"].get<std::string>();
    }
    
    return key.str();
}

std::string NotificationProcessor::generate_batch_id() const {
    uuid_t uuid;
    uuid_generate_random(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    return std::string(uuid_str);
}

bool NotificationProcessor::send_notification_to_channels(const models::Notification& notification) {
    bool any_success = false;
    
    // Get user preferences from repository
    auto preferences_future = pimpl_->repository->get_user_preferences(notification.user_id);
    
    try {
        auto preferences_opt = preferences_future.get();
        models::NotificationPreferences preferences;
        if (preferences_opt) {
            preferences = *preferences_opt;
        } else {
            // Use default preferences
            preferences.user_id = notification.user_id;
            preferences.email_enabled = true;
            preferences.push_enabled = true;
            preferences.websocket_enabled = true;
        }
        
        // Send to WebSocket channel (real-time)
        if (pimpl_->websocket_channel && preferences.websocket_enabled) {
            auto ws_future = pimpl_->websocket_channel->send_to_user(notification, notification.user_id);
            // Don't wait for WebSocket delivery to complete
            any_success = true;
        }
        
        // Send to push channel
        if (pimpl_->push_channel && preferences.push_enabled) {
            auto push_future = pimpl_->push_channel->send_to_user(notification, notification.user_id, preferences);
            // TODO: Handle push delivery result
            any_success = true;
        }
        
        // Send to email channel (for important notifications)
        if (pimpl_->email_channel && preferences.email_enabled && 
            (notification.priority == models::NotificationPriority::HIGH || 
             notification.priority == models::NotificationPriority::URGENT)) {
            auto email_future = pimpl_->email_channel->send_notification_email(notification, preferences);
            // TODO: Handle email delivery result
            any_success = true;
        }
        
    } catch (const std::exception& e) {
        // Log error
        return false;
    }
    
    return any_success;
}

// Statistics tracking methods
void NotificationProcessor::track_notification_processed() {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    pimpl_->stats.notifications_processed++;
}

void NotificationProcessor::track_notification_batched() {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    pimpl_->stats.notifications_batched++;
}

void NotificationProcessor::track_notification_deduplicated() {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    pimpl_->stats.notifications_deduplicated++;
}

void NotificationProcessor::track_notification_rate_limited() {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    pimpl_->stats.notifications_rate_limited++;
}

void NotificationProcessor::track_notification_failed() {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    pimpl_->stats.notifications_failed++;
}

void NotificationProcessor::track_batch_created() {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    pimpl_->stats.batches_created++;
}

void NotificationProcessor::track_batch_sent() {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    pimpl_->stats.batches_sent++;
}

// Placeholder implementations for remaining methods
std::optional<std::string> NotificationProcessor::find_or_create_batch(const models::Notification& notification) {
    // TODO: Implement batch finding/creation logic
    return std::nullopt;
}

bool NotificationProcessor::add_to_batch(const std::string& batch_id, const models::Notification& notification) {
    // TODO: Implement batch addition logic
    return false;
}

void NotificationProcessor::check_ready_batches() {
    // TODO: Implement ready batch checking
}

void NotificationProcessor::cleanup_expired_batches() {
    // TODO: Implement expired batch cleanup
}

void NotificationProcessor::cleanup_expired_rate_limits() {
    // TODO: Implement rate limit cleanup
}

void NotificationProcessor::flush_metrics() {
    // TODO: Implement metrics flushing
}

// Factory implementation
std::unique_ptr<NotificationProcessor> NotificationProcessorFactory::create(
    std::shared_ptr<repositories::NotificationRepository> repository,
    const NotificationProcessor::Config& config) {
    
    return std::make_unique<NotificationProcessor>(repository, config);
}

NotificationProcessor::Config NotificationProcessorFactory::create_default_config() {
    NotificationProcessor::Config config;
    config.worker_thread_count = 4;
    config.max_queue_size = 10000;
    config.enable_rate_limiting = true;
    config.enable_batching = true;
    config.enable_deduplication = true;
    return config;
}

} // namespace processors
} // namespace notification_service
} // namespace sonet