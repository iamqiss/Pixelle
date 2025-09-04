/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This is the repository interface for notifications. I designed this to be
 * database-agnostic so we can switch from postgresql to something else if needed.
 * The interface is clean but powerful enough to handle millions of notifications.
 */

#pragma once

#include "../models/notification.h"
#include <memory>
#include <vector>
#include <optional>
#include <functional>
#include <chrono>
#include <unordered_map>
#include <future>

namespace sonet {
namespace notification_service {
namespace repositories {

/**
 * Query filter for finding notifications efficiently
 * I made this flexible so we can filter by any combination of criteria
 */
struct NotificationFilter {
    std::optional<std::string> user_id;
    std::optional<std::string> sender_id;
    std::optional<models::NotificationType> type;
    std::optional<models::DeliveryStatus> status;
    std::optional<models::NotificationPriority> priority;
    std::optional<bool> is_read;
    std::optional<std::chrono::system_clock::time_point> created_after;
    std::optional<std::chrono::system_clock::time_point> created_before;
    std::optional<std::chrono::system_clock::time_point> expires_after;
    std::optional<std::chrono::system_clock::time_point> expires_before;
    std::vector<std::string> group_keys;
    std::vector<std::string> batch_ids;
    std::optional<int> delivery_channels; // Bitfield filter
    
    // Pagination
    std::optional<int> limit;
    std::optional<int> offset;
    std::optional<std::string> cursor; // For cursor-based pagination
    
    // Sorting
    enum class SortBy {
        CREATED_AT,
        PRIORITY,
        STATUS,
        TYPE
    };
    SortBy sort_by = SortBy::CREATED_AT;
    bool sort_ascending = false; // Default to newest first
};

/**
 * Analytics data for understanding notification performance
 * I track these metrics to optimize the notification experience
 */
struct NotificationStats {
    std::string user_id;
    int total_notifications;
    int unread_count;
    int pending_count;
    int delivered_count;
    int failed_count;
    std::unordered_map<models::NotificationType, int> counts_by_type;
    std::unordered_map<models::DeliveryChannel, int> counts_by_channel;
    std::chrono::system_clock::time_point last_notification_at;
    std::chrono::system_clock::time_point last_read_at;
    double avg_delivery_time_ms;
    double delivery_success_rate;
};

/**
 * Result tracking for bulk operations
 * When you're processing thousands of notifications, you need to know what worked
 */
struct BulkOperationResult {
    int total_requested;
    int successful;
    int failed;
    std::vector<std::string> failed_ids;
    std::vector<std::string> error_messages;
    std::chrono::milliseconds execution_time;
    
    bool is_successful() const { return failed == 0; }
    double success_rate() const { 
        return total_requested > 0 ? static_cast<double>(successful) / total_requested : 0.0; 
    }
};

/**
 * Main repository interface for notification storage
 * I keep this abstract so we can swap out databases if needed
 */
class NotificationRepository {
public:
    virtual ~NotificationRepository() = default;
    
    // Core CRUD operations - the bread and butter
    virtual std::future<std::optional<models::Notification>> get_notification(
        const std::string& notification_id) = 0;
    
    virtual std::future<std::vector<models::Notification>> get_notifications(
        const NotificationFilter& filter) = 0;
    
    virtual std::future<std::string> create_notification(
        const models::Notification& notification) = 0;
    
    virtual std::future<bool> update_notification(
        const models::Notification& notification) = 0;
    
    virtual std::future<bool> delete_notification(
        const std::string& notification_id) = 0;
    
    // Bulk operations for performance at scale
    virtual std::future<BulkOperationResult> create_notifications_bulk(
        const std::vector<models::Notification>& notifications) = 0;
    
    virtual std::future<BulkOperationResult> update_notifications_bulk(
        const std::vector<models::Notification>& notifications) = 0;
    
    virtual std::future<BulkOperationResult> delete_notifications_bulk(
        const std::vector<std::string>& notification_ids) = 0;
    
    virtual std::future<BulkOperationResult> mark_as_read_bulk(
        const std::string& user_id, const std::vector<std::string>& notification_ids) = 0;
    
    // User-focused operations - what the app actually needs
    virtual std::future<std::vector<models::Notification>> get_user_notifications(
        const std::string& user_id, int limit = 50, int offset = 0) = 0;
    
    virtual std::future<std::vector<models::Notification>> get_unread_notifications(
        const std::string& user_id, int limit = 50) = 0;
    
    virtual std::future<int> get_unread_count(const std::string& user_id) = 0;
    
    virtual std::future<bool> mark_notification_as_read(
        const std::string& notification_id, const std::string& user_id) = 0;
    
    virtual std::future<bool> mark_all_as_read(const std::string& user_id) = 0;
    
    // Delivery status tracking
    virtual std::future<bool> update_delivery_status(
        const std::string& notification_id, models::DeliveryStatus status,
        const std::string& failure_reason = "") = 0;
    
    virtual std::future<std::vector<models::Notification>> get_pending_notifications(
        int limit = 100) = 0;
    
    virtual std::future<std::vector<models::Notification>> get_scheduled_notifications(
        const std::chrono::system_clock::time_point& until_time, int limit = 100) = 0;
    
    virtual std::future<std::vector<models::Notification>> get_expired_notifications(
        int limit = 100) = 0;
    
    // Batch Operations
    virtual std::future<std::string> create_notification_batch(
        const models::NotificationBatch& batch) = 0;
    
    virtual std::future<std::optional<models::NotificationBatch>> get_notification_batch(
        const std::string& batch_id) = 0;
    
    virtual std::future<bool> update_notification_batch(
        const models::NotificationBatch& batch) = 0;
    
    virtual std::future<std::vector<models::NotificationBatch>> get_pending_batches(
        int limit = 50) = 0;
    
    // User preferences for customization
    virtual std::future<std::optional<models::NotificationPreferences>> get_user_preferences(
        const std::string& user_id) = 0;
    
    virtual std::future<bool> save_user_preferences(
        const models::NotificationPreferences& preferences) = 0;
    
    virtual std::future<bool> delete_user_preferences(const std::string& user_id) = 0;
    
    // Analytics and insights
    virtual std::future<NotificationStats> get_user_stats(
        const std::string& user_id) = 0;
    
    virtual std::future<std::unordered_map<std::string, NotificationStats>> get_user_stats_bulk(
        const std::vector<std::string>& user_ids) = 0;
    
    virtual std::future<std::unordered_map<models::NotificationType, int>> get_notification_counts_by_type(
        const std::string& user_id, 
        const std::chrono::system_clock::time_point& since) = 0;
    
    virtual std::future<std::vector<models::Notification>> get_failed_notifications(
        const std::chrono::system_clock::time_point& since, int limit = 100) = 0;
    
    // Maintenance operations - keeping things clean
    virtual std::future<int> cleanup_expired_notifications() = 0;
    
    virtual std::future<int> cleanup_old_notifications(
        const std::chrono::system_clock::time_point& older_than) = 0;
    
    virtual std::future<bool> vacuum_database() = 0;
    
    virtual std::future<std::unordered_map<std::string, std::string>> get_health_metrics() = 0;
    
    // Advanced queries for complex use cases
    virtual std::future<std::vector<models::Notification>> search_notifications(
        const std::string& query, const NotificationFilter& filter) = 0;
    
    virtual std::future<std::vector<std::string>> get_grouped_notifications(
        const std::string& group_key, int limit = 50) = 0;
    
    virtual std::future<std::vector<models::Notification>> get_notifications_by_note(
        const std::string& note_id, int limit = 50) = 0;
    
    virtual std::future<std::vector<models::Notification>> get_notifications_by_conversation(
        const std::string& conversation_id, int limit = 50) = 0;
    
    // Cache management for performance
    virtual void invalidate_user_cache(const std::string& user_id) = 0;
    virtual void invalidate_notification_cache(const std::string& notification_id) = 0;
    virtual void clear_all_caches() = 0;
    virtual std::future<std::unordered_map<std::string, std::string>> get_cache_stats() = 0;
};

/**
 * postgresql implementation of NotificationRepository
 * I optimized this for Sonet-scale notification handling with multi-layer caching
 */
class NotegreSQLNotificationRepository : public NotificationRepository {
public:
    // Configuration for database connection and caching
    struct Config {
        std::string connection_string;
        int max_connections = 20;
        int min_connections = 5;
        std::chrono::seconds connection_timeout{30};
        
        // Cache configuration
        bool enable_redis_cache = true;
        std::string redis_host = "localhost";
        int redis_port = 6379;
        std::string redis_password;
        int redis_db = 0;
        std::chrono::seconds cache_ttl{300}; // 5 minutes
        
        // Performance tuning
        int bulk_insert_batch_size = 1000;
        int query_timeout_seconds = 30;
        bool enable_prepared_statements = true;
        bool enable_connection_pooling = true;
        
        // Monitoring
        bool enable_performance_tracking = true;
        bool enable_query_logging = false;
    };
    
    explicit NotegreSQLNotificationRepository(const Config& config);
    virtual ~NotegreSQLNotificationRepository();
    
    // Implement all virtual methods from base class
    std::future<std::optional<models::Notification>> get_notification(
        const std::string& notification_id) override;
    
    std::future<std::vector<models::Notification>> get_notifications(
        const NotificationFilter& filter) override;
    
    std::future<std::string> create_notification(
        const models::Notification& notification) override;
    
    std::future<bool> update_notification(
        const models::Notification& notification) override;
    
    std::future<bool> delete_notification(
        const std::string& notification_id) override;
    
    std::future<BulkOperationResult> create_notifications_bulk(
        const std::vector<models::Notification>& notifications) override;
    
    std::future<BulkOperationResult> update_notifications_bulk(
        const std::vector<models::Notification>& notifications) override;
    
    std::future<BulkOperationResult> delete_notifications_bulk(
        const std::vector<std::string>& notification_ids) override;
    
    std::future<BulkOperationResult> mark_as_read_bulk(
        const std::string& user_id, const std::vector<std::string>& notification_ids) override;
    
    std::future<std::vector<models::Notification>> get_user_notifications(
        const std::string& user_id, int limit = 50, int offset = 0) override;
    
    std::future<std::vector<models::Notification>> get_unread_notifications(
        const std::string& user_id, int limit = 50) override;
    
    std::future<int> get_unread_count(const std::string& user_id) override;
    
    std::future<bool> mark_notification_as_read(
        const std::string& notification_id, const std::string& user_id) override;
    
    std::future<bool> mark_all_as_read(const std::string& user_id) override;
    
    std::future<bool> update_delivery_status(
        const std::string& notification_id, models::DeliveryStatus status,
        const std::string& failure_reason = "") override;
    
    std::future<std::vector<models::Notification>> get_pending_notifications(
        int limit = 100) override;
    
    std::future<std::vector<models::Notification>> get_scheduled_notifications(
        const std::chrono::system_clock::time_point& until_time, int limit = 100) override;
    
    std::future<std::vector<models::Notification>> get_expired_notifications(
        int limit = 100) override;
    
    std::future<std::string> create_notification_batch(
        const models::NotificationBatch& batch) override;
    
    std::future<std::optional<models::NotificationBatch>> get_notification_batch(
        const std::string& batch_id) override;
    
    std::future<bool> update_notification_batch(
        const models::NotificationBatch& batch) override;
    
    std::future<std::vector<models::NotificationBatch>> get_pending_batches(
        int limit = 50) override;
    
    std::future<std::optional<models::NotificationPreferences>> get_user_preferences(
        const std::string& user_id) override;
    
    std::future<bool> save_user_preferences(
        const models::NotificationPreferences& preferences) override;
    
    std::future<bool> delete_user_preferences(const std::string& user_id) override;
    
    std::future<NotificationStats> get_user_stats(
        const std::string& user_id) override;
    
    std::future<std::unordered_map<std::string, NotificationStats>> get_user_stats_bulk(
        const std::vector<std::string>& user_ids) override;
    
    std::future<std::unordered_map<models::NotificationType, int>> get_notification_counts_by_type(
        const std::string& user_id, 
        const std::chrono::system_clock::time_point& since) override;
    
    std::future<std::vector<models::Notification>> get_failed_notifications(
        const std::chrono::system_clock::time_point& since, int limit = 100) override;
    
    std::future<int> cleanup_expired_notifications() override;
    
    std::future<int> cleanup_old_notifications(
        const std::chrono::system_clock::time_point& older_than) override;
    
    std::future<bool> vacuum_database() override;
    
    std::future<std::unordered_map<std::string, std::string>> get_health_metrics() override;
    
    std::future<std::vector<models::Notification>> search_notifications(
        const std::string& query, const NotificationFilter& filter) override;
    
    std::future<std::vector<std::string>> get_grouped_notifications(
        const std::string& group_key, int limit = 50) override;
    
    std::future<std::vector<models::Notification>> get_notifications_by_note(
        const std::string& note_id, int limit = 50) override;
    
    std::future<std::vector<models::Notification>> get_notifications_by_conversation(
        const std::string& conversation_id, int limit = 50) override;
    
    void invalidate_user_cache(const std::string& user_id) override;
    void invalidate_notification_cache(const std::string& notification_id) override;
    void clear_all_caches() override;
    std::future<std::unordered_map<std::string, std::string>> get_cache_stats() override;
    
    // Additional methods specific to postgresql implementation
    std::future<bool> initialize_database();
    std::future<bool> migrate_schema();
    std::future<bool> create_indexes();
    std::future<bool> setup_partitioning();
    
    // Performance monitoring
    struct PerformanceMetrics {
        std::chrono::microseconds avg_query_time{0};
        std::chrono::microseconds max_query_time{0};
        int total_queries = 0;
        int cache_hits = 0;
        int cache_misses = 0;
        int connection_pool_size = 0;
        int active_connections = 0;
        std::chrono::system_clock::time_point last_reset;
    };
    
    PerformanceMetrics get_performance_metrics() const;
    void reset_performance_metrics();
    
private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    // Internal helper methods
    std::string build_filter_query(const NotificationFilter& filter) const;
    std::vector<std::string> build_filter_params(const NotificationFilter& filter) const;
    models::Notification map_row_to_notification(const void* row) const;
    models::NotificationBatch map_row_to_batch(const void* row) const;
    models::NotificationPreferences map_row_to_preferences(const void* row) const;
    
    // Cache helper methods
    std::string get_cache_key(const std::string& prefix, const std::string& id) const;
    std::optional<std::string> get_from_cache(const std::string& key) const;
    void set_cache(const std::string& key, const std::string& value, 
                   std::chrono::seconds ttl = std::chrono::seconds{300}) const;
    void delete_from_cache(const std::string& key) const;
    void delete_cache_pattern(const std::string& pattern) const;
    
    // Performance tracking
    void track_query_start(const std::string& query_type) const;
    void track_query_end(const std::string& query_type, 
                        std::chrono::microseconds duration) const;
    void track_cache_hit() const;
    void track_cache_miss() const;
};

/**
 * Factory for creating repository instances
 * I use this pattern to keep the implementation details hidden
 */
class NotificationRepositoryFactory {
public:
    enum class RepositoryType {
        postgresql,
        MONGODB,
        MEMORY // For testing
    };
    
    static std::unique_ptr<NotificationRepository> create(
        RepositoryType type, const nlohmann::json& config);
    
    static std::unique_ptr<NotificationRepository> create_notegresql(
        const NotegreSQLNotificationRepository::Config& config);
};

} // namespace repositories
} // namespace notification_service
} // namespace sonet
