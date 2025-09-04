/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * I designed this notification system to handle millions of users efficiently
 * while keeping the code clean and maintainable. Every notification type here
 * serves a specific purpose in building meaningful social interactions.
 */

#pragma once

#include <string>
#include <chrono>
#include <memory>
#include <unordered_map>
#include <vector>
#include <nlohmann/json.hpp>

namespace sonet {
namespace notification_service {
namespace models {

/**
 * Notification types for different social media events
 * I keep these focused on meaningful interactions that users actually care about
 */
enum class NotificationType {
    LIKE = 1,           // Someone liked your note
    COMMENT = 2,        // Someone commented on your note
    FOLLOW = 3,         // Someone started following you
    MENTION = 4,        // Someone mentioned you in a note
    REPLY = 5,          // Someone replied to your comment
    RENOTE = 6,         // Someone renoted your note (like retweet but better)
    QUOTE_NOTE = 7,     // Someone quoted your note with their own commentary
    DIRECT_MESSAGE = 8, // Someone sent you a DM
    SYSTEM_ALERT = 9,   // System notifications for important updates
    PROMOTION = 10,     // Promotional content (used sparingly)
    TRENDING_NOTE = 11, // Your note is trending
    FOLLOWER_MILESTONE = 12, // You hit a follower milestone
    NOTE_MILESTONE = 13      // Your note hit engagement milestones
};

/**
 * Notification delivery channels
 */
enum class DeliveryChannel {
    IN_APP = 1,
    PUSH_NOTIFICATION = 2,
    EMAIL = 4,
    SMS = 8,
    WEBHOOK = 16
};

/**
 * Notification priority levels for delivery optimization
 */
enum class NotificationPriority {
    LOW = 1,
    NORMAL = 2,
    HIGH = 3,
    URGENT = 4
};

/**
 * Notification delivery status tracking
 */
enum class DeliveryStatus {
    PENDING = 1,
    SENT = 2,
    DELIVERED = 3,
    READ = 4,
    FAILED = 5,
    CANCELLED = 6
};

/**
 * Core notification model for Sonet's social media platform
 * I designed this to handle millions of notifications efficiently while keeping
 * all the data we need for smart delivery and user preferences
 */
class Notification {
public:
    // Core notification data - the essentials every notification needs
    std::string id;
    std::string user_id;          // Who's getting this notification
    std::string sender_id;        // Who triggered it (can be system)
    NotificationType type;
    std::string title;
    std::string message;
    std::string action_url;       // Where to go when they tap it
    
    // Content references - linking back to the social content
    std::string note_id;          // Related note ID (our version of notes)
    std::string comment_id;       // Related comment ID if applicable
    std::string conversation_id;  // Related DM conversation if applicable
    
    // Delivery configuration
    int delivery_channels;        // Bitfield of DeliveryChannel values
    NotificationPriority priority;
    
    // Timing and scheduling
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point scheduled_at;
    std::chrono::system_clock::time_point expires_at;
    
    // Delivery tracking
    DeliveryStatus status;
    std::chrono::system_clock::time_point delivered_at;
    std::chrono::system_clock::time_point read_at;
    int delivery_attempts;
    std::string failure_reason;
    
    // Grouping and batching - I use this to avoid spam
    std::string group_key;        // For grouping similar notifications together
    std::string batch_id;         // For processing notifications in batches
    bool is_batched;
    
    // Metadata and customization - where the magic happens
    nlohmann::json metadata;      // Extra data specific to notification type
    nlohmann::json template_data; // Variables for rendering templates
    std::string template_id;      // Which template to use for rendering
    
    // Performance and analytics - I track everything for insights
    std::string tracking_id;      // Unique ID for analytics and debugging
    nlohmann::json analytics_data; // Click tracking, engagement metrics, etc.
    
    // User preferences - respecting how people want to be notified
    bool respect_quiet_hours;     // Don't wake people up at 3am
    bool allow_bundling;          // Can this be grouped with similar notifications
    
    // Constructors - keeping it simple but flexible
    Notification();
    Notification(const std::string& user_id, const std::string& sender_id, 
                NotificationType type, const std::string& title, 
                const std::string& message);
    
    // Copy/Move constructors and assignment operators
    Notification(const Notification& other) = default;
    Notification(Notification&& other) noexcept = default;
    Notification& operator=(const Notification& other) = default;
    Notification& operator=(Notification&& other) noexcept = default;
    
    // Destructor
    virtual ~Notification() = default;
    
    // Delivery channel management
    void add_delivery_channel(DeliveryChannel channel);
    void remove_delivery_channel(DeliveryChannel channel);
    bool has_delivery_channel(DeliveryChannel channel) const;
    std::vector<DeliveryChannel> get_delivery_channels() const;
    
    // Status management
    void mark_as_sent();
    void mark_as_delivered();
    void mark_as_read();
    void mark_as_failed(const std::string& reason);
    bool is_expired() const;
    bool is_readable() const;
    
    // Grouping and batching
    void set_group_key(const std::string& key);
    bool can_be_grouped_with(const Notification& other) const;
    
    // Template and rendering
    void set_template(const std::string& template_id, const nlohmann::json& data);
    std::string render_message() const;
    std::string render_title() const;
    
    // Serialization
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& json);
    
    // Validation
    bool is_valid() const;
    std::vector<std::string> validate() const;
    
    // Utility methods
    std::string get_display_text() const;
    std::string get_summary() const;
    std::chrono::milliseconds get_age() const;
    bool should_send_now() const;
    
    // Analytics and tracking
    void record_click(const std::string& element_id = "");
    void record_view();
    void record_dismiss();
    nlohmann::json get_analytics_summary() const;
    
    // Comparison operators
    bool operator==(const Notification& other) const;
    bool operator!=(const Notification& other) const;
    bool operator<(const Notification& other) const; // For sorting by creation time
    
    // Hash function for unordered containers
    struct Hash {
        std::size_t operator()(const Notification& notification) const;
    };
    
private:
    // Internal utility methods
    void initialize_defaults();
    std::string generate_tracking_id() const;
    void update_delivery_attempt();
    
    // Template rendering helpers
    std::string process_template_variables(const std::string& template_str) const;
    nlohmann::json get_template_context() const;
};

/**
 * Notification batch for efficient bulk processing
 */
class NotificationBatch {
public:
    std::string batch_id;
    std::vector<std::shared_ptr<Notification>> notifications;
    NotificationType common_type;
    std::string target_user_id;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point scheduled_at;
    DeliveryStatus status;
    int total_count;
    int delivered_count;
    int failed_count;
    
    // Constructors
    NotificationBatch();
    explicit NotificationBatch(const std::string& user_id);
    
    // Batch management
    void add_notification(std::shared_ptr<Notification> notification);
    void remove_notification(const std::string& notification_id);
    bool can_add_notification(const Notification& notification) const;
    
    // Processing
    void mark_as_processed();
    void mark_notification_delivered(const std::string& notification_id);
    void mark_notification_failed(const std::string& notification_id, 
                                  const std::string& reason);
    
    // Summary and analytics
    std::string get_summary_message() const;
    nlohmann::json get_batch_analytics() const;
    bool is_complete() const;
    
    // Serialization
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& json);
};

/**
 * Notification preferences for user customization
 */
class NotificationPreferences {
public:
    std::string user_id;
    
    // Channel preferences
    std::unordered_map<NotificationType, int> channel_preferences; // Bitfield per type
    
    // Timing preferences
    bool enable_quiet_hours;
    std::chrono::system_clock::time_point quiet_start;
    std::chrono::system_clock::time_point quiet_end;
    std::string timezone;
    
    // Frequency preferences
    std::unordered_map<NotificationType, int> frequency_limits; // Max per hour
    bool enable_batching;
    std::chrono::minutes batch_interval;
    
    // Content preferences
    std::unordered_map<NotificationType, bool> type_enabled;
    std::vector<std::string> blocked_senders;
    std::vector<std::string> priority_senders;
    
    // Privacy preferences
    bool show_preview_in_lock_screen;
    bool show_sender_name;
    bool enable_read_receipts;
    
    // Constructors
    NotificationPreferences();
    explicit NotificationPreferences(const std::string& user_id);
    
    // Preference management
    void set_channel_preference(NotificationType type, int channels);
    int get_channel_preference(NotificationType type) const;
    bool is_channel_enabled(NotificationType type, DeliveryChannel channel) const;
    
    void set_type_enabled(NotificationType type, bool enabled);
    bool is_type_enabled(NotificationType type) const;
    
    void add_blocked_sender(const std::string& sender_id);
    void remove_blocked_sender(const std::string& sender_id);
    bool is_sender_blocked(const std::string& sender_id) const;
    
    // Timing checks
    bool is_in_quiet_hours() const;
    bool should_batch_notifications() const;
    bool can_send_notification(NotificationType type) const;
    
    // Serialization
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& json);
    
    // Validation
    bool is_valid() const;
    void apply_defaults();
};

// Utility functions for notification type conversion
std::string notification_type_to_string(NotificationType type);
NotificationType string_to_notification_type(const std::string& type_str);
std::string delivery_channel_to_string(DeliveryChannel channel);
DeliveryChannel string_to_delivery_channel(const std::string& channel_str);
std::string priority_to_string(NotificationPriority priority);
NotificationPriority string_to_priority(const std::string& priority_str);
std::string status_to_string(DeliveryStatus status);
DeliveryStatus string_to_status(const std::string& status_str);

// Helper functions for creating common notifications
// I made these to keep notification creation consistent across the app
std::shared_ptr<Notification> create_like_notification(
    const std::string& recipient_id, const std::string& liker_id, 
    const std::string& note_id);

std::shared_ptr<Notification> create_follow_notification(
    const std::string& recipient_id, const std::string& follower_id);

std::shared_ptr<Notification> create_comment_notification(
    const std::string& recipient_id, const std::string& commenter_id,
    const std::string& note_id, const std::string& comment_id);

std::shared_ptr<Notification> create_mention_notification(
    const std::string& recipient_id, const std::string& mentioner_id,
    const std::string& note_id);

std::shared_ptr<Notification> create_renote_notification(
    const std::string& recipient_id, const std::string& renoter_id,
    const std::string& note_id);

std::shared_ptr<Notification> create_system_notification(
    const std::string& recipient_id, const std::string& title,
    const std::string& message, NotificationPriority priority = NotificationPriority::NORMAL);

} // namespace models
} // namespace notification_service
} // namespace sonet
