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
#include <mutex>
#include <atomic>
#include <functional>
#include <future>
#include <queue>
#include <json/json.h>

namespace sonet::messaging::threading {

/**
 * @brief Thread visibility and access control
 */
enum class ThreadVisibility {
    PUBLIC = 0,      // Visible to all chat members
    PRIVATE = 1,     // Only visible to thread participants
    RESTRICTED = 2   // Only visible to specific roles
};

/**
 * @brief Thread participation level
 */
enum class ParticipationLevel {
    OBSERVER = 0,    // Can view but not participate
    PARTICIPANT = 1, // Can send messages and react
    MODERATOR = 2,   // Can moderate thread content
    ADMIN = 3        // Full thread control
};

/**
 * @brief Thread status for lifecycle management
 */
enum class ThreadStatus {
    ACTIVE = 0,      // Thread is active and accepting messages
    ARCHIVED = 1,    // Thread is archived but viewable
    LOCKED = 2,      // Thread is locked, no new messages
    DELETED = 3      // Thread is soft-deleted
};

/**
 * @brief Thread metadata for organization
 */
struct ThreadMetadata {
    std::string thread_id;
    std::string chat_id;
    std::string parent_message_id;
    std::string title;
    std::string description;
    ThreadVisibility visibility;
    ThreadStatus status;
    std::string creator_id;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::chrono::system_clock::time_point last_activity;
    
    // Statistics
    uint32_t message_count;
    uint32_t participant_count;
    uint32_t view_count;
    
    // Configuration
    bool allow_reactions;
    bool allow_replies;
    bool auto_archive;
    std::chrono::hours auto_archive_duration;
    uint32_t max_participants;
    
    // Tags and categories
    std::vector<std::string> tags;
    std::string category;
    uint8_t priority;
    
    Json::Value to_json() const;
    static ThreadMetadata from_json(const Json::Value& json);
};

/**
 * @brief Thread participant with role and permissions
 */
struct ThreadParticipant {
    std::string user_id;
    std::string thread_id;
    ParticipationLevel level;
    std::chrono::system_clock::time_point joined_at;
    std::chrono::system_clock::time_point last_read;
    bool notifications_enabled;
    bool is_muted;
    uint32_t unread_count;
    
    // Participation stats
    uint32_t messages_sent;
    uint32_t reactions_given;
    std::chrono::system_clock::time_point last_active;
    
    Json::Value to_json() const;
    static ThreadParticipant from_json(const Json::Value& json);
};

/**
 * @brief Reply relationship between messages
 */
struct MessageReply {
    std::string reply_id;
    std::string parent_message_id;
    std::string replying_message_id;
    std::string user_id;
    std::string quoted_text;
    std::chrono::system_clock::time_point created_at;
    bool is_thread_starter;
    uint32_t depth_level;
    
    Json::Value to_json() const;
    static MessageReply from_json(const Json::Value& json);
};

/**
 * @brief Thread statistics and analytics
 */
struct ThreadAnalytics {
    std::string thread_id;
    std::chrono::system_clock::time_point period_start;
    std::chrono::system_clock::time_point period_end;
    
    // Message statistics
    uint32_t total_messages;
    uint32_t messages_per_hour;
    double average_message_length;
    uint32_t peak_concurrent_users;
    
    // User engagement
    uint32_t unique_participants;
    uint32_t active_participants;
    double participation_rate;
    std::unordered_map<std::string, uint32_t> user_message_counts;
    
    // Content analysis
    std::unordered_map<std::string, uint32_t> popular_reactions;
    std::vector<std::string> trending_topics;
    uint32_t media_shares;
    uint32_t link_shares;
    
    Json::Value to_json() const;
    void reset();
};

/**
 * @brief Thread search and discovery
 */
struct ThreadSearchQuery {
    std::string query_text;
    std::string chat_id;
    std::vector<std::string> tags;
    std::string category;
    ThreadStatus status;
    ThreadVisibility visibility;
    std::chrono::system_clock::time_point created_after;
    std::chrono::system_clock::time_point created_before;
    uint32_t min_participants;
    uint32_t max_participants;
    std::string creator_id;
    bool include_archived;
    
    // Pagination
    uint32_t limit;
    uint32_t offset;
    
    // Sorting
    enum class SortBy {
        CREATED_AT,
        UPDATED_AT,
        LAST_ACTIVITY,
        MESSAGE_COUNT,
        PARTICIPANT_COUNT,
        RELEVANCE
    };
    SortBy sort_by;
    bool ascending;
    
    Json::Value to_json() const;
    static ThreadSearchQuery from_json(const Json::Value& json);
};

/**
 * @brief High-performance message threading and reply system
 * 
 * Provides advanced threading capabilities including:
 * - Hierarchical message threading with unlimited depth
 * - Real-time thread participation and notifications
 * - Advanced role-based access control
 * - Thread analytics and engagement metrics
 * - Smart thread discovery and search
 * - Automatic thread management and archival
 */
class MessageThreadManager {
public:
    MessageThreadManager();
    ~MessageThreadManager();
    
    // Thread lifecycle management
    std::future<ThreadMetadata> create_thread(const std::string& chat_id,
                                             const std::string& parent_message_id,
                                             const std::string& creator_id,
                                             const std::string& title = "",
                                             const std::string& description = "");
    
    std::future<bool> update_thread(const std::string& thread_id,
                                   const ThreadMetadata& metadata);
    
    std::future<bool> archive_thread(const std::string& thread_id,
                                    const std::string& user_id);
    
    std::future<bool> delete_thread(const std::string& thread_id,
                                   const std::string& user_id);
    
    std::future<std::optional<ThreadMetadata>> get_thread(const std::string& thread_id);
    
    // Participant management
    std::future<bool> add_participant(const std::string& thread_id,
                                     const std::string& user_id,
                                     ParticipationLevel level = ParticipationLevel::PARTICIPANT);
    
    std::future<bool> remove_participant(const std::string& thread_id,
                                        const std::string& user_id,
                                        const std::string& remover_id);
    
    std::future<bool> update_participation_level(const std::string& thread_id,
                                                 const std::string& user_id,
                                                 ParticipationLevel new_level,
                                                 const std::string& updater_id);
    
    std::future<std::vector<ThreadParticipant>> get_participants(const std::string& thread_id);
    
    // Reply management
    std::future<MessageReply> create_reply(const std::string& parent_message_id,
                                          const std::string& replying_message_id,
                                          const std::string& user_id,
                                          const std::string& quoted_text = "");
    
    std::future<std::vector<MessageReply>> get_replies(const std::string& message_id);
    std::future<std::vector<MessageReply>> get_thread_replies(const std::string& thread_id);
    
    // Thread discovery and search
    std::future<std::vector<ThreadMetadata>> search_threads(const ThreadSearchQuery& query);
    std::future<std::vector<ThreadMetadata>> get_user_threads(const std::string& user_id,
                                                             bool include_archived = false);
    std::future<std::vector<ThreadMetadata>> get_chat_threads(const std::string& chat_id,
                                                             bool include_archived = false);
    
    // Analytics and insights
    std::future<ThreadAnalytics> get_thread_analytics(const std::string& thread_id,
                                                      const std::chrono::system_clock::time_point& start,
                                                      const std::chrono::system_clock::time_point& end);
    
    std::future<std::vector<ThreadMetadata>> get_trending_threads(const std::string& chat_id,
                                                                 uint32_t limit = 10);
    
    // Real-time notifications
    void subscribe_to_thread(const std::string& thread_id,
                           const std::string& user_id,
                           std::function<void(const Json::Value&)> callback);
    
    void unsubscribe_from_thread(const std::string& thread_id,
                                const std::string& user_id);
    
    // Thread permissions
    bool can_view_thread(const std::string& thread_id, const std::string& user_id);
    bool can_participate_in_thread(const std::string& thread_id, const std::string& user_id);
    bool can_moderate_thread(const std::string& thread_id, const std::string& user_id);
    
    // Read status management
    std::future<bool> mark_thread_read(const std::string& thread_id,
                                      const std::string& user_id,
                                      const std::string& last_message_id);
    
    std::future<uint32_t> get_unread_count(const std::string& thread_id,
                                          const std::string& user_id);
    
    // Configuration
    void set_auto_archive_enabled(bool enabled) { auto_archive_enabled_ = enabled; }
    void set_max_thread_depth(uint32_t depth) { max_thread_depth_ = depth; }
    void set_analytics_enabled(bool enabled) { analytics_enabled_ = enabled; }
    
    // Callbacks
    void set_thread_created_callback(std::function<void(const ThreadMetadata&)> callback);
    void set_thread_updated_callback(std::function<void(const ThreadMetadata&)> callback);
    void set_participant_joined_callback(std::function<void(const ThreadParticipant&)> callback);
    
private:
    // Storage
    std::unordered_map<std::string, std::shared_ptr<ThreadMetadata>> threads_;
    std::unordered_map<std::string, std::vector<ThreadParticipant>> thread_participants_;
    std::unordered_map<std::string, std::vector<MessageReply>> message_replies_;
    std::unordered_map<std::string, ThreadAnalytics> thread_analytics_;
    
    // Indexing for fast lookups
    std::unordered_map<std::string, std::unordered_set<std::string>> chat_threads_;
    std::unordered_map<std::string, std::unordered_set<std::string>> user_threads_;
    std::unordered_map<std::string, std::unordered_set<std::string>> parent_message_threads_;
    
    // Subscriptions
    std::unordered_map<std::string, std::unordered_map<std::string, std::function<void(const Json::Value&)>>> subscriptions_;
    
    // Configuration
    std::atomic<bool> auto_archive_enabled_;
    std::atomic<uint32_t> max_thread_depth_;
    std::atomic<bool> analytics_enabled_;
    
    // Thread safety
    mutable std::shared_mutex threads_mutex_;
    mutable std::shared_mutex participants_mutex_;
    mutable std::shared_mutex replies_mutex_;
    mutable std::shared_mutex subscriptions_mutex_;
    
    // Background processing
    std::thread analytics_thread_;
    std::thread cleanup_thread_;
    std::atomic<bool> background_running_;
    
    // Callbacks
    std::function<void(const ThreadMetadata&)> thread_created_callback_;
    std::function<void(const ThreadMetadata&)> thread_updated_callback_;
    std::function<void(const ThreadParticipant&)> participant_joined_callback_;
    
    // Internal methods
    std::string generate_thread_id();
    std::string generate_reply_id();
    void notify_thread_subscribers(const std::string& thread_id, const Json::Value& event);
    void update_thread_analytics(const std::string& thread_id, const std::string& event_type);
    void run_analytics_loop();
    void run_cleanup_loop();
    bool has_permission(const std::string& thread_id, const std::string& user_id, ParticipationLevel required_level);
    void archive_inactive_threads();
    void calculate_trending_scores();
    
    // Utility methods
    void log_info(const std::string& message);
    void log_warning(const std::string& message);
    void log_error(const std::string& message);
};

/**
 * @brief Thread event types for real-time notifications
 */
enum class ThreadEventType {
    THREAD_CREATED,
    THREAD_UPDATED,
    THREAD_ARCHIVED,
    THREAD_DELETED,
    PARTICIPANT_JOINED,
    PARTICIPANT_LEFT,
    PARTICIPANT_LEVEL_CHANGED,
    MESSAGE_REPLIED,
    THREAD_READ,
    THREAD_MENTION
};

/**
 * @brief Thread event for real-time notifications
 */
struct ThreadEvent {
    ThreadEventType type;
    std::string thread_id;
    std::string user_id;
    std::string target_user_id;
    Json::Value data;
    std::chrono::system_clock::time_point timestamp;
    std::string event_id;
    
    Json::Value to_json() const;
    static ThreadEvent from_json(const Json::Value& json);
};

/**
 * @brief Thread utilities and helpers
 */
class ThreadUtils {
public:
    // Thread hierarchy utilities
    static std::vector<std::string> get_thread_hierarchy(const std::string& thread_id,
                                                         const std::unordered_map<std::string, MessageReply>& replies);
    
    static uint32_t calculate_thread_depth(const std::string& message_id,
                                          const std::unordered_map<std::string, MessageReply>& replies);
    
    // Search utilities
    static bool matches_search_query(const ThreadMetadata& thread, const ThreadSearchQuery& query);
    static double calculate_relevance_score(const ThreadMetadata& thread, const std::string& query);
    
    // Analytics utilities
    static double calculate_engagement_score(const ThreadAnalytics& analytics);
    static std::vector<std::string> extract_trending_topics(const std::vector<std::string>& messages);
    
    // Validation
    static bool validate_thread_title(const std::string& title);
    static bool validate_thread_description(const std::string& description);
    static bool validate_participation_level(ParticipationLevel level, ParticipationLevel required);
};

} // namespace sonet::messaging::threading
