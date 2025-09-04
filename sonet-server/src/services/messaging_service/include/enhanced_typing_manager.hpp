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
#include <thread>
#include <queue>
#include <json/json.h>

namespace sonet::messaging::typing {

/**
 * @brief Enhanced typing activity types
 */
enum class TypingActivity {
    TYPING = 0,          // User is typing text
    RECORDING_AUDIO = 1, // User is recording voice message
    RECORDING_VIDEO = 2, // User is recording video
    UPLOADING_FILE = 3,  // User is uploading file/attachment
    THINKING = 4,        // User is composing (long pause)
    EDITING = 5          // User is editing existing message
};

/**
 * @brief Typing context for different conversation areas
 */
enum class TypingContext {
    MAIN_CHAT = 0,       // Main chat conversation
    THREAD = 1,          // In a specific thread
    REPLY = 2,           // Replying to specific message
    DIRECT_MESSAGE = 3   // Private direct message
};

/**
 * @brief Enhanced typing indicator with rich metadata
 */
struct EnhancedTypingIndicator {
    std::string typing_id;
    std::string user_id;
    std::string chat_id;
    std::string thread_id;         // Optional: for thread typing
    std::string reply_to_message_id; // Optional: for reply typing
    TypingActivity activity;
    TypingContext context;
    std::chrono::system_clock::time_point started_at;
    std::chrono::system_clock::time_point last_update;
    std::chrono::system_clock::time_point expires_at;
    
    // Rich metadata
    std::string device_type;       // mobile, desktop, tablet
    std::string platform;         // ios, android, web, etc.
    bool is_dictating;           // Voice-to-text
    uint32_t estimated_length;   // Estimated message length
    double typing_speed_wpm;     // Words per minute
    bool is_draft_saved;         // Has auto-saved draft
    
    // Location context
    bool in_foreground;          // App is in foreground
    bool has_focus;             // Input has focus
    bool is_mobile_keyboard;    // Using mobile keyboard
    
    Json::Value to_json() const;
    static EnhancedTypingIndicator from_json(const Json::Value& json);
    bool is_expired() const;
    std::chrono::milliseconds time_since_start() const;
    std::chrono::milliseconds time_since_update() const;
};

/**
 * @brief Typing pattern analytics
 */
struct TypingPatterns {
    std::string user_id;
    std::chrono::system_clock::time_point analysis_period_start;
    std::chrono::system_clock::time_point analysis_period_end;
    
    // Speed metrics
    double average_typing_speed_wpm;
    double peak_typing_speed_wpm;
    double typing_consistency_score; // 0.0 to 1.0
    
    // Behavioral patterns
    std::chrono::milliseconds average_thinking_pause;
    std::chrono::milliseconds longest_thinking_pause;
    uint32_t backspace_frequency;
    uint32_t autocorrect_usage;
    
    // Activity distribution
    std::unordered_map<TypingActivity, uint32_t> activity_counts;
    std::unordered_map<std::string, uint32_t> device_usage; // device -> count
    std::unordered_map<int, uint32_t> hourly_activity; // hour -> count
    
    // Message characteristics
    double average_message_length;
    uint32_t draft_save_frequency;
    double completion_rate; // messages sent vs typing sessions
    
    Json::Value to_json() const;
    void reset();
};

/**
 * @brief Typing session for comprehensive tracking
 */
struct TypingSession {
    std::string session_id;
    std::string user_id;
    std::string chat_id;
    std::string thread_id;
    std::chrono::system_clock::time_point started_at;
    std::chrono::system_clock::time_point ended_at;
    bool completed_message; // Whether session resulted in sent message
    
    // Session metrics
    std::chrono::milliseconds total_typing_time;
    std::chrono::milliseconds total_pause_time;
    uint32_t keystroke_count;
    uint32_t backspace_count;
    uint32_t word_count;
    uint32_t character_count;
    
    // Activity timeline
    std::vector<std::pair<std::chrono::system_clock::time_point, TypingActivity>> activity_timeline;
    
    // Context switches
    uint32_t focus_changes;
    uint32_t app_switches;
    
    Json::Value to_json() const;
    void add_activity(TypingActivity activity);
    std::chrono::milliseconds get_session_duration() const;
    double calculate_effective_typing_speed() const;
};

/**
 * @brief Real-time typing aggregation for chat
 */
struct ChatTypingState {
    std::string chat_id;
    std::unordered_map<std::string, EnhancedTypingIndicator> active_typers;
    std::unordered_map<TypingActivity, std::unordered_set<std::string>> activity_groups;
    std::chrono::system_clock::time_point last_update;
    
    // Aggregated metrics
    uint32_t total_active_typers;
    uint32_t typing_text_count;
    uint32_t recording_audio_count;
    uint32_t uploading_file_count;
    
    Json::Value to_json() const;
    void add_typer(const EnhancedTypingIndicator& indicator);
    void remove_typer(const std::string& user_id);
    void cleanup_expired(const std::chrono::system_clock::time_point& now);
    bool has_activity() const;
};

/**
 * @brief Typing notification preferences
 */
struct TypingNotificationConfig {
    std::string user_id;
    bool enabled;
    bool show_detailed_activity; // Show what type of activity
    bool show_typing_speed;      // Show fast/slow typing indicators
    bool show_device_type;       // Show device context
    bool group_similar_activities; // Group multiple typers
    std::chrono::milliseconds notification_delay; // Delay before showing
    std::chrono::milliseconds min_duration; // Minimum display time
    
    // Activity filters
    std::unordered_set<TypingActivity> visible_activities;
    
    Json::Value to_json() const;
    static TypingNotificationConfig from_json(const Json::Value& json);
    static TypingNotificationConfig default_config();
};

/**
 * @brief Advanced typing manager with rich real-time indicators
 * 
 * Provides sophisticated typing awareness including:
 * - Multi-modal activity detection (typing, voice, file upload)
 * - Context-aware indicators (threads, replies, main chat)
 * - Typing pattern analytics and user behavior insights
 * - Smart notification aggregation and filtering
 * - Cross-platform typing synchronization
 * - Performance-optimized real-time updates
 */
class EnhancedTypingManager {
public:
    EnhancedTypingManager();
    ~EnhancedTypingManager();
    
    // Typing activity management
    std::future<bool> start_typing(const std::string& user_id,
                                  const std::string& chat_id,
                                  TypingActivity activity = TypingActivity::TYPING,
                                  TypingContext context = TypingContext::MAIN_CHAT,
                                  const std::string& thread_id = "",
                                  const std::string& reply_to_message_id = "");
    
    std::future<bool> update_typing(const std::string& user_id,
                                   const std::string& chat_id,
                                   TypingActivity activity,
                                   uint32_t estimated_length = 0,
                                   double typing_speed = 0.0);
    
    std::future<bool> stop_typing(const std::string& user_id,
                                 const std::string& chat_id,
                                 bool message_sent = false);
    
    std::future<bool> pause_typing(const std::string& user_id,
                                  const std::string& chat_id,
                                  std::chrono::milliseconds pause_duration);
    
    // Context-specific typing
    std::future<bool> start_thread_typing(const std::string& user_id,
                                         const std::string& chat_id,
                                         const std::string& thread_id,
                                         TypingActivity activity = TypingActivity::TYPING);
    
    std::future<bool> start_reply_typing(const std::string& user_id,
                                        const std::string& chat_id,
                                        const std::string& reply_to_message_id,
                                        TypingActivity activity = TypingActivity::TYPING);
    
    // Querying typing state
    std::future<std::vector<EnhancedTypingIndicator>> get_chat_typers(const std::string& chat_id);
    std::future<std::vector<EnhancedTypingIndicator>> get_thread_typers(const std::string& thread_id);
    std::future<ChatTypingState> get_chat_typing_state(const std::string& chat_id);
    std::future<std::optional<EnhancedTypingIndicator>> get_user_typing_state(const std::string& user_id,
                                                                             const std::string& chat_id);
    
    // Session management
    std::future<TypingSession> start_typing_session(const std::string& user_id,
                                                    const std::string& chat_id,
                                                    const std::string& thread_id = "");
    
    std::future<bool> end_typing_session(const std::string& session_id,
                                        bool message_completed = false);
    
    std::future<TypingSession> get_typing_session(const std::string& session_id);
    
    // Analytics and insights
    std::future<TypingPatterns> get_user_typing_patterns(const std::string& user_id,
                                                        const std::chrono::system_clock::time_point& start,
                                                        const std::chrono::system_clock::time_point& end);
    
    std::future<std::vector<TypingPatterns>> get_chat_typing_analytics(const std::string& chat_id,
                                                                      const std::chrono::system_clock::time_point& start,
                                                                      const std::chrono::system_clock::time_point& end);
    
    // Real-time subscriptions
    void subscribe_to_chat_typing(const std::string& chat_id,
                                 const std::string& subscriber_id,
                                 std::function<void(const ChatTypingState&)> callback);
    
    void subscribe_to_user_typing(const std::string& user_id,
                                 const std::string& subscriber_id,
                                 std::function<void(const EnhancedTypingIndicator&)> callback);
    
    void unsubscribe_from_chat_typing(const std::string& chat_id,
                                     const std::string& subscriber_id);
    
    void unsubscribe_from_user_typing(const std::string& user_id,
                                     const std::string& subscriber_id);
    
    // Configuration management
    std::future<bool> set_user_notification_config(const std::string& user_id,
                                                   const TypingNotificationConfig& config);
    
    std::future<TypingNotificationConfig> get_user_notification_config(const std::string& user_id);
    
    // Device and platform context
    std::future<bool> set_device_context(const std::string& user_id,
                                        const std::string& device_type,
                                        const std::string& platform,
                                        bool is_mobile_keyboard = false);
    
    std::future<bool> set_app_context(const std::string& user_id,
                                     bool in_foreground,
                                     bool has_focus);
    
    // Advanced features
    std::future<bool> save_typing_draft(const std::string& user_id,
                                       const std::string& chat_id,
                                       const std::string& draft_content,
                                       const std::string& thread_id = "");
    
    std::future<std::string> get_typing_draft(const std::string& user_id,
                                             const std::string& chat_id,
                                             const std::string& thread_id = "");
    
    std::future<bool> clear_typing_draft(const std::string& user_id,
                                        const std::string& chat_id,
                                        const std::string& thread_id = "");
    
    // Cleanup and maintenance
    void cleanup_expired_indicators();
    void cleanup_old_sessions();
    void optimize_storage();
    
    // Configuration
    void set_default_timeout(std::chrono::milliseconds timeout) { default_timeout_ = timeout; }
    void set_analytics_enabled(bool enabled) { analytics_enabled_ = enabled; }
    void set_draft_auto_save(bool enabled) { draft_auto_save_ = enabled; }
    
    // Callbacks
    void set_typing_started_callback(std::function<void(const EnhancedTypingIndicator&)> callback);
    void set_typing_stopped_callback(std::function<void(const std::string&, const std::string&)> callback);
    void set_activity_changed_callback(std::function<void(const EnhancedTypingIndicator&)> callback);
    
private:
    // Storage
    std::unordered_map<std::string, std::unordered_map<std::string, EnhancedTypingIndicator>> chat_typers_;
    std::unordered_map<std::string, ChatTypingState> chat_states_;
    std::unordered_map<std::string, TypingSession> active_sessions_;
    std::unordered_map<std::string, TypingPatterns> user_patterns_;
    std::unordered_map<std::string, TypingNotificationConfig> notification_configs_;
    
    // Drafts storage
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> user_drafts_; // user_id -> chat_id -> draft
    
    // Subscriptions
    std::unordered_map<std::string, std::unordered_map<std::string, std::function<void(const ChatTypingState&)>>> chat_subscriptions_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::function<void(const EnhancedTypingIndicator&)>>> user_subscriptions_;
    
    // Configuration
    std::atomic<std::chrono::milliseconds> default_timeout_;
    std::atomic<bool> analytics_enabled_;
    std::atomic<bool> draft_auto_save_;
    
    // Thread safety
    mutable std::shared_mutex typers_mutex_;
    mutable std::shared_mutex sessions_mutex_;
    mutable std::shared_mutex patterns_mutex_;
    mutable std::shared_mutex subscriptions_mutex_;
    mutable std::shared_mutex drafts_mutex_;
    
    // Background processing
    std::thread cleanup_thread_;
    std::thread analytics_thread_;
    std::atomic<bool> background_running_;
    
    // Callbacks
    std::function<void(const EnhancedTypingIndicator&)> typing_started_callback_;
    std::function<void(const std::string&, const std::string&)> typing_stopped_callback_;
    std::function<void(const EnhancedTypingIndicator&)> activity_changed_callback_;
    
    // Internal methods
    std::string generate_typing_id();
    std::string generate_session_id();
    void notify_chat_subscribers(const std::string& chat_id, const ChatTypingState& state);
    void notify_user_subscribers(const std::string& user_id, const EnhancedTypingIndicator& indicator);
    void update_typing_patterns(const TypingSession& session);
    void run_cleanup_loop();
    void run_analytics_loop();
    bool should_notify_subscriber(const std::string& subscriber_id,
                                 const EnhancedTypingIndicator& indicator);
    
    // Utility methods
    void log_info(const std::string& message);
    void log_warning(const std::string& message);
    void log_error(const std::string& message);
};

/**
 * @brief Typing event types for notifications
 */
enum class TypingEventType {
    TYPING_STARTED,
    TYPING_UPDATED,
    TYPING_STOPPED,
    ACTIVITY_CHANGED,
    SESSION_STARTED,
    SESSION_ENDED,
    DRAFT_SAVED,
    DRAFT_LOADED
};

/**
 * @brief Typing event for real-time notifications
 */
struct TypingEvent {
    TypingEventType type;
    std::string user_id;
    std::string chat_id;
    std::string thread_id;
    TypingActivity activity;
    Json::Value data;
    std::chrono::system_clock::time_point timestamp;
    std::string event_id;
    
    Json::Value to_json() const;
    static TypingEvent from_json(const Json::Value& json);
};

/**
 * @brief Typing utilities and helpers
 */
class TypingUtils {
public:
    // Activity detection
    static TypingActivity detect_activity_from_input(const std::string& input_type);
    static bool is_voice_activity(TypingActivity activity);
    static bool is_file_activity(TypingActivity activity);
    
    // Speed calculations
    static double calculate_typing_speed_wpm(uint32_t characters, 
                                           std::chrono::milliseconds duration);
    static std::string get_speed_description(double wpm);
    
    // Pattern analysis
    static double calculate_consistency_score(const std::vector<double>& speeds);
    static std::chrono::milliseconds detect_thinking_pause(const std::vector<std::chrono::system_clock::time_point>& timestamps);
    
    // Notification formatting
    static std::string format_typing_notification(const std::vector<EnhancedTypingIndicator>& indicators,
                                                 const TypingNotificationConfig& config);
    static std::string get_activity_description(TypingActivity activity);
    static std::string get_device_icon(const std::string& device_type);
    
    // Validation
    static bool validate_typing_context(TypingContext context, const std::string& thread_id, const std::string& reply_id);
    static bool is_reasonable_typing_speed(double wpm);
    
    // Aggregation
    static ChatTypingState aggregate_chat_typing(const std::vector<EnhancedTypingIndicator>& indicators);
    static std::vector<std::string> group_similar_typers(const std::vector<EnhancedTypingIndicator>& indicators);
};

} // namespace sonet::messaging::typing