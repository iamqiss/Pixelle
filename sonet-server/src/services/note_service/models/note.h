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
#include <map>
#include <optional>
#include <ctime>
#include <memory>
#include <nlohmann/json.hpp>
#include "attachment.h"

namespace sonet::note::models {

// Note visibility settings
enum class NoteVisibility {
    PUBLIC = 0,          // Visible to everyone
    FOLLOWERS_ONLY = 1,  // Only followers can see
    MENTIONED_ONLY = 2,  // Only mentioned users can see
    PRIVATE = 3,         // Only author can see (draft)
    CIRCLE = 4           // Only close friends/circle
};

// Note type classifications
enum class NoteType {
    ORIGINAL = 0,        // Original note
    REPLY = 1,           // Reply to another note
    RENOST = 2,          // Renote of another note
    QUOTE = 3,           // Quote with additional content
    THREAD = 4           // Part of a thread
};

// Content warning types
enum class ContentWarning {
    NONE = 0,
    SENSITIVE = 1,       // Sensitive content
    VIOLENCE = 2,        // Violence/graphic content
    ADULT = 3,           // Adult content
    SPOILER = 4,         // Contains spoilers
    HARASSMENT = 5       // Potential harassment
};

// Note status
enum class NoteStatus {
    ACTIVE = 0,          // Normal active note
    DELETED = 1,         // Soft deleted
    HIDDEN = 2,          // Hidden by moderators
    FLAGGED = 3,         // Flagged for review
    DRAFT = 4,           // Saved as draft
    SCHEDULED = 5        // Scheduled for future noteing
};

// Validation error types
enum class NoteValidationError {
    CONTENT_TOO_LONG,
    CONTENT_EMPTY,
    INVALID_MENTIONS,
    INVALID_HASHTAGS,
    INVALID_VISIBILITY,
    INVALID_REPLY_TARGET,
    INVALID_RENOTE_TARGET,
    TOO_MANY_ATTACHMENTS,
    ATTACHMENT_TOO_LARGE,
    ATTACHMENT_INVALID_FORMAT,
    ATTACHMENT_PROCESSING_FAILED,
    ATTACHMENT_CONTAINS_VIRUS,
    ATTACHMENT_POLICY_VIOLATION,
    MIXED_ATTACHMENT_TYPES,
    TENOR_GIF_INVALID,
    POLL_INVALID_OPTIONS,
    LOCATION_INVALID_COORDINATES,
    LINK_PREVIEW_FAILED,
    INVALID_SCHEDULED_TIME,
    SPAM_DETECTED,
    PROFANITY_DETECTED
};

// Forward declarations
class NoteAttachment;
class NoteMention;
class NoteHashtag;
class NoteMetrics;

/**
 * Core Note model representing a Twitter-style tweet/note
 * Maximum content length: 300 characters
 */
class Note {
public:
    // Core identification
    std::string note_id;
    std::string author_id;
    std::string author_username;
    
    // Content
    std::string content;                    // Max 300 characters
    std::string raw_content;                // Original unprocessed content
    std::string processed_content;          // Processed with links, mentions, etc.
    
    // Note relationships
    std::optional<std::string> reply_to_id;      // ID of note being replied to
    std::optional<std::string> reply_to_user_id; // User being replied to
    std::optional<std::string> renote_of_id;     // ID of note being renoted
    std::optional<std::string> quote_of_id;      // ID of note being quoted
    std::optional<std::string> thread_id;        // Thread this note belongs to
    int thread_position = 0;                     // Position in thread (0 = first)
    
    // Classification
    NoteType type = NoteType::ORIGINAL;
    NoteVisibility visibility = NoteVisibility::PUBLIC;
    NoteStatus status = NoteStatus::ACTIVE;
    ContentWarning content_warning = ContentWarning::NONE;
    
    // Content features
    std::vector<std::string> mentioned_user_ids;
    std::vector<std::string> mentioned_usernames;
    std::vector<std::string> hashtags;
    std::vector<std::string> urls;
    std::vector<std::string> attachment_ids;
    AttachmentCollection attachments;        // Media attachments (images, videos, GIFs, etc.)
    
    // Engagement metrics
    int like_count = 0;
    int renote_count = 0;
    int reply_count = 0;
    int quote_count = 0;
    int view_count = 0;
    int bookmark_count = 0;
    
    // Geographic data
    std::optional<double> latitude;
    std::optional<double> longitude;
    std::string location_name;
    
    // Content moderation
    bool is_sensitive = false;
    bool is_nsfw = false;
    bool contains_spoilers = false;
    double spam_score = 0.0;          // 0.0 = not spam, 1.0 = definitely spam
    double toxicity_score = 0.0;      // AI toxicity detection score
    std::vector<std::string> detected_languages;
    
    // Timestamps
    std::time_t created_at;
    std::time_t updated_at;
    std::optional<std::time_t> scheduled_at;    // For scheduled notes
    std::optional<std::time_t> deleted_at;      // Soft delete timestamp
    
    // Client information
    std::string client_name;           // App/client used to note
    std::string client_version;
    std::string user_agent;
    std::string ip_address;
    
    // Engagement tracking
    std::vector<std::string> liked_by_user_ids;      // Recent likes (limited)
    std::vector<std::string> renoted_by_user_ids;    // Recent renotes (limited)
    std::map<std::string, std::time_t> user_interactions; // User ID -> last interaction time
    
    // Analytics
    std::map<std::string, int> daily_metrics;        // Date -> engagement count
    std::map<std::string, int> hourly_metrics;       // Hour -> view count
    std::vector<std::string> trending_countries;     // Countries where trending
    
    // Additional metadata
    std::map<std::string, std::string> metadata;     // Flexible key-value pairs
    bool is_promoted = false;                        // Promoted/sponsored content
    bool is_verified_author = false;                 // Author has verified badge
    bool allow_replies = true;
    bool allow_renotes = true;
    bool allow_quotes = true;
    
    // Constants
    static constexpr size_t MAX_CONTENT_LENGTH = 300;
    static constexpr size_t MAX_ATTACHMENTS = 4;
    static constexpr size_t MAX_MENTIONS = 10;
    static constexpr size_t MAX_HASHTAGS = 10;
    static constexpr size_t MAX_URLS = 5;

public:
    // Constructors
    Note() = default;
    Note(const std::string& author_id, const std::string& content);
    Note(const std::string& author_id, const std::string& content, NoteType type);
    
    // Validation
    bool is_valid() const;
    std::vector<NoteValidationError> get_validation_errors() const;
    bool validate_content() const;
    bool validate_mentions() const;
    bool validate_hashtags() const;
    bool validate_attachments() const;
    bool validate_scheduled_time() const;
    
    // Content processing
    void process_content();
    void extract_mentions();
    void extract_hashtags();
    void extract_urls();
    void detect_language();
    void calculate_spam_score();
    void calculate_toxicity_score();
    
    // Content manipulation
    bool set_content(const std::string& new_content);
    void add_mention(const std::string& user_id, const std::string& username);
    void add_hashtag(const std::string& hashtag);
    void add_attachment(const std::string& attachment_id);
    void remove_attachment(const std::string& attachment_id);
    
    // Attachment management
    bool add_media_attachment(const Attachment& attachment);
    bool remove_media_attachment(const std::string& attachment_id);
    void clear_attachments();
    bool has_attachments() const;
    size_t get_attachment_count() const;
    std::vector<Attachment> get_attachments_by_type(AttachmentType type) const;
    std::vector<Attachment> get_image_attachments() const;
    std::vector<Attachment> get_video_attachments() const;
    std::vector<Attachment> get_gif_attachments() const;
    bool has_sensitive_attachments() const;
    bool has_processing_attachments() const;
    size_t get_total_attachment_size() const;
    std::optional<Attachment> get_primary_attachment() const;
    std::string get_primary_attachment_url() const;
    std::string get_attachment_summary() const; // e.g., "3 images, 1 video"
    void mark_all_attachments_sensitive(bool sensitive = true);
    bool validate_attachments() const;
    
    // Relationships
    void set_reply_target(const std::string& note_id, const std::string& user_id);
    void set_renote_target(const std::string& note_id);
    void set_quote_target(const std::string& note_id);
    void set_thread_info(const std::string& thread_id, int position);
    
    // Engagement
    void increment_likes();
    void decrement_likes();
    void increment_renotes();
    void decrement_renotes();
    void increment_replies();
    void increment_quotes();
    void increment_views();
    void increment_bookmarks();
    void record_user_interaction(const std::string& user_id, const std::string& interaction_type);
    
    // Metrics and analytics
    void update_daily_metrics(const std::string& date, int value);
    void update_hourly_metrics(const std::string& hour, int value);
    void add_trending_country(const std::string& country);
    double calculate_engagement_rate() const;
    double calculate_virality_score() const;
    int get_total_engagement() const;
    
    // Geographic
    void set_location(double lat, double lng, const std::string& name = "");
    void clear_location();
    bool has_location() const;
    
    // Moderation
    void mark_sensitive(bool sensitive = true);
    void mark_nsfw(bool nsfw = true);
    void mark_spoilers(bool spoilers = true);
    void set_content_warning(ContentWarning warning);
    void flag_for_review();
    void hide_note();
    void soft_delete();
    void restore_note();
    
    // Scheduling
    void schedule_note(std::time_t scheduled_time);
    void unschedule_note();
    bool is_scheduled() const;
    bool should_be_published() const;
    
    // Privacy and permissions
    void set_visibility(NoteVisibility visibility);
    bool is_visible_to_user(const std::string& user_id, const std::vector<std::string>& following_ids = {},
                           const std::vector<std::string>& circle_ids = {}) const;
    bool can_user_reply(const std::string& user_id) const;
    bool can_user_renote(const std::string& user_id) const;
    bool can_user_quote(const std::string& user_id) const;
    
    // Thread management
    bool is_part_of_thread() const;
    bool is_thread_starter() const;
    bool can_add_to_thread(const std::string& user_id) const;
    
    // Serialization
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& json);
    std::string to_string() const;
    
    // Display helpers
    std::string get_display_content() const;
    std::string get_preview_text(size_t max_length = 100) const;
    std::string get_formatted_timestamp() const;
    std::string get_relative_timestamp() const;
    std::string get_engagement_summary() const;
    
    // Comparison and sorting
    bool operator==(const Note& other) const;
    bool operator!=(const Note& other) const;
    bool operator<(const Note& other) const;  // For sorting by timestamp
    
    // Utility functions
    size_t get_content_length() const;
    size_t get_remaining_characters() const;
    bool is_empty() const;
    bool is_deleted() const;
    bool is_draft() const;
    bool is_public() const;
    bool has_attachments() const;
    bool has_mentions() const;
    bool has_hashtags() const;
    bool has_urls() const;
    bool is_reply() const;
    bool is_renote() const;
    bool is_quote() const;
    bool is_original() const;
    
    // Content analysis
    double get_readability_score() const;
    int count_words() const;
    int count_sentences() const;
    std::vector<std::string> get_keywords() const;
    
    // Age and freshness
    std::time_t get_age_seconds() const;
    std::time_t get_age_minutes() const;
    std::time_t get_age_hours() const;
    std::time_t get_age_days() const;
    bool is_fresh(int minutes = 5) const;
    bool is_recent(int hours = 24) const;
    
    // Statistical helpers
    double get_likes_per_hour() const;
    double get_renotes_per_hour() const;
    double get_replies_per_hour() const;
    double get_engagement_velocity() const;
    
private:
    // Internal helpers
    void initialize_defaults();
    void update_timestamps();
    std::string sanitize_content(const std::string& content) const;
    std::vector<std::string> extract_tokens(const std::string& content, const std::string& prefix) const;
    bool is_valid_mention(const std::string& mention) const;
    bool is_valid_hashtag(const std::string& hashtag) const;
    bool is_valid_url(const std::string& url) const;
    double calculate_content_quality_score() const;
    
    // Content processing helpers
    std::string process_mentions(const std::string& content) const;
    std::string process_hashtags(const std::string& content) const;
    std::string process_urls(const std::string& content) const;
    std::string highlight_content_features(const std::string& content) const;
};

// Supporting structures for complex data
struct NoteMention {
    std::string user_id;
    std::string username;
    size_t start_position;
    size_t end_position;
    bool is_verified = false;
    std::time_t mentioned_at;
    
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& json);
};

struct NoteHashtag {
    std::string tag;
    size_t start_position;
    size_t end_position;
    int trending_rank = 0;
    std::time_t first_used;
    
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& json);
};

struct NoteUrl {
    std::string original_url;
    std::string shortened_url;
    std::string expanded_url;
    std::string title;
    std::string description;
    std::string image_url;
    size_t start_position;
    size_t end_position;
    bool is_secure = false;
    std::time_t last_checked;
    
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& json);
};

struct NoteMetrics {
    std::string note_id;
    std::time_t calculated_at;
    
    // Engagement metrics
    int total_likes = 0;
    int total_renotes = 0;
    int total_replies = 0;
    int total_quotes = 0;
    int total_views = 0;
    int total_bookmarks = 0;
    int total_shares = 0;
    
    // Reach metrics
    int unique_viewers = 0;
    int follower_views = 0;
    int non_follower_views = 0;
    
    // Time-based metrics
    std::map<std::string, int> hourly_engagement;
    std::map<std::string, int> daily_engagement;
    
    // Geographic metrics
    std::map<std::string, int> country_views;
    std::map<std::string, int> city_views;
    
    // Demographic metrics
    std::map<std::string, int> age_group_views;
    std::map<std::string, int> gender_views;
    
    // Calculated scores
    double engagement_rate = 0.0;
    double virality_score = 0.0;
    double reach_score = 0.0;
    double quality_score = 0.0;
    
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& json);
};

// Utility functions
std::string note_type_to_string(NoteType type);
NoteType string_to_note_type(const std::string& type_str);
std::string note_visibility_to_string(NoteVisibility visibility);
NoteVisibility string_to_note_visibility(const std::string& visibility_str);
std::string note_status_to_string(NoteStatus status);
NoteStatus string_to_note_status(const std::string& status_str);
std::string content_warning_to_string(ContentWarning warning);
ContentWarning string_to_content_warning(const std::string& warning_str);

// JSON conversion helpers
void to_json(nlohmann::json& j, const Note& note);
void from_json(const nlohmann::json& j, Note& note);
void to_json(nlohmann::json& j, const NoteMention& mention);
void from_json(const nlohmann::json& j, NoteMention& mention);
void to_json(nlohmann::json& j, const NoteHashtag& hashtag);
void from_json(const nlohmann::json& j, NoteHashtag& hashtag);
void to_json(nlohmann::json& j, const NoteUrl& url);
void from_json(const nlohmann::json& j, NoteUrl& url);
void to_json(nlohmann::json& j, const NoteMetrics& metrics);
void from_json(const nlohmann::json& j, NoteMetrics& metrics);

} // namespace sonet::note::models