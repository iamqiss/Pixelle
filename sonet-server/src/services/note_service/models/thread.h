/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/note.h"
#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <ctime>
#include <nlohmann/json.hpp>

namespace sonet::note::models {

/**
 * Thread model for managing Twitter-style note threads
 * 
 * I'm building this to handle thread conversations properly - you know how 
 * Twitter threads work where people chain multiple notes together? That's 
 * exactly what this does. Need to track the order, who can reply, all that stuff.
 */
class Thread {
public:
    // Core identification
    std::string thread_id;
    std::string starter_note_id;        // The original note that started the thread
    std::string author_id;              // Who started the thread
    std::string author_username;
    
    // Thread metadata
    std::string title;                  // Optional thread title
    std::string description;            // Brief description of the thread
    std::vector<std::string> tags;      // Thread tags for categorization
    
    // Thread structure
    std::vector<std::string> note_ids;  // All notes in order
    int total_notes = 0;                // Total number of notes in thread
    int max_depth = 0;                  // Maximum reply depth
    
    // Thread state
    bool is_locked = false;             // Can new notes be added?
    bool is_pinned = false;             // Pinned to author's profile?
    bool is_published = true;           // Is thread visible?
    bool allow_replies = true;          // Can others reply to thread notes?
    bool allow_renotes = true;          // Can thread notes be renoted?
    
    // Engagement metrics
    int total_likes = 0;
    int total_renotes = 0;
    int total_replies = 0;
    int total_views = 0;
    int total_bookmarks = 0;
    int unique_participants = 0;        // How many different users participated
    
    // Timestamps
    std::time_t created_at;
    std::time_t updated_at;
    std::optional<std::time_t> last_activity_at;
    std::optional<std::time_t> completed_at;  // When thread was marked complete
    
    // Visibility and moderation
    NoteVisibility visibility = NoteVisibility::PUBLIC;
    std::vector<std::string> moderator_ids;    // Who can moderate this thread
    std::vector<std::string> blocked_user_ids; // Users blocked from participating
    
    // Analytics
    std::map<std::string, int> daily_metrics;     // Date -> engagement
    std::vector<std::string> trending_keywords;   // What's trending in this thread
    double engagement_rate = 0.0;
    double completion_rate = 0.0;                 // How many people read to the end
    
    // Constructors
    Thread() = default;
    Thread(const std::string& starter_note_id, const std::string& author_id);
    Thread(const std::string& starter_note_id, const std::string& author_id, const std::string& title);
    
    // Core thread operations
    bool add_note(const std::string& note_id, int position = -1);
    bool remove_note(const std::string& note_id);
    bool reorder_note(const std::string& note_id, int new_position);
    std::vector<std::string> get_notes_in_order() const;
    
    // Thread management
    void lock_thread();
    void unlock_thread();
    void pin_thread();
    void unpin_thread();
    void complete_thread();
    void reopen_thread();
    
    // Moderation
    void add_moderator(const std::string& user_id);
    void remove_moderator(const std::string& user_id);
    void block_user(const std::string& user_id);
    void unblock_user(const std::string& user_id);
    bool is_user_blocked(const std::string& user_id) const;
    bool can_user_moderate(const std::string& user_id) const;
    
    // Permissions
    bool can_user_add_note(const std::string& user_id) const;
    bool can_user_reply_to_thread(const std::string& user_id) const;
    bool can_user_view_thread(const std::string& user_id) const;
    
    // Engagement tracking
    void update_engagement_metrics();
    void record_view(const std::string& user_id);
    void calculate_completion_rate();
    
    // Analytics
    std::vector<std::string> get_participant_ids() const;
    int get_note_position(const std::string& note_id) const;
    std::string get_next_note_id(const std::string& current_note_id) const;
    std::string get_previous_note_id(const std::string& current_note_id) const;
    
    // Content analysis
    std::vector<std::string> extract_thread_hashtags() const;
    std::vector<std::string> extract_thread_mentions() const;
    std::string generate_thread_summary() const;
    
    // Validation
    bool is_valid() const;
    std::vector<std::string> get_validation_errors() const;
    
    // Serialization
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& j);
    std::string to_string() const;
    
    // Display helpers
    std::string get_display_title() const;
    std::string get_thread_preview() const;
    std::string get_formatted_timestamp() const;
    std::string get_relative_timestamp() const;
    
    // Utility
    bool is_empty() const;
    bool is_single_note() const;
    bool is_completed() const;
    std::time_t get_age_seconds() const;
    std::time_t get_age_hours() const;
    bool is_recent(int hours) const;
    
    // Comparison operators
    bool operator==(const Thread& other) const;
    bool operator!=(const Thread& other) const;
    bool operator<(const Thread& other) const;

private:
    // Helper methods
    void initialize_defaults();
    void update_timestamps();
    void update_thread_metrics();
    void validate_note_position(int position) const;
    std::string generate_thread_id();
};

/**
 * Thread statistics for analytics
 */
struct ThreadStatistics {
    std::string thread_id;
    std::time_t calculated_at;
    
    // Basic metrics
    int total_notes = 0;
    int total_participants = 0;
    int total_views = 0;
    int total_engagement = 0;
    
    // Timing metrics
    double average_time_between_notes = 0.0;  // In minutes
    double total_thread_duration = 0.0;       // In hours
    
    // Engagement metrics
    double engagement_rate = 0.0;
    double completion_rate = 0.0;
    double bounce_rate = 0.0;                 // People who only read first note
    
    // Content metrics
    int average_note_length = 0;
    int total_hashtags = 0;
    int total_mentions = 0;
    int total_urls = 0;
    
    // Quality metrics
    double spam_score = 0.0;
    double toxicity_score = 0.0;
    double readability_score = 0.0;
    
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& j);
};

/**
 * Thread participant info
 */
struct ThreadParticipant {
    std::string user_id;
    std::string username;
    int notes_contributed = 0;
    int total_likes_received = 0;
    int total_replies_received = 0;
    std::time_t first_participation;
    std::time_t last_participation;
    bool is_moderator = false;
    bool is_blocked = false;
    
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& j);
};

// Utility functions
std::string thread_visibility_to_string(NoteVisibility visibility);
NoteVisibility string_to_thread_visibility(const std::string& visibility_str);

// JSON conversion helpers
void to_json(nlohmann::json& j, const Thread& thread);
void from_json(const nlohmann::json& j, Thread& thread);
void to_json(nlohmann::json& j, const ThreadStatistics& stats);
void from_json(const nlohmann::json& j, ThreadStatistics& stats);
void to_json(nlohmann::json& j, const ThreadParticipant& participant);
void from_json(const nlohmann::json& j, ThreadParticipant& participant);

} // namespace sonet::note::models
