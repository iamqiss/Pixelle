/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "thread.h"
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <random>
#include <unordered_set>
#include <spdlog/spdlog.h>

namespace sonet::note::models {

// Constructors

Thread::Thread(const std::string& starter_note_id, const std::string& author_id)
    : starter_note_id(starter_note_id), author_id(author_id) {
    initialize_defaults();
    
    // Add the starter note to the thread
    note_ids.push_back(starter_note_id);
    total_notes = 1;
    
    spdlog::debug("Created thread {} with starter note {}", thread_id, starter_note_id);
}

Thread::Thread(const std::string& starter_note_id, const std::string& author_id, const std::string& title)
    : starter_note_id(starter_note_id), author_id(author_id), title(title) {
    initialize_defaults();
    
    // Add the starter note to the thread
    note_ids.push_back(starter_note_id);
    total_notes = 1;
    
    spdlog::debug("Created titled thread '{}' with ID {}", title, thread_id);
}

void Thread::initialize_defaults() {
    // Generate thread ID - in real implementation, use UUID
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(100000, 999999);
    thread_id = "thread_" + std::to_string(dis(gen));
    
    auto now = std::time(nullptr);
    created_at = now;
    updated_at = now;
    last_activity_at = now;
    
    // Initialize engagement metrics
    total_likes = 0;
    total_renotes = 0;
    total_replies = 0;
    total_views = 0;
    total_bookmarks = 0;
    unique_participants = 1; // At least the author
    
    // Initialize flags
    is_locked = false;
    is_pinned = false;
    is_published = true;
    allow_replies = true;
    allow_renotes = true;
    
    // Set default visibility
    visibility = NoteVisibility::PUBLIC;
    
    // Default analytics
    engagement_rate = 0.0;
    completion_rate = 0.0;
}

// Core thread operations

bool Thread::add_note(const std::string& note_id, int position) {
    if (is_locked) {
        spdlog::warn("Attempted to add note to locked thread {}", thread_id);
        return false;
    }
    
    // Check if note already exists in thread
    if (std::find(note_ids.begin(), note_ids.end(), note_id) != note_ids.end()) {
        spdlog::warn("Note {} already exists in thread {}", note_id, thread_id);
        return false;
    }
    
    try {
        if (position == -1 || position >= static_cast<int>(note_ids.size())) {
            // Add to end
            note_ids.push_back(note_id);
        } else {
            // Insert at specific position
            validate_note_position(position);
            note_ids.insert(note_ids.begin() + position, note_id);
        }
        
        total_notes = static_cast<int>(note_ids.size());
        update_timestamps();
        update_thread_metrics();
        
        spdlog::debug("Added note {} to thread {} at position {}", 
                     note_id, thread_id, position);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to add note {} to thread {}: {}", 
                     note_id, thread_id, e.what());
        return false;
    }
}

bool Thread::remove_note(const std::string& note_id) {
    if (note_id == starter_note_id) {
        spdlog::warn("Cannot remove starter note {} from thread {}", note_id, thread_id);
        return false;
    }
    
    auto it = std::find(note_ids.begin(), note_ids.end(), note_id);
    if (it == note_ids.end()) {
        spdlog::warn("Note {} not found in thread {}", note_id, thread_id);
        return false;
    }
    
    note_ids.erase(it);
    total_notes = static_cast<int>(note_ids.size());
    update_timestamps();
    update_thread_metrics();
    
    spdlog::debug("Removed note {} from thread {}", note_id, thread_id);
    return true;
}

bool Thread::reorder_note(const std::string& note_id, int new_position) {
    if (note_id == starter_note_id && new_position != 0) {
        spdlog::warn("Cannot move starter note {} from position 0", note_id);
        return false;
    }
    
    // Find current position
    auto it = std::find(note_ids.begin(), note_ids.end(), note_id);
    if (it == note_ids.end()) {
        return false;
    }
    
    try {
        validate_note_position(new_position);
        
        // Remove from current position
        note_ids.erase(it);
        
        // Insert at new position
        note_ids.insert(note_ids.begin() + new_position, note_id);
        
        update_timestamps();
        
        spdlog::debug("Reordered note {} to position {} in thread {}", 
                     note_id, new_position, thread_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to reorder note {} in thread {}: {}", 
                     note_id, thread_id, e.what());
        return false;
    }
}

std::vector<std::string> Thread::get_notes_in_order() const {
    return note_ids;
}

// Thread management

void Thread::lock_thread() {
    is_locked = true;
    update_timestamps();
    spdlog::debug("Locked thread {}", thread_id);
}

void Thread::unlock_thread() {
    is_locked = false;
    update_timestamps();
    spdlog::debug("Unlocked thread {}", thread_id);
}

void Thread::pin_thread() {
    is_pinned = true;
    update_timestamps();
    spdlog::debug("Pinned thread {}", thread_id);
}

void Thread::unpin_thread() {
    is_pinned = false;
    update_timestamps();
    spdlog::debug("Unpinned thread {}", thread_id);
}

void Thread::complete_thread() {
    completed_at = std::time(nullptr);
    is_locked = true; // Completed threads are automatically locked
    update_timestamps();
    spdlog::debug("Completed thread {}", thread_id);
}

void Thread::reopen_thread() {
    completed_at.reset();
    is_locked = false;
    update_timestamps();
    spdlog::debug("Reopened thread {}", thread_id);
}

// Moderation

void Thread::add_moderator(const std::string& user_id) {
    if (std::find(moderator_ids.begin(), moderator_ids.end(), user_id) == moderator_ids.end()) {
        moderator_ids.push_back(user_id);
        update_timestamps();
        spdlog::debug("Added moderator {} to thread {}", user_id, thread_id);
    }
}

void Thread::remove_moderator(const std::string& user_id) {
    moderator_ids.erase(
        std::remove(moderator_ids.begin(), moderator_ids.end(), user_id),
        moderator_ids.end()
    );
    update_timestamps();
    spdlog::debug("Removed moderator {} from thread {}", user_id, thread_id);
}

void Thread::block_user(const std::string& user_id) {
    if (user_id == author_id) {
        spdlog::warn("Cannot block thread author {} from their own thread", user_id);
        return;
    }
    
    if (std::find(blocked_user_ids.begin(), blocked_user_ids.end(), user_id) == blocked_user_ids.end()) {
        blocked_user_ids.push_back(user_id);
        update_timestamps();
        spdlog::debug("Blocked user {} from thread {}", user_id, thread_id);
    }
}

void Thread::unblock_user(const std::string& user_id) {
    blocked_user_ids.erase(
        std::remove(blocked_user_ids.begin(), blocked_user_ids.end(), user_id),
        blocked_user_ids.end()
    );
    update_timestamps();
    spdlog::debug("Unblocked user {} from thread {}", user_id, thread_id);
}

bool Thread::is_user_blocked(const std::string& user_id) const {
    return std::find(blocked_user_ids.begin(), blocked_user_ids.end(), user_id) 
           != blocked_user_ids.end();
}

bool Thread::can_user_moderate(const std::string& user_id) const {
    return user_id == author_id || 
           std::find(moderator_ids.begin(), moderator_ids.end(), user_id) != moderator_ids.end();
}

// Permissions

bool Thread::can_user_add_note(const std::string& user_id) const {
    if (is_locked) return false;
    if (is_user_blocked(user_id)) return false;
    if (user_id == author_id) return true; // Author can always add
    
    // For now, anyone can add to public threads
    // In production, you might have more complex rules
    return visibility == NoteVisibility::PUBLIC;
}

bool Thread::can_user_reply_to_thread(const std::string& user_id) const {
    if (!allow_replies) return false;
    if (is_user_blocked(user_id)) return false;
    
    return can_user_view_thread(user_id);
}

bool Thread::can_user_view_thread(const std::string& user_id) const {
    if (user_id == author_id) return true;
    if (!is_published) return false;
    
    switch (visibility) {
        case NoteVisibility::PUBLIC:
            return true;
        case NoteVisibility::FOLLOWERS_ONLY:
            // In real implementation, check if user follows author
            return true; // Simplified for now
        case NoteVisibility::PRIVATE:
            return false;
        default:
            return false;
    }
}

// Engagement tracking

void Thread::update_engagement_metrics() {
    // This would be called when notes in the thread are liked, renoted, etc.
    // For now, just update the timestamp
    update_timestamps();
}

void Thread::record_view(const std::string& user_id) {
    total_views++;
    last_activity_at = std::time(nullptr);
    
    // In real implementation, track unique viewers
    spdlog::debug("Recorded view for thread {} by user {}", thread_id, user_id);
}

void Thread::calculate_completion_rate() {
    if (total_views == 0) {
        completion_rate = 0.0;
        return;
    }
    
    // This is a simplified calculation
    // In real implementation, track how many users read all notes
    completion_rate = 0.8; // Placeholder
}

// Analytics

std::vector<std::string> Thread::get_participant_ids() const {
    // In real implementation, this would query the database
    // for all users who contributed notes to this thread
    std::unordered_set<std::string> participants;
    participants.insert(author_id);
    
    // Placeholder - would get actual participants from note data
    return std::vector<std::string>(participants.begin(), participants.end());
}

int Thread::get_note_position(const std::string& note_id) const {
    auto it = std::find(note_ids.begin(), note_ids.end(), note_id);
    if (it == note_ids.end()) {
        return -1;
    }
    return static_cast<int>(std::distance(note_ids.begin(), it));
}

std::string Thread::get_next_note_id(const std::string& current_note_id) const {
    int pos = get_note_position(current_note_id);
    if (pos == -1 || pos >= static_cast<int>(note_ids.size()) - 1) {
        return "";
    }
    return note_ids[pos + 1];
}

std::string Thread::get_previous_note_id(const std::string& current_note_id) const {
    int pos = get_note_position(current_note_id);
    if (pos <= 0) {
        return "";
    }
    return note_ids[pos - 1];
}

// Content analysis

std::vector<std::string> Thread::extract_thread_hashtags() const {
    // In real implementation, analyze all notes in thread for hashtags
    return {}; // Placeholder
}

std::vector<std::string> Thread::extract_thread_mentions() const {
    // In real implementation, analyze all notes in thread for mentions
    return {}; // Placeholder
}

std::string Thread::generate_thread_summary() const {
    if (!title.empty()) {
        return title;
    }
    
    // Generate summary from first note content
    return "Thread with " + std::to_string(total_notes) + " notes";
}

// Validation

bool Thread::is_valid() const {
    return get_validation_errors().empty();
}

std::vector<std::string> Thread::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (thread_id.empty()) {
        errors.push_back("Thread ID cannot be empty");
    }
    
    if (starter_note_id.empty()) {
        errors.push_back("Starter note ID cannot be empty");
    }
    
    if (author_id.empty()) {
        errors.push_back("Author ID cannot be empty");
    }
    
    if (note_ids.empty()) {
        errors.push_back("Thread must have at least one note");
    }
    
    if (!note_ids.empty() && note_ids[0] != starter_note_id) {
        errors.push_back("First note must be the starter note");
    }
    
    return errors;
}

// Serialization

nlohmann::json Thread::to_json() const {
    nlohmann::json j;
    
    // Core fields
    j["thread_id"] = thread_id;
    j["starter_note_id"] = starter_note_id;
    j["author_id"] = author_id;
    j["author_username"] = author_username;
    
    // Metadata
    j["title"] = title;
    j["description"] = description;
    j["tags"] = tags;
    
    // Structure
    j["note_ids"] = note_ids;
    j["total_notes"] = total_notes;
    j["max_depth"] = max_depth;
    
    // State
    j["is_locked"] = is_locked;
    j["is_pinned"] = is_pinned;
    j["is_published"] = is_published;
    j["allow_replies"] = allow_replies;
    j["allow_renotes"] = allow_renotes;
    
    // Engagement
    j["total_likes"] = total_likes;
    j["total_renotes"] = total_renotes;
    j["total_replies"] = total_replies;
    j["total_views"] = total_views;
    j["total_bookmarks"] = total_bookmarks;
    j["unique_participants"] = unique_participants;
    
    // Timestamps
    j["created_at"] = created_at;
    j["updated_at"] = updated_at;
    if (last_activity_at.has_value()) j["last_activity_at"] = last_activity_at.value();
    if (completed_at.has_value()) j["completed_at"] = completed_at.value();
    
    // Visibility and moderation
    j["visibility"] = static_cast<int>(visibility);
    j["moderator_ids"] = moderator_ids;
    j["blocked_user_ids"] = blocked_user_ids;
    
    // Analytics
    j["daily_metrics"] = daily_metrics;
    j["trending_keywords"] = trending_keywords;
    j["engagement_rate"] = engagement_rate;
    j["completion_rate"] = completion_rate;
    
    return j;
}

void Thread::from_json(const nlohmann::json& j) {
    // Core fields
    j.at("thread_id").get_to(thread_id);
    j.at("starter_note_id").get_to(starter_note_id);
    j.at("author_id").get_to(author_id);
    if (j.contains("author_username")) j.at("author_username").get_to(author_username);
    
    // Metadata
    if (j.contains("title")) j.at("title").get_to(title);
    if (j.contains("description")) j.at("description").get_to(description);
    if (j.contains("tags")) j.at("tags").get_to(tags);
    
    // Structure
    if (j.contains("note_ids")) j.at("note_ids").get_to(note_ids);
    if (j.contains("total_notes")) j.at("total_notes").get_to(total_notes);
    if (j.contains("max_depth")) j.at("max_depth").get_to(max_depth);
    
    // State
    if (j.contains("is_locked")) j.at("is_locked").get_to(is_locked);
    if (j.contains("is_pinned")) j.at("is_pinned").get_to(is_pinned);
    if (j.contains("is_published")) j.at("is_published").get_to(is_published);
    if (j.contains("allow_replies")) j.at("allow_replies").get_to(allow_replies);
    if (j.contains("allow_renotes")) j.at("allow_renotes").get_to(allow_renotes);
    
    // Engagement
    if (j.contains("total_likes")) j.at("total_likes").get_to(total_likes);
    if (j.contains("total_renotes")) j.at("total_renotes").get_to(total_renotes);
    if (j.contains("total_replies")) j.at("total_replies").get_to(total_replies);
    if (j.contains("total_views")) j.at("total_views").get_to(total_views);
    if (j.contains("total_bookmarks")) j.at("total_bookmarks").get_to(total_bookmarks);
    if (j.contains("unique_participants")) j.at("unique_participants").get_to(unique_participants);
    
    // Timestamps
    if (j.contains("created_at")) j.at("created_at").get_to(created_at);
    if (j.contains("updated_at")) j.at("updated_at").get_to(updated_at);
    if (j.contains("last_activity_at") && !j["last_activity_at"].is_null()) {
        last_activity_at = j["last_activity_at"].get<std::time_t>();
    }
    if (j.contains("completed_at") && !j["completed_at"].is_null()) {
        completed_at = j["completed_at"].get<std::time_t>();
    }
    
    // Visibility and moderation
    if (j.contains("visibility")) visibility = static_cast<NoteVisibility>(j["visibility"].get<int>());
    if (j.contains("moderator_ids")) j.at("moderator_ids").get_to(moderator_ids);
    if (j.contains("blocked_user_ids")) j.at("blocked_user_ids").get_to(blocked_user_ids);
    
    // Analytics
    if (j.contains("daily_metrics")) j.at("daily_metrics").get_to(daily_metrics);
    if (j.contains("trending_keywords")) j.at("trending_keywords").get_to(trending_keywords);
    if (j.contains("engagement_rate")) j.at("engagement_rate").get_to(engagement_rate);
    if (j.contains("completion_rate")) j.at("completion_rate").get_to(completion_rate);
}

std::string Thread::to_string() const {
    return to_json().dump(2);
}

// Display helpers

std::string Thread::get_display_title() const {
    if (!title.empty()) {
        return title;
    }
    return "Thread by " + author_username;
}

std::string Thread::get_thread_preview() const {
    return generate_thread_summary() + " (" + std::to_string(total_notes) + " notes)";
}

std::string Thread::get_formatted_timestamp() const {
    std::tm* tm_info = std::localtime(&created_at);
    std::stringstream ss;
    ss << std::put_time(tm_info, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

std::string Thread::get_relative_timestamp() const {
    auto now = std::time(nullptr);
    auto diff = now - created_at;
    
    if (diff < 60) return std::to_string(diff) + "s";
    if (diff < 3600) return std::to_string(diff / 60) + "m";
    if (diff < 86400) return std::to_string(diff / 3600) + "h";
    if (diff < 604800) return std::to_string(diff / 86400) + "d";
    return std::to_string(diff / 604800) + "w";
}

// Utility

bool Thread::is_empty() const {
    return note_ids.empty();
}

bool Thread::is_single_note() const {
    return note_ids.size() == 1;
}

bool Thread::is_completed() const {
    return completed_at.has_value();
}

std::time_t Thread::get_age_seconds() const {
    return std::time(nullptr) - created_at;
}

std::time_t Thread::get_age_hours() const {
    return get_age_seconds() / 3600;
}

bool Thread::is_recent(int hours) const {
    return get_age_hours() <= hours;
}

// Comparison operators

bool Thread::operator==(const Thread& other) const {
    return thread_id == other.thread_id;
}

bool Thread::operator!=(const Thread& other) const {
    return !(*this == other);
}

bool Thread::operator<(const Thread& other) const {
    return created_at < other.created_at;
}

// Private helper methods

void Thread::update_timestamps() {
    updated_at = std::time(nullptr);
    last_activity_at = updated_at;
}

void Thread::update_thread_metrics() {
    total_notes = static_cast<int>(note_ids.size());
    // In real implementation, recalculate engagement metrics
    update_timestamps();
}

void Thread::validate_note_position(int position) const {
    if (position < 0 || position > static_cast<int>(note_ids.size())) {
        throw std::out_of_range("Invalid note position");
    }
}

std::string Thread::generate_thread_id() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(100000, 999999);
    return "thread_" + std::to_string(dis(gen));
}

// Supporting structure implementations

nlohmann::json ThreadStatistics::to_json() const {
    nlohmann::json j;
    j["thread_id"] = thread_id;
    j["calculated_at"] = calculated_at;
    j["total_notes"] = total_notes;
    j["total_participants"] = total_participants;
    j["total_views"] = total_views;
    j["total_engagement"] = total_engagement;
    j["average_time_between_notes"] = average_time_between_notes;
    j["total_thread_duration"] = total_thread_duration;
    j["engagement_rate"] = engagement_rate;
    j["completion_rate"] = completion_rate;
    j["bounce_rate"] = bounce_rate;
    j["average_note_length"] = average_note_length;
    j["total_hashtags"] = total_hashtags;
    j["total_mentions"] = total_mentions;
    j["total_urls"] = total_urls;
    j["spam_score"] = spam_score;
    j["toxicity_score"] = toxicity_score;
    j["readability_score"] = readability_score;
    return j;
}

void ThreadStatistics::from_json(const nlohmann::json& j) {
    j.at("thread_id").get_to(thread_id);
    j.at("calculated_at").get_to(calculated_at);
    if (j.contains("total_notes")) j.at("total_notes").get_to(total_notes);
    if (j.contains("total_participants")) j.at("total_participants").get_to(total_participants);
    if (j.contains("total_views")) j.at("total_views").get_to(total_views);
    if (j.contains("total_engagement")) j.at("total_engagement").get_to(total_engagement);
    if (j.contains("average_time_between_notes")) j.at("average_time_between_notes").get_to(average_time_between_notes);
    if (j.contains("total_thread_duration")) j.at("total_thread_duration").get_to(total_thread_duration);
    if (j.contains("engagement_rate")) j.at("engagement_rate").get_to(engagement_rate);
    if (j.contains("completion_rate")) j.at("completion_rate").get_to(completion_rate);
    if (j.contains("bounce_rate")) j.at("bounce_rate").get_to(bounce_rate);
    if (j.contains("average_note_length")) j.at("average_note_length").get_to(average_note_length);
    if (j.contains("total_hashtags")) j.at("total_hashtags").get_to(total_hashtags);
    if (j.contains("total_mentions")) j.at("total_mentions").get_to(total_mentions);
    if (j.contains("total_urls")) j.at("total_urls").get_to(total_urls);
    if (j.contains("spam_score")) j.at("spam_score").get_to(spam_score);
    if (j.contains("toxicity_score")) j.at("toxicity_score").get_to(toxicity_score);
    if (j.contains("readability_score")) j.at("readability_score").get_to(readability_score);
}

nlohmann::json ThreadParticipant::to_json() const {
    nlohmann::json j;
    j["user_id"] = user_id;
    j["username"] = username;
    j["notes_contributed"] = notes_contributed;
    j["total_likes_received"] = total_likes_received;
    j["total_replies_received"] = total_replies_received;
    j["first_participation"] = first_participation;
    j["last_participation"] = last_participation;
    j["is_moderator"] = is_moderator;
    j["is_blocked"] = is_blocked;
    return j;
}

void ThreadParticipant::from_json(const nlohmann::json& j) {
    j.at("user_id").get_to(user_id);
    j.at("username").get_to(username);
    if (j.contains("notes_contributed")) j.at("notes_contributed").get_to(notes_contributed);
    if (j.contains("total_likes_received")) j.at("total_likes_received").get_to(total_likes_received);
    if (j.contains("total_replies_received")) j.at("total_replies_received").get_to(total_replies_received);
    if (j.contains("first_participation")) j.at("first_participation").get_to(first_participation);
    if (j.contains("last_participation")) j.at("last_participation").get_to(last_participation);
    if (j.contains("is_moderator")) j.at("is_moderator").get_to(is_moderator);
    if (j.contains("is_blocked")) j.at("is_blocked").get_to(is_blocked);
}

// Utility functions

std::string thread_visibility_to_string(NoteVisibility visibility) {
    return note_visibility_to_string(visibility);
}

NoteVisibility string_to_thread_visibility(const std::string& visibility_str) {
    return string_to_note_visibility(visibility_str);
}

// JSON conversion helpers

void to_json(nlohmann::json& j, const Thread& thread) {
    j = thread.to_json();
}

void from_json(const nlohmann::json& j, Thread& thread) {
    thread.from_json(j);
}

void to_json(nlohmann::json& j, const ThreadStatistics& stats) {
    j = stats.to_json();
}

void from_json(const nlohmann::json& j, ThreadStatistics& stats) {
    stats.from_json(j);
}

void to_json(nlohmann::json& j, const ThreadParticipant& participant) {
    j = participant.to_json();
}

void from_json(const nlohmann::json& j, ThreadParticipant& participant) {
    participant.from_json(j);
}

} // namespace sonet::note::models
