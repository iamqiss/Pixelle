/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "note.h"
#include <algorithm>
#include <regex>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <random>
#include <unordered_set>

namespace sonet::note::models {

// Note constructors
Note::Note(const std::string& author_id, const std::string& content)
    : author_id(author_id), content(content), raw_content(content) {
    initialize_defaults();
    process_content();
}

Note::Note(const std::string& author_id, const std::string& content, NoteType type)
    : author_id(author_id), content(content), raw_content(content), type(type) {
    initialize_defaults();
    process_content();
}

void Note::initialize_defaults() {
    // Generate note ID (in real implementation, use UUID)
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(100000, 999999);
    note_id = "note_" + std::to_string(dis(gen));
    
    auto now = std::time(nullptr);
    created_at = now;
    updated_at = now;
    
    // Initialize engagement metrics
    like_count = 0;
    renote_count = 0;
    reply_count = 0;
    quote_count = 0;
    view_count = 0;
    bookmark_count = 0;
    
    // Initialize moderation scores
    spam_score = 0.0;
    toxicity_score = 0.0;
    
    // Initialize flags
    is_sensitive = false;
    is_nsfw = false;
    contains_spoilers = false;
    is_promoted = false;
    is_verified_author = false;
    
    // Initialize permissions
    allow_replies = true;
    allow_renotes = true;
    allow_quotes = true;
}

// Validation methods
bool Note::is_valid() const {
    return get_validation_errors().empty();
}

std::vector<NoteValidationError> Note::get_validation_errors() const {
    std::vector<NoteValidationError> errors;
    
    // Content validation
    if (content.empty() && type != NoteType::RENOST) {
        errors.push_back(NoteValidationError::CONTENT_EMPTY);
    }
    
    if (content.length() > MAX_CONTENT_LENGTH) {
        errors.push_back(NoteValidationError::CONTENT_TOO_LONG);
    }
    
    // Mentions validation
    if (mentioned_user_ids.size() > MAX_MENTIONS) {
        errors.push_back(NoteValidationError::INVALID_MENTIONS);
    }
    
    // Hashtags validation
    if (hashtags.size() > MAX_HASHTAGS) {
        errors.push_back(NoteValidationError::INVALID_HASHTAGS);
    }
    
    // Attachments validation
    if (attachment_ids.size() > MAX_ATTACHMENTS) {
        errors.push_back(NoteValidationError::TOO_MANY_ATTACHMENTS);
    }
    
    // Reply validation
    if (type == NoteType::REPLY && !reply_to_id.has_value()) {
        errors.push_back(NoteValidationError::INVALID_REPLY_TARGET);
    }
    
    // Renote validation
    if (type == NoteType::RENOST && !renote_of_id.has_value()) {
        errors.push_back(NoteValidationError::INVALID_RENOTE_TARGET);
    }
    
    // Scheduled time validation
    if (scheduled_at.has_value() && scheduled_at.value() <= std::time(nullptr)) {
        errors.push_back(NoteValidationError::INVALID_SCHEDULED_TIME);
    }
    
    // Spam detection
    if (spam_score > 0.8) {
        errors.push_back(NoteValidationError::SPAM_DETECTED);
    }
    
    // Toxicity detection
    if (toxicity_score > 0.9) {
        errors.push_back(NoteValidationError::PROFANITY_DETECTED);
    }
    
    return errors;
}

bool Note::validate_content() const {
    return !content.empty() && content.length() <= MAX_CONTENT_LENGTH;
}

bool Note::validate_mentions() const {
    if (mentioned_user_ids.size() > MAX_MENTIONS) return false;
    
    for (const auto& mention : mentioned_usernames) {
        if (!is_valid_mention(mention)) return false;
    }
    return true;
}

bool Note::validate_hashtags() const {
    if (hashtags.size() > MAX_HASHTAGS) return false;
    
    for (const auto& hashtag : hashtags) {
        if (!is_valid_hashtag(hashtag)) return false;
    }
    return true;
}

bool Note::validate_attachments() const {
    // Check legacy attachment limit
    if (attachment_ids.size() > MAX_ATTACHMENTS) {
        return false;
    }
    
    // Validate attachment collection
    if (!attachments.validate()) {
        return false;
    }
    
    // Check for failed attachments
    auto failed_attachments = attachments.get_failed_attachments();
    if (!failed_attachments.empty()) {
        return false;
    }
    
    // Ensure attachment count matches legacy attachment_ids
    if (attachments.size() != attachment_ids.size()) {
        return false;
    }
    
    // Check total attachment size
    if (!attachments.is_within_total_size_limit()) {
        return false;
    }
    
    return true;
}

bool Note::validate_scheduled_time() const {
    if (!scheduled_at.has_value()) return true;
    return scheduled_at.value() > std::time(nullptr);
}

// Content processing methods
void Note::process_content() {
    extract_mentions();
    extract_hashtags();
    extract_urls();
    detect_language();
    calculate_spam_score();
    calculate_toxicity_score();
    
    // Process content for display
    processed_content = highlight_content_features(content);
    update_timestamps();
}

void Note::extract_mentions() {
    mentioned_user_ids.clear();
    mentioned_usernames.clear();
    
    std::regex mention_regex(R"(@([a-zA-Z0-9_]{1,15}))");
    std::sregex_iterator begin(content.begin(), content.end(), mention_regex);
    std::sregex_iterator end;
    
    for (auto it = begin; it != end; ++it) {
        std::string username = (*it)[1].str();
        if (std::find(mentioned_usernames.begin(), mentioned_usernames.end(), username) == mentioned_usernames.end()) {
            mentioned_usernames.push_back(username);
            // In real implementation, resolve username to user_id
            mentioned_user_ids.push_back("user_" + username);
        }
    }
}

void Note::extract_hashtags() {
    hashtags.clear();
    
    std::regex hashtag_regex(R"(#([a-zA-Z0-9_]{1,100}))");
    std::sregex_iterator begin(content.begin(), content.end(), hashtag_regex);
    std::sregex_iterator end;
    
    for (auto it = begin; it != end; ++it) {
        std::string hashtag = (*it)[1].str();
        if (std::find(hashtags.begin(), hashtags.end(), hashtag) == hashtags.end()) {
            hashtags.push_back(hashtag);
        }
    }
}

void Note::extract_urls() {
    urls.clear();
    
    std::regex url_regex(R"((https?://[^\s]+))");
    std::sregex_iterator begin(content.begin(), content.end(), url_regex);
    std::sregex_iterator end;
    
    for (auto it = begin; it != end; ++it) {
        std::string url = (*it)[1].str();
        if (std::find(urls.begin(), urls.end(), url) == urls.end()) {
            urls.push_back(url);
        }
    }
}

void Note::detect_language() {
    // Simple language detection based on character patterns
    // In real implementation, use proper language detection library
    
    // Check for common English words
    std::vector<std::string> english_words = {"the", "and", "is", "to", "a", "in", "it", "you", "that", "he", "was", "for"};
    int english_matches = 0;
    
    std::string lower_content = content;
    std::transform(lower_content.begin(), lower_content.end(), lower_content.begin(), ::tolower);
    
    for (const auto& word : english_words) {
        if (lower_content.find(word) != std::string::npos) {
            english_matches++;
        }
    }
    
    if (english_matches >= 2) {
        detected_languages.push_back("en");
    } else {
        detected_languages.push_back("unknown");
    }
}

void Note::calculate_spam_score() {
    spam_score = 0.0;
    
    // Check for excessive capitalization
    int caps_count = 0;
    for (char c : content) {
        if (std::isupper(c)) caps_count++;
    }
    double caps_ratio = static_cast<double>(caps_count) / content.length();
    if (caps_ratio > 0.7) spam_score += 0.3;
    
    // Check for excessive exclamation marks
    int exclamation_count = std::count(content.begin(), content.end(), '!');
    if (exclamation_count > 3) spam_score += 0.2;
    
    // Check for excessive hashtags
    if (hashtags.size() > 5) spam_score += 0.2;
    
    // Check for excessive mentions
    if (mentioned_user_ids.size() > 5) spam_score += 0.2;
    
    // Check for repeated characters
    std::regex repeated_chars(R"((.)\1{4,})");
    if (std::regex_search(content, repeated_chars)) {
        spam_score += 0.1;
    }
    
    // Ensure score doesn't exceed 1.0
    spam_score = std::min(spam_score, 1.0);
}

void Note::calculate_toxicity_score() {
    toxicity_score = 0.0;
    
    // Simple toxicity detection (in real implementation, use AI model)
    std::vector<std::string> toxic_words = {"hate", "stupid", "idiot", "kill", "die", "worst"};
    
    std::string lower_content = content;
    std::transform(lower_content.begin(), lower_content.end(), lower_content.begin(), ::tolower);
    
    for (const auto& word : toxic_words) {
        if (lower_content.find(word) != std::string::npos) {
            toxicity_score += 0.2;
        }
    }
    
    // Check for excessive profanity indicators
    if (std::count(lower_content.begin(), lower_content.end(), '*') > 3) {
        toxicity_score += 0.1;
    }
    
    toxicity_score = std::min(toxicity_score, 1.0);
}

// Content manipulation methods
bool Note::set_content(const std::string& new_content) {
    if (new_content.length() > MAX_CONTENT_LENGTH) {
        return false;
    }
    
    content = new_content;
    raw_content = new_content;
    process_content();
    return true;
}

void Note::add_mention(const std::string& user_id, const std::string& username) {
    if (mentioned_user_ids.size() < MAX_MENTIONS) {
        if (std::find(mentioned_user_ids.begin(), mentioned_user_ids.end(), user_id) == mentioned_user_ids.end()) {
            mentioned_user_ids.push_back(user_id);
            mentioned_usernames.push_back(username);
        }
    }
}

void Note::add_hashtag(const std::string& hashtag) {
    if (hashtags.size() < MAX_HASHTAGS) {
        if (std::find(hashtags.begin(), hashtags.end(), hashtag) == hashtags.end()) {
            hashtags.push_back(hashtag);
        }
    }
}

void Note::add_attachment(const std::string& attachment_id) {
    if (attachment_ids.size() < MAX_ATTACHMENTS) {
        if (std::find(attachment_ids.begin(), attachment_ids.end(), attachment_id) == attachment_ids.end()) {
            attachment_ids.push_back(attachment_id);
        }
    }
}

void Note::remove_attachment(const std::string& attachment_id) {
    attachment_ids.erase(
        std::remove(attachment_ids.begin(), attachment_ids.end(), attachment_id),
        attachment_ids.end()
    );
}

// New attachment management methods

bool Note::add_media_attachment(const Attachment& attachment) {
    if (!attachments.add_attachment(attachment)) {
        return false;
    }
    
    // Add to legacy attachment_ids for compatibility
    add_attachment(attachment.attachment_id);
    
    // Update note metadata based on attachment content
    if (attachment.is_sensitive || attachment.has_moderation_flags()) {
        is_sensitive = true;
    }
    
    // Mark as NSFW if attachment contains explicit content
    auto moderation_flags = attachment.get_moderation_flags();
    for (const auto& flag : moderation_flags) {
        if (flag == "nsfw" || flag == "explicit") {
            is_nsfw = true;
            break;
        }
    }
    
    update_timestamps();
    return true;
}

bool Note::remove_media_attachment(const std::string& attachment_id) {
    bool removed = attachments.remove_attachment(attachment_id);
    if (removed) {
        remove_attachment(attachment_id); // Update legacy attachment_ids
        update_timestamps();
    }
    return removed;
}

void Note::clear_attachments() {
    attachments.clear();
    attachment_ids.clear();
    update_timestamps();
}

bool Note::has_attachments() const {
    return !attachments.empty();
}

size_t Note::get_attachment_count() const {
    return attachments.size();
}

std::vector<Attachment> Note::get_attachments_by_type(AttachmentType type) const {
    return attachments.get_by_type(type);
}

std::vector<Attachment> Note::get_image_attachments() const {
    return get_attachments_by_type(AttachmentType::IMAGE);
}

std::vector<Attachment> Note::get_video_attachments() const {
    return get_attachments_by_type(AttachmentType::VIDEO);
}

std::vector<Attachment> Note::get_gif_attachments() const {
    std::vector<Attachment> gifs;
    auto regular_gifs = get_attachments_by_type(AttachmentType::GIF);
    auto tenor_gifs = get_attachments_by_type(AttachmentType::TENOR_GIF);
    
    gifs.insert(gifs.end(), regular_gifs.begin(), regular_gifs.end());
    gifs.insert(gifs.end(), tenor_gifs.begin(), tenor_gifs.end());
    
    return gifs;
}

bool Note::has_sensitive_attachments() const {
    for (const auto& attachment : attachments) {
        if (attachment.is_sensitive || attachment.has_moderation_flags()) {
            return true;
        }
    }
    return false;
}

bool Note::has_processing_attachments() const {
    return !attachments.get_processing_attachments().empty();
}

size_t Note::get_total_attachment_size() const {
    return attachments.get_total_size();
}

std::optional<Attachment> Note::get_primary_attachment() const {
    if (attachments.empty()) {
        return std::nullopt;
    }
    
    // Return first attachment as primary
    return attachments.attachments[0];
}

std::string Note::get_primary_attachment_url() const {
    auto primary = get_primary_attachment();
    if (primary) {
        return primary->get_url();
    }
    return "";
}

std::string Note::get_attachment_summary() const {
    if (attachments.empty()) {
        return "";
    }
    
    std::unordered_map<AttachmentType, int> type_counts;
    for (const auto& attachment : attachments) {
        type_counts[attachment.type]++;
    }
    
    std::vector<std::string> parts;
    
    if (type_counts[AttachmentType::IMAGE] > 0) {
        int count = type_counts[AttachmentType::IMAGE];
        parts.push_back(std::to_string(count) + (count == 1 ? " image" : " images"));
    }
    
    if (type_counts[AttachmentType::VIDEO] > 0) {
        int count = type_counts[AttachmentType::VIDEO];
        parts.push_back(std::to_string(count) + (count == 1 ? " video" : " videos"));
    }
    
    if (type_counts[AttachmentType::GIF] > 0 || type_counts[AttachmentType::TENOR_GIF] > 0) {
        int count = type_counts[AttachmentType::GIF] + type_counts[AttachmentType::TENOR_GIF];
        parts.push_back(std::to_string(count) + (count == 1 ? " GIF" : " GIFs"));
    }
    
    if (type_counts[AttachmentType::AUDIO] > 0) {
        int count = type_counts[AttachmentType::AUDIO];
        parts.push_back(std::to_string(count) + (count == 1 ? " audio" : " audio files"));
    }
    
    if (type_counts[AttachmentType::POLL] > 0) {
        parts.push_back("poll");
    }
    
    if (type_counts[AttachmentType::LOCATION] > 0) {
        parts.push_back("location");
    }
    
    if (type_counts[AttachmentType::LINK_PREVIEW] > 0) {
        int count = type_counts[AttachmentType::LINK_PREVIEW];
        parts.push_back(std::to_string(count) + (count == 1 ? " link" : " links"));
    }
    
    // Join parts with commas
    if (parts.empty()) {
        return "";
    } else if (parts.size() == 1) {
        return parts[0];
    } else if (parts.size() == 2) {
        return parts[0] + " and " + parts[1];
    } else {
        std::string result;
        for (size_t i = 0; i < parts.size() - 1; ++i) {
            result += parts[i] + ", ";
        }
        result += "and " + parts.back();
        return result;
    }
}

void Note::mark_all_attachments_sensitive(bool sensitive) {
    attachments.mark_all_as_sensitive(sensitive);
    if (sensitive) {
        is_sensitive = true;
    }
    update_timestamps();
}

// Relationship methods
void Note::set_reply_target(const std::string& note_id, const std::string& user_id) {
    type = NoteType::REPLY;
    reply_to_id = note_id;
    reply_to_user_id = user_id;
}

void Note::set_renote_target(const std::string& note_id) {
    type = NoteType::RENOTE;
    renote_of_id = note_id;
}

void Note::set_quote_target(const std::string& note_id) {
    type = NoteType::QUOTE;
    quote_of_id = note_id;
}

void Note::set_thread_info(const std::string& thread_id_val, int position) {
    thread_id = thread_id_val;
    thread_position = position;
    if (position > 0) {
        type = NoteType::THREAD;
    }
}

// Engagement methods
void Note::increment_likes() {
    like_count++;
    update_timestamps();
}

void Note::decrement_likes() {
    if (like_count > 0) {
        like_count--;
        update_timestamps();
    }
}

void Note::increment_renotes() {
    renote_count++;
    update_timestamps();
}

void Note::decrement_renotes() {
    if (renote_count > 0) {
        renote_count--;
        update_timestamps();
    }
}

void Note::increment_replies() {
    reply_count++;
    update_timestamps();
}

void Note::increment_quotes() {
    quote_count++;
    update_timestamps();
}

void Note::increment_views() {
    view_count++;
}

void Note::increment_bookmarks() {
    bookmark_count++;
    update_timestamps();
}

void Note::record_user_interaction(const std::string& user_id, const std::string& interaction_type) {
    user_interactions[user_id] = std::time(nullptr);
    
    // Add to recent interactions lists (limit to 50 entries)
    if (interaction_type == "like") {
        liked_by_user_ids.push_back(user_id);
        if (liked_by_user_ids.size() > 50) {
            liked_by_user_ids.erase(liked_by_user_ids.begin());
        }
    } else if (interaction_type == "renote") {
        renoted_by_user_ids.push_back(user_id);
        if (renoted_by_user_ids.size() > 50) {
            renoted_by_user_ids.erase(renoted_by_user_ids.begin());
        }
    }
}

// Metrics and analytics methods
void Note::update_daily_metrics(const std::string& date, int value) {
    daily_metrics[date] = value;
}

void Note::update_hourly_metrics(const std::string& hour, int value) {
    hourly_metrics[hour] = value;
}

void Note::add_trending_country(const std::string& country) {
    if (std::find(trending_countries.begin(), trending_countries.end(), country) == trending_countries.end()) {
        trending_countries.push_back(country);
    }
}

double Note::calculate_engagement_rate() const {
    if (view_count == 0) return 0.0;
    int total_engagement = like_count + renote_count + reply_count + quote_count;
    return static_cast<double>(total_engagement) / view_count;
}

double Note::calculate_virality_score() const {
    // Simple virality calculation based on engagement velocity
    auto age_hours = get_age_hours();
    if (age_hours == 0) age_hours = 1;
    
    double engagement_velocity = static_cast<double>(get_total_engagement()) / age_hours;
    double renote_factor = renote_count * 2.0; // Renotes spread content further
    
    return std::min(1.0, (engagement_velocity + renote_factor) / 100.0);
}

int Note::get_total_engagement() const {
    return like_count + renote_count + reply_count + quote_count + bookmark_count;
}

// Geographic methods
void Note::set_location(double lat, double lng, const std::string& name) {
    latitude = lat;
    longitude = lng;
    location_name = name;
}

void Note::clear_location() {
    latitude.reset();
    longitude.reset();
    location_name.clear();
}

bool Note::has_location() const {
    return latitude.has_value() && longitude.has_value();
}

// Moderation methods
void Note::mark_sensitive(bool sensitive) {
    is_sensitive = sensitive;
    if (sensitive) {
        content_warning = ContentWarning::SENSITIVE;
    }
}

void Note::mark_nsfw(bool nsfw) {
    is_nsfw = nsfw;
    if (nsfw) {
        content_warning = ContentWarning::ADULT;
        visibility = NoteVisibility::FOLLOWERS_ONLY; // Restrict visibility
    }
}

void Note::mark_spoilers(bool spoilers) {
    contains_spoilers = spoilers;
    if (spoilers) {
        content_warning = ContentWarning::SPOILER;
    }
}

void Note::set_content_warning(ContentWarning warning) {
    content_warning = warning;
    if (warning != ContentWarning::NONE) {
        is_sensitive = true;
    }
}

void Note::flag_for_review() {
    status = NoteStatus::FLAGGED;
    update_timestamps();
}

void Note::hide_note() {
    status = NoteStatus::HIDDEN;
    update_timestamps();
}

void Note::soft_delete() {
    status = NoteStatus::DELETED;
    deleted_at = std::time(nullptr);
    update_timestamps();
}

void Note::restore_note() {
    status = NoteStatus::ACTIVE;
    deleted_at.reset();
    update_timestamps();
}

// Scheduling methods
void Note::schedule_note(std::time_t scheduled_time) {
    scheduled_at = scheduled_time;
    status = NoteStatus::SCHEDULED;
}

void Note::unschedule_note() {
    scheduled_at.reset();
    status = NoteStatus::DRAFT;
}

bool Note::is_scheduled() const {
    return scheduled_at.has_value() && status == NoteStatus::SCHEDULED;
}

bool Note::should_be_published() const {
    return is_scheduled() && scheduled_at.value() <= std::time(nullptr);
}

// Privacy and permissions methods
void Note::set_visibility(NoteVisibility new_visibility) {
    visibility = new_visibility;
    update_timestamps();
}

bool Note::is_visible_to_user(const std::string& user_id, const std::vector<std::string>& following_ids,
                             const std::vector<std::string>& circle_ids) const {
    
    // Author can always see their own notes
    if (user_id == author_id) return true;
    
    // Deleted or hidden notes are not visible
    if (status == NoteStatus::DELETED || status == NoteStatus::HIDDEN) return false;
    
    // Drafts and scheduled notes are only visible to author
    if (status == NoteStatus::DRAFT || status == NoteStatus::SCHEDULED) return false;
    
    switch (visibility) {
        case NoteVisibility::PUBLIC:
            return true;
            
        case NoteVisibility::FOLLOWERS_ONLY:
            return std::find(following_ids.begin(), following_ids.end(), author_id) != following_ids.end();
            
        case NoteVisibility::MENTIONED_ONLY:
            return std::find(mentioned_user_ids.begin(), mentioned_user_ids.end(), user_id) != mentioned_user_ids.end();
            
        case NoteVisibility::PRIVATE:
            return false;
            
        case NoteVisibility::CIRCLE:
            return std::find(circle_ids.begin(), circle_ids.end(), user_id) != circle_ids.end();
            
        default:
            return false;
    }
}

bool Note::can_user_reply(const std::string& user_id) const {
    return allow_replies && is_visible_to_user(user_id);
}

bool Note::can_user_renote(const std::string& user_id) const {
    return allow_renotes && is_visible_to_user(user_id) && user_id != author_id;
}

bool Note::can_user_quote(const std::string& user_id) const {
    return allow_quotes && is_visible_to_user(user_id);
}

// Thread management methods
bool Note::is_part_of_thread() const {
    return thread_id.has_value();
}

bool Note::is_thread_starter() const {
    return is_part_of_thread() && thread_position == 0;
}

bool Note::can_add_to_thread(const std::string& user_id) const {
    return user_id == author_id; // Only author can continue their own thread
}

// Serialization methods
nlohmann::json Note::to_json() const {
    nlohmann::json j;
    
    // Core fields
    j["note_id"] = note_id;
    j["author_id"] = author_id;
    j["author_username"] = author_username;
    j["content"] = content;
    j["raw_content"] = raw_content;
    j["processed_content"] = processed_content;
    
    // Relationships
    if (reply_to_id.has_value()) j["reply_to_id"] = reply_to_id.value();
    if (reply_to_user_id.has_value()) j["reply_to_user_id"] = reply_to_user_id.value();
    if (renote_of_id.has_value()) j["renote_of_id"] = renote_of_id.value();
    if (quote_of_id.has_value()) j["quote_of_id"] = quote_of_id.value();
    if (thread_id.has_value()) j["thread_id"] = thread_id.value();
    j["thread_position"] = thread_position;
    
    // Classification
    j["type"] = static_cast<int>(type);
    j["visibility"] = static_cast<int>(visibility);
    j["status"] = static_cast<int>(status);
    j["content_warning"] = static_cast<int>(content_warning);
    
    // Content features
    j["mentioned_user_ids"] = mentioned_user_ids;
    j["mentioned_usernames"] = mentioned_usernames;
    j["hashtags"] = hashtags;
    j["urls"] = urls;
    j["attachment_ids"] = attachment_ids;
    
    // Media attachments
    j["attachments"] = attachments.to_json();
    j["attachment_summary"] = get_attachment_summary();
    j["has_attachments"] = has_attachments();
    j["attachment_count"] = get_attachment_count();
    if (has_attachments()) {
        j["primary_attachment_url"] = get_primary_attachment_url();
        j["has_sensitive_attachments"] = has_sensitive_attachments();
        j["has_processing_attachments"] = has_processing_attachments();
        j["total_attachment_size"] = get_total_attachment_size();
    }
    
    // Engagement metrics
    j["like_count"] = like_count;
    j["renote_count"] = renote_count;
    j["reply_count"] = reply_count;
    j["quote_count"] = quote_count;
    j["view_count"] = view_count;
    j["bookmark_count"] = bookmark_count;
    
    // Geographic data
    if (latitude.has_value()) j["latitude"] = latitude.value();
    if (longitude.has_value()) j["longitude"] = longitude.value();
    j["location_name"] = location_name;
    
    // Content moderation
    j["is_sensitive"] = is_sensitive;
    j["is_nsfw"] = is_nsfw;
    j["contains_spoilers"] = contains_spoilers;
    j["spam_score"] = spam_score;
    j["toxicity_score"] = toxicity_score;
    j["detected_languages"] = detected_languages;
    
    // Timestamps
    j["created_at"] = created_at;
    j["updated_at"] = updated_at;
    if (scheduled_at.has_value()) j["scheduled_at"] = scheduled_at.value();
    if (deleted_at.has_value()) j["deleted_at"] = deleted_at.value();
    
    // Client information
    j["client_name"] = client_name;
    j["client_version"] = client_version;
    j["user_agent"] = user_agent;
    j["ip_address"] = ip_address;
    
    // Additional metadata
    j["metadata"] = metadata;
    j["is_promoted"] = is_promoted;
    j["is_verified_author"] = is_verified_author;
    j["allow_replies"] = allow_replies;
    j["allow_renotes"] = allow_renotes;
    j["allow_quotes"] = allow_quotes;
    
    return j;
}

void Note::from_json(const nlohmann::json& j) {
    // Core fields
    j.at("note_id").get_to(note_id);
    j.at("author_id").get_to(author_id);
    if (j.contains("author_username")) j.at("author_username").get_to(author_username);
    j.at("content").get_to(content);
    if (j.contains("raw_content")) j.at("raw_content").get_to(raw_content);
    if (j.contains("processed_content")) j.at("processed_content").get_to(processed_content);
    
    // Relationships
    if (j.contains("reply_to_id") && !j["reply_to_id"].is_null()) {
        reply_to_id = j["reply_to_id"].get<std::string>();
    }
    if (j.contains("reply_to_user_id") && !j["reply_to_user_id"].is_null()) {
        reply_to_user_id = j["reply_to_user_id"].get<std::string>();
    }
    if (j.contains("renote_of_id") && !j["renote_of_id"].is_null()) {
        renote_of_id = j["renote_of_id"].get<std::string>();
    }
    if (j.contains("quote_of_id") && !j["quote_of_id"].is_null()) {
        quote_of_id = j["quote_of_id"].get<std::string>();
    }
    if (j.contains("thread_id") && !j["thread_id"].is_null()) {
        thread_id = j["thread_id"].get<std::string>();
    }
    if (j.contains("thread_position")) j.at("thread_position").get_to(thread_position);
    
    // Classification
    if (j.contains("type")) type = static_cast<NoteType>(j["type"].get<int>());
    if (j.contains("visibility")) visibility = static_cast<NoteVisibility>(j["visibility"].get<int>());
    if (j.contains("status")) status = static_cast<NoteStatus>(j["status"].get<int>());
    if (j.contains("content_warning")) content_warning = static_cast<ContentWarning>(j["content_warning"].get<int>());
    
    // Content features
    if (j.contains("mentioned_user_ids")) j.at("mentioned_user_ids").get_to(mentioned_user_ids);
    if (j.contains("mentioned_usernames")) j.at("mentioned_usernames").get_to(mentioned_usernames);
    if (j.contains("hashtags")) j.at("hashtags").get_to(hashtags);
    if (j.contains("urls")) j.at("urls").get_to(urls);
    if (j.contains("attachment_ids")) j.at("attachment_ids").get_to(attachment_ids);
    
    // Media attachments
    if (j.contains("attachments")) {
        attachments = AttachmentCollection::from_json(j["attachments"]);
        // Ensure note_id is set for all attachments
        attachments.set_note_id(note_id);
    }
    
    // Engagement metrics
    if (j.contains("like_count")) j.at("like_count").get_to(like_count);
    if (j.contains("renote_count")) j.at("renote_count").get_to(renote_count);
    if (j.contains("reply_count")) j.at("reply_count").get_to(reply_count);
    if (j.contains("quote_count")) j.at("quote_count").get_to(quote_count);
    if (j.contains("view_count")) j.at("view_count").get_to(view_count);
    if (j.contains("bookmark_count")) j.at("bookmark_count").get_to(bookmark_count);
    
    // Geographic data
    if (j.contains("latitude") && !j["latitude"].is_null()) {
        latitude = j["latitude"].get<double>();
    }
    if (j.contains("longitude") && !j["longitude"].is_null()) {
        longitude = j["longitude"].get<double>();
    }
    if (j.contains("location_name")) j.at("location_name").get_to(location_name);
    
    // Content moderation
    if (j.contains("is_sensitive")) j.at("is_sensitive").get_to(is_sensitive);
    if (j.contains("is_nsfw")) j.at("is_nsfw").get_to(is_nsfw);
    if (j.contains("contains_spoilers")) j.at("contains_spoilers").get_to(contains_spoilers);
    if (j.contains("spam_score")) j.at("spam_score").get_to(spam_score);
    if (j.contains("toxicity_score")) j.at("toxicity_score").get_to(toxicity_score);
    if (j.contains("detected_languages")) j.at("detected_languages").get_to(detected_languages);
    
    // Timestamps
    if (j.contains("created_at")) j.at("created_at").get_to(created_at);
    if (j.contains("updated_at")) j.at("updated_at").get_to(updated_at);
    if (j.contains("scheduled_at") && !j["scheduled_at"].is_null()) {
        scheduled_at = j["scheduled_at"].get<std::time_t>();
    }
    if (j.contains("deleted_at") && !j["deleted_at"].is_null()) {
        deleted_at = j["deleted_at"].get<std::time_t>();
    }
    
    // Client information
    if (j.contains("client_name")) j.at("client_name").get_to(client_name);
    if (j.contains("client_version")) j.at("client_version").get_to(client_version);
    if (j.contains("user_agent")) j.at("user_agent").get_to(user_agent);
    if (j.contains("ip_address")) j.at("ip_address").get_to(ip_address);
    
    // Additional metadata
    if (j.contains("metadata")) j.at("metadata").get_to(metadata);
    if (j.contains("is_promoted")) j.at("is_promoted").get_to(is_promoted);
    if (j.contains("is_verified_author")) j.at("is_verified_author").get_to(is_verified_author);
    if (j.contains("allow_replies")) j.at("allow_replies").get_to(allow_replies);
    if (j.contains("allow_renotes")) j.at("allow_renotes").get_to(allow_renotes);
    if (j.contains("allow_quotes")) j.at("allow_quotes").get_to(allow_quotes);
}

std::string Note::to_string() const {
    return to_json().dump(2);
}

// Display helper methods
std::string Note::get_display_content() const {
    if (is_sensitive || is_nsfw || contains_spoilers) {
        return "[Content hidden - " + content_warning_to_string(content_warning) + "]";
    }
    return processed_content.empty() ? content : processed_content;
}

std::string Note::get_preview_text(size_t max_length) const {
    std::string preview = content;
    if (preview.length() > max_length) {
        preview = preview.substr(0, max_length - 3) + "...";
    }
    return preview;
}

std::string Note::get_formatted_timestamp() const {
    std::tm* tm_info = std::localtime(&created_at);
    std::stringstream ss;
    ss << std::put_time(tm_info, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

std::string Note::get_relative_timestamp() const {
    auto now = std::time(nullptr);
    auto diff = now - created_at;
    
    if (diff < 60) return std::to_string(diff) + "s";
    if (diff < 3600) return std::to_string(diff / 60) + "m";
    if (diff < 86400) return std::to_string(diff / 3600) + "h";
    if (diff < 604800) return std::to_string(diff / 86400) + "d";
    return std::to_string(diff / 604800) + "w";
}

std::string Note::get_engagement_summary() const {
    std::stringstream ss;
    ss << like_count << " likes, " << renote_count << " renotes, " 
       << reply_count << " replies, " << quote_count << " quotes";
    return ss.str();
}

// Comparison operators
bool Note::operator==(const Note& other) const {
    return note_id == other.note_id;
}

bool Note::operator!=(const Note& other) const {
    return !(*this == other);
}

bool Note::operator<(const Note& other) const {
    return created_at < other.created_at;
}

// Utility methods
size_t Note::get_content_length() const {
    return content.length();
}

size_t Note::get_remaining_characters() const {
    return MAX_CONTENT_LENGTH - content.length();
}

bool Note::is_empty() const {
    return content.empty();
}

bool Note::is_deleted() const {
    return status == NoteStatus::DELETED;
}

bool Note::is_draft() const {
    return status == NoteStatus::DRAFT;
}

bool Note::is_public() const {
    return visibility == NoteVisibility::PUBLIC;
}

bool Note::has_attachments() const {
    return !attachment_ids.empty();
}

bool Note::has_mentions() const {
    return !mentioned_user_ids.empty();
}

bool Note::has_hashtags() const {
    return !hashtags.empty();
}

bool Note::has_urls() const {
    return !urls.empty();
}

bool Note::is_reply() const {
    return type == NoteType::REPLY;
}

bool Note::is_renote() const {
    return type == NoteType::RENOTE;
}

bool Note::is_quote() const {
    return type == NoteType::QUOTE;
}

bool Note::is_original() const {
    return type == NoteType::ORIGINAL;
}

// Content analysis methods
double Note::get_readability_score() const {
    // Simple readability calculation (Flesch Reading Ease approximation)
    int words = count_words();
    int sentences = count_sentences();
    
    if (words == 0 || sentences == 0) return 0.0;
    
    double avg_sentence_length = static_cast<double>(words) / sentences;
    double score = 206.835 - (1.015 * avg_sentence_length);
    
    return std::max(0.0, std::min(100.0, score));
}

int Note::count_words() const {
    std::istringstream iss(content);
    std::string word;
    int count = 0;
    while (iss >> word) {
        count++;
    }
    return count;
}

int Note::count_sentences() const {
    return std::count(content.begin(), content.end(), '.') +
           std::count(content.begin(), content.end(), '!') +
           std::count(content.begin(), content.end(), '?');
}

std::vector<std::string> Note::get_keywords() const {
    std::vector<std::string> keywords;
    std::istringstream iss(content);
    std::string word;
    
    while (iss >> word) {
        // Remove punctuation
        word.erase(std::remove_if(word.begin(), word.end(), ::ispunct), word.end());
        
        // Convert to lowercase
        std::transform(word.begin(), word.end(), word.begin(), ::tolower);
        
        // Skip common stop words and short words
        if (word.length() > 3 && 
            word != "this" && word != "that" && word != "with" && 
            word != "have" && word != "they" && word != "were" && 
            word != "from" && word != "what" && word != "your") {
            keywords.push_back(word);
        }
    }
    
    return keywords;
}

// Age and freshness methods
std::time_t Note::get_age_seconds() const {
    return std::time(nullptr) - created_at;
}

std::time_t Note::get_age_minutes() const {
    return get_age_seconds() / 60;
}

std::time_t Note::get_age_hours() const {
    return get_age_seconds() / 3600;
}

std::time_t Note::get_age_days() const {
    return get_age_seconds() / 86400;
}

bool Note::is_fresh(int minutes) const {
    return get_age_minutes() <= minutes;
}

bool Note::is_recent(int hours) const {
    return get_age_hours() <= hours;
}

// Statistical helper methods
double Note::get_likes_per_hour() const {
    auto age_hours = get_age_hours();
    if (age_hours == 0) age_hours = 1;
    return static_cast<double>(like_count) / age_hours;
}

double Note::get_renotes_per_hour() const {
    auto age_hours = get_age_hours();
    if (age_hours == 0) age_hours = 1;
    return static_cast<double>(renote_count) / age_hours;
}

double Note::get_replies_per_hour() const {
    auto age_hours = get_age_hours();
    if (age_hours == 0) age_hours = 1;
    return static_cast<double>(reply_count) / age_hours;
}

double Note::get_engagement_velocity() const {
    auto age_hours = get_age_hours();
    if (age_hours == 0) age_hours = 1;
    return static_cast<double>(get_total_engagement()) / age_hours;
}

// Private helper methods
void Note::update_timestamps() {
    updated_at = std::time(nullptr);
}

std::string Note::sanitize_content(const std::string& input_content) const {
    std::string sanitized = input_content;
    
    // Remove null bytes and control characters
    sanitized.erase(std::remove_if(sanitized.begin(), sanitized.end(), 
        [](char c) { return c < 32 && c != '\n' && c != '\r' && c != '\t'; }), 
        sanitized.end());
    
    return sanitized;
}

std::vector<std::string> Note::extract_tokens(const std::string& text, const std::string& prefix) const {
    std::vector<std::string> tokens;
    std::regex token_regex(prefix + R"(([a-zA-Z0-9_]+))");
    std::sregex_iterator begin(text.begin(), text.end(), token_regex);
    std::sregex_iterator end;
    
    for (auto it = begin; it != end; ++it) {
        tokens.push_back((*it)[1].str());
    }
    
    return tokens;
}

bool Note::is_valid_mention(const std::string& mention) const {
    return !mention.empty() && mention.length() <= 15 && 
           std::all_of(mention.begin(), mention.end(), 
               [](char c) { return std::isalnum(c) || c == '_'; });
}

bool Note::is_valid_hashtag(const std::string& hashtag) const {
    return !hashtag.empty() && hashtag.length() <= 100 && 
           std::all_of(hashtag.begin(), hashtag.end(), 
               [](char c) { return std::isalnum(c) || c == '_'; });
}

bool Note::is_valid_url(const std::string& url) const {
    std::regex url_regex(R"(^https?://[^\s/$.?#].[^\s]*$)");
    return std::regex_match(url, url_regex);
}

double Note::calculate_content_quality_score() const {
    double score = 1.0;
    
    // Penalize spam indicators
    score -= spam_score * 0.5;
    score -= toxicity_score * 0.7;
    
    // Reward good engagement
    if (view_count > 0) {
        score += calculate_engagement_rate() * 0.3;
    }
    
    // Reward readability
    double readability = get_readability_score();
    if (readability > 50) {
        score += 0.2;
    }
    
    return std::max(0.0, std::min(1.0, score));
}

std::string Note::process_mentions(const std::string& input_content) const {
    std::string processed = input_content;
    
    for (const auto& username : mentioned_usernames) {
        std::string mention = "@" + username;
        std::string link = "<a href=\"/user/" + username + "\">" + mention + "</a>";
        
        size_t pos = 0;
        while ((pos = processed.find(mention, pos)) != std::string::npos) {
            processed.replace(pos, mention.length(), link);
            pos += link.length();
        }
    }
    
    return processed;
}

std::string Note::process_hashtags(const std::string& input_content) const {
    std::string processed = input_content;
    
    for (const auto& hashtag : hashtags) {
        std::string tag = "#" + hashtag;
        std::string link = "<a href=\"/hashtag/" + hashtag + "\">" + tag + "</a>";
        
        size_t pos = 0;
        while ((pos = processed.find(tag, pos)) != std::string::npos) {
            processed.replace(pos, tag.length(), link);
            pos += link.length();
        }
    }
    
    return processed;
}

std::string Note::process_urls(const std::string& input_content) const {
    std::string processed = input_content;
    
    for (const auto& url : urls) {
        std::string link = "<a href=\"" + url + "\" target=\"_blank\">" + url + "</a>";
        
        size_t pos = 0;
        while ((pos = processed.find(url, pos)) != std::string::npos) {
            processed.replace(pos, url.length(), link);
            pos += link.length();
        }
    }
    
    return processed;
}

std::string Note::highlight_content_features(const std::string& input_content) const {
    std::string highlighted = input_content;
    highlighted = process_mentions(highlighted);
    highlighted = process_hashtags(highlighted);
    highlighted = process_urls(highlighted);
    return highlighted;
}

// Utility function implementations
std::string note_type_to_string(NoteType type) {
    switch (type) {
        case NoteType::ORIGINAL: return "original";
        case NoteType::REPLY: return "reply";
        case NoteType::RENOTE: return "renote";
        case NoteType::QUOTE: return "quote";
        case NoteType::THREAD: return "thread";
        default: return "unknown";
    }
}

NoteType string_to_note_type(const std::string& type_str) {
    if (type_str == "original") return NoteType::ORIGINAL;
    if (type_str == "reply") return NoteType::REPLY;
    if (type_str == "renote") return NoteType::RENOTE;
    if (type_str == "quote") return NoteType::QUOTE;
    if (type_str == "thread") return NoteType::THREAD;
    return NoteType::ORIGINAL;
}

std::string note_visibility_to_string(NoteVisibility visibility) {
    switch (visibility) {
        case NoteVisibility::PUBLIC: return "public";
        case NoteVisibility::FOLLOWERS_ONLY: return "followers_only";
        case NoteVisibility::MENTIONED_ONLY: return "mentioned_only";
        case NoteVisibility::PRIVATE: return "private";
        case NoteVisibility::CIRCLE: return "circle";
        default: return "public";
    }
}

NoteVisibility string_to_note_visibility(const std::string& visibility_str) {
    if (visibility_str == "public") return NoteVisibility::PUBLIC;
    if (visibility_str == "followers_only") return NoteVisibility::FOLLOWERS_ONLY;
    if (visibility_str == "mentioned_only") return NoteVisibility::MENTIONED_ONLY;
    if (visibility_str == "private") return NoteVisibility::PRIVATE;
    if (visibility_str == "circle") return NoteVisibility::CIRCLE;
    return NoteVisibility::PUBLIC;
}

std::string note_status_to_string(NoteStatus status) {
    switch (status) {
        case NoteStatus::ACTIVE: return "active";
        case NoteStatus::DELETED: return "deleted";
        case NoteStatus::HIDDEN: return "hidden";
        case NoteStatus::FLAGGED: return "flagged";
        case NoteStatus::DRAFT: return "draft";
        case NoteStatus::SCHEDULED: return "scheduled";
        default: return "active";
    }
}

NoteStatus string_to_note_status(const std::string& status_str) {
    if (status_str == "active") return NoteStatus::ACTIVE;
    if (status_str == "deleted") return NoteStatus::DELETED;
    if (status_str == "hidden") return NoteStatus::HIDDEN;
    if (status_str == "flagged") return NoteStatus::FLAGGED;
    if (status_str == "draft") return NoteStatus::DRAFT;
    if (status_str == "scheduled") return NoteStatus::SCHEDULED;
    return NoteStatus::ACTIVE;
}

std::string content_warning_to_string(ContentWarning warning) {
    switch (warning) {
        case ContentWarning::NONE: return "none";
        case ContentWarning::SENSITIVE: return "sensitive";
        case ContentWarning::VIOLENCE: return "violence";
        case ContentWarning::ADULT: return "adult";
        case ContentWarning::SPOILER: return "spoiler";
        case ContentWarning::HARASSMENT: return "harassment";
        default: return "none";
    }
}

ContentWarning string_to_content_warning(const std::string& warning_str) {
    if (warning_str == "none") return ContentWarning::NONE;
    if (warning_str == "sensitive") return ContentWarning::SENSITIVE;
    if (warning_str == "violence") return ContentWarning::VIOLENCE;
    if (warning_str == "adult") return ContentWarning::ADULT;
    if (warning_str == "spoiler") return ContentWarning::SPOILER;
    if (warning_str == "harassment") return ContentWarning::HARASSMENT;
    return ContentWarning::NONE;
}

// JSON conversion helper implementations
void to_json(nlohmann::json& j, const Note& note) {
    j = note.to_json();
}

void from_json(const nlohmann::json& j, Note& note) {
    note.from_json(j);
}

// Supporting structure implementations
nlohmann::json NoteMention::to_json() const {
    return nlohmann::json{
        {"user_id", user_id},
        {"username", username},
        {"start_position", start_position},
        {"end_position", end_position},
        {"is_verified", is_verified},
        {"mentioned_at", mentioned_at}
    };
}

void NoteMention::from_json(const nlohmann::json& j) {
    j.at("user_id").get_to(user_id);
    j.at("username").get_to(username);
    j.at("start_position").get_to(start_position);
    j.at("end_position").get_to(end_position);
    if (j.contains("is_verified")) j.at("is_verified").get_to(is_verified);
    if (j.contains("mentioned_at")) j.at("mentioned_at").get_to(mentioned_at);
}

nlohmann::json NoteHashtag::to_json() const {
    return nlohmann::json{
        {"tag", tag},
        {"start_position", start_position},
        {"end_position", end_position},
        {"trending_rank", trending_rank},
        {"first_used", first_used}
    };
}

void NoteHashtag::from_json(const nlohmann::json& j) {
    j.at("tag").get_to(tag);
    j.at("start_position").get_to(start_position);
    j.at("end_position").get_to(end_position);
    if (j.contains("trending_rank")) j.at("trending_rank").get_to(trending_rank);
    if (j.contains("first_used")) j.at("first_used").get_to(first_used);
}

nlohmann::json NoteUrl::to_json() const {
    return nlohmann::json{
        {"original_url", original_url},
        {"shortened_url", shortened_url},
        {"expanded_url", expanded_url},
        {"title", title},
        {"description", description},
        {"image_url", image_url},
        {"start_position", start_position},
        {"end_position", end_position},
        {"is_secure", is_secure},
        {"last_checked", last_checked}
    };
}

void NoteUrl::from_json(const nlohmann::json& j) {
    j.at("original_url").get_to(original_url);
    if (j.contains("shortened_url")) j.at("shortened_url").get_to(shortened_url);
    if (j.contains("expanded_url")) j.at("expanded_url").get_to(expanded_url);
    if (j.contains("title")) j.at("title").get_to(title);
    if (j.contains("description")) j.at("description").get_to(description);
    if (j.contains("image_url")) j.at("image_url").get_to(image_url);
    j.at("start_position").get_to(start_position);
    j.at("end_position").get_to(end_position);
    if (j.contains("is_secure")) j.at("is_secure").get_to(is_secure);
    if (j.contains("last_checked")) j.at("last_checked").get_to(last_checked);
}

nlohmann::json NoteMetrics::to_json() const {
    return nlohmann::json{
        {"note_id", note_id},
        {"calculated_at", calculated_at},
        {"total_likes", total_likes},
        {"total_renotes", total_renotes},
        {"total_replies", total_replies},
        {"total_quotes", total_quotes},
        {"total_views", total_views},
        {"total_bookmarks", total_bookmarks},
        {"total_shares", total_shares},
        {"unique_viewers", unique_viewers},
        {"follower_views", follower_views},
        {"non_follower_views", non_follower_views},
        {"hourly_engagement", hourly_engagement},
        {"daily_engagement", daily_engagement},
        {"country_views", country_views},
        {"city_views", city_views},
        {"age_group_views", age_group_views},
        {"gender_views", gender_views},
        {"engagement_rate", engagement_rate},
        {"virality_score", virality_score},
        {"reach_score", reach_score},
        {"quality_score", quality_score}
    };
}

void NoteMetrics::from_json(const nlohmann::json& j) {
    j.at("note_id").get_to(note_id);
    j.at("calculated_at").get_to(calculated_at);
    if (j.contains("total_likes")) j.at("total_likes").get_to(total_likes);
    if (j.contains("total_renotes")) j.at("total_renotes").get_to(total_renotes);
    if (j.contains("total_replies")) j.at("total_replies").get_to(total_replies);
    if (j.contains("total_quotes")) j.at("total_quotes").get_to(total_quotes);
    if (j.contains("total_views")) j.at("total_views").get_to(total_views);
    if (j.contains("total_bookmarks")) j.at("total_bookmarks").get_to(total_bookmarks);
    if (j.contains("total_shares")) j.at("total_shares").get_to(total_shares);
    if (j.contains("unique_viewers")) j.at("unique_viewers").get_to(unique_viewers);
    if (j.contains("follower_views")) j.at("follower_views").get_to(follower_views);
    if (j.contains("non_follower_views")) j.at("non_follower_views").get_to(non_follower_views);
    if (j.contains("hourly_engagement")) j.at("hourly_engagement").get_to(hourly_engagement);
    if (j.contains("daily_engagement")) j.at("daily_engagement").get_to(daily_engagement);
    if (j.contains("country_views")) j.at("country_views").get_to(country_views);
    if (j.contains("city_views")) j.at("city_views").get_to(city_views);
    if (j.contains("age_group_views")) j.at("age_group_views").get_to(age_group_views);
    if (j.contains("gender_views")) j.at("gender_views").get_to(gender_views);
    if (j.contains("engagement_rate")) j.at("engagement_rate").get_to(engagement_rate);
    if (j.contains("virality_score")) j.at("virality_score").get_to(virality_score);
    if (j.contains("reach_score")) j.at("reach_score").get_to(reach_score);
    if (j.contains("quality_score")) j.at("quality_score").get_to(quality_score);
}

void to_json(nlohmann::json& j, const NoteMention& mention) {
    j = mention.to_json();
}

void from_json(const nlohmann::json& j, NoteMention& mention) {
    mention.from_json(j);
}

void to_json(nlohmann::json& j, const NoteHashtag& hashtag) {
    j = hashtag.to_json();
}

void from_json(const nlohmann::json& j, NoteHashtag& hashtag) {
    hashtag.from_json(j);
}

void to_json(nlohmann::json& j, const NoteUrl& url) {
    j = url.to_json();
}

void from_json(const nlohmann::json& j, NoteUrl& url) {
    url.from_json(j);
}

void to_json(nlohmann::json& j, const NoteMetrics& metrics) {
    j = metrics.to_json();
}

void from_json(const nlohmann::json& j, NoteMetrics& metrics) {
    metrics.from_json(j);
}

} // namespace sonet::note::models