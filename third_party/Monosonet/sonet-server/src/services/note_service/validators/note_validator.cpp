/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "note_validator.h"
#include "../../core/utils/string_utils.h"
#include "../../core/validation/input_sanitizer.h"
#include <spdlog/spdlog.h>
#include <regex>
#include <algorithm>
#include <unordered_set>
#include <cctype>

namespace sonet::note::validators {

// Constructor
NoteValidator::NoteValidator() {
    // Initialize regex patterns for validation
    url_regex_ = std::regex(R"(https?://(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*))");
    mention_regex_ = std::regex(R"(@[a-zA-Z0-9_]{1,15})");
    hashtag_regex_ = std::regex(R"(#[a-zA-Z0-9_]{1,139})");
    email_regex_ = std::regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
    
    // Initialize spam and profanity detection patterns
    initialize_spam_patterns();
    initialize_profanity_filters();
    
    spdlog::info("NoteValidator initialized with comprehensive validation rules");
}

// ========== CORE VALIDATION METHODS ==========

ValidationResult NoteValidator::validate_note(const json& note_data) const {
    ValidationResult result;
    result.is_valid = true;
    
    try {
        // Basic structure validation
        if (!validate_basic_structure(note_data, result)) {
            return result;
        }
        
        // Content validation
        if (!validate_content(note_data, result)) {
            return result;
        }
        
        // Metadata validation
        if (!validate_metadata(note_data, result)) {
            return result;
        }
        
        // Attachment validation
        if (!validate_attachments(note_data, result)) {
            return result;
        }
        
        // Relationship validation
        if (!validate_relationships(note_data, result)) {
            return result;
        }
        
        // Privacy and visibility validation
        if (!validate_visibility_settings(note_data, result)) {
            return result;
        }
        
        // Content policy validation
        if (!validate_content_policy(note_data, result)) {
            return result;
        }
        
        // Advanced security checks
        if (!validate_security_constraints(note_data, result)) {
            return result;
        }
        
        spdlog::debug("Note validation completed successfully");
        
    } catch (const std::exception& e) {
        result.is_valid = false;
        result.error_code = "VALIDATION_ERROR";
        result.error_message = "Validation failed: " + std::string(e.what());
        spdlog::error("Note validation exception: {}", e.what());
    }
    
    return result;
}

ValidationResult NoteValidator::validate_content_only(const std::string& content) const {
    ValidationResult result;
    result.is_valid = true;
    
    // Length validation
    if (!validate_content_length(content, result)) {
        return result;
    }
    
    // Character validation
    if (!validate_character_set(content, result)) {
        return result;
    }
    
    // Spam detection
    if (!validate_against_spam(content, result)) {
        return result;
    }
    
    // Profanity filtering
    if (!validate_against_profanity(content, result)) {
        return result;
    }
    
    // URL validation
    if (!validate_urls_in_content(content, result)) {
        return result;
    }
    
    return result;
}

ValidationResult NoteValidator::validate_update_request(const json& update_data, const models::Note& existing_note) const {
    ValidationResult result;
    result.is_valid = true;
    
    // Check if note is editable (within edit window)
    auto now = std::time(nullptr);
    if (now - existing_note.created_at > MAX_EDIT_WINDOW_MINUTES * 60) {
        result.is_valid = false;
        result.error_code = "EDIT_WINDOW_EXPIRED";
        result.error_message = "Note can no longer be edited (edit window expired)";
        return result;
    }
    
    // Validate update fields
    if (update_data.contains("content")) {
        std::string new_content = update_data["content"];
        auto content_result = validate_content_only(new_content);
        if (!content_result.is_valid) {
            return content_result;
        }
        
        // Check for significant content changes (for edit tracking)
        if (calculate_content_similarity(existing_note.content, new_content) < MIN_EDIT_SIMILARITY) {
            result.warnings.push_back("Significant content change detected - edit will be tracked");
        }
    }
    
    // Validate attachment changes
    if (update_data.contains("attachments")) {
        if (!validate_attachments(update_data, result)) {
            return result;
        }
    }
    
    // Validate sensitivity changes
    if (update_data.contains("is_sensitive")) {
        bool new_sensitivity = update_data["is_sensitive"];
        if (new_sensitivity != existing_note.is_sensitive) {
            result.warnings.push_back("Content sensitivity setting changed");
        }
    }
    
    return result;
}

// ========== BASIC STRUCTURE VALIDATION ==========

bool NoteValidator::validate_basic_structure(const json& note_data, ValidationResult& result) const {
    // Check required fields
    if (!note_data.contains("content")) {
        result.is_valid = false;
        result.error_code = "MISSING_CONTENT";
        result.error_message = "Note content is required";
        return false;
    }
    
    if (!note_data["content"].is_string()) {
        result.is_valid = false;
        result.error_code = "INVALID_CONTENT_TYPE";
        result.error_message = "Note content must be a string";
        return false;
    }
    
    // Check optional fields types
    if (note_data.contains("visibility") && !note_data["visibility"].is_string()) {
        result.is_valid = false;
        result.error_code = "INVALID_VISIBILITY_TYPE";
        result.error_message = "Visibility must be a string";
        return false;
    }
    
    if (note_data.contains("is_sensitive") && !note_data["is_sensitive"].is_boolean()) {
        result.is_valid = false;
        result.error_code = "INVALID_SENSITIVE_TYPE";
        result.error_message = "is_sensitive must be a boolean";
        return false;
    }
    
    if (note_data.contains("attachments") && !note_data["attachments"].is_array()) {
        result.is_valid = false;
        result.error_code = "INVALID_ATTACHMENTS_TYPE";
        result.error_message = "Attachments must be an array";
        return false;
    }
    
    return true;
}

// ========== CONTENT VALIDATION ==========

bool NoteValidator::validate_content(const json& note_data, ValidationResult& result) const {
    std::string content = note_data["content"];
    
    // Length validation
    if (!validate_content_length(content, result)) {
        return false;
    }
    
    // Empty content check
    if (core::utils::trim(content).empty()) {
        result.is_valid = false;
        result.error_code = "EMPTY_CONTENT";
        result.error_message = "Note content cannot be empty";
        return false;
    }
    
    // Character set validation
    if (!validate_character_set(content, result)) {
        return false;
    }
    
    // Validate mentions, hashtags, and URLs
    if (!validate_content_features(content, result)) {
        return false;
    }
    
    // Spam detection
    if (!validate_against_spam(content, result)) {
        return false;
    }
    
    // Profanity filtering
    if (!validate_against_profanity(content, result)) {
        return false;
    }
    
    return true;
}

bool NoteValidator::validate_content_length(const std::string& content, ValidationResult& result) const {
    size_t length = core::utils::utf8_length(content);
    
    if (length > MAX_CONTENT_LENGTH) {
        result.is_valid = false;
        result.error_code = "CONTENT_TOO_LONG";
        result.error_message = "Note content exceeds maximum length of " + std::to_string(MAX_CONTENT_LENGTH) + " characters";
        result.details["current_length"] = length;
        result.details["max_length"] = MAX_CONTENT_LENGTH;
        return false;
    }
    
    if (length > MAX_CONTENT_LENGTH * 0.9) {
        result.warnings.push_back("Content is approaching maximum length limit");
    }
    
    return true;
}

bool NoteValidator::validate_character_set(const std::string& content, ValidationResult& result) const {
    // Check for null bytes and control characters
    for (char c : content) {
        if (c == '\0') {
            result.is_valid = false;
            result.error_code = "INVALID_CHARACTERS";
            result.error_message = "Content contains null bytes";
            return false;
        }
        
        // Allow common control characters (newline, tab, carriage return)
        if (std::iscntrl(c) && c != '\n' && c != '\t' && c != '\r') {
            result.is_valid = false;
            result.error_code = "INVALID_CHARACTERS";
            result.error_message = "Content contains invalid control characters";
            return false;
        }
    }
    
    // Check for excessive whitespace
    if (count_consecutive_whitespace(content) > MAX_CONSECUTIVE_WHITESPACE) {
        result.warnings.push_back("Content contains excessive whitespace");
    }
    
    return true;
}

bool NoteValidator::validate_content_features(const std::string& content, ValidationResult& result) const {
    // Extract and validate mentions
    auto mentions = extract_mentions(content);
    if (mentions.size() > MAX_MENTIONS) {
        result.is_valid = false;
        result.error_code = "TOO_MANY_MENTIONS";
        result.error_message = "Maximum " + std::to_string(MAX_MENTIONS) + " mentions allowed per note";
        return false;
    }
    
    // Extract and validate hashtags
    auto hashtags = extract_hashtags(content);
    if (hashtags.size() > MAX_HASHTAGS) {
        result.is_valid = false;
        result.error_code = "TOO_MANY_HASHTAGS";
        result.error_message = "Maximum " + std::to_string(MAX_HASHTAGS) + " hashtags allowed per note";
        return false;
    }
    
    // Extract and validate URLs
    auto urls = extract_urls(content);
    if (urls.size() > MAX_URLS) {
        result.is_valid = false;
        result.error_code = "TOO_MANY_URLS";
        result.error_message = "Maximum " + std::to_string(MAX_URLS) + " URLs allowed per note";
        return false;
    }
    
    // Validate URL safety
    for (const auto& url : urls) {
        if (!is_safe_url(url)) {
            result.is_valid = false;
            result.error_code = "UNSAFE_URL";
            result.error_message = "Content contains potentially unsafe URL";
            return false;
        }
    }
    
    return true;
}

// ========== METADATA VALIDATION ==========

bool NoteValidator::validate_metadata(const json& note_data, ValidationResult& result) const {
    // Validate scheduled publishing
    if (note_data.contains("scheduled_at")) {
        if (!validate_scheduled_time(note_data["scheduled_at"], result)) {
            return false;
        }
    }
    
    // Validate tags
    if (note_data.contains("tags") && note_data["tags"].is_array()) {
        if (!validate_tags(note_data["tags"], result)) {
            return false;
        }
    }
    
    // Validate language setting
    if (note_data.contains("language")) {
        if (!validate_language_code(note_data["language"], result)) {
            return false;
        }
    }
    
    // Validate location data
    if (note_data.contains("location")) {
        if (!validate_location_data(note_data["location"], result)) {
            return false;
        }
    }
    
    return true;
}

bool NoteValidator::validate_scheduled_time(const json& scheduled_time, ValidationResult& result) const {
    if (!scheduled_time.is_number()) {
        result.is_valid = false;
        result.error_code = "INVALID_SCHEDULED_TIME";
        result.error_message = "Scheduled time must be a timestamp";
        return false;
    }
    
    time_t scheduled_timestamp = scheduled_time;
    time_t now = std::time(nullptr);
    
    // Must be in the future
    if (scheduled_timestamp <= now) {
        result.is_valid = false;
        result.error_code = "PAST_SCHEDULED_TIME";
        result.error_message = "Scheduled time must be in the future";
        return false;
    }
    
    // Cannot be more than 1 year in the future
    time_t max_future = now + (365 * 24 * 3600);
    if (scheduled_timestamp > max_future) {
        result.is_valid = false;
        result.error_code = "TOO_FAR_FUTURE";
        result.error_message = "Cannot schedule notes more than 1 year in advance";
        return false;
    }
    
    return true;
}

bool NoteValidator::validate_tags(const json& tags, ValidationResult& result) const {
    if (tags.size() > MAX_CUSTOM_TAGS) {
        result.is_valid = false;
        result.error_code = "TOO_MANY_TAGS";
        result.error_message = "Maximum " + std::to_string(MAX_CUSTOM_TAGS) + " custom tags allowed";
        return false;
    }
    
    for (const auto& tag : tags) {
        if (!tag.is_string()) {
            result.is_valid = false;
            result.error_code = "INVALID_TAG_TYPE";
            result.error_message = "All tags must be strings";
            return false;
        }
        
        std::string tag_str = tag;
        if (tag_str.length() > MAX_TAG_LENGTH) {
            result.is_valid = false;
            result.error_code = "TAG_TOO_LONG";
            result.error_message = "Tag length cannot exceed " + std::to_string(MAX_TAG_LENGTH) + " characters";
            return false;
        }
        
        // Validate tag format (alphanumeric and underscores only)
        if (!std::regex_match(tag_str, std::regex("^[a-zA-Z0-9_]+$"))) {
            result.is_valid = false;
            result.error_code = "INVALID_TAG_FORMAT";
            result.error_message = "Tags can only contain letters, numbers, and underscores";
            return false;
        }
    }
    
    return true;
}

// ========== ATTACHMENT VALIDATION ==========

bool NoteValidator::validate_attachments(const json& note_data, ValidationResult& result) const {
    if (!note_data.contains("attachments")) {
        return true; // No attachments is valid
    }
    
    const auto& attachments = note_data["attachments"];
    
    if (attachments.size() > MAX_ATTACHMENTS) {
        result.is_valid = false;
        result.error_code = "TOO_MANY_ATTACHMENTS";
        result.error_message = "Maximum " + std::to_string(MAX_ATTACHMENTS) + " attachments allowed per note";
        return false;
    }
    
    size_t total_attachment_size = 0;
    
    for (const auto& attachment : attachments) {
        if (!validate_single_attachment(attachment, result)) {
            return false;
        }
        
        if (attachment.contains("file_size")) {
            total_attachment_size += attachment["file_size"];
        }
    }
    
    // Check total attachment size
    if (total_attachment_size > MAX_TOTAL_ATTACHMENT_SIZE) {
        result.is_valid = false;
        result.error_code = "ATTACHMENTS_TOO_LARGE";
        result.error_message = "Total attachment size exceeds limit";
        result.details["total_size"] = total_attachment_size;
        result.details["max_size"] = MAX_TOTAL_ATTACHMENT_SIZE;
        return false;
    }
    
    return true;
}

bool NoteValidator::validate_single_attachment(const json& attachment, ValidationResult& result) const {
    // Check required fields
    if (!attachment.contains("type")) {
        result.is_valid = false;
        result.error_code = "MISSING_ATTACHMENT_TYPE";
        result.error_message = "Attachment type is required";
        return false;
    }
    
    std::string type = attachment["type"];
    
    // Validate attachment type
    if (VALID_ATTACHMENT_TYPES.find(type) == VALID_ATTACHMENT_TYPES.end()) {
        result.is_valid = false;
        result.error_code = "INVALID_ATTACHMENT_TYPE";
        result.error_message = "Invalid attachment type: " + type;
        return false;
    }
    
    // Type-specific validation
    if (type == "image" || type == "video" || type == "audio") {
        if (!validate_media_attachment(attachment, result)) {
            return false;
        }
    } else if (type == "poll") {
        if (!validate_poll_attachment(attachment, result)) {
            return false;
        }
    } else if (type == "location") {
        if (!validate_location_attachment(attachment, result)) {
            return false;
        }
    } else if (type == "link_preview") {
        if (!validate_link_preview_attachment(attachment, result)) {
            return false;
        }
    }
    
    return true;
}

bool NoteValidator::validate_media_attachment(const json& attachment, ValidationResult& result) const {
    // Check file size
    if (attachment.contains("file_size")) {
        size_t file_size = attachment["file_size"];
        std::string type = attachment["type"];
        
        size_t max_size = (type == "image") ? MAX_IMAGE_SIZE : 
                         (type == "video") ? MAX_VIDEO_SIZE : MAX_AUDIO_SIZE;
        
        if (file_size > max_size) {
            result.is_valid = false;
            result.error_code = "ATTACHMENT_TOO_LARGE";
            result.error_message = type + " file size exceeds maximum limit";
            return false;
        }
    }
    
    // Check MIME type
    if (attachment.contains("mime_type")) {
        std::string mime_type = attachment["mime_type"];
        if (!is_valid_mime_type(mime_type)) {
            result.is_valid = false;
            result.error_code = "INVALID_MIME_TYPE";
            result.error_message = "Unsupported MIME type: " + mime_type;
            return false;
        }
    }
    
    // Check dimensions for images/videos
    if (attachment.contains("width") && attachment.contains("height")) {
        int width = attachment["width"];
        int height = attachment["height"];
        
        if (width <= 0 || height <= 0) {
            result.is_valid = false;
            result.error_code = "INVALID_DIMENSIONS";
            result.error_message = "Invalid media dimensions";
            return false;
        }
        
        if (width > MAX_MEDIA_DIMENSION || height > MAX_MEDIA_DIMENSION) {
            result.is_valid = false;
            result.error_code = "DIMENSIONS_TOO_LARGE";
            result.error_message = "Media dimensions exceed maximum limit";
            return false;
        }
    }
    
    return true;
}

bool NoteValidator::validate_poll_attachment(const json& attachment, ValidationResult& result) const {
    if (!attachment.contains("poll_data")) {
        result.is_valid = false;
        result.error_code = "MISSING_POLL_DATA";
        result.error_message = "Poll attachment requires poll_data";
        return false;
    }
    
    const auto& poll_data = attachment["poll_data"];
    
    // Check question
    if (!poll_data.contains("question") || !poll_data["question"].is_string()) {
        result.is_valid = false;
        result.error_code = "MISSING_POLL_QUESTION";
        result.error_message = "Poll must have a question";
        return false;
    }
    
    std::string question = poll_data["question"];
    if (question.length() > MAX_POLL_QUESTION_LENGTH) {
        result.is_valid = false;
        result.error_code = "POLL_QUESTION_TOO_LONG";
        result.error_message = "Poll question too long";
        return false;
    }
    
    // Check options
    if (!poll_data.contains("options") || !poll_data["options"].is_array()) {
        result.is_valid = false;
        result.error_code = "MISSING_POLL_OPTIONS";
        result.error_message = "Poll must have options";
        return false;
    }
    
    const auto& options = poll_data["options"];
    if (options.size() < MIN_POLL_OPTIONS || options.size() > MAX_POLL_OPTIONS) {
        result.is_valid = false;
        result.error_code = "INVALID_POLL_OPTIONS_COUNT";
        result.error_message = "Poll must have between " + std::to_string(MIN_POLL_OPTIONS) + 
                              " and " + std::to_string(MAX_POLL_OPTIONS) + " options";
        return false;
    }
    
    for (const auto& option : options) {
        if (!option.is_string()) {
            result.is_valid = false;
            result.error_code = "INVALID_POLL_OPTION_TYPE";
            result.error_message = "Poll options must be strings";
            return false;
        }
        
        std::string option_text = option;
        if (option_text.length() > MAX_POLL_OPTION_LENGTH) {
            result.is_valid = false;
            result.error_code = "POLL_OPTION_TOO_LONG";
            result.error_message = "Poll option too long";
            return false;
        }
    }
    
    return true;
}

// ========== SPAM AND CONTENT POLICY VALIDATION ==========

bool NoteValidator::validate_against_spam(const std::string& content, ValidationResult& result) const {
    // Check for excessive repetition
    if (has_excessive_repetition(content)) {
        result.is_valid = false;
        result.error_code = "SPAM_REPETITION";
        result.error_message = "Content contains excessive repetition";
        return false;
    }
    
    // Check for known spam patterns
    for (const auto& pattern : spam_patterns_) {
        if (std::regex_search(content, pattern)) {
            result.is_valid = false;
            result.error_code = "SPAM_PATTERN";
            result.error_message = "Content matches known spam patterns";
            return false;
        }
    }
    
    // Check for excessive capitalization
    if (calculate_caps_ratio(content) > MAX_CAPS_RATIO) {
        result.warnings.push_back("Content has excessive capitalization");
    }
    
    // Check for excessive punctuation
    if (calculate_punctuation_ratio(content) > MAX_PUNCTUATION_RATIO) {
        result.warnings.push_back("Content has excessive punctuation");
    }
    
    return true;
}

bool NoteValidator::validate_against_profanity(const std::string& content, ValidationResult& result) const {
    std::string lower_content = core::utils::to_lower(content);
    
    for (const auto& word : profanity_words_) {
        if (lower_content.find(word) != std::string::npos) {
            result.is_valid = false;
            result.error_code = "PROFANITY_DETECTED";
            result.error_message = "Content contains inappropriate language";
            return false;
        }
    }
    
    return true;
}

bool NoteValidator::validate_content_policy(const json& note_data, ValidationResult& result) const {
    std::string content = note_data["content"];
    
    // Check for harassment keywords
    if (contains_harassment_language(content)) {
        result.is_valid = false;
        result.error_code = "HARASSMENT_CONTENT";
        result.error_message = "Content may constitute harassment";
        return false;
    }
    
    // Check for hate speech indicators
    if (contains_hate_speech_indicators(content)) {
        result.is_valid = false;
        result.error_code = "HATE_SPEECH";
        result.error_message = "Content may constitute hate speech";
        return false;
    }
    
    // Check for illegal content indicators
    if (contains_illegal_content_indicators(content)) {
        result.is_valid = false;
        result.error_code = "ILLEGAL_CONTENT";
        result.error_message = "Content may be illegal";
        return false;
    }
    
    return true;
}

// ========== HELPER METHODS ==========

std::vector<std::string> NoteValidator::extract_mentions(const std::string& content) const {
    std::vector<std::string> mentions;
    std::sregex_iterator iter(content.begin(), content.end(), mention_regex_);
    std::sregex_iterator end;
    
    for (; iter != end; ++iter) {
        mentions.push_back(iter->str());
    }
    
    return mentions;
}

std::vector<std::string> NoteValidator::extract_hashtags(const std::string& content) const {
    std::vector<std::string> hashtags;
    std::sregex_iterator iter(content.begin(), content.end(), hashtag_regex_);
    std::sregex_iterator end;
    
    for (; iter != end; ++iter) {
        hashtags.push_back(iter->str());
    }
    
    return hashtags;
}

std::vector<std::string> NoteValidator::extract_urls(const std::string& content) const {
    std::vector<std::string> urls;
    std::sregex_iterator iter(content.begin(), content.end(), url_regex_);
    std::sregex_iterator end;
    
    for (; iter != end; ++iter) {
        urls.push_back(iter->str());
    }
    
    return urls;
}

bool NoteValidator::is_safe_url(const std::string& url) const {
    // Check against known malicious domains
    for (const auto& domain : malicious_domains_) {
        if (url.find(domain) != std::string::npos) {
            return false;
        }
    }
    
    // Check for suspicious URL patterns
    if (url.find("bit.ly") != std::string::npos || 
        url.find("tinyurl") != std::string::npos ||
        url.find("t.co") != std::string::npos) {
        // URL shorteners require additional validation
        // This would typically involve checking against a safe list or expanding the URL
        return true; // For now, allow but could be enhanced
    }
    
    return true;
}

double NoteValidator::calculate_content_similarity(const std::string& content1, const std::string& content2) const {
    // Simple Levenshtein distance-based similarity
    size_t len1 = content1.length();
    size_t len2 = content2.length();
    
    if (len1 == 0) return len2 == 0 ? 1.0 : 0.0;
    if (len2 == 0) return 0.0;
    
    // Use simple character-based similarity for now
    size_t common_chars = 0;
    size_t min_len = std::min(len1, len2);
    
    for (size_t i = 0; i < min_len; ++i) {
        if (content1[i] == content2[i]) {
            common_chars++;
        }
    }
    
    return static_cast<double>(common_chars) / std::max(len1, len2);
}

void NoteValidator::initialize_spam_patterns() {
    // Common spam patterns
    spam_patterns_.emplace_back(R"(\b(URGENT|WINNER|CONGRATULATIONS|FREE MONEY|CLICK HERE|BUY NOW)\b)", std::regex_constants::icase);
    spam_patterns_.emplace_back(R"((.)\1{10,})"); // Excessive character repetition
    spam_patterns_.emplace_back(R"([!]{5,})"); // Excessive exclamation marks
    spam_patterns_.emplace_back(R"([?]{5,})"); // Excessive question marks
    spam_patterns_.emplace_back(R"(\b\d{10,}\b)"); // Long number sequences (might be spam)
}

void NoteValidator::initialize_profanity_filters() {
    // Initialize with common profanity words (simplified for example)
    profanity_words_ = {
        "spam", "scam", "fake", // Basic spam indicators
        // Add actual profanity words as needed
    };
}

bool NoteValidator::has_excessive_repetition(const std::string& content) const {
    // Check for repeated words
    std::istringstream iss(content);
    std::string word;
    std::unordered_map<std::string, int> word_count;
    
    while (iss >> word) {
        word = core::utils::to_lower(word);
        word_count[word]++;
        if (word_count[word] > MAX_WORD_REPETITION) {
            return true;
        }
    }
    
    return false;
}

double NoteValidator::calculate_caps_ratio(const std::string& content) const {
    int upper_count = 0;
    int letter_count = 0;
    
    for (char c : content) {
        if (std::isalpha(c)) {
            letter_count++;
            if (std::isupper(c)) {
                upper_count++;
            }
        }
    }
    
    return letter_count > 0 ? static_cast<double>(upper_count) / letter_count : 0.0;
}

double NoteValidator::calculate_punctuation_ratio(const std::string& content) const {
    int punct_count = 0;
    int total_count = content.length();
    
    for (char c : content) {
        if (std::ispunct(c)) {
            punct_count++;
        }
    }
    
    return total_count > 0 ? static_cast<double>(punct_count) / total_count : 0.0;
}

int NoteValidator::count_consecutive_whitespace(const std::string& content) const {
    int max_consecutive = 0;
    int current_consecutive = 0;
    
    for (char c : content) {
        if (std::isspace(c)) {
            current_consecutive++;
            max_consecutive = std::max(max_consecutive, current_consecutive);
        } else {
            current_consecutive = 0;
        }
    }
    
    return max_consecutive;
}

bool NoteValidator::contains_harassment_language(const std::string& content) const {
    // Simple harassment detection (would be more sophisticated in production)
    std::vector<std::string> harassment_keywords = {
        "kill yourself", "die", "threat", "violence"
    };
    
    std::string lower_content = core::utils::to_lower(content);
    for (const auto& keyword : harassment_keywords) {
        if (lower_content.find(keyword) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

bool NoteValidator::contains_hate_speech_indicators(const std::string& content) const {
    // Simple hate speech detection (would use ML models in production)
    std::vector<std::string> hate_keywords = {
        "hate", "racist", "discriminat"
    };
    
    std::string lower_content = core::utils::to_lower(content);
    for (const auto& keyword : hate_keywords) {
        if (lower_content.find(keyword) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

bool NoteValidator::contains_illegal_content_indicators(const std::string& content) const {
    // Simple illegal content detection
    std::vector<std::string> illegal_keywords = {
        "drugs", "illegal", "counterfeit", "piracy"
    };
    
    std::string lower_content = core::utils::to_lower(content);
    for (const auto& keyword : illegal_keywords) {
        if (lower_content.find(keyword) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

} // namespace sonet::note::validators
