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
#include <optional>
#include <regex>
#include <nlohmann/json.hpp>
#include "../models/note.h"

namespace sonet::note::validators {

using json = nlohmann::json;

/**
 * Result object returned by validation routines
 */
struct ValidationResult {
    bool is_valid {true};
    std::string error_code;
    std::string error_message;
    std::vector<std::string> warnings;

    nlohmann::json to_json() const {
        return nlohmann::json{
            {"is_valid", is_valid},
            {"error_code", error_code},
            {"error_message", error_message},
            {"warnings", warnings}
        };
    }
};

/**
 * Comprehensive note validator (instance-based to hold compiled regex and caches)
 */
class NoteValidator {
public:
    NoteValidator();
    ~NoteValidator() = default;

    // Core validation
    ValidationResult validate_note(const json& note_data) const;
    ValidationResult validate_content_only(const std::string& content) const;
    ValidationResult validate_update_request(const json& update_data, const models::Note& existing_note) const;

private:
    // High-level validation helpers
    bool validate_basic_structure(const json& note_data, ValidationResult& result) const;
    bool validate_content(const json& note_data, ValidationResult& result) const;
    bool validate_metadata(const json& note_data, ValidationResult& result) const;
    bool validate_attachments(const json& note_data, ValidationResult& result) const;
    bool validate_relationships(const json& note_data, ValidationResult& result) const;
    bool validate_visibility_settings(const json& note_data, ValidationResult& result) const;
    bool validate_content_policy(const json& note_data, ValidationResult& result) const;
    bool validate_security_constraints(const json& note_data, ValidationResult& result) const;

    // Content-specific helpers
    bool validate_content_length(const std::string& content, ValidationResult& result) const;
    bool validate_character_set(const std::string& content, ValidationResult& result) const;
    bool validate_content_features(const std::string& content, ValidationResult& result) const;

    // Attachments
    bool validate_single_attachment(const json& attachment, ValidationResult& result) const;
    bool validate_media_attachment(const json& attachment, ValidationResult& result) const;
    bool validate_poll_attachment(const json& attachment, ValidationResult& result) const;

    // Scheduling/metadata
    bool validate_scheduled_time(const json& scheduled_time, ValidationResult& result) const;
    bool validate_tags(const json& tags, ValidationResult& result) const;

    // Spam/profanity/policy checks
    bool validate_against_spam(const std::string& content, ValidationResult& result) const;
    bool validate_against_profanity(const std::string& content, ValidationResult& result) const;

    // Extraction helpers
    std::vector<std::string> extract_mentions(const std::string& content) const;
    std::vector<std::string> extract_hashtags(const std::string& content) const;
    std::vector<std::string> extract_urls(const std::string& content) const;

    // URL safety
    bool is_safe_url(const std::string& url) const;

    // Content similarity/helpers
    double calculate_content_similarity(const std::string& content1, const std::string& content2) const;
    bool has_excessive_repetition(const std::string& content) const;
    double calculate_caps_ratio(const std::string& content) const;
    double calculate_punctuation_ratio(const std::string& content) const;
    int count_consecutive_whitespace(const std::string& content) const;
    bool contains_harassment_language(const std::string& content) const;
    bool contains_hate_speech_indicators(const std::string& content) const;
    bool contains_illegal_content_indicators(const std::string& content) const;

    // Initialization
    void initialize_spam_patterns();
    void initialize_profanity_filters();

private:
    // Compiled regex patterns
    std::regex url_regex_;
    std::regex mention_regex_;
    std::regex hashtag_regex_;
    std::regex email_regex_;

    // Tunables/constants
    static constexpr size_t MAX_CUSTOM_TAGS = 10;
    static constexpr int MAX_EDIT_WINDOW_MINUTES = 30;
    static constexpr double MIN_EDIT_SIMILARITY = 0.65;
};

} // namespace sonet::note::validators