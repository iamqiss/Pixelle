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
#include <unordered_set>
#include <regex>

namespace sonet::user::validators {

/**
 * Comprehensive validation system for user-related data.
 * Implements Twitter-scale validation rules with security focus.
 */
class UserValidator {
public:
    UserValidator();
    ~UserValidator() = default;

    // Core validation methods
    struct ValidationResult {
        bool is_valid;
        std::vector<std::string> errors;
        std::vector<std::string> warnings;
    };

    // User registration validation
    ValidationResult validate_username(const std::string& username);
    ValidationResult validate_email(const std::string& email);
    ValidationResult validate_password(const std::string& password);
    ValidationResult validate_full_name(const std::string& full_name);
    ValidationResult validate_bio(const std::string& bio);
    ValidationResult validate_location(const std::string& location);
    ValidationResult validate_website(const std::string& website);

    // Profile data validation
    ValidationResult validate_profile_update(
        const std::string& full_name,
        const std::string& bio,
        const std::string& location,
        const std::string& website,
        const std::string& avatar_url,
        const std::string& banner_url
    );

    // Input sanitization
    std::string sanitize_username(const std::string& username);
    std::string sanitize_display_text(const std::string& text);
    std::string sanitize_url(const std::string& url);

    // Security validation
    bool is_suspicious_username(const std::string& username);
    bool is_disposable_email(const std::string& email);
    bool contains_profanity(const std::string& text);
    bool is_spam_like_content(const std::string& content);
    
    // Advanced validation
    ValidationResult validate_phone_number(const std::string& phone, const std::string& country_code = "US");
    ValidationResult validate_age(int age);
    ValidationResult validate_timezone(const std::string& timezone);

    // Rate limiting validation
    bool is_registration_rate_limited(const std::string& ip_address);
    bool is_login_rate_limited(const std::string& username, const std::string& ip_address);

    // Content policy validation
    bool violates_content_policy(const std::string& content);
    bool contains_harmful_links(const std::string& content);

private:
    // Regex patterns
    std::regex username_pattern_;
    std::regex email_pattern_;
    std::regex url_pattern_;
    std::regex phone_pattern_;
    
    // Blacklists and restricted content
    std::unordered_set<std::string> reserved_usernames_;
    std::unordered_set<std::string> profanity_words_;
    std::unordered_set<std::string> disposable_email_domains_;
    std::unordered_set<std::string> suspicious_patterns_;
    std::unordered_set<std::string> harmful_domains_;

    // Helper methods
    void initialize_patterns();
    void load_blacklists();
    bool contains_sequential_chars(const std::string& text, int max_sequential = 3);
    bool has_repeated_patterns(const std::string& text);
    std::string normalize_text(const std::string& text);
    double calculate_entropy(const std::string& text);
    bool is_valid_tld(const std::string& domain);
    
    // Rate limiting storage (in production, this would use Redis)
    std::unordered_map<std::string, std::vector<int64_t>> registration_attempts_;
    std::unordered_map<std::string, std::vector<int64_t>> login_attempts_;
    
    void cleanup_old_attempts();
};

} // namespace sonet::user::validators
