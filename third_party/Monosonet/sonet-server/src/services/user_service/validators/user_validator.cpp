/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "user_validator.h"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <unordered_map>
#include <cmath>

namespace sonet::user::validators {

UserValidator::UserValidator() {
    initialize_patterns();
    load_blacklists();
    spdlog::info("User validator initialized with comprehensive rules");
}

UserValidator::ValidationResult UserValidator::validate_username(const std::string& username) {
    ValidationResult result;
    result.is_valid = true;
    
    // Length validation
    if (username.empty()) {
        result.errors.push_back("Username cannot be empty");
        result.is_valid = false;
        return result;
    }
    
    if (username.length() < 3) {
        result.errors.push_back("Username must be at least 3 characters long");
        result.is_valid = false;
    }
    
    if (username.length() > 30) {
        result.errors.push_back("Username cannot exceed 30 characters");
        result.is_valid = false;
    }
    
    // Pattern validation
    if (!std::regex_match(username, username_pattern_)) {
        result.errors.push_back("Username can only contain letters, numbers, and underscores");
        result.is_valid = false;
    }
    
    // Cannot start or end with underscore
    if (username.front() == '_' || username.back() == '_') {
        result.errors.push_back("Username cannot start or end with underscore");
        result.is_valid = false;
    }
    
    // Cannot have consecutive underscores
    if (username.find("__") != std::string::npos) {
        result.errors.push_back("Username cannot contain consecutive underscores");
        result.is_valid = false;
    }
    
    // Reserved username check
    std::string lower_username = username;
    std::transform(lower_username.begin(), lower_username.end(), lower_username.begin(),
               [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    
    if (reserved_usernames_.count(lower_username)) {
        result.errors.push_back("This username is reserved and cannot be used");
        result.is_valid = false;
    }
    
    // Suspicious pattern check
    if (is_suspicious_username(username)) {
        result.warnings.push_back("Username appears to be suspicious or spam-like");
    }
    
    // Profanity check
    if (contains_profanity(username)) {
        result.errors.push_back("Username contains inappropriate content");
        result.is_valid = false;
    }
    
    return result;
}

UserValidator::ValidationResult UserValidator::validate_email(const std::string& email) {
    ValidationResult result;
    result.is_valid = true;
    
    if (email.empty()) {
        result.errors.push_back("Email cannot be empty");
        result.is_valid = false;
        return result;
    }
    
    if (email.length() > 320) {  // RFC 5321 limit
        result.errors.push_back("Email address is too long");
        result.is_valid = false;
    }
    
    // Pattern validation
    bool pattern_ok = std::regex_match(email, email_pattern_);
    if (!pattern_ok) {
        result.errors.push_back("Invalid email format");
        result.is_valid = false;
    }
    
    // Check for disposable email only if email structure looks valid
    if (pattern_ok && email.find('@') != std::string::npos) {
        if (is_disposable_email(email)) {
            result.warnings.push_back("Disposable email addresses are discouraged");
        }
    }
    
    // Check for suspicious patterns
    if (email.find('+') != std::string::npos) {
        result.warnings.push_back("Email contains plus addressing");
    }
    
    return result;
}

UserValidator::ValidationResult UserValidator::validate_password(const std::string& password) {
    ValidationResult result;
    result.is_valid = true;
    
    if (password.empty()) {
        result.errors.push_back("Password cannot be empty");
        result.is_valid = false;
        return result;
    }
    
    // Length requirements
    if (password.length() < 8) {
        result.errors.push_back("Password must be at least 8 characters long");
        result.is_valid = false;
    }
    
    if (password.length() > 128) {
        result.errors.push_back("Password cannot exceed 128 characters");
        result.is_valid = false;
    }
    
    // Character requirements
    bool has_upper = false, has_lower = false, has_digit = false, has_special = false;
    
    for (char c : password) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (std::isupper(uc)) has_upper = true;
        else if (std::islower(uc)) has_lower = true;
        else if (std::isdigit(uc)) has_digit = true;
        else if (std::ispunct(uc)) has_special = true;
    }
    
    if (!has_upper) {
        result.errors.push_back("Password must contain at least one uppercase letter");
        result.is_valid = false;
    }
    
    if (!has_lower) {
        result.errors.push_back("Password must contain at least one lowercase letter");
        result.is_valid = false;
    }
    
    if (!has_digit) {
        result.errors.push_back("Password must contain at least one digit");
        result.is_valid = false;
    }
    
    if (!has_special) {
        result.errors.push_back("Password must contain at least one special character");
        result.is_valid = false;
    }
    
    // Entropy check
    double entropy = calculate_entropy(password);
    if (entropy < 3.0) {
        result.warnings.push_back("Password has low complexity - consider making it more diverse");
    }
    
    // Sequential characters check
    if (contains_sequential_chars(password, 4)) {
        result.warnings.push_back("Password contains sequential characters");
    }
    
    // Repeated patterns check
    if (has_repeated_patterns(password)) {
        result.warnings.push_back("Password contains repeated patterns");
    }
    
    return result;
}

UserValidator::ValidationResult UserValidator::validate_full_name(const std::string& full_name) {
    ValidationResult result;
    result.is_valid = true;
    
    if (full_name.length() > 100) {
        result.errors.push_back("Full name cannot exceed 100 characters");
        result.is_valid = false;
    }
    
    // Check for suspicious patterns
    if (contains_profanity(full_name)) {
        result.errors.push_back("Full name contains inappropriate content");
        result.is_valid = false;
    }
    
    // Check for excessive special characters
    int special_count = 0;
    for (char c : full_name) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (!std::isalnum(uc) && !std::isspace(uc) && c != '.' && c != '\'' && c != '-') {
            special_count++;
        }
    }
    
    if (special_count > 3) {
        result.warnings.push_back("Full name contains many special characters");
    }
    
    return result;
}

UserValidator::ValidationResult UserValidator::validate_bio(const std::string& bio) {
    ValidationResult result;
    result.is_valid = true;
    
    if (bio.length() > 500) {
        result.errors.push_back("Bio cannot exceed 500 characters");
        result.is_valid = false;
    }
    
    if (contains_profanity(bio)) {
        result.errors.push_back("Bio contains inappropriate content");
        result.is_valid = false;
    }
    
    if (is_spam_like_content(bio)) {
        result.warnings.push_back("Bio appears to be spam-like");
    }
    
    if (contains_harmful_links(bio)) {
        result.errors.push_back("Bio contains harmful or suspicious links");
        result.is_valid = false;
    }
    
    return result;
}

UserValidator::ValidationResult UserValidator::validate_location(const std::string& location) {
    ValidationResult result;
    result.is_valid = true;
    
    if (location.length() > 100) {
        result.errors.push_back("Location cannot exceed 100 characters");
        result.is_valid = false;
    }
    
    if (contains_profanity(location)) {
        result.errors.push_back("Location contains inappropriate content");
        result.is_valid = false;
    }
    
    return result;
}

UserValidator::ValidationResult UserValidator::validate_website(const std::string& website) {
    ValidationResult result;
    result.is_valid = true;
    
    if (website.empty()) {
        return result;  // Optional field
    }
    
    if (website.length() > 200) {
        result.errors.push_back("Website URL cannot exceed 200 characters");
        result.is_valid = false;
    }
    
    if (!std::regex_match(website, url_pattern_)) {
        result.errors.push_back("Invalid website URL format");
        result.is_valid = false;
    }
    
    // Must be HTTPS for security
    if (!website.starts_with("https://")) {
        result.warnings.push_back("Website should use HTTPS for security");
    }
    
    if (contains_harmful_links(website)) {
        result.errors.push_back("Website URL appears to be harmful or suspicious");
        result.is_valid = false;
    }
    
    return result;
}

UserValidator::ValidationResult UserValidator::validate_profile_update(
    const std::string& full_name,
    const std::string& bio,
    const std::string& location,
    const std::string& website,
    const std::string& avatar_url,
    const std::string& banner_url) {
    
    ValidationResult result;
    result.is_valid = true;
    
    // Validate each field
    auto name_result = validate_full_name(full_name);
    auto bio_result = validate_bio(bio);
    auto location_result = validate_location(location);
    auto website_result = validate_website(website);
    
    // Combine results
    if (!name_result.is_valid || !bio_result.is_valid || !location_result.is_valid || !website_result.is_valid) {
        result.is_valid = false;
    }
    
    // Merge errors and warnings
    result.errors.insert(result.errors.end(), name_result.errors.begin(), name_result.errors.end());
    result.errors.insert(result.errors.end(), bio_result.errors.begin(), bio_result.errors.end());
    result.errors.insert(result.errors.end(), location_result.errors.begin(), location_result.errors.end());
    result.errors.insert(result.errors.end(), website_result.errors.begin(), website_result.errors.end());
    
    result.warnings.insert(result.warnings.end(), name_result.warnings.begin(), name_result.warnings.end());
    result.warnings.insert(result.warnings.end(), bio_result.warnings.begin(), bio_result.warnings.end());
    result.warnings.insert(result.warnings.end(), location_result.warnings.begin(), location_result.warnings.end());
    result.warnings.insert(result.warnings.end(), website_result.warnings.begin(), website_result.warnings.end());
    
    return result;
}

// Sanitization methods

std::string UserValidator::sanitize_username(const std::string& username) {
    std::string sanitized;
    for (char c : username) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (std::isalnum(uc) || c == '_') {
            sanitized += c;
        }
    }
    return sanitized;
}

std::string UserValidator::sanitize_display_text(const std::string& text) {
    std::string sanitized = text;
    
    // Remove or escape potentially harmful characters
    std::vector<std::pair<std::string, std::string>> replacements = {
        {"<", "&lt;"},
        {">", "&gt;"},
        {"&", "&amp;"},
        {"\"", "&quot;"},
        {"'", "&#x27;"},
        {"\n", " "},
        {"\r", " "},
        {"\t", " "}
    };
    
    for (const auto& [from, to] : replacements) {
        size_t pos = 0;
        while ((pos = sanitized.find(from, pos)) != std::string::npos) {
            sanitized.replace(pos, from.length(), to);
            pos += to.length();
        }
    }
    
    return sanitized;
}

// Security validation methods

bool UserValidator::is_suspicious_username(const std::string& username) {
    std::string lower_username = username;
    std::transform(lower_username.begin(), lower_username.end(), lower_username.begin(),
               [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    
    // Check for patterns indicating bot accounts
    for (const auto& pattern : suspicious_patterns_) {
        if (lower_username.find(pattern) != std::string::npos) {
            return true;
        }
    }
    
    // Check for excessive numbers at the end
    int trailing_digits = 0;
    for (auto it = username.rbegin(); it != username.rend() && std::isdigit(static_cast<unsigned char>(*it)); ++it) {
        trailing_digits++;
    }
    
    if (trailing_digits > 6) {
        return true;
    }
    
    // Check for very low entropy (repeated characters)
    if (calculate_entropy(username) < 1.5) {
        return true;
    }
    
    return false;
}

bool UserValidator::is_disposable_email(const std::string& email) {
    auto at_pos = email.find('@');
    if (at_pos == std::string::npos || at_pos + 1 >= email.size()) {
        return false;
    }
    std::string domain = email.substr(at_pos + 1);
    std::transform(domain.begin(), domain.end(), domain.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    
    return disposable_email_domains_.count(domain) > 0;
}

bool UserValidator::contains_profanity(const std::string& text) {
    std::string normalized = normalize_text(text);
    
    for (const auto& word : profanity_words_) {
        if (normalized.find(word) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

bool UserValidator::is_spam_like_content(const std::string& content) {
    // Check for excessive capitalization
    int uppercase_count = 0;
    for (char c : content) {
        if (std::isupper(static_cast<unsigned char>(c))) {
            uppercase_count++;
        }
    }
    
    if (uppercase_count > content.length() * 0.7) {
        return true;
    }
    
    // Check for excessive punctuation
    int punct_count = 0;
    for (char c : content) {
        if (std::ispunct(static_cast<unsigned char>(c))) {
            punct_count++;
        }
    }
    
    if (punct_count > content.length() * 0.3) {
        return true;
    }
    
    // Check for repeated patterns
    if (has_repeated_patterns(content)) {
        return true;
    }
    
    return false;
}

bool UserValidator::contains_harmful_links(const std::string& content) {
    std::regex url_regex(R"(https?://[^\s]+)");
    std::sregex_iterator begin(content.begin(), content.end(), url_regex);
    std::sregex_iterator end;
    
    for (std::sregex_iterator i = begin; i != end; ++i) {
        std::string url = (*i).str();
        
        // Extract domain
        size_t domain_start = url.find("://") + 3;
        size_t domain_end = url.find('/', domain_start);
        if (domain_end == std::string::npos) {
            domain_end = url.length();
        }
        
        std::string domain = url.substr(domain_start, domain_end - domain_start);
        std::transform(domain.begin(), domain.end(), domain.begin(), ::tolower);
        
        if (harmful_domains_.count(domain) > 0) {
            return true;
        }
    }
    
    return false;
}

// Rate limiting methods

bool UserValidator::is_registration_rate_limited(const std::string& ip_address) {
    cleanup_old_attempts();
    
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    auto& attempts = registration_attempts_[ip_address];
    
    // Allow max 3 registrations per hour per IP
    int recent_attempts = 0;
    for (auto timestamp : attempts) {
        if (now - timestamp < 3600) {  // 1 hour
            recent_attempts++;
        }
    }
    
    if (recent_attempts >= 3) {
        return true;
    }
    
    attempts.push_back(now);
    return false;
}

bool UserValidator::is_login_rate_limited(const std::string& username, const std::string& ip_address) {
    cleanup_old_attempts();
    
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    auto& attempts = login_attempts_[username + "|" + ip_address];
    
    // Allow max 5 failed login attempts per 15 minutes
    int recent_attempts = 0;
    for (auto timestamp : attempts) {
        if (now - timestamp < 900) {  // 15 minutes
            recent_attempts++;
        }
    }
    
    if (recent_attempts >= 5) {
        return true;
    }
    
    attempts.push_back(now);
    return false;
}

// Private helper methods

void UserValidator::initialize_patterns() {
    username_pattern_ = std::regex("^[a-zA-Z0-9_]+$");
    email_pattern_ = std::regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
    url_pattern_ = std::regex(R"(^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$)");
    phone_pattern_ = std::regex(R"(^\+?[1-9]\d{1,14}$)");
}

void UserValidator::load_blacklists() {
    // Reserved usernames
    reserved_usernames_ = {
        "admin", "administrator", "root", "api", "www", "mail", "email",
        "support", "help", "info", "contact", "about", "legal", "privacy",
        "terms", "security", "safety", "team", "staff", "moderator", "mod",
        "sonet", "twitter", "facebook", "instagram", "tiktok", "youtube",
        "system", "service", "bot", "official", "verified", "test", "demo"
    };
    
    // Basic profanity words (in production, this would be a comprehensive list)
    profanity_words_ = {
        "spam", "scam", "fake", "bot", "admin", "moderator"
    };
    
    // Common disposable email domains
    disposable_email_domains_ = {
        "10minutemail.com", "temp-mail.org", "guerrillamail.com",
        "mailinator.com", "yopmail.com", "throwaway.email"
    };
    
    // Suspicious username patterns
    suspicious_patterns_ = {
        "bot", "fake", "spam", "scam", "admin", "official", "verified"
    };
    
    // Harmful domains (in production, this would be constantly updated)
    harmful_domains_ = {
        "malware.com", "phishing.net", "spam.org"
    };
}

bool UserValidator::contains_sequential_chars(const std::string& text, int max_sequential) {
    for (size_t i = 0; i < text.length() - max_sequential + 1; ++i) {
        bool is_sequential = true;
        for (size_t j = 1; j < max_sequential; ++j) {
            if (text[i + j] != text[i + j - 1] + 1) {
                is_sequential = false;
                break;
            }
        }
        if (is_sequential) {
            return true;
        }
    }
    return false;
}

bool UserValidator::has_repeated_patterns(const std::string& text) {
    // Check for repeated substrings
    for (size_t len = 2; len <= text.length() / 2; ++len) {
        for (size_t i = 0; i <= text.length() - 2 * len; ++i) {
            std::string pattern = text.substr(i, len);
            if (text.substr(i + len, len) == pattern) {
                return true;
            }
        }
    }
    return false;
}

std::string UserValidator::normalize_text(const std::string& text) {
    std::string normalized = text;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
               [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    
    // Remove spaces and special characters for comparison
    normalized.erase(std::remove_if(normalized.begin(), normalized.end(),
        [](char c) { return !std::isalnum(static_cast<unsigned char>(c)); }), normalized.end());
    
    return normalized;
}

double UserValidator::calculate_entropy(const std::string& text) {
    std::unordered_map<char, int> frequency;
    for (char c : text) {
        frequency[c]++;
    }
    
    double entropy = 0.0;
    for (const auto& [ch, count] : frequency) {
        double p = static_cast<double>(count) / text.length();
        entropy -= p * std::log2(p);
    }
    
    return entropy;
}

void UserValidator::cleanup_old_attempts() {
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    // Clean up registration attempts older than 24 hours
    for (auto& [ip, attempts] : registration_attempts_) {
        attempts.erase(std::remove_if(attempts.begin(), attempts.end(),
            [now](int64_t timestamp) { return now - timestamp > 86400; }), attempts.end());
    }
    
    // Clean up login attempts older than 24 hours
    for (auto& [key, attempts] : login_attempts_) {
        attempts.erase(std::remove_if(attempts.begin(), attempts.end(),
            [now](int64_t timestamp) { return now - timestamp > 86400; }), attempts.end());
    }
}

} // namespace sonet::user::validators
