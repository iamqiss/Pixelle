/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <chrono>
#include <optional>
#include <vector>
#include <unordered_map>

namespace sonet::user {

// I'm keeping this simple but extensible - learned this from years of over-engineering
enum class UserStatus {
    ACTIVE,
    SUSPENDED,
    PENDING_VERIFICATION,
    DEACTIVATED,
    BANNED
};

enum class AuthResult {
    SUCCESS,
    INVALID_CREDENTIALS,
    ACCOUNT_LOCKED,
    ACCOUNT_SUSPENDED,
    EMAIL_NOT_VERIFIED,
    TOO_MANY_ATTEMPTS,
    INTERNAL_ERROR
};

enum class SessionType {
    WEB,
    MOBILE,
    API,
    ADMIN
};

// This is our core user structure - everything else builds on this
struct User {
    std::string user_id;
    std::string username;
    std::string email;
    std::string display_name;
    std::string bio;
    std::string avatar_url;
    std::string location;
    std::string website;
    UserStatus status;
    bool is_verified;  // That blue checkmark everyone wants
    bool is_private;   // Private accounts need special handling
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::chrono::system_clock::time_point last_login;
    
    // Social metrics - because everyone cares about the numbers
    uint64_t follower_count;
    uint64_t following_count;
    uint64_t note_count;
    
    // Privacy and security settings
    std::unordered_map<std::string, std::string> settings;
    std::unordered_map<std::string, std::string> privacy_settings;
};

// Authentication credentials - keeping passwords far away from user data
struct AuthCredentials {
    std::string email;
    std::string password;
    std::optional<std::string> two_factor_code;
    std::string client_info;  // Browser, device info for security
    std::string ip_address;
};

// Session info - I track everything for security reasons
struct UserSession {
    std::string session_id;
    std::string user_id;
    std::string device_id;
    std::string device_name;
    std::string ip_address;
    std::string user_agent;
    SessionType type;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_activity;
    std::chrono::system_clock::time_point expires_at;
    bool is_active;
    
    // Security flags
    bool is_suspicious;  // Flagged by our security algorithms
    std::string location_info;  // Geo data for security
};

// JWT token structure - contains everything we need for stateless auth
struct JWTClaims {
    std::string user_id;
    std::string username;
    std::string email;
    std::vector<std::string> roles;
    std::string session_id;
    SessionType session_type;
    std::chrono::system_clock::time_point issued_at;
    std::chrono::system_clock::time_point expires_at;
    std::string issuer;
    std::string audience;
    
    // Security claims
    std::string device_fingerprint;
    std::string ip_address;
    bool requires_2fa;
};

// Passphrase requirements - Modern security through memorable strength
struct PasswordPolicy {
    static constexpr size_t MIN_LENGTH = 20;        // Minimum 20 characters for passphrases
    static constexpr size_t MAX_LENGTH = 200;       // Maximum 200 characters
    static constexpr size_t MIN_WORD_COUNT = 4;     // Minimum 4 words
    static constexpr size_t MAX_WORD_COUNT = 12;    // Maximum 12 words
    static constexpr bool REQUIRE_MIXED_CASE = false; // Not required for passphrases
    static constexpr bool REQUIRE_DIGITS = false;    // Not required for passphrases
    static constexpr bool REQUIRE_SPECIAL = false;   // Not required for passphrases
    static constexpr size_t MIN_UNIQUE_CHARS = 8;   // Minimum unique characters
    
    // These are the common phrases and passwords I absolutely won't allow
    static const std::vector<std::string> FORBIDDEN_PASSWORDS;
    static const std::vector<std::string> FORBIDDEN_PHRASES;
};

// Rate limiting configuration - because attackers gonna attack
struct RateLimitConfig {
    uint32_t login_attempts_per_hour = 10;
    uint32_t registration_attempts_per_hour = 5;
    uint32_t password_reset_attempts_per_hour = 3;
    uint32_t verification_attempts_per_hour = 10;
    
    // Lockout periods in minutes
    uint32_t account_lockout_duration = 30;
    uint32_t ip_block_duration = 60;
};

// Registration data - everything we need to create a new user
struct RegistrationRequest {
    std::string username;
    std::string email;
    std::string password;
    std::string display_name;
    std::optional<std::string> invitation_code;  // For invite-only periods
    std::string client_info;
    std::string ip_address;
    bool accept_terms;
    bool accept_privacy;
};

// Two-factor authentication setup
struct TwoFactorAuth {
    std::string user_id;
    bool is_enabled;
    std::string secret_key;  // TOTP secret
    std::vector<std::string> backup_codes;
    std::chrono::system_clock::time_point setup_at;
    std::chrono::system_clock::time_point last_used;
};

// Security events - I log everything suspicious
enum class SecurityEventType {
    LOGIN_SUCCESS,
    LOGIN_FAILED,
    LOGIN_SUSPICIOUS,
    PASSWORD_CHANGED,
    EMAIL_CHANGED,
    TWO_FACTOR_ENABLED,
    TWO_FACTOR_DISABLED,
    SESSION_CREATED,
    SESSION_TERMINATED,
    ACCOUNT_LOCKED,
    ACCOUNT_UNLOCKED
};

struct SecurityEvent {
    std::string event_id;
    std::string user_id;
    SecurityEventType type;
    std::string description;
    std::string ip_address;
    std::string user_agent;
    std::unordered_map<std::string, std::string> metadata;
    std::chrono::system_clock::time_point timestamp;
};

} // namespace sonet::user
