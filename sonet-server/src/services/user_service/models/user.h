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
#include <vector>
#include <optional>

namespace sonet::user::models {

enum class UserStatus {
    ACTIVE = 0,
    INACTIVE = 1,
    SUSPENDED = 2,
    BANNED = 3,
    PENDING_VERIFICATION = 4,
    DEACTIVATED = 5
};

enum class AccountType {
    PERSONAL = 0,
    BUSINESS = 1,
    VERIFIED = 2,
    PREMIUM = 3,
    DEVELOPER = 4
};

enum class PrivacyLevel {
    PUBLIC = 0,
    PROTECTED = 1,    // Followers only
    PRIVATE = 2       // Approved followers only
};

/**
 * Complete User model with all Twitter-scale features
 */
class User {
public:
    User();
    User(const std::string& user_id, const std::string& username, const std::string& email);
    ~User() = default;

    // Core identity
    std::string user_id;
    std::string username;
    std::string email;
    std::string phone_number;
    
    // Security
    std::string password_hash;
    std::string salt;
    bool is_email_verified = false;
    bool is_phone_verified = false;
    std::string email_verification_token;
    std::string phone_verification_code;
    int64_t verification_token_expires_at = 0;
    
    // Profile information
    std::string display_name;
    std::string first_name;
    std::string last_name;
    std::string bio;
    std::string location;
    std::string website;
    std::string avatar_url;
    std::string banner_url;
    std::string timezone = "UTC";
    std::string language = "en";
    
    // Account settings
    UserStatus status = UserStatus::ACTIVE;
    AccountType account_type = AccountType::PERSONAL;
    PrivacyLevel privacy_level = PrivacyLevel::PUBLIC;
    bool is_verified = false;          // Blue checkmark
    bool is_premium = false;
    bool is_developer = false;
    
    // Privacy settings
    bool discoverable_by_email = true;
    bool discoverable_by_phone = false;
    bool allow_direct_messages = true;
    bool allow_message_requests = true;
    bool show_activity_status = true;
    bool show_read_receipts = true;
    
    // Content settings
    bool nsfw_content_enabled = false;
    bool autoplay_videos = true;
    bool high_quality_images = true;
    
    // Notification preferences
    bool email_notifications = true;
    bool push_notifications = true;
    bool sms_notifications = false;
    
    // Statistics (cached for performance)
    int64_t followers_count = 0;
    int64_t following_count = 0;
    int64_t notes_count = 0;
    int64_t likes_count = 0;
    int64_t media_count = 0;
    int64_t profile_views_count = 0;
    
    // Relationships
    std::vector<std::string> blocked_users;
    std::vector<std::string> muted_users;
    std::vector<std::string> close_friends;
    
    // Metadata
    int64_t created_at;
    int64_t updated_at;
    int64_t last_login_at = 0;
    int64_t last_active_at = 0;
    std::string created_from_ip;
    std::string last_login_ip;
    
    // Suspension/Ban information
    std::optional<int64_t> suspended_until;
    std::string suspension_reason;
    std::string banned_reason;
    
    // Account deletion
    bool is_deleted = false;
    int64_t deleted_at = 0;
    std::string deletion_reason;
    
    // Utility methods
    bool is_active() const;
    bool can_login() const;
    bool can_note() const;
    bool is_public() const;
    bool is_protected() const;
    bool is_private() const;
    bool is_blocked_user(const std::string& user_id) const;
    bool is_muted_user(const std::string& user_id) const;
    bool is_close_friend(const std::string& user_id) const;
    
    // Profile completeness
    double get_profile_completeness_percentage() const;
    std::vector<std::string> get_missing_profile_fields() const;
    
    // Age and verification
    std::optional<int> get_account_age_days() const;
    bool needs_reverification() const;
    
    // JSON serialization
    std::string to_json() const;
    void from_json(const std::string& json);
    
    // Database serialization
    std::string to_db_string() const;
    void from_db_string(const std::string& db_data);
    
    // Privacy filters
    User get_public_view() const;
    User get_protected_view() const;
    User get_follower_view() const;
    User get_self_view() const;
    
    // Validation
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
};

/**
 * User creation request with validation
 */
struct UserCreateRequest {
    std::string username;
    std::string email;
    std::string password;
    std::string display_name;
    std::string first_name;
    std::string last_name;
    std::string bio;
    std::string phone_number;
    std::string timezone = "UTC";
    std::string language = "en";
    std::string referral_code;
    bool terms_accepted = false;
    bool privacy_policy_accepted = false;
    bool marketing_emails_accepted = false;
    
    // Device information for security
    std::string device_fingerprint;
    std::string user_agent;
    std::string ip_address;
    std::string country_code;
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
};

/**
 * User update request with field masking
 */
struct UserUpdateRequest {
    std::string user_id;
    
    // Optional fields (only set fields will be updated)
    std::optional<std::string> display_name;
    std::optional<std::string> first_name;
    std::optional<std::string> last_name;
    std::optional<std::string> bio;
    std::optional<std::string> location;
    std::optional<std::string> website;
    std::optional<std::string> avatar_url;
    std::optional<std::string> banner_url;
    std::optional<std::string> timezone;
    std::optional<std::string> language;
    std::optional<PrivacyLevel> privacy_level;
    std::optional<bool> discoverable_by_email;
    std::optional<bool> discoverable_by_phone;
    std::optional<bool> allow_direct_messages;
    std::optional<bool> allow_message_requests;
    std::optional<bool> show_activity_status;
    std::optional<bool> show_read_receipts;
    std::optional<bool> nsfw_content_enabled;
    std::optional<bool> autoplay_videos;
    std::optional<bool> high_quality_images;
    std::optional<bool> email_notifications;
    std::optional<bool> push_notifications;
    std::optional<bool> sms_notifications;
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    std::vector<std::string> get_updated_fields() const;
};

// Utility functions
std::string user_status_to_string(UserStatus status);
UserStatus string_to_user_status(const std::string& status);
std::string account_type_to_string(AccountType type);
AccountType string_to_account_type(const std::string& type);
std::string privacy_level_to_string(PrivacyLevel level);
PrivacyLevel string_to_privacy_level(const std::string& level);

// Comparison operators
bool operator==(const User& lhs, const User& rhs);
bool operator!=(const User& lhs, const User& rhs);

} // namespace sonet::user::models