#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <optional>
#include <nlohmann/json.hpp>

namespace sonet {
namespace user {

// User status enumeration
enum class UserStatus {
    ACTIVE = 0,
    INACTIVE = 1,
    SUSPENDED = 2,
    BANNED = 3,
    PENDING_VERIFICATION = 4
};

// User role enumeration
enum class UserRole {
    USER = 0,
    MODERATOR = 1,
    ADMIN = 2,
    SUPER_ADMIN = 3,
    FOUNDER = 4  // Single founder account with full privileges
};

// User moderation status
enum class ModerationStatus {
    CLEAN = 0,
    FLAGGED = 1,
    WARNED = 2,
    SHADOWBANNED = 3,
    SUSPENDED = 4,
    BANNED = 5
};

// User verification status
enum class VerificationStatus {
    UNVERIFIED = 0,
    PENDING = 1,
    VERIFIED = 2,
    REJECTED = 3,
    FOUNDER_VERIFIED = 4  // Special founder verification
};

// Main user entity
struct User {
    std::string id;
    std::string username;
    std::string email;
    std::string hashed_password;
    std::string first_name;
    std::string last_name;
    std::string display_name;
    std::string bio;
    std::string avatar_url;
    std::string banner_url;
    std::string location;
    std::string website;
    std::string phone_number;
    std::string language;
    std::string timezone;
    UserStatus status;
    UserRole role;
    ModerationStatus moderation_status;
    VerificationStatus email_verified;
    VerificationStatus phone_verified;
    bool is_public_profile;
    bool allow_direct_messages;
    bool allow_mentions;
    std::vector<std::string> interests;
    std::vector<std::string> skills;
    std::vector<std::string> social_links;
    std::chrono::system_clock::time_point last_active_at;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::chrono::system_clock::time_point deleted_at;
    std::chrono::system_clock::time_point flagged_at;
    std::chrono::system_clock::time_point flag_expires_at;
    std::string flag_reason;
    std::string flag_warning_message;
    std::string created_by;
    std::string updated_by;
    std::string deleted_by;
    std::string metadata; // JSON string for additional data

    // Default constructor
    User() = default;

    // Constructor with required fields
    User(const std::string& username, const std::string& email, const std::string& hashed_password)
        : username(username), email(email), hashed_password(hashed_password),
          status(UserStatus::PENDING_VERIFICATION), role(UserRole::USER),
          email_verified(VerificationStatus::UNVERIFIED), phone_verified(VerificationStatus::UNVERIFIED),
          is_public_profile(true), allow_direct_messages(true), allow_mentions(true),
          last_active_at(std::chrono::system_clock::now()),
          created_at(std::chrono::system_clock::now()),
          updated_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static User from_json(const nlohmann::json& j);

    // Utility methods
    bool is_active() const { return status == UserStatus::ACTIVE; }
    bool is_verified() const { return email_verified == VerificationStatus::VERIFIED; }
    bool is_admin() const { return role == UserRole::ADMIN || role == UserRole::SUPER_ADMIN; }
    bool is_founder() const { return role == UserRole::FOUNDER; }
    bool is_moderator() const { return role >= UserRole::MODERATOR; }
    bool is_flagged() const { return moderation_status == ModerationStatus::FLAGGED; }
    bool is_shadowbanned() const { return moderation_status == ModerationStatus::SHADOWBANNED; }
    bool is_suspended() const { return moderation_status == ModerationStatus::SUSPENDED; }
    bool is_banned() const { return moderation_status == ModerationStatus::BANNED; }
    bool is_under_moderation() const { return moderation_status != ModerationStatus::CLEAN; }
    bool is_flag_expired() const { 
        return flag_expires_at < std::chrono::system_clock::now(); 
    }
    std::string get_full_name() const;
    std::string get_display_name_or_username() const;
    int get_age() const; // Calculate age from birth_date if available
};

// User profile information
struct UserProfile {
    std::string id;
    std::string user_id;
    std::string bio;
    std::string avatar_url;
    std::string banner_url;
    std::string location;
    std::string website;
    std::string phone_number;
    std::string birth_date;
    std::string gender;
    std::string occupation;
    std::string company;
    std::string education;
    std::vector<std::string> interests;
    std::vector<std::string> skills;
    std::vector<std::string> languages;
    std::vector<std::string> social_links;
    std::string personal_statement;
    std::string achievements;
    std::string certifications;
    std::string volunteer_work;
    std::string hobbies;
    std::string favorite_books;
    std::string favorite_movies;
    std::string favorite_music;
    std::string travel_destinations;
    std::string goals;
    std::string inspirations;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::string metadata; // JSON string for additional data

    // Default constructor
    UserProfile() = default;

    // Constructor with user_id
    explicit UserProfile(const std::string& user_id) : user_id(user_id),
        created_at(std::chrono::system_clock::now()),
        updated_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static UserProfile from_json(const nlohmann::json& j);
};

// User session for authentication
struct UserSession {
    std::string id;
    std::string user_id;
    std::string token;
    std::string refresh_token;
    std::string device_id;
    std::string device_type;
    std::string device_name;
    std::string ip_address;
    std::string user_agent;
    std::string location;
    bool is_active;
    std::chrono::system_clock::time_point expires_at;
    std::chrono::system_clock::time_point last_used_at;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::string metadata; // JSON string for additional data

    // Default constructor
    UserSession() = default;

    // Constructor with required fields
    UserSession(const std::string& user_id, const std::string& token)
        : user_id(user_id), token(token), is_active(true),
          last_used_at(std::chrono::system_clock::now()),
          created_at(std::chrono::system_clock::now()),
          updated_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static UserSession from_json(const nlohmann::json& j);

    // Utility methods
    bool is_expired() const;
    bool needs_refresh() const;
    std::chrono::seconds get_remaining_time() const;
};

// Two-factor authentication
struct TwoFactorAuth {
    std::string id;
    std::string user_id;
    std::string secret_key;
    std::string backup_codes;
    std::vector<std::string> backup_codes_list;
    bool is_enabled;
    bool is_verified;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::chrono::system_clock::time_point last_used_at;
    std::string metadata; // JSON string for additional data

    // Default constructor
    TwoFactorAuth() = default;

    // Constructor with user_id
    explicit TwoFactorAuth(const std::string& user_id) : user_id(user_id),
        is_enabled(false), is_verified(false),
        created_at(std::chrono::system_clock::now()),
        updated_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static TwoFactorAuth from_json(const nlohmann::json& j);

    // Utility methods
    bool has_backup_codes() const;
    std::vector<std::string> get_backup_codes() const;
    void set_backup_codes(const std::vector<std::string>& codes);
    bool verify_backup_code(const std::string& code);
};

// Password reset token
struct PasswordResetToken {
    std::string id;
    std::string user_id;
    std::string token;
    std::string email;
    bool is_used;
    std::chrono::system_clock::time_point expires_at;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point used_at;
    std::string ip_address;
    std::string user_agent;
    std::string metadata; // JSON string for additional data

    // Default constructor
    PasswordResetToken() = default;

    // Constructor with required fields
    PasswordResetToken(const std::string& user_id, const std::string& token, const std::string& email)
        : user_id(user_id), token(token), email(email), is_used(false),
          created_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static PasswordResetToken from_json(const nlohmann::json& j);

    // Utility methods
    bool is_expired() const;
    std::chrono::seconds get_remaining_time() const;
    void mark_as_used();
};

// Email verification token
struct EmailVerificationToken {
    std::string id;
    std::string user_id;
    std::string token;
    std::string email;
    bool is_used;
    std::chrono::system_clock::time_point expires_at;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point used_at;
    std::string ip_address;
    std::string user_agent;
    std::string metadata; // JSON string for additional data

    // Default constructor
    EmailVerificationToken() = default;

    // Constructor with required fields
    EmailVerificationToken(const std::string& user_id, const std::string& token, const std::string& email)
        : user_id(user_id), token(token), email(email), is_used(false),
          created_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static EmailVerificationToken from_json(const nlohmann::json& j);

    // Utility methods
    bool is_expired() const;
    std::chrono::seconds get_remaining_time() const;
    void mark_as_used();
};

// User settings
struct UserSettings {
    std::string id;
    std::string user_id;
    bool email_notifications;
    bool push_notifications;
    bool sms_notifications;
    bool marketing_emails;
    bool profile_visibility;
    bool allow_direct_messages;
    bool allow_mentions;
    bool allow_follow_requests;
    bool show_online_status;
    bool show_last_seen;
    bool show_read_receipts;
    std::string privacy_level;
    std::string content_filter_level;
    std::string language;
    std::string timezone;
    std::string date_format;
    std::string time_format;
    std::string theme;
    std::string font_size;
    std::string auto_save_interval;
    bool two_factor_required;
    bool session_timeout_enabled;
    int session_timeout_minutes;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::string metadata; // JSON string for additional data

    // Default constructor
    UserSettings() = default;

    // Constructor with user_id
    explicit UserSettings(const std::string& user_id) : user_id(user_id),
        email_notifications(true), push_notifications(true), sms_notifications(false),
        marketing_emails(false), profile_visibility(true), allow_direct_messages(true),
        allow_mentions(true), allow_follow_requests(true), show_online_status(true),
        show_last_seen(true), show_read_receipts(true), privacy_level("public"),
        content_filter_level("moderate"), language("en"), timezone("UTC"),
        date_format("YYYY-MM-DD"), time_format("24h"), theme("light"),
        font_size("medium"), auto_save_interval("5m"), two_factor_required(false),
        session_timeout_enabled(true), session_timeout_minutes(60),
        created_at(std::chrono::system_clock::now()),
        updated_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static UserSettings from_json(const nlohmann::json& j);
};

// User statistics
struct UserStats {
    std::string id;
    std::string user_id;
    int total_notes;
    int total_followers;
    int total_following;
    int total_likes_received;
    int total_likes_given;
    int total_comments_received;
    int total_comments_given;
    int total_renotes_received;
    int total_renotes_given;
    int total_bookmarks;
    int total_views;
    int total_shares;
    int total_mentions;
    int total_hashtags_used;
    int total_media_uploads;
    int total_login_count;
    int total_session_count;
    std::chrono::system_clock::time_point last_note_at;
    std::chrono::system_clock::time_point last_login_at;
    std::chrono::system_clock::time_point last_activity_at;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::string metadata; // JSON string for additional data

    // Default constructor
    UserStats() = default;

    // Constructor with user_id
    explicit UserStats(const std::string& user_id) : user_id(user_id),
        total_notes(0), total_followers(0), total_following(0),
        total_likes_received(0), total_likes_given(0), total_comments_received(0),
        total_comments_given(0), total_renotes_received(0), total_renotes_given(0),
        total_bookmarks(0), total_views(0), total_shares(0), total_mentions(0),
        total_hashtags_used(0), total_media_uploads(0), total_login_count(0),
        total_session_count(0),
        created_at(std::chrono::system_clock::now()),
        updated_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static UserStats from_json(const nlohmann::json& j);

    // Utility methods
    double get_engagement_rate() const;
    int get_total_interactions() const;
    bool has_activity() const;
};

// User login history
struct UserLoginHistory {
    std::string id;
    std::string user_id;
    std::string session_id;
    std::string device_id;
    std::string device_type;
    std::string device_name;
    std::string ip_address;
    std::string user_agent;
    std::string location;
    std::string country;
    std::string city;
    std::string timezone;
    bool is_successful;
    std::string failure_reason;
    std::chrono::system_clock::time_point login_at;
    std::chrono::system_clock::time_point logout_at;
    std::chrono::system_clock::time_point created_at;
    std::string metadata; // JSON string for additional data

    // Default constructor
    UserLoginHistory() = default;

    // Constructor with required fields
    UserLoginHistory(const std::string& user_id, const std::string& ip_address, bool is_successful)
        : user_id(user_id), ip_address(ip_address), is_successful(is_successful),
          login_at(std::chrono::system_clock::now()),
          created_at(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static UserLoginHistory from_json(const nlohmann::json& j);

    // Utility methods
    std::chrono::seconds get_session_duration() const;
    bool is_current_session() const;
    std::string get_location_display() const;
};

// User search result
struct UserSearchResult {
    std::vector<User> users;
    int total_count;
    int page;
    int page_size;
    bool has_more;
    std::string search_query;
    std::chrono::system_clock::time_point search_timestamp;

    // Default constructor
    UserSearchResult() = default;

    // Constructor with search parameters
    UserSearchResult(const std::string& query, int page, int page_size)
        : total_count(0), page(page), page_size(page_size), has_more(false),
          search_query(query), search_timestamp(std::chrono::system_clock::now()) {}

    // JSON serialization
    nlohmann::json to_json() const;
    static UserSearchResult from_json(const nlohmann::json& j);

    // Utility methods
    void add_user(const User& user);
    bool is_empty() const;
    int get_total_pages() const;
};

} // namespace user
} // namespace sonet