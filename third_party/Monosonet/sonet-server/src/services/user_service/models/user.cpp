/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "user.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <regex>
#include <algorithm>

namespace sonet::user::models {

User::User() {
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    created_at = now;
    updated_at = now;
}

User::User(const std::string& user_id, const std::string& username, const std::string& email)
    : user_id(user_id), username(username), email(email) {
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    created_at = now;
    updated_at = now;
}

bool User::is_active() const {
    return status == UserStatus::ACTIVE && !is_deleted;
}

bool User::can_login() const {
    return is_active() && is_email_verified && 
           (suspended_until.has_value() ? *suspended_until < std::time(nullptr) : true);
}

bool User::can_note() const {
    return can_login() && status != UserStatus::SUSPENDED;
}

bool User::is_public() const {
    return privacy_level == PrivacyLevel::PUBLIC;
}

bool User::is_protected() const {
    return privacy_level == PrivacyLevel::PROTECTED;
}

bool User::is_private() const {
    return privacy_level == PrivacyLevel::PRIVATE;
}

bool User::is_blocked_user(const std::string& user_id) const {
    return std::find(blocked_users.begin(), blocked_users.end(), user_id) != blocked_users.end();
}

bool User::is_muted_user(const std::string& user_id) const {
    return std::find(muted_users.begin(), muted_users.end(), user_id) != muted_users.end();
}

bool User::is_close_friend(const std::string& user_id) const {
    return std::find(close_friends.begin(), close_friends.end(), user_id) != close_friends.end();
}

double User::get_profile_completeness_percentage() const {
    int completed_fields = 0;
    int total_fields = 12;
    
    if (!username.empty()) completed_fields++;
    if (!email.empty()) completed_fields++;
    if (!display_name.empty()) completed_fields++;
    if (!first_name.empty()) completed_fields++;
    if (!last_name.empty()) completed_fields++;
    if (!bio.empty()) completed_fields++;
    if (!location.empty()) completed_fields++;
    if (!website.empty()) completed_fields++;
    if (!avatar_url.empty()) completed_fields++;
    if (!banner_url.empty()) completed_fields++;
    if (!phone_number.empty()) completed_fields++;
    if (is_email_verified) completed_fields++;
    
    return (static_cast<double>(completed_fields) / total_fields) * 100.0;
}

std::vector<std::string> User::get_missing_profile_fields() const {
    std::vector<std::string> missing_fields;
    
    if (username.empty()) missing_fields.push_back("username");
    if (email.empty()) missing_fields.push_back("email");
    if (display_name.empty()) missing_fields.push_back("display_name");
    if (first_name.empty()) missing_fields.push_back("first_name");
    if (last_name.empty()) missing_fields.push_back("last_name");
    if (bio.empty()) missing_fields.push_back("bio");
    if (location.empty()) missing_fields.push_back("location");
    if (website.empty()) missing_fields.push_back("website");
    if (avatar_url.empty()) missing_fields.push_back("avatar_url");
    if (banner_url.empty()) missing_fields.push_back("banner_url");
    if (phone_number.empty()) missing_fields.push_back("phone_number");
    if (!is_email_verified) missing_fields.push_back("email_verification");
    
    return missing_fields;
}

std::optional<int> User::get_account_age_days() const {
    auto now = std::time(nullptr);
    if (created_at > 0) {
        return static_cast<int>((now - created_at) / (24 * 3600));
    }
    return std::nullopt;
}

bool User::needs_reverification() const {
    // Require reverification if account is old and not verified
    auto age = get_account_age_days();
    return age.has_value() && *age > 30 && !is_verified;
}

std::string User::to_json() const {
    nlohmann::json j;
    
    j["user_id"] = user_id;
    j["username"] = username;
    j["email"] = email;
    j["phone_number"] = phone_number;
    j["display_name"] = display_name;
    j["first_name"] = first_name;
    j["last_name"] = last_name;
    j["bio"] = bio;
    j["location"] = location;
    j["website"] = website;
    j["avatar_url"] = avatar_url;
    j["banner_url"] = banner_url;
    j["timezone"] = timezone;
    j["language"] = language;
    j["status"] = static_cast<int>(status);
    j["account_type"] = static_cast<int>(account_type);
    j["privacy_level"] = static_cast<int>(privacy_level);
    j["is_verified"] = is_verified;
    j["is_premium"] = is_premium;
    j["is_developer"] = is_developer;
    j["is_email_verified"] = is_email_verified;
    j["is_phone_verified"] = is_phone_verified;
    j["discoverable_by_email"] = discoverable_by_email;
    j["discoverable_by_phone"] = discoverable_by_phone;
    j["allow_direct_messages"] = allow_direct_messages;
    j["allow_message_requests"] = allow_message_requests;
    j["show_activity_status"] = show_activity_status;
    j["show_read_receipts"] = show_read_receipts;
    j["nsfw_content_enabled"] = nsfw_content_enabled;
    j["autoplay_videos"] = autoplay_videos;
    j["high_quality_images"] = high_quality_images;
    j["email_notifications"] = email_notifications;
    j["push_notifications"] = push_notifications;
    j["sms_notifications"] = sms_notifications;
    j["followers_count"] = followers_count;
    j["following_count"] = following_count;
    j["notes_count"] = notes_count;
    j["likes_count"] = likes_count;
    j["media_count"] = media_count;
    j["profile_views_count"] = profile_views_count;
    j["blocked_users"] = blocked_users;
    j["muted_users"] = muted_users;
    j["close_friends"] = close_friends;
    j["created_at"] = created_at;
    j["updated_at"] = updated_at;
    j["last_login_at"] = last_login_at;
    j["last_active_at"] = last_active_at;
    j["created_from_ip"] = created_from_ip;
    j["last_login_ip"] = last_login_ip;
    j["is_deleted"] = is_deleted;
    j["deleted_at"] = deleted_at;
    j["deletion_reason"] = deletion_reason;
    
    if (suspended_until.has_value()) {
        j["suspended_until"] = *suspended_until;
    }
    j["suspension_reason"] = suspension_reason;
    j["banned_reason"] = banned_reason;
    
    return j.dump();
}

void User::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        
        user_id = j.value("user_id", "");
        username = j.value("username", "");
        email = j.value("email", "");
        phone_number = j.value("phone_number", "");
        display_name = j.value("display_name", "");
        first_name = j.value("first_name", "");
        last_name = j.value("last_name", "");
        bio = j.value("bio", "");
        location = j.value("location", "");
        website = j.value("website", "");
        avatar_url = j.value("avatar_url", "");
        banner_url = j.value("banner_url", "");
        timezone = j.value("timezone", "UTC");
        language = j.value("language", "en");
        status = static_cast<UserStatus>(j.value("status", 0));
        account_type = static_cast<AccountType>(j.value("account_type", 0));
        privacy_level = static_cast<PrivacyLevel>(j.value("privacy_level", 0));
        is_verified = j.value("is_verified", false);
        is_premium = j.value("is_premium", false);
        is_developer = j.value("is_developer", false);
        is_email_verified = j.value("is_email_verified", false);
        is_phone_verified = j.value("is_phone_verified", false);
        discoverable_by_email = j.value("discoverable_by_email", true);
        discoverable_by_phone = j.value("discoverable_by_phone", false);
        allow_direct_messages = j.value("allow_direct_messages", true);
        allow_message_requests = j.value("allow_message_requests", true);
        show_activity_status = j.value("show_activity_status", true);
        show_read_receipts = j.value("show_read_receipts", true);
        nsfw_content_enabled = j.value("nsfw_content_enabled", false);
        autoplay_videos = j.value("autoplay_videos", true);
        high_quality_images = j.value("high_quality_images", true);
        email_notifications = j.value("email_notifications", true);
        push_notifications = j.value("push_notifications", true);
        sms_notifications = j.value("sms_notifications", false);
        followers_count = j.value("followers_count", 0);
        following_count = j.value("following_count", 0);
        notes_count = j.value("notes_count", 0);
        likes_count = j.value("likes_count", 0);
        media_count = j.value("media_count", 0);
        profile_views_count = j.value("profile_views_count", 0);
        
        if (j.contains("blocked_users")) {
            blocked_users = j["blocked_users"].get<std::vector<std::string>>();
        }
        if (j.contains("muted_users")) {
            muted_users = j["muted_users"].get<std::vector<std::string>>();
        }
        if (j.contains("close_friends")) {
            close_friends = j["close_friends"].get<std::vector<std::string>>();
        }
        
        created_at = j.value("created_at", 0);
        updated_at = j.value("updated_at", 0);
        last_login_at = j.value("last_login_at", 0);
        last_active_at = j.value("last_active_at", 0);
        created_from_ip = j.value("created_from_ip", "");
        last_login_ip = j.value("last_login_ip", "");
        is_deleted = j.value("is_deleted", false);
        deleted_at = j.value("deleted_at", 0);
        deletion_reason = j.value("deletion_reason", "");
        
        if (j.contains("suspended_until")) {
            suspended_until = j["suspended_until"];
        }
        suspension_reason = j.value("suspension_reason", "");
        banned_reason = j.value("banned_reason", "");
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse user JSON: {}", e.what());
    }
}

User User::get_public_view() const {
    User public_user = *this;
    
    // Clear sensitive information
    public_user.email = "";
    public_user.phone_number = "";
    public_user.password_hash = "";
    public_user.salt = "";
    public_user.email_verification_token = "";
    public_user.phone_verification_code = "";
    public_user.blocked_users.clear();
    public_user.muted_users.clear();
    public_user.close_friends.clear();
    public_user.created_from_ip = "";
    public_user.last_login_ip = "";
    public_user.last_login_at = 0;
    public_user.last_active_at = 0;
    
    // Hide notification preferences
    public_user.email_notifications = false;
    public_user.push_notifications = false;
    public_user.sms_notifications = false;
    
    return public_user;
}

User User::get_protected_view() const {
    User protected_user = get_public_view();
    
    // Show some additional info for followers
    if (!is_private()) {
        protected_user.email = email.substr(0, 3) + "****@" + email.substr(email.find('@') + 1);
    }
    
    return protected_user;
}

User User::get_follower_view() const {
    User follower_user = get_protected_view();
    
    // Show activity status to followers
    follower_user.last_active_at = last_active_at;
    
    return follower_user;
}

User User::get_self_view() const {
    return *this; // Show everything to self
}

bool User::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> User::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (user_id.empty()) {
        errors.push_back("User ID is required");
    }
    
    if (username.empty()) {
        errors.push_back("Username is required");
    } else if (username.length() < 3 || username.length() > 50) {
        errors.push_back("Username must be between 3 and 50 characters");
    }
    
    if (email.empty()) {
        errors.push_back("Email is required");
    } else {
        std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
        if (!std::regex_match(email, email_regex)) {
            errors.push_back("Invalid email format");
        }
    }
    
    if (bio.length() > 500) {
        errors.push_back("Bio cannot exceed 500 characters");
    }
    
    if (location.length() > 100) {
        errors.push_back("Location cannot exceed 100 characters");
    }
    
    if (!website.empty()) {
        std::regex url_regex(R"(^https?://[^\s/$.?#].[^\s]*$)");
        if (!std::regex_match(website, url_regex)) {
            errors.push_back("Invalid website URL format");
        }
    }
    
    return errors;
}

// UserCreateRequest implementation
bool UserCreateRequest::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> UserCreateRequest::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (username.empty() || username.length() < 3 || username.length() > 50) {
        errors.push_back("Username must be between 3 and 50 characters");
    }
    
    if (email.empty()) {
        errors.push_back("Email is required");
    } else {
        std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
        if (!std::regex_match(email, email_regex)) {
            errors.push_back("Invalid email format");
        }
    }
    
    if (password.empty() || password.length() < 8) {
        errors.push_back("Password must be at least 8 characters");
    }
    
    if (!terms_accepted) {
        errors.push_back("Terms of service must be accepted");
    }
    
    if (!privacy_policy_accepted) {
        errors.push_back("Privacy policy must be accepted");
    }
    
    if (bio.length() > 500) {
        errors.push_back("Bio cannot exceed 500 characters");
    }
    
    return errors;
}

// UserUpdateRequest implementation
bool UserUpdateRequest::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> UserUpdateRequest::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (user_id.empty()) {
        errors.push_back("User ID is required");
    }
    
    if (bio.has_value() && bio->length() > 500) {
        errors.push_back("Bio cannot exceed 500 characters");
    }
    
    if (location.has_value() && location->length() > 100) {
        errors.push_back("Location cannot exceed 100 characters");
    }
    
    if (website.has_value() && !website->empty()) {
        std::regex url_regex(R"(^https?://[^\s/$.?#].[^\s]*$)");
        if (!std::regex_match(*website, url_regex)) {
            errors.push_back("Invalid website URL format");
        }
    }
    
    return errors;
}

std::vector<std::string> UserUpdateRequest::get_updated_fields() const {
    std::vector<std::string> fields;
    
    if (display_name.has_value()) fields.push_back("display_name");
    if (first_name.has_value()) fields.push_back("first_name");
    if (last_name.has_value()) fields.push_back("last_name");
    if (bio.has_value()) fields.push_back("bio");
    if (location.has_value()) fields.push_back("location");
    if (website.has_value()) fields.push_back("website");
    if (avatar_url.has_value()) fields.push_back("avatar_url");
    if (banner_url.has_value()) fields.push_back("banner_url");
    if (timezone.has_value()) fields.push_back("timezone");
    if (language.has_value()) fields.push_back("language");
    if (privacy_level.has_value()) fields.push_back("privacy_level");
    if (discoverable_by_email.has_value()) fields.push_back("discoverable_by_email");
    if (discoverable_by_phone.has_value()) fields.push_back("discoverable_by_phone");
    if (allow_direct_messages.has_value()) fields.push_back("allow_direct_messages");
    if (allow_message_requests.has_value()) fields.push_back("allow_message_requests");
    if (show_activity_status.has_value()) fields.push_back("show_activity_status");
    if (show_read_receipts.has_value()) fields.push_back("show_read_receipts");
    if (nsfw_content_enabled.has_value()) fields.push_back("nsfw_content_enabled");
    if (autoplay_videos.has_value()) fields.push_back("autoplay_videos");
    if (high_quality_images.has_value()) fields.push_back("high_quality_images");
    if (email_notifications.has_value()) fields.push_back("email_notifications");
    if (push_notifications.has_value()) fields.push_back("push_notifications");
    if (sms_notifications.has_value()) fields.push_back("sms_notifications");
    
    return fields;
}

// Utility functions
std::string user_status_to_string(UserStatus status) {
    switch (status) {
        case UserStatus::ACTIVE: return "active";
        case UserStatus::INACTIVE: return "inactive";
        case UserStatus::SUSPENDED: return "suspended";
        case UserStatus::BANNED: return "banned";
        case UserStatus::PENDING_VERIFICATION: return "pending_verification";
        case UserStatus::DEACTIVATED: return "deactivated";
        default: return "unknown";
    }
}

UserStatus string_to_user_status(const std::string& status) {
    if (status == "active") return UserStatus::ACTIVE;
    if (status == "inactive") return UserStatus::INACTIVE;
    if (status == "suspended") return UserStatus::SUSPENDED;
    if (status == "banned") return UserStatus::BANNED;
    if (status == "pending_verification") return UserStatus::PENDING_VERIFICATION;
    if (status == "deactivated") return UserStatus::DEACTIVATED;
    return UserStatus::ACTIVE;
}

std::string account_type_to_string(AccountType type) {
    switch (type) {
        case AccountType::PERSONAL: return "personal";
        case AccountType::BUSINESS: return "business";
        case AccountType::VERIFIED: return "verified";
        case AccountType::PREMIUM: return "premium";
        case AccountType::DEVELOPER: return "developer";
        default: return "personal";
    }
}

AccountType string_to_account_type(const std::string& type) {
    if (type == "personal") return AccountType::PERSONAL;
    if (type == "business") return AccountType::BUSINESS;
    if (type == "verified") return AccountType::VERIFIED;
    if (type == "premium") return AccountType::PREMIUM;
    if (type == "developer") return AccountType::DEVELOPER;
    return AccountType::PERSONAL;
}

std::string privacy_level_to_string(PrivacyLevel level) {
    switch (level) {
        case PrivacyLevel::PUBLIC: return "public";
        case PrivacyLevel::PROTECTED: return "protected";
        case PrivacyLevel::PRIVATE: return "private";
        default: return "public";
    }
}

PrivacyLevel string_to_privacy_level(const std::string& level) {
    if (level == "public") return PrivacyLevel::PUBLIC;
    if (level == "protected") return PrivacyLevel::PROTECTED;
    if (level == "private") return PrivacyLevel::PRIVATE;
    return PrivacyLevel::PUBLIC;
}

bool operator==(const User& lhs, const User& rhs) {
    return lhs.user_id == rhs.user_id;
}

bool operator!=(const User& lhs, const User& rhs) {
    return !(lhs == rhs);
}

} // namespace sonet::user::models