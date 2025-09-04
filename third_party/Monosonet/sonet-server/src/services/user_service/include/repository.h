/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/user.h"
#include "../models/session.h"
#include <memory>
#include <vector>
#include <optional>
#include <map>

namespace sonet::user::repository {

struct UserSearchCriteria {
    std::string query;
    std::vector<std::string> fields; // username, full_name, bio, etc.
    bool include_private = false;
    std::string exclude_user_id; // Exclude this user from results
    int limit = 20;
    int offset = 0;
};

struct UserStatistics {
    std::string user_id;
    int64_t followers_count = 0;
    int64_t following_count = 0;
    int64_t notes_count = 0;
    int64_t likes_count = 0;
    int64_t media_count = 0;
    int64_t profile_views_count = 0;
    int64_t last_updated;
};

struct ActivityLogEntry {
    std::string activity_id;
    std::string user_id;
    std::string activity_type; // login, logout, profile_update, password_change, etc.
    std::map<std::string, std::string> details;
    std::string ip_address;
    std::string user_agent;
    int64_t timestamp;
};

struct PrivacySettings {
    std::string user_id;
    bool is_private_account = false;
    bool allow_message_requests = true;
    bool show_activity_status = true;
    bool show_read_receipts = true;
    bool discoverable_by_email = false;
    bool discoverable_by_phone = false;
    std::vector<std::string> blocked_users;
    std::vector<std::string> muted_users;
    std::vector<std::string> close_friends;
    int64_t updated_at;
};

struct NotificationSettings {
    std::string user_id;
    bool email_notifications = true;
    bool push_notifications = true;
    bool sms_notifications = false;
    std::map<std::string, bool> notification_types; // likes, comments, follows, mentions, etc.
    std::string timezone = "UTC";
    int quiet_hours_start = 22; // 10 PM
    int quiet_hours_end = 8;    // 8 AM
    int64_t updated_at;
};

class UserRepository {
public:
    virtual ~UserRepository() = default;

    // User CRUD operations
    virtual std::future<std::optional<User>> create_user(const User& user) = 0;
    virtual std::future<std::optional<User>> get_user_by_id(const std::string& user_id) = 0;
    virtual std::future<std::optional<User>> get_user_by_username(const std::string& username) = 0;
    virtual std::future<std::optional<User>> get_user_by_email(const std::string& email) = 0;
    virtual std::future<bool> update_user(const User& user) = 0;
    virtual std::future<bool> delete_user(const std::string& user_id) = 0;
    virtual std::future<bool> soft_delete_user(const std::string& user_id) = 0;
    
    // Availability checks
    virtual std::future<bool> is_username_available(const std::string& username) = 0;
    virtual std::future<bool> is_email_available(const std::string& email) = 0;
    
    // Email verification
    virtual std::future<bool> mark_email_verified(const std::string& user_id) = 0;
    virtual std::future<bool> store_verification_token(const std::string& user_id, const std::string& token, int64_t expires_at) = 0;
    virtual std::future<std::optional<std::string>> get_user_by_verification_token(const std::string& token) = 0;
    virtual std::future<bool> delete_verification_token(const std::string& token) = 0;
    
    // Password reset
    virtual std::future<bool> store_password_reset_token(const std::string& user_id, const std::string& token, int64_t expires_at) = 0;
    virtual std::future<std::optional<std::string>> get_user_by_reset_token(const std::string& token) = 0;
    virtual std::future<bool> delete_password_reset_token(const std::string& token) = 0;
    
    // User search
    virtual std::future<std::vector<User>> search_users(const UserSearchCriteria& criteria) = 0;
    virtual std::future<std::vector<User>> get_suggested_users(const std::string& user_id, int limit = 10) = 0;
    
    // User statistics
    virtual std::future<UserStatistics> get_user_statistics(const std::string& user_id) = 0;
    virtual std::future<bool> update_user_statistics(const UserStatistics& stats) = 0;
    virtual std::future<bool> increment_profile_views(const std::string& user_id) = 0;
    
    // Activity logging
    virtual std::future<bool> log_user_activity(const ActivityLogEntry& entry) = 0;
    virtual std::future<std::vector<ActivityLogEntry>> get_user_activity_log(
        const std::string& user_id, 
        const std::string& activity_type = "",
        int limit = 50, 
        int offset = 0) = 0;
    
    // Privacy settings
    virtual std::future<std::optional<PrivacySettings>> get_privacy_settings(const std::string& user_id) = 0;
    virtual std::future<bool> update_privacy_settings(const PrivacySettings& settings) = 0;
    virtual std::future<bool> block_user(const std::string& user_id, const std::string& blocked_user_id) = 0;
    virtual std::future<bool> unblock_user(const std::string& user_id, const std::string& blocked_user_id) = 0;
    virtual std::future<bool> mute_user(const std::string& user_id, const std::string& muted_user_id) = 0;
    virtual std::future<bool> unmute_user(const std::string& user_id, const std::string& muted_user_id) = 0;
    virtual std::future<std::vector<std::string>> get_blocked_users(const std::string& user_id) = 0;
    virtual std::future<std::vector<std::string>> get_muted_users(const std::string& user_id) = 0;
    
    // Notification settings
    virtual std::future<std::optional<NotificationSettings>> get_notification_settings(const std::string& user_id) = 0;
    virtual std::future<bool> update_notification_settings(const NotificationSettings& settings) = 0;
    
    // User reports
    virtual std::future<bool> create_user_report(
        const std::string& reporter_id,
        const std::string& reported_user_id,
        const std::string& reason,
        const std::string& description) = 0;
    
    // Data export
    virtual std::future<std::map<std::string, std::string>> export_user_data(
        const std::string& user_id,
        const std::vector<std::string>& data_types) = 0;
    
    // Health and maintenance
    virtual std::future<bool> cleanup_expired_tokens() = 0;
    virtual std::future<size_t> get_total_users() = 0;
    virtual std::future<size_t> get_active_users(int days = 30) = 0;
    virtual std::future<bool> is_healthy() = 0;
};

class SessionRepository {
public:
    virtual ~SessionRepository() = default;

    // Session CRUD operations
    virtual std::future<bool> create_session(const Session& session) = 0;
    virtual std::future<std::optional<Session>> get_session(const std::string& session_id) = 0;
    virtual std::future<bool> update_session(const Session& session) = 0;
    virtual std::future<bool> delete_session(const std::string& session_id) = 0;
    virtual std::future<std::vector<Session>> get_user_sessions(const std::string& user_id) = 0;
    virtual std::future<bool> delete_user_sessions(const std::string& user_id, const std::string& except_session_id = "") = 0;
    
    // Session validation
    virtual std::future<bool> is_session_valid(const std::string& session_id) = 0;
    virtual std::future<bool> extend_session(const std::string& session_id, int64_t new_expires_at) = 0;
    
    // Device management
    virtual std::future<std::vector<Session>> get_user_devices(const std::string& user_id) = 0;
    virtual std::future<bool> delete_device_sessions(const std::string& user_id, const std::string& device_fingerprint) = 0;
    
    // Cleanup
    virtual std::future<size_t> cleanup_expired_sessions() = 0;
    virtual std::future<bool> is_healthy() = 0;
};

// Factory for creating repository instances
class RepositoryFactory {
public:
    static std::unique_ptr<UserRepository> create_user_repository(const std::string& connection_string);
    static std::unique_ptr<SessionRepository> create_session_repository(const std::string& connection_string);
};

} // namespace sonet::user::repository
