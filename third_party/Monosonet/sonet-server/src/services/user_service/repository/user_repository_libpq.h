/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../../../common/database/base_repository.h"
#include "../models/user.h"
#include "../models/profile.h"
#include "../models/session.h"
#include "../models/user_models.h"
#include <memory>
#include <vector>
#include <optional>
#include <string>

namespace sonet {
namespace user {

// User repository interface using libpq infrastructure
class UserRepositoryLibpq : public database::BaseRepository {
public:
    explicit UserRepositoryLibpq(database::ConnectionPool* pool);
    ~UserRepositoryLibpq() override = default;

    // User management
    std::optional<models::User> create_user(const models::User& user);
    std::optional<models::User> get_user_by_id(const std::string& user_id);
    std::optional<models::User> get_user_by_email(const std::string& email);
    std::optional<models::User> get_user_by_username(const std::string& username);
    bool update_user(const models::User& user);
    bool delete_user(const std::string& user_id);
    bool deactivate_user(const std::string& user_id);
    bool reactivate_user(const std::string& user_id);

    // User search and listing
    std::vector<models::User> search_users(const std::string& query, int limit = 20, int offset = 0);
    std::vector<models::User> get_users_by_ids(const std::vector<std::string>& user_ids);
    std::vector<models::User> get_active_users(int limit = 100, int offset = 0);
    std::vector<models::User> get_users_by_role(const std::string& role, int limit = 100, int offset = 0);

    // User profile management
    std::optional<models::Profile> get_user_profile(const std::string& user_id);
    bool update_user_profile(const models::Profile& profile);
    bool update_user_avatar(const std::string& user_id, const std::string& avatar_url);
    bool update_user_banner(const std::string& user_id, const std::string& banner_url);

    // Authentication and sessions
    std::optional<models::Session> create_session(const models::Session& session);
    std::optional<models::Session> get_session_by_token(const std::string& token);
    bool update_session(const models::Session& session);
    bool delete_session(const std::string& token);
    bool delete_user_sessions(const std::string& user_id);
    bool delete_expired_sessions();
    std::vector<models::Session> get_user_sessions(const std::string& user_id);

    // Two-factor authentication
    std::optional<TwoFactorAuth> create_two_factor_auth(const TwoFactorAuth& tfa);
    std::optional<TwoFactorAuth> get_two_factor_auth(const std::string& user_id);
    bool update_two_factor_auth(const TwoFactorAuth& tfa);
    bool delete_two_factor_auth(const std::string& user_id);
    bool verify_two_factor_code(const std::string& user_id, const std::string& code);

    // Password management
    std::optional<PasswordResetToken> create_password_reset_token(const PasswordResetToken& token);
    std::optional<PasswordResetToken> get_password_reset_token(const std::string& token);
    bool delete_password_reset_token(const std::string& token);
    bool delete_expired_password_reset_tokens();
    bool update_user_password(const std::string& user_id, const std::string& hashed_password);

    // Email verification
    std::optional<EmailVerificationToken> create_email_verification_token(const EmailVerificationToken& token);
    std::optional<EmailVerificationToken> get_email_verification_token(const std::string& token);
    bool delete_email_verification_token(const std::string& token);
    bool delete_expired_email_verification_tokens();
    bool verify_user_email(const std::string& user_id);

    // User settings
    std::optional<UserSettings> get_user_settings(const std::string& user_id);
    bool update_user_settings(const UserSettings& settings);
    bool update_user_setting(const std::string& user_id, const std::string& setting_key, const std::string& setting_value);

    // User statistics
    std::optional<UserStats> get_user_stats(const std::string& user_id);
    bool update_user_stats(const UserStats& stats);
    bool increment_user_stat(const std::string& user_id, const std::string& stat_name, int increment = 1);

    // Login history
    bool add_login_history(const UserLoginHistory& history);
    std::vector<UserLoginHistory> get_user_login_history(const std::string& user_id, int limit = 50);
    bool delete_old_login_history(int days_to_keep = 90);

    // User validation
    bool is_email_taken(const std::string& email);
    bool is_username_taken(const std::string& username);
    bool is_user_active(const std::string& user_id);
    bool is_user_verified(const std::string& user_id);

    // Bulk operations
    bool bulk_update_users(const std::vector<models::User>& users);
    bool bulk_delete_users(const std::vector<std::string>& user_ids);
    bool bulk_deactivate_users(const std::vector<std::string>& user_ids);

    // Analytics and reporting
    int get_total_user_count();
    int get_active_user_count();
    int get_verified_user_count();
    std::vector<std::pair<std::string, int>> get_users_by_role_count();
    std::vector<std::pair<std::string, int>> get_users_by_status_count();

private:
    // Helper methods for query building
    std::string build_user_select_query() const;
    std::string build_user_insert_query() const;
    std::string build_user_update_query() const;
    std::string build_user_search_query(const std::string& query) const;

    // Helper methods for result mapping
    models::User map_result_to_user(pg_result* result, int row) const;
    models::Profile map_result_to_user_profile(pg_result* result, int row) const;
    models::Session map_result_to_user_session(pg_result* result, int row) const;
    TwoFactorAuth map_result_to_two_factor_auth(pg_result* result, int row) const;
    PasswordResetToken map_result_to_password_reset_token(pg_result* result, int row) const;
    EmailVerificationToken map_result_to_email_verification_token(pg_result* result, int row) const;
    UserSettings map_result_to_user_settings(pg_result* result, int row) const;
    UserStats map_result_to_user_stats(pg_result* result, int row) const;
    UserLoginHistory map_result_to_user_login_history(pg_result* result, int row) const;

    // Initialize prepared statements
    bool initialize_prepared_statements();
    
    // Helper for UUID generation
    std::string generate_uuid() const;
    
    // Helper for timestamp conversion
    std::string timestamp_to_db_string(const std::chrono::system_clock::time_point& tp) const;
    std::chrono::system_clock::time_point db_string_to_timestamp(const std::string& str) const;
};

} // namespace user
} // namespace sonet