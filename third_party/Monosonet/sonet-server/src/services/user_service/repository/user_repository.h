#pragma once

#include "../../../common/database/base_repository.h"
#include <memory>
#include <vector>
#include <optional>
#include <string>

namespace sonet {
namespace user {

// Forward declarations
struct User;
struct UserProfile;
struct UserSession;
struct TwoFactorAuth;
struct PasswordResetToken;
struct EmailVerificationToken;
struct UserSettings;
struct UserStats;
struct UserLoginHistory;

// User repository interface
class UserRepository : public database::BaseRepository {
public:
    explicit UserRepository(database::ConnectionPool* pool);
    ~UserRepository() override = default;

    // User management
    std::optional<User> create_user(const User& user);
    std::optional<User> get_user_by_id(const std::string& user_id);
    std::optional<User> get_user_by_email(const std::string& email);
    std::optional<User> get_user_by_username(const std::string& username);
    bool update_user(const User& user);
    bool delete_user(const std::string& user_id);
    bool deactivate_user(const std::string& user_id);
    bool reactivate_user(const std::string& user_id);

    // User search and listing
    std::vector<User> search_users(const std::string& query, int limit = 20, int offset = 0);
    std::vector<User> get_users_by_ids(const std::vector<std::string>& user_ids);
    std::vector<User> get_active_users(int limit = 100, int offset = 0);
    std::vector<User> get_users_by_role(const std::string& role, int limit = 100, int offset = 0);

    // User profile management
    std::optional<UserProfile> get_user_profile(const std::string& user_id);
    bool update_user_profile(const UserProfile& profile);
    bool update_user_avatar(const std::string& user_id, const std::string& avatar_url);
    bool update_user_banner(const std::string& user_id, const std::string& banner_url);

    // Authentication and sessions
    std::optional<UserSession> create_session(const UserSession& session);
    std::optional<UserSession> get_session_by_token(const std::string& token);
    bool update_session(const UserSession& session);
    bool delete_session(const std::string& token);
    bool delete_user_sessions(const std::string& user_id);
    bool delete_expired_sessions();
    std::vector<UserSession> get_user_sessions(const std::string& user_id);

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
    bool bulk_update_users(const std::vector<User>& users);
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
    User map_result_to_user(pg_result* result, int row) const;
    UserProfile map_result_to_user_profile(pg_result* result, int row) const;
    UserSession map_result_to_user_session(pg_result* result, int row) const;
    TwoFactorAuth map_result_to_two_factor_auth(pg_result* result, int row) const;
    PasswordResetToken map_result_to_password_reset_token(pg_result* result, int row) const;
    EmailVerificationToken map_result_to_email_verification_token(pg_result* result, int row) const;
    UserSettings map_result_to_user_settings(pg_result* result, int row) const;
    UserStats map_result_to_user_stats(pg_result* result, int row) const;
    UserLoginHistory map_result_to_user_login_history(pg_result* result, int row) const;

    // Prepared statement names
    static constexpr const char* STMT_CREATE_USER = "create_user";
    static constexpr const char* STMT_GET_USER_BY_ID = "get_user_by_id";
    static constexpr const char* STMT_GET_USER_BY_EMAIL = "get_user_by_email";
    static constexpr const char* STMT_GET_USER_BY_USERNAME = "get_user_by_username";
    static constexpr const char* STMT_UPDATE_USER = "update_user";
    static constexpr const char* STMT_DELETE_USER = "delete_user";
    static constexpr const char* STMT_SEARCH_USERS = "search_users";
    static constexpr const char* STMT_GET_USERS_BY_IDS = "get_users_by_ids";
    static constexpr const char* STMT_GET_ACTIVE_USERS = "get_active_users";
    static constexpr const char* STMT_GET_USERS_BY_ROLE = "get_users_by_role";
    static constexpr const char* STMT_GET_USER_PROFILE = "get_user_profile";
    static constexpr const char* STMT_UPDATE_USER_PROFILE = "update_user_profile";
    static constexpr const char* STMT_CREATE_SESSION = "create_session";
    static constexpr const char* STMT_GET_SESSION_BY_TOKEN = "get_session_by_token";
    static constexpr const char* STMT_UPDATE_SESSION = "update_session";
    static constexpr const char* STMT_DELETE_SESSION = "delete_session";
    static constexpr const char* STMT_GET_USER_SESSIONS = "get_user_sessions";
    static constexpr const char* STMT_CREATE_TWO_FACTOR_AUTH = "create_two_factor_auth";
    static constexpr const char* STMT_GET_TWO_FACTOR_AUTH = "get_two_factor_auth";
    static constexpr const char* STMT_UPDATE_TWO_FACTOR_AUTH = "update_two_factor_auth";
    static constexpr const char* STMT_VERIFY_TWO_FACTOR_CODE = "verify_two_factor_code";
    static constexpr const char* STMT_CREATE_PASSWORD_RESET_TOKEN = "create_password_reset_token";
    static constexpr const char* STMT_GET_PASSWORD_RESET_TOKEN = "get_password_reset_token";
    static constexpr const char* STMT_CREATE_EMAIL_VERIFICATION_TOKEN = "create_email_verification_token";
    static constexpr const char* STMT_GET_EMAIL_VERIFICATION_TOKEN = "get_email_verification_token";
    static constexpr const char* STMT_GET_USER_SETTINGS = "get_user_settings";
    static constexpr const char* STMT_UPDATE_USER_SETTINGS = "update_user_settings";
    static constexpr const char* STMT_GET_USER_STATS = "get_user_stats";
    static constexpr const char* STMT_UPDATE_USER_STATS = "update_user_stats";
    static constexpr const char* STMT_ADD_LOGIN_HISTORY = "add_login_history";
    static constexpr const char* STMT_GET_USER_LOGIN_HISTORY = "get_user_login_history";

    // Initialize prepared statements
    bool initialize_prepared_statements();
};

} // namespace user
} // namespace sonet