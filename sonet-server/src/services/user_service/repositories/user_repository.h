/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../include/repository.h"
#include "../models/user.h"
#include "../models/session.h"
#include <pqxx/pqxx>
#include <memory>
#include <optional>
#include <vector>
#include <string>

namespace sonet::user::repositories {

using namespace sonet::user::models;

// postgresql implementation of UserRepository
class PostgreSQLUserRepository : public IUserRepository {
private:
    std::shared_ptr<pqxx::connection> db_connection_;
    std::string table_name_;
    std::string user_stats_table_;
    std::string user_settings_table_;
    std::string blocked_users_table_;
    std::string muted_users_table_;
    std::string close_friends_table_;
    
    // Database connection management
    void ensure_connection();
    void reconnect_if_needed();
    bool test_connection();
    
    // Query builders
    std::string build_select_query(const std::vector<std::string>& fields = {}) const;
    std::string build_insert_query(const User& user) const;
    std::string build_update_query(const User& user) const;
    std::string build_search_query(const UserSearchCriteria& criteria) const;
    
    // Result mapping
    User map_row_to_user(const pqxx::row& row) const;
    std::vector<User> map_result_to_users(const pqxx::result& result) const;
    
    // Helper methods for complex operations
    void update_user_statistics(pqxx::work& txn, const std::string& user_id, 
                               const std::string& stat_name, int delta);
    void update_user_relationships(pqxx::work& txn, const User& user);
    void handle_user_deletion(pqxx::work& txn, const std::string& user_id, 
                             const std::string& reason);
    
    // Validation and security
    bool validate_user_data(const User& user) const;
    bool check_unique_constraints(const User& user, bool is_update = false) const;
    void log_user_operation(const std::string& operation, const std::string& user_id) const;

public:
    explicit PostgreSQLUserRepository(std::shared_ptr<pqxx::connection> connection);
    virtual ~PostgreSQLUserRepository() = default;
    
    // Basic CRUD operations
    bool create(const User& user) override;
    std::optional<User> get_by_id(const std::string& user_id) override;
    std::optional<User> get_by_username(const std::string& username) override;
    std::optional<User> get_by_email(const std::string& email) override;
    bool update(const User& user) override;
    bool delete_user(const std::string& user_id, const std::string& reason) override;
    
    // Batch operations
    std::vector<User> get_by_ids(const std::vector<std::string>& user_ids) override;
    bool update_multiple(const std::vector<User>& users) override;
    bool delete_multiple(const std::vector<std::string>& user_ids, const std::string& reason) override;
    
    // Search and filtering
    SearchResult<User> search(const UserSearchCriteria& criteria) override;
    std::vector<User> get_recently_active(int limit, int hours_back) override;
    std::vector<User> get_new_users(int limit, int days_back) override;
    std::vector<User> get_users_by_status(UserStatus status, int limit, int offset) override;
    
    // User statistics and metrics
    bool increment_stat(const std::string& user_id, const std::string& stat_name, int amount = 1) override;
    bool decrement_stat(const std::string& user_id, const std::string& stat_name, int amount = 1) override;
    UserStats get_user_stats(const std::string& user_id) override;
    bool update_user_stats(const std::string& user_id, const UserStats& stats) override;
    
    // User verification and security
    bool verify_email(const std::string& user_id, const std::string& verification_token) override;
    bool verify_phone(const std::string& user_id, const std::string& verification_code) override;
    bool update_password(const std::string& user_id, const std::string& password_hash, 
                        const std::string& salt) override;
    bool reset_password(const std::string& user_id, const std::string& new_password_hash,
                       const std::string& salt, const std::string& reset_token) override;
    
    // User relationships
    bool block_user(const std::string& user_id, const std::string& blocked_user_id) override;
    bool unblock_user(const std::string& user_id, const std::string& blocked_user_id) override;
    bool mute_user(const std::string& user_id, const std::string& muted_user_id) override;
    bool unmute_user(const std::string& user_id, const std::string& muted_user_id) override;
    bool add_close_friend(const std::string& user_id, const std::string& friend_user_id) override;
    bool remove_close_friend(const std::string& user_id, const std::string& friend_user_id) override;
    std::vector<std::string> get_blocked_users(const std::string& user_id) override;
    std::vector<std::string> get_muted_users(const std::string& user_id) override;
    std::vector<std::string> get_close_friends(const std::string& user_id) override;
    
    // User account management
    bool suspend_user(const std::string& user_id, const std::string& reason, 
                     std::time_t until_timestamp) override;
    bool unsuspend_user(const std::string& user_id) override;
    bool ban_user(const std::string& user_id, const std::string& reason) override;
    bool unban_user(const std::string& user_id) override;
    bool deactivate_user(const std::string& user_id) override;
    bool reactivate_user(const std::string& user_id) override;
    
    // User discovery and recommendations
    std::vector<User> find_users_by_email_domain(const std::string& domain, int limit) override;
    std::vector<User> find_users_by_location(const std::string& location, int limit) override;
    std::vector<User> get_verified_users(int limit, int offset) override;
    std::vector<User> get_premium_users(int limit, int offset) override;
    
    // Analytics and reporting
    int count_total_users() override;
    int count_active_users(int days_back = 30) override;
    int count_users_by_status(UserStatus status) override;
    std::map<std::string, int> get_user_registration_stats(int days_back = 30) override;
    std::map<std::string, int> get_user_activity_stats(int days_back = 7) override;
    
    // Database maintenance
    bool cleanup_deleted_users(int days_old = 30) override;
    bool vacuum_user_data() override;
    bool reindex_user_tables() override;
    DatabaseHealthStatus check_database_health() override;
    
    // Additional postgresql-specific methods
    bool create_indexes();
    bool optimize_performance();
        std::string get_connection_info() const;
    bool backup_user_data(const std::string& backup_path);
    bool restore_user_data(const std::string& backup_path);
    
    private:
// Internal helper methods
    void setup_prepared_statements();
    void create_database_schema();
    void migrate_database_schema();
    bool validate_database_schema() const;
    void handle_database_error(const std::exception& e, const std::string& operation) const;
};

// Session repository for managing user sessions
class NotegreSQLSessionRepository : public ISessionRepository {
private:
    std::shared_ptr<pqxx::connection> db_connection_;
    std::string sessions_table_;
    std::string session_devices_table_;
    std::string session_locations_table_;
    std::string session_activities_table_;
    
    void ensure_connection();
    Session map_row_to_session(const pqxx::row& row) const;
    std::vector<Session> map_result_to_sessions(const pqxx::result& result) const;
    void update_session_activity(pqxx::work& txn, const std::string& session_id);
    void cleanup_expired_sessions(pqxx::work& txn);

public:
    explicit NotegreSQLSessionRepository(std::shared_ptr<pqxx::connection> connection);
    virtual ~NotegreSQLSessionRepository() = default;
    
    // Session lifecycle
    bool create_session(const Session& session) override;
    std::optional<Session> get_session(const std::string& session_id) override;
    std::optional<Session> get_by_access_token(const std::string& access_token) override;
    bool update_session(const Session& session) override;
    bool delete_session(const std::string& session_id) override;
    bool expire_session(const std::string& session_id) override;
    bool revoke_session(const std::string& session_id, const std::string& reason) override;
    
    // User session management
    std::vector<Session> get_user_sessions(const std::string& user_id, bool active_only = true) override;
    bool revoke_all_user_sessions(const std::string& user_id, const std::string& reason,
                                 const std::string& except_session_id = "") override;
    bool revoke_user_sessions_except(const std::string& user_id, 
                                    const std::vector<std::string>& keep_session_ids) override;
    
    // Session analytics
    int count_active_sessions(const std::string& user_id = "") override;
    int count_sessions_by_device_type(DeviceType device_type) override;
    std::vector<Session> get_suspicious_sessions() override;
    std::vector<Session> get_expired_sessions(int hours_old = 24) override;
    
    // Session cleanup
    bool cleanup_expired_sessions(int hours_old = 48) override;
    bool cleanup_revoked_sessions(int days_old = 7) override;
    SessionCleanupResult perform_maintenance() override;
};

// Factory for creating repository instances
class RepositoryFactory {
public:
    static std::unique_ptr<PostgreSQLUserRepository> create_user_repository(
        const std::string& connection_string);
    static std::unique_ptr<NotegreSQLSessionRepository> create_session_repository(
        const std::string& connection_string);
    static std::shared_ptr<pqxx::connection> create_database_connection(
        const std::string& connection_string);
    
    // Connection pool management
    static void initialize_connection_pool(const std::string& connection_string, int pool_size = 10);
    static std::shared_ptr<pqxx::connection> get_pooled_connection();
    static void return_connection(std::shared_ptr<pqxx::connection> connection);
    static void shutdown_connection_pool();
};

} // namespace sonet::user::repositories