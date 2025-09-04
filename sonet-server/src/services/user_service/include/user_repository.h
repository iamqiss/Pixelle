/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "models/user.h"
#include "models/user_session.h"
#include <pqxx/pqxx>
#include <memory>
#include <optional>
#include <vector>
#include <string>

namespace sonet::user {

/**
 * Repository class for user-related database operations.
 * Handles all CRUD operations for users and sessions with optimized queries.
 */
class UserRepository {
public:
    explicit UserRepository(std::shared_ptr<pqxx::connection_pool> pool);
    ~UserRepository() = default;

    // User CRUD operations
    std::optional<User> create_user(const User& user);
    std::optional<User> get_user_by_id(const std::string& user_id);
    std::optional<User> get_user_by_username(const std::string& username);
    std::optional<User> get_user_by_email(const std::string& email);
    std::optional<User> update_user(const User& user);
    bool delete_user(const std::string& user_id);
    
    // User status operations
    bool verify_user_email(const std::string& user_id);
    bool update_user_status(const std::string& user_id, UserStatus status);
    bool update_last_login(const std::string& user_id);
    
    // Password operations
    bool update_password_hash(const std::string& user_id, const std::string& password_hash);
    std::optional<std::string> get_password_hash(const std::string& user_id);
    
    // Session operations
    std::optional<UserSession> create_session(const UserSession& session);
    std::optional<UserSession> get_session_by_id(const std::string& session_id);
    std::vector<UserSession> get_user_sessions(const std::string& user_id);
    bool update_session_activity(const std::string& session_id);
    bool delete_session(const std::string& session_id);
    bool delete_user_sessions(const std::string& user_id);
    bool delete_expired_sessions();
    
    // Security operations
    bool increment_failed_login_attempts(const std::string& user_id);
    bool reset_failed_login_attempts(const std::string& user_id);
    bool is_user_locked(const std::string& user_id);
    
    // Search and pagination
    std::vector<User> search_users(const std::string& query, int limit = 20, int offset = 0);
    std::vector<User> get_users_by_status(UserStatus status, int limit = 100, int offset = 0);
    
    // Statistics
    int64_t count_total_users();
    int64_t count_active_users();
    int64_t count_verified_users();

private:
    std::shared_ptr<pqxx::connection_pool> connection_pool_;
    
    // Helper methods for result mapping
    User map_row_to_user(const pqxx::row& row);
    UserSession map_row_to_session(const pqxx::row& row);
    
    // Query builders
    std::string build_user_insert_query();
    std::string build_user_update_query();
    std::string build_session_insert_query();
    
    // Connection management
    pqxx::connection get_connection();
    void execute_transaction(std::function<void(pqxx::work&)> transaction_func);
    
    // Validation helpers
    bool validate_user_data(const User& user);
    bool validate_session_data(const UserSession& session);
};

} // namespace sonet::user
