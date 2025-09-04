/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "user_repository.h"
#include <spdlog/spdlog.h>
#include <uuid/uuid.h>
#include <chrono>
#include <stdexcept>

namespace sonet::user {

UserRepository::UserRepository(std::shared_ptr<pqxx::connection_pool> pool)
    : connection_pool_(std::move(pool)) {
    
    if (!connection_pool_) {
        throw std::invalid_argument("Connection pool cannot be null");
    }
    
    spdlog::info("User repository initialized with connection pool");
}

std::optional<User> UserRepository::create_user(const User& user) {
    if (!validate_user_data(user)) {
        spdlog::error("Invalid user data provided for creation");
        return std::nullopt;
    }
    
    try {
        User new_user = user;
        
        // Generate UUID if not provided
        if (new_user.user_id.empty()) {
            uuid_t uuid;
            uuid_generate(uuid);
            char uuid_str[37];
            uuid_unparse(uuid, uuid_str);
            new_user.user_id = std::string(uuid_str);
        }
        
        // Set creation timestamp
        new_user.created_at = std::chrono::system_clock::now();
        new_user.updated_at = new_user.created_at;
        
        std::optional<User> result;
        
        execute_transaction([&](pqxx::work& txn) {
            std::string query = R"(
                INSERT INTO users (
                    user_id, username, email, password_hash, full_name,
                    bio, avatar_url, banner_url, location, website,
                    is_verified, is_private, status, failed_login_attempts,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                ) RETURNING *
            )";
            
            auto res = txn.exec_params(query,
                new_user.user_id,
                new_user.username,
                new_user.email,
                new_user.password_hash,
                new_user.full_name,
                new_user.bio,
                new_user.avatar_url,
                new_user.banner_url,
                new_user.location,
                new_user.website,
                new_user.is_verified,
                new_user.is_private,
                static_cast<int>(new_user.status),
                new_user.failed_login_attempts,
                std::chrono::duration_cast<std::chrono::seconds>(
                    new_user.created_at.time_since_epoch()).count(),
                std::chrono::duration_cast<std::chrono::seconds>(
                    new_user.updated_at.time_since_epoch()).count()
            );
            
            if (!res.empty()) {
                result = map_row_to_user(res[0]);
            }
        });
        
        if (result) {
            spdlog::info("User created successfully: {}", result->user_id);
        }
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to create user: {}", e.what());
        return std::nullopt;
    }
}

std::optional<User> UserRepository::get_user_by_id(const std::string& user_id) {
    try {
        std::optional<User> result;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "SELECT * FROM users WHERE user_id = $1",
                user_id
            );
            
            if (!res.empty()) {
                result = map_row_to_user(res[0]);
            }
        });
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to get user by ID {}: {}", user_id, e.what());
        return std::nullopt;
    }
}

std::optional<User> UserRepository::get_user_by_username(const std::string& username) {
    try {
        std::optional<User> result;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "SELECT * FROM users WHERE LOWER(username) = LOWER($1)",
                username
            );
            
            if (!res.empty()) {
                result = map_row_to_user(res[0]);
            }
        });
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to get user by username {}: {}", username, e.what());
        return std::nullopt;
    }
}

std::optional<User> UserRepository::get_user_by_email(const std::string& email) {
    try {
        std::optional<User> result;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "SELECT * FROM users WHERE LOWER(email) = LOWER($1)",
                email
            );
            
            if (!res.empty()) {
                result = map_row_to_user(res[0]);
            }
        });
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to get user by email {}: {}", email, e.what());
        return std::nullopt;
    }
}

std::optional<User> UserRepository::update_user(const User& user) {
    if (!validate_user_data(user)) {
        spdlog::error("Invalid user data provided for update");
        return std::nullopt;
    }
    
    try {
        User updated_user = user;
        updated_user.updated_at = std::chrono::system_clock::now();
        
        std::optional<User> result;
        
        execute_transaction([&](pqxx::work& txn) {
            std::string query = R"(
                UPDATE users SET 
                    username = $2, email = $3, full_name = $4, bio = $5,
                    avatar_url = $6, banner_url = $7, location = $8, website = $9,
                    is_verified = $10, is_private = $11, status = $12,
                    updated_at = $13
                WHERE user_id = $1
                RETURNING *
            )";
            
            auto res = txn.exec_params(query,
                updated_user.user_id,
                updated_user.username,
                updated_user.email,
                updated_user.full_name,
                updated_user.bio,
                updated_user.avatar_url,
                updated_user.banner_url,
                updated_user.location,
                updated_user.website,
                updated_user.is_verified,
                updated_user.is_private,
                static_cast<int>(updated_user.status),
                std::chrono::duration_cast<std::chrono::seconds>(
                    updated_user.updated_at.time_since_epoch()).count()
            );
            
            if (!res.empty()) {
                result = map_row_to_user(res[0]);
            }
        });
        
        if (result) {
            spdlog::info("User updated successfully: {}", result->user_id);
        }
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to update user {}: {}", user.user_id, e.what());
        return std::nullopt;
    }
}

bool UserRepository::delete_user(const std::string& user_id) {
    try {
        bool success = false;
        
        execute_transaction([&](pqxx::work& txn) {
            // First delete all user sessions
            txn.exec_params("DELETE FROM user_sessions WHERE user_id = $1", user_id);
            
            // Then delete the user
            auto res = txn.exec_params("DELETE FROM users WHERE user_id = $1", user_id);
            success = res.affected_rows() > 0;
        });
        
        if (success) {
            spdlog::info("User deleted successfully: {}", user_id);
        }
        
        return success;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to delete user {}: {}", user_id, e.what());
        return false;
    }
}

bool UserRepository::verify_user_email(const std::string& user_id) {
    try {
        bool success = false;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "UPDATE users SET is_verified = true, updated_at = $2 WHERE user_id = $1",
                user_id,
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()
            );
            success = res.affected_rows() > 0;
        });
        
        if (success) {
            spdlog::info("User email verified: {}", user_id);
        }
        
        return success;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to verify user email {}: {}", user_id, e.what());
        return false;
    }
}

bool UserRepository::update_user_status(const std::string& user_id, UserStatus status) {
    try {
        bool success = false;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "UPDATE users SET status = $2, updated_at = $3 WHERE user_id = $1",
                user_id,
                static_cast<int>(status),
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()
            );
            success = res.affected_rows() > 0;
        });
        
        if (success) {
            spdlog::info("User status updated: {} -> {}", user_id, static_cast<int>(status));
        }
        
        return success;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to update user status {}: {}", user_id, e.what());
        return false;
    }
}

bool UserRepository::update_last_login(const std::string& user_id) {
    try {
        bool success = false;
        
        execute_transaction([&](pqxx::work& txn) {
            auto now = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            auto res = txn.exec_params(
                "UPDATE users SET last_login_at = $2, updated_at = $2 WHERE user_id = $1",
                user_id, now
            );
            success = res.affected_rows() > 0;
        });
        
        return success;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to update last login for user {}: {}", user_id, e.what());
        return false;
    }
}

bool UserRepository::update_password_hash(const std::string& user_id, const std::string& password_hash) {
    try {
        bool success = false;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "UPDATE users SET password_hash = $2, updated_at = $3 WHERE user_id = $1",
                user_id,
                password_hash,
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()
            );
            success = res.affected_rows() > 0;
        });
        
        if (success) {
            spdlog::info("Password hash updated for user: {}", user_id);
        }
        
        return success;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to update password hash for user {}: {}", user_id, e.what());
        return false;
    }
}

std::optional<std::string> UserRepository::get_password_hash(const std::string& user_id) {
    try {
        std::optional<std::string> result;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "SELECT password_hash FROM users WHERE user_id = $1",
                user_id
            );
            
            if (!res.empty() && !res[0]["password_hash"].is_null()) {
                result = res[0]["password_hash"].as<std::string>();
            }
        });
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to get password hash for user {}: {}", user_id, e.what());
        return std::nullopt;
    }
}

// Session operations implementation continues...
std::optional<UserSession> UserRepository::create_session(const UserSession& session) {
    if (!validate_session_data(session)) {
        spdlog::error("Invalid session data provided for creation");
        return std::nullopt;
    }
    
    try {
        UserSession new_session = session;
        
        // Generate UUID if not provided
        if (new_session.session_id.empty()) {
            uuid_t uuid;
            uuid_generate(uuid);
            char uuid_str[37];
            uuid_unparse(uuid, uuid_str);
            new_session.session_id = std::string(uuid_str);
        }
        
        new_session.created_at = std::chrono::system_clock::now();
        new_session.last_activity = new_session.created_at;
        
        std::optional<UserSession> result;
        
        execute_transaction([&](pqxx::work& txn) {
            std::string query = R"(
                INSERT INTO user_sessions (
                    session_id, user_id, device_id, device_type, ip_address,
                    user_agent, type, created_at, last_activity, expires_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                ) RETURNING *
            )";
            
            auto res = txn.exec_params(query,
                new_session.session_id,
                new_session.user_id,
                new_session.device_id,
                static_cast<int>(new_session.device_type),
                new_session.ip_address,
                new_session.user_agent,
                static_cast<int>(new_session.type),
                std::chrono::duration_cast<std::chrono::seconds>(
                    new_session.created_at.time_since_epoch()).count(),
                std::chrono::duration_cast<std::chrono::seconds>(
                    new_session.last_activity.time_since_epoch()).count(),
                std::chrono::duration_cast<std::chrono::seconds>(
                    new_session.expires_at.time_since_epoch()).count()
            );
            
            if (!res.empty()) {
                result = map_row_to_session(res[0]);
            }
        });
        
        if (result) {
            spdlog::info("Session created successfully: {}", result->session_id);
        }
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to create session: {}", e.what());
        return std::nullopt;
    }
}

// Helper methods implementation

User UserRepository::map_row_to_user(const pqxx::row& row) {
    User user;
    user.user_id = row["user_id"].as<std::string>();
    user.username = row["username"].as<std::string>();
    user.email = row["email"].as<std::string>();
    user.password_hash = row["password_hash"].as<std::string>();
    user.full_name = row["full_name"].as<std::string>("");
    user.bio = row["bio"].as<std::string>("");
    user.avatar_url = row["avatar_url"].as<std::string>("");
    user.banner_url = row["banner_url"].as<std::string>("");
    user.location = row["location"].as<std::string>("");
    user.website = row["website"].as<std::string>("");
    user.is_verified = row["is_verified"].as<bool>();
    user.is_private = row["is_private"].as<bool>();
    user.status = static_cast<UserStatus>(row["status"].as<int>());
    user.failed_login_attempts = row["failed_login_attempts"].as<int>(0);
    
    // Convert timestamps
    auto created_timestamp = row["created_at"].as<int64_t>();
    auto updated_timestamp = row["updated_at"].as<int64_t>();
    
    user.created_at = std::chrono::system_clock::from_time_t(created_timestamp);
    user.updated_at = std::chrono::system_clock::from_time_t(updated_timestamp);
    
    if (!row["last_login_at"].is_null()) {
        auto last_login_timestamp = row["last_login_at"].as<int64_t>();
        user.last_login_at = std::chrono::system_clock::from_time_t(last_login_timestamp);
    }
    
    return user;
}

UserSession UserRepository::map_row_to_session(const pqxx::row& row) {
    UserSession session;
    session.session_id = row["session_id"].as<std::string>();
    session.user_id = row["user_id"].as<std::string>();
    session.device_id = row["device_id"].as<std::string>();
    session.device_type = static_cast<DeviceType>(row["device_type"].as<int>());
    session.ip_address = row["ip_address"].as<std::string>();
    session.user_agent = row["user_agent"].as<std::string>();
    session.type = static_cast<SessionType>(row["type"].as<int>());
    
    // Convert timestamps
    auto created_timestamp = row["created_at"].as<int64_t>();
    auto last_activity_timestamp = row["last_activity"].as<int64_t>();
    auto expires_timestamp = row["expires_at"].as<int64_t>();
    
    session.created_at = std::chrono::system_clock::from_time_t(created_timestamp);
    session.last_activity = std::chrono::system_clock::from_time_t(last_activity_timestamp);
    session.expires_at = std::chrono::system_clock::from_time_t(expires_timestamp);
    
    return session;
}

void UserRepository::execute_transaction(std::function<void(pqxx::work&)> transaction_func) {
    auto conn = connection_pool_->acquire();
    pqxx::work txn{*conn, "user_repo_txn"};
    
    try {
        transaction_func(txn);
        txn.commit();
    } catch (const std::exception& e) {
        txn.abort();
        throw;
    }
}

bool UserRepository::validate_user_data(const User& user) {
    // Basic validation - extend as needed
    return !user.username.empty() && 
           !user.email.empty() && 
           !user.password_hash.empty();
}

bool UserRepository::validate_session_data(const UserSession& session) {
    // Basic validation - extend as needed
    return !session.user_id.empty() && 
           !session.device_id.empty() && 
           !session.ip_address.empty();
}

bool UserRepository::increment_failed_login_attempts(const std::string& user_id) {
    try {
        bool success = false;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "UPDATE users SET failed_login_attempts = failed_login_attempts + 1, updated_at = $2 WHERE user_id = $1",
                user_id,
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()
            );
            success = res.affected_rows() > 0;
        });
        
        return success;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to increment failed login attempts for user {}: {}", user_id, e.what());
        return false;
    }
}

bool UserRepository::reset_failed_login_attempts(const std::string& user_id) {
    try {
        bool success = false;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec_params(
                "UPDATE users SET failed_login_attempts = 0, updated_at = $2 WHERE user_id = $1",
                user_id,
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()
            );
            success = res.affected_rows() > 0;
        });
        
        return success;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to reset failed login attempts for user {}: {}", user_id, e.what());
        return false;
    }
}

int64_t UserRepository::count_total_users() {
    try {
        int64_t count = 0;
        
        execute_transaction([&](pqxx::work& txn) {
            auto res = txn.exec("SELECT COUNT(*) FROM users");
            if (!res.empty()) {
                count = res[0][0].as<int64_t>();
            }
        });
        
        return count;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to count total users: {}", e.what());
        return 0;
    }
}

} // namespace sonet::user
