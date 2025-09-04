/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "user_repository.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <iomanip>
#include <regex>

namespace sonet::user::repositories {

// PostgreSQLUserRepository implementation
PostgreSQLUserRepository::PostgreSQLUserRepository(std::shared_ptr<pqxx::connection> connection)
    : db_connection_(connection), 
      table_name_("users"),
      user_stats_table_("user_stats"),
      user_settings_table_("user_settings"),
      blocked_users_table_("blocked_users"),
      muted_users_table_("muted_users"),
      close_friends_table_("close_friends") {
    
    ensure_connection();
    create_database_schema();
    setup_prepared_statements();
}

void PostgreSQLUserRepository::ensure_connection() {
    if (!db_connection_ || !test_connection()) {
        reconnect_if_needed();
    }
}

void PostgreSQLUserRepository::reconnect_if_needed() {
    try {
        if (db_connection_) {
            db_connection_.reset();
        }
        // Connection will be re-established by the factory
        spdlog::info("Database connection reset for user repository");
    } catch (const std::exception& e) {
        spdlog::error("Failed to reconnect database: {}", e.what());
        throw;
    }
}

bool PostgreSQLUserRepository::test_connection() {
    try {
        if (!db_connection_) return false;
        pqxx::work txn(*db_connection_);
        txn.exec("SELECT 1");
        txn.commit();
        return true;
    } catch (const std::exception& e) {
        spdlog::warn("Database connection test failed: {}", e.what());
        return false;
    }
}

std::string PostgreSQLUserRepository::build_select_query(const std::vector<std::string>& fields) const {
    std::stringstream query;
    query << "SELECT ";
    
    if (fields.empty()) {
        query << "*";
    } else {
        for (size_t i = 0; i < fields.size(); ++i) {
            if (i > 0) query << ", ";
            query << fields[i];
        }
    }
    
    query << " FROM " << table_name_;
    return query.str();
}

std::string PostgreSQLUserRepository::build_insert_query(const User& user) const {
    return R"(
        INSERT INTO users (
            user_id, username, email, phone_number, password_hash, salt,
            display_name, first_name, last_name, bio, location, website,
            avatar_url, banner_url, timezone, language, status, account_type,
            privacy_level, is_verified, is_premium, is_developer,
            is_email_verified, is_phone_verified, discoverable_by_email,
            discoverable_by_phone, allow_direct_messages, allow_message_requests,
            show_activity_status, show_read_receipts, nsfw_content_enabled,
            autoplay_videos, high_quality_images, email_notifications,
            push_notifications, sms_notifications, followers_count,
            following_count, notes_count, likes_count, media_count,
            profile_views_count, created_at, updated_at, last_login_at,
            last_active_at, created_from_ip, last_login_ip, is_deleted,
            deleted_at, deletion_reason, suspended_until, suspension_reason,
            banned_reason, email_verification_token, phone_verification_code
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
            $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44,
            $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56
        )
    )";
}

std::string PostgreSQLUserRepository::build_update_query(const User& user) const {
    return R"(
        UPDATE users SET
            username = $2, email = $3, phone_number = $4, password_hash = $5,
            salt = $6, display_name = $7, first_name = $8, last_name = $9,
            bio = $10, location = $11, website = $12, avatar_url = $13,
            banner_url = $14, timezone = $15, language = $16, status = $17,
            account_type = $18, privacy_level = $19, is_verified = $20,
            is_premium = $21, is_developer = $22, is_email_verified = $23,
            is_phone_verified = $24, discoverable_by_email = $25,
            discoverable_by_phone = $26, allow_direct_messages = $27,
            allow_message_requests = $28, show_activity_status = $29,
            show_read_receipts = $30, nsfw_content_enabled = $31,
            autoplay_videos = $32, high_quality_images = $33,
            email_notifications = $34, push_notifications = $35,
            sms_notifications = $36, followers_count = $37,
            following_count = $38, notes_count = $39, likes_count = $40,
            media_count = $41, profile_views_count = $42, updated_at = $43,
            last_login_at = $44, last_active_at = $45, last_login_ip = $46,
            is_deleted = $47, deleted_at = $48, deletion_reason = $49,
            suspended_until = $50, suspension_reason = $51, banned_reason = $52,
            email_verification_token = $53, phone_verification_code = $54
        WHERE user_id = $1
    )";
}

User PostgreSQLUserRepository::map_row_to_user(const pqxx::row& row) const {
    User user;
    
    try {
        user.user_id = row["user_id"].as<std::string>();
        user.username = row["username"].as<std::string>();
        user.email = row["email"].as<std::string>();
        user.phone_number = row["phone_number"].as<std::string>("");
        user.password_hash = row["password_hash"].as<std::string>();
        user.salt = row["salt"].as<std::string>();
        user.display_name = row["display_name"].as<std::string>("");
        user.first_name = row["first_name"].as<std::string>("");
        user.last_name = row["last_name"].as<std::string>("");
        user.bio = row["bio"].as<std::string>("");
        user.location = row["location"].as<std::string>("");
        user.website = row["website"].as<std::string>("");
        user.avatar_url = row["avatar_url"].as<std::string>("");
        user.banner_url = row["banner_url"].as<std::string>("");
        user.timezone = row["timezone"].as<std::string>("UTC");
        user.language = row["language"].as<std::string>("en");
        
        user.status = static_cast<UserStatus>(row["status"].as<int>());
        user.account_type = static_cast<AccountType>(row["account_type"].as<int>());
        user.privacy_level = static_cast<PrivacyLevel>(row["privacy_level"].as<int>());
        
        user.is_verified = row["is_verified"].as<bool>(false);
        user.is_premium = row["is_premium"].as<bool>(false);
        user.is_developer = row["is_developer"].as<bool>(false);
        user.is_email_verified = row["is_email_verified"].as<bool>(false);
        user.is_phone_verified = row["is_phone_verified"].as<bool>(false);
        user.discoverable_by_email = row["discoverable_by_email"].as<bool>(true);
        user.discoverable_by_phone = row["discoverable_by_phone"].as<bool>(false);
        user.allow_direct_messages = row["allow_direct_messages"].as<bool>(true);
        user.allow_message_requests = row["allow_message_requests"].as<bool>(true);
        user.show_activity_status = row["show_activity_status"].as<bool>(true);
        user.show_read_receipts = row["show_read_receipts"].as<bool>(true);
        user.nsfw_content_enabled = row["nsfw_content_enabled"].as<bool>(false);
        user.autoplay_videos = row["autoplay_videos"].as<bool>(true);
        user.high_quality_images = row["high_quality_images"].as<bool>(true);
        user.email_notifications = row["email_notifications"].as<bool>(true);
        user.push_notifications = row["push_notifications"].as<bool>(true);
        user.sms_notifications = row["sms_notifications"].as<bool>(false);
        
        user.followers_count = row["followers_count"].as<int>(0);
        user.following_count = row["following_count"].as<int>(0);
        user.notes_count = row["notes_count"].as<int>(0);
        user.likes_count = row["likes_count"].as<int>(0);
        user.media_count = row["media_count"].as<int>(0);
        user.profile_views_count = row["profile_views_count"].as<int>(0);
        
        user.created_at = row["created_at"].as<std::time_t>();
        user.updated_at = row["updated_at"].as<std::time_t>();
        user.last_login_at = row["last_login_at"].as<std::time_t>(0);
        user.last_active_at = row["last_active_at"].as<std::time_t>(0);
        
        user.created_from_ip = row["created_from_ip"].as<std::string>("");
        user.last_login_ip = row["last_login_ip"].as<std::string>("");
        
        user.is_deleted = row["is_deleted"].as<bool>(false);
        user.deleted_at = row["deleted_at"].as<std::time_t>(0);
        user.deletion_reason = row["deletion_reason"].as<std::string>("");
        
        if (!row["suspended_until"].is_null()) {
            user.suspended_until = row["suspended_until"].as<std::time_t>();
        }
        user.suspension_reason = row["suspension_reason"].as<std::string>("");
        user.banned_reason = row["banned_reason"].as<std::string>("");
        
        user.email_verification_token = row["email_verification_token"].as<std::string>("");
        user.phone_verification_code = row["phone_verification_code"].as<std::string>("");
        
        // Load relationships
        user.blocked_users = get_blocked_users(user.user_id);
        user.muted_users = get_muted_users(user.user_id);
        user.close_friends = get_close_friends(user.user_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to map database row to user: {}", e.what());
        throw;
    }
    
    return user;
}

std::vector<User> PostgreSQLUserRepository::map_result_to_users(const pqxx::result& result) const {
    std::vector<User> users;
    users.reserve(result.size());
    
    for (const auto& row : result) {
        users.push_back(map_row_to_user(row));
    }
    
    return users;
}

bool PostgreSQLUserRepository::validate_user_data(const User& user) const {
    auto errors = user.get_validation_errors();
    if (!errors.empty()) {
        spdlog::error("User validation failed: {}", nlohmann::json(errors).dump());
        return false;
    }
    return true;
}

bool PostgreSQLUserRepository::check_unique_constraints(const User& user, bool is_update) const {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        std::string query = "SELECT user_id FROM users WHERE ";
        if (is_update) {
            query += "(username = $1 OR email = $2) AND user_id != $3";
            auto result = txn.exec_params(query, user.username, user.email, user.user_id);
            return result.empty();
        } else {
            query += "username = $1 OR email = $2";
            auto result = txn.exec_params(query, user.username, user.email);
            return result.empty();
        }
    } catch (const std::exception& e) {
        spdlog::error("Failed to check unique constraints: {}", e.what());
        return false;
    }
}

void PostgreSQLUserRepository::log_user_operation(const std::string& operation, const std::string& user_id) const {
    spdlog::info("User operation: {} for user_id: {}", operation, user_id);
}

bool PostgreSQLUserRepository::create(const User& user) {
    try {
        if (!validate_user_data(user)) {
            return false;
        }
        
        if (!check_unique_constraints(user, false)) {
            spdlog::error("User creation failed: username or email already exists");
            return false;
        }
        
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_insert_query(user);
        
        // Execute insert with all parameters
        txn.exec_params(query,
            user.user_id, user.username, user.email, user.phone_number,
            user.password_hash, user.salt, user.display_name, user.first_name,
            user.last_name, user.bio, user.location, user.website,
            user.avatar_url, user.banner_url, user.timezone, user.language,
            static_cast<int>(user.status), static_cast<int>(user.account_type),
            static_cast<int>(user.privacy_level), user.is_verified, user.is_premium,
            user.is_developer, user.is_email_verified, user.is_phone_verified,
            user.discoverable_by_email, user.discoverable_by_phone,
            user.allow_direct_messages, user.allow_message_requests,
            user.show_activity_status, user.show_read_receipts,
            user.nsfw_content_enabled, user.autoplay_videos,
            user.high_quality_images, user.email_notifications,
            user.push_notifications, user.sms_notifications,
            user.followers_count, user.following_count, user.notes_count,
            user.likes_count, user.media_count, user.profile_views_count,
            user.created_at, user.updated_at, user.last_login_at,
            user.last_active_at, user.created_from_ip, user.last_login_ip,
            user.is_deleted, user.deleted_at, user.deletion_reason,
            user.suspended_until.value_or(0), user.suspension_reason,
            user.banned_reason, user.email_verification_token,
            user.phone_verification_code
        );
        
        // Insert relationships
        update_user_relationships(txn, user);
        
        txn.commit();
        log_user_operation("CREATE", user.user_id);
        return true;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "create user");
        return false;
    }
}

std::optional<User> PostgreSQLUserRepository::get_by_id(const std::string& user_id) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_select_query() + " WHERE user_id = $1 AND is_deleted = FALSE";
        auto result = txn.exec_params(query, user_id);
        
        if (result.empty()) {
            return std::nullopt;
        }
        
        return map_row_to_user(result[0]);
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get user by id");
        return std::nullopt;
    }
}

std::optional<User> PostgreSQLUserRepository::get_by_username(const std::string& username) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_select_query() + " WHERE username = $1 AND is_deleted = FALSE";
        auto result = txn.exec_params(query, username);
        
        if (result.empty()) {
            return std::nullopt;
        }
        
        return map_row_to_user(result[0]);
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get user by username");
        return std::nullopt;
    }
}

std::optional<User> PostgreSQLUserRepository::get_by_email(const std::string& email) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_select_query() + " WHERE email = $1 AND is_deleted = FALSE";
        auto result = txn.exec_params(query, email);
        
        if (result.empty()) {
            return std::nullopt;
        }
        
        return map_row_to_user(result[0]);
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get user by email");
        return std::nullopt;
    }
}

bool PostgreSQLUserRepository::update(const User& user) {
    try {
        if (!validate_user_data(user)) {
            return false;
        }
        
        if (!check_unique_constraints(user, true)) {
            spdlog::error("User update failed: username or email already exists");
            return false;
        }
        
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_update_query(user);
        
        auto result = txn.exec_params(query,
            user.user_id, user.username, user.email, user.phone_number,
            user.password_hash, user.salt, user.display_name, user.first_name,
            user.last_name, user.bio, user.location, user.website,
            user.avatar_url, user.banner_url, user.timezone, user.language,
            static_cast<int>(user.status), static_cast<int>(user.account_type),
            static_cast<int>(user.privacy_level), user.is_verified, user.is_premium,
            user.is_developer, user.is_email_verified, user.is_phone_verified,
            user.discoverable_by_email, user.discoverable_by_phone,
            user.allow_direct_messages, user.allow_message_requests,
            user.show_activity_status, user.show_read_receipts,
            user.nsfw_content_enabled, user.autoplay_videos,
            user.high_quality_images, user.email_notifications,
            user.push_notifications, user.sms_notifications,
            user.followers_count, user.following_count, user.notes_count,
            user.likes_count, user.media_count, user.profile_views_count,
            user.updated_at, user.last_login_at, user.last_active_at,
            user.last_login_ip, user.is_deleted, user.deleted_at,
            user.deletion_reason, user.suspended_until.value_or(0),
            user.suspension_reason, user.banned_reason,
            user.email_verification_token, user.phone_verification_code
        );
        
        if (result.affected_rows() == 0) {
            return false;
        }
        
        // Update relationships
        update_user_relationships(txn, user);
        
        txn.commit();
        log_user_operation("UPDATE", user.user_id);
        return true;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "update user");
        return false;
    }
}

bool PostgreSQLUserRepository::delete_user(const std::string& user_id, const std::string& reason) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        handle_user_deletion(txn, user_id, reason);
        
        txn.commit();
        log_user_operation("DELETE", user_id);
        return true;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "delete user");
        return false;
    }
}

std::vector<User> PostgreSQLUserRepository::get_by_ids(const std::vector<std::string>& user_ids) {
    try {
        if (user_ids.empty()) {
            return {};
        }
        
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        std::stringstream query;
        query << build_select_query() << " WHERE user_id = ANY($1) AND is_deleted = FALSE";
        
        // Create array of user IDs
        std::stringstream array_str;
        array_str << "{";
        for (size_t i = 0; i < user_ids.size(); ++i) {
            if (i > 0) array_str << ",";
            array_str << "\"" << user_ids[i] << "\"";
        }
        array_str << "}";
        
        auto result = txn.exec_params(query.str(), array_str.str());
        return map_result_to_users(result);
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get users by ids");
        return {};
    }
}

// Additional complex methods implementation
void PostgreSQLUserRepository::update_user_relationships(pqxx::work& txn, const User& user) {
    // Clear existing relationships
    txn.exec_params("DELETE FROM blocked_users WHERE user_id = $1", user.user_id);
    txn.exec_params("DELETE FROM muted_users WHERE user_id = $1", user.user_id);
    txn.exec_params("DELETE FROM close_friends WHERE user_id = $1", user.user_id);
    
    // Insert new relationships
    for (const auto& blocked_id : user.blocked_users) {
        txn.exec_params("INSERT INTO blocked_users (user_id, blocked_user_id, created_at) VALUES ($1, $2, $3)",
                       user.user_id, blocked_id, std::time(nullptr));
    }
    
    for (const auto& muted_id : user.muted_users) {
        txn.exec_params("INSERT INTO muted_users (user_id, muted_user_id, created_at) VALUES ($1, $2, $3)",
                       user.user_id, muted_id, std::time(nullptr));
    }
    
    for (const auto& friend_id : user.close_friends) {
        txn.exec_params("INSERT INTO close_friends (user_id, friend_user_id, created_at) VALUES ($1, $2, $3)",
                       user.user_id, friend_id, std::time(nullptr));
    }
}

void PostgreSQLUserRepository::handle_user_deletion(pqxx::work& txn, const std::string& user_id, const std::string& reason) {
    auto now = std::time(nullptr);
    
    // Soft delete: mark as deleted but keep data
    txn.exec_params(
        "UPDATE users SET is_deleted = TRUE, deleted_at = $2, deletion_reason = $3, updated_at = $2 WHERE user_id = $1",
        user_id, now, reason
    );
    
    // Clean up relationships
    txn.exec_params("DELETE FROM blocked_users WHERE user_id = $1 OR blocked_user_id = $1", user_id);
    txn.exec_params("DELETE FROM muted_users WHERE user_id = $1 OR muted_user_id = $1", user_id);
    txn.exec_params("DELETE FROM close_friends WHERE user_id = $1 OR friend_user_id = $1", user_id);
}

bool PostgreSQLUserRepository::increment_stat(const std::string& user_id, const std::string& stat_name, int amount) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        update_user_statistics(txn, user_id, stat_name, amount);
        
        txn.commit();
        return true;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "increment user stat");
        return false;
    }
}

void PostgreSQLUserRepository::update_user_statistics(pqxx::work& txn, const std::string& user_id, 
                                                      const std::string& stat_name, int delta) {
    std::string column_name;
    if (stat_name == "followers") column_name = "followers_count";
    else if (stat_name == "following") column_name = "following_count";
    else if (stat_name == "notes") column_name = "notes_count";
    else if (stat_name == "likes") column_name = "likes_count";
    else if (stat_name == "media") column_name = "media_count";
    else if (stat_name == "profile_views") column_name = "profile_views_count";
    else return; // Invalid stat name
    
    std::stringstream query;
    query << "UPDATE users SET " << column_name << " = " << column_name << " + $2, updated_at = $3 WHERE user_id = $1";
    
    txn.exec_params(query.str(), user_id, delta, std::time(nullptr));
}

std::vector<std::string> PostgreSQLUserRepository::get_blocked_users(const std::string& user_id) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto result = txn.exec_params("SELECT blocked_user_id FROM blocked_users WHERE user_id = $1", user_id);
        
        std::vector<std::string> blocked_users;
        for (const auto& row : result) {
            blocked_users.push_back(row["blocked_user_id"].as<std::string>());
        }
        
        return blocked_users;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get blocked users");
        return {};
    }
}

std::vector<std::string> PostgreSQLUserRepository::get_muted_users(const std::string& user_id) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto result = txn.exec_params("SELECT muted_user_id FROM muted_users WHERE user_id = $1", user_id);
        
        std::vector<std::string> muted_users;
        for (const auto& row : result) {
            muted_users.push_back(row["muted_user_id"].as<std::string>());
        }
        
        return muted_users;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get muted users");
        return {};
    }
}

std::vector<std::string> PostgreSQLUserRepository::get_close_friends(const std::string& user_id) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto result = txn.exec_params("SELECT friend_user_id FROM close_friends WHERE user_id = $1", user_id);
        
        std::vector<std::string> close_friends;
        for (const auto& row : result) {
            close_friends.push_back(row["friend_user_id"].as<std::string>());
        }
        
        return close_friends;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get close friends");
        return {};
    }
}

void PostgreSQLUserRepository::setup_prepared_statements() {
    try {
        ensure_connection();
        
        // Prepare commonly used statements for better performance
        db_connection_->prepare("get_user_by_id", 
            build_select_query() + " WHERE user_id = $1 AND is_deleted = FALSE");
        
        db_connection_->prepare("get_user_by_username", 
            build_select_query() + " WHERE username = $1 AND is_deleted = FALSE");
        
        db_connection_->prepare("get_user_by_email", 
            build_select_query() + " WHERE email = $1 AND is_deleted = FALSE");
        
        spdlog::info("Prepared statements created for user repository");
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to setup prepared statements: {}", e.what());
    }
}

void PostgreSQLUserRepository::create_database_schema() {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        // Create users table
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(255) PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                phone_number VARCHAR(20),
                password_hash VARCHAR(255) NOT NULL,
                salt VARCHAR(255) NOT NULL,
                display_name VARCHAR(100),
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                bio TEXT,
                location VARCHAR(100),
                website VARCHAR(255),
                avatar_url VARCHAR(500),
                banner_url VARCHAR(500),
                timezone VARCHAR(50) DEFAULT 'UTC',
                language VARCHAR(10) DEFAULT 'en',
                status INTEGER DEFAULT 0,
                account_type INTEGER DEFAULT 0,
                privacy_level INTEGER DEFAULT 0,
                is_verified BOOLEAN DEFAULT FALSE,
                is_premium BOOLEAN DEFAULT FALSE,
                is_developer BOOLEAN DEFAULT FALSE,
                is_email_verified BOOLEAN DEFAULT FALSE,
                is_phone_verified BOOLEAN DEFAULT FALSE,
                discoverable_by_email BOOLEAN DEFAULT TRUE,
                discoverable_by_phone BOOLEAN DEFAULT FALSE,
                allow_direct_messages BOOLEAN DEFAULT TRUE,
                allow_message_requests BOOLEAN DEFAULT TRUE,
                show_activity_status BOOLEAN DEFAULT TRUE,
                show_read_receipts BOOLEAN DEFAULT TRUE,
                nsfw_content_enabled BOOLEAN DEFAULT FALSE,
                autoplay_videos BOOLEAN DEFAULT TRUE,
                high_quality_images BOOLEAN DEFAULT TRUE,
                email_notifications BOOLEAN DEFAULT TRUE,
                push_notifications BOOLEAN DEFAULT TRUE,
                sms_notifications BOOLEAN DEFAULT FALSE,
                followers_count INTEGER DEFAULT 0,
                following_count INTEGER DEFAULT 0,
                notes_count INTEGER DEFAULT 0,
                likes_count INTEGER DEFAULT 0,
                media_count INTEGER DEFAULT 0,
                profile_views_count INTEGER DEFAULT 0,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                last_login_at BIGINT DEFAULT 0,
                last_active_at BIGINT DEFAULT 0,
                created_from_ip VARCHAR(45),
                last_login_ip VARCHAR(45),
                is_deleted BOOLEAN DEFAULT FALSE,
                deleted_at BIGINT DEFAULT 0,
                deletion_reason TEXT,
                suspended_until BIGINT,
                suspension_reason TEXT,
                banned_reason TEXT,
                email_verification_token VARCHAR(255),
                phone_verification_code VARCHAR(10)
            )
        )");
        
        // Create relationship tables
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS blocked_users (
                user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE CASCADE,
                blocked_user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE CASCADE,
                created_at BIGINT NOT NULL,
                PRIMARY KEY (user_id, blocked_user_id)
            )
        )");
        
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS muted_users (
                user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE CASCADE,
                muted_user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE CASCADE,
                created_at BIGINT NOT NULL,
                PRIMARY KEY (user_id, muted_user_id)
            )
        )");
        
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS close_friends (
                user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE CASCADE,
                friend_user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE CASCADE,
                created_at BIGINT NOT NULL,
                PRIMARY KEY (user_id, friend_user_id)
            )
        )");
        
        // Create indexes for performance
        txn.exec("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active_at)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_users_is_deleted ON users(is_deleted)");
        
        txn.commit();
        spdlog::info("Database schema created successfully");
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to create database schema: {}", e.what());
        throw;
    }
}

void PostgreSQLUserRepository::handle_database_error(const std::exception& e, const std::string& operation) const {
    spdlog::error("Database error during {}: {}", operation, e.what());
}

// Stub implementations for remaining interface methods
bool PostgreSQLUserRepository::update_multiple(const std::vector<User>& users) {
    // Implementation would batch update users
    return false;
}

bool PostgreSQLUserRepository::delete_multiple(const std::vector<std::string>& user_ids, const std::string& reason) {
    // Implementation would batch delete users
    return false;
}

SearchResult<User> PostgreSQLUserRepository::search(const UserSearchCriteria& criteria) {
    // Implementation would build dynamic search query
    return SearchResult<User>{};
}

std::vector<User> PostgreSQLUserRepository::get_recently_active(int limit, int hours_back) {
    // Implementation would query recent activity
    return {};
}

std::vector<User> PostgreSQLUserRepository::get_new_users(int limit, int days_back) {
    // Implementation would query new registrations
    return {};
}

std::vector<User> PostgreSQLUserRepository::get_users_by_status(UserStatus status, int limit, int offset) {
    // Implementation would query by status
    return {};
}

bool PostgreSQLUserRepository::decrement_stat(const std::string& user_id, const std::string& stat_name, int amount) {
    return increment_stat(user_id, stat_name, -amount);
}

UserStats PostgreSQLUserRepository::get_user_stats(const std::string& user_id) {
    // Implementation would return user statistics
    return UserStats{};
}

bool PostgreSQLUserRepository::update_user_stats(const std::string& user_id, const UserStats& stats) {
    // Implementation would update user statistics
    return false;
}

bool PostgreSQLUserRepository::verify_email(const std::string& user_id, const std::string& verification_token) {
    // Implementation would verify email with token
    return false;
}

bool PostgreSQLUserRepository::verify_phone(const std::string& user_id, const std::string& verification_code) {
    // Implementation would verify phone with code
    return false;
}

bool PostgreSQLUserRepository::update_password(const std::string& user_id, const std::string& password_hash, const std::string& salt) {
    // Implementation would update password
    return false;
}

bool PostgreSQLUserRepository::reset_password(const std::string& user_id, const std::string& new_password_hash, const std::string& salt, const std::string& reset_token) {
    // Implementation would reset password with token
    return false;
}

bool PostgreSQLUserRepository::block_user(const std::string& user_id, const std::string& blocked_user_id) {
    // Implementation would add to blocked users
    return false;
}

bool PostgreSQLUserRepository::unblock_user(const std::string& user_id, const std::string& blocked_user_id) {
    // Implementation would remove from blocked users
    return false;
}

bool PostgreSQLUserRepository::mute_user(const std::string& user_id, const std::string& muted_user_id) {
    // Implementation would add to muted users
    return false;
}

bool PostgreSQLUserRepository::unmute_user(const std::string& user_id, const std::string& muted_user_id) {
    // Implementation would remove from muted users
    return false;
}

bool PostgreSQLUserRepository::add_close_friend(const std::string& user_id, const std::string& friend_user_id) {
    // Implementation would add to close friends
    return false;
}

bool PostgreSQLUserRepository::remove_close_friend(const std::string& user_id, const std::string& friend_user_id) {
    // Implementation would remove from close friends
    return false;
}

bool PostgreSQLUserRepository::suspend_user(const std::string& user_id, const std::string& reason, std::time_t until_timestamp) {
    // Implementation would suspend user
    return false;
}

bool PostgreSQLUserRepository::unsuspend_user(const std::string& user_id) {
    // Implementation would unsuspend user
    return false;
}

bool PostgreSQLUserRepository::ban_user(const std::string& user_id, const std::string& reason) {
    // Implementation would ban user
    return false;
}

bool PostgreSQLUserRepository::unban_user(const std::string& user_id) {
    // Implementation would unban user
    return false;
}

bool PostgreSQLUserRepository::deactivate_user(const std::string& user_id) {
    // Implementation would deactivate user
    return false;
}

bool PostgreSQLUserRepository::reactivate_user(const std::string& user_id) {
    // Implementation would reactivate user
    return false;
}

std::vector<User> PostgreSQLUserRepository::find_users_by_email_domain(const std::string& domain, int limit) {
    // Implementation would find users by email domain
    return {};
}

std::vector<User> PostgreSQLUserRepository::find_users_by_location(const std::string& location, int limit) {
    // Implementation would find users by location
    return {};
}

std::vector<User> PostgreSQLUserRepository::get_verified_users(int limit, int offset) {
    // Implementation would get verified users
    return {};
}

std::vector<User> PostgreSQLUserRepository::get_premium_users(int limit, int offset) {
    // Implementation would get premium users
    return {};
}

int PostgreSQLUserRepository::count_total_users() {
    // Implementation would count total users
    return 0;
}

int PostgreSQLUserRepository::count_active_users(int days_back) {
    // Implementation would count active users
    return 0;
}

int PostgreSQLUserRepository::count_users_by_status(UserStatus status) {
    // Implementation would count users by status
    return 0;
}

std::map<std::string, int> PostgreSQLUserRepository::get_user_registration_stats(int days_back) {
    // Implementation would get registration statistics
    return {};
}

std::map<std::string, int> PostgreSQLUserRepository::get_user_activity_stats(int days_back) {
    // Implementation would get activity statistics
    return {};
}

bool PostgreSQLUserRepository::cleanup_deleted_users(int days_old) {
    // Implementation would cleanup old deleted users
    return false;
}

bool PostgreSQLUserRepository::vacuum_user_data() {
    // Implementation would vacuum database
    return false;
}

bool PostgreSQLUserRepository::reindex_user_tables() {
    // Implementation would reindex tables
    return false;
}

DatabaseHealthStatus PostgreSQLUserRepository::check_database_health() {
    // Implementation would check database health
    return DatabaseHealthStatus{};
}

// Session Repository implementation stubs
NotegreSQLSessionRepository::NotegreSQLSessionRepository(std::shared_ptr<pqxx::connection> connection)
    : db_connection_(connection),
      sessions_table_("sessions"),
      session_devices_table_("session_devices"),
      session_locations_table_("session_locations"),
      session_activities_table_("session_activities") {
    ensure_connection();
}

void NotegreSQLSessionRepository::ensure_connection() {
    if (!db_connection_) {
        throw std::runtime_error("Database connection is null");
    }
}

Session NotegreSQLSessionRepository::map_row_to_session(const pqxx::row& row) const {
    // Implementation would map database row to session
    return Session{};
}

std::vector<Session> NotegreSQLSessionRepository::map_result_to_sessions(const pqxx::result& result) const {
    // Implementation would map result set to sessions
    return {};
}

void NotegreSQLSessionRepository::update_session_activity(pqxx::work& txn, const std::string& session_id) {
    // Implementation would update session activity
}

void NotegreSQLSessionRepository::cleanup_expired_sessions(pqxx::work& txn) {
    // Implementation would cleanup expired sessions
}

// All session repository method stubs
bool NotegreSQLSessionRepository::create_session(const Session& session) { return false; }
std::optional<Session> NotegreSQLSessionRepository::get_session(const std::string& session_id) { return std::nullopt; }
std::optional<Session> NotegreSQLSessionRepository::get_by_access_token(const std::string& access_token) { return std::nullopt; }
bool NotegreSQLSessionRepository::update_session(const Session& session) { return false; }
bool NotegreSQLSessionRepository::delete_session(const std::string& session_id) { return false; }
bool NotegreSQLSessionRepository::expire_session(const std::string& session_id) { return false; }
bool NotegreSQLSessionRepository::revoke_session(const std::string& session_id, const std::string& reason) { return false; }
std::vector<Session> NotegreSQLSessionRepository::get_user_sessions(const std::string& user_id, bool active_only) { return {}; }
bool NotegreSQLSessionRepository::revoke_all_user_sessions(const std::string& user_id, const std::string& reason, const std::string& except_session_id) { return false; }
bool NotegreSQLSessionRepository::revoke_user_sessions_except(const std::string& user_id, const std::vector<std::string>& keep_session_ids) { return false; }
int NotegreSQLSessionRepository::count_active_sessions(const std::string& user_id) { return 0; }
int NotegreSQLSessionRepository::count_sessions_by_device_type(DeviceType device_type) { return 0; }
std::vector<Session> NotegreSQLSessionRepository::get_suspicious_sessions() { return {}; }
std::vector<Session> NotegreSQLSessionRepository::get_expired_sessions(int hours_old) { return {}; }
bool NotegreSQLSessionRepository::cleanup_expired_sessions(int hours_old) { return false; }
bool NotegreSQLSessionRepository::cleanup_revoked_sessions(int days_old) { return false; }
SessionCleanupResult NotegreSQLSessionRepository::perform_maintenance() { return SessionCleanupResult{}; }

// RepositoryFactory implementation
std::unique_ptr<PostgreSQLUserRepository> RepositoryFactory::create_user_repository(const std::string& connection_string) {
    auto connection = create_database_connection(connection_string);
    return std::make_unique<PostgreSQLUserRepository>(connection);
}

std::unique_ptr<NotegreSQLSessionRepository> RepositoryFactory::create_session_repository(const std::string& connection_string) {
    auto connection = create_database_connection(connection_string);
    return std::make_unique<NotegreSQLSessionRepository>(connection);
}

std::shared_ptr<pqxx::connection> RepositoryFactory::create_database_connection(const std::string& connection_string) {
    try {
        auto connection = std::make_shared<pqxx::connection>(connection_string);
        spdlog::info("Database connection established successfully");
        return connection;
    } catch (const std::exception& e) {
        spdlog::error("Failed to create database connection: {}", e.what());
        throw;
    }
}

void RepositoryFactory::initialize_connection_pool(const std::string& connection_string, int pool_size) {
    // Implementation would initialize connection pool
}

std::shared_ptr<pqxx::connection> RepositoryFactory::get_pooled_connection() {
    // Implementation would return pooled connection
    return nullptr;
}

void RepositoryFactory::return_connection(std::shared_ptr<pqxx::connection> connection) {
    // Implementation would return connection to pool
}

void RepositoryFactory::shutdown_connection_pool() {
    // Implementation would shutdown connection pool
}

} // namespace sonet::user::repositories