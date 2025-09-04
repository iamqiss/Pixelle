/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "user_repository_libpq.h"
#include <spdlog/spdlog.h>
#include <sstream>
#include <algorithm>
#include <cstring>
#include <random>
#include <iomanip>

namespace sonet::user {

UserRepositoryLibpq::UserRepositoryLibpq(database::ConnectionPool* pool)
    : database::BaseRepository(pool) {
    
    if (!pool) {
        throw std::invalid_argument("Connection pool cannot be null");
    }
    
    spdlog::info("UserRepositoryLibpq initialized with connection pool");
    
    // Initialize prepared statements
    if (!initialize_prepared_statements()) {
        spdlog::warn("Failed to initialize some prepared statements");
    }
}

std::optional<models::User> UserRepositoryLibpq::create_user(const models::User& user) {
    if (user.username.empty() || user.email.empty()) {
        spdlog::error("Cannot create user: username or email is empty");
        return std::nullopt;
    }
    
    // Check if username or email already exists
    if (is_username_taken(user.username)) {
        spdlog::error("Username already taken: {}", user.username);
        return std::nullopt;
    }
    
    if (is_email_taken(user.email)) {
        spdlog::error("Email already taken: {}", user.email);
        return std::nullopt;
    }
    
    models::User new_user = user;
    
    // Generate UUID if not provided
    if (new_user.user_id.empty()) {
        new_user.user_id = generate_uuid();
    }
    
    // Set creation timestamp
    auto now = std::chrono::system_clock::now();
    new_user.created_at = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    new_user.updated_at = new_user.created_at;
    
    std::string query = R"(
        INSERT INTO user_schema.users (
            user_id, username, email, password_hash, salt, display_name, 
            first_name, last_name, bio, location, website, avatar_url, banner_url,
            timezone, language, status, account_type, privacy_level, is_verified,
            is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
            allow_direct_messages, allow_message_requests, show_activity_status,
            show_read_receipts, nsfw_content_enabled, autoplay_videos,
            high_quality_images, email_notifications, push_notifications,
            sms_notifications, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
            $29, $30, $31, $32, $33, $34
        ) RETURNING user_id
    )";
    
    std::vector<std::string> params = {
        new_user.user_id,
        new_user.username,
        new_user.email,
        new_user.password_hash,
        new_user.salt,
        new_user.display_name,
        new_user.first_name,
        new_user.last_name,
        new_user.bio,
        new_user.location,
        new_user.website,
        new_user.avatar_url,
        new_user.banner_url,
        new_user.timezone,
        new_user.language,
        std::to_string(static_cast<int>(new_user.status)),
        std::to_string(static_cast<int>(new_user.account_type)),
        std::to_string(static_cast<int>(new_user.privacy_level)),
        new_user.is_verified ? "true" : "false",
        new_user.is_premium ? "true" : "false",
        new_user.is_developer ? "true" : "false",
        new_user.discoverable_by_email ? "true" : "false",
        new_user.discoverable_by_phone ? "true" : "false",
        new_user.allow_direct_messages ? "true" : "false",
        new_user.allow_message_requests ? "true" : "false",
        new_user.show_activity_status ? "true" : "false",
        new_user.show_read_receipts ? "true" : "false",
        new_user.nsfw_content_enabled ? "true" : "false",
        new_user.autoplay_videos ? "true" : "false",
        new_user.high_quality_images ? "true" : "false",
        new_user.email_notifications ? "true" : "false",
        new_user.push_notifications ? "true" : "false",
        new_user.sms_notifications ? "true" : "false",
        timestamp_to_db_string(now),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_prepared("create_user", params);
    if (result) {
        spdlog::info("User created successfully: {}", new_user.user_id);
        return new_user;
    }
    
    spdlog::error("Failed to create user: {}", new_user.username);
    return std::nullopt;
}

std::optional<models::User> UserRepositoryLibpq::get_user_by_id(const std::string& user_id) {
    if (user_id.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT user_id, username, email, password_hash, salt, display_name,
               first_name, last_name, bio, location, website, avatar_url, banner_url,
               timezone, language, status, account_type, privacy_level, is_verified,
               is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
               allow_direct_messages, allow_message_requests, show_activity_status,
               show_read_receipts, nsfw_content_enabled, autoplay_videos,
               high_quality_images, email_notifications, push_notifications,
               sms_notifications, created_at, updated_at
        FROM user_schema.users 
        WHERE user_id = $1 AND is_deleted = false
    )";
    
    std::vector<std::string> params = {user_id};
    auto result = execute_prepared("get_user_by_id", params);
    
    if (result && PQntuples(result.get()) > 0) {
        return map_result_to_user(result.get(), 0);
    }
    
    return std::nullopt;
}

std::optional<models::User> UserRepositoryLibpq::get_user_by_email(const std::string& email) {
    if (email.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT user_id, username, email, password_hash, salt, display_name,
               first_name, last_name, bio, location, website, avatar_url, banner_url,
               timezone, language, status, account_type, privacy_level, is_verified,
               is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
               allow_direct_messages, allow_message_requests, show_activity_status,
               show_read_receipts, nsfw_content_enabled, autoplay_videos,
               high_quality_images, email_notifications, push_notifications,
               sms_notifications, created_at, updated_at
        FROM user_schema.users 
        WHERE email = $1 AND is_deleted = false
    )";
    
    std::vector<std::string> params = {email};
    auto result = execute_prepared("get_user_by_email", params);
    
    if (result && PQntuples(result.get()) > 0) {
        return map_result_to_user(result.get(), 0);
    }
    
    return std::nullopt;
}

std::optional<models::User> UserRepositoryLibpq::get_user_by_username(const std::string& username) {
    if (username.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT user_id, username, email, password_hash, salt, display_name,
               first_name, last_name, bio, location, website, avatar_url, banner_url,
               timezone, language, status, account_type, privacy_level, is_verified,
               is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
               allow_direct_messages, allow_message_requests, show_activity_status,
               show_read_receipts, nsfw_content_enabled, autoplay_videos,
               high_quality_images, email_notifications, push_notifications,
               sms_notifications, created_at, updated_at
        FROM user_schema.users 
        WHERE username = $1 AND is_deleted = false
    )";
    
    std::vector<std::string> params = {username};
    auto result = execute_prepared("get_user_by_username", params);
    
    if (result && PQntuples(result.get()) > 0) {
        return map_result_to_user(result.get(), 0);
    }
    
    return std::nullopt;
}

bool UserRepositoryLibpq::update_user(const models::User& user) {
    if (user.user_id.empty()) {
        spdlog::error("Cannot update user: user_id is empty");
        return false;
    }
    
    std::string query = R"(
        UPDATE user_schema.users SET
            username = $2, email = $3, display_name = $4, first_name = $5,
            last_name = $6, bio = $7, location = $8, website = $9, avatar_url = $10,
            banner_url = $11, timezone = $12, language = $13, status = $14,
            account_type = $15, privacy_level = $16, is_verified = $17,
            is_premium = $18, is_developer = $19, discoverable_by_email = $20,
            discoverable_by_phone = $21, allow_direct_messages = $22,
            allow_message_requests = $23, show_activity_status = $24,
            show_read_receipts = $25, nsfw_content_enabled = $26,
            autoplay_videos = $27, high_quality_images = $28,
            email_notifications = $29, push_notifications = $30,
            sms_notifications = $31, updated_at = $32
        WHERE user_id = $1 AND is_deleted = false
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        user.user_id,
        user.username,
        user.email,
        user.display_name,
        user.first_name,
        user.last_name,
        user.bio,
        user.location,
        user.website,
        user.avatar_url,
        user.banner_url,
        user.timezone,
        user.language,
        std::to_string(static_cast<int>(user.status)),
        std::to_string(static_cast<int>(user.account_type)),
        std::to_string(static_cast<int>(user.privacy_level)),
        user.is_verified ? "true" : "false",
        user.is_premium ? "true" : "false",
        user.is_developer ? "true" : "false",
        user.discoverable_by_email ? "true" : "false",
        user.discoverable_by_phone ? "true" : "false",
        user.allow_direct_messages ? "true" : "false",
        user.allow_message_requests ? "true" : "false",
        user.show_activity_status ? "true" : "false",
        user.show_read_receipts ? "true" : "false",
        user.nsfw_content_enabled ? "true" : "false",
        user.autoplay_videos ? "true" : "false",
        user.high_quality_images ? "true" : "false",
        user.email_notifications ? "true" : "false",
        user.push_notifications ? "true" : "false",
        user.sms_notifications ? "true" : "false",
        timestamp_to_db_string(now)
    };
    
    auto result = execute_prepared("update_user", params);
    if (result) {
        spdlog::info("User updated successfully: {}", user.user_id);
        return true;
    }
    
    spdlog::error("Failed to update user: {}", user.user_id);
    return false;
}

bool UserRepositoryLibpq::delete_user(const std::string& user_id) {
    if (user_id.empty()) {
        return false;
    }
    
    // Soft delete - mark as deleted
    std::string query = R"(
        UPDATE user_schema.users 
        SET is_deleted = true, updated_at = $2, deleted_at = $2
        WHERE user_id = $1
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        user_id,
        timestamp_to_db_string(now)
    };
    
    auto result = execute_prepared("delete_user", params);
    if (result) {
        spdlog::info("User deleted successfully: {}", user_id);
        return true;
    }
    
    spdlog::error("Failed to delete user: {}", user_id);
    return false;
}

bool UserRepositoryLibpq::deactivate_user(const std::string& user_id) {
    if (user_id.empty()) {
        return false;
    }
    
    std::string query = R"(
        UPDATE user_schema.users 
        SET status = $2, updated_at = $3
        WHERE user_id = $1
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        user_id,
        std::to_string(static_cast<int>(models::UserStatus::INACTIVE)),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_prepared("deactivate_user", params);
    if (result) {
        spdlog::info("User deactivated: {}", user_id);
        return true;
    }
    
    spdlog::error("Failed to deactivate user: {}", user_id);
    return false;
}

bool UserRepositoryLibpq::reactivate_user(const std::string& user_id) {
    if (user_id.empty()) {
        return false;
    }
    
    std::string query = R"(
        UPDATE user_schema.users 
        SET status = $2, updated_at = $3
        WHERE user_id = $1
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        user_id,
        std::to_string(static_cast<int>(models::UserStatus::ACTIVE)),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_prepared("reactivate_user", params);
    if (result) {
        spdlog::info("User reactivated: {}", user_id);
        return true;
    }
    
    spdlog::error("Failed to reactivate user: {}", user_id);
    return false;
}

// Helper methods
std::string UserRepositoryLibpq::generate_uuid() const {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    static const char* hex_chars = "0123456789abcdef";
    
    std::string uuid = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx";
    
    for (char& c : uuid) {
        if (c == 'x') {
            c = hex_chars[dis(gen)];
        } else if (c == 'y') {
            c = hex_chars[dis(gen) & 0x3 | 0x8];
        }
    }
    
    return uuid;
}

std::string UserRepositoryLibpq::timestamp_to_db_string(const std::chrono::system_clock::time_point& tp) const {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::tm* tm = std::gmtime(&time_t);
    
    char buffer[64];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm);
    return std::string(buffer);
}

std::chrono::system_clock::time_point UserRepositoryLibpq::db_string_to_timestamp(const std::string& str) const {
    std::tm tm = {};
    std::istringstream ss(str);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    
    auto time_t = std::mktime(&tm);
    return std::chrono::system_clock::from_time_t(time_t);
}

// Result mapping methods
models::User UserRepositoryLibpq::map_result_to_user(pg_result* result, int row) const {
    models::User user;
    
    user.user_id = get_result_value(result, row, 0);
    user.username = get_result_value(result, row, 1);
    user.email = get_result_value(result, row, 2);
    user.password_hash = get_result_value(result, row, 3);
    user.salt = get_result_value(result, row, 4);
    user.display_name = get_result_value(result, row, 5);
    user.first_name = get_result_value(result, row, 6);
    user.last_name = get_result_value(result, row, 7);
    user.bio = get_result_value(result, row, 8);
    user.location = get_result_value(result, row, 9);
    user.website = get_result_value(result, row, 10);
    user.avatar_url = get_result_value(result, row, 11);
    user.banner_url = get_result_value(result, row, 12);
    user.timezone = get_result_value(result, row, 13);
    user.language = get_result_value(result, row, 14);
    
    // Parse enums
    if (!get_result_value(result, row, 15).empty()) {
        user.status = static_cast<models::UserStatus>(std::stoi(get_result_value(result, row, 15)));
    }
    if (!get_result_value(result, row, 16).empty()) {
        user.account_type = static_cast<models::AccountType>(std::stoi(get_result_value(result, row, 16)));
    }
    if (!get_result_value(result, row, 17).empty()) {
        user.privacy_level = static_cast<models::PrivacyLevel>(std::stoi(get_result_value(result, row, 17)));
    }
    
    // Parse booleans
    user.is_verified = get_result_bool(result, row, 18);
    user.is_premium = get_result_bool(result, row, 19);
    user.is_developer = get_result_bool(result, row, 20);
    user.discoverable_by_email = get_result_bool(result, row, 21);
    user.discoverable_by_phone = get_result_bool(result, row, 22);
    user.allow_direct_messages = get_result_bool(result, row, 23);
    user.allow_message_requests = get_result_bool(result, row, 24);
    user.show_activity_status = get_result_bool(result, row, 25);
    user.show_read_receipts = get_result_bool(result, row, 26);
    user.nsfw_content_enabled = get_result_bool(result, row, 27);
    user.autoplay_videos = get_result_bool(result, row, 28);
    user.high_quality_images = get_result_bool(result, row, 29);
    user.email_notifications = get_result_bool(result, row, 30);
    user.push_notifications = get_result_bool(result, row, 31);
    user.sms_notifications = get_result_bool(result, row, 32);
    
    // Parse timestamps
    std::string created_at_str = get_result_value(result, row, 33);
    if (!created_at_str.empty()) {
        user.created_at = std::stoll(created_at_str);
    }
    
    std::string updated_at_str = get_result_value(result, row, 34);
    if (!updated_at_str.empty()) {
        user.updated_at = std::stoll(updated_at_str);
    }
    
    return user;
}

// Initialize prepared statements for better performance and security
bool UserRepositoryLibpq::initialize_prepared_statements() {
    try {
        // User management statements
        prepare_statement("create_user", R"(
            INSERT INTO user_schema.users (
                user_id, username, email, password_hash, salt, display_name, 
                first_name, last_name, bio, location, website, avatar_url, banner_url,
                timezone, language, status, account_type, privacy_level, is_verified,
                is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
                allow_direct_messages, allow_message_requests, show_activity_status,
                show_read_receipts, nsfw_content_enabled, autoplay_videos,
                high_quality_images, email_notifications, push_notifications,
                sms_notifications, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
                $29, $30, $31, $32, $33, $34
            ) RETURNING user_id
        )");

        prepare_statement("get_user_by_id", R"(
            SELECT user_id, username, email, password_hash, salt, display_name,
                   first_name, last_name, bio, location, website, avatar_url, banner_url,
                   timezone, language, status, account_type, privacy_level, is_verified,
                   is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
                   allow_direct_messages, allow_message_requests, show_activity_status,
                   show_read_receipts, nsfw_content_enabled, autoplay_videos,
                   high_quality_images, email_notifications, push_notifications,
                   sms_notifications, created_at, updated_at
            FROM user_schema.users 
            WHERE user_id = $1 AND is_deleted = false
        )");

        prepare_statement("get_user_by_email", R"(
            SELECT user_id, username, email, password_hash, salt, display_name,
                   first_name, last_name, bio, location, website, avatar_url, banner_url,
                   timezone, language, status, account_type, privacy_level, is_verified,
                   is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
                   allow_direct_messages, allow_message_requests, show_activity_status,
                   show_read_receipts, nsfw_content_enabled, autoplay_videos,
                   high_quality_images, email_notifications, push_notifications,
                   sms_notifications, created_at, updated_at
            FROM user_schema.users 
            WHERE email = $1 AND is_deleted = false
        )");

        prepare_statement("get_user_by_username", R"(
            SELECT user_id, username, email, password_hash, salt, display_name,
                   first_name, last_name, bio, location, website, avatar_url, banner_url,
                   timezone, language, status, account_type, privacy_level, is_verified,
                   is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
                   allow_direct_messages, allow_message_requests, show_activity_status,
                   show_read_receipts, nsfw_content_enabled, autoplay_videos,
                   high_quality_images, email_notifications, push_notifications,
                   sms_notifications, created_at, updated_at
            FROM user_schema.users 
            WHERE username = $1 AND is_deleted = false
        )");

        prepare_statement("update_user", R"(
            UPDATE user_schema.users SET
                username = $2, email = $3, display_name = $4, first_name = $5,
                last_name = $6, bio = $7, location = $8, website = $9, avatar_url = $10,
                banner_url = $11, timezone = $12, language = $13, status = $14,
                account_type = $15, privacy_level = $16, is_verified = $17,
                is_premium = $18, is_developer = $19, discoverable_by_email = $20,
                discoverable_by_phone = $21, allow_direct_messages = $22,
                allow_message_requests = $23, show_activity_status = $24,
                show_read_receipts = $25, nsfw_content_enabled = $26,
                autoplay_videos = $27, high_quality_images = $28,
                email_notifications = $29, push_notifications = $30,
                sms_notifications = $31, updated_at = $32
            WHERE user_id = $1 AND is_deleted = false
        )");

        prepare_statement("delete_user", R"(
            UPDATE user_schema.users 
            SET is_deleted = true, updated_at = $2, deleted_at = $2
            WHERE user_id = $1
        )");

        prepare_statement("deactivate_user", R"(
            UPDATE user_schema.users 
            SET status = $2, updated_at = $3
            WHERE user_id = $1
        )");

        prepare_statement("reactivate_user", R"(
            UPDATE user_schema.users 
            SET status = $2, updated_at = $3
            WHERE user_id = $1
        )");

        // Validation statements
        prepare_statement("check_email_taken", R"(
            SELECT COUNT(*) FROM user_schema.users WHERE email = $1 AND is_deleted = false
        )");

        prepare_statement("check_username_taken", R"(
            SELECT COUNT(*) FROM user_schema.users WHERE username = $1 AND is_deleted = false
        )");

        // Search and listing statements
        prepare_statement("search_users", R"(
            SELECT user_id, username, email, display_name, bio, location, website,
                   avatar_url, banner_url, timezone, language, status, account_type,
                   privacy_level, is_verified, is_premium, is_developer, created_at, updated_at
            FROM user_schema.users 
            WHERE is_deleted = false 
            AND (username ILIKE $1 OR display_name ILIKE $1 OR bio ILIKE $1)
            ORDER BY 
                CASE WHEN username ILIKE $1 THEN 1
                     WHEN display_name ILIKE $1 THEN 2
                     ELSE 3 END,
                created_at DESC
            LIMIT $2 OFFSET $3
        )");

        prepare_statement("get_active_users", R"(
            SELECT user_id, username, email, display_name, bio, location, website,
                   avatar_url, banner_url, timezone, language, status, account_type,
                   privacy_level, is_verified, is_premium, is_developer, created_at, updated_at
            FROM user_schema.users 
            WHERE is_deleted = false AND status = 0
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
        )");

        // Profile management statements
        prepare_statement("update_user_avatar", R"(
            UPDATE user_schema.users 
            SET avatar_url = $2, updated_at = $3
            WHERE user_id = $1 AND is_deleted = false
        )");

        prepare_statement("update_user_banner", R"(
            UPDATE user_schema.users 
            SET banner_url = $2, updated_at = $3
            WHERE user_id = $1 AND is_deleted = false
        )");

        // Session management statements
        prepare_statement("create_session", R"(
            INSERT INTO user_schema.sessions (
                session_id, user_id, token, device_id, device_name, ip_address,
                user_agent, session_type, created_at, last_activity, expires_at, is_active
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING session_id
        )");

        prepare_statement("get_session_by_token", R"(
            SELECT session_id, user_id, token, device_id, device_name, ip_address,
                   user_agent, session_type, created_at, last_activity, expires_at, is_active
            FROM user_schema.sessions 
            WHERE token = $1 AND is_active = true
        )");

        prepare_statement("update_session", R"(
            UPDATE user_schema.sessions 
            SET last_activity = $2, expires_at = $3, is_active = $4
            WHERE session_id = $1
        )");

        prepare_statement("delete_session", R"(
            UPDATE user_schema.sessions 
            SET is_active = false, last_activity = $2
            WHERE token = $1
        )");

        prepare_statement("delete_user_sessions", R"(
            UPDATE user_schema.sessions 
            SET is_active = false, last_activity = $2
            WHERE user_id = $1
        )");

        prepare_statement("delete_expired_sessions", R"(
            UPDATE user_schema.sessions 
            SET is_active = false, last_activity = $1
            WHERE expires_at < $1 AND is_active = true
        )");

        // Analytics statements
        prepare_statement("get_total_user_count", R"(
            SELECT COUNT(*) FROM user_schema.users WHERE is_deleted = false
        )");

        prepare_statement("get_active_user_count", R"(
            SELECT COUNT(*) FROM user_schema.users WHERE is_deleted = false AND status = 0
        )");

        prepare_statement("get_verified_user_count", R"(
            SELECT COUNT(*) FROM user_schema.users WHERE is_deleted = false AND is_email_verified = true
        )");

        spdlog::info("Successfully initialized {} prepared statements", get_prepared_statement_count());
        return true;

    } catch (const std::exception& e) {
        spdlog::error("Failed to initialize prepared statements: {}", e.what());
        return false;
    }
}

// Implement user search functionality
std::vector<models::User> UserRepositoryLibpq::search_users(const std::string& query, int limit, int offset) {
    if (query.empty()) {
        return {};
    }
    
    std::string search_pattern = "%" + query + "%";
    std::vector<std::string> params = {
        search_pattern,
        std::to_string(limit),
        std::to_string(offset)
    };
    
    auto result = execute_prepared("search_users", params);
    if (!result) {
        spdlog::error("Failed to execute user search query");
        return {};
    }
    
    std::vector<models::User> users;
    int row_count = PQntuples(result.get());
    
    for (int i = 0; i < row_count; ++i) {
        try {
            models::User user;
            user.user_id = get_result_value(result.get(), i, 0);
            user.username = get_result_value(result.get(), i, 1);
            user.email = get_result_value(result.get(), i, 2);
            user.display_name = get_result_value(result.get(), i, 3);
            user.bio = get_result_value(result.get(), i, 4);
            user.location = get_result_value(result.get(), i, 5);
            user.website = get_result_value(result.get(), i, 6);
            user.avatar_url = get_result_value(result.get(), i, 7);
            user.banner_url = get_result_value(result.get(), i, 8);
            user.timezone = get_result_value(result.get(), i, 9);
            user.language = get_result_value(result.get(), i, 10);
            
            // Parse enums
            if (!get_result_value(result.get(), i, 11).empty()) {
                user.status = static_cast<models::UserStatus>(std::stoi(get_result_value(result.get(), i, 11)));
            }
            if (!get_result_value(result.get(), i, 12).empty()) {
                user.account_type = static_cast<models::AccountType>(std::stoi(get_result_value(result.get(), i, 12)));
            }
            if (!get_result_value(result.get(), i, 13).empty()) {
                user.privacy_level = static_cast<models::PrivacyLevel>(std::stoi(get_result_value(result.get(), i, 13)));
            }
            
            // Parse booleans
            user.is_verified = get_result_bool(result.get(), i, 14);
            user.is_premium = get_result_bool(result.get(), i, 15);
            user.is_developer = get_result_bool(result.get(), i, 16);
            
            // Parse timestamps
            std::string created_at_str = get_result_value(result.get(), i, 17);
            if (!created_at_str.empty()) {
                user.created_at = std::stoll(created_at_str);
            }
            
            std::string updated_at_str = get_result_value(result.get(), i, 18);
            if (!updated_at_str.empty()) {
                user.updated_at = std::stoll(updated_at_str);
            }
            
            users.push_back(user);
            
        } catch (const std::exception& e) {
            spdlog::error("Error parsing user search result row {}: {}", i, e.what());
            continue;
        }
    }
    
    spdlog::info("User search returned {} results for query: {}", users.size(), query);
    return users;
}

std::vector<models::User> UserRepositoryLibpq::get_users_by_ids(const std::vector<std::string>& user_ids) {
    if (user_ids.empty()) {
        return {};
    }
    
    // Build dynamic query for multiple user IDs
    std::stringstream query;
    query << "SELECT user_id, username, email, password_hash, salt, display_name, "
          << "first_name, last_name, bio, location, website, avatar_url, banner_url, "
          << "timezone, language, status, account_type, privacy_level, is_verified, "
          << "is_premium, is_developer, discoverable_by_email, discoverable_by_phone, "
          << "allow_direct_messages, allow_message_requests, show_activity_status, "
          << "show_read_receipts, nsfw_content_enabled, autoplay_videos, "
          << "high_quality_images, email_notifications, push_notifications, "
          << "sms_notifications, created_at, updated_at "
          << "FROM user_schema.users WHERE user_id IN (";
    
    std::vector<std::string> params;
    for (size_t i = 0; i < user_ids.size(); ++i) {
        if (i > 0) query << ",";
        query << "$" << (i + 1);
        params.push_back(user_ids[i]);
    }
    query << ") AND is_deleted = false";
    
    auto result = execute_query(query.str(), params);
    if (!result) {
        spdlog::error("Failed to execute get_users_by_ids query");
        return {};
    }
    
    std::vector<models::User> users;
    int row_count = PQntuples(result.get());
    
    for (int i = 0; i < row_count; ++i) {
        try {
            users.push_back(map_result_to_user(result.get(), i));
        } catch (const std::exception& e) {
            spdlog::error("Error parsing user result row {}: {}", i, e.what());
            continue;
        }
    }
    
    spdlog::info("Retrieved {} users by IDs", users.size());
    return users;
}

std::vector<models::User> UserRepositoryLibpq::get_active_users(int limit, int offset) {
    std::vector<std::string> params = {
        std::to_string(limit),
        std::to_string(offset)
    };
    
    auto result = execute_prepared("get_active_users", params);
    if (!result) {
        spdlog::error("Failed to execute get_active_users query");
        return {};
    }
    
    std::vector<models::User> users;
    int row_count = PQntuples(result.get());
    
    for (int i = 0; i < row_count; ++i) {
        try {
            users.push_back(map_result_to_user(result.get(), i));
        } catch (const std::exception& e) {
            spdlog::error("Error parsing active user result row {}: {}", i, e.what());
            continue;
        }
    }
    
    spdlog::info("Retrieved {} active users", users.size());
    return users;
}

std::vector<models::User> UserRepositoryLibpq::get_users_by_role(const std::string& role, int limit, int offset) {
    if (role.empty()) {
        return {};
    }
    
    // Map role string to account type enum
    models::AccountType account_type;
    if (role == "business") {
        account_type = models::AccountType::BUSINESS;
    } else if (role == "verified") {
        account_type = models::AccountType::VERIFIED;
    } else if (role == "premium") {
        account_type = models::AccountType::PREMIUM;
    } else if (role == "developer") {
        account_type = models::AccountType::DEVELOPER;
    } else {
        account_type = models::AccountType::PERSONAL;
    }
    
    std::string query = R"(
        SELECT user_id, username, email, password_hash, salt, display_name,
               first_name, last_name, bio, location, website, avatar_url, banner_url,
               timezone, language, status, account_type, privacy_level, is_verified,
               is_premium, is_developer, discoverable_by_email, discoverable_by_phone,
               allow_direct_messages, allow_message_requests, show_activity_status,
               show_read_receipts, nsfw_content_enabled, autoplay_videos,
               high_quality_images, email_notifications, push_notifications,
               sms_notifications, created_at, updated_at
        FROM user_schema.users 
        WHERE account_type = $1 AND is_deleted = false
        ORDER BY created_at DESC
        LIMIT $2 OFFSET $3
    )";
    
    std::vector<std::string> params = {
        std::to_string(static_cast<int>(account_type)),
        std::to_string(limit),
        std::to_string(offset)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to execute get_users_by_role query");
        return {};
    }
    
    std::vector<models::User> users;
    int row_count = PQntuples(result.get());
    
    for (int i = 0; i < row_count; ++i) {
        try {
            users.push_back(map_result_to_user(result.get(), i));
        } catch (const std::exception& e) {
            spdlog::error("Error parsing role-based user result row {}: {}", i, e.what());
            continue;
        }
    }
    
    spdlog::info("Retrieved {} users with role: {}", users.size(), role);
    return users;
}

// Profile management methods
std::optional<models::Profile> UserRepositoryLibpq::get_user_profile(const std::string& user_id) {
    if (user_id.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT p.user_id, p.bio, p.location, p.website, p.avatar_url, p.banner_url,
               p.timezone, p.language, p.created_at, p.updated_at,
               u.username, u.display_name, u.first_name, u.last_name, u.status,
               u.account_type, u.privacy_level, u.is_verified, u.is_premium, u.is_developer
        FROM user_schema.profiles p
        JOIN user_schema.users u ON p.user_id = u.user_id
        WHERE p.user_id = $1 AND u.is_deleted = false
    )";
    
    std::vector<std::string> params = {user_id};
    auto result = execute_query(query, params);
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::warn("Profile not found for user: {}", user_id);
        return std::nullopt;
    }
    
    try {
        return map_result_to_user_profile(result.get(), 0);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing profile result: {}", e.what());
        return std::nullopt;
    }
}

bool UserRepositoryLibpq::update_user_profile(const models::Profile& profile) {
    if (profile.user_id.empty()) {
        spdlog::error("Cannot update profile: user_id is empty");
        return false;
    }
    
    std::string query = R"(
        UPDATE user_schema.profiles SET
            bio = $2, location = $3, website = $4, avatar_url = $5, banner_url = $6,
            timezone = $7, language = $8, updated_at = $9
        WHERE user_id = $1
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        profile.user_id,
        profile.bio,
        profile.location,
        profile.website,
        profile.avatar_url,
        profile.banner_url,
        profile.timezone,
        profile.language,
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to update profile for user: {}", profile.user_id);
        return false;
    }
    
    spdlog::info("Successfully updated profile for user: {}", profile.user_id);
    return true;
}

bool UserRepositoryLibpq::update_user_avatar(const std::string& user_id, const std::string& avatar_url) {
    if (user_id.empty()) {
        spdlog::error("Cannot update avatar: user_id is empty");
        return false;
    }
    
    std::vector<std::string> params = {
        user_id,
        avatar_url,
        timestamp_to_db_string(std::chrono::system_clock::now())
    };
    
    auto result = execute_prepared("update_user_avatar", params);
    if (!result) {
        spdlog::error("Failed to update avatar for user: {}", user_id);
        return false;
    }
    
    spdlog::info("Successfully updated avatar for user: {}", user_id);
    return true;
}

bool UserRepositoryLibpq::update_user_banner(const std::string& user_id, const std::string& banner_url) {
    if (user_id.empty()) {
        spdlog::error("Cannot update banner: user_id is empty");
        return false;
    }
    
    std::vector<std::string> params = {
        user_id,
        banner_url,
        timestamp_to_db_string(std::chrono::system_clock::now())
    };
    
    auto result = execute_prepared("update_user_banner", params);
    if (!result) {
        spdlog::error("Failed to update banner for user: {}", user_id);
        return false;
    }
    
    spdlog::info("Successfully updated banner for user: {}", user_id);
    return true;
}

// Session management methods
std::optional<models::Session> UserRepositoryLibpq::create_session(const models::Session& session) {
    if (session.user_id.empty() || session.token.empty()) {
        spdlog::error("Cannot create session: user_id or token is empty");
        return std::nullopt;
    }
    
    std::vector<std::string> params = {
        session.session_id,
        session.user_id,
        session.token,
        session.device_id,
        session.device_name,
        session.ip_address,
        session.user_agent,
        std::to_string(static_cast<int>(session.session_type)),
        timestamp_to_db_string(session.created_at),
        timestamp_to_db_string(session.last_activity),
        timestamp_to_db_string(session.expires_at),
        session.is_active ? "true" : "false"
    };
    
    auto result = execute_prepared("create_session", params);
    if (!result) {
        spdlog::error("Failed to create session for user: {}", session.user_id);
        return std::nullopt;
    }
    
    spdlog::info("Successfully created session for user: {}", session.user_id);
    return session;
}

std::optional<models::Session> UserRepositoryLibpq::get_session_by_token(const std::string& token) {
    if (token.empty()) {
        return std::nullopt;
    }
    
    std::vector<std::string> params = {token};
    auto result = execute_prepared("get_session_by_token", params);
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::warn("Session not found for token: {}", token);
        return std::nullopt;
    }
    
    try {
        return map_result_to_user_session(result.get(), 0);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing session result: {}", e.what());
        return std::nullopt;
    }
}

bool UserRepositoryLibpq::update_session(const models::Session& session) {
    if (session.session_id.empty()) {
        spdlog::error("Cannot update session: session_id is empty");
        return false;
    }
    
    std::vector<std::string> params = {
        session.session_id,
        timestamp_to_db_string(session.last_activity),
        timestamp_to_db_string(session.expires_at),
        session.is_active ? "true" : "false"
    };
    
    auto result = execute_prepared("update_session", params);
    if (!result) {
        spdlog::error("Failed to update session: {}", session.session_id);
        return false;
    }
    
    spdlog::info("Successfully updated session: {}", session.session_id);
    return true;
}

bool UserRepositoryLibpq::delete_session(const std::string& token) {
    if (token.empty()) {
        spdlog::error("Cannot delete session: token is empty");
        return false;
    }
    
    std::vector<std::string> params = {
        token,
        timestamp_to_db_string(std::chrono::system_clock::now())
    };
    
    auto result = execute_prepared("delete_session", params);
    if (!result) {
        spdlog::error("Failed to delete session with token: {}", token);
        return false;
    }
    
    spdlog::info("Successfully deleted session with token: {}", token);
    return true;
}

bool UserRepositoryLibpq::delete_user_sessions(const std::string& user_id) {
    if (user_id.empty()) {
        spdlog::error("Cannot delete user sessions: user_id is empty");
        return false;
    }
    
    std::vector<std::string> params = {
        user_id,
        timestamp_to_db_string(std::chrono::system_clock::now())
    };
    
    auto result = execute_prepared("delete_user_sessions", params);
    if (!result) {
        spdlog::error("Failed to delete sessions for user: {}", user_id);
        return false;
    }
    
    spdlog::info("Successfully deleted all sessions for user: {}", user_id);
    return true;
}

bool UserRepositoryLibpq::delete_expired_sessions() {
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {timestamp_to_db_string(now)};
    
    auto result = execute_prepared("delete_expired_sessions", params);
    if (!result) {
        spdlog::error("Failed to delete expired sessions");
        return false;
    }
    
    spdlog::info("Successfully cleaned up expired sessions");
    return true;
}

std::vector<models::Session> UserRepositoryLibpq::get_user_sessions(const std::string& user_id) {
    if (user_id.empty()) {
        return {};
    }
    
    std::string query = R"(
        SELECT session_id, user_id, token, device_id, device_name, ip_address,
               user_agent, session_type, created_at, last_activity, expires_at, is_active
        FROM user_schema.sessions 
        WHERE user_id = $1 AND is_active = true
        ORDER BY last_activity DESC
    )";
    
    std::vector<std::string> params = {user_id};
    auto result = execute_query(query, params);
    
    if (!result) {
        spdlog::error("Failed to get sessions for user: {}", user_id);
        return {};
    }
    
    std::vector<models::Session> sessions;
    int row_count = PQntuples(result.get());
    
    for (int i = 0; i < row_count; ++i) {
        try {
            sessions.push_back(map_result_to_user_session(result.get(), i));
        } catch (const std::exception& e) {
            spdlog::error("Error parsing session result row {}: {}", i, e.what());
            continue;
        }
    }
    
    spdlog::info("Retrieved {} active sessions for user: {}", sessions.size(), user_id);
    return sessions;
}

// Two-factor authentication methods
std::optional<TwoFactorAuth> UserRepositoryLibpq::create_two_factor_auth(const TwoFactorAuth& tfa) {
    if (tfa.user_id.empty()) {
        spdlog::error("Cannot create 2FA: user_id is empty");
        return std::nullopt;
    }
    
    std::string query = R"(
        INSERT INTO user_schema.two_factor_auth (
            user_id, secret_key, backup_codes, is_enabled, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (user_id) DO UPDATE SET
            secret_key = EXCLUDED.secret_key,
            backup_codes = EXCLUDED.backup_codes,
            is_enabled = EXCLUDED.is_enabled,
            updated_at = EXCLUDED.updated_at
        RETURNING user_id
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        tfa.user_id,
        tfa.secret_key,
        tfa.backup_codes,
        tfa.is_enabled ? "true" : "false",
        timestamp_to_db_string(now),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to create 2FA for user: {}", tfa.user_id);
        return std::nullopt;
    }
    
    spdlog::info("Successfully created/updated 2FA for user: {}", tfa.user_id);
    return tfa;
}

std::optional<TwoFactorAuth> UserRepositoryLibpq::get_two_factor_auth(const std::string& user_id) {
    if (user_id.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT user_id, secret_key, backup_codes, is_enabled, created_at, updated_at
        FROM user_schema.two_factor_auth 
        WHERE user_id = $1
    )";
    
    std::vector<std::string> params = {user_id};
    auto result = execute_query(query, params);
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::warn("2FA not found for user: {}", user_id);
        return std::nullopt;
    }
    
    try {
        return map_result_to_two_factor_auth(result.get(), 0);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing 2FA result: {}", e.what());
        return std::nullopt;
    }
}

bool UserRepositoryLibpq::update_two_factor_auth(const TwoFactorAuth& tfa) {
    if (tfa.user_id.empty()) {
        spdlog::error("Cannot update 2FA: user_id is empty");
        return false;
    }
    
    std::string query = R"(
        UPDATE user_schema.two_factor_auth SET
            secret_key = $2, backup_codes = $3, is_enabled = $4, updated_at = $5
        WHERE user_id = $1
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        tfa.user_id,
        tfa.secret_key,
        tfa.backup_codes,
        tfa.is_enabled ? "true" : "false",
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to update 2FA for user: {}", tfa.user_id);
        return false;
    }
    
    spdlog::info("Successfully updated 2FA for user: {}", tfa.user_id);
    return true;
}

bool UserRepositoryLibpq::delete_two_factor_auth(const std::string& user_id) {
    if (user_id.empty()) {
        spdlog::error("Cannot delete 2FA: user_id is empty");
        return false;
    }
    
    std::string query = "DELETE FROM user_schema.two_factor_auth WHERE user_id = $1";
    std::vector<std::string> params = {user_id};
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to delete 2FA for user: {}", user_id);
        return false;
    }
    
    spdlog::info("Successfully deleted 2FA for user: {}", user_id);
    return true;
}

bool UserRepositoryLibpq::verify_two_factor_code(const std::string& user_id, const std::string& code) {
    if (user_id.empty() || code.empty()) {
        spdlog::error("Cannot verify 2FA code: user_id or code is empty");
        return false;
    }
    
    // This is a simplified verification - in a real implementation,
    // you would use a TOTP library to verify the code against the stored secret
    auto tfa = get_two_factor_auth(user_id);
    if (!tfa || !tfa->is_enabled) {
        spdlog::warn("2FA not enabled for user: {}", user_id);
        return false;
    }
    
    // For now, just check if the code exists in backup codes
    // In production, implement proper TOTP verification
    if (tfa->backup_codes.find(code) != std::string::npos) {
        spdlog::info("2FA code verified for user: {}", user_id);
        return true;
    }
    
    spdlog::warn("Invalid 2FA code for user: {}", user_id);
    return false;
}

// Password management methods
std::optional<PasswordResetToken> UserRepositoryLibpq::create_password_reset_token(const PasswordResetToken& token) {
    if (token.user_id.empty() || token.token.empty()) {
        spdlog::error("Cannot create password reset token: user_id or token is empty");
        return std::nullopt;
    }
    
    std::string query = R"(
        INSERT INTO user_schema.password_reset_tokens (
            user_id, token, expires_at, created_at
        ) VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id) DO UPDATE SET
            token = EXCLUDED.token,
            expires_at = EXCLUDED.expires_at,
            created_at = EXCLUDED.created_at
        RETURNING user_id
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        token.user_id,
        token.token,
        timestamp_to_db_string(token.expires_at),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to create password reset token for user: {}", token.user_id);
        return std::nullopt;
    }
    
    spdlog::info("Successfully created password reset token for user: {}", token.user_id);
    return token;
}

std::optional<PasswordResetToken> UserRepositoryLibpq::get_password_reset_token(const std::string& token) {
    if (token.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT user_id, token, expires_at, created_at
        FROM user_schema.password_reset_tokens 
        WHERE token = $1 AND expires_at > NOW()
    )";
    
    std::vector<std::string> params = {token};
    auto result = execute_query(query, params);
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::warn("Password reset token not found or expired: {}", token);
        return std::nullopt;
    }
    
    try {
        return map_result_to_password_reset_token(result.get(), 0);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing password reset token result: {}", e.what());
        return std::nullopt;
    }
}

bool UserRepositoryLibpq::delete_password_reset_token(const std::string& token) {
    if (token.empty()) {
        spdlog::error("Cannot delete password reset token: token is empty");
        return false;
    }
    
    std::string query = "DELETE FROM user_schema.password_reset_tokens WHERE token = $1";
    std::vector<std::string> params = {token};
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to delete password reset token: {}", token);
        return false;
    }
    
    spdlog::info("Successfully deleted password reset token: {}", token);
    return true;
}

bool UserRepositoryLibpq::delete_expired_password_reset_tokens() {
    std::string query = "DELETE FROM user_schema.password_reset_tokens WHERE expires_at <= NOW()";
    
    auto result = execute_query(query, {});
    if (!result) {
        spdlog::error("Failed to delete expired password reset tokens");
        return false;
    }
    
    spdlog::info("Successfully cleaned up expired password reset tokens");
    return true;
}

bool UserRepositoryLibpq::update_user_password(const std::string& user_id, const std::string& hashed_password) {
    if (user_id.empty() || hashed_password.empty()) {
        spdlog::error("Cannot update password: user_id or hashed_password is empty");
        return false;
    }
    
    std::string query = R"(
        UPDATE user_schema.users 
        SET password_hash = $2, updated_at = $3
        WHERE user_id = $1 AND is_deleted = false
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        user_id,
        hashed_password,
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to update password for user: {}", user_id);
        return false;
    }
    
    // Delete any existing password reset tokens for this user
    std::string delete_tokens_query = "DELETE FROM user_schema.password_reset_tokens WHERE user_id = $1";
    execute_query(delete_tokens_query, {user_id});
    
    spdlog::info("Successfully updated password for user: {}", user_id);
    return true;
}

// Email verification methods
std::optional<EmailVerificationToken> UserRepositoryLibpq::create_email_verification_token(const EmailVerificationToken& token) {
    if (token.user_id.empty() || token.token.empty()) {
        spdlog::error("Cannot create email verification token: user_id or token is empty");
        return std::nullopt;
    }
    
    std::string query = R"(
        INSERT INTO user_schema.email_verification_tokens (
            user_id, token, expires_at, created_at
        ) VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id) DO UPDATE SET
            token = EXCLUDED.token,
            expires_at = EXCLUDED.expires_at,
            created_at = EXCLUDED.created_at
        RETURNING user_id
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        token.user_id,
        token.token,
        timestamp_to_db_string(token.expires_at),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to create email verification token for user: {}", token.user_id);
        return std::nullopt;
    }
    
    spdlog::info("Successfully created email verification token for user: {}", token.user_id);
    return token;
}

std::optional<EmailVerificationToken> UserRepositoryLibpq::get_email_verification_token(const std::string& token) {
    if (token.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT user_id, token, expires_at, created_at
        FROM user_schema.email_verification_tokens 
        WHERE token = $1 AND expires_at > NOW()
    )";
    
    std::vector<std::string> params = {token};
    auto result = execute_query(query, params);
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::warn("Email verification token not found or expired: {}", token);
        return std::nullopt;
    }
    
    try {
        return map_result_to_email_verification_token(result.get(), 0);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing email verification token result: {}", e.what());
        return std::nullopt;
    }
}

bool UserRepositoryLibpq::delete_email_verification_token(const std::string& token) {
    if (token.empty()) {
        spdlog::error("Cannot delete email verification token: token is empty");
        return false;
    }
    
    std::string query = "DELETE FROM user_schema.email_verification_tokens WHERE token = $1";
    std::vector<std::string> params = {token};
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to delete email verification token: {}", token);
        return false;
    }
    
    spdlog::info("Successfully deleted email verification token: {}", token);
    return true;
}

bool UserRepositoryLibpq::delete_expired_email_verification_tokens() {
    std::string query = "DELETE FROM user_schema.email_verification_tokens WHERE expires_at <= NOW()";
    
    auto result = execute_query(query, {});
    if (!result) {
        spdlog::error("Failed to delete expired email verification tokens");
        return false;
    }
    
    spdlog::info("Successfully cleaned up expired email verification tokens");
    return true;
}

bool UserRepositoryLibpq::verify_user_email(const std::string& user_id) {
    if (user_id.empty()) {
        spdlog::error("Cannot verify email: user_id is empty");
        return false;
    }
    
    std::string query = R"(
        UPDATE user_schema.users 
        SET is_email_verified = true, updated_at = $2
        WHERE user_id = $1 AND is_deleted = false
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        user_id,
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to verify email for user: {}", user_id);
        return false;
    }
    
    // Delete the verification token
    std::string delete_token_query = "DELETE FROM user_schema.email_verification_tokens WHERE user_id = $1";
    execute_query(delete_token_query, {user_id});
    
    spdlog::info("Successfully verified email for user: {}", user_id);
    return true;
}

// User settings methods
std::optional<UserSettings> UserRepositoryLibpq::get_user_settings(const std::string& user_id) {
    if (user_id.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT user_id, theme, language, timezone, notifications_enabled, 
               email_notifications, push_notifications, sms_notifications,
               privacy_level, created_at, updated_at
        FROM user_schema.user_settings 
        WHERE user_id = $1
    )";
    
    std::vector<std::string> params = {user_id};
    auto result = execute_query(query, params);
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::warn("User settings not found for user: {}", user_id);
        return std::nullopt;
    }
    
    try {
        return map_result_to_user_settings(result.get(), 0);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing user settings result: {}", e.what());
        return std::nullopt;
    }
}

bool UserRepositoryLibpq::update_user_settings(const UserSettings& settings) {
    if (settings.user_id.empty()) {
        spdlog::error("Cannot update user settings: user_id is empty");
        return false;
    }
    
    std::string query = R"(
        INSERT INTO user_schema.user_settings (
            user_id, theme, language, timezone, notifications_enabled,
            email_notifications, push_notifications, sms_notifications,
            privacy_level, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (user_id) DO UPDATE SET
            theme = EXCLUDED.theme,
            language = EXCLUDED.language,
            timezone = EXCLUDED.timezone,
            notifications_enabled = EXCLUDED.notifications_enabled,
            email_notifications = EXCLUDED.email_notifications,
            push_notifications = EXCLUDED.push_notifications,
            sms_notifications = EXCLUDED.sms_notifications,
            privacy_level = EXCLUDED.privacy_level,
            updated_at = EXCLUDED.updated_at
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        settings.user_id,
        settings.theme,
        settings.language,
        settings.timezone,
        settings.notifications_enabled ? "true" : "false",
        settings.email_notifications ? "true" : "false",
        settings.push_notifications ? "true" : "false",
        settings.sms_notifications ? "true" : "false",
        std::to_string(static_cast<int>(settings.privacy_level)),
        timestamp_to_db_string(now),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to update user settings for user: {}", settings.user_id);
        return false;
    }
    
    spdlog::info("Successfully updated user settings for user: {}", settings.user_id);
    return true;
}

bool UserRepositoryLibpq::update_user_setting(const std::string& user_id, const std::string& setting_key, const std::string& setting_value) {
    if (user_id.empty() || setting_key.empty()) {
        spdlog::error("Cannot update user setting: user_id or setting_key is empty");
        return false;
    }
    
    // For individual settings, we'll use a JSON-based approach or specific column updates
    // This is a simplified implementation - in production you might want to use JSON columns
    std::string query;
    std::vector<std::string> params;
    
    if (setting_key == "theme") {
        query = "UPDATE user_schema.user_settings SET theme = $2, updated_at = $3 WHERE user_id = $1";
        params = {user_id, setting_value, timestamp_to_db_string(std::chrono::system_clock::now())};
    } else if (setting_key == "language") {
        query = "UPDATE user_schema.user_settings SET language = $2, updated_at = $3 WHERE user_id = $1";
        params = {user_id, setting_value, timestamp_to_db_string(std::chrono::system_clock::now())};
    } else if (setting_key == "timezone") {
        query = "UPDATE user_schema.user_settings SET timezone = $2, updated_at = $3 WHERE user_id = $1";
        params = {user_id, setting_value, timestamp_to_db_string(std::chrono::system_clock::now())};
    } else {
        spdlog::error("Unknown setting key: {}", setting_key);
        return false;
    }
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to update user setting {} for user: {}", setting_key, user_id);
        return false;
    }
    
    spdlog::info("Successfully updated user setting {} for user: {}", setting_key, user_id);
    return true;
}

// User statistics methods
std::optional<UserStats> UserRepositoryLibpq::get_user_stats(const std::string& user_id) {
    if (user_id.empty()) {
        return std::nullopt;
    }
    
    std::string query = R"(
        SELECT user_id, notes_count, followers_count, following_count, 
               likes_received, comments_received, shares_received,
               total_views, total_engagement, last_activity, created_at, updated_at
        FROM user_schema.user_stats 
        WHERE user_id = $1
    )";
    
    std::vector<std::string> params = {user_id};
    auto result = execute_query(query, params);
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::warn("User stats not found for user: {}", user_id);
        return std::nullopt;
    }
    
    try {
        return map_result_to_user_stats(result.get(), 0);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing user stats result: {}", e.what());
        return std::nullopt;
    }
}

bool UserRepositoryLibpq::update_user_stats(const UserStats& stats) {
    if (stats.user_id.empty()) {
        spdlog::error("Cannot update user stats: user_id is empty");
        return false;
    }
    
    std::string query = R"(
        INSERT INTO user_schema.user_stats (
            user_id, notes_count, followers_count, following_count,
            likes_received, comments_received, shares_received,
            total_views, total_engagement, last_activity, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (user_id) DO UPDATE SET
            notes_count = EXCLUDED.notes_count,
            followers_count = EXCLUDED.followers_count,
            following_count = EXCLUDED.following_count,
            likes_received = EXCLUDED.likes_received,
            comments_received = EXCLUDED.comments_received,
            shares_received = EXCLUDED.shares_received,
            total_views = EXCLUDED.total_views,
            total_engagement = EXCLUDED.total_engagement,
            last_activity = EXCLUDED.last_activity,
            updated_at = EXCLUDED.updated_at
    )";
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        stats.user_id,
        std::to_string(stats.notes_count),
        std::to_string(stats.followers_count),
        std::to_string(stats.following_count),
        std::to_string(stats.likes_received),
        std::to_string(stats.comments_received),
        std::to_string(stats.shares_received),
        std::to_string(stats.total_views),
        std::to_string(stats.total_engagement),
        timestamp_to_db_string(stats.last_activity),
        timestamp_to_db_string(now),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to update user stats for user: {}", stats.user_id);
        return false;
    }
    
    spdlog::info("Successfully updated user stats for user: {}", stats.user_id);
    return true;
}

bool UserRepositoryLibpq::increment_user_stat(const std::string& user_id, const std::string& stat_name, int increment) {
    if (user_id.empty() || stat_name.empty()) {
        spdlog::error("Cannot increment user stat: user_id or stat_name is empty");
        return false;
    }
    
    std::string query;
    if (stat_name == "notes_count") {
        query = "UPDATE user_schema.user_stats SET notes_count = notes_count + $2, updated_at = $3 WHERE user_id = $1";
    } else if (stat_name == "followers_count") {
        query = "UPDATE user_schema.user_stats SET followers_count = followers_count + $2, updated_at = $3 WHERE user_id = $1";
    } else if (stat_name == "following_count") {
        query = "UPDATE user_schema.user_stats SET following_count = following_count + $2, updated_at = $3 WHERE user_id = $1";
    } else if (stat_name == "likes_received") {
        query = "UPDATE user_schema.user_stats SET likes_received = likes_received + $2, updated_at = $3 WHERE user_id = $1";
    } else if (stat_name == "comments_received") {
        query = "UPDATE user_schema.user_stats SET comments_received = comments_received + $2, updated_at = $3 WHERE user_id = $1";
    } else if (stat_name == "shares_received") {
        query = "UPDATE user_schema.user_stats SET shares_received = shares_received + $2, updated_at = $3 WHERE user_id = $1";
    } else if (stat_name == "total_views") {
        query = "UPDATE user_schema.user_stats SET total_views = total_views + $2, updated_at = $3 WHERE user_id = $1";
    } else if (stat_name == "total_engagement") {
        query = "UPDATE user_schema.user_stats SET total_engagement = total_engagement + $2, updated_at = $3 WHERE user_id = $1";
    } else {
        spdlog::error("Unknown stat name: {}", stat_name);
        return false;
    }
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> params = {
        user_id,
        std::to_string(increment),
        timestamp_to_db_string(now)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to increment user stat {} for user: {}", stat_name, user_id);
        return false;
    }
    
    spdlog::info("Successfully incremented user stat {} by {} for user: {}", stat_name, increment, user_id);
    return true;
}

// Login history methods
bool UserRepositoryLibpq::add_login_history(const UserLoginHistory& history) {
    if (history.user_id.empty() || history.session_id.empty()) {
        spdlog::error("Cannot add login history: user_id or session_id is empty");
        return false;
    }
    
    std::string query = R"(
        INSERT INTO user_schema.user_login_history (
            user_id, session_id, login_timestamp, logout_timestamp,
            ip_address, user_agent, device_id, device_name,
            location, success, failure_reason, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    )";
    
    std::vector<std::string> params = {
        history.user_id,
        history.session_id,
        timestamp_to_db_string(history.login_timestamp),
        history.logout_timestamp.has_value() ? timestamp_to_db_string(history.logout_timestamp.value()) : "",
        history.ip_address,
        history.user_agent,
        history.device_id,
        history.device_name,
        history.location,
        history.success ? "true" : "false",
        history.failure_reason.value_or(""),
        timestamp_to_db_string(history.created_at)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to add login history for user: {}", history.user_id);
        return false;
    }
    
    spdlog::info("Successfully added login history for user: {}", history.user_id);
    return true;
}

std::vector<UserLoginHistory> UserRepositoryLibpq::get_user_login_history(const std::string& user_id, int limit) {
    if (user_id.empty()) {
        return {};
    }
    
    std::string query = R"(
        SELECT user_id, session_id, login_timestamp, logout_timestamp, 
               ip_address, user_agent, device_id, device_name, 
               location, success, failure_reason, created_at
        FROM user_schema.user_login_history 
        WHERE user_id = $1
        ORDER BY login_timestamp DESC
        LIMIT $2
    )";
    
    std::vector<std::string> params = {
        user_id,
        std::to_string(limit)
    };
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to execute get_user_login_history query");
        return {};
    }
    
    std::vector<UserLoginHistory> history;
    int row_count = PQntuples(result.get());
    
    for (int i = 0; i < row_count; ++i) {
        try {
            history.push_back(map_result_to_user_login_history(result.get(), i));
        } catch (const std::exception& e) {
            spdlog::error("Error parsing login history result row {}: {}", i, e.what());
            continue;
        }
    }
    
    spdlog::info("Retrieved {} login history records for user: {}", history.size(), user_id);
    return history;
}

bool UserRepositoryLibpq::delete_old_login_history(int days_to_keep) {
    if (days_to_keep <= 0) {
        spdlog::error("Cannot delete old login history: days_to_keep must be positive");
        return false;
    }
    
    std::string query = R"(
        DELETE FROM user_schema.user_login_history 
        WHERE login_timestamp < NOW() - INTERVAL '$1 days'
    )";
    
    std::vector<std::string> params = {std::to_string(days_to_keep)};
    
    auto result = execute_query(query, params);
    if (!result) {
        spdlog::error("Failed to delete old login history older than {} days", days_to_keep);
        return false;
    }
    
    spdlog::info("Successfully deleted old login history older than {} days", days_to_keep);
    return true;
}

// User validation placeholders
bool UserRepositoryLibpq::is_email_taken(const std::string& email) {
    if (email.empty()) return false;
    
    std::string query = "SELECT COUNT(*) FROM user_schema.users WHERE email = $1 AND is_deleted = false";
    std::vector<std::string> params = {email};
    
    auto result = execute_prepared("check_email_taken", params);
    if (result && PQntuples(result.get()) > 0) {
        int count = std::stoi(get_result_value(result.get(), 0, 0));
        return count > 0;
    }
    
    return false;
}

bool UserRepositoryLibpq::is_username_taken(const std::string& username) {
    if (username.empty()) return false;
    
    std::string query = "SELECT COUNT(*) FROM user_schema.users WHERE username = $1 AND is_deleted = false";
    std::vector<std::string> params = {username};
    
    auto result = execute_prepared("check_username_taken", params);
    if (result && PQntuples(result.get()) > 0) {
        int count = std::stoi(get_result_value(result.get(), 0, 0));
        return count > 0;
    }
    
    return false;
}

bool UserRepositoryLibpq::is_user_active(const std::string& user_id) {
    if (user_id.empty()) return false;
    
    auto user = get_user_by_id(user_id);
    return user.has_value() && user->is_active();
}

bool UserRepositoryLibpq::is_user_verified(const std::string& user_id) {
    if (user_id.empty()) return false;
    
    auto user = get_user_by_id(user_id);
    return user.has_value() && user->is_email_verified;
}

// Bulk operations methods
bool UserRepositoryLibpq::bulk_update_users(const std::vector<models::User>& users) {
    if (users.empty()) {
        spdlog::warn("No users provided for bulk update");
        return true;
    }
    
    // Start transaction
    if (!begin_transaction()) {
        spdlog::error("Failed to begin transaction for bulk user update");
        return false;
    }
    
    bool success = true;
    for (const auto& user : users) {
        if (!update_user(user)) {
            spdlog::error("Failed to update user in bulk operation: {}", user.user_id);
            success = false;
            break;
        }
    }
    
    if (success) {
        if (!commit_transaction()) {
            spdlog::error("Failed to commit transaction for bulk user update");
            return false;
        }
        spdlog::info("Successfully updated {} users in bulk operation", users.size());
    } else {
        if (!rollback_transaction()) {
            spdlog::error("Failed to rollback transaction for bulk user update");
        }
    }
    
    return success;
}

bool UserRepositoryLibpq::bulk_delete_users(const std::vector<std::string>& user_ids) {
    if (user_ids.empty()) {
        spdlog::warn("No user IDs provided for bulk deletion");
        return true;
    }
    
    // Start transaction
    if (!begin_transaction()) {
        spdlog::error("Failed to begin transaction for bulk user deletion");
        return false;
    }
    
    bool success = true;
    for (const auto& user_id : user_ids) {
        if (!delete_user(user_id)) {
            spdlog::error("Failed to delete user in bulk operation: {}", user_id);
            success = false;
            break;
        }
    }
    
    if (success) {
        if (!commit_transaction()) {
            spdlog::error("Failed to commit transaction for bulk user deletion");
            return false;
        }
        spdlog::info("Successfully deleted {} users in bulk operation", user_ids.size());
    } else {
        if (!rollback_transaction()) {
            spdlog::error("Failed to rollback transaction for bulk user deletion");
        }
    }
    
    return success;
}

bool UserRepositoryLibpq::bulk_deactivate_users(const std::vector<std::string>& user_ids) {
    if (user_ids.empty()) {
        spdlog::warn("No user IDs provided for bulk deactivation");
        return true;
    }
    
    // Start transaction
    if (!begin_transaction()) {
        spdlog::error("Failed to begin transaction for bulk user deactivation");
        return false;
    }
    
    bool success = true;
    for (const auto& user_id : user_ids) {
        if (!deactivate_user(user_id)) {
            spdlog::error("Failed to deactivate user in bulk operation: {}", user_id);
            success = false;
            break;
        }
    }
    
    if (success) {
        if (!commit_transaction()) {
            spdlog::error("Failed to commit transaction for bulk user deactivation");
            return false;
        }
        spdlog::info("Successfully deactivated {} users in bulk operation", user_ids.size());
    } else {
        if (!rollback_transaction()) {
            spdlog::error("Failed to rollback transaction for bulk user deactivation");
        }
    }
    
    return success;
}

// Analytics and reporting methods
int UserRepositoryLibpq::get_total_user_count() {
    std::string query = "SELECT COUNT(*) FROM user_schema.users WHERE is_deleted = false";
    auto result = execute_query(query, {});
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::error("Failed to get total user count");
        return 0;
    }
    
    try {
        int count = std::stoi(get_result_value(result.get(), 0, 0));
        spdlog::info("Total user count: {}", count);
        return count;
    } catch (const std::exception& e) {
        spdlog::error("Error parsing total user count: {}", e.what());
        return 0;
    }
}

int UserRepositoryLibpq::get_active_user_count() {
    std::string query = "SELECT COUNT(*) FROM user_schema.users WHERE status = $1 AND is_deleted = false";
    std::vector<std::string> params = {std::to_string(static_cast<int>(models::UserStatus::ACTIVE))};
    
    auto result = execute_query(query, params);
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::error("Failed to get active user count");
        return 0;
    }
    
    try {
        int count = std::stoi(get_result_value(result.get(), 0, 0));
        spdlog::info("Active user count: {}", count);
        return count;
    } catch (const std::exception& e) {
        spdlog::error("Error parsing active user count: {}", e.what());
        return 0;
    }
}

int UserRepositoryLibpq::get_verified_user_count() {
    std::string query = "SELECT COUNT(*) FROM user_schema.users WHERE is_verified = true AND is_deleted = false";
    auto result = execute_query(query, {});
    
    if (!result || PQntuples(result.get()) == 0) {
        spdlog::error("Failed to get verified user count");
        return 0;
    }
    
    try {
        int count = std::stoi(get_result_value(result.get(), 0, 0));
        spdlog::info("Verified user count: {}", count);
        return count;
    } catch (const std::exception& e) {
        spdlog::error("Error parsing verified user count: {}", e.what());
        return 0;
    }
}

std::vector<std::pair<std::string, int>> UserRepositoryLibpq::get_users_by_role_count() {
    std::string query = R"(
        SELECT account_type, COUNT(*) 
        FROM user_schema.users 
        WHERE is_deleted = false 
        GROUP BY account_type 
        ORDER BY account_type
    )";
    
    auto result = execute_query(query, {});
    if (!result) {
        spdlog::error("Failed to get users by role count");
        return {};
    }
    
    std::vector<std::pair<std::string, int>> role_counts;
    int row_count = PQntuples(result.get());
    
    for (int i = 0; i < row_count; ++i) {
        try {
            int account_type_int = std::stoi(get_result_value(result.get(), i, 0));
            int count = std::stoi(get_result_value(result.get(), i, 1));
            
            std::string role_name;
            switch (static_cast<models::AccountType>(account_type_int)) {
                case models::AccountType::PERSONAL: role_name = "personal"; break;
                case models::AccountType::BUSINESS: role_name = "business"; break;
                case models::AccountType::VERIFIED: role_name = "verified"; break;
                case models::AccountType::PREMIUM: role_name = "premium"; break;
                case models::AccountType::DEVELOPER: role_name = "developer"; break;
                default: role_name = "unknown"; break;
            }
            
            role_counts.emplace_back(role_name, count);
        } catch (const std::exception& e) {
            spdlog::error("Error parsing role count result row {}: {}", i, e.what());
            continue;
        }
    }
    
    spdlog::info("Retrieved role-based user counts: {} roles", role_counts.size());
    return role_counts;
}

std::vector<std::pair<std::string, int>> UserRepositoryLibpq::get_users_by_status_count() {
    std::string query = R"(
        SELECT status, COUNT(*) 
        FROM user_schema.users 
        WHERE is_deleted = false 
        GROUP BY status 
        ORDER BY status
    )";
    
    auto result = execute_query(query, {});
    if (!result) {
        spdlog::error("Failed to get users by status count");
        return {};
    }
    
    std::vector<std::pair<std::string, int>> status_counts;
    int row_count = PQntuples(result.get());
    
    for (int i = 0; i < row_count; ++i) {
        try {
            int status_int = std::stoi(get_result_value(result.get(), i, 0));
            int count = std::stoi(get_result_value(result.get(), i, 1));
            
            std::string status_name;
            switch (static_cast<models::UserStatus>(status_int)) {
                case models::UserStatus::ACTIVE: status_name = "active"; break;
                case models::UserStatus::INACTIVE: status_name = "inactive"; break;
                case models::UserStatus::SUSPENDED: status_name = "suspended"; break;
                case models::UserStatus::BANNED: status_name = "banned"; break;
                case models::UserStatus::PENDING: status_name = "pending"; break;
                default: status_name = "unknown"; break;
            }
            
            status_counts.emplace_back(status_name, count);
        } catch (const std::exception& e) {
            spdlog::error("Error parsing status count result row {}: {}", i, e.what());
            continue;
        }
    }
    
    spdlog::info("Retrieved status-based user counts: {} statuses", status_counts.size());
    return status_counts;
}

// Additional mapping methods
models::Profile UserRepositoryLibpq::map_result_to_user_profile(pg_result* result, int row) const {
    models::Profile profile;
    
    try {
        profile.user_id = get_result_value(result, row, 0);
        profile.bio = get_result_value(result, row, 1);
        profile.location = get_result_value(result, row, 2);
        profile.website = get_result_value(result, row, 3);
        profile.avatar_url = get_result_value(result, row, 4);
        profile.banner_url = get_result_value(result, row, 5);
        profile.timezone = get_result_value(result, row, 6);
        profile.language = get_result_value(result, row, 7);
        profile.created_at = db_string_to_timestamp(get_result_value(result, row, 8));
        profile.updated_at = db_string_to_timestamp(get_result_value(result, row, 9));
        
        // Additional user data from join
        profile.username = get_result_value(result, row, 10);
        profile.display_name = get_result_value(result, row, 11);
        profile.first_name = get_result_value(result, row, 12);
        profile.last_name = get_result_value(result, row, 13);
        
        int status_int = std::stoi(get_result_value(result, row, 14));
        profile.status = static_cast<models::UserStatus>(status_int);
        
        int account_type_int = std::stoi(get_result_value(result, row, 15));
        profile.account_type = static_cast<models::AccountType>(account_type_int);
        
        int privacy_level_int = std::stoi(get_result_value(result, row, 16));
        profile.privacy_level = static_cast<models::PrivacyLevel>(privacy_level_int);
        
        profile.is_verified = get_result_value(result, row, 17) == "true";
        profile.is_premium = get_result_value(result, row, 18) == "true";
        profile.is_developer = get_result_value(result, row, 19) == "true";
        
    } catch (const std::exception& e) {
        spdlog::error("Error mapping profile result row {}: {}", row, e.what());
        throw;
    }
    
    return profile;
}

models::Session UserRepositoryLibpq::map_result_to_user_session(pg_result* result, int row) const {
    models::Session session;
    
    try {
        session.session_id = get_result_value(result, row, 0);
        session.user_id = get_result_value(result, row, 1);
        session.token = get_result_value(result, row, 2);
        session.device_id = get_result_value(result, row, 3);
        session.device_name = get_result_value(result, row, 4);
        session.ip_address = get_result_value(result, row, 5);
        session.user_agent = get_result_value(result, row, 6);
        
        int session_type_int = std::stoi(get_result_value(result, row, 7));
        session.session_type = static_cast<models::SessionType>(session_type_int);
        
        session.created_at = db_string_to_timestamp(get_result_value(result, row, 8));
        session.last_activity = db_string_to_timestamp(get_result_value(result, row, 9));
        session.expires_at = db_string_to_timestamp(get_result_value(result, row, 10));
        session.is_active = get_result_value(result, row, 11) == "true";
        
    } catch (const std::exception& e) {
        spdlog::error("Error mapping session result row {}: {}", row, e.what());
        throw;
    }
    
    return session;
}

TwoFactorAuth UserRepositoryLibpq::map_result_to_two_factor_auth(pg_result* result, int row) const {
    TwoFactorAuth tfa;
    
    try {
        tfa.user_id = get_result_value(result, row, 0);
        tfa.secret_key = get_result_value(result, row, 1);
        tfa.backup_codes = get_result_value(result, row, 2);
        tfa.is_enabled = get_result_value(result, row, 3) == "true";
        tfa.created_at = db_string_to_timestamp(get_result_value(result, row, 4));
        tfa.updated_at = db_string_to_timestamp(get_result_value(result, row, 5));
        
    } catch (const std::exception& e) {
        spdlog::error("Error mapping 2FA result row {}: {}", row, e.what());
        throw;
    }
    
    return tfa;
}

PasswordResetToken UserRepositoryLibpq::map_result_to_password_reset_token(pg_result* result, int row) const {
    PasswordResetToken token;
    
    try {
        token.user_id = get_result_value(result, row, 0);
        token.token = get_result_value(result, row, 1);
        token.expires_at = db_string_to_timestamp(get_result_value(result, row, 2));
        token.created_at = db_string_to_timestamp(get_result_value(result, row, 3));
        
    } catch (const std::exception& e) {
        spdlog::error("Error mapping password reset token result row {}: {}", row, e.what());
        throw;
    }
    
    return token;
}

EmailVerificationToken UserRepositoryLibpq::map_result_to_email_verification_token(pg_result* result, int row) const {
    EmailVerificationToken token;
    
    try {
        token.user_id = get_result_value(result, row, 0);
        token.token = get_result_value(result, row, 1);
        token.expires_at = db_string_to_timestamp(get_result_value(result, row, 2));
        token.created_at = db_string_to_timestamp(get_result_value(result, row, 3));
        
    } catch (const std::exception& e) {
        spdlog::error("Error mapping email verification token result row {}: {}", row, e.what());
        throw;
    }
    
    return token;
}

UserSettings UserRepositoryLibpq::map_result_to_user_settings(pg_result* result, int row) const {
    UserSettings settings;
    
    try {
        settings.user_id = get_result_value(result, row, 0);
        settings.theme = get_result_value(result, row, 1);
        settings.language = get_result_value(result, row, 2);
        settings.timezone = get_result_value(result, row, 3);
        settings.notifications_enabled = get_result_value(result, row, 4) == "true";
        settings.email_notifications = get_result_value(result, row, 5) == "true";
        settings.push_notifications = get_result_value(result, row, 6) == "true";
        settings.sms_notifications = get_result_value(result, row, 7) == "true";
        
        int privacy_level_int = std::stoi(get_result_value(result, row, 8));
        settings.privacy_level = static_cast<models::PrivacyLevel>(privacy_level_int);
        
        settings.created_at = db_string_to_timestamp(get_result_value(result, row, 9));
        settings.updated_at = db_string_to_timestamp(get_result_value(result, row, 10));
        
    } catch (const std::exception& e) {
        spdlog::error("Error mapping user settings result row {}: {}", row, e.what());
        throw;
    }
    
    return settings;
}

UserStats UserRepositoryLibpq::map_result_to_user_stats(pg_result* result, int row) const {
    UserStats stats;
    
    try {
        stats.user_id = get_result_value(result, row, 0);
        stats.notes_count = std::stoi(get_result_value(result, row, 1));
        stats.followers_count = std::stoi(get_result_value(result, row, 2));
        stats.following_count = std::stoi(get_result_value(result, row, 3));
        stats.likes_received = std::stoi(get_result_value(result, row, 4));
        stats.comments_received = std::stoi(get_result_value(result, row, 5));
        stats.shares_received = std::stoi(get_result_value(result, row, 6));
        stats.total_views = std::stoi(get_result_value(result, row, 7));
        stats.total_engagement = std::stoi(get_result_value(result, row, 8));
        stats.last_activity = db_string_to_timestamp(get_result_value(result, row, 9));
        stats.created_at = db_string_to_timestamp(get_result_value(result, row, 10));
        stats.updated_at = db_string_to_timestamp(get_result_value(result, row, 11));
        
    } catch (const std::exception& e) {
        spdlog::error("Error mapping user stats result row {}: {}", row, e.what());
        throw;
    }
    
    return stats;
}

UserLoginHistory UserRepositoryLibpq::map_result_to_user_login_history(pg_result* result, int row) const {
    UserLoginHistory history;
    
    try {
        history.user_id = get_result_value(result, row, 0);
        history.session_id = get_result_value(result, row, 1);
        history.login_timestamp = db_string_to_timestamp(get_result_value(result, row, 2));
        
        std::string logout_str = get_result_value(result, row, 3);
        if (!logout_str.empty()) {
            history.logout_timestamp = db_string_to_timestamp(logout_str);
        }
        
        history.ip_address = get_result_value(result, row, 4);
        history.user_agent = get_result_value(result, row, 5);
        history.device_id = get_result_value(result, row, 6);
        history.device_name = get_result_value(result, row, 7);
        history.location = get_result_value(result, row, 8);
        history.success = get_result_value(result, row, 9) == "true";
        
        std::string failure_reason_str = get_result_value(result, row, 10);
        if (!failure_reason_str.empty()) {
            history.failure_reason = failure_reason_str;
        }
        
        history.created_at = db_string_to_timestamp(get_result_value(result, row, 11));
        
    } catch (const std::exception& e) {
        spdlog::error("Error mapping user login history result row {}: {}", row, e.what());
        throw;
    }
    
    return history;
}

} // namespace user
} // namespace sonet