/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "profile_repository.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <iomanip>
#include <regex>
#include <algorithm>
#include <cmath>

namespace sonet::user::repositories {

// PostgreSQLProfileRepository implementation
PostgreSQLProfileRepository::PostgreSQLProfileRepository(std::shared_ptr<pqxx::connection> connection)
    : db_connection_(connection),
      profiles_table_("profiles"),
      social_links_table_("profile_social_links"),
      custom_fields_table_("profile_custom_fields"),
      profile_analytics_table_("profile_analytics"),
      profile_views_table_("profile_views") {
    
    ensure_connection();
    create_database_schema();
    setup_prepared_statements();
}

void PostgreSQLProfileRepository::ensure_connection() {
    if (!db_connection_ || !test_connection()) {
        reconnect_if_needed();
    }
}

void PostgreSQLProfileRepository::reconnect_if_needed() {
    try {
        if (db_connection_) {
            db_connection_.reset();
        }
        spdlog::info("Database connection reset for profile repository");
    } catch (const std::exception& e) {
        spdlog::error("Failed to reconnect profile database: {}", e.what());
        throw;
    }
}

bool PostgreSQLProfileRepository::test_connection() {
    try {
        if (!db_connection_) return false;
        pqxx::work txn(*db_connection_);
        txn.exec("SELECT 1");
        txn.commit();
        return true;
    } catch (const std::exception& e) {
        spdlog::warn("Profile database connection test failed: {}", e.what());
        return false;
    }
}

std::string PostgreSQLProfileRepository::build_select_query(const std::vector<std::string>& fields) const {
    std::stringstream query;
    query << "SELECT ";
    
    if (fields.empty()) {
        query << "p.*";
    } else {
        for (size_t i = 0; i < fields.size(); ++i) {
            if (i > 0) query << ", ";
            query << "p." << fields[i];
        }
    }
    
    query << " FROM " << profiles_table_ << " p";
    return query.str();
}

std::string PostgreSQLProfileRepository::build_insert_query(const Profile& profile) const {
    return R"(
        INSERT INTO profiles (
            profile_id, user_id, display_name, bio, location, website,
            avatar_url, banner_url, tagline, profession, company, education,
            pronouns, birth_date, birth_location, contact_email, contact_phone,
            theme_color, accent_color, background_image, show_birth_year,
            show_join_date, show_last_seen, visibility, is_featured,
            is_searchable, allow_indexing, is_identity_verified,
            is_address_verified, is_phone_verified, is_email_verified,
            verification_badge_type, completeness_score, created_at,
            updated_at, last_profile_update
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
            $29, $30, $31, $32, $33, $34, $35, $36
        )
    )";
}

std::string PostgreSQLProfileRepository::build_update_query(const Profile& profile) const {
    return R"(
        UPDATE profiles SET
            display_name = $3, bio = $4, location = $5, website = $6,
            avatar_url = $7, banner_url = $8, tagline = $9, profession = $10,
            company = $11, education = $12, pronouns = $13, birth_date = $14,
            birth_location = $15, contact_email = $16, contact_phone = $17,
            theme_color = $18, accent_color = $19, background_image = $20,
            show_birth_year = $21, show_join_date = $22, show_last_seen = $23,
            visibility = $24, is_featured = $25, is_searchable = $26,
            allow_indexing = $27, is_identity_verified = $28,
            is_address_verified = $29, is_phone_verified = $30,
            is_email_verified = $31, verification_badge_type = $32,
            completeness_score = $33, updated_at = $34, last_profile_update = $35
        WHERE profile_id = $1 AND user_id = $2
    )";
}

Profile PostgreSQLProfileRepository::map_row_to_profile(const pqxx::row& row) const {
    Profile profile;
    
    try {
        profile.profile_id = row["profile_id"].as<std::string>();
        profile.user_id = row["user_id"].as<std::string>();
        profile.display_name = row["display_name"].as<std::string>("");
        profile.bio = row["bio"].as<std::string>("");
        profile.location = row["location"].as<std::string>("");
        profile.website = row["website"].as<std::string>("");
        profile.avatar_url = row["avatar_url"].as<std::string>("");
        profile.banner_url = row["banner_url"].as<std::string>("");
        profile.tagline = row["tagline"].as<std::string>("");
        profile.profession = row["profession"].as<std::string>("");
        profile.company = row["company"].as<std::string>("");
        profile.education = row["education"].as<std::string>("");
        profile.pronouns = row["pronouns"].as<std::string>("");
        
        if (!row["birth_date"].is_null()) {
            profile.birth_date = row["birth_date"].as<std::time_t>();
        }
        
        profile.birth_location = row["birth_location"].as<std::string>("");
        profile.contact_email = row["contact_email"].as<std::string>("");
        profile.contact_phone = row["contact_phone"].as<std::string>("");
        profile.theme_color = row["theme_color"].as<std::string>("#1DA1F2");
        profile.accent_color = row["accent_color"].as<std::string>("#657786");
        profile.background_image = row["background_image"].as<std::string>("");
        profile.show_birth_year = row["show_birth_year"].as<bool>(false);
        profile.show_join_date = row["show_join_date"].as<bool>(true);
        profile.show_last_seen = row["show_last_seen"].as<bool>(true);
        
        profile.visibility = static_cast<ProfileVisibility>(row["visibility"].as<int>());
        profile.is_featured = row["is_featured"].as<bool>(false);
        profile.is_searchable = row["is_searchable"].as<bool>(true);
        profile.allow_indexing = row["allow_indexing"].as<bool>(true);
        profile.is_identity_verified = row["is_identity_verified"].as<bool>(false);
        profile.is_address_verified = row["is_address_verified"].as<bool>(false);
        profile.is_phone_verified = row["is_phone_verified"].as<bool>(false);
        profile.is_email_verified = row["is_email_verified"].as<bool>(false);
        profile.verification_badge_type = row["verification_badge_type"].as<std::string>("");
        profile.completeness_score = row["completeness_score"].as<double>(0.0);
        
        profile.created_at = row["created_at"].as<std::time_t>();
        profile.updated_at = row["updated_at"].as<std::time_t>();
        profile.last_profile_update = row["last_profile_update"].as<std::time_t>();
        
        // Load related data
        load_profile_relations(profile);
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to map database row to profile: {}", e.what());
        throw;
    }
    
    return profile;
}

std::vector<Profile> PostgreSQLProfileRepository::map_result_to_profiles(const pqxx::result& result) const {
    std::vector<Profile> profiles;
    profiles.reserve(result.size());
    
    for (const auto& row : result) {
        profiles.push_back(map_row_to_profile(row));
    }
    
    return profiles;
}

SocialLink PostgreSQLProfileRepository::map_row_to_social_link(const pqxx::row& row) const {
    SocialLink link;
    link.platform = row["platform"].as<std::string>();
    link.username = row["username"].as<std::string>();
    link.url = row["url"].as<std::string>();
    link.is_verified = row["is_verified"].as<bool>(false);
    link.visibility = static_cast<ProfileVisibility>(row["visibility"].as<int>());
    link.created_at = row["created_at"].as<std::time_t>();
    link.updated_at = row["updated_at"].as<std::time_t>();
    return link;
}

CustomProfileField PostgreSQLProfileRepository::map_row_to_custom_field(const pqxx::row& row) const {
    CustomProfileField field;
    field.field_id = row["field_id"].as<std::string>();
    field.label = row["label"].as<std::string>();
    field.value = row["value"].as<std::string>();
    field.type = static_cast<ProfileFieldType>(row["field_type"].as<int>());
    field.visibility = static_cast<ProfileVisibility>(row["visibility"].as<int>());
    field.display_order = row["display_order"].as<int>();
    field.is_verified = row["is_verified"].as<bool>(false);
    field.created_at = row["created_at"].as<std::time_t>();
    field.updated_at = row["updated_at"].as<std::time_t>();
    return field;
}

ProfileAnalytics PostgreSQLProfileRepository::map_row_to_analytics(const pqxx::row& row) const {
    ProfileAnalytics analytics;
    analytics.user_id = row["user_id"].as<std::string>();
    analytics.profile_views_today = row["profile_views_today"].as<int>(0);
    analytics.profile_views_week = row["profile_views_week"].as<int>(0);
    analytics.profile_views_month = row["profile_views_month"].as<int>(0);
    analytics.profile_views_total = row["profile_views_total"].as<int>(0);
    analytics.unique_visitors_today = row["unique_visitors_today"].as<int>(0);
    analytics.unique_visitors_week = row["unique_visitors_week"].as<int>(0);
    analytics.unique_visitors_month = row["unique_visitors_month"].as<int>(0);
    analytics.unique_visitors_total = row["unique_visitors_total"].as<int>(0);
    analytics.last_updated = row["last_updated"].as<std::time_t>();
    
    // Load recent visitors (stored as JSON array)
    if (!row["recent_visitors"].is_null()) {
        std::string visitors_json = row["recent_visitors"].as<std::string>();
        try {
            nlohmann::json j = nlohmann::json::parse(visitors_json);
            analytics.recent_visitors = j.get<std::vector<std::string>>();
        } catch (const std::exception& e) {
            spdlog::warn("Failed to parse recent visitors JSON: {}", e.what());
        }
    }
    
    return analytics;
}

void PostgreSQLProfileRepository::load_profile_relations(Profile& profile) const {
    try {
        // Load social links
        load_social_links(profile);
        
        // Load custom fields
        load_custom_fields(profile);
        
        // Load analytics
        load_analytics(profile);
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to load profile relations for user {}: {}", profile.user_id, e.what());
    }
}

void PostgreSQLProfileRepository::load_social_links(Profile& profile) const {
    try {
        pqxx::work txn(*db_connection_);
        
        auto result = txn.exec_params(
            "SELECT * FROM " + social_links_table_ + " WHERE user_id = $1 ORDER BY platform",
            profile.user_id
        );
        
        profile.social_links.clear();
        for (const auto& row : result) {
            profile.social_links.push_back(map_row_to_social_link(row));
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to load social links for user {}: {}", profile.user_id, e.what());
    }
}

void PostgreSQLProfileRepository::load_custom_fields(Profile& profile) const {
    try {
        pqxx::work txn(*db_connection_);
        
        auto result = txn.exec_params(
            "SELECT * FROM " + custom_fields_table_ + " WHERE user_id = $1 ORDER BY display_order",
            profile.user_id
        );
        
        profile.custom_fields.clear();
        for (const auto& row : result) {
            profile.custom_fields.push_back(map_row_to_custom_field(row));
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to load custom fields for user {}: {}", profile.user_id, e.what());
    }
}

void PostgreSQLProfileRepository::load_analytics(Profile& profile) const {
    try {
        pqxx::work txn(*db_connection_);
        
        auto result = txn.exec_params(
            "SELECT * FROM " + profile_analytics_table_ + " WHERE user_id = $1",
            profile.user_id
        );
        
        if (!result.empty()) {
            profile.analytics = map_row_to_analytics(result[0]);
        } else {
            // Initialize analytics if not found
            profile.analytics = ProfileAnalytics(profile.user_id);
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to load analytics for user {}: {}", profile.user_id, e.what());
        profile.analytics = ProfileAnalytics(profile.user_id);
    }
}

bool PostgreSQLProfileRepository::validate_profile_data(const Profile& profile) const {
    auto errors = profile.get_validation_errors();
    if (!errors.empty()) {
        spdlog::error("Profile validation failed: {}", nlohmann::json(errors).dump());
        return false;
    }
    return true;
}

void PostgreSQLProfileRepository::log_profile_operation(const std::string& operation, const std::string& profile_id) const {
    spdlog::info("Profile operation: {} for profile_id: {}", operation, profile_id);
}

bool PostgreSQLProfileRepository::create(const Profile& profile) {
    try {
        if (!validate_profile_data(profile)) {
            return false;
        }
        
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_insert_query(profile);
        
        // Execute insert with all parameters
        txn.exec_params(query,
            profile.profile_id, profile.user_id, profile.display_name, profile.bio,
            profile.location, profile.website, profile.avatar_url, profile.banner_url,
            profile.tagline, profile.profession, profile.company, profile.education,
            profile.pronouns, profile.birth_date.value_or(0), profile.birth_location,
            profile.contact_email, profile.contact_phone, profile.theme_color,
            profile.accent_color, profile.background_image, profile.show_birth_year,
            profile.show_join_date, profile.show_last_seen, static_cast<int>(profile.visibility),
            profile.is_featured, profile.is_searchable, profile.allow_indexing,
            profile.is_identity_verified, profile.is_address_verified,
            profile.is_phone_verified, profile.is_email_verified,
            profile.verification_badge_type, profile.completeness_score,
            profile.created_at, profile.updated_at, profile.last_profile_update
        );
        
        // Insert related data
        update_social_links(txn, profile);
        update_custom_fields(txn, profile);
        update_profile_analytics(txn, profile);
        
        txn.commit();
        log_profile_operation("CREATE", profile.profile_id);
        return true;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "create profile");
        return false;
    }
}

std::optional<Profile> PostgreSQLProfileRepository::get_by_id(const std::string& profile_id) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_select_query() + " WHERE p.profile_id = $1";
        auto result = txn.exec_params(query, profile_id);
        
        if (result.empty()) {
            return std::nullopt;
        }
        
        return map_row_to_profile(result[0]);
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get profile by id");
        return std::nullopt;
    }
}

std::optional<Profile> PostgreSQLProfileRepository::get_by_user_id(const std::string& user_id) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_select_query() + " WHERE p.user_id = $1";
        auto result = txn.exec_params(query, user_id);
        
        if (result.empty()) {
            return std::nullopt;
        }
        
        return map_row_to_profile(result[0]);
        
    } catch (const std::exception& e) {
        handle_database_error(e, "get profile by user id");
        return std::nullopt;
    }
}

bool PostgreSQLProfileRepository::update(const Profile& profile) {
    try {
        if (!validate_profile_data(profile)) {
            return false;
        }
        
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        auto query = build_update_query(profile);
        
        auto result = txn.exec_params(query,
            profile.profile_id, profile.user_id, profile.display_name, profile.bio,
            profile.location, profile.website, profile.avatar_url, profile.banner_url,
            profile.tagline, profile.profession, profile.company, profile.education,
            profile.pronouns, profile.birth_date.value_or(0), profile.birth_location,
            profile.contact_email, profile.contact_phone, profile.theme_color,
            profile.accent_color, profile.background_image, profile.show_birth_year,
            profile.show_join_date, profile.show_last_seen, static_cast<int>(profile.visibility),
            profile.is_featured, profile.is_searchable, profile.allow_indexing,
            profile.is_identity_verified, profile.is_address_verified,
            profile.is_phone_verified, profile.is_email_verified,
            profile.verification_badge_type, profile.completeness_score,
            profile.updated_at, profile.last_profile_update
        );
        
        if (result.affected_rows() == 0) {
            return false;
        }
        
        // Update related data
        update_social_links(txn, profile);
        update_custom_fields(txn, profile);
        update_profile_analytics(txn, profile);
        
        // Update search index
        update_search_index(profile);
        
        txn.commit();
        log_profile_operation("UPDATE", profile.profile_id);
        return true;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "update profile");
        return false;
    }
}

bool PostgreSQLProfileRepository::delete_profile(const std::string& profile_id) {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        // Delete related data first
        txn.exec_params("DELETE FROM " + social_links_table_ + " WHERE profile_id = $1", profile_id);
        txn.exec_params("DELETE FROM " + custom_fields_table_ + " WHERE profile_id = $1", profile_id);
        txn.exec_params("DELETE FROM " + profile_analytics_table_ + " WHERE profile_id = $1", profile_id);
        txn.exec_params("DELETE FROM " + profile_views_table_ + " WHERE profile_user_id = $1", profile_id);
        
        // Delete profile
        auto result = txn.exec_params("DELETE FROM " + profiles_table_ + " WHERE profile_id = $1", profile_id);
        
        // Remove from search index
        remove_from_search_index(profile_id);
        
        txn.commit();
        log_profile_operation("DELETE", profile_id);
        return result.affected_rows() > 0;
        
    } catch (const std::exception& e) {
        handle_database_error(e, "delete profile");
        return false;
    }
}

void PostgreSQLProfileRepository::update_social_links(pqxx::work& txn, const Profile& profile) {
    // Clear existing social links
    txn.exec_params("DELETE FROM " + social_links_table_ + " WHERE user_id = $1", profile.user_id);
    
    // Insert new social links
    for (const auto& link : profile.social_links) {
        txn.exec_params(
            "INSERT INTO " + social_links_table_ + " (user_id, platform, username, url, is_verified, visibility, created_at, updated_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            profile.user_id, link.platform, link.username, link.url,
            link.is_verified, static_cast<int>(link.visibility),
            link.created_at, link.updated_at
        );
    }
}

void PostgreSQLProfileRepository::update_custom_fields(pqxx::work& txn, const Profile& profile) {
    // Clear existing custom fields
    txn.exec_params("DELETE FROM " + custom_fields_table_ + " WHERE user_id = $1", profile.user_id);
    
    // Insert new custom fields
    for (const auto& field : profile.custom_fields) {
        txn.exec_params(
            "INSERT INTO " + custom_fields_table_ + " (field_id, user_id, label, value, field_type, visibility, display_order, is_verified, created_at, updated_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            field.field_id, profile.user_id, field.label, field.value,
            static_cast<int>(field.type), static_cast<int>(field.visibility),
            field.display_order, field.is_verified, field.created_at, field.updated_at
        );
    }
}

void PostgreSQLProfileRepository::update_profile_analytics(pqxx::work& txn, const Profile& profile) {
    // Convert recent visitors to JSON
    nlohmann::json visitors_json = profile.analytics.recent_visitors;
    
    // Upsert analytics
    txn.exec_params(
        "INSERT INTO " + profile_analytics_table_ + " (user_id, profile_views_today, profile_views_week, profile_views_month, profile_views_total, "
        "unique_visitors_today, unique_visitors_week, unique_visitors_month, unique_visitors_total, recent_visitors, last_updated) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) "
        "ON CONFLICT (user_id) DO UPDATE SET "
        "profile_views_today = EXCLUDED.profile_views_today, "
        "profile_views_week = EXCLUDED.profile_views_week, "
        "profile_views_month = EXCLUDED.profile_views_month, "
        "profile_views_total = EXCLUDED.profile_views_total, "
        "unique_visitors_today = EXCLUDED.unique_visitors_today, "
        "unique_visitors_week = EXCLUDED.unique_visitors_week, "
        "unique_visitors_month = EXCLUDED.unique_visitors_month, "
        "unique_visitors_total = EXCLUDED.unique_visitors_total, "
        "recent_visitors = EXCLUDED.recent_visitors, "
        "last_updated = EXCLUDED.last_updated",
        profile.analytics.user_id, profile.analytics.profile_views_today,
        profile.analytics.profile_views_week, profile.analytics.profile_views_month,
        profile.analytics.profile_views_total, profile.analytics.unique_visitors_today,
        profile.analytics.unique_visitors_week, profile.analytics.unique_visitors_month,
        profile.analytics.unique_visitors_total, visitors_json.dump(),
        profile.analytics.last_updated
    );
}

void PostgreSQLProfileRepository::setup_prepared_statements() {
    try {
        ensure_connection();
        
        // Prepare commonly used statements
        db_connection_->prepare("get_profile_by_user_id",
            build_select_query() + " WHERE p.user_id = $1");
        
        db_connection_->prepare("get_profile_by_id",
            build_select_query() + " WHERE p.profile_id = $1");
        
        spdlog::info("Prepared statements created for profile repository");
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to setup profile prepared statements: {}", e.what());
    }
}

void PostgreSQLProfileRepository::create_database_schema() {
    try {
        ensure_connection();
        pqxx::work txn(*db_connection_);
        
        // Create profiles table
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS profiles (
                profile_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255) UNIQUE NOT NULL,
                display_name VARCHAR(100),
                bio TEXT,
                location VARCHAR(100),
                website VARCHAR(255),
                avatar_url VARCHAR(500),
                banner_url VARCHAR(500),
                tagline VARCHAR(200),
                profession VARCHAR(100),
                company VARCHAR(100),
                education VARCHAR(200),
                pronouns VARCHAR(20),
                birth_date BIGINT,
                birth_location VARCHAR(100),
                contact_email VARCHAR(255),
                contact_phone VARCHAR(20),
                theme_color VARCHAR(7) DEFAULT '#1DA1F2',
                accent_color VARCHAR(7) DEFAULT '#657786',
                background_image VARCHAR(500),
                show_birth_year BOOLEAN DEFAULT FALSE,
                show_join_date BOOLEAN DEFAULT TRUE,
                show_last_seen BOOLEAN DEFAULT TRUE,
                visibility INTEGER DEFAULT 0,
                is_featured BOOLEAN DEFAULT FALSE,
                is_searchable BOOLEAN DEFAULT TRUE,
                allow_indexing BOOLEAN DEFAULT TRUE,
                is_identity_verified BOOLEAN DEFAULT FALSE,
                is_address_verified BOOLEAN DEFAULT FALSE,
                is_phone_verified BOOLEAN DEFAULT FALSE,
                is_email_verified BOOLEAN DEFAULT FALSE,
                verification_badge_type VARCHAR(50),
                completeness_score DOUBLE PRECISION DEFAULT 0.0,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                last_profile_update BIGINT NOT NULL
            )
        )");
        
        // Create social links table
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS profile_social_links (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                platform VARCHAR(50) NOT NULL,
                username VARCHAR(100) NOT NULL,
                url VARCHAR(500) NOT NULL,
                is_verified BOOLEAN DEFAULT FALSE,
                visibility INTEGER DEFAULT 0,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                UNIQUE(user_id, platform)
            )
        )");
        
        // Create custom fields table
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS profile_custom_fields (
                field_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                label VARCHAR(50) NOT NULL,
                value VARCHAR(500),
                field_type INTEGER DEFAULT 0,
                visibility INTEGER DEFAULT 0,
                display_order INTEGER DEFAULT 0,
                is_verified BOOLEAN DEFAULT FALSE,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL
            )
        )");
        
        // Create profile analytics table
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS profile_analytics (
                user_id VARCHAR(255) PRIMARY KEY,
                profile_views_today INTEGER DEFAULT 0,
                profile_views_week INTEGER DEFAULT 0,
                profile_views_month INTEGER DEFAULT 0,
                profile_views_total INTEGER DEFAULT 0,
                unique_visitors_today INTEGER DEFAULT 0,
                unique_visitors_week INTEGER DEFAULT 0,
                unique_visitors_month INTEGER DEFAULT 0,
                unique_visitors_total INTEGER DEFAULT 0,
                recent_visitors TEXT,
                last_updated BIGINT NOT NULL
            )
        )");
        
        // Create profile views table for detailed tracking
        txn.exec(R"(
            CREATE TABLE IF NOT EXISTS profile_views (
                id SERIAL PRIMARY KEY,
                profile_user_id VARCHAR(255) NOT NULL,
                viewer_id VARCHAR(255),
                ip_address VARCHAR(45),
                user_agent TEXT,
                referrer VARCHAR(500),
                viewed_at BIGINT NOT NULL,
                session_id VARCHAR(255)
            )
        )");
        
        // Create indexes for performance
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles(user_id)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profiles_visibility ON profiles(visibility)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profiles_searchable ON profiles(is_searchable)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profiles_featured ON profiles(is_featured)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profiles_verified ON profiles(is_identity_verified)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profiles_location ON profiles(location)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profiles_profession ON profiles(profession)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profiles_updated ON profiles(updated_at)");
        
        txn.exec("CREATE INDEX IF NOT EXISTS idx_social_links_user_id ON profile_social_links(user_id)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_social_links_platform ON profile_social_links(platform)");
        
        txn.exec("CREATE INDEX IF NOT EXISTS idx_custom_fields_user_id ON profile_custom_fields(user_id)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_custom_fields_order ON profile_custom_fields(display_order)");
        
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profile_views_user_id ON profile_views(profile_user_id)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profile_views_viewer ON profile_views(viewer_id)");
        txn.exec("CREATE INDEX IF NOT EXISTS idx_profile_views_time ON profile_views(viewed_at)");
        
        // Create full-text search index
        txn.exec(R"(
            CREATE INDEX IF NOT EXISTS idx_profiles_fulltext ON profiles 
            USING gin(to_tsvector('english', coalesce(display_name, '') || ' ' || 
                                            coalesce(bio, '') || ' ' || 
                                            coalesce(tagline, '') || ' ' ||
                                            coalesce(profession, '') || ' ' ||
                                            coalesce(company, '') || ' ' ||
                                            coalesce(location, '')))
        )");
        
        txn.commit();
        spdlog::info("Profile database schema created successfully");
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to create profile database schema: {}", e.what());
        throw;
    }
}

void PostgreSQLProfileRepository::handle_database_error(const std::exception& e, const std::string& operation) const {
    spdlog::error("Profile database error during {}: {}", operation, e.what());
}

std::string PostgreSQLProfileRepository::build_full_text_search_query(const std::vector<std::string>& keywords) const {
    if (keywords.empty()) {
        return "";
    }
    
    std::stringstream search_query;
    for (size_t i = 0; i < keywords.size(); ++i) {
        if (i > 0) search_query << " & ";
        search_query << keywords[i] << ":*";
    }
    
    return search_query.str();
}

void PostgreSQLProfileRepository::update_search_index(const Profile& profile) {
    // In a real implementation, this would update external search indices
    // For now, we rely on postgresql's built-in full-text search
    spdlog::debug("Updated search index for profile: {}", profile.profile_id);
}

void PostgreSQLProfileRepository::remove_from_search_index(const std::string& profile_id) {
    // Remove from external search indices if needed
    spdlog::debug("Removed profile from search index: {}", profile_id);
}

// Stub implementations for remaining interface methods
std::vector<Profile> PostgreSQLProfileRepository::get_by_user_ids(const std::vector<std::string>& user_ids) {
    // Implementation would batch fetch profiles
    return {};
}

bool PostgreSQLProfileRepository::update_multiple(const std::vector<Profile>& profiles) {
    // Implementation would batch update profiles
    return false;
}

SearchResult<Profile> PostgreSQLProfileRepository::search(const ProfileSearchCriteria& criteria) {
    // Implementation would build dynamic search query
    return SearchResult<Profile>{};
}

std::vector<Profile> PostgreSQLProfileRepository::get_featured_profiles(int limit, int offset) {
    // Implementation would get featured profiles
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::get_verified_profiles(int limit, int offset) {
    // Implementation would get verified profiles
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::get_profiles_by_location(const std::string& location, int limit) {
    // Implementation would search by location
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::get_profiles_by_profession(const std::string& profession, int limit) {
    // Implementation would search by profession
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::get_recently_updated(int limit, int hours_back) {
    // Implementation would get recently updated profiles
    return {};
}

bool PostgreSQLProfileRepository::update_avatar(const std::string& user_id, const std::string& avatar_url) {
    // Implementation would update avatar URL
    return false;
}

bool PostgreSQLProfileRepository::update_banner(const std::string& user_id, const std::string& banner_url) {
    // Implementation would update banner URL
    return false;
}

bool PostgreSQLProfileRepository::remove_avatar(const std::string& user_id) {
    // Implementation would remove avatar
    return false;
}

bool PostgreSQLProfileRepository::remove_banner(const std::string& user_id) {
    // Implementation would remove banner
    return false;
}

bool PostgreSQLProfileRepository::add_social_link(const std::string& user_id, const SocialLink& link) {
    // Implementation would add social link
    return false;
}

bool PostgreSQLProfileRepository::update_social_link(const std::string& user_id, const SocialLink& link) {
    // Implementation would update social link
    return false;
}

bool PostgreSQLProfileRepository::remove_social_link(const std::string& user_id, const std::string& platform) {
    // Implementation would remove social link
    return false;
}

std::vector<SocialLink> PostgreSQLProfileRepository::get_social_links(const std::string& user_id) {
    // Implementation would get social links
    return {};
}

bool PostgreSQLProfileRepository::add_custom_field(const std::string& user_id, const CustomProfileField& field) {
    // Implementation would add custom field
    return false;
}

bool PostgreSQLProfileRepository::update_custom_field(const std::string& user_id, const CustomProfileField& field) {
    // Implementation would update custom field
    return false;
}

bool PostgreSQLProfileRepository::remove_custom_field(const std::string& user_id, const std::string& field_id) {
    // Implementation would remove custom field
    return false;
}

bool PostgreSQLProfileRepository::reorder_custom_fields(const std::string& user_id, const std::vector<std::string>& field_order) {
    // Implementation would reorder custom fields
    return false;
}

std::vector<CustomProfileField> PostgreSQLProfileRepository::get_custom_fields(const std::string& user_id) {
    // Implementation would get custom fields
    return {};
}

bool PostgreSQLProfileRepository::record_profile_view(const std::string& user_id, const std::string& viewer_id, const std::string& source) {
    // Implementation would record profile view
    return false;
}

ProfileAnalytics PostgreSQLProfileRepository::get_profile_analytics(const std::string& user_id) {
    // Implementation would get analytics
    return ProfileAnalytics{};
}

bool PostgreSQLProfileRepository::update_profile_analytics(const std::string& user_id, const ProfileAnalytics& analytics) {
    // Implementation would update analytics
    return false;
}

std::vector<std::string> PostgreSQLProfileRepository::get_recent_profile_visitors(const std::string& user_id, int limit) {
    // Implementation would get recent visitors
    return {};
}

bool PostgreSQLProfileRepository::set_verification_status(const std::string& user_id, const std::string& badge_type, bool verified) {
    // Implementation would set verification status
    return false;
}

bool PostgreSQLProfileRepository::is_profile_verified(const std::string& user_id) {
    // Implementation would check verification status
    return false;
}

std::string PostgreSQLProfileRepository::get_verification_badge(const std::string& user_id) {
    // Implementation would get verification badge
    return "";
}

bool PostgreSQLProfileRepository::update_visibility(const std::string& user_id, ProfileVisibility visibility) {
    // Implementation would update visibility
    return false;
}

ProfileVisibility PostgreSQLProfileRepository::get_visibility(const std::string& user_id) {
    // Implementation would get visibility
    return ProfileVisibility::PUBLIC;
}

bool PostgreSQLProfileRepository::is_profile_visible_to(const std::string& user_id, const std::string& viewer_id) {
    // Implementation would check visibility
    return false;
}

double PostgreSQLProfileRepository::calculate_profile_completeness(const std::string& user_id) {
    // Implementation would calculate completeness
    return 0.0;
}

std::vector<std::string> PostgreSQLProfileRepository::get_missing_profile_fields(const std::string& user_id) {
    // Implementation would get missing fields
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::get_incomplete_profiles(double threshold, int limit) {
    // Implementation would get incomplete profiles
    return {};
}

int PostgreSQLProfileRepository::count_total_profiles() {
    // Implementation would count profiles
    return 0;
}

int PostgreSQLProfileRepository::count_public_profiles() {
    // Implementation would count public profiles
    return 0;
}

int PostgreSQLProfileRepository::count_verified_profiles() {
    // Implementation would count verified profiles
    return 0;
}

std::map<std::string, int> PostgreSQLProfileRepository::get_profile_completion_stats() {
    // Implementation would get completion stats
    return {};
}

std::map<std::string, int> PostgreSQLProfileRepository::get_verification_stats() {
    // Implementation would get verification stats
    return {};
}

bool PostgreSQLProfileRepository::cleanup_inactive_profiles(int months_inactive) {
    // Implementation would cleanup inactive profiles
    return false;
}

bool PostgreSQLProfileRepository::optimize_profile_search() {
    // Implementation would optimize search
    return false;
}

ProfileMaintenanceResult PostgreSQLProfileRepository::perform_maintenance() {
    // Implementation would perform maintenance
    return ProfileMaintenanceResult{};
}

std::vector<Profile> PostgreSQLProfileRepository::search_by_keywords(const std::vector<std::string>& keywords, int limit) {
    // Implementation would search by keywords
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::find_similar_profiles(const std::string& user_id, int limit) {
    // Implementation would find similar profiles
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::get_trending_profiles(int hours_back, int limit) {
    // Implementation would get trending profiles
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::get_recommended_profiles(const std::string& user_id, int limit) {
    // Implementation would get recommendations
    return {};
}

std::vector<Profile> PostgreSQLProfileRepository::get_profiles_to_follow(const std::string& user_id, int limit) {
    // Implementation would get follow suggestions
    return {};
}

// ProfileViewTracker implementation stubs
ProfileViewTracker::ProfileViewTracker(std::shared_ptr<pqxx::connection> connection)
    : db_connection_(connection), views_table_("profile_views") {
}

bool ProfileViewTracker::track_view(const std::string& profile_user_id, const std::string& viewer_id,
                                   const std::string& ip_address, const std::string& user_agent,
                                   const std::string& referrer) {
    return false;
}

bool ProfileViewTracker::is_duplicate_view(const std::string& profile_user_id, const std::string& viewer_id,
                                          int minutes_window) {
    return false;
}

int ProfileViewTracker::get_view_count(const std::string& profile_user_id, int hours_back) {
    return 0;
}

std::vector<std::string> ProfileViewTracker::get_recent_viewers(const std::string& profile_user_id, int limit) {
    return {};
}

std::map<std::string, int> ProfileViewTracker::get_view_statistics(const std::string& profile_user_id, int days_back) {
    return {};
}

bool ProfileViewTracker::cleanup_old_views(int days_old) {
    return false;
}

// ProfileRecommendationEngine implementation stubs
ProfileRecommendationEngine::ProfileRecommendationEngine(std::shared_ptr<pqxx::connection> connection)
    : db_connection_(connection) {
}

std::vector<Profile> ProfileRecommendationEngine::get_recommendations(const std::string& user_id, int limit) {
    return {};
}

std::vector<Profile> ProfileRecommendationEngine::get_location_based_recommendations(const std::string& user_id, int limit) {
    return {};
}

std::vector<Profile> ProfileRecommendationEngine::get_interest_based_recommendations(const std::string& user_id, int limit) {
    return {};
}

std::vector<Profile> ProfileRecommendationEngine::get_mutual_connection_recommendations(const std::string& user_id, int limit) {
    return {};
}

std::vector<Profile> ProfileRecommendationEngine::get_trending_recommendations(int hours_back, int limit) {
    return {};
}

double ProfileRecommendationEngine::calculate_location_similarity(const Profile& profile1, const Profile& profile2) {
    return 0.0;
}

double ProfileRecommendationEngine::calculate_interest_similarity(const Profile& profile1, const Profile& profile2) {
    return 0.0;
}

double ProfileRecommendationEngine::calculate_social_similarity(const std::string& user_id1, const std::string& user_id2) {
    return 0.0;
}

std::vector<ProfileRecommendationEngine::RecommendationScore> ProfileRecommendationEngine::score_candidates(
    const std::string& user_id, const std::vector<std::string>& candidate_ids) {
    return {};
}

// ProfileRepositoryFactory implementation
std::unique_ptr<PostgreSQLProfileRepository> ProfileRepositoryFactory::create_profile_repository(
    const std::string& connection_string) {
    auto connection = create_database_connection(connection_string);
    return std::make_unique<PostgreSQLProfileRepository>(connection);
}

std::unique_ptr<ProfileViewTracker> ProfileRepositoryFactory::create_view_tracker(
    const std::string& connection_string) {
    auto connection = create_database_connection(connection_string);
    return std::make_unique<ProfileViewTracker>(connection);
}

std::unique_ptr<ProfileRecommendationEngine> ProfileRepositoryFactory::create_recommendation_engine(
    const std::string& connection_string) {
    auto connection = create_database_connection(connection_string);
    return std::make_unique<ProfileRecommendationEngine>(connection);
}

std::shared_ptr<pqxx::connection> ProfileRepositoryFactory::create_database_connection(
    const std::string& connection_string) {
    try {
        auto connection = std::make_shared<pqxx::connection>(connection_string);
        spdlog::info("Profile database connection established successfully");
        return connection;
    } catch (const std::exception& e) {
        spdlog::error("Failed to create profile database connection: {}", e.what());
        throw;
    }
}

} // namespace sonet::user::repositories