/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "user_types.h"
#include <string>
#include <optional>
#include <memory>

namespace sonet::user {

/**
 * JWT Manager - Modern stateless authentication
 * 
 * I'm using JWT for stateless auth because it scales beautifully.
 * But I'm not naive about security - proper validation, secure signing,
 * and all the best practices are baked in.
 */
class JWTManager {
public:
    JWTManager(const std::string& secret_key, const std::string& issuer = "sonet");
    ~JWTManager() = default;

    // Core JWT operations
    std::string generate_token(const JWTClaims& claims);
    std::optional<JWTClaims> verify_token(const std::string& token);
    
    // Token lifecycle management
    std::string generate_access_token(const User& user, const UserSession& session);
    std::string generate_refresh_token(const std::string& user_id, const std::string& session_id);
    
    // Specialized tokens for different use cases
    std::string generate_email_verification_token(const std::string& user_id);
    std::string generate_password_reset_token(const std::string& user_id);
    std::string generate_2fa_token(const std::string& user_id);
    
    // Token validation and introspection
    bool is_token_valid(const std::string& token);
    bool is_token_expired(const std::string& token);
    std::optional<std::string> get_user_id_from_token(const std::string& token);
    std::optional<std::string> get_session_id_from_token(const std::string& token);
    
    // Security features
    void blacklist_token(const std::string& token);
    bool is_token_blacklisted(const std::string& token);
    void rotate_signing_key(const std::string& new_secret);
    
    // Token configuration
    void set_access_token_lifetime(std::chrono::seconds lifetime);
    void set_refresh_token_lifetime(std::chrono::seconds lifetime);

private:
    std::string secret_key_;
    std::string issuer_;
    std::string audience_;
    
    // Token lifetimes - I keep these reasonable for security
    std::chrono::seconds access_token_lifetime_{std::chrono::hours(1)};   // 1 hour
    std::chrono::seconds refresh_token_lifetime_{std::chrono::days(30)};  // 30 days
    std::chrono::seconds verification_token_lifetime_{std::chrono::hours(24)}; // 24 hours
    
    // Token blacklist (in production, this goes to Redis)
    std::unordered_set<std::string> blacklisted_tokens_;
    
    // Helper methods
    std::string encode_header();
    std::string encode_payload(const JWTClaims& claims);
    std::string generate_signature(const std::string& header, const std::string& payload);
    bool verify_signature(const std::string& token);
    std::optional<nlohmann::json> decode_payload(const std::string& token);
    JWTClaims json_to_claims(const nlohmann::json& json);
    nlohmann::json claims_to_json(const JWTClaims& claims);
    
    // Security utilities
    std::string base64_url_encode(const std::string& input);
    std::string base64_url_decode(const std::string& input);
    std::string hmac_sha256(const std::string& key, const std::string& data);
    
    // Time utilities
    int64_t get_current_timestamp();
    std::chrono::system_clock::time_point timestamp_to_time_point(int64_t timestamp);
    int64_t time_point_to_timestamp(const std::chrono::system_clock::time_point& tp);
};

} // namespace sonet::user
