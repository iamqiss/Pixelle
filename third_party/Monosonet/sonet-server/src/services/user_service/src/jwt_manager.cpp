/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "jwt_manager.h"
#include "security_utils.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <sstream>

namespace sonet::user {

JWTManager::JWTManager(const std::string& secret_key, const std::string& issuer)
    : secret_key_(secret_key), issuer_(issuer), audience_("sonet-users") {
    
    if (secret_key.length() < 32) {
        spdlog::error("JWT secret key is too short - this is a security risk!");
        throw std::invalid_argument("JWT secret key must be at least 32 characters");
    }
    
    spdlog::info("JWT manager initialized with secure key and issuer: {}", issuer);
}

std::string JWTManager::generate_token(const JWTClaims& claims) {
    try {
        // Create the header
        std::string header = SecurityUtils::create_jwt_header();
        
        // Create the payload
        nlohmann::json payload_json = claims_to_json(claims);
        std::string payload = SecurityUtils::base64_url_encode(payload_json.dump());
        
        // Create the signature
        std::string signature = SecurityUtils::create_jwt_signature(header, payload, secret_key_);
        
        // Combine everything
        return header + "." + payload + "." + signature;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to generate JWT token: {}", e.what());
        throw std::runtime_error("Token generation failed");
    }
}

std::optional<JWTClaims> JWTManager::verify_token(const std::string& token) {
    try {
        // Split the token into parts
        std::vector<std::string> parts;
        std::stringstream ss(token);
        std::string part;
        
        while (std::getline(ss, part, '.')) {
            parts.push_back(part);
        }
        
        if (parts.size() != 3) {
            spdlog::warn("Invalid JWT token format: wrong number of parts");
            return std::nullopt;
        }
        
        const std::string& header = parts[0];
        const std::string& payload = parts[1];
        const std::string& signature = parts[2];
        
        // Verify the signature first - no point checking anything else if it's wrong
        std::string expected_signature = SecurityUtils::create_jwt_signature(header, payload, secret_key_);
        if (!SecurityUtils::secure_compare(signature, expected_signature)) {
            spdlog::warn("JWT token signature verification failed");
            return std::nullopt;
        }
        
        // Check if token is blacklisted
        if (is_token_blacklisted(token)) {
            spdlog::warn("Attempted to use blacklisted JWT token");
            return std::nullopt;
        }
        
        // Decode and validate the payload
        std::string payload_json_str = SecurityUtils::base64_url_decode(payload);
        nlohmann::json payload_json = nlohmann::json::parse(payload_json_str);
        
        JWTClaims claims = json_to_claims(payload_json);
        
        // Validate the claims
        auto now = std::chrono::system_clock::now();
        if (now > claims.expires_at) {
            spdlog::debug("JWT token has expired");
            return std::nullopt;
        }
        
        if (claims.issuer != issuer_) {
            spdlog::warn("JWT token has invalid issuer: {}", claims.issuer);
            return std::nullopt;
        }
        
        if (claims.audience != audience_) {
            spdlog::warn("JWT token has invalid audience: {}", claims.audience);
            return std::nullopt;
        }
        
        return claims;
        
    } catch (const std::exception& e) {
        spdlog::error("JWT token verification failed: {}", e.what());
        return std::nullopt;
    }
}

std::string JWTManager::generate_access_token(const User& user, const UserSession& session) {
    JWTClaims claims;
    claims.user_id = user.user_id;
    claims.username = user.username;
    claims.email = user.email;
    claims.session_id = session.session_id;
    claims.session_type = session.type;
    claims.device_fingerprint = session.device_id;
    claims.ip_address = session.ip_address;
    claims.issuer = issuer_;
    claims.audience = audience_;
    
    auto now = std::chrono::system_clock::now();
    claims.issued_at = now;
    claims.expires_at = now + access_token_lifetime_;
    
    // Add roles based on user status and verification
    claims.roles.push_back("user");
    if (user.is_verified) {
        claims.roles.push_back("verified");
    }
    if (user.status == UserStatus::ACTIVE) {
        claims.roles.push_back("active");
    }
    
    // Security flags
    claims.requires_2fa = false; // Would be determined by user settings and risk assessment
    
    return generate_token(claims);
}

std::string JWTManager::generate_refresh_token(const std::string& user_id, const std::string& session_id) {
    JWTClaims claims;
    claims.user_id = user_id;
    claims.session_id = session_id;
    claims.issuer = issuer_;
    claims.audience = audience_;
    
    auto now = std::chrono::system_clock::now();
    claims.issued_at = now;
    claims.expires_at = now + refresh_token_lifetime_;
    
    // Refresh tokens have minimal claims for security
    claims.roles.push_back("refresh");
    
    return generate_token(claims);
}

std::string JWTManager::generate_email_verification_token(const std::string& user_id) {
    JWTClaims claims;
    claims.user_id = user_id;
    claims.issuer = issuer_;
    claims.audience = audience_;
    
    auto now = std::chrono::system_clock::now();
    claims.issued_at = now;
    claims.expires_at = now + verification_token_lifetime_;
    
    claims.roles.push_back("email_verification");
    
    return generate_token(claims);
}

std::string JWTManager::generate_password_reset_token(const std::string& user_id) {
    JWTClaims claims;
    claims.user_id = user_id;
    claims.issuer = issuer_;
    claims.audience = audience_;
    
    auto now = std::chrono::system_clock::now();
    claims.issued_at = now;
    claims.expires_at = now + std::chrono::hours(1); // Password reset tokens expire quickly
    
    claims.roles.push_back("password_reset");
    
    return generate_token(claims);
}

bool JWTManager::is_token_valid(const std::string& token) {
    return verify_token(token).has_value();
}

bool JWTManager::is_token_expired(const std::string& token) {
    auto claims = verify_token(token);
    if (!claims) {
        return true; // Invalid tokens are considered "expired"
    }
    
    return std::chrono::system_clock::now() > claims->expires_at;
}

std::optional<std::string> JWTManager::get_user_id_from_token(const std::string& token) {
    auto claims = verify_token(token);
    if (claims) {
        return claims->user_id;
    }
    return std::nullopt;
}

std::optional<std::string> JWTManager::get_session_id_from_token(const std::string& token) {
    auto claims = verify_token(token);
    if (claims) {
        return claims->session_id;
    }
    return std::nullopt;
}

void JWTManager::blacklist_token(const std::string& token) {
    blacklisted_tokens_.insert(SecurityUtils::sha256(token)); // Store hash for privacy
    spdlog::info("Token added to blacklist");
}

bool JWTManager::is_token_blacklisted(const std::string& token) {
    std::string token_hash = SecurityUtils::sha256(token);
    return blacklisted_tokens_.find(token_hash) != blacklisted_tokens_.end();
}

void JWTManager::rotate_signing_key(const std::string& new_secret) {
    if (new_secret.length() < 32) {
        throw std::invalid_argument("New secret key must be at least 32 characters");
    }
    
    // In production, you'd want to support multiple keys during rotation period
    secret_key_ = new_secret;
    spdlog::info("JWT signing key rotated successfully");
}

// Private helper methods

nlohmann::json JWTManager::claims_to_json(const JWTClaims& claims) {
    nlohmann::json json;
    
    // Standard JWT claims
    json["sub"] = claims.user_id;    // Subject
    json["iss"] = claims.issuer;     // Issuer
    json["aud"] = claims.audience;   // Audience
    json["iat"] = time_point_to_timestamp(claims.issued_at);   // Issued at
    json["exp"] = time_point_to_timestamp(claims.expires_at);  // Expires at
    
    // Custom claims
    json["username"] = claims.username;
    json["email"] = claims.email;
    json["roles"] = claims.roles;
    json["session_id"] = claims.session_id;
    json["session_type"] = static_cast<int>(claims.session_type);
    json["device_fingerprint"] = claims.device_fingerprint;
    json["ip_address"] = claims.ip_address;
    json["requires_2fa"] = claims.requires_2fa;
    
    return json;
}

JWTClaims JWTManager::json_to_claims(const nlohmann::json& json) {
    JWTClaims claims;
    
    // Standard claims
    claims.user_id = json.value("sub", "");
    claims.issuer = json.value("iss", "");
    claims.audience = json.value("aud", "");
    claims.issued_at = timestamp_to_time_point(json.value("iat", 0));
    claims.expires_at = timestamp_to_time_point(json.value("exp", 0));
    
    // Custom claims
    claims.username = json.value("username", "");
    claims.email = json.value("email", "");
    
    if (json.contains("roles") && json["roles"].is_array()) {
        for (const auto& role : json["roles"]) {
            claims.roles.push_back(role.get<std::string>());
        }
    }
    
    claims.session_id = json.value("session_id", "");
    claims.session_type = static_cast<SessionType>(json.value("session_type", 0));
    claims.device_fingerprint = json.value("device_fingerprint", "");
    claims.ip_address = json.value("ip_address", "");
    claims.requires_2fa = json.value("requires_2fa", false);
    
    return claims;
}

int64_t JWTManager::get_current_timestamp() {
    return SecurityUtils::get_current_unix_timestamp();
}

std::chrono::system_clock::time_point JWTManager::timestamp_to_time_point(int64_t timestamp) {
    return std::chrono::system_clock::from_time_t(static_cast<std::time_t>(timestamp));
}

int64_t JWTManager::time_point_to_timestamp(const std::chrono::system_clock::time_point& tp) {
    return std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
}

void JWTManager::set_access_token_lifetime(std::chrono::seconds lifetime) {
    access_token_lifetime_ = lifetime;
    spdlog::info("Access token lifetime set to {} seconds", lifetime.count());
}

void JWTManager::set_refresh_token_lifetime(std::chrono::seconds lifetime) {
    refresh_token_lifetime_ = lifetime;
    spdlog::info("Refresh token lifetime set to {} seconds", lifetime.count());
}

} // namespace sonet::user
