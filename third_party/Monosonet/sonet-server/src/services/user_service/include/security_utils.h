/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

namespace sonet::user {

/**
 * Security Utils - My collection of essential security functions
 * 
 * I've gathered all the cryptographic and security utilities here.
 * These are the building blocks that make everything else secure.
 * No shortcuts, no weak implementations - only battle-tested algorithms.
 */
class SecurityUtils {
public:
    // Random generation - the foundation of all security
    static std::string generate_secure_random_string(size_t length);
    static std::vector<uint8_t> generate_secure_random_bytes(size_t length);
    static std::string generate_uuid();
    
    // Hashing functions - because passwords need protection
    static std::string hash_string(const std::string& input);
    static std::string hash_with_salt(const std::string& input, const std::string& salt);
    static std::string sha256(const std::string& input);
    static std::string hmac_sha256(const std::string& key, const std::string& data);
    
    // Encoding utilities
    static std::string base64_encode(const std::string& input);
    static std::string base64_decode(const std::string& input);
    static std::string base64_url_encode(const std::string& input);
    static std::string base64_url_decode(const std::string& input);
    static std::string hex_encode(const std::vector<uint8_t>& input);
    static std::vector<uint8_t> hex_decode(const std::string& input);
    
    // Timing-safe operations - because timing attacks are real
    static bool secure_compare(const std::string& a, const std::string& b);
    static bool secure_compare(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b);
    
    // Input validation and sanitization
    static bool is_valid_email(const std::string& email);
    static bool is_valid_username(const std::string& username);
    static bool is_safe_string(const std::string& input);
    static std::string sanitize_string(const std::string& input);
    
    // Rate limiting helpers
    static std::string get_rate_limit_key(const std::string& prefix, const std::string& identifier);
    static bool is_rate_limited(const std::string& key, int max_requests, std::chrono::seconds window);
    
    // Device fingerprinting
    static std::string create_device_fingerprint(const std::string& user_agent, 
                                                const std::string& ip_address,
                                                const std::string& accept_language = "");
    
    // IP address utilities
    static bool is_private_ip(const std::string& ip_address);
    static bool is_loopback_ip(const std::string& ip_address);
    static std::string normalize_ip_address(const std::string& ip_address);
    
    // Time utilities for security
    static int64_t get_current_unix_timestamp();
    static bool is_timestamp_recent(int64_t timestamp, std::chrono::seconds max_age);
    static std::string format_security_timestamp(const std::chrono::system_clock::time_point& time_point);
    
    // JWT helpers
    static std::string create_jwt_header();
    static std::string encode_jwt_payload(const std::string& payload);
    static std::string create_jwt_signature(const std::string& header, 
                                           const std::string& payload, 
                                           const std::string& secret);
    
    // Password strength estimation
    static int calculate_password_strength(const std::string& password);
    static std::vector<std::string> get_password_weaknesses(const std::string& password);
    
    // Security event helpers
    static std::string create_security_event_id();
    static std::string hash_sensitive_data(const std::string& data);
    
private:
    // Internal helper functions
    static std::string to_base64_impl(const std::string& input, bool url_safe = false);
    static std::string from_base64_impl(const std::string& input, bool url_safe = false);
    static char random_char();
    static std::string get_random_string_from_charset(size_t length, const std::string& charset);
    
    // Character sets for random generation
    static const std::string ALPHANUMERIC_CHARSET;
    static const std::string SAFE_CHARSET;
    static const std::string HEX_CHARSET;
};

} // namespace sonet::user
