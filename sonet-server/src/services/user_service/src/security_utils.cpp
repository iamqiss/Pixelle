/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "security_utils.h"
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <regex>
#include <algorithm>
#include <random>
#include <iomanip>
#include <sstream>
#include <cassert>

namespace sonet::user {

// Character sets for different purposes
const std::string SecurityUtils::ALPHANUMERIC_CHARSET = 
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const std::string SecurityUtils::SAFE_CHARSET = 
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
const std::string SecurityUtils::HEX_CHARSET = "0123456789abcdef";

std::string SecurityUtils::generate_secure_random_string(size_t length) {
    // I don't trust pseudo-random for security - only cryptographically secure
    std::vector<uint8_t> random_bytes = generate_secure_random_bytes(length);
    
    // Convert to safe characters
    std::string result;
    result.reserve(length);
    
    for (uint8_t byte : random_bytes) {
        result += SAFE_CHARSET[byte % SAFE_CHARSET.length()];
    }
    
    return result;
}

std::vector<uint8_t> SecurityUtils::generate_secure_random_bytes(size_t length) {
    std::vector<uint8_t> bytes(length);
    
    if (RAND_bytes(bytes.data(), static_cast<int>(length)) != 1) {
        throw std::runtime_error("Failed to generate secure random bytes");
    }
    
    return bytes;
}

std::string SecurityUtils::generate_uuid() {
    // Generate a UUID v4 - because I need unique identifiers
    std::vector<uint8_t> bytes = generate_secure_random_bytes(16);
    
    // Set version (4) and variant bits
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // Version 4
    bytes[8] = (bytes[8] & 0x3F) | 0x80; // Variant bits
    
    std::stringstream ss;
    ss << std::hex;
    
    for (size_t i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            ss << "-";
        }
        ss << std::setw(2) << std::setfill('0') << static_cast<int>(bytes[i]);
    }
    
    return ss.str();
}

std::string SecurityUtils::hash_string(const std::string& input) {
    // SHA-256 hash for general purposes
    return sha256(input);
}

std::string SecurityUtils::sha256(const std::string& input) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, input.c_str(), input.length());
    SHA256_Final(hash, &sha256);
    
    // Convert to hex string
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    
    return ss.str();
}

std::string SecurityUtils::hmac_sha256(const std::string& key, const std::string& data) {
    unsigned char result[EVP_MAX_MD_SIZE];
    unsigned int result_len;
    
    HMAC(EVP_sha256(), 
         key.c_str(), static_cast<int>(key.length()),
         reinterpret_cast<const unsigned char*>(data.c_str()), data.length(),
         result, &result_len);
    
    // Convert to hex string
    std::stringstream ss;
    for (unsigned int i = 0; i < result_len; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(result[i]);
    }
    
    return ss.str();
}

std::string SecurityUtils::base64_encode(const std::string& input) {
    return to_base64_impl(input, false);
}

std::string SecurityUtils::base64_decode(const std::string& input) {
    return from_base64_impl(input, false);
}

std::string SecurityUtils::base64_url_encode(const std::string& input) {
    return to_base64_impl(input, true);
}

std::string SecurityUtils::base64_url_decode(const std::string& input) {
    return from_base64_impl(input, true);
}

bool SecurityUtils::secure_compare(const std::string& a, const std::string& b) {
    // Constant-time comparison to prevent timing attacks
    if (a.length() != b.length()) {
        return false;
    }
    
    volatile char result = 0;
    for (size_t i = 0; i < a.length(); i++) {
        result |= a[i] ^ b[i];
    }
    
    return result == 0;
}

bool SecurityUtils::secure_compare(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
    if (a.size() != b.size()) {
        return false;
    }
    
    volatile uint8_t result = 0;
    for (size_t i = 0; i < a.size(); i++) {
        result |= a[i] ^ b[i];
    }
    
    return result == 0;
}

bool SecurityUtils::is_valid_email(const std::string& email) {
    // Basic email validation - in production I'd use a more sophisticated library
    const std::regex email_pattern(
        R"(^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$)"
    );
    
    return std::regex_match(email, email_pattern) && email.length() <= 320; // RFC 5321 limit
}

bool SecurityUtils::is_valid_username(const std::string& username) {
    // Username rules: 3-30 chars, alphanumeric plus underscore, no consecutive underscores
    if (username.length() < 3 || username.length() > 30) {
        return false;
    }
    
    const std::regex username_pattern(R"(^[a-zA-Z0-9]([a-zA-Z0-9_]*[a-zA-Z0-9])?$)");
    
    if (!std::regex_match(username, username_pattern)) {
        return false;
    }
    
    // No consecutive underscores
    return username.find("__") == std::string::npos;
}

bool SecurityUtils::is_safe_string(const std::string& input) {
    // Check for dangerous characters that could be used in injection attacks
    const std::string dangerous_chars = "<>\"'&;(){}[]\\|`$";
    
    return input.find_first_of(dangerous_chars) == std::string::npos;
}

std::string SecurityUtils::sanitize_string(const std::string& input) {
    // Remove or escape dangerous characters
    std::string result = input;
    
    // Remove control characters
    result.erase(std::remove_if(result.begin(), result.end(), 
                               [](char c) { return c < 32 && c != '\t' && c != '\n' && c != '\r'; }), 
                result.end());
    
    // Trim whitespace
    result.erase(result.begin(), std::find_if(result.begin(), result.end(), 
                                             [](unsigned char ch) { return !std::isspace(ch); }));
    result.erase(std::find_if(result.rbegin(), result.rend(), 
                             [](unsigned char ch) { return !std::isspace(ch); }).base(), 
                result.end());
    
    return result;
}

std::string SecurityUtils::create_device_fingerprint(const std::string& user_agent, 
                                                    const std::string& ip_address,
                                                    const std::string& accept_language) {
    // Create a device fingerprint from available headers
    std::string fingerprint_data = user_agent + "|" + ip_address + "|" + accept_language;
    return sha256(fingerprint_data);
}

bool SecurityUtils::is_private_ip(const std::string& ip_address) {
    // Robust check for IPv4 private ranges: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
    // Fallback to simple checks if parsing fails
    int a = -1, b = -1, c = -1, d = -1;
    char dot;
    std::stringstream ss(ip_address);
    if ((ss >> a >> dot >> b >> dot >> c >> dot >> d) && dot == '.') {
        if (a == 10) return true;
        if (a == 192 && b == 168) return true;
        if (a == 172 && b >= 16 && b <= 31) return true;
        return false;
    }
    
    // Non-IPv4 simple fallbacks (keep minimal behavior)
    return ip_address.rfind("fc", 0) == 0 ||  // IPv6 unique local (fc00::/7)
           ip_address.rfind("fd", 0) == 0;    // IPv6 unique local (fd00::/8 subset)
}

bool SecurityUtils::is_loopback_ip(const std::string& ip_address) {
    return ip_address == "127.0.0.1" || 
           ip_address == "::1" || 
           ip_address.starts_with("127.");
}

int64_t SecurityUtils::get_current_unix_timestamp() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

bool SecurityUtils::is_timestamp_recent(int64_t timestamp, std::chrono::seconds max_age) {
    int64_t current_time = get_current_unix_timestamp();
    return (current_time - timestamp) <= max_age.count();
}

std::string SecurityUtils::create_jwt_header() {
    // Standard JWT header for our tokens
    return base64_url_encode(R"({"alg":"HS256","typ":"JWT"})");
}

std::string SecurityUtils::create_jwt_signature(const std::string& header, 
                                               const std::string& payload, 
                                               const std::string& secret) {
    std::string signing_input = header + "." + payload;
    std::string signature = hmac_sha256(secret, signing_input);
    
    // Convert hex signature to bytes then base64url encode
    std::vector<uint8_t> signature_bytes = hex_decode(signature);
    std::string signature_str(signature_bytes.begin(), signature_bytes.end());
    
    return base64_url_encode(signature_str);
}

int SecurityUtils::calculate_password_strength(const std::string& password) {
    // Simple password strength scoring (0-100)
    int score = 0;
    
    // Length bonus
    score += std::min(25, static_cast<int>(password.length() * 2));
    
    // Character variety
    bool has_lower = false, has_upper = false, has_digit = false, has_special = false;
    
    for (char c : password) {
        if (std::islower(c)) has_lower = true;
        else if (std::isupper(c)) has_upper = true;
        else if (std::isdigit(c)) has_digit = true;
        else has_special = true;
    }
    
    score += (has_lower + has_upper + has_digit + has_special) * 10;
    
    // Entropy bonus
    std::set<char> unique_chars(password.begin(), password.end());
    score += std::min(20, static_cast<int>(unique_chars.size()));
    
    // Penalty for common patterns
    if (password.find("123") != std::string::npos ||
        password.find("abc") != std::string::npos ||
        password.find("password") != std::string::npos) {
        score -= 20;
    }
    
    return std::max(0, std::min(100, score));
}

std::string SecurityUtils::create_security_event_id() {
    return "sec_" + generate_uuid();
}

// Private implementation methods

std::string SecurityUtils::to_base64_impl(const std::string& input, bool url_safe) {
    BIO* bio = BIO_new(BIO_s_mem());
    BIO* b64 = BIO_new(BIO_f_base64());
    
    // Always avoid newlines to keep tokens compact and interoperable
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    
    bio = BIO_push(b64, bio);
    BIO_write(bio, input.c_str(), static_cast<int>(input.length()));
    BIO_flush(bio);
    
    BUF_MEM* buffer_ptr;
    BIO_get_mem_ptr(bio, &buffer_ptr);
    
    std::string result(buffer_ptr->data, buffer_ptr->length);
    
    BIO_free_all(bio);
    
    if (url_safe) {
        // Convert to URL-safe base64
        std::replace(result.begin(), result.end(), '+', '-');
        std::replace(result.begin(), result.end(), '/', '_');
        
        // Remove padding
        result.erase(std::find(result.begin(), result.end(), '='), result.end());
    }
    
    return result;
}

std::string SecurityUtils::from_base64_impl(const std::string& input, bool url_safe) {
    std::string padded_input = input;
    
    if (url_safe) {
        // Convert from URL-safe base64
        std::replace(padded_input.begin(), padded_input.end(), '-', '+');
        std::replace(padded_input.begin(), padded_input.end(), '_', '/');
        
        // Add padding if needed
        while (padded_input.length() % 4) {
            padded_input += '=';
        }
    }
    
    BIO* bio = BIO_new_mem_buf(padded_input.c_str(), static_cast<int>(padded_input.length()));
    BIO* b64 = BIO_new(BIO_f_base64());
    
    // Always avoid newlines to keep decoding consistent
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    
    bio = BIO_push(b64, bio);
    
    std::vector<char> buffer(padded_input.length());
    int decoded_length = BIO_read(bio, buffer.data(), static_cast<int>(buffer.size()));
    
    BIO_free_all(bio);
    
    if (decoded_length < 0) {
        throw std::runtime_error("Base64 decoding failed");
    }
    
    return std::string(buffer.data(), decoded_length);
}

std::vector<uint8_t> SecurityUtils::hex_decode(const std::string& hex) {
    std::vector<uint8_t> bytes;
    
    for (size_t i = 0; i < hex.length(); i += 2) {
        std::string byte_string = hex.substr(i, 2);
        uint8_t byte = static_cast<uint8_t>(std::stoi(byte_string, nullptr, 16));
        bytes.push_back(byte);
    }
    
    return bytes;
}

} // namespace sonet::user
