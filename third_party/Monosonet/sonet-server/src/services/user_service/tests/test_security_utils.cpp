/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include <gtest/gtest.h>
#include "security_utils.h"
#include <string>
#include <regex>

using namespace sonet::user;

class SecurityUtilsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Any setup needed for security utils tests
    }
};

TEST_F(SecurityUtilsTest, GenerateRandomStringCreatesCorrectLength) {
    std::string random_str = SecurityUtils::generate_random_string(32);
    EXPECT_EQ(random_str.length(), 32);
    
    std::string longer_str = SecurityUtils::generate_random_string(64);
    EXPECT_EQ(longer_str.length(), 64);
}

TEST_F(SecurityUtilsTest, GenerateRandomStringCreatesUniqueValues) {
    std::string str1 = SecurityUtils::generate_random_string(32);
    std::string str2 = SecurityUtils::generate_random_string(32);
    std::string str3 = SecurityUtils::generate_random_string(32);
    
    EXPECT_NE(str1, str2);
    EXPECT_NE(str2, str3);
    EXPECT_NE(str1, str3);
}

TEST_F(SecurityUtilsTest, GenerateSecureRandomBytesCreatesCorrectLength) {
    std::vector<uint8_t> bytes = SecurityUtils::generate_secure_random_bytes(16);
    EXPECT_EQ(bytes.size(), 16);
    
    std::vector<uint8_t> longer_bytes = SecurityUtils::generate_secure_random_bytes(32);
    EXPECT_EQ(longer_bytes.size(), 32);
}

TEST_F(SecurityUtilsTest, GenerateSecureRandomBytesCreatesUniqueValues) {
    auto bytes1 = SecurityUtils::generate_secure_random_bytes(16);
    auto bytes2 = SecurityUtils::generate_secure_random_bytes(16);
    auto bytes3 = SecurityUtils::generate_secure_random_bytes(16);
    
    EXPECT_NE(bytes1, bytes2);
    EXPECT_NE(bytes2, bytes3);
    EXPECT_NE(bytes1, bytes3);
}

TEST_F(SecurityUtilsTest, Base64EncodeAndDecode) {
    std::string original = "Hello, World! This is a test message.";
    std::string encoded = SecurityUtils::base64_encode(original);
    std::string decoded = SecurityUtils::base64_decode(encoded);
    
    EXPECT_NE(original, encoded);  // Should be different when encoded
    EXPECT_EQ(original, decoded);  // Should match when decoded
}

TEST_F(SecurityUtilsTest, Base64UrlEncodeAndDecode) {
    std::string original = "This is a URL-safe base64 test with special chars: +/=";
    std::string encoded = SecurityUtils::base64_url_encode(original);
    std::string decoded = SecurityUtils::base64_url_decode(encoded);
    
    // URL-safe base64 should not contain +, /, or = characters
    EXPECT_EQ(encoded.find('+'), std::string::npos);
    EXPECT_EQ(encoded.find('/'), std::string::npos);
    EXPECT_EQ(encoded.find('='), std::string::npos);
    
    EXPECT_EQ(original, decoded);
}

TEST_F(SecurityUtilsTest, SHA256HashCreatesConsistentResults) {
    std::string input = "test input for hashing";
    std::string hash1 = SecurityUtils::sha256(input);
    std::string hash2 = SecurityUtils::sha256(input);
    
    EXPECT_EQ(hash1, hash2);  // Same input should produce same hash
    EXPECT_EQ(hash1.length(), 64);  // SHA256 produces 64-character hex string
}

TEST_F(SecurityUtilsTest, SHA256HashCreatesDifferentResultsForDifferentInputs) {
    std::string input1 = "first input";
    std::string input2 = "second input";
    
    std::string hash1 = SecurityUtils::sha256(input1);
    std::string hash2 = SecurityUtils::sha256(input2);
    
    EXPECT_NE(hash1, hash2);
}

TEST_F(SecurityUtilsTest, HMACSHA256CreatesConsistentResults) {
    std::string data = "test data for HMAC";
    std::string key = "secret key";
    
    std::string hmac1 = SecurityUtils::hmac_sha256(data, key);
    std::string hmac2 = SecurityUtils::hmac_sha256(data, key);
    
    EXPECT_EQ(hmac1, hmac2);
    EXPECT_EQ(hmac1.length(), 64);  // HMAC-SHA256 produces 64-character hex string
}

TEST_F(SecurityUtilsTest, HMACSHA256CreatesDifferentResultsForDifferentKeys) {
    std::string data = "test data";
    std::string key1 = "first key";
    std::string key2 = "second key";
    
    std::string hmac1 = SecurityUtils::hmac_sha256(data, key1);
    std::string hmac2 = SecurityUtils::hmac_sha256(data, key2);
    
    EXPECT_NE(hmac1, hmac2);
}

TEST_F(SecurityUtilsTest, SecureCompareWithSameStrings) {
    std::string str1 = "identical string";
    std::string str2 = "identical string";
    
    EXPECT_TRUE(SecurityUtils::secure_compare(str1, str2));
}

TEST_F(SecurityUtilsTest, SecureCompareWithDifferentStrings) {
    std::string str1 = "first string";
    std::string str2 = "second string";
    
    EXPECT_FALSE(SecurityUtils::secure_compare(str1, str2));
}

TEST_F(SecurityUtilsTest, SecureCompareWithDifferentLengths) {
    std::string str1 = "short";
    std::string str2 = "much longer string";
    
    EXPECT_FALSE(SecurityUtils::secure_compare(str1, str2));
}

TEST_F(SecurityUtilsTest, IsValidEmailWithValidEmails) {
    EXPECT_TRUE(SecurityUtils::is_valid_email("user@example.com"));
    EXPECT_TRUE(SecurityUtils::is_valid_email("test.email+tag@domain.co.uk"));
    EXPECT_TRUE(SecurityUtils::is_valid_email("simple@test.org"));
    EXPECT_TRUE(SecurityUtils::is_valid_email("user.name@company-name.com"));
}

TEST_F(SecurityUtilsTest, IsValidEmailWithInvalidEmails) {
    EXPECT_FALSE(SecurityUtils::is_valid_email(""));
    EXPECT_FALSE(SecurityUtils::is_valid_email("invalid"));
    EXPECT_FALSE(SecurityUtils::is_valid_email("@example.com"));
    EXPECT_FALSE(SecurityUtils::is_valid_email("user@"));
    EXPECT_FALSE(SecurityUtils::is_valid_email("user@.com"));
    EXPECT_FALSE(SecurityUtils::is_valid_email("user.example.com"));
    EXPECT_FALSE(SecurityUtils::is_valid_email("user@example."));
}

TEST_F(SecurityUtilsTest, IsStrongPasswordWithValidPasswords) {
    EXPECT_TRUE(SecurityUtils::is_strong_password("StrongP@ssw0rd!"));
    EXPECT_TRUE(SecurityUtils::is_strong_password("MySecure123$"));
    EXPECT_TRUE(SecurityUtils::is_strong_password("C0mpl3x!P@ssw0rd"));
    EXPECT_TRUE(SecurityUtils::is_strong_password("Test1234!@#$"));
}

TEST_F(SecurityUtilsTest, IsStrongPasswordWithWeakPasswords) {
    EXPECT_FALSE(SecurityUtils::is_strong_password(""));
    EXPECT_FALSE(SecurityUtils::is_strong_password("short"));
    EXPECT_FALSE(SecurityUtils::is_strong_password("password"));  // No uppercase, numbers, or special chars
    EXPECT_FALSE(SecurityUtils::is_strong_password("PASSWORD"));  // No lowercase, numbers, or special chars
    EXPECT_FALSE(SecurityUtils::is_strong_password("12345678"));  // No letters or special chars
    EXPECT_FALSE(SecurityUtils::is_strong_password("Password"));  // No numbers or special chars
    EXPECT_FALSE(SecurityUtils::is_strong_password("Password123"));  // No special chars
    EXPECT_FALSE(SecurityUtils::is_strong_password("password123!"));  // No uppercase
}

TEST_F(SecurityUtilsTest, SanitizeInputRemovesHarmfulCharacters) {
    std::string dangerous_input = "<script>alert('xss')</script>";
    std::string sanitized = SecurityUtils::sanitize_input(dangerous_input);
    
    EXPECT_EQ(sanitized.find('<'), std::string::npos);
    EXPECT_EQ(sanitized.find('>'), std::string::npos);
    EXPECT_EQ(sanitized.find("script"), std::string::npos);
}

TEST_F(SecurityUtilsTest, SanitizeInputPreservesNormalText) {
    std::string normal_input = "This is normal text with numbers 123 and symbols: !@#$%^&*()";
    std::string sanitized = SecurityUtils::sanitize_input(normal_input);
    
    // Should preserve most normal characters
    EXPECT_TRUE(sanitized.find("This is normal text") != std::string::npos);
    EXPECT_TRUE(sanitized.find("123") != std::string::npos);
}

TEST_F(SecurityUtilsTest, CreateJWTHeaderReturnsValidBase64) {
    std::string header = SecurityUtils::create_jwt_header();
    
    EXPECT_FALSE(header.empty());
    
    // Should be valid base64 URL-encoded
    std::string decoded = SecurityUtils::base64_url_decode(header);
    EXPECT_FALSE(decoded.empty());
    
    // Should contain JWT header information
    EXPECT_TRUE(decoded.find("alg") != std::string::npos);
    EXPECT_TRUE(decoded.find("typ") != std::string::npos);
}

TEST_F(SecurityUtilsTest, CreateJWTSignatureCreatesConsistentResults) {
    std::string header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
    std::string payload = "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ";
    std::string secret = "your-256-bit-secret";
    
    std::string signature1 = SecurityUtils::create_jwt_signature(header, payload, secret);
    std::string signature2 = SecurityUtils::create_jwt_signature(header, payload, secret);
    
    EXPECT_EQ(signature1, signature2);
    EXPECT_FALSE(signature1.empty());
}

TEST_F(SecurityUtilsTest, GetCurrentUnixTimestamp) {
    int64_t timestamp = SecurityUtils::get_current_unix_timestamp();
    
    // Should be a reasonable Unix timestamp (after year 2020)
    EXPECT_GT(timestamp, 1577836800);  // Jan 1, 2020
    
    // Should be close to current time (within 1 minute)
    auto now = std::chrono::system_clock::now();
    auto now_timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count();
    
    EXPECT_LE(std::abs(timestamp - now_timestamp), 60);
}
