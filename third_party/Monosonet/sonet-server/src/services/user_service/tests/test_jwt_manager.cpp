/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include <gtest/gtest.h>
#include "jwt_manager.h"
#include "models/user.h"
#include "models/user_session.h"
#include <chrono>

using namespace sonet::user;

class JWTManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string secret = "this_is_a_very_secure_secret_key_for_testing_purposes_at_least_32_chars";
        std::string issuer = "test-issuer";
        jwt_manager = std::make_unique<JWTManager>(secret, issuer);
    }
    
    std::unique_ptr<JWTManager> jwt_manager;
    
    User create_test_user() {
        User user;
        user.user_id = "test-user-123";
        user.username = "testuser";
        user.email = "test@example.com";
        user.is_verified = true;
        user.status = UserStatus::ACTIVE;
        return user;
    }
    
    UserSession create_test_session() {
        UserSession session;
        session.session_id = "test-session-456";
        session.user_id = "test-user-123";
        session.device_id = "device-789";
        session.device_type = DeviceType::WEB;
        session.ip_address = "192.168.1.100";
        session.type = SessionType::WEB;
        return session;
    }
};

TEST_F(JWTManagerTest, GenerateAccessTokenCreatesValidToken) {
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    
    EXPECT_FALSE(token.empty());
    
    // JWT tokens should have 3 parts separated by dots
    size_t first_dot = token.find('.');
    size_t second_dot = token.find('.', first_dot + 1);
    
    EXPECT_NE(first_dot, std::string::npos);
    EXPECT_NE(second_dot, std::string::npos);
    EXPECT_EQ(token.find('.', second_dot + 1), std::string::npos); // Should be exactly 3 parts
}

TEST_F(JWTManagerTest, VerifyValidAccessToken) {
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    auto claims = jwt_manager->verify_token(token);
    
    ASSERT_TRUE(claims.has_value());
    EXPECT_EQ(claims->user_id, user.user_id);
    EXPECT_EQ(claims->username, user.username);
    EXPECT_EQ(claims->email, user.email);
    EXPECT_EQ(claims->session_id, session.session_id);
}

TEST_F(JWTManagerTest, VerifyInvalidTokenReturnsNullopt) {
    std::string invalid_token = "invalid.token.here";
    auto claims = jwt_manager->verify_token(invalid_token);
    
    EXPECT_FALSE(claims.has_value());
}

TEST_F(JWTManagerTest, VerifyExpiredTokenReturnsNullopt) {
    // Create a JWT manager with very short token lifetime for testing
    jwt_manager->set_access_token_lifetime(std::chrono::milliseconds(1));
    
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    
    // Wait for token to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    auto claims = jwt_manager->verify_token(token);
    EXPECT_FALSE(claims.has_value());
}

TEST_F(JWTManagerTest, GenerateRefreshToken) {
    std::string user_id = "test-user-123";
    std::string session_id = "test-session-456";
    
    std::string refresh_token = jwt_manager->generate_refresh_token(user_id, session_id);
    
    EXPECT_FALSE(refresh_token.empty());
    
    auto claims = jwt_manager->verify_token(refresh_token);
    ASSERT_TRUE(claims.has_value());
    EXPECT_EQ(claims->user_id, user_id);
    EXPECT_EQ(claims->session_id, session_id);
    
    // Check that it has the refresh role
    bool has_refresh_role = std::find(claims->roles.begin(), claims->roles.end(), "refresh") != claims->roles.end();
    EXPECT_TRUE(has_refresh_role);
}

TEST_F(JWTManagerTest, GenerateEmailVerificationToken) {
    std::string user_id = "test-user-123";
    
    std::string verification_token = jwt_manager->generate_email_verification_token(user_id);
    
    EXPECT_FALSE(verification_token.empty());
    
    auto claims = jwt_manager->verify_token(verification_token);
    ASSERT_TRUE(claims.has_value());
    EXPECT_EQ(claims->user_id, user_id);
    
    // Check that it has the email_verification role
    bool has_verification_role = std::find(claims->roles.begin(), claims->roles.end(), "email_verification") != claims->roles.end();
    EXPECT_TRUE(has_verification_role);
}

TEST_F(JWTManagerTest, GeneratePasswordResetToken) {
    std::string user_id = "test-user-123";
    
    std::string reset_token = jwt_manager->generate_password_reset_token(user_id);
    
    EXPECT_FALSE(reset_token.empty());
    
    auto claims = jwt_manager->verify_token(reset_token);
    ASSERT_TRUE(claims.has_value());
    EXPECT_EQ(claims->user_id, user_id);
    
    // Check that it has the password_reset role
    bool has_reset_role = std::find(claims->roles.begin(), claims->roles.end(), "password_reset") != claims->roles.end();
    EXPECT_TRUE(has_reset_role);
}

TEST_F(JWTManagerTest, IsTokenValidWithValidToken) {
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    
    EXPECT_TRUE(jwt_manager->is_token_valid(token));
}

TEST_F(JWTManagerTest, IsTokenValidWithInvalidToken) {
    std::string invalid_token = "invalid.token.here";
    
    EXPECT_FALSE(jwt_manager->is_token_valid(invalid_token));
}

TEST_F(JWTManagerTest, GetUserIdFromToken) {
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    auto user_id = jwt_manager->get_user_id_from_token(token);
    
    ASSERT_TRUE(user_id.has_value());
    EXPECT_EQ(*user_id, user.user_id);
}

TEST_F(JWTManagerTest, GetSessionIdFromToken) {
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    auto session_id = jwt_manager->get_session_id_from_token(token);
    
    ASSERT_TRUE(session_id.has_value());
    EXPECT_EQ(*session_id, session.session_id);
}

TEST_F(JWTManagerTest, BlacklistTokenMakesItInvalid) {
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    
    // Token should be valid initially
    EXPECT_TRUE(jwt_manager->is_token_valid(token));
    
    // Blacklist the token
    jwt_manager->blacklist_token(token);
    
    // Token should now be invalid
    EXPECT_FALSE(jwt_manager->is_token_valid(token));
}

TEST_F(JWTManagerTest, RotateSigningKey) {
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    EXPECT_TRUE(jwt_manager->is_token_valid(token));
    
    // Rotate the signing key
    std::string new_secret = "this_is_a_completely_new_secret_key_for_testing_purposes_with_sufficient_length";
    jwt_manager->rotate_signing_key(new_secret);
    
    // Old token should no longer be valid with new key
    EXPECT_FALSE(jwt_manager->is_token_valid(token));
    
    // New tokens should work with new key
    std::string new_token = jwt_manager->generate_access_token(user, session);
    EXPECT_TRUE(jwt_manager->is_token_valid(new_token));
}

TEST_F(JWTManagerTest, TokenLifetimeConfiguration) {
    // Set very short lifetime for testing
    jwt_manager->set_access_token_lifetime(std::chrono::seconds(1));
    
    User user = create_test_user();
    UserSession session = create_test_session();
    
    std::string token = jwt_manager->generate_access_token(user, session);
    EXPECT_TRUE(jwt_manager->is_token_valid(token));
    
    // Wait for token to expire
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    EXPECT_FALSE(jwt_manager->is_token_valid(token));
    EXPECT_TRUE(jwt_manager->is_token_expired(token));
}

TEST_F(JWTManagerTest, InvalidSecretKeyThrowsException) {
    std::string short_secret = "short";
    
    EXPECT_THROW(
        JWTManager(short_secret, "test-issuer"),
        std::invalid_argument
    );
}
