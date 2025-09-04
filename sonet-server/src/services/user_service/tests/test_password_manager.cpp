/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include <gtest/gtest.h>
#include "password_manager.h"
#include <string>

using namespace sonet::user;

class PasswordManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        password_manager = std::make_unique<PasswordManager>();
    }
    
    std::unique_ptr<PasswordManager> password_manager;
};

TEST_F(PasswordManagerTest, HashPasswordCreatesValidHash) {
    std::string password = "TestPassword123!";
    std::string hash = password_manager->hash_password(password);
    
    EXPECT_FALSE(hash.empty());
    EXPECT_NE(hash, password);
    
    // Argon2id hashes should start with $argon2id$
    EXPECT_TRUE(hash.starts_with("$argon2id$"));
}

TEST_F(PasswordManagerTest, VerifyPasswordWithCorrectPassword) {
    std::string password = "MySecurePassword456!";
    std::string hash = password_manager->hash_password(password);
    
    EXPECT_TRUE(password_manager->verify_password(password, hash));
}

TEST_F(PasswordManagerTest, VerifyPasswordWithIncorrectPassword) {
    std::string correct_password = "CorrectPassword789!";
    std::string wrong_password = "WrongPassword123!";
    std::string hash = password_manager->hash_password(correct_password);
    
    EXPECT_FALSE(password_manager->verify_password(wrong_password, hash));
}

TEST_F(PasswordManagerTest, DifferentPasswordsProduceDifferentHashes) {
    std::string password1 = "Password1!";
    std::string password2 = "Password2!";
    
    std::string hash1 = password_manager->hash_password(password1);
    std::string hash2 = password_manager->hash_password(password2);
    
    EXPECT_NE(hash1, hash2);
}

TEST_F(PasswordManagerTest, SamePasswordProducesDifferentHashesDueToSalt) {
    std::string password = "SamePassword123!";
    
    std::string hash1 = password_manager->hash_password(password);
    std::string hash2 = password_manager->hash_password(password);
    
    // Different salts should produce different hashes
    EXPECT_NE(hash1, hash2);
    
    // But both should verify correctly
    EXPECT_TRUE(password_manager->verify_password(password, hash1));
    EXPECT_TRUE(password_manager->verify_password(password, hash2));
}

TEST_F(PasswordManagerTest, EmptyPasswordHandling) {
    std::string empty_password = "";
    
    // Should handle empty passwords gracefully
    std::string hash = password_manager->hash_password(empty_password);
    EXPECT_FALSE(hash.empty());
    EXPECT_TRUE(password_manager->verify_password(empty_password, hash));
}

TEST_F(PasswordManagerTest, VeryLongPasswordHandling) {
    // Create a very long password (1KB)
    std::string long_password(1024, 'A');
    long_password += "123!";
    
    std::string hash = password_manager->hash_password(long_password);
    EXPECT_FALSE(hash.empty());
    EXPECT_TRUE(password_manager->verify_password(long_password, hash));
}

TEST_F(PasswordManagerTest, UnicodePasswordHandling) {
    std::string unicode_password = "密码123!@#测试";
    
    std::string hash = password_manager->hash_password(unicode_password);
    EXPECT_FALSE(hash.empty());
    EXPECT_TRUE(password_manager->verify_password(unicode_password, hash));
}

TEST_F(PasswordManagerTest, InvalidHashHandling) {
    std::string password = "TestPassword123!";
    std::string invalid_hash = "not_a_valid_hash";
    
    EXPECT_FALSE(password_manager->verify_password(password, invalid_hash));
}

TEST_F(PasswordManagerTest, SecurityTimingConsistency) {
    std::string password = "TestPassword123!";
    std::string hash = password_manager->hash_password(password);
    
    // Multiple verification attempts should take roughly the same time
    // This is a basic test - in practice you'd use more sophisticated timing analysis
    auto start1 = std::chrono::high_resolution_clock::now();
    password_manager->verify_password(password, hash);
    auto end1 = std::chrono::high_resolution_clock::now();
    
    auto start2 = std::chrono::high_resolution_clock::now();
    password_manager->verify_password("WrongPassword", hash);
    auto end2 = std::chrono::high_resolution_clock::now();
    
    auto duration1 = std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start1);
    auto duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end2 - start2);
    
    // The timing difference shouldn't be too significant
    // This is a rough test - proper timing analysis would be more sophisticated
    EXPECT_TRUE(std::abs(duration1.count() - duration2.count()) < 100);
}
