#include <gtest/gtest.h>
#include "password_manager.h"

using namespace sonet::user;

class PassphraseManagerTest : public ::testing::Test {
protected:
    PasswordManager manager;
};

TEST_F(PassphraseManagerTest, ValidPassphrase) {
    // Test a valid passphrase
    std::string valid_passphrase = "correct horse battery staple";
    EXPECT_TRUE(manager.is_password_strong(valid_passphrase));
}

TEST_F(PassphraseManagerTest, TooShortPassphrase) {
    // Test a passphrase that's too short
    std::string short_passphrase = "short phrase";
    EXPECT_FALSE(manager.is_password_strong(short_passphrase));
}

TEST_F(PassphraseManagerTest, InsufficientWords) {
    // Test a passphrase with too few words
    std::string few_words = "just three words";
    EXPECT_FALSE(manager.is_password_strong(few_words));
}

TEST_F(PassphraseManagerTest, CommonPhraseRejected) {
    // Test that common phrases are rejected
    std::string common_phrase = "correct horse battery staple";
    EXPECT_TRUE(manager.is_password_compromised(common_phrase));
}

TEST_F(PassphraseManagerTest, GenerateSecurePassphrase) {
    // Test passphrase generation
    std::string generated = manager.generate_secure_passphrase(4);
    EXPECT_GE(generated.length(), 20);
    
    // Count words
    std::istringstream iss(generated);
    std::string word;
    size_t word_count = 0;
    while (iss >> word) {
        if (word.length() >= 2) {
            word_count++;
        }
    }
    EXPECT_EQ(word_count, 4);
}

TEST_F(PassphraseManagerTest, HashAndVerify) {
    // Test that hashing and verification work
    std::string passphrase = "my favorite coffee shop downtown";
    std::string hash = manager.hash_password(passphrase);
    
    EXPECT_TRUE(manager.verify_password(passphrase, hash));
    EXPECT_FALSE(manager.verify_password("wrong passphrase", hash));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}