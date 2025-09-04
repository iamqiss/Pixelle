#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <string>
#include "mls_protocol.hpp"
#include "pqc_algorithms.hpp"
#include "e2e_encryption_manager.hpp"

class SecurityFeaturesTest : public ::testing::Test {
protected:
    void SetUp() override {
        mls_protocol_ = std::make_unique<sonet::mls::MLSProtocol>();
        pqc_algorithms_ = std::make_unique<sonet::pqc::PQCAlgorithms>();
        e2e_manager_ = std::make_unique<sonet::messaging::crypto::E2EEncryptionManager>();
    }

    void TearDown() override {
        mls_protocol_.reset();
        pqc_algorithms_.reset();
        e2e_manager_.reset();
    }

    std::unique_ptr<sonet::mls::MLSProtocol> mls_protocol_;
    std::unique_ptr<sonet::pqc::PQCAlgorithms> pqc_algorithms_;
    std::unique_ptr<sonet::messaging::crypto::E2EEncryptionManager> e2e_manager_;
};

// MLS Protocol Tests
TEST_F(SecurityFeaturesTest, MLSCreateGroup) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    auto result = mls_protocol_->create_group(group_id, 
                                             sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                             extensions);
    
    EXPECT_FALSE(result.empty());
    EXPECT_GT(result.size(), 100); // Should contain group data
}

TEST_F(SecurityFeaturesTest, MLSAddMember) {
    // Create group first
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Create key package for new member
    sonet::mls::KeyPackage key_package;
    key_package.version = {0x00, 0x01};
    key_package.cipher_suite = {0x00, 0x01};
    key_package.init_key = std::vector<uint8_t>(32, 0x42);
    key_package.leaf_node.public_key = std::vector<uint8_t>(32, 0x43);
    key_package.leaf_node.signature_key = std::vector<uint8_t>(32, 0x44);
    key_package.leaf_node.encryption_key = std::vector<uint8_t>(32, 0x45);
    key_package.leaf_node.signature = std::vector<uint8_t>(64, 0x46);
    
    auto result = mls_protocol_->add_member(group_id, key_package);
    
    EXPECT_FALSE(result.empty());
    EXPECT_GT(result.size(), 100);
}

TEST_F(SecurityFeaturesTest, MLSEncryptDecrypt) {
    // Create group
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Test message encryption/decryption
    std::string plaintext = "Hello, MLS group!";
    std::vector<uint8_t> plaintext_bytes(plaintext.begin(), plaintext.end());
    std::vector<uint8_t> aad = {0x01, 0x02, 0x03};
    
    auto encrypted = mls_protocol_->encrypt_message(group_id, plaintext_bytes, aad);
    EXPECT_FALSE(encrypted.empty());
    EXPECT_NE(encrypted, plaintext_bytes);
    
    auto decrypted = mls_protocol_->decrypt_message(group_id, encrypted, aad);
    EXPECT_EQ(decrypted, plaintext_bytes);
}

// PQC Algorithms Tests
TEST_F(SecurityFeaturesTest, PQCKyberKeyGeneration) {
    auto keypair = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    
    EXPECT_FALSE(keypair.public_key.empty());
    EXPECT_FALSE(keypair.private_key.empty());
    EXPECT_EQ(keypair.algorithm, sonet::pqc::PQCAlgorithm::KYBER_768);
    EXPECT_EQ(keypair.public_key.size(), 1184);
    EXPECT_EQ(keypair.private_key.size(), 2400);
}

TEST_F(SecurityFeaturesTest, PQCKyberEncapsulation) {
    auto keypair = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    
    auto ciphertext = pqc_algorithms_->kyber_encapsulate(keypair.public_key, sonet::pqc::PQCAlgorithm::KYBER_768);
    EXPECT_FALSE(ciphertext.empty());
    EXPECT_EQ(ciphertext.size(), 1088);
    
    auto shared_secret = pqc_algorithms_->kyber_decapsulate(ciphertext, keypair.private_key, sonet::pqc::PQCAlgorithm::KYBER_768);
    EXPECT_FALSE(shared_secret.empty());
    EXPECT_EQ(shared_secret.size(), 32);
}

TEST_F(SecurityFeaturesTest, PQCDilithiumSignVerify) {
    auto keypair = pqc_algorithms_->generate_dilithium_keypair(sonet::pqc::PQCAlgorithm::DILITHIUM_3);
    
    std::string message = "Test message for Dilithium signature";
    std::vector<uint8_t> message_bytes(message.begin(), message.end());
    
    auto signature = pqc_algorithms_->dilithium_sign(message_bytes, keypair.private_key, sonet::pqc::PQCAlgorithm::DILITHIUM_3);
    EXPECT_FALSE(signature.empty());
    EXPECT_EQ(signature.size(), 3366);
    
    bool verified = pqc_algorithms_->dilithium_verify(message_bytes, signature, keypair.public_key, sonet::pqc::PQCAlgorithm::DILITHIUM_3);
    EXPECT_TRUE(verified);
}

TEST_F(SecurityFeaturesTest, PQCHybridEncryption) {
    auto keypair = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    
    std::string plaintext = "Test message for hybrid encryption";
    std::vector<uint8_t> plaintext_bytes(plaintext.begin(), plaintext.end());
    
    auto encrypted = pqc_algorithms_->hybrid_encrypt(plaintext_bytes, keypair.public_key, sonet::pqc::PQCAlgorithm::KYBER_768);
    EXPECT_FALSE(encrypted.classical_ciphertext.empty());
    EXPECT_FALSE(encrypted.pqc_ciphertext.empty());
    EXPECT_FALSE(encrypted.nonce.empty());
    EXPECT_EQ(encrypted.pqc_algorithm, sonet::pqc::PQCAlgorithm::KYBER_768);
    
    auto decrypted = pqc_algorithms_->hybrid_decrypt(encrypted, keypair.private_key, sonet::pqc::PQCAlgorithm::KYBER_768);
    EXPECT_EQ(decrypted, plaintext_bytes);
}

// E2E Manager Integration Tests
TEST_F(SecurityFeaturesTest, E2EPQCEncryptDecrypt) {
    // Generate PQC keypair
    auto keypair = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    
    std::string plaintext = "Test message for E2E PQC encryption";
    std::vector<uint8_t> plaintext_bytes(plaintext.begin(), plaintext.end());
    
    auto encrypted = e2e_manager_->pqc_encrypt(plaintext_bytes, keypair.public_key);
    EXPECT_FALSE(encrypted.empty());
    EXPECT_NE(encrypted, plaintext_bytes);
    
    auto decrypted = e2e_manager_->pqc_decrypt(encrypted, keypair.private_key);
    EXPECT_EQ(decrypted, plaintext_bytes);
}

TEST_F(SecurityFeaturesTest, E2EPQCSignVerify) {
    // Generate PQC keypair
    auto keypair = pqc_algorithms_->generate_dilithium_keypair(sonet::pqc::PQCAlgorithm::DILITHIUM_3);
    
    std::string message = "Test message for E2E PQC signature";
    std::vector<uint8_t> message_bytes(message.begin(), message.end());
    
    auto signature = e2e_manager_->pqc_sign(message_bytes, keypair.private_key);
    EXPECT_FALSE(signature.empty());
    
    bool verified = e2e_manager_->pqc_verify(message_bytes, signature, keypair.public_key);
    EXPECT_TRUE(verified);
}

TEST_F(SecurityFeaturesTest, E2EHybridEncryption) {
    // Generate PQC keypair
    auto keypair = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    
    std::string plaintext = "Test message for E2E hybrid encryption";
    std::vector<uint8_t> plaintext_bytes(plaintext.begin(), plaintext.end());
    
    auto encrypted = e2e_manager_->hybrid_encrypt(plaintext_bytes, keypair.public_key);
    EXPECT_FALSE(encrypted.empty());
    EXPECT_NE(encrypted, plaintext_bytes);
    
    auto decrypted = e2e_manager_->hybrid_decrypt(encrypted, keypair.private_key);
    EXPECT_EQ(decrypted, plaintext_bytes);
}

// Performance Tests
TEST_F(SecurityFeaturesTest, PQCPerformance) {
    auto keypair = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    
    std::vector<uint8_t> test_data(1024, 0x42); // 1KB test data
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < 100; i++) {
        auto encrypted = pqc_algorithms_->hybrid_encrypt(test_data, keypair.public_key, sonet::pqc::PQCAlgorithm::KYBER_768);
        auto decrypted = pqc_algorithms_->hybrid_decrypt(encrypted, keypair.private_key, sonet::pqc::PQCAlgorithm::KYBER_768);
        EXPECT_EQ(decrypted, test_data);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Should complete 100 operations in reasonable time (less than 10 seconds)
    EXPECT_LT(duration.count(), 10000);
}

// Security Tests
TEST_F(SecurityFeaturesTest, PQCKeyUniqueness) {
    auto keypair1 = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    auto keypair2 = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    
    // Keys should be different
    EXPECT_NE(keypair1.public_key, keypair2.public_key);
    EXPECT_NE(keypair1.private_key, keypair2.private_key);
}

TEST_F(SecurityFeaturesTest, PQCEncryptionUniqueness) {
    auto keypair = pqc_algorithms_->generate_kyber_keypair(sonet::pqc::PQCAlgorithm::KYBER_768);
    
    std::string plaintext = "Test message";
    std::vector<uint8_t> plaintext_bytes(plaintext.begin(), plaintext.end());
    
    auto encrypted1 = pqc_algorithms_->hybrid_encrypt(plaintext_bytes, keypair.public_key, sonet::pqc::PQCAlgorithm::KYBER_768);
    auto encrypted2 = pqc_algorithms_->hybrid_encrypt(plaintext_bytes, keypair.public_key, sonet::pqc::PQCAlgorithm::KYBER_768);
    
    // Encryptions should be different due to random nonce
    EXPECT_NE(encrypted1.classical_ciphertext, encrypted2.classical_ciphertext);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}