#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "../include/e2e_encryption_manager.hpp"
#include "../include/crypto_engine.hpp"
#include <memory>
#include <chrono>
#include <thread>

using namespace sonet::messaging::crypto;

class AdvancedFeaturesTest : public ::testing::Test {
protected:
    void SetUp() override {
        crypto_engine_ = std::make_shared<CryptoEngine>();
        e2e_manager_ = std::make_unique<E2EEncryptionManager>(crypto_engine_);
    }

    void TearDown() override {
        e2e_manager_.reset();
        crypto_engine_.reset();
    }

    std::shared_ptr<CryptoEngine> crypto_engine_;
    std::unique_ptr<E2EEncryptionManager> e2e_manager_;
};

// X3DH Protocol Completion Tests
TEST_F(AdvancedFeaturesTest, OneTimePrekeyRotation) {
    // Add a device for testing
    auto [identity_priv, identity_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "test_user", "device1");
    
    ASSERT_TRUE(e2e_manager_->add_device("test_user", "device1", *identity_pub));
    
    // Test one-time prekey rotation
    ASSERT_TRUE(e2e_manager_->rotate_one_time_prekeys("test_user", 15));
    
    // Verify new prekeys were generated
    auto otks = e2e_manager_->get_one_time_prekeys("test_user", 5);
    ASSERT_EQ(otks.size(), 5);
    
    // Verify all prekeys are different
    std::set<std::string> otk_ids;
    for (const auto& otk : otks) {
        otk_ids.insert(otk.key_id);
    }
    ASSERT_EQ(otk_ids.size(), otks.size());
}

TEST_F(AdvancedFeaturesTest, KeyBundlePublishing) {
    // Add a device
    auto [identity_priv, identity_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "test_user", "device1");
    
    ASSERT_TRUE(e2e_manager_->add_device("test_user", "device1", *identity_pub));
    
    // Publish key bundle
    ASSERT_TRUE(e2e_manager_->publish_key_bundle("test_user", "device1"));
    
    // Retrieve and verify key bundle
    auto bundle_opt = e2e_manager_->get_key_bundle("test_user", "device1");
    ASSERT_TRUE(bundle_opt.has_value());
    
    const auto& bundle = bundle_opt.value();
    ASSERT_EQ(bundle.user_id, "test_user");
    ASSERT_EQ(bundle.device_id, "device1");
    ASSERT_FALSE(bundle.signature.empty());
    ASSERT_FALSE(bundle.is_stale);
    
    // Verify signature
    ASSERT_TRUE(e2e_manager_->verify_signed_prekey_signature("test_user", "device1"));
}

TEST_F(AdvancedFeaturesTest, DeviceManagement) {
    // Add multiple devices
    auto [identity1_priv, identity1_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "test_user", "device1");
    auto [identity2_priv, identity2_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "test_user", "device2");
    
    ASSERT_TRUE(e2e_manager_->add_device("test_user", "device1", *identity1_pub));
    ASSERT_TRUE(e2e_manager_->add_device("test_user", "device2", *identity2_pub));
    
    // Get user devices
    auto devices = e2e_manager_->get_user_devices("test_user");
    ASSERT_EQ(devices.size(), 2);
    ASSERT_TRUE(std::find(devices.begin(), devices.end(), "device1") != devices.end());
    ASSERT_TRUE(std::find(devices.begin(), devices.end(), "device2") != devices.end());
    
    // Remove a device
    ASSERT_TRUE(e2e_manager_->remove_device("test_user", "device1"));
    
    // Verify device was removed
    devices = e2e_manager_->get_user_devices("test_user");
    ASSERT_EQ(devices.size(), 1);
    ASSERT_EQ(devices[0], "device2");
}

TEST_F(AdvancedFeaturesTest, KeyBundleVersioning) {
    // Add a device
    auto [identity_priv, identity_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "test_user", "device1");
    
    ASSERT_TRUE(e2e_manager_->add_device("test_user", "device1", *identity_pub));
    
    // Get initial version
    uint32_t initial_version = e2e_manager_->get_key_bundle_version("test_user", "device1");
    
    // Rotate prekeys (should increment version)
    ASSERT_TRUE(e2e_manager_->rotate_one_time_prekeys("test_user", 10));
    
    // Verify version increased
    uint32_t new_version = e2e_manager_->get_key_bundle_version("test_user", "device1");
    ASSERT_GT(new_version, initial_version);
    
    // Refresh key bundle
    ASSERT_TRUE(e2e_manager_->refresh_key_bundle("test_user", "device1"));
    
    // Mark as stale
    ASSERT_TRUE(e2e_manager_->mark_key_bundle_stale("test_user", "device1"));
    
    // Verify stale status
    auto bundle_opt = e2e_manager_->get_key_bundle("test_user", "device1");
    ASSERT_TRUE(bundle_opt.has_value());
    ASSERT_TRUE(bundle_opt.value().is_stale);
}

// MLS Group Chat Tests
TEST_F(AdvancedFeaturesTest, MLSGroupCreation) {
    std::vector<std::string> member_ids = {"user1", "user2", "user3"};
    
    // Create MLS group
    std::string group_id = e2e_manager_->create_mls_group(member_ids, "Test Group");
    ASSERT_FALSE(group_id.empty());
    ASSERT_TRUE(group_id.find("mls_group_") == 0);
    
    // Verify group state
    // Note: In real implementation, we'd have access to group state
    // For now, we'll test the group ID format
    ASSERT_GT(group_id.length(), 20);
}

TEST_F(AdvancedFeaturesTest, MLSGroupMemberManagement) {
    std::vector<std::string> initial_members = {"user1", "user2"};
    
    // Create group
    std::string group_id = e2e_manager_->create_mls_group(initial_members, "Test Group");
    
    // Add member
    ASSERT_TRUE(e2e_manager_->add_group_member(group_id, "user3", "device1"));
    
    // Remove member
    ASSERT_TRUE(e2e_manager_->remove_group_member(group_id, "user2"));
    
    // Verify group operations succeeded
    ASSERT_FALSE(group_id.empty());
}

TEST_F(AdvancedFeaturesTest, MLSGroupKeyRotation) {
    std::vector<std::string> members = {"user1", "user2"};
    
    // Create group
    std::string group_id = e2e_manager_->create_mls_group(members, "Test Group");
    
    // Rotate group keys
    ASSERT_TRUE(e2e_manager_->rotate_group_keys(group_id));
    
    // Verify rotation succeeded
    ASSERT_FALSE(group_id.empty());
}

TEST_F(AdvancedFeaturesTest, MLSGroupMessageEncryption) {
    std::vector<std::string> members = {"user1", "user2"};
    
    // Create group
    std::string group_id = e2e_manager_->create_mls_group(members, "Test Group");
    
    // Test message encryption/decryption
    std::string plaintext = "Hello, MLS group!";
    std::vector<uint8_t> plaintext_bytes(plaintext.begin(), plaintext.end());
    
    auto encrypted = e2e_manager_->encrypt_group_message(group_id, plaintext_bytes);
    ASSERT_FALSE(encrypted.empty());
    
    auto decrypted = e2e_manager_->decrypt_group_message(group_id, encrypted);
    ASSERT_FALSE(decrypted.empty());
    
    std::string decrypted_text(decrypted.begin(), decrypted.end());
    ASSERT_EQ(plaintext, decrypted_text);
}

// Key Transparency Tests
TEST_F(AdvancedFeaturesTest, KeyChangeLogging) {
    auto [old_key_priv, old_key_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "test_user", "device1");
    auto [new_key_priv, new_key_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "test_user", "device1");
    
    // Log key change
    ASSERT_TRUE(e2e_manager_->log_key_change("test_user", "device1", "rotate", 
                                            *old_key_pub, *new_key_pub, "Scheduled rotation"));
    
    // Retrieve key log
    auto key_log = e2e_manager_->get_key_log("test_user");
    ASSERT_FALSE(key_log.empty());
    
    // Verify log entry
    const auto& entry = key_log[0];
    ASSERT_EQ(entry.user_id, "test_user");
    ASSERT_EQ(entry.device_id, "device1");
    ASSERT_EQ(entry.operation, "rotate");
    ASSERT_EQ(entry.reason, "Scheduled rotation");
    ASSERT_FALSE(entry.signature.empty());
}

TEST_F(AdvancedFeaturesTest, SafetyNumberGeneration) {
    // Generate safety number
    std::string safety_number = e2e_manager_->generate_safety_number("user1", "user2");
    
    // Verify format (5 groups of 5 digits)
    ASSERT_FALSE(safety_number.empty());
    
    // Count spaces (should be 4 for 5 groups)
    size_t space_count = std::count(safety_number.begin(), safety_number.end(), ' ');
    ASSERT_EQ(space_count, 4);
    
    // Verify consistency
    std::string safety_number2 = e2e_manager_->generate_safety_number("user1", "user2");
    ASSERT_EQ(safety_number, safety_number2);
    
    // Verify different for different user pairs
    std::string safety_number3 = e2e_manager_->generate_safety_number("user1", "user3");
    ASSERT_NE(safety_number, safety_number3);
}

TEST_F(AdvancedFeaturesTest, QRCodeGeneration) {
    // Generate QR code data
    std::string qr_data = e2e_manager_->generate_qr_code("user1", "user2");
    
    // Verify format
    ASSERT_FALSE(qr_data.empty());
    ASSERT_TRUE(qr_data.find("sonet://verify/") == 0);
    ASSERT_TRUE(qr_data.find("user1") != std::string::npos);
    ASSERT_TRUE(qr_data.find("user2") != std::string::npos);
    
    // Verify contains safety number
    std::string safety_number = e2e_manager_->generate_safety_number("user1", "user2");
    ASSERT_TRUE(qr_data.find(safety_number) != std::string::npos);
}

TEST_F(AdvancedFeaturesTest, UserIdentityVerification) {
    // Test different verification methods
    ASSERT_TRUE(e2e_manager_->verify_user_identity("user1", "user2", "safety_number"));
    ASSERT_TRUE(e2e_manager_->verify_user_identity("user1", "user2", "qr"));
    
    // Test invalid method
    ASSERT_FALSE(e2e_manager_->verify_user_identity("user1", "user2", "invalid_method"));
}

// Trust Management Tests
TEST_F(AdvancedFeaturesTest, TrustEstablishment) {
    // Establish trust
    ASSERT_TRUE(e2e_manager_->establish_trust("user1", "user2", "verified", "manual"));
    
    // Get trust relationships
    auto trust_relationships = e2e_manager_->get_trust_relationships("user1");
    ASSERT_FALSE(trust_relationships.empty());
    
    // Verify trust state
    const auto& trust_state = trust_relationships[0];
    ASSERT_EQ(trust_state.user_id, "user1");
    ASSERT_EQ(trust_state.trusted_user_id, "user2");
    ASSERT_EQ(trust_state.trust_level, "verified");
    ASSERT_EQ(trust_state.verification_method, "manual");
    ASSERT_TRUE(trust_state.is_active);
}

TEST_F(AdvancedFeaturesTest, TrustLevelUpdates) {
    // Establish initial trust
    ASSERT_TRUE(e2e_manager_->establish_trust("user1", "user2", "unverified", "qr"));
    
    // Update trust level
    ASSERT_TRUE(e2e_manager_->update_trust_level("user1", "user2", "verified"));
    
    // Verify update
    auto trust_relationships = e2e_manager_->get_trust_relationships("user1");
    ASSERT_FALSE(trust_relationships.empty());
    
    const auto& trust_state = trust_relationships[0];
    ASSERT_EQ(trust_state.trust_level, "verified");
}

TEST_F(AdvancedFeaturesTest, MultipleTrustRelationships) {
    // Establish multiple trust relationships
    ASSERT_TRUE(e2e_manager_->establish_trust("user1", "user2", "verified", "manual"));
    ASSERT_TRUE(e2e_manager_->establish_trust("user1", "user3", "unverified", "qr"));
    ASSERT_TRUE(e2e_manager_->establish_trust("user1", "user4", "blocked", "safety_number"));
    
    // Get all relationships
    auto trust_relationships = e2e_manager_->get_trust_relationships("user1");
    ASSERT_EQ(trust_relationships.size(), 3);
    
    // Verify different trust levels
    std::set<std::string> trust_levels;
    for (const auto& trust : trust_relationships) {
        trust_levels.insert(trust.trust_level);
    }
    
    ASSERT_EQ(trust_levels.size(), 3);
    ASSERT_TRUE(trust_levels.find("verified") != trust_levels.end());
    ASSERT_TRUE(trust_levels.find("unverified") != trust_levels.end());
    ASSERT_TRUE(trust_levels.find("blocked") != trust_levels.end());
}

// Performance and Scalability Tests
TEST_F(AdvancedFeaturesTest, ConcurrentDeviceOperations) {
    // Test concurrent device operations
    std::vector<std::thread> threads;
    std::vector<bool> results(10);
    
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&, i]() {
            auto [identity_priv, identity_pub] = crypto_engine_->generate_keypair(
                KeyExchangeProtocol::X25519, "user" + std::to_string(i), "device" + std::to_string(i));
            
            bool result = e2e_manager_->add_device("user" + std::to_string(i), 
                                                 "device" + std::to_string(i), *identity_pub);
            results[i] = result;
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify all operations succeeded
    for (bool result : results) {
        ASSERT_TRUE(result);
    }
}

TEST_F(AdvancedFeaturesTest, LargeGroupManagement) {
    // Create a large group
    std::vector<std::string> members;
    for (int i = 0; i < 100; ++i) {
        members.push_back("user" + std::to_string(i));
    }
    
    // Create group
    std::string group_id = e2e_manager_->create_mls_group(members, "Large Test Group");
    ASSERT_FALSE(group_id.empty());
    
    // Add more members
    for (int i = 100; i < 150; ++i) {
        ASSERT_TRUE(e2e_manager_->add_group_member(group_id, "user" + std::to_string(i), "device1"));
    }
    
    // Remove some members
    for (int i = 0; i < 25; ++i) {
        ASSERT_TRUE(e2e_manager_->remove_group_member(group_id, "user" + std::to_string(i)));
    }
    
    // Verify operations succeeded
    ASSERT_FALSE(group_id.empty());
}

TEST_F(AdvancedFeaturesTest, KeyBundlePerformance) {
    // Test key bundle operations performance
    auto [identity_priv, identity_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "perf_user", "device1");
    
    ASSERT_TRUE(e2e_manager_->add_device("perf_user", "device1", *identity_pub));
    
    // Measure key bundle publishing performance
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(e2e_manager_->publish_key_bundle("perf_user", "device1"));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Verify performance is reasonable (should complete in under 1 second for 100 operations)
    ASSERT_LT(duration.count(), 1000);
}

// Security and Validation Tests
TEST_F(AdvancedFeaturesTest, KeyBundleSignatureVerification) {
    // Add device and publish bundle
    auto [identity_priv, identity_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, "verify_user", "device1");
    
    ASSERT_TRUE(e2e_manager_->add_device("verify_user", "device1", *identity_pub));
    ASSERT_TRUE(e2e_manager_->publish_key_bundle("verify_user", "device1"));
    
    // Verify signature
    ASSERT_TRUE(e2e_manager_->verify_signed_prekey_signature("verify_user", "device1"));
    
    // Test with non-existent user/device
    ASSERT_FALSE(e2e_manager_->verify_signed_prekey_signature("nonexistent_user", "device1"));
    ASSERT_FALSE(e2e_manager_->verify_signed_prekey_signature("verify_user", "nonexistent_device"));
}

TEST_F(AdvancedFeaturesTest, TrustRelationshipValidation) {
    // Test trust relationship validation
    ASSERT_TRUE(e2e_manager_->establish_trust("user1", "user2", "verified", "manual"));
    
    // Verify trust relationship exists
    auto trust_relationships = e2e_manager_->get_trust_relationships("user1");
    ASSERT_FALSE(trust_relationships.empty());
    
    // Test non-existent user
    auto empty_relationships = e2e_manager_->get_trust_relationships("nonexistent_user");
    ASSERT_TRUE(empty_relationships.empty());
}

TEST_F(AdvancedFeaturesTest, MLSGroupSecurity) {
    // Test MLS group security features
    std::vector<std::string> members = {"user1", "user2"};
    
    // Create group
    std::string group_id = e2e_manager_->create_mls_group(members, "Secure Group");
    
    // Test message encryption with group keys
    std::string secret_message = "This is a secret message for the group";
    std::vector<uint8_t> message_bytes(secret_message.begin(), secret_message.end());
    
    auto encrypted = e2e_manager_->encrypt_group_message(group_id, message_bytes);
    ASSERT_FALSE(encrypted.empty());
    
    // Verify encrypted content is different from plaintext
    ASSERT_NE(encrypted, message_bytes);
    
    // Test decryption
    auto decrypted = e2e_manager_->decrypt_group_message(group_id, encrypted);
    ASSERT_EQ(decrypted, message_bytes);
    
    std::string decrypted_text(decrypted.begin(), decrypted.end());
    ASSERT_EQ(secret_message, decrypted_text);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}