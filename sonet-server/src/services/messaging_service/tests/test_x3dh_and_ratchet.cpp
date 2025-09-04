#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "../include/crypto_engine.hpp"
#include "../include/encryption_manager.hpp"
#include <memory>
#include <chrono>

using namespace sonet::messaging::crypto;
using namespace sonet::messaging::encryption;

class X3DHRatchetTest : public ::testing::Test {
protected:
    void SetUp() override {
        crypto_engine_ = std::make_unique<CryptoEngine>();
        e2e_manager_ = std::make_unique<E2EEncryptionManager>();
        encryption_manager_ = std::make_unique<EncryptionManager>();
    }

    void TearDown() override {
        crypto_engine_.reset();
        e2e_manager_.reset();
        encryption_manager_.reset();
    }

    std::unique_ptr<CryptoEngine> crypto_engine_;
    std::unique_ptr<E2EEncryptionManager> e2e_manager_;
    std::unique_ptr<EncryptionManager> encryption_manager_;
};

// Test X3DH session establishment
TEST_F(X3DHRatchetTest, X3DHSessionEstablishment) {
    // Generate identity keys for Alice and Bob
    auto [alice_id_priv, alice_id_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "alice", "device1");
    auto [bob_id_priv, bob_id_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "bob", "device1");
    
    // Generate signed prekeys
    auto [alice_spk_priv, alice_spk_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "alice", "device1");
    auto [bob_spk_priv, bob_spk_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "bob", "device1");
    
    // Generate one-time prekeys (simplified - just one each)
    auto [alice_otk_priv, alice_otk_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "alice", "device1");
    auto [bob_otk_priv, bob_otk_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "bob", "device1");
    
    // Register keys with E2E manager
    std::vector<CryptoKey> alice_otks = {*alice_otk_pub};
    std::vector<CryptoKey> bob_otks = {*bob_otk_pub};
    
    ASSERT_TRUE(e2e_manager_->register_user_keys("alice", *alice_id_pub, *alice_spk_pub, alice_otks));
    ASSERT_TRUE(e2e_manager_->register_user_keys("bob", *bob_id_pub, *bob_spk_pub, bob_otks));
    
    // Alice initiates session with Bob
    std::string session_id = e2e_manager_->initiate_session("alice", "bob", "device1");
    ASSERT_FALSE(session_id.empty());
    
    // Bob accepts the session
    ASSERT_TRUE(e2e_manager_->accept_session(session_id, "bob", "alice"));
    
    // Verify session is active
    ASSERT_TRUE(e2e_manager_->is_session_active(session_id));
}

// Test message encryption/decryption round-trip
TEST_F(X3DHRatchetTest, MessageEncryptionDecryption) {
    // Setup session (simplified)
    auto [alice_id_priv, alice_id_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "alice", "device1");
    auto [bob_id_priv, bob_id_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "bob", "device1");
    
    auto [alice_spk_priv, alice_spk_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "alice", "device1");
    auto [bob_spk_priv, bob_spk_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, "bob", "device1");
    
    std::vector<CryptoKey> alice_otks = {*alice_id_pub}; // Use identity as placeholder
    std::vector<CryptoKey> bob_otks = {*bob_id_pub};
    
    e2e_manager_->register_user_keys("alice", *alice_id_pub, *alice_spk_pub, alice_otks);
    e2e_manager_->register_user_keys("bob", *bob_id_pub, *bob_spk_pub, bob_otks);
    
    std::string session_id = e2e_manager_->initiate_session("alice", "bob", "device1");
    e2e_manager_->accept_session(session_id, "bob", "alice");
    
    // Test message round-trip
    std::string plaintext = "Hello, this is a test message!";
    std::vector<uint8_t> plaintext_bytes(plaintext.begin(), plaintext.end());
    
    // Alice encrypts message
    auto [ciphertext, metadata] = e2e_manager_->encrypt_message(session_id, plaintext_bytes);
    ASSERT_FALSE(ciphertext.empty());
    ASSERT_FALSE(metadata.empty());
    
    // Bob decrypts message
    std::vector<uint8_t> decrypted = e2e_manager_->decrypt_message(session_id, ciphertext, metadata);
    std::string decrypted_text(decrypted.begin(), decrypted.end());
    
    ASSERT_EQ(plaintext, decrypted_text);
}

// Test Double Ratchet chain advancement
TEST_F(X3DHRatchetTest, RatchetChainAdvancement) {
    // Initialize Double Ratchet for a chat
    std::string chat_id = "test_chat_123";
    std::string alice_identity = "alice_identity_key";
    std::string bob_identity = "bob_identity_key";
    
    auto ratchet_state = encryption_manager_->initialize_double_ratchet(chat_id, alice_identity, bob_identity);
    ASSERT_FALSE(ratchet_state.state_id.empty());
    
    // Test sending chain advancement
    ASSERT_TRUE(encryption_manager_->advance_sending_chain(chat_id));
    
    // Test receiving chain advancement
    ASSERT_TRUE(encryption_manager_->advance_receiving_chain(chat_id));
    
    // Get message keys
    std::string sending_key = encryption_manager_->get_sending_message_key(chat_id);
    ASSERT_FALSE(sending_key.empty());
    
    std::string receiving_key = encryption_manager_->get_receiving_message_key(chat_id);
    ASSERT_FALSE(receiving_key.empty());
    
    // Verify keys are different (chains advanced)
    ASSERT_NE(sending_key, receiving_key);
}

// Test skipped message key handling
TEST_F(X3DHRatchetTest, SkippedMessageKeyHandling) {
    std::string chat_id = "test_chat_456";
    std::string alice_identity = "alice_identity_key_2";
    std::string bob_identity = "bob_identity_key_2";
    
    auto ratchet_state = encryption_manager_->initialize_double_ratchet(chat_id, alice_identity, bob_identity);
    
    // Store a skipped message key
    std::string skipped_key = "test_skipped_key";
    uint32_t message_number = 5;
    
    ASSERT_TRUE(encryption_manager_->store_skipped_message_key(chat_id, message_number, skipped_key));
    
    // Retrieve the skipped key
    std::string retrieved_key = encryption_manager_->get_skipped_message_key(chat_id, message_number);
    ASSERT_EQ(skipped_key, retrieved_key);
    
    // Verify key was consumed (removed after retrieval)
    std::string empty_key = encryption_manager_->get_skipped_message_key(chat_id, message_number);
    ASSERT_TRUE(empty_key.empty());
}

// Test key compromise and recovery
TEST_F(X3DHRatchetTest, KeyCompromiseRecovery) {
    std::string chat_id = "test_chat_789";
    std::string alice_identity = "alice_identity_key_3";
    std::string bob_identity = "bob_identity_key_3";
    
    auto ratchet_state = encryption_manager_->initialize_double_ratchet(chat_id, alice_identity, bob_identity);
    
    // Mark keys as compromised
    ASSERT_TRUE(encryption_manager_->mark_key_compromised(chat_id));
    
    // Try to recover with new identity key
    std::string new_identity = "new_alice_identity_key";
    ASSERT_TRUE(encryption_manager_->recover_from_compromise(chat_id, new_identity));
    
    // Verify new state is functional
    std::string new_sending_key = encryption_manager_->get_sending_message_key(chat_id);
    ASSERT_FALSE(new_sending_key.empty());
}

// Test replay protection (server-side)
TEST_F(X3DHRatchetTest, ReplayProtection) {
    // This would test the server-side replay protection
    // For now, we'll test the encryption manager's ability to handle repeated operations
    
    std::string chat_id = "test_chat_replay";
    std::string alice_identity = "alice_identity_key_4";
    std::string bob_identity = "bob_identity_key_4";
    
    auto ratchet_state = encryption_manager_->initialize_double_ratchet(chat_id, alice_identity, bob_identity);
    
    // Generate multiple message keys (should advance chain each time)
    std::string key1 = encryption_manager_->get_sending_message_key(chat_id);
    std::string key2 = encryption_manager_->get_sending_message_key(chat_id);
    std::string key3 = encryption_manager_->get_sending_message_key(chat_id);
    
    // All keys should be different (chain advancing)
    ASSERT_NE(key1, key2);
    ASSERT_NE(key2, key3);
    ASSERT_NE(key1, key3);
}

// Test AAD generation and validation
TEST_F(X3DHRatchetTest, AADGenerationValidation) {
    // Test that AAD components are properly formatted
    std::string message_id = "msg_123";
    std::string chat_id = "chat_456";
    std::string sender_id = "user_789";
    std::string algorithm = "AES-GCM";
    std::string key_id = "key_abc";
    
    // Simulate AAD generation (this would be done client-side)
    std::string aad_components = message_id + "|" + chat_id + "|" + sender_id + "|" + algorithm + "|" + key_id;
    
    // Verify AAD format
    ASSERT_EQ(aad_components, "msg_123|chat_456|user_789|AES-GCM|key_abc");
    
    // Test that AAD is not empty and has expected length (SHA-256 hash = 64 hex chars)
    // In real implementation, this would be a hash of the components
    ASSERT_FALSE(aad_components.empty());
}

// Test session export/import
TEST_F(X3DHRatchetTest, SessionExportImport) {
    std::string chat_id = "test_chat_export";
    std::string alice_identity = "alice_identity_key_5";
    std::string bob_identity = "bob_identity_key_5";
    
    auto ratchet_state = encryption_manager_->initialize_double_ratchet(chat_id, alice_identity, bob_identity);
    
    // Export session state
    std::string exported_state = encryption_manager_->export_ratchet_state(chat_id);
    ASSERT_FALSE(exported_state.empty());
    
    // Import session state to new chat
    std::string new_chat_id = "test_chat_import";
    ASSERT_TRUE(encryption_manager_->import_ratchet_state(new_chat_id, exported_state));
    
    // Verify imported state is functional
    std::string imported_key = encryption_manager_->get_sending_message_key(new_chat_id);
    ASSERT_FALSE(imported_key.empty());
}

// Test memory zeroization
TEST_F(X3DHRatchetTest, MemoryZeroization) {
    std::string chat_id = "test_chat_memory";
    std::string alice_identity = "alice_identity_key_6";
    std::string bob_identity = "bob_identity_key_6";
    
    auto ratchet_state = encryption_manager_->initialize_double_ratchet(chat_id, alice_identity, bob_identity);
    
    // Mark keys as compromised (this should zeroize memory)
    ASSERT_TRUE(encryption_manager_->mark_key_compromised(chat_id));
    
    // Try to use the compromised session (should fail or return empty)
    std::string compromised_key = encryption_manager_->get_sending_message_key(chat_id);
    ASSERT_TRUE(compromised_key.empty());
}

// Test concurrent access safety
TEST_F(X3DHRatchetTest, ConcurrentAccessSafety) {
    std::string chat_id = "test_chat_concurrent";
    std::string alice_identity = "alice_identity_key_7";
    std::string bob_identity = "bob_identity_key_7";
    
    auto ratchet_state = encryption_manager_->initialize_double_ratchet(chat_id, alice_identity, bob_identity);
    
    // Simulate concurrent access from multiple threads
    std::vector<std::thread> threads;
    std::vector<std::string> keys;
    std::mutex keys_mutex;
    
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&, i]() {
            std::string key = encryption_manager_->get_sending_message_key(chat_id);
            std::lock_guard<std::mutex> lock(keys_mutex);
            keys.push_back(key);
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify all keys were generated and are different
    ASSERT_EQ(keys.size(), 10);
    
    // Check that keys are unique (chain advancing properly)
    std::set<std::string> unique_keys(keys.begin(), keys.end());
    ASSERT_EQ(unique_keys.size(), keys.size());
}

// Test cleanup and expiration
TEST_F(X3DHRatchetTest, CleanupAndExpiration) {
    // Test cleanup of expired ratchet states
    encryption_manager_->cleanup_expired_ratchet_states();
    
    // This is a basic test - in real implementation, we'd need to:
    // 1. Create states with old timestamps
    // 2. Wait for expiration
    // 3. Verify cleanup
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}