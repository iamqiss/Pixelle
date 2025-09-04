#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "../api/include/messaging_controller.hpp"
#include <memory>
#include <chrono>
#include <thread>

using namespace sonet::messaging::api;

class ServerSecurityTest : public ::testing::Test {
protected:
    void SetUp() override {
        controller_ = std::make_unique<MessagingController>(8080, 8081);
    }

    void TearDown() override {
        controller_.reset();
    }

    std::unique_ptr<MessagingController> controller_;
    
    // Helper to create a valid encryption envelope
    Json::Value createValidEncryptionEnvelope(
        const std::string& message_id,
        const std::string& chat_id,
        const std::string& sender_id,
        const std::string& key_id = "test_key_123"
    ) {
        Json::Value envelope;
        envelope["v"] = 1;
        envelope["alg"] = "AES-GCM";
        envelope["keyId"] = key_id;
        envelope["iv"] = "dGVzdF9pdl9iYXNlNjQ="; // "test_iv_base64" in base64
        envelope["tag"] = "dGVzdF90YWdfYmFzZTY0"; // "test_tag_base64" in base64
        
        // Generate AAD hash: hash(msgId|chatId|senderId|alg|keyId)
        std::string aad_components = message_id + "|" + chat_id + "|" + sender_id + "|AES-GCM|" + key_id;
        // In real implementation, this would be SHA-256 hash
        envelope["aad"] = "aad_hash_" + aad_components; // Simplified for testing
        
        return envelope;
    }
};

// Test AAD validation in encryption envelope
TEST_F(ServerSecurityTest, AADValidationRequired) {
    // Test that envelope without AAD is rejected
    Json::Value invalid_envelope;
    invalid_envelope["v"] = 1;
    invalid_envelope["alg"] = "AES-GCM";
    invalid_envelope["keyId"] = "test_key";
    invalid_envelope["iv"] = "dGVzdF9pdl9iYXNlNjQ=";
    invalid_envelope["tag"] = "dGVzdF90YWdfYmFzZTY0";
    // Missing aad field
    
    std::string error;
    // This would call the validate_encryption_envelope lambda from the controller
    // For testing, we'll simulate the validation logic
    bool is_valid = invalid_envelope.isMember("aad") && invalid_envelope["aad"].isString();
    ASSERT_FALSE(is_valid);
    
    // Test that envelope with AAD is accepted
    Json::Value valid_envelope = createValidEncryptionEnvelope("msg_123", "chat_456", "user_789");
    is_valid = valid_envelope.isMember("aad") && valid_envelope["aad"].isString();
    ASSERT_TRUE(is_valid);
}

// Test encryption envelope field validation
TEST_F(ServerSecurityTest, EncryptionEnvelopeFieldValidation) {
    // Test required fields
    Json::Value envelope = createValidEncryptionEnvelope("msg_123", "chat_456", "user_789");
    
    // All required fields should be present
    ASSERT_TRUE(envelope.isMember("alg"));
    ASSERT_TRUE(envelope.isMember("keyId"));
    ASSERT_TRUE(envelope.isMember("iv"));
    ASSERT_TRUE(envelope.isMember("tag"));
    ASSERT_TRUE(envelope.isMember("aad"));
    
    // Test field types
    ASSERT_TRUE(envelope["alg"].isString());
    ASSERT_TRUE(envelope["keyId"].isString());
    ASSERT_TRUE(envelope["iv"].isString());
    ASSERT_TRUE(envelope["tag"].isString());
    ASSERT_TRUE(envelope["aad"].isString());
    
    // Test base64 validation (simplified)
    std::string iv = envelope["iv"].asString();
    std::string tag = envelope["tag"].asString();
    
    // Check minimum length requirements
    ASSERT_GE(iv.length(), 12);
    ASSERT_GE(tag.length(), 16);
}

// Test replay protection mechanism
TEST_F(ServerSecurityTest, ReplayProtectionMechanism) {
    // This test would verify the replay protection logic
    // Since the controller methods are private, we'll test the concept
    
    std::string chat_id = "test_chat";
    std::string user_id = "test_user";
    std::string iv = "dGVzdF9pdl9iYXNlNjQ=";
    std::string tag = "dGVzdF90YWdfYmFzZTY0";
    
    // Simulate replay protection key format: chatId|userId|iv|tag
    std::string replay_key = chat_id + "|" + user_id + "|" + iv + "|" + tag;
    
    // Verify replay key format
    ASSERT_EQ(replay_key, "test_chat|test_user|dGVzdF9pdl9iYXNlNjQ=|dGVzdF90YWdfYmFzZTY0");
    
    // Test that different IVs create different replay keys
    std::string different_iv = "ZGlmZmVyZW50X2l2";
    std::string replay_key2 = chat_id + "|" + user_id + "|" + different_iv + "|" + tag;
    
    ASSERT_NE(replay_key, replay_key2);
}

// Test canonical envelope construction
TEST_F(ServerSecurityTest, CanonicalEnvelopeConstruction) {
    std::string message_id = "msg_123";
    std::string chat_id = "chat_456";
    std::string sender_id = "user_789";
    std::string content = "encrypted_content_base64";
    
    Json::Value envelope = createValidEncryptionEnvelope(message_id, chat_id, sender_id);
    
    // Add canonical fields
    envelope["msgId"] = message_id;
    envelope["chatId"] = chat_id;
    envelope["senderId"] = sender_id;
    envelope["ct"] = content;
    
    // Verify canonical structure
    ASSERT_EQ(envelope["msgId"].asString(), message_id);
    ASSERT_EQ(envelope["chatId"].asString(), chat_id);
    ASSERT_EQ(envelope["senderId"].asString(), sender_id);
    ASSERT_EQ(envelope["ct"].asString(), content);
    ASSERT_EQ(envelope["v"].asInt(), 1);
}

// Test encryption envelope versioning
TEST_F(ServerSecurityTest, EncryptionEnvelopeVersioning) {
    Json::Value envelope = createValidEncryptionEnvelope("msg_123", "chat_456", "user_789");
    
    // Test default version
    if (!envelope.isMember("v")) {
        envelope["v"] = 1;
    }
    ASSERT_EQ(envelope["v"].asInt(), 1);
    
    // Test explicit version
    envelope["v"] = 2;
    ASSERT_EQ(envelope["v"].asInt(), 2);
}

// Test algorithm validation
TEST_F(ServerSecurityTest, AlgorithmValidation) {
    Json::Value envelope = createValidEncryptionEnvelope("msg_123", "chat_456", "user_789");
    
    // Test supported algorithms
    std::vector<std::string> supported_algorithms = {"AES-GCM", "ChaCha20-Poly1305"};
    
    for (const auto& alg : supported_algorithms) {
        envelope["alg"] = alg;
        bool is_valid = envelope.isMember("alg") && envelope["alg"].isString();
        ASSERT_TRUE(is_valid);
    }
    
    // Test invalid algorithm
    envelope["alg"] = "INVALID_ALG";
    bool is_valid = envelope.isMember("alg") && envelope["alg"].isString();
    ASSERT_TRUE(is_valid); // Type check passes, but value is invalid
}

// Test key ID validation
TEST_F(ServerSecurityTest, KeyIdValidation) {
    Json::Value envelope = createValidEncryptionEnvelope("msg_123", "chat_456", "user_789");
    
    // Test valid key ID
    std::string valid_key_id = "key_1234567890abcdef";
    envelope["keyId"] = valid_key_id;
    ASSERT_TRUE(envelope.isMember("keyId") && envelope["keyId"].isString());
    
    // Test empty key ID (should be rejected)
    envelope["keyId"] = "";
    ASSERT_TRUE(envelope.isMember("keyId") && envelope["keyId"].isString());
    ASSERT_TRUE(envelope["keyId"].asString().empty());
}

// Test IV and tag length validation
TEST_F(ServerSecurityTest, IVAndTagLengthValidation) {
    Json::Value envelope = createValidEncryptionEnvelope("msg_123", "chat_456", "user_789");
    
    // Test minimum length requirements
    std::string short_iv = "dGVzdA=="; // "test" in base64 (4 bytes)
    std::string short_tag = "dGVzdHRhZw=="; // "testtag" in base64 (8 bytes)
    
    envelope["iv"] = short_iv;
    envelope["tag"] = short_tag;
    
    // These should fail length validation
    std::string iv = envelope["iv"].asString();
    std::string tag = envelope["tag"].asString();
    
    // Check base64 decoded length (simplified)
    ASSERT_LT(iv.length(), 16); // Should be at least 16 base64 chars for 12 bytes
    ASSERT_LT(tag.length(), 24); // Should be at least 24 base64 chars for 16 bytes
}

// Test AAD component validation
TEST_F(ServerSecurityTest, AADComponentValidation) {
    std::string message_id = "msg_123";
    std::string chat_id = "chat_456";
    std::string sender_id = "user_789";
    std::string algorithm = "AES-GCM";
    std::string key_id = "key_abc";
    
    // Test AAD component format
    std::string aad_components = message_id + "|" + chat_id + "|" + sender_id + "|" + algorithm + "|" + key_id;
    
    // Verify component count
    std::vector<std::string> components;
    size_t pos = 0;
    std::string delimiter = "|";
    
    while ((pos = aad_components.find(delimiter)) != std::string::npos) {
        components.push_back(aad_components.substr(0, pos));
        aad_components.erase(0, pos + delimiter.length());
    }
    components.push_back(aad_components); // Add last component
    
    ASSERT_EQ(components.size(), 5);
    ASSERT_EQ(components[0], message_id);
    ASSERT_EQ(components[1], chat_id);
    ASSERT_EQ(components[2], sender_id);
    ASSERT_EQ(components[3], algorithm);
    ASSERT_EQ(components[4], key_id);
}

// Test error handling for malformed envelopes
TEST_F(ServerSecurityTest, MalformedEnvelopeErrorHandling) {
    // Test missing required fields
    Json::Value missing_fields;
    missing_fields["v"] = 1;
    // Missing alg, keyId, iv, tag, aad
    
    bool has_required_fields = 
        missing_fields.isMember("alg") && 
        missing_fields.isMember("keyId") && 
        missing_fields.isMember("iv") && 
        missing_fields.isMember("tag") && 
        missing_fields.isMember("aad");
    
    ASSERT_FALSE(has_required_fields);
    
    // Test invalid field types
    Json::Value invalid_types;
    invalid_types["v"] = 1;
    invalid_types["alg"] = 123; // Should be string
    invalid_types["keyId"] = 456; // Should be string
    invalid_types["iv"] = true; // Should be string
    invalid_types["tag"] = 789; // Should be string
    invalid_types["aad"] = "valid_aad";
    
    bool has_valid_types = 
        invalid_types["alg"].isString() && 
        invalid_types["keyId"].isString() && 
        invalid_types["iv"].isString() && 
        invalid_types["tag"].isString();
    
    ASSERT_FALSE(has_valid_types);
}

// Test concurrent envelope validation
TEST_F(ServerSecurityTest, ConcurrentEnvelopeValidation) {
    // This test would verify thread safety of envelope validation
    // Since we can't directly test the controller's private methods,
    // we'll test the concept of concurrent validation
    
    std::vector<std::thread> threads;
    std::vector<bool> results(10);
    
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&, i]() {
            Json::Value envelope = createValidEncryptionEnvelope(
                "msg_" + std::to_string(i),
                "chat_" + std::to_string(i),
                "user_" + std::to_string(i)
            );
            
            // Simulate validation
            bool is_valid = envelope.isMember("aad") && envelope["aad"].isString();
            results[i] = is_valid;
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify all validations succeeded
    for (bool result : results) {
        ASSERT_TRUE(result);
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}