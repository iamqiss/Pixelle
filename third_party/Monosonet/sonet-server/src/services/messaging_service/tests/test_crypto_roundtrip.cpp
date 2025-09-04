#include <gtest/gtest.h>
#include "../include/encryption_manager.hpp"
#include "../include/crypto_engine.hpp"

using namespace sonet::messaging;

TEST(CryptoRoundTrip, EncryptionManagerChaCha) {
    encryption::EncryptionManager mgr;
    auto sk = mgr.create_session_key("chat1", "userA", encryption::EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
    ASSERT_FALSE(sk.session_id.empty());
    auto env = mgr.encrypt_message(sk.session_id, "hello world", "aad");
    ASSERT_FALSE(env.ciphertext.empty());
    auto pt = mgr.decrypt_message(env);
    EXPECT_EQ(pt, "hello world");
}

TEST(CryptoRoundTrip, CryptoEngineGCM) {
    crypto::CryptoEngine ce;
    auto key = ce.generate_symmetric_key(crypto::CryptoAlgorithm::AES_256_GCM);
    auto pair = ce.encrypt_string("hello world", *key);
    auto decrypted = ce.decrypt_string(pair.first, *key, pair.second);
    EXPECT_EQ(decrypted, std::string("hello world"));
}