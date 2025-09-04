/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <unordered_map>
#include <optional>
#include <mutex>
#include <random>
#include <json/json.h>

namespace sonet::messaging::crypto {

enum class CryptoAlgorithm {
    AES_256_GCM,
    CHACHA20_POLY1305,
    AES_256_CBC,
    XCHACHA20_POLY1305,
    AES_256_SIV
};

enum class KeyExchangeProtocol {
    ECDH_P256,
    ECDH_P384,
    ECDH_P521,
    X25519,
    X448,
    KYBER_512,
    KYBER_768,
    KYBER_1024
};

enum class HashAlgorithm {
    SHA256,
    SHA512,
    SHA3_256,
    SHA3_512,
    BLAKE2B,
    BLAKE3
};

enum class SignatureAlgorithm {
    ECDSA_P256,
    ECDSA_P384,
    ECDSA_P521,
    ED25519,
    ED448,
    DILITHIUM_2,
    DILITHIUM_3,
    DILITHIUM_5
};

struct CryptoKey {
    std::string id;
    std::string algorithm;
    std::vector<uint8_t> key_data;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point expires_at;
    std::string user_id;
    std::string device_id;
    std::optional<std::string> parent_key_id;
    bool is_ephemeral = false;
    
    Json::Value to_json() const;
    static std::unique_ptr<CryptoKey> from_json(const Json::Value& json);
    bool is_expired() const;
    void secure_erase();
};

struct EncryptionContext {
    CryptoAlgorithm algorithm;
    std::string key_id;
    std::vector<uint8_t> initialization_vector;
    std::vector<uint8_t> authentication_tag;
    std::optional<std::vector<uint8_t>> additional_data;
    std::string session_id;
    
    Json::Value to_json() const;
    static EncryptionContext from_json(const Json::Value& json);
};

struct SignatureData {
    SignatureAlgorithm algorithm;
    std::vector<uint8_t> signature;
    std::string signer_key_id;
    std::chrono::system_clock::time_point signed_at;
    HashAlgorithm hash_algorithm;
    
    Json::Value to_json() const;
    static SignatureData from_json(const Json::Value& json);
};

struct KeyDerivationParams {
    std::string algorithm;
    std::vector<uint8_t> salt;
    uint32_t iterations;
    uint32_t memory_cost;
    uint32_t parallelism;
    std::string info;
    
    Json::Value to_json() const;
    static KeyDerivationParams from_json(const Json::Value& json);
};

class SecureRandom {
private:
    std::random_device rd_;
    std::mt19937_64 gen_;
    std::mutex mutex_;
    
public:
    SecureRandom();
    
    std::vector<uint8_t> generate_bytes(size_t length);
    std::string generate_hex(size_t length);
    std::string generate_base64(size_t length);
    uint64_t generate_uint64();
    void seed_additional_entropy(const std::vector<uint8_t>& entropy);
};

class CryptoEngine {
private:
    std::unique_ptr<SecureRandom> random_;
    std::unordered_map<std::string, std::shared_ptr<CryptoKey>> key_cache_;
    std::mutex key_cache_mutex_;
    
    // Configuration
    CryptoAlgorithm default_encryption_algorithm_;
    KeyExchangeProtocol default_key_exchange_;
    HashAlgorithm default_hash_algorithm_;
    SignatureAlgorithm default_signature_algorithm_;
    
    // Performance and security settings
    uint32_t key_rotation_interval_hours_;
    uint32_t max_cached_keys_;
    bool perfect_forward_secrecy_enabled_;
    bool quantum_resistant_mode_;

    // Internal cipher helpers (implemented in .cpp)
    std::vector<uint8_t> encrypt_aes_256_gcm(
        const std::vector<uint8_t>& plaintext,
        const std::vector<uint8_t>& key,
        const std::vector<uint8_t>& iv,
        const std::optional<std::vector<uint8_t>>& aad,
        std::vector<uint8_t>& tag);

    std::vector<uint8_t> decrypt_aes_256_gcm(
        const std::vector<uint8_t>& ciphertext,
        const std::vector<uint8_t>& key,
        const std::vector<uint8_t>& iv,
        const std::optional<std::vector<uint8_t>>& aad,
        const std::vector<uint8_t>& tag);

    std::vector<uint8_t> encrypt_chacha20_poly1305(
        const std::vector<uint8_t>& plaintext,
        const std::vector<uint8_t>& key,
        const std::vector<uint8_t>& nonce,
        const std::optional<std::vector<uint8_t>>& aad,
        std::vector<uint8_t>& tag);

    std::vector<uint8_t> decrypt_chacha20_poly1305(
        const std::vector<uint8_t>& ciphertext,
        const std::vector<uint8_t>& key,
        const std::vector<uint8_t>& nonce,
        const std::optional<std::vector<uint8_t>>& aad,
        const std::vector<uint8_t>& tag);

public:
    CryptoEngine();
    ~CryptoEngine();
    
    // Key generation and management
    std::unique_ptr<CryptoKey> generate_symmetric_key(
        CryptoAlgorithm algorithm = CryptoAlgorithm::AES_256_GCM,
        const std::string& user_id = "",
        const std::string& device_id = "",
        std::chrono::hours expiry = std::chrono::hours(24 * 30)
    );
    
    std::pair<std::unique_ptr<CryptoKey>, std::unique_ptr<CryptoKey>> 
    generate_keypair(
        KeyExchangeProtocol protocol = KeyExchangeProtocol::X25519,
        const std::string& user_id = "",
        const std::string& device_id = ""
    );
    
    std::unique_ptr<CryptoKey> derive_key(
        const CryptoKey& parent_key,
        const KeyDerivationParams& params,
        const std::string& context = ""
    );
    
    // Encryption and decryption
    std::pair<std::vector<uint8_t>, EncryptionContext> encrypt(
        const std::vector<uint8_t>& plaintext,
        const CryptoKey& key,
        const std::optional<std::vector<uint8_t>>& additional_data = std::nullopt
    );
    
    std::vector<uint8_t> decrypt(
        const std::vector<uint8_t>& ciphertext,
        const CryptoKey& key,
        const EncryptionContext& context
    );
    
    // String variants for convenience
    std::pair<std::string, EncryptionContext> encrypt_string(
        const std::string& plaintext,
        const CryptoKey& key
    );
    
    std::string decrypt_string(
        const std::string& ciphertext_base64,
        const CryptoKey& key,
        const EncryptionContext& context
    );
    
    // Digital signatures
    SignatureData sign(
        const std::vector<uint8_t>& data,
        const CryptoKey& private_key,
        HashAlgorithm hash_algorithm = HashAlgorithm::SHA256
    );
    
    bool verify_signature(
        const std::vector<uint8_t>& data,
        const SignatureData& signature,
        const CryptoKey& public_key
    );
    
    // Key exchange
    std::unique_ptr<CryptoKey> perform_key_exchange(
        const CryptoKey& private_key,
        const CryptoKey& public_key,
        const std::string& session_id = ""
    );
    
    // Hashing
    std::vector<uint8_t> hash(
        const std::vector<uint8_t>& data,
        HashAlgorithm algorithm = HashAlgorithm::SHA256
    );
    
    std::string hash_hex(
        const std::vector<uint8_t>& data,
        HashAlgorithm algorithm = HashAlgorithm::SHA256
    );
    
    // Key fingerprinting
    std::string calculate_key_fingerprint(
        const CryptoKey& key,
        HashAlgorithm algorithm = HashAlgorithm::SHA256
    );
    
    // Key validation
    bool validate_key(const CryptoKey& key);
    bool is_key_compromised(const std::string& key_id);
    void mark_key_compromised(const std::string& key_id);
    
    // Key cache management
    void cache_key(std::unique_ptr<CryptoKey> key);
    std::shared_ptr<CryptoKey> get_cached_key(const std::string& key_id);
    void remove_cached_key(const std::string& key_id);
    void clear_key_cache();
    void cleanup_expired_keys();
    
    // Configuration
    void set_default_algorithms(
        CryptoAlgorithm encryption,
        KeyExchangeProtocol key_exchange,
        HashAlgorithm hash,
        SignatureAlgorithm signature
    );
    
    void enable_quantum_resistant_mode(bool enabled);
    void set_key_rotation_interval(std::chrono::hours interval);
    void enable_perfect_forward_secrecy(bool enabled);
    
    // Utilities
    std::vector<uint8_t> generate_salt(size_t length = 32);
    std::vector<uint8_t> generate_iv(CryptoAlgorithm algorithm);
    std::string generate_session_id();
    
    // Security analysis
    bool is_algorithm_secure(CryptoAlgorithm algorithm);
    bool is_key_exchange_secure(KeyExchangeProtocol protocol);
    uint32_t calculate_security_level(CryptoAlgorithm algorithm);
    
    // Performance monitoring
    void enable_performance_monitoring(bool enabled);
    Json::Value get_performance_metrics();
    void reset_performance_metrics();
    
    // Secure memory management
    void secure_zero_memory(void* ptr, size_t size);
    std::vector<uint8_t> secure_allocate(size_t size);
    void secure_deallocate(std::vector<uint8_t>& data);
};

class E2EEncryptionManager {
private:
    std::unique_ptr<CryptoEngine> crypto_engine_;
    std::unordered_map<std::string, std::unique_ptr<CryptoKey>> user_keys_;
    std::unordered_map<std::string, std::unique_ptr<CryptoKey>> session_keys_;
    std::mutex keys_mutex_;
    
    // Double ratchet state for perfect forward secrecy
    struct RatchetState {
        std::unique_ptr<CryptoKey> root_key;
        std::unique_ptr<CryptoKey> chain_key_send;
        std::unique_ptr<CryptoKey> chain_key_recv;
        std::unique_ptr<CryptoKey> dh_self;
        std::unique_ptr<CryptoKey> dh_remote;
        uint32_t send_count = 0;
        uint32_t recv_count = 0;
        std::unordered_map<uint32_t, std::unique_ptr<CryptoKey>> skipped_keys;
    };
    
    std::unordered_map<std::string, std::unique_ptr<RatchetState>> ratchet_states_;
    
public:
    E2EEncryptionManager();
    ~E2EEncryptionManager();
    
    // User key management
    bool register_user_keys(
        const std::string& user_id,
        const CryptoKey& identity_key,
        const CryptoKey& signed_prekey,
        const std::vector<CryptoKey>& one_time_prekeys
    );
    
    bool update_user_keys(
        const std::string& user_id,
        const CryptoKey& new_signed_prekey,
        const std::vector<CryptoKey>& new_one_time_prekeys
    );
    
    // Session establishment
    std::string initiate_session(
        const std::string& sender_id,
        const std::string& recipient_id,
        const std::string& device_id = ""
    );
    
    bool accept_session(
        const std::string& session_id,
        const std::string& recipient_id,
        const std::string& sender_id
    );
    
    // Message encryption/decryption
    std::pair<std::vector<uint8_t>, Json::Value> encrypt_message(
        const std::string& session_id,
        const std::vector<uint8_t>& plaintext,
        const std::optional<std::vector<uint8_t>>& additional_data = std::nullopt
    );
    
    std::vector<uint8_t> decrypt_message(
        const std::string& session_id,
        const std::vector<uint8_t>& ciphertext,
        const Json::Value& encryption_metadata
    );
    
    // Double ratchet operations
    void advance_ratchet(const std::string& session_id);
    void reset_ratchet(const std::string& session_id);
    
    // Session management
    bool is_session_active(const std::string& session_id);
    void close_session(const std::string& session_id);
    void close_all_sessions(const std::string& user_id);
    
    // Key rotation
    void rotate_session_keys(const std::string& session_id);
    void rotate_all_user_keys(const std::string& user_id);
    
    // Security verification
    std::string get_session_fingerprint(const std::string& session_id);
    bool verify_session_integrity(const std::string& session_id);
    bool compare_fingerprints(
        const std::string& session_id,
        const std::string& expected_fingerprint
    );
    
    // Utilities
    Json::Value export_session_info(const std::string& session_id);
    bool import_session_info(const Json::Value& session_info);
    std::vector<std::string> get_active_sessions(const std::string& user_id);
    
    // Performance and monitoring
    Json::Value get_encryption_metrics();
    void cleanup_old_sessions(std::chrono::hours max_age);
    void optimize_memory_usage();
};

} // namespace sonet::messaging::crypto
