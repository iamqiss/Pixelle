#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <chrono>
#include <optional>
#include <mutex>
#include <atomic>
#include <thread>
#include "crypto_engine.hpp"
#include "mls_protocol.hpp"
#include "pqc_algorithms.hpp"

namespace sonet::messaging::crypto {

// Forward declarations
class CryptoEngine;
class CryptoKey;

// X3DH Protocol Structures
struct KeyBundle {
    std::string user_id;
    std::string device_id;
    CryptoKey identity_key;
    CryptoKey signed_prekey;
    std::vector<CryptoKey> one_time_prekeys;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_refresh;
    uint32_t version;
    std::string signature;
    bool is_stale;
    
    Json::Value to_json() const;
    static KeyBundle from_json(const Json::Value& json);
};

struct DeviceState {
    std::string device_id;
    CryptoKey identity_key;
    CryptoKey signed_prekey;
    std::vector<CryptoKey> one_time_prekeys;
    std::chrono::system_clock::time_point last_activity;
    uint32_t key_bundle_version;
    bool is_active;
    
    Json::Value to_json() const;
    static DeviceState from_json(const Json::Value& json);
};

// MLS Group Chat Structures
struct MLSGroupState {
    std::string group_id;
    std::string epoch_id;
    std::vector<std::string> member_ids;
    CryptoKey group_key;
    std::vector<CryptoKey> epoch_keys;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_epoch_change;
    uint32_t epoch_number;
    bool is_active;
    
    Json::Value to_json() const;
    static MLSGroupState from_json(const Json::Value& json);
};

struct MLSMember {
    std::string user_id;
    std::string device_id;
    CryptoKey identity_key;
    CryptoKey leaf_key;
    uint32_t leaf_index;
    std::chrono::system_clock::time_point joined_at;
    bool is_active;
    
    Json::Value to_json() const;
    static MLSMember from_json(const Json::Value& json);
};

// Key Transparency Structures
struct KeyLogEntry {
    std::string user_id;
    std::string device_id;
    std::string operation; // "add", "remove", "rotate", "compromise"
    CryptoKey old_key;
    CryptoKey new_key;
    std::chrono::system_clock::time_point timestamp;
    std::string signature;
    std::string reason;
    
    Json::Value to_json() const;
    static KeyLogEntry from_json(const Json::Value& json);
};

struct TrustState {
    std::string user_id;
    std::string trusted_user_id;
    std::string trust_level; // "verified", "unverified", "blocked"
    std::chrono::system_clock::time_point established_at;
    std::chrono::system_clock::time_point last_verified;
    std::string verification_method; // "manual", "qr", "safety_number"
    bool is_active;
    
    Json::Value to_json() const;
    static TrustState from_json(const Json::Value& json);
};

class E2EEncryptionManager {
public:
    explicit E2EEncryptionManager(std::shared_ptr<CryptoEngine> crypto_engine);
    ~E2EEncryptionManager();

    // Core X3DH Operations
    bool register_user_keys(const std::string& user_id, const CryptoKey& identity_key, 
                           const CryptoKey& signed_prekey, const std::vector<CryptoKey>& one_time_prekeys);
    bool update_user_keys(const std::string& user_id, const CryptoKey& identity_key, 
                         const CryptoKey& signed_prekey, const std::vector<CryptoKey>& one_time_prekeys);
    
    // Session Management
    std::string initiate_session(const std::string& sender_id, const std::string& recipient_id, 
                                const std::string& device_id);
    bool accept_session(const std::string& session_id, const std::string& recipient_id, 
                       const std::string& sender_id);
    
    // Message Encryption/Decryption
    std::pair<std::vector<uint8_t>, std::string> encrypt_message(const std::string& session_id, 
                                                                const std::vector<uint8_t>& plaintext);
    std::vector<uint8_t> decrypt_message(const std::string& session_id, 
                                       const std::vector<uint8_t>& ciphertext, 
                                       const std::string& metadata);
    
    // X3DH Protocol Completion
    bool rotate_one_time_prekeys(const std::string& user_id, uint32_t count = 10);
    std::vector<CryptoKey> get_one_time_prekeys(const std::string& user_id, uint32_t count = 3);
    bool publish_key_bundle(const std::string& user_id, const std::string& device_id);
    std::optional<KeyBundle> get_key_bundle(const std::string& user_id, const std::string& device_id);
    bool verify_signed_prekey_signature(const std::string& user_id, const std::string& device_id);
    
    // Device Management
    bool add_device(const std::string& user_id, const std::string& device_id, const CryptoKey& identity_key);
    bool remove_device(const std::string& user_id, const std::string& device_id);
    std::vector<std::string> get_user_devices(const std::string& user_id);
    bool update_device_keys(const std::string& user_id, const std::string& device_id, 
                           const CryptoKey& identity_key, const CryptoKey& signed_prekey,
                           const std::vector<CryptoKey>& one_time_prekeys);
    
    // Key Bundle Management
    bool refresh_key_bundle(const std::string& user_id, const std::string& device_id);
    uint32_t get_key_bundle_version(const std::string& user_id, const std::string& device_id);
    bool mark_key_bundle_stale(const std::string& user_id, const std::string& device_id);
    
    // MLS Group Chat Support
    std::string create_mls_group(const std::vector<std::string>& member_ids, const std::string& group_name);
    bool add_group_member(const std::string& group_id, const std::string& user_id, const std::string& device_id);
    bool remove_group_member(const std::string& group_id, const std::string& user_id);
    bool rotate_group_keys(const std::string& group_id);
    std::vector<uint8_t> encrypt_group_message(const std::string& group_id, const std::vector<uint8_t>& plaintext);
    std::vector<uint8_t> decrypt_group_message(const std::string& group_id, const std::vector<uint8_t>& ciphertext);
    
    // PQC Operations
    std::vector<uint8_t> pqc_encrypt(const std::vector<uint8_t>& plaintext, const std::vector<uint8_t>& public_key);
    std::vector<uint8_t> pqc_decrypt(const std::vector<uint8_t>& ciphertext, const std::vector<uint8_t>& private_key);
    std::vector<uint8_t> pqc_sign(const std::vector<uint8_t>& message, const std::vector<uint8_t>& private_key);
    bool pqc_verify(const std::vector<uint8_t>& message, const std::vector<uint8_t>& signature, const std::vector<uint8_t>& public_key);
    
    // Hybrid Encryption
    std::vector<uint8_t> hybrid_encrypt(const std::vector<uint8_t>& plaintext, const std::vector<uint8_t>& pqc_public_key);
    std::vector<uint8_t> hybrid_decrypt(const std::vector<uint8_t>& encrypted_data, const std::vector<uint8_t>& pqc_private_key);
    
    // Key Transparency & Verification
    bool log_key_change(const std::string& user_id, const std::string& device_id, 
                       const std::string& operation, const CryptoKey& old_key, 
                       const CryptoKey& new_key, const std::string& reason = "");
    std::vector<KeyLogEntry> get_key_log(const std::string& user_id, 
                                        const std::chrono::system_clock::time_point& since = std::chrono::system_clock::time_point::min());
    std::string generate_safety_number(const std::string& user_id, const std::string& other_user_id);
    std::string generate_qr_code(const std::string& user_id, const std::string& other_user_id);
    bool verify_user_identity(const std::string& user_id, const std::string& other_user_id, 
                             const std::string& verification_method);
    
    // Trust Management
    bool establish_trust(const std::string& user_id, const std::string& trusted_user_id, 
                        const std::string& trust_level, const std::string& verification_method);
    bool update_trust_level(const std::string& user_id, const std::string& trusted_user_id, 
                           const std::string& new_trust_level);
    std::vector<TrustState> get_trust_relationships(const std::string& user_id);
    
    // Session Management
    bool is_session_active(const std::string& session_id);
    bool close_session(const std::string& session_id);
    bool close_all_sessions(const std::string& user_id);
    
    // Key Rotation & Security
    bool rotate_session_keys(const std::string& session_id);
    bool rotate_all_user_keys(const std::string& user_id);
    bool mark_session_compromised(const std::string& session_id);
    bool recover_from_compromise(const std::string& session_id, const std::string& new_identity_key);
    
    // Utilities
    std::string get_session_fingerprint(const std::string& session_id);
    bool verify_session_integrity(const std::string& session_id);
    bool compare_fingerprints(const std::string& session_id, const std::string& other_fingerprint);
    std::string export_session_info(const std::string& session_id);
    bool import_session_info(const std::string& session_id, const std::string& info);
    
    // Monitoring & Metrics
    std::vector<std::string> get_active_sessions(const std::string& user_id);
    std::unordered_map<std::string, uint64_t> get_encryption_metrics();
    bool cleanup_old_sessions();
    bool optimize_memory_usage();

private:
    std::shared_ptr<CryptoEngine> crypto_engine_;
    
    // Session state
    std::unordered_map<std::string, std::unique_ptr<RatchetState>> ratchet_states_;
    std::unordered_map<std::string, std::string> session_users_; // session_id -> "user1:user2"
    
    // X3DH Protocol State
    std::unordered_map<std::string, std::unordered_map<std::string, DeviceState>> user_devices_;
    std::unordered_map<std::string, std::unordered_map<std::string, KeyBundle>> key_bundles_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::chrono::system_clock::time_point>> last_prekey_rotation_;
    
    // MLS Group Chat State
    std::unordered_map<std::string, MLSGroupState> mls_groups_;
    std::unordered_map<std::string, std::vector<MLSMember>> group_members_;
    
    // Key Transparency State
    std::vector<KeyLogEntry> key_log_;
    std::unordered_map<std::string, std::vector<TrustState>> trust_relationships_;
    
    // Configuration
    uint32_t max_one_time_prekeys_ = 100;
    std::chrono::hours prekey_rotation_interval_ = std::chrono::hours(24);
    std::chrono::hours key_bundle_ttl_ = std::chrono::hours(168); // 1 week
    uint32_t max_key_log_entries_ = 10000;
    
    // Threading
    std::mutex ratchet_states_mutex_;
    std::mutex user_devices_mutex_;
    std::mutex key_bundles_mutex_;
    std::mutex mls_groups_mutex_;
    std::mutex key_log_mutex_;
    std::mutex trust_mutex_;
    
    std::thread cleanup_thread_;
    std::atomic<bool> running_{true};
    
    // Helper methods
    bool generate_and_sign_prekeys(const std::string& user_id, const std::string& device_id);
    std::string sign_key_bundle(const KeyBundle& bundle);
    bool verify_key_bundle_signature(const KeyBundle& bundle);
    void cleanup_expired_prekeys();
    void rotate_stale_key_bundles();
    void cleanup_expired_key_logs();
    void background_cleanup();
    
    // MLS helpers
    bool perform_mls_key_exchange(const std::string& group_id);
    bool update_mls_epoch(const std::string& group_id);
    
    // PQC and MLS instances
    std::unique_ptr<sonet::mls::MLSProtocol> mls_protocol_;
    std::unique_ptr<sonet::pqc::PQCAlgorithms> pqc_algorithms_;
    
    // Trust helpers
    std::string calculate_trust_hash(const std::string& user_id, const std::string& other_user_id);
    bool verify_trust_signature(const TrustState& trust_state);
};

} // namespace sonet::messaging::crypto