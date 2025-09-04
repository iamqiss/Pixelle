/*
 * Encryption Manager - header
 */
#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <json/json.h>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <thread>
#include <atomic>

namespace sonet::messaging::encryption {

enum class EncryptionAlgorithm {
    AES_256_GCM = 0,
    CHACHA20_POLY1305 = 1,
    X25519_CHACHA20_POLY1305 = 2
};

struct EncryptionKeyPair {
    std::string key_id;
    EncryptionAlgorithm algorithm;
    std::string public_key;
    std::string private_key;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point expires_at;
    bool is_ephemeral = false;

    std::string serialize_public_key() const;
    std::string serialize_private_key() const;
    bool load_from_hex(const std::string& public_hex, const std::string& private_hex);
    Json::Value to_json() const;
    bool is_expired() const;
};

struct SessionKey {
    std::string session_id;
    std::string chat_id;
    std::string user_id;
    EncryptionAlgorithm algorithm;
    std::string key_material; // binary stored as string
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point expires_at;
    uint32_t message_count = 0;
    uint32_t max_messages = 0;

    std::string get_key_material() const;
    Json::Value to_json() const;
    bool is_expired() const;
    void increment_usage();
};

struct EncryptedMessage {
    std::string message_id;
    std::string session_id;
    EncryptionAlgorithm algorithm;
    std::string ciphertext;      // base64
    std::string nonce;           // base64
    std::string tag;             // base64
    std::string additional_data; // raw AAD if any
    std::chrono::system_clock::time_point timestamp;

    Json::Value to_json() const;
    static EncryptedMessage from_json(const Json::Value& json);
};

// Double Ratchet State for perfect forward secrecy
struct DoubleRatchetState {
    std::string state_id;
    std::string chat_id;
    std::string our_identity_key;
    std::string their_identity_key;
    std::string root_key;
    std::string sending_chain_key;
    std::string receiving_chain_key;
    std::string our_ratchet_private_key;
    std::string our_ratchet_public_key;
    std::string their_ratchet_public_key;
    uint32_t sending_message_number;
    uint32_t receiving_message_number;
    uint32_t previous_sending_chain_length;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_ratchet;
    
    // Skipped message keys for out-of-order delivery
    std::unordered_map<uint32_t, std::string> skipped_message_keys;
    uint32_t max_skipped_keys = 1000; // Bounded window for memory safety
    
    // Chain key evolution tracking
    uint32_t sending_chain_length = 0;
    uint32_t receiving_chain_length = 0;
    
    // Rekeying parameters
    uint32_t messages_since_rekey = 0;
    uint32_t max_messages_per_chain = 1000;
    std::chrono::hours rekey_interval = std::chrono::hours(24);
    
    Json::Value to_json() const;
    static DoubleRatchetState from_json(const Json::Value& json);
    bool should_ratchet() const;
    bool should_rekey() const;
    void cleanup_old_skipped_keys();
};

class EncryptionManager {
public:
    EncryptionManager();
    ~EncryptionManager();

    // Key management
    EncryptionKeyPair generate_key_pair(EncryptionAlgorithm algorithm);
    SessionKey create_session_key(const std::string& chat_id,
                                  const std::string& user_id,
                                  EncryptionAlgorithm algorithm);

    // Message encryption/decryption
    EncryptedMessage encrypt_message(const std::string& session_id,
                                     const std::string& plaintext,
                                     const std::string& additional_data);

    std::string decrypt_message(const EncryptedMessage& encrypted_msg);

    // Double Ratchet operations
    DoubleRatchetState initialize_double_ratchet(const std::string& chat_id,
                                                 const std::string& our_identity_key,
                                                 const std::string& their_identity_key);
    
    bool perform_dh_ratchet(const std::string& chat_id,
                             const std::string& their_new_public_key);
    
    std::string derive_message_key(const std::string& chain_key, uint32_t message_number);
    
    // Advanced Double Ratchet methods
    bool advance_sending_chain(const std::string& chat_id);
    bool advance_receiving_chain(const std::string& chat_id);
    std::string get_sending_message_key(const std::string& chat_id);
    std::string get_receiving_message_key(const std::string& chat_id);
    bool store_skipped_message_key(const std::string& chat_id, uint32_t message_number, const std::string& key);
    std::string get_skipped_message_key(const std::string& chat_id, uint32_t message_number);
    void cleanup_expired_ratchet_states();
    
    // Message handling with Double Ratchet
    bool process_incoming_message(const std::string& chat_id, uint32_t message_number, const std::string& encrypted_content);
    std::string prepare_outgoing_message(const std::string& chat_id, const std::string& plaintext);
    
    // Security and recovery methods
    bool mark_key_compromised(const std::string& chat_id);
    bool recover_from_compromise(const std::string& chat_id, const std::string& new_identity_key);
    std::string export_ratchet_state(const std::string& chat_id);
    bool import_ratchet_state(const std::string& chat_id, const std::string& state_data);
    
    // Group chat encryption (interim implementation)
    bool add_group_member(const std::string& chat_id, const std::string& user_id, const std::string& public_key);
    bool remove_group_member(const std::string& chat_id, const std::string& user_id);
    std::vector<std::string> get_group_members(const std::string& chat_id);
    
    // Key exchange and session management
    std::string compute_shared_secret(const std::string& private_key, const std::string& public_key);
    std::string derive_key(const std::string& input_key_material, const std::string& info, size_t output_length);
    
    // Utilities
    std::vector<std::string> get_supported_algorithms() const;
    std::string base64_encode(const std::string& input);
    std::string base64_decode(const std::string& input);
    std::string generate_key_id();
    std::string generate_session_id();
    std::string generate_message_id();
    std::string generate_state_id();
    std::string algorithm_to_string(EncryptionAlgorithm algorithm);

private:
    // Random number generator
    std::unique_ptr<CryptoPP::AutoSeededRandomPool> rng_;
    
    // Key storage
    std::unordered_map<std::string, EncryptionKeyPair> key_pairs_;
    std::unordered_map<std::string, SessionKey> session_keys_;
    std::unordered_map<std::string, std::vector<std::string>> chat_session_keys_;
    std::unordered_map<std::string, std::vector<std::string>> user_session_keys_;
    
    // Double Ratchet state storage
    std::unordered_map<std::string, DoubleRatchetState> ratchet_states_;
    
    // Mutexes for thread safety
    mutable std::mutex key_pairs_mutex_;
    mutable std::mutex session_keys_mutex_;
    mutable std::mutex ratchet_states_mutex_;
    
    // Configuration
    std::vector<EncryptionAlgorithm> supported_algorithms_;
    EncryptionAlgorithm preferred_algorithm_;
    std::chrono::hours key_rotation_interval_;
    uint32_t max_messages_per_key_;
    
    // Background cleanup
    std::thread cleanup_thread_;
    std::atomic<bool> running_{true};
    
    // Helper methods
    std::string generate_random_id(const std::string& prefix);
    size_t get_key_size(EncryptionAlgorithm algorithm);
    void cleanup_expired_keys();
    void cleanup_expired_ratchet_states();
    
    // Double Ratchet helper methods
    std::string derive_chain_key(const std::string& input_key, const std::string& info);
    std::string derive_message_key_from_chain(const std::string& chain_key, uint32_t message_number);
    void ratchet_sending_chain(DoubleRatchetState& state);
    void ratchet_receiving_chain(DoubleRatchetState& state);
    bool should_perform_dh_ratchet(const DoubleRatchetState& state);
    void perform_rekey_if_needed(DoubleRatchetState& state);
};

} // namespace sonet::messaging::encryption