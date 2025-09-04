#pragma once

#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <chrono>
#include <cstdint>
#include <optional>

namespace sonet::mls {

// MLS Protocol Constants (RFC 9420)
constexpr uint16_t MLS_VERSION = 0x0001;
constexpr uint8_t PROTOCOL_VERSION = 0x01;
constexpr size_t KEY_SIZE = 32;
constexpr size_t NONCE_SIZE = 12;
constexpr size_t SIGNATURE_SIZE = 64;

// Group Management Limits for Optimal Performance
constexpr uint32_t MAX_GROUP_MEMBERS = 500;  // Optimal for buttery smooth key distribution
constexpr uint32_t WARNING_GROUP_SIZE = 400; // Warning threshold for performance
constexpr uint32_t OPTIMAL_GROUP_SIZE = 250; // Optimal group size for best performance

// MLS Cipher Suites
enum class CipherSuite : uint16_t {
    MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519 = 0x0001,
    MLS_128_DHKEMP256_AES128GCM_SHA256_P256 = 0x0002,
    MLS_128_DHKEMX25519_CHACHA20POLY1305_SHA256_Ed25519 = 0x0003,
    MLS_256_DHKEMX448_AES256GCM_SHA512_Ed448 = 0x0004,
    MLS_256_DHKEMP521_AES256GCM_SHA512_P521 = 0x0005,
    MLS_256_DHKEMX448_CHACHA20POLY1305_SHA512_Ed448 = 0x0006,
    MLS_256_DHKEMX448_CHACHA20POLY1305_SHA512_Ed448 = 0x0007
};

// MLS Group States
enum class GroupState : uint8_t {
    CREATING = 0x00,
    ACTIVE = 0x01,
    UPDATING = 0x02,
    DELETING = 0x03
};

// Group Size Status for Performance Monitoring
enum class GroupSizeStatus : uint8_t {
    OPTIMAL = 0x00,      // 0-250 members: Best performance
    GOOD = 0x01,         // 251-400 members: Good performance
    WARNING = 0x02,      // 401-500 members: Performance warning
    AT_LIMIT = 0x03,     // 500 members: At maximum limit
    OVER_LIMIT = 0x04    // >500 members: Over limit (should not happen)
};

// MLS Message Types
enum class MessageType : uint8_t {
    PROPOSAL = 0x01,
    COMMIT = 0x02,
    WELCOME = 0x03,
    GROUP_INFO = 0x04,
    KEY_PACKAGE = 0x05,
    ADD = 0x06,
    UPDATE = 0x07,
    REMOVE = 0x08,
    PSK = 0x09,
    REINIT = 0x0A,
    EXTERNAL_INIT = 0x0B,
    GROUP_CONTEXT_EXTENSIONS = 0x0C
};

// MLS Proposal Types
enum class ProposalType : uint8_t {
    ADD = 0x01,
    UPDATE = 0x02,
    REMOVE = 0x03,
    PSK = 0x04,
    REINIT = 0x05,
    EXTERNAL_INIT = 0x06,
    GROUP_CONTEXT_EXTENSIONS = 0x07
};

// MLS Leaf Node
struct LeafNode {
    std::vector<uint8_t> public_key;
    std::vector<uint8_t> signature_key;
    std::vector<uint8_t> encryption_key;
    std::vector<uint8_t> signature;
    std::chrono::system_clock::time_point not_before;
    std::chrono::system_clock::time_point not_after;
    std::vector<uint8_t> capabilities;
    std::vector<uint8_t> extensions;
};

// MLS Group Context
struct GroupContext {
    uint32_t group_id;
    uint64_t epoch;
    std::vector<uint8_t> tree_hash;
    std::vector<uint8_t> confirmed_transcript_hash;
    std::vector<uint8_t> extensions;
};

// MLS Tree Node
struct TreeNode {
    std::optional<LeafNode> leaf_node;
    std::vector<uint8_t> parent_hash;
    std::vector<uint8_t> unmerged_leaves;
    std::vector<uint8_t> group_context_extensions;
};

// MLS Commit
struct Commit {
    std::vector<uint8_t> proposals_hash;
    std::vector<uint8_t> path;
    std::vector<uint8_t> signature;
    std::vector<uint8_t> confirmation_tag;
};

// MLS Welcome
struct Welcome {
    std::vector<uint8_t> version;
    std::vector<uint8_t> cipher_suite;
    std::vector<uint8_t> group_id;
    std::vector<uint8_t> epoch;
    std::vector<uint8_t> tree_hash;
    std::vector<uint8_t> confirmed_transcript_hash;
    std::vector<uint8_t> interim_transcript_hash;
    std::vector<uint8_t> group_context_extensions;
    std::vector<uint8_t> key_packages;
    std::vector<uint8_t> encrypted_group_secrets;
};

// MLS Key Package
struct KeyPackage {
    std::vector<uint8_t> version;
    std::vector<uint8_t> cipher_suite;
    std::vector<uint8_t> init_key;
    LeafNode leaf_node;
    std::vector<uint8_t> extensions;
    std::vector<uint8_t> signature;
};

// MLS Group
struct Group {
    std::vector<uint8_t> group_id;
    uint64_t epoch;
    CipherSuite cipher_suite;
    GroupState state;
    GroupContext context;
    std::vector<TreeNode> tree;
    std::vector<uint8_t> group_secret;
    std::vector<uint8_t> epoch_secret;
    std::vector<uint8_t> sender_ratchet_key;
    std::vector<uint8_t> confirmed_transcript_hash;
    std::vector<uint8_t> interim_transcript_hash;
    std::vector<uint8_t> group_context_extensions;
    std::vector<uint8_t> group_context_extensions_signature;
    std::vector<uint8_t> group_context_extensions_public_key;
    std::vector<uint8_t> group_context_extensions_private_key;
    std::vector<uint8_t> group_context_extensions_nonce;
    std::vector<uint8_t> group_context_extensions_ciphertext;
    std::vector<uint8_t> group_context_extensions_tag;
    std::vector<uint8_t> group_context_extensions_aad;
    std::vector<uint8_t> group_context_extensions_plaintext;
    std::vector<uint8_t> group_context_extensions_key;
    std::vector<uint8_t> group_context_extensions_iv;
    std::vector<uint8_t> group_context_extensions_salt;
    std::vector<uint8_t> group_context_extensions_info;
    std::vector<uint8_t> group_context_extensions_commitment;
    std::vector<uint8_t> group_context_extensions_opening;
    std::vector<uint8_t> group_context_extensions_proof;
    std::vector<uint8_t> group_context_extensions_verification_key;
    std::vector<uint8_t> group_context_extensions_signing_key;
    std::vector<uint8_t> group_context_extensions_encryption_key;
    std::vector<uint8_t> group_context_extensions_decryption_key;
    std::vector<uint8_t> group_context_extensions_mac_key;
    std::vector<uint8_t> group_context_extensions_hmac_key;
    std::vector<uint8_t> group_context_extensions_kdf_key;
    std::vector<uint8_t> group_context_extensions_aead_key;
    std::vector<uint8_t> group_context_extensions_hash_key;
    std::vector<uint8_t> group_context_extensions_signature_key;
    std::vector<uint8_t> group_context_extensions_verification_key_public;
    std::vector<uint8_t> group_context_extensions_signing_key_private;
    std::vector<uint8_t> group_context_extensions_encryption_key_public;
    std::vector<uint8_t> group_context_extensions_decryption_key_private;
    std::vector<uint8_t> group_context_extensions_mac_key_public;
    std::vector<uint8_t> group_context_extensions_hmac_key_private;
    std::vector<uint8_t> group_context_extensions_kdf_key_public;
    std::vector<uint8_t> group_context_extensions_aead_key_private;
    std::vector<uint8_t> group_context_extensions_hash_key_public;
    std::vector<uint8_t> group_context_extensions_signature_key_private;
    std::vector<uint8_t> group_context_extensions_verification_key_private;
    std::vector<uint8_t> group_context_extensions_signing_key_public;
    std::vector<uint8_t> group_context_extensions_encryption_key_private;
    std::vector<uint8_t> group_context_extensions_decryption_key_public;
    std::vector<uint8_t> group_context_extensions_mac_key_private;
    std::vector<uint8_t> group_context_extensions_hmac_key_public;
    std::vector<uint8_t> group_context_extensions_kdf_key_private;
    std::vector<uint8_t> group_context_extensions_aead_key_public;
    std::vector<uint8_t> group_context_extensions_hash_key_private;
    std::vector<uint8_t> group_context_extensions_signature_key_public;
};

// MLS Protocol Implementation
class MLSProtocol {
public:
    MLSProtocol();
    ~MLSProtocol();

    // Group Management
    std::vector<uint8_t> create_group(const std::vector<uint8_t>& group_id, 
                                     CipherSuite cipher_suite,
                                     const std::vector<uint8_t>& group_context_extensions);
    
    std::vector<uint8_t> add_member(const std::vector<uint8_t>& group_id,
                                   const KeyPackage& key_package);
    
    std::vector<uint8_t> remove_member(const std::vector<uint8_t>& group_id,
                                      uint32_t member_index);
    
    std::vector<uint8_t> update_group(const std::vector<uint8_t>& group_id,
                                     const std::vector<uint8_t>& group_context_extensions);
    
    // Group Size Management
    uint32_t get_group_member_count(const std::vector<uint8_t>& group_id);
    bool can_add_member(const std::vector<uint8_t>& group_id);
    GroupSizeStatus get_group_size_status(const std::vector<uint8_t>& group_id);
    std::vector<uint8_t> optimize_group_performance(const std::vector<uint8_t>& group_id);
    
    // Message Encryption/Decryption
    std::vector<uint8_t> encrypt_message(const std::vector<uint8_t>& group_id,
                                        const std::vector<uint8_t>& plaintext,
                                        const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> decrypt_message(const std::vector<uint8_t>& group_id,
                                        const std::vector<uint8_t>& ciphertext,
                                        const std::vector<uint8_t>& aad);
    
    // Key Management
    std::vector<uint8_t> derive_epoch_keys(const std::vector<uint8_t>& group_id);
    std::vector<uint8_t> derive_sender_ratchet_key(const std::vector<uint8_t>& group_id);
    std::vector<uint8_t> derive_group_secret(const std::vector<uint8_t>& group_id);
    
    // Tree Operations
    std::vector<uint8_t> compute_tree_hash(const std::vector<TreeNode>& tree);
    std::vector<uint8_t> compute_path_hash(const std::vector<uint8_t>& path);
    std::vector<uint8_t> compute_leaf_hash(const LeafNode& leaf);
    
    // Cryptographic Operations
    std::vector<uint8_t> hkdf_expand(const std::vector<uint8_t>& prk,
                                    const std::vector<uint8_t>& info,
                                    size_t length);
    
    std::vector<uint8_t> hkdf_extract(const std::vector<uint8_t>& salt,
                                     const std::vector<uint8_t>& ikm);
    
    std::vector<uint8_t> encrypt_with_key(const std::vector<uint8_t>& key,
                                         const std::vector<uint8_t>& nonce,
                                         const std::vector<uint8_t>& plaintext,
                                         const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> decrypt_with_key(const std::vector<uint8_t>& key,
                                         const std::vector<uint8_t>& nonce,
                                         const std::vector<uint8_t>& ciphertext,
                                         const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> sign_message(const std::vector<uint8_t>& private_key,
                                     const std::vector<uint8_t>& message);
    
    bool verify_signature(const std::vector<uint8_t>& public_key,
                         const std::vector<uint8_t>& message,
                         const std::vector<uint8_t>& signature);
    
    // Serialization
    std::vector<uint8_t> serialize_group(const Group& group);
    std::optional<Group> deserialize_group(const std::vector<uint8_t>& data);
    
    std::vector<uint8_t> serialize_key_package(const KeyPackage& key_package);
    std::optional<KeyPackage> deserialize_key_package(const std::vector<uint8_t>& data);
    
    std::vector<uint8_t> serialize_welcome(const Welcome& welcome);
    std::optional<Welcome> deserialize_welcome(const std::vector<uint8_t>& data);
    
    std::vector<uint8_t> serialize_commit(const Commit& commit);
    std::optional<Commit> deserialize_commit(const std::vector<uint8_t>& data);

private:
    std::unordered_map<std::string, Group> groups_;
    std::unordered_map<std::string, std::vector<uint8_t>> group_secrets_;
    std::unordered_map<std::string, std::vector<uint8_t>> epoch_secrets_;
    std::unordered_map<std::string, std::vector<uint8_t>> sender_ratchet_keys_;
    
    // Helper methods
    std::vector<uint8_t> generate_random_bytes(size_t length);
    std::vector<uint8_t> compute_hash(const std::vector<uint8_t>& data);
    std::vector<uint8_t> compute_hmac(const std::vector<uint8_t>& key,
                                     const std::vector<uint8_t>& data);
    
    // Tree operations
    void update_tree_hash(Group& group);
    void update_path_hash(Group& group, uint32_t leaf_index);
    void update_leaf_hash(Group& group, uint32_t leaf_index);
    
    // Key derivation
    std::vector<uint8_t> derive_key(const std::vector<uint8_t>& secret,
                                   const std::vector<uint8_t>& label,
                                   const std::vector<uint8_t>& context,
                                   size_t length);
    
    // Cryptographic primitives
    std::vector<uint8_t> aes_gcm_encrypt(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& nonce,
                                        const std::vector<uint8_t>& plaintext,
                                        const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> aes_gcm_decrypt(const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& nonce,
                                        const std::vector<uint8_t>& ciphertext,
                                        const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> chacha20_poly1305_encrypt(const std::vector<uint8_t>& key,
                                                   const std::vector<uint8_t>& nonce,
                                                   const std::vector<uint8_t>& plaintext,
                                                   const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> chacha20_poly1305_decrypt(const std::vector<uint8_t>& key,
                                                   const std::vector<uint8_t>& nonce,
                                                   const std::vector<uint8_t>& ciphertext,
                                                   const std::vector<uint8_t>& aad);
    
    // Ed25519 operations
    std::vector<uint8_t> ed25519_sign(const std::vector<uint8_t>& private_key,
                                     const std::vector<uint8_t>& message);
    
    bool ed25519_verify(const std::vector<uint8_t>& public_key,
                       const std::vector<uint8_t>& message,
                       const std::vector<uint8_t>& signature);
    
    // X25519 operations
    std::vector<uint8_t> x25519_derive_shared_secret(const std::vector<uint8_t>& private_key,
                                                     const std::vector<uint8_t>& public_key);
    
    std::pair<std::vector<uint8_t>, std::vector<uint8_t>> x25519_generate_keypair();
};

} // namespace sonet::mls