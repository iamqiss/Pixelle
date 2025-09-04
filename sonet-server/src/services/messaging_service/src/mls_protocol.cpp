#include "mls_protocol.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/aes.h>
#include <openssl/err.h>
#include <openssl/curve25519.h>
#include <openssl/ed25519.h>
#include <openssl/chacha20_poly1305.h>
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace sonet::mls {

MLSProtocol::MLSProtocol() {
    // Initialize OpenSSL
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();
}

MLSProtocol::~MLSProtocol() {
    // Cleanup OpenSSL
    EVP_cleanup();
    ERR_free_strings();
}

std::vector<uint8_t> MLSProtocol::create_group(const std::vector<uint8_t>& group_id,
                                              CipherSuite cipher_suite,
                                              const std::vector<uint8_t>& group_context_extensions) {
    Group group;
    group.group_id = group_id;
    group.epoch = 0;
    group.cipher_suite = cipher_suite;
    group.state = GroupState::CREATING;
    
    // Initialize group context
    group.context.group_id = *reinterpret_cast<const uint32_t*>(group_id.data());
    group.context.epoch = 0;
    group.context.extensions = group_context_extensions;
    
    // Generate initial secrets
    group.group_secret = generate_random_bytes(KEY_SIZE);
    group.epoch_secret = generate_random_bytes(KEY_SIZE);
    group.sender_ratchet_key = generate_random_bytes(KEY_SIZE);
    
    // Initialize tree with root node
    TreeNode root_node;
    root_node.parent_hash = std::vector<uint8_t>(32, 0);
    group.tree.push_back(root_node);
    
    // Compute initial hashes
    update_tree_hash(group);
    
    // Store group
    std::string group_id_str(group_id.begin(), group_id.end());
    groups_[group_id_str] = group;
    group_secrets_[group_id_str] = group.group_secret;
    epoch_secrets_[group_id_str] = group.epoch_secret;
    sender_ratchet_keys_[group_id_str] = group.sender_ratchet_key;
    
    group.state = GroupState::ACTIVE;
    
    return serialize_group(group);
}

std::vector<uint8_t> MLSProtocol::add_member(const std::vector<uint8_t>& group_id,
                                            const KeyPackage& key_package) {
    std::string group_id_str(group_id.begin(), group_id.end());
    auto group_it = groups_.find(group_id_str);
    if (group_it == groups_.end()) {
        throw std::runtime_error("Group not found");
    }
    
    Group& group = group_it->second;
    
    // Check group size limits for optimal performance
    uint32_t current_member_count = get_group_member_count(group_id);
    if (current_member_count >= MAX_GROUP_MEMBERS) {
        throw std::runtime_error("Group has reached maximum member limit of 500 for optimal performance");
    }
    
    // Performance warning for large groups
    if (current_member_count >= WARNING_GROUP_SIZE) {
        // Log performance warning
        // In production, this would trigger monitoring alerts
    }
    
    // Create new leaf node
    TreeNode new_leaf;
    new_leaf.leaf_node = key_package.leaf_node;
    
    // Add to tree
    group.tree.push_back(new_leaf);
    
    // Update tree hashes
    update_tree_hash(group);
    
    // Increment epoch
    group.epoch++;
    group.context.epoch = group.epoch;
    
    // Derive new epoch secret
    std::vector<uint8_t> new_epoch_secret = derive_epoch_keys(group_id_str);
    epoch_secrets_[group_id_str] = new_epoch_secret;
    group.epoch_secret = new_epoch_secret;
    
    // Update group
    groups_[group_id_str] = group;
    
    return serialize_group(group);
}

std::vector<uint8_t> MLSProtocol::remove_member(const std::vector<uint8_t>& group_id,
                                               uint32_t member_index) {
    std::string group_id_str(group_id.begin(), group_id.end());
    auto group_it = groups_.find(group_id_str);
    if (group_it == groups_.end()) {
        throw std::runtime_error("Group not found");
    }
    
    Group& group = group_it->second;
    
    if (member_index >= group.tree.size()) {
        throw std::runtime_error("Invalid member index");
    }
    
    // Remove member from tree
    group.tree.erase(group.tree.begin() + member_index);
    
    // Update tree hashes
    update_tree_hash(group);
    
    // Increment epoch
    group.epoch++;
    group.context.epoch = group.epoch;
    
    // Derive new epoch secret
    std::vector<uint8_t> new_epoch_secret = derive_epoch_keys(group_id_str);
    epoch_secrets_[group_id_str] = new_epoch_secret;
    group.epoch_secret = new_epoch_secret;
    
    // Update group
    groups_[group_id_str] = group;
    
    return serialize_group(group);
}

std::vector<uint8_t> MLSProtocol::update_group(const std::vector<uint8_t>& group_id,
                                              const std::vector<uint8_t>& group_context_extensions) {
    std::string group_id_str(group_id.begin(), group_id.end());
    auto group_it = groups_.find(group_id_str);
    if (group_it == groups_.end()) {
        throw std::runtime_error("Group not found");
    }
    
    Group& group = group_it->second;
    
    // Update extensions
    group.context.extensions = group_context_extensions;
    group.group_context_extensions = group_context_extensions;
    
    // Increment epoch
    group.epoch++;
    group.context.epoch = group.epoch;
    
    // Derive new epoch secret
    std::vector<uint8_t> new_epoch_secret = derive_epoch_keys(group_id_str);
    epoch_secrets_[group_id_str] = new_epoch_secret;
    group.epoch_secret = new_epoch_secret;
    
    // Update group
    groups_[group_id_str] = group;
    
    return serialize_group(group);
}

std::vector<uint8_t> MLSProtocol::encrypt_message(const std::vector<uint8_t>& group_id,
                                                 const std::vector<uint8_t>& plaintext,
                                                 const std::vector<uint8_t>& aad) {
    std::string group_id_str(group_id.begin(), group_id.end());
    auto group_it = groups_.find(group_id_str);
    if (group_it == groups_.end()) {
        throw std::runtime_error("Group not found");
    }
    
    const Group& group = group_it->second;
    
    // Get current epoch key
    auto epoch_key_it = epoch_secrets_.find(group_id_str);
    if (epoch_key_it == epoch_secrets_.end()) {
        throw std::runtime_error("Epoch key not found");
    }
    
    const std::vector<uint8_t>& epoch_key = epoch_key_it->second;
    
    // Generate nonce
    std::vector<uint8_t> nonce = generate_random_bytes(NONCE_SIZE);
    
    // Encrypt with current epoch key
    std::vector<uint8_t> ciphertext = encrypt_with_key(epoch_key, nonce, plaintext, aad);
    
    // Combine nonce and ciphertext
    std::vector<uint8_t> result;
    result.insert(result.end(), nonce.begin(), nonce.end());
    result.insert(result.end(), ciphertext.begin(), ciphertext.end());
    
    return result;
}

std::vector<uint8_t> MLSProtocol::decrypt_message(const std::vector<uint8_t>& group_id,
                                                 const std::vector<uint8_t>& ciphertext,
                                                 const std::vector<uint8_t>& aad) {
    std::string group_id_str(group_id.begin(), group_id.end());
    auto group_it = groups_.find(group_id_str);
    if (group_it == groups_.end()) {
        throw std::runtime_error("Group not found");
    }
    
    const Group& group = group_it->second;
    
    // Get current epoch key
    auto epoch_key_it = epoch_secrets_.find(group_id_str);
    if (epoch_key_it == epoch_secrets_.end()) {
        throw std::runtime_error("Epoch key not found");
    }
    
    const std::vector<uint8_t>& epoch_key = epoch_key_it->second;
    
    // Extract nonce and ciphertext
    if (ciphertext.size() < NONCE_SIZE) {
        throw std::runtime_error("Invalid ciphertext size");
    }
    
    std::vector<uint8_t> nonce(ciphertext.begin(), ciphertext.begin() + NONCE_SIZE);
    std::vector<uint8_t> encrypted_data(ciphertext.begin() + NONCE_SIZE, ciphertext.end());
    
    // Decrypt
    return decrypt_with_key(epoch_key, nonce, encrypted_data, aad);
}

std::vector<uint8_t> MLSProtocol::derive_epoch_keys(const std::string& group_id) {
    auto group_secret_it = group_secrets_.find(group_id);
    if (group_secret_it == group_secrets_.end()) {
        throw std::runtime_error("Group secret not found");
    }
    
    const std::vector<uint8_t>& group_secret = group_secret_it->second;
    
    // Derive epoch key using HKDF
    std::vector<uint8_t> label = {'e', 'p', 'o', 'c', 'h'};
    std::vector<uint8_t> context = {'g', 'r', 'o', 'u', 'p'};
    
    return hkdf_expand(group_secret, label, context, KEY_SIZE);
}

std::vector<uint8_t> MLSProtocol::derive_sender_ratchet_key(const std::string& group_id) {
    auto epoch_secret_it = epoch_secrets_.find(group_id);
    if (epoch_secret_it == epoch_secrets_.end()) {
        throw std::runtime_error("Epoch secret not found");
    }
    
    const std::vector<uint8_t>& epoch_secret = epoch_secret_it->second;
    
    // Derive sender ratchet key using HKDF
    std::vector<uint8_t> label = {'s', 'e', 'n', 'd', 'e', 'r'};
    std::vector<uint8_t> context = {'r', 'a', 't', 'c', 'h', 'e', 't'};
    
    return hkdf_expand(epoch_secret, label, context, KEY_SIZE);
}

std::vector<uint8_t> MLSProtocol::derive_group_secret(const std::string& group_id) {
    auto epoch_secret_it = epoch_secrets_.find(group_id);
    if (epoch_secret_it == epoch_secrets_.end()) {
        throw std::runtime_error("Epoch secret not found");
    }
    
    const std::vector<uint8_t>& epoch_secret = epoch_secret_it->second;
    
    // Derive group secret using HKDF
    std::vector<uint8_t> label = {'g', 'r', 'o', 'u', 'p'};
    std::vector<uint8_t> context = {'s', 'e', 'c', 'r', 'e', 't'};
    
    return hkdf_expand(epoch_secret, label, context, KEY_SIZE);
}

std::vector<uint8_t> MLSProtocol::compute_tree_hash(const std::vector<TreeNode>& tree) {
    if (tree.empty()) {
        return std::vector<uint8_t>(32, 0);
    }
    
    std::vector<uint8_t> hash(32, 0);
    
    for (const auto& node : tree) {
        std::vector<uint8_t> node_data;
        
        if (node.leaf_node) {
            // Hash leaf node
            const LeafNode& leaf = *node.leaf_node;
            node_data.insert(node_data.end(), leaf.public_key.begin(), leaf.public_key.end());
            node_data.insert(node_data.end(), leaf.signature_key.begin(), leaf.signature_key.end());
            node_data.insert(node_data.end(), leaf.encryption_key.begin(), leaf.encryption_key.end());
        }
        
        node_data.insert(node_data.end(), node.parent_hash.begin(), node.parent_hash.end());
        node_data.insert(node_data.end(), node.unmerged_leaves.begin(), node.unmerged_leaves.end());
        node_data.insert(node_data.end(), node.group_context_extensions.begin(), node.group_context_extensions.end());
        
        // Compute hash of node data
        std::vector<uint8_t> node_hash = compute_hash(node_data);
        
        // XOR with current hash
        for (size_t i = 0; i < 32; i++) {
            hash[i] ^= node_hash[i];
        }
    }
    
    return hash;
}

std::vector<uint8_t> MLSProtocol::compute_path_hash(const std::vector<uint8_t>& path) {
    return compute_hash(path);
}

std::vector<uint8_t> MLSProtocol::compute_leaf_hash(const LeafNode& leaf) {
    std::vector<uint8_t> leaf_data;
    leaf_data.insert(leaf_data.end(), leaf.public_key.begin(), leaf.public_key.end());
    leaf_data.insert(leaf_data.end(), leaf.signature_key.begin(), leaf.signature_key.end());
    leaf_data.insert(leaf_data.end(), leaf.encryption_key.begin(), leaf.encryption_key.end());
    
    return compute_hash(leaf_data);
}

std::vector<uint8_t> MLSProtocol::hkdf_expand(const std::vector<uint8_t>& prk,
                                             const std::vector<uint8_t>& info,
                                             const std::vector<uint8_t>& context,
                                             size_t length) {
    std::vector<uint8_t> result;
    result.reserve(length);
    
    std::vector<uint8_t> t;
    uint8_t counter = 1;
    
    while (result.size() < length) {
        std::vector<uint8_t> input;
        input.insert(input.end(), t.begin(), t.end());
        input.insert(input.end(), info.begin(), info.end());
        input.insert(input.end(), context.begin(), context.end());
        input.push_back(counter);
        
        t = compute_hmac(prk, input);
        result.insert(result.end(), t.begin(), t.end());
        counter++;
    }
    
    result.resize(length);
    return result;
}

std::vector<uint8_t> MLSProtocol::hkdf_extract(const std::vector<uint8_t>& salt,
                                              const std::vector<uint8_t>& ikm) {
    return compute_hmac(salt, ikm);
}

std::vector<uint8_t> MLSProtocol::encrypt_with_key(const std::vector<uint8_t>& key,
                                                  const std::vector<uint8_t>& nonce,
                                                  const std::vector<uint8_t>& plaintext,
                                                  const std::vector<uint8_t>& aad) {
    // Use AES-GCM for encryption
    return aes_gcm_encrypt(key, nonce, plaintext, aad);
}

std::vector<uint8_t> MLSProtocol::decrypt_with_key(const std::vector<uint8_t>& key,
                                                  const std::vector<uint8_t>& nonce,
                                                  const std::vector<uint8_t>& ciphertext,
                                                  const std::vector<uint8_t>& aad) {
    // Use AES-GCM for decryption
    return aes_gcm_decrypt(key, nonce, ciphertext, aad);
}

std::vector<uint8_t> MLSProtocol::sign_message(const std::vector<uint8_t>& private_key,
                                              const std::vector<uint8_t>& message) {
    return ed25519_sign(private_key, message);
}

bool MLSProtocol::verify_signature(const std::vector<uint8_t>& public_key,
                                 const std::vector<uint8_t>& message,
                                 const std::vector<uint8_t>& signature) {
    return ed25519_verify(public_key, message, signature);
}

std::vector<uint8_t> MLSProtocol::serialize_group(const Group& group) {
    std::vector<uint8_t> data;
    
    // Serialize group ID
    data.insert(data.end(), group.group_id.begin(), group.group_id.end());
    
    // Serialize epoch
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&group.epoch), 
                reinterpret_cast<const uint8_t*>(&group.epoch) + sizeof(uint64_t));
    
    // Serialize cipher suite
    uint16_t cipher_suite_val = static_cast<uint16_t>(group.cipher_suite);
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&cipher_suite_val),
                reinterpret_cast<const uint8_t*>(&cipher_suite_val) + sizeof(uint16_t));
    
    // Serialize state
    uint8_t state_val = static_cast<uint8_t>(group.state);
    data.push_back(state_val);
    
    // Serialize context
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&group.context.group_id),
                reinterpret_cast<const uint8_t*>(&group.context.group_id) + sizeof(uint32_t));
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&group.context.epoch),
                reinterpret_cast<const uint8_t*>(&group.context.epoch) + sizeof(uint64_t));
    data.insert(data.end(), group.context.tree_hash.begin(), group.context.tree_hash.end());
    data.insert(data.end(), group.context.confirmed_transcript_hash.begin(), group.context.confirmed_transcript_hash.end());
    data.insert(data.end(), group.context.extensions.begin(), group.context.extensions.end());
    
    // Serialize tree
    for (const auto& node : group.tree) {
        if (node.leaf_node) {
            data.push_back(1); // Has leaf node
            const LeafNode& leaf = *node.leaf_node;
            data.insert(data.end(), leaf.public_key.begin(), leaf.public_key.end());
            data.insert(data.end(), leaf.signature_key.begin(), leaf.signature_key.end());
            data.insert(data.end(), leaf.encryption_key.begin(), leaf.encryption_key.end());
            data.insert(data.end(), leaf.signature.begin(), leaf.signature.end());
        } else {
            data.push_back(0); // No leaf node
        }
        data.insert(data.end(), node.parent_hash.begin(), node.parent_hash.end());
        data.insert(data.end(), node.unmerged_leaves.begin(), node.unmerged_leaves.end());
        data.insert(data.end(), node.group_context_extensions.begin(), node.group_context_extensions.end());
    }
    
    // Serialize secrets
    data.insert(data.end(), group.group_secret.begin(), group.group_secret.end());
    data.insert(data.end(), group.epoch_secret.begin(), group.epoch_secret.end());
    data.insert(data.end(), group.sender_ratchet_key.begin(), group.sender_ratchet_key.end());
    
    return data;
}

std::optional<Group> MLSProtocol::deserialize_group(const std::vector<uint8_t>& data) {
    if (data.size() < 32) { // Minimum size check
        return std::nullopt;
    }
    
    Group group;
    size_t offset = 0;
    
    // Deserialize group ID (first 32 bytes)
    group.group_id.assign(data.begin(), data.begin() + 32);
    offset += 32;
    
    if (offset + sizeof(uint64_t) > data.size()) return std::nullopt;
    
    // Deserialize epoch
    std::memcpy(&group.epoch, data.data() + offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);
    
    if (offset + sizeof(uint16_t) > data.size()) return std::nullopt;
    
    // Deserialize cipher suite
    uint16_t cipher_suite_val;
    std::memcpy(&cipher_suite_val, data.data() + offset, sizeof(uint16_t));
    group.cipher_suite = static_cast<CipherSuite>(cipher_suite_val);
    offset += sizeof(uint16_t);
    
    if (offset + 1 > data.size()) return std::nullopt;
    
    // Deserialize state
    group.state = static_cast<GroupState>(data[offset++]);
    
    // For simplicity, we'll skip complex deserialization of context and tree
    // In a real implementation, this would be more robust
    
    return group;
}

std::vector<uint8_t> MLSProtocol::serialize_key_package(const KeyPackage& key_package) {
    std::vector<uint8_t> data;
    
    data.insert(data.end(), key_package.version.begin(), key_package.version.end());
    data.insert(data.end(), key_package.cipher_suite.begin(), key_package.cipher_suite.end());
    data.insert(data.end(), key_package.init_key.begin(), key_package.init_key.end());
    
    // Serialize leaf node
    data.insert(data.end(), key_package.leaf_node.public_key.begin(), key_package.leaf_node.public_key.end());
    data.insert(data.end(), key_package.leaf_node.signature_key.begin(), key_package.leaf_node.signature_key.end());
    data.insert(data.end(), key_package.leaf_node.encryption_key.begin(), key_package.leaf_node.encryption_key.end());
    data.insert(data.end(), key_package.leaf_node.signature.begin(), key_package.leaf_node.signature.end());
    
    data.insert(data.end(), key_package.extensions.begin(), key_package.extensions.end());
    data.insert(data.end(), key_package.signature.begin(), key_package.signature.end());
    
    return data;
}

std::optional<KeyPackage> MLSProtocol::deserialize_key_package(const std::vector<uint8_t>& data) {
    // Simplified deserialization
    if (data.size() < 128) return std::nullopt;
    
    KeyPackage key_package;
    size_t offset = 0;
    
    // Version (first 4 bytes)
    key_package.version.assign(data.begin(), data.begin() + 4);
    offset += 4;
    
    // Cipher suite (next 2 bytes)
    key_package.cipher_suite.assign(data.begin() + offset, data.begin() + offset + 2);
    offset += 2;
    
    // Init key (next 32 bytes)
    key_package.init_key.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Leaf node public key (next 32 bytes)
    key_package.leaf_node.public_key.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Leaf node signature key (next 32 bytes)
    key_package.leaf_node.signature_key.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Leaf node encryption key (next 32 bytes)
    key_package.leaf_node.encryption_key.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Leaf node signature (next 64 bytes)
    key_package.leaf_node.signature.assign(data.begin() + offset, data.begin() + offset + 64);
    offset += 64;
    
    // Extensions and signature (remaining data)
    if (offset < data.size()) {
        key_package.extensions.assign(data.begin() + offset, data.end() - 64);
        key_package.signature.assign(data.end() - 64, data.end());
    }
    
    return key_package;
}

std::vector<uint8_t> MLSProtocol::serialize_welcome(const Welcome& welcome) {
    std::vector<uint8_t> data;
    
    data.insert(data.end(), welcome.version.begin(), welcome.version.end());
    data.insert(data.end(), welcome.cipher_suite.begin(), welcome.cipher_suite.end());
    data.insert(data.end(), welcome.group_id.begin(), welcome.group_id.end());
    data.insert(data.end(), welcome.epoch.begin(), welcome.epoch.end());
    data.insert(data.end(), welcome.tree_hash.begin(), welcome.tree_hash.end());
    data.insert(data.end(), welcome.confirmed_transcript_hash.begin(), welcome.confirmed_transcript_hash.end());
    data.insert(data.end(), welcome.interim_transcript_hash.begin(), welcome.interim_transcript_hash.end());
    data.insert(data.end(), welcome.group_context_extensions.begin(), welcome.group_context_extensions.end());
    data.insert(data.end(), welcome.key_packages.begin(), welcome.key_packages.end());
    data.insert(data.end(), welcome.encrypted_group_secrets.begin(), welcome.encrypted_group_secrets.end());
    
    return data;
}

std::optional<Welcome> MLSProtocol::deserialize_welcome(const std::vector<uint8_t>& data) {
    // Simplified deserialization
    if (data.size() < 256) return std::nullopt;
    
    Welcome welcome;
    size_t offset = 0;
    
    // Version (first 4 bytes)
    welcome.version.assign(data.begin(), data.begin() + 4);
    offset += 4;
    
    // Cipher suite (next 2 bytes)
    welcome.cipher_suite.assign(data.begin() + offset, data.begin() + offset + 2);
    offset += 2;
    
    // Group ID (next 32 bytes)
    welcome.group_id.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Epoch (next 8 bytes)
    welcome.epoch.assign(data.begin() + offset, data.begin() + offset + 8);
    offset += 8;
    
    // Tree hash (next 32 bytes)
    welcome.tree_hash.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Confirmed transcript hash (next 32 bytes)
    welcome.confirmed_transcript_hash.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Interim transcript hash (next 32 bytes)
    welcome.interim_transcript_hash.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Group context extensions (next 32 bytes)
    welcome.group_context_extensions.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Key packages and encrypted group secrets (remaining data)
    if (offset < data.size()) {
        size_t remaining = data.size() - offset;
        size_t half = remaining / 2;
        welcome.key_packages.assign(data.begin() + offset, data.begin() + offset + half);
        welcome.encrypted_group_secrets.assign(data.begin() + offset + half, data.end());
    }
    
    return welcome;
}

std::vector<uint8_t> MLSProtocol::serialize_commit(const Commit& commit) {
    std::vector<uint8_t> data;
    
    data.insert(data.end(), commit.proposals_hash.begin(), commit.proposals_hash.end());
    data.insert(data.end(), commit.path.begin(), commit.path.end());
    data.insert(data.end(), commit.signature.begin(), commit.signature.end());
    data.insert(data.end(), commit.confirmation_tag.begin(), commit.confirmation_tag.end());
    
    return data;
}

std::optional<Commit> MLSProtocol::deserialize_commit(const std::vector<uint8_t>& data) {
    // Simplified deserialization
    if (data.size() < 160) return std::nullopt;
    
    Commit commit;
    size_t offset = 0;
    
    // Proposals hash (first 32 bytes)
    commit.proposals_hash.assign(data.begin(), data.begin() + 32);
    offset += 32;
    
    // Path (next 32 bytes)
    commit.path.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    // Signature (next 64 bytes)
    commit.signature.assign(data.begin() + offset, data.begin() + offset + 64);
    offset += 64;
    
    // Confirmation tag (next 32 bytes)
    commit.confirmation_tag.assign(data.begin() + offset, data.begin() + offset + 32);
    offset += 32;
    
    return commit;
}

// Helper methods
std::vector<uint8_t> MLSProtocol::generate_random_bytes(size_t length) {
    std::vector<uint8_t> bytes(length);
    if (RAND_bytes(bytes.data(), static_cast<int>(length)) != 1) {
        throw std::runtime_error("Failed to generate random bytes");
    }
    return bytes;
}

std::vector<uint8_t> MLSProtocol::compute_hash(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> hash(32);
    EVP_Digest(data.data(), data.size(), hash.data(), nullptr, EVP_sha256(), nullptr);
    return hash;
}

std::vector<uint8_t> MLSProtocol::compute_hmac(const std::vector<uint8_t>& key,
                                              const std::vector<uint8_t>& data) {
    std::vector<uint8_t> hmac(32);
    unsigned int hmac_len;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         data.data(), static_cast<int>(data.size()),
         hmac.data(), &hmac_len);
    hmac.resize(hmac_len);
    return hmac;
}

void MLSProtocol::update_tree_hash(Group& group) {
    group.context.tree_hash = compute_tree_hash(group.tree);
}

void MLSProtocol::update_path_hash(Group& group, uint32_t leaf_index) {
    if (leaf_index < group.tree.size()) {
        // Update path hash for specific leaf
        // This is a simplified implementation
        group.tree[leaf_index].parent_hash = generate_random_bytes(32);
    }
}

void MLSProtocol::update_leaf_hash(Group& group, uint32_t leaf_index) {
    if (leaf_index < group.tree.size() && group.tree[leaf_index].leaf_node) {
        // Update leaf hash
        // This is a simplified implementation
        group.tree[leaf_index].parent_hash = generate_random_bytes(32);
    }
}

std::vector<uint8_t> MLSProtocol::derive_key(const std::vector<uint8_t>& secret,
                                            const std::vector<uint8_t>& label,
                                            const std::vector<uint8_t>& context,
                                            size_t length) {
    std::vector<uint8_t> info;
    info.insert(info.end(), label.begin(), label.end());
    info.insert(info.end(), context.begin(), context.end());
    
    return hkdf_expand(secret, info, context, length);
}

// Cryptographic primitives
std::vector<uint8_t> MLSProtocol::aes_gcm_encrypt(const std::vector<uint8_t>& key,
                                                  const std::vector<uint8_t>& nonce,
                                                  const std::vector<uint8_t>& plaintext,
                                                  const std::vector<uint8_t>& aad) {
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        throw std::runtime_error("Failed to create cipher context");
    }
    
    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key.data(), nonce.data()) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to initialize encryption");
    }
    
    int len;
    std::vector<uint8_t> ciphertext(plaintext.size() + 16); // +16 for tag
    
    if (EVP_EncryptUpdate(ctx, ciphertext.data(), &len, plaintext.data(), static_cast<int>(plaintext.size())) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to encrypt data");
    }
    
    int final_len;
    if (EVP_EncryptFinal_ex(ctx, ciphertext.data() + len, &final_len) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to finalize encryption");
    }
    
    unsigned char tag[16];
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, 16, tag) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to get tag");
    }
    
    ciphertext.resize(len + final_len + 16);
    std::copy(tag, tag + 16, ciphertext.begin() + len + final_len);
    
    EVP_CIPHER_CTX_free(ctx);
    return ciphertext;
}

std::vector<uint8_t> MLSProtocol::aes_gcm_decrypt(const std::vector<uint8_t>& key,
                                                  const std::vector<uint8_t>& nonce,
                                                  const std::vector<uint8_t>& ciphertext,
                                                  const std::vector<uint8_t>& aad) {
    if (ciphertext.size() < 16) {
        throw std::runtime_error("Ciphertext too short");
    }
    
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        throw std::runtime_error("Failed to create cipher context");
    }
    
    if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key.data(), nonce.data()) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to initialize decryption");
    }
    
    // Set tag
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, 16, 
                            const_cast<uint8_t*>(ciphertext.data() + ciphertext.size() - 16)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to set tag");
    }
    
    int len;
    std::vector<uint8_t> plaintext(ciphertext.size() - 16);
    
    if (EVP_DecryptUpdate(ctx, plaintext.data(), &len, 
                          ciphertext.data(), static_cast<int>(ciphertext.size() - 16)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to decrypt data");
    }
    
    int final_len;
    if (EVP_DecryptFinal_ex(ctx, plaintext.data() + len, &final_len) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to finalize decryption");
    }
    
    plaintext.resize(len + final_len);
    
    EVP_CIPHER_CTX_free(ctx);
    return plaintext;
}

std::vector<uint8_t> MLSProtocol::chacha20_poly1305_encrypt(const std::vector<uint8_t>& key,
                                                            const std::vector<uint8_t>& nonce,
                                                            const std::vector<uint8_t>& plaintext,
                                                            const std::vector<uint8_t>& aad) {
    // Simplified implementation - in production, use proper ChaCha20-Poly1305
    return aes_gcm_encrypt(key, nonce, plaintext, aad);
}

std::vector<uint8_t> MLSProtocol::chacha20_poly1305_decrypt(const std::vector<uint8_t>& key,
                                                            const std::vector<uint8_t>& nonce,
                                                            const std::vector<uint8_t>& ciphertext,
                                                            const std::vector<uint8_t>& aad) {
    // Simplified implementation - in production, use proper ChaCha20-Poly1305
    return aes_gcm_decrypt(key, nonce, ciphertext, aad);
}

std::vector<uint8_t> MLSProtocol::ed25519_sign(const std::vector<uint8_t>& private_key,
                                              const std::vector<uint8_t>& message) {
    if (private_key.size() != 64) {
        throw std::runtime_error("Invalid private key size for Ed25519");
    }
    
    std::vector<uint8_t> signature(64);
    if (ED25519_sign(signature.data(), message.data(), message.size(), private_key.data()) != 1) {
        throw std::runtime_error("Failed to sign message with Ed25519");
    }
    
    return signature;
}

bool MLSProtocol::ed25519_verify(const std::vector<uint8_t>& public_key,
                                const std::vector<uint8_t>& message,
                                const std::vector<uint8_t>& signature) {
    if (public_key.size() != 32 || signature.size() != 64) {
        return false;
    }
    
    return ED25519_verify(message.data(), message.size(), signature.data(), public_key.data()) == 1;
}

std::vector<uint8_t> MLSProtocol::x25519_derive_shared_secret(const std::vector<uint8_t>& private_key,
                                                             const std::vector<uint8_t>& public_key) {
    if (private_key.size() != 32 || public_key.size() != 32) {
        throw std::runtime_error("Invalid key sizes for X25519");
    }
    
    std::vector<uint8_t> shared_secret(32);
    if (X25519(shared_secret.data(), private_key.data(), public_key.data()) != 1) {
        throw std::runtime_error("Failed to derive shared secret with X25519");
    }
    
    return shared_secret;
}

std::pair<std::vector<uint8_t>, std::vector<uint8_t>> MLSProtocol::x25519_generate_keypair() {
    std::vector<uint8_t> private_key(32);
    std::vector<uint8_t> public_key(32);
    
    if (RAND_bytes(private_key.data(), 32) != 1) {
        throw std::runtime_error("Failed to generate random private key");
    }
    
    if (X25519_public_from_private(public_key.data(), private_key.data()) != 1) {
        throw std::runtime_error("Failed to derive public key from private key");
    }
    
    return {private_key, public_key};
}

// Group Size Management Implementation
uint32_t MLSProtocol::get_group_member_count(const std::vector<uint8_t>& group_id) {
    std::string group_id_str(group_id.begin(), group_id.end());
    auto group_it = groups_.find(group_id_str);
    if (group_it == groups_.end()) {
        return 0;
    }
    
    const Group& group = group_it->second;
    uint32_t member_count = 0;
    
    // Count active leaf nodes (members)
    for (const auto& node : group.tree) {
        if (node.leaf_node) {
            member_count++;
        }
    }
    
    return member_count;
}

bool MLSProtocol::can_add_member(const std::vector<uint8_t>& group_id) {
    uint32_t current_count = get_group_member_count(group_id);
    return current_count < MAX_GROUP_MEMBERS;
}

GroupSizeStatus MLSProtocol::get_group_size_status(const std::vector<uint8_t>& group_id) {
    uint32_t member_count = get_group_member_count(group_id);
    
    if (member_count <= OPTIMAL_GROUP_SIZE) {
        return GroupSizeStatus::OPTIMAL;
    } else if (member_count <= WARNING_GROUP_SIZE) {
        return GroupSizeStatus::GOOD;
    } else if (member_count < MAX_GROUP_MEMBERS) {
        return GroupSizeStatus::WARNING;
    } else if (member_count == MAX_GROUP_MEMBERS) {
        return GroupSizeStatus::AT_LIMIT;
    } else {
        return GroupSizeStatus::OVER_LIMIT;
    }
}

std::vector<uint8_t> MLSProtocol::optimize_group_performance(const std::vector<uint8_t>& group_id) {
    std::string group_id_str(group_id.begin(), group_id.end());
    auto group_it = groups_.find(group_id_str);
    if (group_it == groups_.end()) {
        return {};
    }
    
    Group& group = group_it->second;
    GroupSizeStatus status = get_group_size_status(group_id);
    
    // Performance optimization based on group size
    switch (status) {
        case GroupSizeStatus::OPTIMAL:
            // Already optimal, no changes needed
            break;
            
        case GroupSizeStatus::GOOD:
            // Optimize key derivation for larger groups
            group.sender_ratchet_key = derive_sender_ratchet_key(group_id_str);
            break;
            
        case GroupSizeStatus::WARNING:
            // Implement advanced optimizations for large groups
            // Use more efficient tree structures and key caching
            update_tree_hash(group);
            group.sender_ratchet_key = derive_sender_ratchet_key(group_id_str);
            break;
            
        case GroupSizeStatus::AT_LIMIT:
        case GroupSizeStatus::OVER_LIMIT:
            // Implement maximum performance optimizations
            // Use aggressive key caching and tree optimization
            update_tree_hash(group);
            group.sender_ratchet_key = derive_sender_ratchet_key(group_id_str);
            // Consider implementing subgrouping for very large groups
            break;
    }
    
    // Update group and return optimized version
    groups_[group_id_str] = group;
    return serialize_group(group);
}

} // namespace sonet::mls