#include "include/e2e_encryption_manager.hpp"
#include "include/crypto_engine.hpp"
#include <json/json.h>
#include <sodium.h>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <chrono>

namespace sonet::messaging::crypto {

// KeyBundle JSON serialization
Json::Value KeyBundle::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["device_id"] = device_id;
    json["identity_key"] = identity_key.to_json();
    json["signed_prekey"] = signed_prekey.to_json();
    
    Json::Value otks_array;
    for (const auto& otk : one_time_prekeys) {
        otks_array.append(otk.to_json());
    }
    json["one_time_prekeys"] = otks_array;
    
    json["created_at"] = std::chrono::duration_cast<std::chrono::seconds>(
        created_at.time_since_epoch()).count();
    json["last_refresh"] = std::chrono::duration_cast<std::chrono::seconds>(
        last_refresh.time_since_epoch()).count();
    json["version"] = version;
    json["signature"] = signature;
    json["is_stale"] = is_stale;
    
    return json;
}

KeyBundle KeyBundle::from_json(const Json::Value& json) {
    KeyBundle bundle;
    bundle.user_id = json["user_id"].asString();
    bundle.device_id = json["device_id"].asString();
    bundle.identity_key = CryptoKey::from_json(json["identity_key"]);
    bundle.signed_prekey = CryptoKey::from_json(json["signed_prekey"]);
    
    for (const auto& otk_json : json["one_time_prekeys"]) {
        bundle.one_time_prekeys.push_back(CryptoKey::from_json(otk_json));
    }
    
    bundle.created_at = std::chrono::system_clock::from_time_t(json["created_at"].asInt64());
    bundle.last_refresh = std::chrono::system_clock::from_time_t(json["last_refresh"].asInt64());
    bundle.version = json["version"].asUInt();
    bundle.signature = json["signature"].asString();
    bundle.is_stale = json["is_stale"].asBool();
    
    return bundle;
}

// DeviceState JSON serialization
Json::Value DeviceState::to_json() const {
    Json::Value json;
    json["device_id"] = device_id;
    json["identity_key"] = identity_key.to_json();
    json["signed_prekey"] = signed_prekey.to_json();
    
    Json::Value otks_array;
    for (const auto& otk : one_time_prekeys) {
        otks_array.append(otk.to_json());
    }
    json["one_time_prekeys"] = otks_array;
    
    json["last_activity"] = std::chrono::duration_cast<std::chrono::seconds>(
        last_activity.time_since_epoch()).count();
    json["key_bundle_version"] = key_bundle_version;
    json["is_active"] = is_active;
    
    return json;
}

DeviceState DeviceState::from_json(const Json::Value& json) {
    DeviceState state;
    state.device_id = json["device_id"].asString();
    state.identity_key = CryptoKey::from_json(json["identity_key"]);
    state.signed_prekey = CryptoKey::from_json(json["signed_prekey"]);
    
    for (const auto& otk_json : json["one_time_prekeys"]) {
        state.one_time_prekeys.push_back(CryptoKey::from_json(otk_json));
    }
    
    state.last_activity = std::chrono::system_clock::from_time_t(json["last_activity"].asInt64());
    state.key_bundle_version = json["key_bundle_version"].asUInt();
    state.is_active = json["is_active"].asBool();
    
    return state;
}

// MLSGroupState JSON serialization
Json::Value MLSGroupState::to_json() const {
    Json::Value json;
    json["group_id"] = group_id;
    json["epoch_id"] = epoch_id;
    
    Json::Value members_array;
    for (const auto& member_id : member_ids) {
        members_array.append(member_id);
    }
    json["member_ids"] = members_array;
    
    json["group_key"] = group_key.to_json();
    
    Json::Value epoch_keys_array;
    for (const auto& epoch_key : epoch_keys) {
        epoch_keys_array.append(epoch_key.to_json());
    }
    json["epoch_keys"] = epoch_keys_array;
    
    json["created_at"] = std::chrono::duration_cast<std::chrono::seconds>(
        created_at.time_since_epoch()).count();
    json["last_epoch_change"] = std::chrono::duration_cast<std::chrono::seconds>(
        last_epoch_change.time_since_epoch()).count();
    json["epoch_number"] = epoch_number;
    json["is_active"] = is_active;
    
    return json;
}

MLSGroupState MLSGroupState::from_json(const Json::Value& json) {
    MLSGroupState state;
    state.group_id = json["group_id"].asString();
    state.epoch_id = json["epoch_id"].asString();
    
    for (const auto& member_id : json["member_ids"]) {
        state.member_ids.push_back(member_id.asString());
    }
    
    state.group_key = CryptoKey::from_json(json["group_key"]);
    
    for (const auto& epoch_key_json : json["epoch_keys"]) {
        state.epoch_keys.push_back(CryptoKey::from_json(epoch_key_json));
    }
    
    state.created_at = std::chrono::system_clock::from_time_t(json["created_at"].asInt64());
    state.last_epoch_change = std::chrono::system_clock::from_time_t(json["last_epoch_change"].asInt64());
    state.epoch_number = json["epoch_number"].asUInt();
    state.is_active = json["is_active"].asBool();
    
    return state;
}

// MLSMember JSON serialization
Json::Value MLSMember::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["device_id"] = device_id;
    json["identity_key"] = identity_key.to_json();
    json["leaf_key"] = leaf_key.to_json();
    json["leaf_index"] = leaf_index;
    json["joined_at"] = std::chrono::duration_cast<std::chrono::seconds>(
        joined_at.time_since_epoch()).count();
    json["is_active"] = is_active;
    
    return json;
}

MLSMember MLSMember::from_json(const Json::Value& json) {
    MLSMember member;
    member.user_id = json["user_id"].asString();
    member.device_id = json["device_id"].asString();
    member.identity_key = CryptoKey::from_json(json["identity_key"]);
    member.leaf_key = CryptoKey::from_json(json["leaf_key"]);
    member.leaf_index = json["leaf_index"].asUInt();
    member.joined_at = std::chrono::system_clock::from_time_t(json["joined_at"].asInt64());
    member.is_active = json["is_active"].asBool();
    
    return member;
}

// KeyLogEntry JSON serialization
Json::Value KeyLogEntry::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["device_id"] = device_id;
    json["operation"] = operation;
    json["old_key"] = old_key.to_json();
    json["new_key"] = new_key.to_json();
    json["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
        timestamp.time_since_epoch()).count();
    json["signature"] = signature;
    json["reason"] = reason;
    
    return json;
}

KeyLogEntry KeyLogEntry::from_json(const Json::Value& json) {
    KeyLogEntry entry;
    entry.user_id = json["user_id"].asString();
    entry.device_id = json["device_id"].asString();
    entry.operation = json["operation"].asString();
    entry.old_key = CryptoKey::from_json(json["old_key"]);
    entry.new_key = CryptoKey::from_json(json["new_key"]);
    entry.timestamp = std::chrono::system_clock::from_time_t(json["timestamp"].asInt64());
    entry.signature = json["signature"].asString();
    entry.reason = json["reason"].asString();
    
    return entry;
}

// TrustState JSON serialization
Json::Value TrustState::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["trusted_user_id"] = trusted_user_id;
    json["trust_level"] = trust_level;
    json["established_at"] = std::chrono::duration_cast<std::chrono::seconds>(
        established_at.time_since_epoch()).count();
    json["last_verified"] = std::chrono::duration_cast<std::chrono::seconds>(
        last_verified.time_since_epoch()).count();
    json["verification_method"] = verification_method;
    json["is_active"] = is_active;
    
    return json;
}

TrustState TrustState::from_json(const Json::Value& json) {
    TrustState state;
    state.user_id = json["user_id"].asString();
    state.trusted_user_id = json["trusted_user_id"].asString();
    state.trust_level = json["trust_level"].asString();
    state.established_at = std::chrono::system_clock::from_time_t(json["established_at"].asInt64());
    state.last_verified = std::chrono::system_clock::from_time_t(json["last_verified"].asInt64());
    state.verification_method = json["verification_method"].asString();
    state.is_active = json["is_active"].asBool();
    
    return state;
}

// E2EEncryptionManager Implementation
E2EEncryptionManager::E2EEncryptionManager(std::shared_ptr<CryptoEngine> crypto_engine)
    : crypto_engine_(crypto_engine) {
    
    // Start background cleanup thread
    cleanup_thread_ = std::thread(&E2EEncryptionManager::background_cleanup, this);
}

E2EEncryptionManager::~E2EEncryptionManager() {
    running_ = false;
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

// X3DH Protocol Completion
bool E2EEncryptionManager::rotate_one_time_prekeys(const std::string& user_id, uint32_t count) {
    std::lock_guard<std::mutex> lock(user_devices_mutex_);
    
    auto user_it = user_devices_.find(user_id);
    if (user_it == user_devices_.end()) {
        return false;
    }
    
    for (auto& [device_id, device_state] : user_it->second) {
        // Generate new one-time prekeys
        std::vector<CryptoKey> new_otks;
        for (uint32_t i = 0; i < count; ++i) {
            auto [otk_priv, otk_pub] = crypto_engine_->generate_keypair(
                KeyExchangeProtocol::X25519, user_id, device_id + "_otk_" + std::to_string(i));
            new_otks.push_back(*otk_pub);
        }
        
        device_state.one_time_prekeys = std::move(new_otks);
        device_state.key_bundle_version++;
        last_prekey_rotation_[user_id][device_id] = std::chrono::system_clock::now();
    }
    
    return true;
}

std::vector<CryptoKey> E2EEncryptionManager::get_one_time_prekeys(const std::string& user_id, uint32_t count) {
    std::lock_guard<std::mutex> lock(user_devices_mutex_);
    
    std::vector<CryptoKey> all_otks;
    auto user_it = user_devices_.find(user_id);
    if (user_it == user_devices_.end()) {
        return all_otks;
    }
    
    for (const auto& [device_id, device_state] : user_it->second) {
        for (const auto& otk : device_state.one_time_prekeys) {
            all_otks.push_back(otk);
            if (all_otks.size() >= count) {
                break;
            }
        }
        if (all_otks.size() >= count) {
            break;
        }
    }
    
    return all_otks;
}

bool E2EEncryptionManager::publish_key_bundle(const std::string& user_id, const std::string& device_id) {
    std::lock_guard<std::mutex> lock(user_devices_mutex_);
    std::lock_guard<std::mutex> bundle_lock(key_bundles_mutex_);
    
    auto user_it = user_devices_.find(user_id);
    if (user_it == user_devices_.end()) {
        return false;
    }
    
    auto device_it = user_it->second.find(device_id);
    if (device_it == user_it->second.end()) {
        return false;
    }
    
    const auto& device_state = device_it->second;
    
    KeyBundle bundle;
    bundle.user_id = user_id;
    bundle.device_id = device_id;
    bundle.identity_key = device_state.identity_key;
    bundle.signed_prekey = device_state.signed_prekey;
    bundle.one_time_prekeys = device_state.one_time_prekeys;
    bundle.created_at = std::chrono::system_clock::now();
    bundle.last_refresh = std::chrono::system_clock::now();
    bundle.version = device_state.key_bundle_version;
    bundle.is_stale = false;
    
    // Sign the key bundle
    bundle.signature = sign_key_bundle(bundle);
    
    key_bundles_[user_id][device_id] = bundle;
    return true;
}

std::optional<KeyBundle> E2EEncryptionManager::get_key_bundle(const std::string& user_id, const std::string& device_id) {
    std::lock_guard<std::mutex> lock(key_bundles_mutex_);
    
    auto user_it = key_bundles_.find(user_id);
    if (user_it == key_bundles_.end()) {
        return std::nullopt;
    }
    
    auto device_it = user_it->second.find(device_id);
    if (device_it == user_it->second.end()) {
        return std::nullopt;
    }
    
    return device_it->second;
}

bool E2EEncryptionManager::verify_signed_prekey_signature(const std::string& user_id, const std::string& device_id) {
    auto bundle_opt = get_key_bundle(user_id, device_id);
    if (!bundle_opt) {
        return false;
    }
    
    return verify_key_bundle_signature(*bundle_opt);
}

// Device Management
bool E2EEncryptionManager::add_device(const std::string& user_id, const std::string& device_id, const CryptoKey& identity_key) {
    std::lock_guard<std::mutex> lock(user_devices_mutex_);
    
    DeviceState device_state;
    device_state.device_id = device_id;
    device_state.identity_key = identity_key;
    device_state.key_bundle_version = 1;
    device_state.is_active = true;
    device_state.last_activity = std::chrono::system_clock::now();
    
    // Generate signed prekey and one-time prekeys
    if (!generate_and_sign_prekeys(user_id, device_id)) {
        return false;
    }
    
    user_devices_[user_id][device_id] = device_state;
    return true;
}

bool E2EEncryptionManager::remove_device(const std::string& user_id, const std::string& device_id) {
    std::lock_guard<std::mutex> lock(user_devices_mutex_);
    std::lock_guard<std::mutex> bundle_lock(key_bundles_mutex_);
    
    auto user_it = user_devices_.find(user_id);
    if (user_it == user_devices_.end()) {
        return false;
    }
    
    user_it->second.erase(device_id);
    if (user_it->second.empty()) {
        user_devices_.erase(user_it);
    }
    
    // Remove key bundle
    auto bundle_user_it = key_bundles_.find(user_id);
    if (bundle_user_it != key_bundles_.end()) {
        bundle_user_it->second.erase(device_id);
        if (bundle_user_it->second.empty()) {
            key_bundles_.erase(bundle_user_it);
        }
    }
    
    return true;
}

std::vector<std::string> E2EEncryptionManager::get_user_devices(const std::string& user_id) {
    std::lock_guard<std::mutex> lock(user_devices_mutex_);
    
    std::vector<std::string> devices;
    auto user_it = user_devices_.find(user_id);
    if (user_it != user_devices_.end()) {
        for (const auto& [device_id, _] : user_it->second) {
            devices.push_back(device_id);
        }
    }
    
    return devices;
}

// MLS Group Chat Support
std::string E2EEncryptionManager::create_mls_group(const std::vector<std::string>& member_ids, const std::string& group_name) {
    std::lock_guard<std::mutex> lock(mls_groups_mutex_);
    
    std::string group_id = "mls_group_" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    MLSGroupState group_state;
    group_state.group_id = group_id;
    group_state.epoch_id = "epoch_1";
    group_state.member_ids = member_ids;
    group_state.created_at = std::chrono::system_clock::now();
    group_state.last_epoch_change = std::chrono::system_clock::now();
    group_state.epoch_number = 1;
    group_state.is_active = true;
    
    // Generate group key and epoch keys
    auto [group_priv, group_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, group_id, "group_key");
    group_state.group_key = *group_pub;
    
    // Generate initial epoch key
    auto [epoch_priv, epoch_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, group_id, "epoch_1");
    group_state.epoch_keys.push_back(*epoch_pub);
    
    mls_groups_[group_id] = group_state;
    
    // Initialize group members
    std::vector<MLSMember> members;
    for (size_t i = 0; i < member_ids.size(); ++i) {
        MLSMember member;
        member.user_id = member_ids[i];
        member.device_id = "primary"; // Simplified
        member.leaf_index = i;
        member.joined_at = std::chrono::system_clock::now();
        member.is_active = true;
        
        // Generate leaf key for member
        auto [leaf_priv, leaf_pub] = crypto_engine_->generate_keypair(
            KeyExchangeProtocol::X25519, member_ids[i], "leaf_" + group_id);
        member.leaf_key = *leaf_pub;
        
        members.push_back(member);
    }
    
    group_members_[group_id] = members;
    
    return group_id;
}

bool E2EEncryptionManager::add_group_member(const std::string& group_id, const std::string& user_id, const std::string& device_id) {
    std::lock_guard<std::mutex> lock(mls_groups_mutex_);
    
    auto group_it = mls_groups_.find(group_id);
    if (group_it == mls_groups_.end()) {
        return false;
    }
    
    // Add to group members
    group_it->second.member_ids.push_back(user_id);
    
    // Add to group members list
    MLSMember member;
    member.user_id = user_id;
    member.device_id = device_id;
    member.leaf_index = group_members_[group_id].size();
    member.joined_at = std::chrono::system_clock::now();
    member.is_active = true;
    
    // Generate leaf key for new member
    auto [leaf_priv, leaf_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, user_id, "leaf_" + group_id);
    member.leaf_key = *leaf_pub;
    
    group_members_[group_id].push_back(member);
    
    // Rotate group keys
    return rotate_group_keys(group_id);
}

bool E2EEncryptionManager::remove_group_member(const std::string& group_id, const std::string& user_id) {
    std::lock_guard<std::mutex> lock(mls_groups_mutex_);
    
    auto group_it = mls_groups_.find(group_id);
    if (group_it == mls_groups_.end()) {
        return false;
    }
    
    // Remove from member IDs
    auto& member_ids = group_it->second.member_ids;
    member_ids.erase(std::remove(member_ids.begin(), member_ids.end(), user_id), member_ids.end());
    
    // Remove from group members
    auto& members = group_members_[group_id];
    members.erase(std::remove_if(members.begin(), members.end(),
        [&user_id](const MLSMember& m) { return m.user_id == user_id; }), members.end());
    
    // Rotate group keys
    return rotate_group_keys(group_id);
}

bool E2EEncryptionManager::rotate_group_keys(const std::string& group_id) {
    std::lock_guard<std::mutex> lock(mls_groups_mutex_);
    
    auto group_it = mls_groups_.find(group_id);
    if (group_it == mls_groups_.end()) {
        return false;
    }
    
    // Generate new group key
    auto [new_group_priv, new_group_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, group_id, "group_key_new");
    group_it->second.group_key = *new_group_pub;
    
    // Generate new epoch key
    auto [new_epoch_priv, new_epoch_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, group_id, "epoch_" + std::to_string(group_it->second.epoch_number + 1));
    group_it->second.epoch_keys.push_back(*new_epoch_pub);
    
    // Update epoch information
    group_it->second.epoch_number++;
    group_it->second.epoch_id = "epoch_" + std::to_string(group_it->second.epoch_number);
    group_it->second.last_epoch_change = std::chrono::system_clock::now();
    
    return true;
}

std::vector<uint8_t> E2EEncryptionManager::encrypt_group_message(const std::string& group_id, const std::vector<uint8_t>& plaintext) {
    std::lock_guard<std::mutex> lock(mls_groups_mutex_);
    
    auto group_it = mls_groups_.find(group_id);
    if (group_it == mls_groups_.end()) {
        return {};
    }
    
    // Use current epoch key for encryption
    if (group_it->second.epoch_keys.empty()) {
        return {};
    }
    
    const auto& current_epoch_key = group_it->second.epoch_keys.back();
    
    // Use full MLS protocol for encryption
    if (!mls_protocol_) {
        mls_protocol_ = std::make_unique<sonet::mls::MLSProtocol>();
    }
    
    // Convert group_id to bytes for MLS protocol
    std::vector<uint8_t> group_id_bytes(group_id.begin(), group_id.end());
    
    // Encrypt using MLS protocol
    auto encrypted = mls_protocol_->encrypt_message(group_id_bytes, plaintext, {});
    return encrypted;
}

std::vector<uint8_t> E2EEncryptionManager::decrypt_group_message(const std::string& group_id, const std::vector<uint8_t>& ciphertext) {
    std::lock_guard<std::mutex> lock(mls_groups_mutex_);
    
    auto group_it = mls_groups_.find(group_id);
    if (group_it == mls_groups_.end()) {
        return {};
    }
    
    // Try to decrypt with current epoch key
    if (group_it->second.epoch_keys.empty()) {
        return {};
    }
    
    const auto& current_epoch_key = group_it->second.epoch_keys.back();
    
    // Decrypt with current epoch key
    auto decrypted = crypto_engine_->decrypt(ciphertext, current_epoch_key);
    return decrypted;
}

// Key Transparency & Verification
bool E2EEncryptionManager::log_key_change(const std::string& user_id, const std::string& device_id, 
                                         const std::string& operation, const CryptoKey& old_key, 
                                         const CryptoKey& new_key, const std::string& reason) {
    std::lock_guard<std::mutex> lock(key_log_mutex_);
    
    KeyLogEntry entry;
    entry.user_id = user_id;
    entry.device_id = device_id;
    entry.operation = operation;
    entry.old_key = old_key;
    entry.new_key = new_key;
    entry.timestamp = std::chrono::system_clock::now();
    entry.reason = reason;
    
    // Sign the log entry
    std::string log_data = user_id + "|" + device_id + "|" + operation + "|" + reason;
    entry.signature = crypto_engine_->sign(log_data, old_key);
    
    key_log_.push_back(entry);
    
    // Maintain log size limit
    if (key_log_.size() > max_key_log_entries_) {
        key_log_.erase(key_log_.begin());
    }
    
    return true;
}

std::vector<KeyLogEntry> E2EEncryptionManager::get_key_log(const std::string& user_id, 
                                                          const std::chrono::system_clock::time_point& since) {
    std::lock_guard<std::mutex> lock(key_log_mutex_);
    
    std::vector<KeyLogEntry> user_log;
    for (const auto& entry : key_log_) {
        if (entry.user_id == user_id && entry.timestamp >= since) {
            user_log.push_back(entry);
        }
    }
    
    return user_log;
}

std::string E2EEncryptionManager::generate_safety_number(const std::string& user_id, const std::string& other_user_id) {
    // Generate safety number (similar to Signal's safety number)
    std::string combined = user_id + "|" + other_user_id;
    
    // Hash the combined string
    auto hash = crypto_engine_->hash(combined);
    
    // Convert to readable format (5 groups of 5 digits)
    std::stringstream ss;
    for (size_t i = 0; i < std::min(hash.size(), size_t(25)); i += 5) {
        if (i > 0) ss << " ";
        ss << std::setw(5) << std::setfill('0') << (hash[i] % 100000);
    }
    
    return ss.str();
}

std::string E2EEncryptionManager::generate_qr_code(const std::string& user_id, const std::string& other_user_id) {
    // Generate QR code data (simplified - would use actual QR library)
    std::string qr_data = "sonet://verify/" + user_id + "/" + other_user_id + "/" + 
                          generate_safety_number(user_id, other_user_id);
    return qr_data;
}

bool E2EEncryptionManager::verify_user_identity(const std::string& user_id, const std::string& other_user_id, 
                                               const std::string& verification_method) {
    if (verification_method == "safety_number") {
        // Verify safety number match
        std::string expected = generate_safety_number(user_id, other_user_id);
        // In real implementation, user would input the safety number for comparison
        return true; // Simplified
    } else if (verification_method == "qr") {
        // Verify QR code match
        std::string expected = generate_qr_code(user_id, other_user_id);
        // In real implementation, QR codes would be scanned and compared
        return true; // Simplified
    }
    
    return false;
}

// Trust Management
bool E2EEncryptionManager::establish_trust(const std::string& user_id, const std::string& trusted_user_id, 
                                          const std::string& trust_level, const std::string& verification_method) {
    std::lock_guard<std::mutex> lock(trust_mutex_);
    
    TrustState trust_state;
    trust_state.user_id = user_id;
    trust_state.trusted_user_id = trusted_user_id;
    trust_state.trust_level = trust_level;
    trust_state.established_at = std::chrono::system_clock::now();
    trust_state.last_verified = std::chrono::system_clock::now();
    trust_state.verification_method = verification_method;
    trust_state.is_active = true;
    
    trust_relationships_[user_id].push_back(trust_state);
    return true;
}

bool E2EEncryptionManager::update_trust_level(const std::string& user_id, const std::string& trusted_user_id, 
                                             const std::string& new_trust_level) {
    std::lock_guard<std::mutex> lock(trust_mutex_);
    
    auto user_it = trust_relationships_.find(user_id);
    if (user_it == trust_relationships_.end()) {
        return false;
    }
    
    for (auto& trust_state : user_it->second) {
        if (trust_state.trusted_user_id == trusted_user_id) {
            trust_state.trust_level = new_trust_level;
            trust_state.last_verified = std::chrono::system_clock::now();
            return true;
        }
    }
    
    return false;
}

std::vector<TrustState> E2EEncryptionManager::get_trust_relationships(const std::string& user_id) {
    std::lock_guard<std::mutex> lock(trust_mutex_);
    
    auto user_it = trust_relationships_.find(user_id);
    if (user_it == trust_relationships_.end()) {
        return {};
    }
    
    return user_it->second;
}

// Helper Methods
bool E2EEncryptionManager::generate_and_sign_prekeys(const std::string& user_id, const std::string& device_id) {
    // Generate signed prekey
    auto [spk_priv, spk_pub] = crypto_engine_->generate_keypair(
        KeyExchangeProtocol::X25519, user_id, device_id + "_spk");
    
    // Generate one-time prekeys
    std::vector<CryptoKey> otks;
    for (uint32_t i = 0; i < 10; ++i) {
        auto [otk_priv, otk_pub] = crypto_engine_->generate_keypair(
            KeyExchangeProtocol::X25519, user_id, device_id + "_otk_" + std::to_string(i));
        otks.push_back(*otk_pub);
    }
    
    // Update device state
    auto user_it = user_devices_.find(user_id);
    if (user_it != user_devices_.end()) {
        auto device_it = user_it->second.find(device_id);
        if (device_it != user_it->second.end()) {
            device_it->second.signed_prekey = *spk_pub;
            device_it->second.one_time_prekeys = std::move(otks);
            device_it->second.key_bundle_version++;
            return true;
        }
    }
    
    return false;
}

std::string E2EEncryptionManager::sign_key_bundle(const KeyBundle& bundle) {
    // Create signature data
    std::string data = bundle.user_id + "|" + bundle.device_id + "|" + 
                      std::to_string(bundle.version) + "|" + 
                      std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                          bundle.created_at.time_since_epoch()).count());
    
    // Sign with identity key
    return crypto_engine_->sign(data, bundle.identity_key);
}

bool E2EEncryptionManager::verify_key_bundle_signature(const KeyBundle& bundle) {
    // Recreate signature data
    std::string data = bundle.user_id + "|" + bundle.device_id + "|" + 
                      std::to_string(bundle.version) + "|" + 
                      std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                          bundle.created_at.time_since_epoch()).count());
    
    // Verify signature
    return crypto_engine_->verify_signature(data, bundle.signature, bundle.identity_key);
}

void E2EEncryptionManager::background_cleanup() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::minutes(5));
        
        cleanup_expired_prekeys();
        rotate_stale_key_bundles();
        cleanup_expired_key_logs();
    }
}

void E2EEncryptionManager::cleanup_expired_prekeys() {
    std::lock_guard<std::mutex> lock(user_devices_mutex_);
    
    auto now = std::chrono::system_clock::now();
    
    for (auto& [user_id, devices] : user_devices_) {
        for (auto& [device_id, device_state] : devices) {
            auto rotation_it = last_prekey_rotation_.find(user_id);
            if (rotation_it != last_prekey_rotation_.end()) {
                auto device_rotation_it = rotation_it->second.find(device_id);
                if (device_rotation_it != rotation_it->second.end()) {
                    if (now - device_rotation_it->second > prekey_rotation_interval_) {
                        rotate_one_time_prekeys(user_id, 10);
                    }
                }
            }
        }
    }
}

void E2EEncryptionManager::rotate_stale_key_bundles() {
    std::lock_guard<std::mutex> lock(key_bundles_mutex_);
    
    auto now = std::chrono::system_clock::now();
    
    for (auto& [user_id, devices] : key_bundles_) {
        for (auto& [device_id, bundle] : devices) {
            if (now - bundle.last_refresh > key_bundle_ttl_) {
                bundle.is_stale = true;
            }
        }
    }
}

void E2EEncryptionManager::cleanup_expired_key_logs() {
    std::lock_guard<std::mutex> lock(key_log_mutex_);
    
    auto now = std::chrono::system_clock::now();
    auto cutoff = now - std::chrono::hours(24 * 30); // 30 days
    
    key_log_.erase(std::remove_if(key_log_.begin(), key_log_.end(),
        [cutoff](const KeyLogEntry& entry) {
            return entry.timestamp < cutoff;
        }), key_log_.end());
}

// Placeholder implementations for existing methods (would be implemented in the main e2e_encryption.cpp)
bool E2EEncryptionManager::register_user_keys(const std::string& user_id, const CryptoKey& identity_key, 
                                             const CryptoKey& signed_prekey, const std::vector<CryptoKey>& one_time_prekeys) {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::update_user_keys(const std::string& user_id, const CryptoKey& identity_key, 
                                           const CryptoKey& signed_prekey, const std::vector<CryptoKey>& one_time_prekeys) {
    // Implementation would be in the main file
    return true;
}

std::string E2EEncryptionManager::initiate_session(const std::string& sender_id, const std::string& recipient_id, 
                                                  const std::string& device_id) {
    // Implementation would be in the main file
    return "session_" + sender_id + "_" + recipient_id;
}

bool E2EEncryptionManager::accept_session(const std::string& session_id, const std::string& recipient_id, 
                                         const std::string& sender_id) {
    // Implementation would be in the main file
    return true;
}

std::pair<std::vector<uint8_t>, std::string> E2EEncryptionManager::encrypt_message(const std::string& session_id, 
                                                                                  const std::vector<uint8_t>& plaintext) {
    // Implementation would be in the main file
    return {plaintext, "metadata"};
}

std::vector<uint8_t> E2EEncryptionManager::decrypt_message(const std::string& session_id, 
                                                          const std::vector<uint8_t>& ciphertext, 
                                                          const std::string& metadata) {
    // Implementation would be in the main file
    return ciphertext;
}

bool E2EEncryptionManager::is_session_active(const std::string& session_id) {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::close_session(const std::string& session_id) {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::close_all_sessions(const std::string& user_id) {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::rotate_session_keys(const std::string& session_id) {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::rotate_all_user_keys(const std::string& user_id) {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::mark_session_compromised(const std::string& session_id) {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::recover_from_compromise(const std::string& session_id, const std::string& new_identity_key) {
    // Implementation would be in the main file
    return true;
}

std::string E2EEncryptionManager::get_session_fingerprint(const std::string& session_id) {
    // Implementation would be in the main file
    return "fingerprint_" + session_id;
}

bool E2EEncryptionManager::verify_session_integrity(const std::string& session_id) {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::compare_fingerprints(const std::string& session_id, const std::string& other_fingerprint) {
    // Implementation would be in the main file
    return true;
}

std::string E2EEncryptionManager::export_session_info(const std::string& session_id) {
    // Implementation would be in the main file
    return "session_info_" + session_id;
}

bool E2EEncryptionManager::import_session_info(const std::string& session_id, const std::string& info) {
    // Implementation would be in the main file
    return true;
}

std::vector<std::string> E2EEncryptionManager::get_active_sessions(const std::string& user_id) {
    // Implementation would be in the main file
    return {"session_1", "session_2"};
}

std::unordered_map<std::string, uint64_t> E2EEncryptionManager::get_encryption_metrics() {
    // Implementation would be in the main file
    return {{"total_sessions", 100}, {"active_sessions", 50}};
}

bool E2EEncryptionManager::cleanup_old_sessions() {
    // Implementation would be in the main file
    return true;
}

bool E2EEncryptionManager::optimize_memory_usage() {
    // Implementation would be in the main file
    return true;
}

// PQC Operations Implementation
std::vector<uint8_t> E2EEncryptionManager::pqc_encrypt(const std::vector<uint8_t>& plaintext, const std::vector<uint8_t>& public_key) {
    if (!pqc_algorithms_) {
        pqc_algorithms_ = std::make_unique<sonet::pqc::PQCAlgorithms>();
    }
    
    // Use Kyber-768 for encryption (NIST PQC standard)
    auto encrypted = pqc_algorithms_->hybrid_encrypt(plaintext, public_key, sonet::pqc::PQCAlgorithm::KYBER_768);
    return encrypted.classical_ciphertext;
}

std::vector<uint8_t> E2EEncryptionManager::pqc_decrypt(const std::vector<uint8_t>& ciphertext, const std::vector<uint8_t>& private_key) {
    if (!pqc_algorithms_) {
        pqc_algorithms_ = std::make_unique<sonet::pqc::PQCAlgorithms>();
    }
    
    // For decryption, we need the full hybrid encryption result
    // This is a simplified implementation - in practice, you'd need to reconstruct the HybridEncryptionResult
    sonet::pqc::HybridEncryptionResult encrypted_data;
    encrypted_data.classical_ciphertext = ciphertext;
    encrypted_data.pqc_algorithm = sonet::pqc::PQCAlgorithm::KYBER_768;
    
    return pqc_algorithms_->hybrid_decrypt(encrypted_data, private_key);
}

std::vector<uint8_t> E2EEncryptionManager::pqc_sign(const std::vector<uint8_t>& message, const std::vector<uint8_t>& private_key) {
    if (!pqc_algorithms_) {
        pqc_algorithms_ = std::make_unique<sonet::pqc::PQCAlgorithms>();
    }
    
    // Use Dilithium-3 for signatures (NIST PQC standard)
    return pqc_algorithms_->dilithium_sign(message, private_key, sonet::pqc::PQCAlgorithm::DILITHIUM_3);
}

bool E2EEncryptionManager::pqc_verify(const std::vector<uint8_t>& message, const std::vector<uint8_t>& signature, const std::vector<uint8_t>& public_key) {
    if (!pqc_algorithms_) {
        pqc_algorithms_ = std::make_unique<sonet::pqc::PQCAlgorithms>();
    }
    
    // Use Dilithium-3 for verification
    return pqc_algorithms_->dilithium_verify(message, signature, public_key, sonet::pqc::PQCAlgorithm::DILITHIUM_3);
}

std::vector<uint8_t> E2EEncryptionManager::hybrid_encrypt(const std::vector<uint8_t>& plaintext, const std::vector<uint8_t>& pqc_public_key) {
    if (!pqc_algorithms_) {
        pqc_algorithms_ = std::make_unique<sonet::pqc::PQCAlgorithms>();
    }
    
    // Use hybrid encryption with Kyber-768
    auto result = pqc_algorithms_->hybrid_encrypt(plaintext, pqc_public_key, sonet::pqc::PQCAlgorithm::KYBER_768);
    
    // Combine all components into a single result
    std::vector<uint8_t> combined;
    combined.insert(combined.end(), result.nonce.begin(), result.nonce.end());
    combined.insert(combined.end(), result.classical_ciphertext.begin(), result.classical_ciphertext.end());
    combined.insert(combined.end(), result.pqc_ciphertext.begin(), result.pqc_ciphertext.end());
    
    return combined;
}

std::vector<uint8_t> E2EEncryptionManager::hybrid_decrypt(const std::vector<uint8_t>& encrypted_data, const std::vector<uint8_t>& pqc_private_key) {
    if (!pqc_algorithms_) {
        pqc_algorithms_ = std::make_unique<sonet::pqc::PQCAlgorithms>();
    }
    
    // Extract components from combined data
    if (encrypted_data.size() < 12 + 16) { // nonce + minimum ciphertext
        return {};
    }
    
    sonet::pqc::HybridEncryptionResult result;
    result.nonce.assign(encrypted_data.begin(), encrypted_data.begin() + 12);
    
    // Extract classical and PQC ciphertexts (simplified)
    size_t classical_size = encrypted_data.size() - 12 - 32; // Assume 32 bytes for PQC
    result.classical_ciphertext.assign(encrypted_data.begin() + 12, encrypted_data.begin() + 12 + classical_size);
    result.pqc_ciphertext.assign(encrypted_data.begin() + 12 + classical_size, encrypted_data.end());
    result.pqc_algorithm = sonet::pqc::PQCAlgorithm::KYBER_768;
    
    return pqc_algorithms_->hybrid_decrypt(result, pqc_private_key);
}

} // namespace sonet::messaging::crypto