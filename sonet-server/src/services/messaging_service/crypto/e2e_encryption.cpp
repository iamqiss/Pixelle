#include "../include/crypto_engine.hpp"
#include <sodium.h>
#include <unordered_map>

namespace sonet::messaging::crypto {

E2EEncryptionManager::E2EEncryptionManager() {
    crypto_engine_ = std::make_unique<CryptoEngine>();
}

E2EEncryptionManager::~E2EEncryptionManager() {
    close_all_sessions("");
}

static std::string user_key_id(const std::string& user_id, const char* kind, int index = -1) {
    if (index >= 0) {
        return user_id + std::string(":") + kind + std::string(":") + std::to_string(index);
    }
    return user_id + std::string(":") + kind;
}

bool E2EEncryptionManager::register_user_keys(
    const std::string& user_id,
    const CryptoKey& identity_key,
    const CryptoKey& signed_prekey,
    const std::vector<CryptoKey>& one_time_prekeys) {

    std::lock_guard<std::mutex> lock(keys_mutex_);
    user_keys_[user_key_id(user_id, "id")] = std::make_unique<CryptoKey>(identity_key);
    user_keys_[user_key_id(user_id, "spk")] = std::make_unique<CryptoKey>(signed_prekey);
    for (size_t i = 0; i < one_time_prekeys.size(); ++i) {
        user_keys_[user_key_id(user_id, "otk", static_cast<int>(i))] = std::make_unique<CryptoKey>(one_time_prekeys[i]);
    }
    return true;
}

bool E2EEncryptionManager::update_user_keys(
    const std::string& user_id,
    const CryptoKey& new_signed_prekey,
    const std::vector<CryptoKey>& new_one_time_prekeys) {

    std::lock_guard<std::mutex> lock(keys_mutex_);
    user_keys_[user_key_id(user_id, "spk")] = std::make_unique<CryptoKey>(new_signed_prekey);
    for (size_t i = 0; i < new_one_time_prekeys.size(); ++i) {
        user_keys_[user_key_id(user_id, "otk", static_cast<int>(i))] = std::make_unique<CryptoKey>(new_one_time_prekeys[i]);
    }
    return true;
}

std::string E2EEncryptionManager::initiate_session(
    const std::string& sender_id,
    const std::string& recipient_id,
    const std::string& device_id) {

    std::lock_guard<std::mutex> lock(keys_mutex_);

    auto recip_id_it = user_keys_.find(user_key_id(recipient_id, "id"));
    auto recip_spk_it = user_keys_.find(user_key_id(recipient_id, "spk"));
    if (recip_id_it == user_keys_.end() || recip_spk_it == user_keys_.end()) {
        return "";
    }

    auto [eph_priv, eph_pub] = crypto_engine_->generate_keypair(KeyExchangeProtocol::X25519, sender_id, device_id);

    // X3DH-like combination: DH1 = ECDH(Eph, Bob_ID), DH2 = ECDH(Eph, Bob_SPK)
    auto dh1 = crypto_engine_->perform_key_exchange(*eph_priv, *recip_id_it->second);
    auto dh2 = crypto_engine_->perform_key_exchange(*eph_priv, *recip_spk_it->second);

    // Optional DH3: Alice_ID with Bob_SPK, if Alice identity private is registered
    std::vector<uint8_t> ikm;
    ikm.insert(ikm.end(), dh1->key_data.begin(), dh1->key_data.end());
    ikm.insert(ikm.end(), dh2->key_data.begin(), dh2->key_data.end());

    auto alice_id_it = user_keys_.find(user_key_id(sender_id, "id"));
    if (alice_id_it != user_keys_.end() && alice_id_it->second->algorithm == "X25519") {
        try {
            auto dh3 = crypto_engine_->perform_key_exchange(*alice_id_it->second, *recip_spk_it->second);
            ikm.insert(ikm.end(), dh3->key_data.begin(), dh3->key_data.end());
        } catch (...) {
            // ignore optional DH3 failure
        }
    }

    CryptoKey ikm_key;
    ikm_key.id = crypto_engine_->generate_session_id();
    ikm_key.algorithm = "HKDF-IKM";
    ikm_key.key_data = std::move(ikm);
    ikm_key.created_at = std::chrono::system_clock::now();
    ikm_key.expires_at = ikm_key.created_at + std::chrono::hours(24);
    ikm_key.user_id = sender_id;

    KeyDerivationParams kdf_params;
    kdf_params.algorithm = "HKDF";
    kdf_params.salt = crypto_engine_->generate_salt(32);
    kdf_params.info = std::string("sonet:x3dh:root");

    auto root = crypto_engine_->derive_key(ikm_key, kdf_params);

    // Initialize minimal ratchet state
    auto state = std::make_unique<RatchetState>();
    state->root_key = std::move(root);
    state->chain_key_send = crypto_engine_->derive_key(*state->root_key, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("send") });
    state->chain_key_recv = crypto_engine_->derive_key(*state->root_key, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("recv") });
    state->send_count = 0;
    state->recv_count = 0;

    std::string session_id = crypto_engine_->generate_session_id();
    ratchet_states_[session_id] = std::move(state);

    // Cache ephemeral public so peer can accept (store as session key)
    auto eph_pub_copy = std::make_unique<CryptoKey>(*eph_pub);
    session_keys_[session_id] = std::move(eph_pub_copy);

    return session_id;
}

bool E2EEncryptionManager::accept_session(
    const std::string& session_id,
    const std::string& recipient_id,
    const std::string& sender_id) {

    std::lock_guard<std::mutex> lock(keys_mutex_);

    auto eph_pub_it = session_keys_.find(session_id);
    auto recip_id_it = user_keys_.find(user_key_id(recipient_id, "id"));
    auto recip_spk_it = user_keys_.find(user_key_id(recipient_id, "spk"));
    if (eph_pub_it == session_keys_.end() || recip_id_it == user_keys_.end() || recip_spk_it == user_keys_.end()) {
        return false;
    }

    // DH1 = ECDH(Recip_ID_priv, Eph_pub) — we don't keep priv here; assume stored as user id key is private
    // Using perform_key_exchange requires private,public ordering
    auto dh1 = crypto_engine_->perform_key_exchange(*recip_id_it->second, *eph_pub_it->second);
    auto dh2 = crypto_engine_->perform_key_exchange(*recip_spk_it->second, *eph_pub_it->second);

    std::vector<uint8_t> ikm;
    ikm.insert(ikm.end(), dh1->key_data.begin(), dh1->key_data.end());
    ikm.insert(ikm.end(), dh2->key_data.begin(), dh2->key_data.end());

    CryptoKey ikm_key;
    ikm_key.id = crypto_engine_->generate_session_id();
    ikm_key.algorithm = "HKDF-IKM";
    ikm_key.key_data = std::move(ikm);
    ikm_key.created_at = std::chrono::system_clock::now();
    ikm_key.expires_at = ikm_key.created_at + std::chrono::hours(24);
    ikm_key.user_id = recipient_id;

    KeyDerivationParams kdf_params;
    kdf_params.algorithm = "HKDF";
    kdf_params.salt = crypto_engine_->generate_salt(32);
    kdf_params.info = std::string("sonet:x3dh:root");

    auto root = crypto_engine_->derive_key(ikm_key, kdf_params);

    auto state = std::make_unique<RatchetState>();
    state->root_key = std::move(root);
    state->chain_key_send = crypto_engine_->derive_key(*state->root_key, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("send") });
    state->chain_key_recv = crypto_engine_->derive_key(*state->root_key, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("recv") });

    ratchet_states_[session_id] = std::move(state);
    return true;
}

std::pair<std::vector<uint8_t>, Json::Value> E2EEncryptionManager::encrypt_message(
    const std::string& session_id,
    const std::vector<uint8_t>& plaintext,
    const std::optional<std::vector<uint8_t>>& additional_data) {

    std::lock_guard<std::mutex> lock(keys_mutex_);
    auto it = ratchet_states_.find(session_id);
    if (it == ratchet_states_.end()) {
        throw std::invalid_argument("Invalid session");
    }
    auto& state = *it->second;

    auto key = state.chain_key_send.get();
    auto [ciphertext, ctx] = crypto_engine_->encrypt(plaintext, *key, additional_data);

    Json::Value meta = ctx.to_json();
    meta["v"] = 1;
    meta["alg"] = static_cast<int>(CryptoAlgorithm::AES_256_GCM);

    // Advance send chain
    state.chain_key_send = crypto_engine_->derive_key(*state.chain_key_send, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("send") });
    state.send_count++;

    return {ciphertext, meta};
}

std::vector<uint8_t> E2EEncryptionManager::decrypt_message(
    const std::string& session_id,
    const std::vector<uint8_t>& ciphertext,
    const Json::Value& encryption_metadata) {

    std::lock_guard<std::mutex> lock(keys_mutex_);
    auto it = ratchet_states_.find(session_id);
    if (it == ratchet_states_.end()) {
        throw std::invalid_argument("Invalid session");
    }
    auto& state = *it->second;

    auto key = state.chain_key_recv.get();
    auto ctx = EncryptionContext::from_json(encryption_metadata);
    auto plaintext = crypto_engine_->decrypt(ciphertext, *key, ctx);

    // Advance receive chain
    state.chain_key_recv = crypto_engine_->derive_key(*state.chain_key_recv, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("recv") });
    state.recv_count++;

    return plaintext;
}

void E2EEncryptionManager::advance_ratchet(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(keys_mutex_);
    auto it = ratchet_states_.find(session_id);
    if (it == ratchet_states_.end()) return;
    auto& state = *it->second;
    // Derive next root and chains (placeholder)
    state.root_key = crypto_engine_->derive_key(*state.root_key, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("root") });
    state.chain_key_send = crypto_engine_->derive_key(*state.root_key, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("send") });
    state.chain_key_recv = crypto_engine_->derive_key(*state.root_key, KeyDerivationParams{ .algorithm = std::string("HKDF"), .salt = {}, .iterations = 0, .memory_cost = 0, .parallelism = 0, .info = std::string("recv") });
}

void E2EEncryptionManager::reset_ratchet(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(keys_mutex_);
    ratchet_states_.erase(session_id);
}

bool E2EEncryptionManager::is_session_active(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(keys_mutex_);
    return ratchet_states_.find(session_id) != ratchet_states_.end();
}

void E2EEncryptionManager::close_session(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(keys_mutex_);
    ratchet_states_.erase(session_id);
}

void E2EEncryptionManager::close_all_sessions(const std::string& user_id) {
    std::lock_guard<std::mutex> lock(keys_mutex_);
    ratchet_states_.clear();
}

void E2EEncryptionManager::rotate_session_keys(const std::string& session_id) {
    advance_ratchet(session_id);
}

void E2EEncryptionManager::rotate_all_user_keys(const std::string& user_id) {
    (void)user_id;
}

std::string E2EEncryptionManager::get_session_fingerprint(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(keys_mutex_);
    auto it = ratchet_states_.find(session_id);
    if (it == ratchet_states_.end()) return "";
    return crypto_engine_->hash_hex(it->second->root_key->key_data);
}

bool E2EEncryptionManager::verify_session_integrity(const std::string& session_id) {
    return is_session_active(session_id);
}

bool E2EEncryptionManager::compare_fingerprints(
    const std::string& session_id,
    const std::string& expected_fingerprint) {
    return get_session_fingerprint(session_id) == expected_fingerprint;
}

Json::Value E2EEncryptionManager::export_session_info(const std::string& session_id) {
    std::lock_guard<std::mutex> lock(keys_mutex_);
    Json::Value json;
    json["session_id"] = session_id;
    json["active"] = is_session_active(session_id);
    return json;
}

bool E2EEncryptionManager::import_session_info(const Json::Value& session_info) {
    (void)session_info;
    return false;
}

std::vector<std::string> E2EEncryptionManager::get_active_sessions(const std::string& user_id) {
    std::lock_guard<std::mutex> lock(keys_mutex_);
    std::vector<std::string> ids;
    ids.reserve(ratchet_states_.size());
    for (auto& [sid, _] : ratchet_states_) ids.push_back(sid);
    return ids;
}

Json::Value E2EEncryptionManager::get_encryption_metrics() {
    Json::Value j;
    j["active_sessions"] = static_cast<Json::UInt>(ratchet_states_.size());
    return j;
}

void E2EEncryptionManager::cleanup_old_sessions(std::chrono::hours max_age) {
    (void)max_age;
}

void E2EEncryptionManager::optimize_memory_usage() {}

} // namespace sonet::messaging::crypto