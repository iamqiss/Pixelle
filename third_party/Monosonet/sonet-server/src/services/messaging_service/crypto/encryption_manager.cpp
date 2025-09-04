/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "../include/encryption_manager.hpp"
#include <cryptopp/aes.h>
#include <cryptopp/gcm.h>
#include <cryptopp/chacha.h>
#include <cryptopp/poly1305.h>
#include <cryptopp/osrng.h>
#include <cryptopp/hex.h>
#include <cryptopp/base64.h>
#include <cryptopp/sha.h>
#include <cryptopp/hkdf.h>
#include <cryptopp/ecdh.h>
#include <cryptopp/ec2n.h>
#include <cryptopp/eccrypto.h>
#include <cryptopp/curve25519.h>
#include <cryptopp/filters.h>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <filesystem>
#include <fstream>
#include <sodium.h>

namespace {
    std::string get_session_keys_path() {
        const char* env = std::getenv("SONET_SESSION_KEYS_PATH");
        if (env && *env) return std::string(env);
        return std::string("/tmp/sonet/messaging/session_keys.json");
    }

    void ensure_parent_dir(const std::string& path) {
        std::error_code ec;
        std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
    }
}

namespace sonet::messaging::encryption {

static inline void secure_zero_string(std::string& s) {
    if (!s.empty()) {
        sodium_memzero(s.data(), s.size());
        s.clear();
        s.shrink_to_fit();
    }
}

// EncryptionKeyPair implementation
std::string EncryptionKeyPair::serialize_public_key() const {
    try {
        std::string encoded;
        CryptoPP::StringSource ss(public_key, true,
            new CryptoPP::HexEncoder(new CryptoPP::StringSink(encoded)));
        return encoded;
    } catch (const std::exception& e) {
        return "";
    }
}

std::string EncryptionKeyPair::serialize_private_key() const {
    try {
        std::string encoded;
        CryptoPP::StringSource ss(private_key, true,
            new CryptoPP::HexEncoder(new CryptoPP::StringSink(encoded)));
        return encoded;
    } catch (const std::exception& e) {
        return "";
    }
}

bool EncryptionKeyPair::load_from_hex(const std::string& public_hex, 
                                     const std::string& private_hex) {
    try {
        CryptoPP::StringSource pub_ss(public_hex, true,
            new CryptoPP::HexDecoder(new CryptoPP::StringSink(public_key)));
        
        CryptoPP::StringSource priv_ss(private_hex, true,
            new CryptoPP::HexDecoder(new CryptoPP::StringSink(private_key)));
        
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

Json::Value EncryptionKeyPair::to_json() const {
    Json::Value json;
    json["key_id"] = key_id;
    json["algorithm"] = static_cast<int>(algorithm);
    json["public_key"] = serialize_public_key();
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        expires_at.time_since_epoch()).count();
    json["is_ephemeral"] = is_ephemeral;
    return json;
}

bool EncryptionKeyPair::is_expired() const {
    return std::chrono::system_clock::now() > expires_at;
}

// SessionKey implementation
std::string SessionKey::get_key_material() const {
    return key_material;
}

Json::Value SessionKey::to_json() const {
    Json::Value json;
    json["session_id"] = session_id;
    json["algorithm"] = static_cast<int>(algorithm);
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        expires_at.time_since_epoch()).count();
    json["message_count"] = message_count;
    json["max_messages"] = max_messages;
    return json;
}

bool SessionKey::is_expired() const {
    return std::chrono::system_clock::now() > expires_at || 
           message_count >= max_messages;
}

void SessionKey::increment_usage() {
    message_count++;
}

// EncryptedMessage implementation
Json::Value EncryptedMessage::to_json() const {
    Json::Value json;
    json["message_id"] = message_id;
    json["session_id"] = session_id;
    json["algorithm"] = static_cast<int>(algorithm);
    json["ciphertext"] = ciphertext;
    json["nonce"] = nonce;
    json["tag"] = tag;
    json["additional_data"] = additional_data;
    json["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        timestamp.time_since_epoch()).count();
    return json;
}

EncryptedMessage EncryptedMessage::from_json(const Json::Value& json) {
    EncryptedMessage msg;
    msg.message_id = json["message_id"].asString();
    msg.session_id = json["session_id"].asString();
    msg.algorithm = static_cast<EncryptionAlgorithm>(json["algorithm"].asInt());
    msg.ciphertext = json["ciphertext"].asString();
    msg.nonce = json["nonce"].asString();
    msg.tag = json["tag"].asString();
    msg.additional_data = json["additional_data"].asString();
    
    auto timestamp_ms = json["timestamp"].asInt64();
    msg.timestamp = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(timestamp_ms));
    
    return msg;
}

// DoubleRatchetState implementation
Json::Value DoubleRatchetState::to_json() const {
    Json::Value json;
    json["state_id"] = state_id;
    json["chat_id"] = chat_id;
    json["our_identity_key"] = our_identity_key;
    json["their_identity_key"] = their_identity_key;
    json["root_key"] = root_key;
    json["sending_chain_key"] = sending_chain_key;
    json["receiving_chain_key"] = receiving_chain_key;
    json["our_ratchet_private_key"] = our_ratchet_private_key;
    json["our_ratchet_public_key"] = our_ratchet_public_key;
    json["their_ratchet_public_key"] = their_ratchet_public_key;
    json["sending_message_number"] = sending_message_number;
    json["receiving_message_number"] = receiving_message_number;
    json["previous_sending_chain_length"] = previous_sending_chain_length;
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["last_ratchet"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_ratchet.time_since_epoch()).count();
    
    // Add new fields
    json["sending_chain_length"] = sending_chain_length;
    json["receiving_chain_length"] = receiving_chain_length;
    json["messages_since_rekey"] = messages_since_rekey;
    
    // Add skipped message keys
    Json::Value skipped_keys;
    for (const auto& [msg_num, key] : skipped_message_keys) {
        skipped_keys[std::to_string(msg_num)] = key;
    }
    json["skipped_message_keys"] = skipped_keys;
    
    return json;
}

bool DoubleRatchetState::should_ratchet() const {
    auto now = std::chrono::system_clock::now();
    auto time_since_ratchet = now - last_ratchet;
    
    return time_since_ratchet > std::chrono::hours(24) || 
           sending_message_number > 1000;
}

bool DoubleRatchetState::should_rekey() const {
    auto now = std::chrono::system_clock::now();
    auto time_since_ratchet = now - last_ratchet;
    
    return time_since_ratchet > rekey_interval || 
           messages_since_rekey > max_messages_per_chain;
}

void DoubleRatchetState::cleanup_old_skipped_keys() {
    if (skipped_message_keys.size() > max_skipped_keys) {
        // Remove oldest keys to maintain bounded memory usage
        std::vector<uint32_t> keys_to_remove;
        for (const auto& [msg_num, _] : skipped_message_keys) {
            if (msg_num < receiving_message_number - max_skipped_keys) {
                keys_to_remove.push_back(msg_num);
            }
        }
        
        for (uint32_t key : keys_to_remove) {
            skipped_message_keys.erase(key);
        }
    }
}

DoubleRatchetState DoubleRatchetState::from_json(const Json::Value& json) {
    DoubleRatchetState state;
    state.state_id = json["state_id"].asString();
    state.chat_id = json["chat_id"].asString();
    state.our_identity_key = json["our_identity_key"].asString();
    state.their_identity_key = json["their_identity_key"].asString();
    state.root_key = json["root_key"].asString();
    state.sending_chain_key = json["sending_chain_key"].asString();
    state.receiving_chain_key = json["receiving_chain_key"].asString();
    state.our_ratchet_private_key = json["our_ratchet_private_key"].asString();
    state.our_ratchet_public_key = json["our_ratchet_public_key"].asString();
    state.their_ratchet_public_key = json["their_ratchet_public_key"].asString();
    state.sending_message_number = json["sending_message_number"].asUInt();
    state.receiving_message_number = json["receiving_message_number"].asUInt();
    state.previous_sending_chain_length = json["previous_sending_chain_length"].asUInt();
    
    auto created_ms = json["created_at"].asInt64();
    state.created_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(created_ms));
    
    auto last_ratchet_ms = json["last_ratchet"].asInt64();
    state.last_ratchet = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(last_ratchet_ms));
    
    // Load skipped message keys if present
    if (json.isMember("skipped_message_keys") && json["skipped_message_keys"].isObject()) {
        for (const auto& key_name : json["skipped_message_keys"].getMemberNames()) {
            uint32_t msg_num = std::stoul(key_name);
            state.skipped_message_keys[msg_num] = json["skipped_message_keys"][key_name].asString();
        }
    }
    
    // Load additional fields if present
    if (json.isMember("sending_chain_length")) {
        state.sending_chain_length = json["sending_chain_length"].asUInt();
    }
    if (json.isMember("receiving_chain_length")) {
        state.receiving_chain_length = json["receiving_chain_length"].asUInt();
    }
    if (json.isMember("messages_since_rekey")) {
        state.messages_since_rekey = json["messages_since_rekey"].asUInt();
    }
    
    return state;
}

// EncryptionManager implementation
EncryptionManager::EncryptionManager() : rng_(std::make_unique<CryptoPP::AutoSeededRandomPool>()) {
    // Initialize supported algorithms
    supported_algorithms_ = {
        EncryptionAlgorithm::AES_256_GCM,
        EncryptionAlgorithm::CHACHA20_POLY1305,
        EncryptionAlgorithm::X25519_CHACHA20_POLY1305
    };
    
    // Set preferred algorithm order
    preferred_algorithm_ = EncryptionAlgorithm::X25519_CHACHA20_POLY1305;
    
    // Initialize key rotation settings
    key_rotation_interval_ = std::chrono::hours(24);
    max_messages_per_key_ = 10000;
    
    // Start background cleanup thread
    cleanup_thread_ = std::thread([this]() {
        while (running_.load()) {
            cleanup_expired_keys();
            cleanup_expired_ratchet_states();
            std::this_thread::sleep_for(std::chrono::minutes(5));
        }
    });

    // Load persisted session keys
    try {
        std::ifstream in(get_session_keys_path());
        if (in.good()) {
            Json::Value root;
            Json::CharReaderBuilder rb;
            std::string errs;
            std::unique_ptr<Json::CharReader> reader(rb.newCharReader());
            std::string buf((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
            if (reader->parse(buf.data(), buf.data() + buf.size(), &root, &errs)) {
                if (root.isMember("session_keys") && root["session_keys"].isArray()) {
                    std::lock_guard<std::mutex> lock(session_keys_mutex_);
                    for (const auto& sk : root["session_keys"]) {
                        SessionKey s;
                        s.session_id = sk["session_id"].asString();
                        s.chat_id = sk["chat_id"].asString();
                        s.user_id = sk["user_id"].asString();
                        s.algorithm = static_cast<EncryptionAlgorithm>(sk["algorithm"].asInt());
                                                 // Do not load key material from disk for security
                         s.created_at = std::chrono::system_clock::time_point(std::chrono::milliseconds(sk["created_at"].asInt64()));
                         s.expires_at = std::chrono::system_clock::time_point(std::chrono::milliseconds(sk["expires_at"].asInt64()));
                         s.message_count = sk["message_count"].asUInt();
                         s.max_messages = sk["max_messages"].asUInt();
                         session_keys_[s.session_id] = s;
chat_session_keys_[s.chat_id].insert(s.session_id);
                        user_session_keys_[s.user_id].insert(s.session_id);
                    }
                }
            }
        }
    } catch (...) {
        // ignore
    }
}

EncryptionManager::~EncryptionManager() {
    running_ = false;
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    // Save on shutdown
    try {
        std::lock_guard<std::mutex> lock(session_keys_mutex_);
        Json::Value root;
        root["session_keys"] = Json::arrayValue;
        for (const auto& [id, sk] : session_keys_) {
            Json::Value j;
            j["session_id"] = sk.session_id;
            j["chat_id"] = sk.chat_id;
            j["user_id"] = sk.user_id;
            j["algorithm"] = static_cast<int>(sk.algorithm);
            j["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(sk.created_at.time_since_epoch()).count();
            j["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(sk.expires_at.time_since_epoch()).count();
            j["message_count"] = sk.message_count;
            j["max_messages"] = sk.max_messages;
            root["session_keys"].append(j);
        }
        auto path = get_session_keys_path();
        ensure_parent_dir(path);
        std::ofstream out(path, std::ios::trunc);
        Json::StreamWriterBuilder wb;
        out << Json::writeString(wb, root);
    } catch (...) { /* ignore */ }
}

EncryptionKeyPair EncryptionManager::generate_key_pair(EncryptionAlgorithm algorithm) {
    EncryptionKeyPair key_pair;
    key_pair.key_id = generate_key_id();
    key_pair.algorithm = algorithm;
    key_pair.created_at = std::chrono::system_clock::now();
    key_pair.expires_at = key_pair.created_at + key_rotation_interval_;
    key_pair.is_ephemeral = false;
    
    try {
        switch (algorithm) {
            case EncryptionAlgorithm::X25519_CHACHA20_POLY1305: {
                // Generate X25519 key pair
                CryptoPP::x25519 curve;
                CryptoPP::SecByteBlock private_key(curve.PrivateKeyLength());
                CryptoPP::SecByteBlock public_key(curve.PublicKeyLength());
                
                curve.GenerateKeyPair(*rng_, private_key, public_key);
                
                key_pair.private_key.assign(private_key.begin(), private_key.end());
                key_pair.public_key.assign(public_key.begin(), public_key.end());
                break;
            }
            
            case EncryptionAlgorithm::AES_256_GCM:
            case EncryptionAlgorithm::CHACHA20_POLY1305: {
                // Generate random 256-bit key
                CryptoPP::SecByteBlock key(32);
                rng_->GenerateBlock(key, key.size());
                
                key_pair.private_key.assign(key.begin(), key.end());
                key_pair.public_key = key_pair.private_key; // Symmetric key
                break;
            }
            
            default:
                throw std::invalid_argument("Unsupported algorithm");
        }
        
        // Store the key pair
        std::lock_guard<std::mutex> lock(key_pairs_mutex_);
        key_pairs_[key_pair.key_id] = key_pair;
        
    } catch (const std::exception& e) {
        // Return empty key pair on error
        key_pair = EncryptionKeyPair{};
    }
    
    return key_pair;
}

SessionKey EncryptionManager::create_session_key(const std::string& chat_id,
                                                const std::string& user_id,
                                                EncryptionAlgorithm algorithm) {
    SessionKey session_key;
    session_key.session_id = generate_session_id();
    session_key.chat_id = chat_id;
    session_key.user_id = user_id;
    session_key.algorithm = algorithm;
    session_key.created_at = std::chrono::system_clock::now();
    session_key.expires_at = session_key.created_at + key_rotation_interval_;
    session_key.message_count = 0;
    session_key.max_messages = max_messages_per_key_;
    
    try {
        // Generate session key material
        size_t key_size = get_key_size(algorithm);
        CryptoPP::SecByteBlock key_material(key_size);
        rng_->GenerateBlock(key_material, key_material.size());
        
        session_key.key_material.assign(key_material.begin(), key_material.end());
        
        // Store the session key
        std::lock_guard<std::mutex> lock(session_keys_mutex_);
        session_keys_[session_key.session_id] = session_key;
        
        // Index by chat and user
        chat_session_keys_[chat_id].insert(session_key.session_id);
        user_session_keys_[user_id].insert(session_key.session_id);
        
        // Persist after creation
        try {
            std::lock_guard<std::mutex> lock(session_keys_mutex_);
            Json::Value root;
            root["session_keys"] = Json::arrayValue;
            for (const auto& [id, sk] : session_keys_) {
                Json::Value j;
                j["session_id"] = sk.second.session_id;
                j["chat_id"] = sk.second.chat_id;
                j["user_id"] = sk.second.user_id;
                j["algorithm"] = static_cast<int>(sk.second.algorithm);
                j["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(sk.second.created_at.time_since_epoch()).count();
                j["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(sk.second.expires_at.time_since_epoch()).count();
                j["message_count"] = sk.second.message_count;
                j["max_messages"] = sk.second.max_messages;
                root["session_keys"].append(j);
            }
            auto path = get_session_keys_path();
            ensure_parent_dir(path);
            std::ofstream out(path, std::ios::trunc);
            Json::StreamWriterBuilder wb;
            out << Json::writeString(wb, root);
        } catch (...) { /* ignore */ }
        
    } catch (const std::exception& e) {
        // Return empty session key on error
        session_key = SessionKey{};
    }
    
    return session_key;
}

EncryptedMessage EncryptionManager::encrypt_message(const std::string& session_id,
                                                   const std::string& plaintext,
                                                   const std::string& additional_data) {
    EncryptedMessage encrypted_msg;
    
    std::lock_guard<std::mutex> lock(session_keys_mutex_);
    auto session_it = session_keys_.find(session_id);
    if (session_it == session_keys_.end()) {
        return encrypted_msg; // Empty on error
    }
    
    const auto& session_key = session_it->second;
    if (session_key.is_expired()) {
        return encrypted_msg; // Empty on error
    }
    
    encrypted_msg.message_id = generate_message_id();
    encrypted_msg.session_id = session_id;
    encrypted_msg.algorithm = session_key.algorithm;
    encrypted_msg.additional_data = additional_data;
    encrypted_msg.timestamp = std::chrono::system_clock::now();
    
    try {
        switch (session_key.algorithm) {
            case EncryptionAlgorithm::AES_256_GCM: {
                // Nonce
                const size_t nonce_size = 12;
                CryptoPP::SecByteBlock nonce(nonce_size);
                rng_->GenerateBlock(nonce, nonce.size());

                // Encrypt
                CryptoPP::GCM<CryptoPP::AES>::Encryption enc;
                enc.SetKeyWithIV(
                    reinterpret_cast<const CryptoPP::byte*>(session_key.key_material.data()),
                    session_key.key_material.size(),
                    nonce, nonce.size()
                );
                enc.SpecifyDataLengths(additional_data.size(), plaintext.size(), 0);

                std::string ct_and_tag;
                CryptoPP::AuthenticatedEncryptionFilter aef(
                    enc,
                    new CryptoPP::StringSink(ct_and_tag),
                    false, // putMessage
                    16     // tag at end
                );
                if (!additional_data.empty()) {
                    aef.ChannelPut(CryptoPP::AAD_CHANNEL,
                        reinterpret_cast<const CryptoPP::byte*>(additional_data.data()),
                        additional_data.size());
                    aef.ChannelMessageEnd(CryptoPP::AAD_CHANNEL);
                }
                aef.ChannelPut(CryptoPP::DEFAULT_CHANNEL,
                    reinterpret_cast<const CryptoPP::byte*>(plaintext.data()),
                    plaintext.size());
                aef.ChannelMessageEnd(CryptoPP::DEFAULT_CHANNEL);

                if (ct_and_tag.size() < 16) return EncryptedMessage{};
                encrypted_msg.ciphertext = ct_and_tag.substr(0, ct_and_tag.size() - 16);
                encrypted_msg.tag = ct_and_tag.substr(ct_and_tag.size() - 16);
                encrypted_msg.nonce.assign(nonce.begin(), nonce.end());
                break;
            }
            case EncryptionAlgorithm::CHACHA20_POLY1305:
            case EncryptionAlgorithm::X25519_CHACHA20_POLY1305: {
                const size_t nonce_size = 12;
                CryptoPP::SecByteBlock nonce(nonce_size);
                rng_->GenerateBlock(nonce, nonce.size());

                CryptoPP::ChaCha20Poly1305::Encryption enc;
                enc.SetKeyWithIV(
                    reinterpret_cast<const CryptoPP::byte*>(session_key.key_material.data()),
                    32,
                    nonce, nonce.size()
                );
                enc.SpecifyDataLengths(additional_data.size(), plaintext.size(), 0);

                std::string ct_and_tag;
                CryptoPP::AuthenticatedEncryptionFilter aef(
                    enc,
                    new CryptoPP::StringSink(ct_and_tag),
                    false,
                    16
                );
                if (!additional_data.empty()) {
                    aef.ChannelPut(CryptoPP::AAD_CHANNEL,
                        reinterpret_cast<const CryptoPP::byte*>(additional_data.data()),
                        additional_data.size());
                    aef.ChannelMessageEnd(CryptoPP::AAD_CHANNEL);
                }
                aef.ChannelPut(CryptoPP::DEFAULT_CHANNEL,
                    reinterpret_cast<const CryptoPP::byte*>(plaintext.data()),
                    plaintext.size());
                aef.ChannelMessageEnd(CryptoPP::DEFAULT_CHANNEL);

                if (ct_and_tag.size() < 16) return EncryptedMessage{};
                encrypted_msg.ciphertext = ct_and_tag.substr(0, ct_and_tag.size() - 16);
                encrypted_msg.tag = ct_and_tag.substr(ct_and_tag.size() - 16);
                encrypted_msg.nonce.assign(nonce.begin(), nonce.end());
                break;
            }
            default:
                return EncryptedMessage{}; // Unsupported algorithm
        }
        
        // Increment usage
        session_key.increment_usage();
        
        // Base64 encode
        encrypted_msg.ciphertext = base64_encode(encrypted_msg.ciphertext);
        encrypted_msg.nonce = base64_encode(encrypted_msg.nonce);
        encrypted_msg.tag = base64_encode(encrypted_msg.tag);
        
    } catch (const std::exception& e) {
        return EncryptedMessage{};
    }
    
    return encrypted_msg;
}

std::string EncryptionManager::decrypt_message(const EncryptedMessage& encrypted_msg) {
    std::lock_guard<std::mutex> lock(session_keys_mutex_);
    auto session_it = session_keys_.find(encrypted_msg.session_id);
    if (session_it == session_keys_.end()) {
        return ""; // Empty on error
    }
    
    const auto& session_key = session_it->second;
    
    try {
        // Decode base64
        std::string ciphertext = base64_decode(encrypted_msg.ciphertext);
        std::string nonce = base64_decode(encrypted_msg.nonce);
        std::string tag = base64_decode(encrypted_msg.tag);
        
        std::string plaintext;
        
        switch (session_key.algorithm) {
            case EncryptionAlgorithm::AES_256_GCM: {
                CryptoPP::GCM<CryptoPP::AES>::Decryption dec;
                dec.SetKeyWithIV(
                    reinterpret_cast<const CryptoPP::byte*>(session_key.key_material.data()),
                    session_key.key_material.size(),
                    reinterpret_cast<const CryptoPP::byte*>(nonce.data()),
                    nonce.size()
                );
                dec.SpecifyDataLengths(encrypted_msg.additional_data.size(), ciphertext.size(), 0);
                
                std::string ct_and_tag = ciphertext + tag;
                CryptoPP::AuthenticatedDecryptionFilter adf(
                    dec,
                    new CryptoPP::StringSink(plaintext),
                    CryptoPP::AuthenticatedDecryptionFilter::DEFAULT_FLAGS,
                    16
                );
                if (!encrypted_msg.additional_data.empty()) {
                    adf.ChannelPut(CryptoPP::AAD_CHANNEL,
                        reinterpret_cast<const CryptoPP::byte*>(encrypted_msg.additional_data.data()),
                        encrypted_msg.additional_data.size());
                    adf.ChannelMessageEnd(CryptoPP::AAD_CHANNEL);
                }
                adf.ChannelPut(CryptoPP::DEFAULT_CHANNEL,
                    reinterpret_cast<const CryptoPP::byte*>(ct_and_tag.data()),
                    ct_and_tag.size());
                adf.ChannelMessageEnd(CryptoPP::DEFAULT_CHANNEL);
                
                if (!adf.GetLastResult()) {
                    return "";
                }
                break;
            }
            case EncryptionAlgorithm::CHACHA20_POLY1305:
            case EncryptionAlgorithm::X25519_CHACHA20_POLY1305: {
                CryptoPP::ChaCha20Poly1305::Decryption dec;
                dec.SetKeyWithIV(
                    reinterpret_cast<const CryptoPP::byte*>(session_key.key_material.data()),
                    32,
                    reinterpret_cast<const CryptoPP::byte*>(nonce.data()),
                    nonce.size()
                );
                dec.SpecifyDataLengths(encrypted_msg.additional_data.size(), ciphertext.size(), 0);
                
                std::string ct_and_tag = ciphertext + tag;
                CryptoPP::AuthenticatedDecryptionFilter adf(
                    dec,
                    new CryptoPP::StringSink(plaintext),
                    CryptoPP::AuthenticatedDecryptionFilter::DEFAULT_FLAGS,
                    16
                );
                if (!encrypted_msg.additional_data.empty()) {
                    adf.ChannelPut(CryptoPP::AAD_CHANNEL,
                        reinterpret_cast<const CryptoPP::byte*>(encrypted_msg.additional_data.data()),
                        encrypted_msg.additional_data.size());
                    adf.ChannelMessageEnd(CryptoPP::AAD_CHANNEL);
                }
                adf.ChannelPut(CryptoPP::DEFAULT_CHANNEL,
                    reinterpret_cast<const CryptoPP::byte*>(ct_and_tag.data()),
                    ct_and_tag.size());
                adf.ChannelMessageEnd(CryptoPP::DEFAULT_CHANNEL);
                
                if (!adf.GetLastResult()) {
                    return "";
                }
                break;
            }
            default:
                return ""; // Unsupported algorithm
        }
        
        return plaintext;
        
    } catch (const std::exception& e) {
        return ""; // Empty on error
    }
}

DoubleRatchetState EncryptionManager::initialize_double_ratchet(const std::string& chat_id,
                                                               const std::string& our_identity_key,
                                                               const std::string& their_identity_key) {
    DoubleRatchetState state;
    state.state_id = generate_state_id();
    state.chat_id = chat_id;
    state.our_identity_key = our_identity_key;
    state.their_identity_key = their_identity_key;
    state.sending_message_number = 0;
    state.receiving_message_number = 0;
    state.previous_sending_chain_length = 0;
    state.created_at = std::chrono::system_clock::now();
    state.last_ratchet = state.created_at;
    
    try {
        // Generate initial root key using HKDF
        std::string shared_secret = compute_shared_secret(our_identity_key, their_identity_key);
        state.root_key = derive_key(shared_secret, "RootKey", 32);
        
        // Generate initial ratchet key pair
        auto ratchet_key_pair = generate_key_pair(EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
        state.our_ratchet_private_key = ratchet_key_pair.serialize_private_key();
        state.our_ratchet_public_key = ratchet_key_pair.serialize_public_key();
        
        // Initialize chain keys
        state.sending_chain_key = derive_key(state.root_key, "SendingChain", 32);
        state.receiving_chain_key = derive_key(state.root_key, "ReceivingChain", 32);
        
        // Store the ratchet state
        std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
        ratchet_states_[chat_id] = state;
        
    } catch (const std::exception& e) {
        // Return empty state on error
        state = DoubleRatchetState{};
    }
    
    return state;
}

bool EncryptionManager::perform_dh_ratchet(const std::string& chat_id,
                                          const std::string& their_new_public_key) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    try {
        // Generate new ratchet key pair
        auto new_ratchet_key_pair = generate_key_pair(EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
        
        // Compute new shared secret
        std::string shared_secret = compute_shared_secret(
            new_ratchet_key_pair.serialize_private_key(),
            their_new_public_key
        );
        
        // Derive new root key and sending chain key
        std::string new_root_key = derive_key(shared_secret, "NewRootKey", 32);
        std::string new_sending_chain_key = derive_key(new_root_key, "NewSendingChain", 32);
        
        // Update state
        state.previous_sending_chain_length = state.sending_message_number;
        state.root_key = new_root_key;
        state.sending_chain_key = new_sending_chain_key;
        state.our_ratchet_private_key = new_ratchet_key_pair.serialize_private_key();
        state.our_ratchet_public_key = new_ratchet_key_pair.serialize_public_key();
        state.their_ratchet_public_key = their_new_public_key;
        state.sending_message_number = 0;
        state.last_ratchet = std::chrono::system_clock::now();
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

std::string EncryptionManager::derive_message_key(const std::string& chain_key, 
                                                 uint32_t message_number) {
    try {
        // Create input for HKDF
        std::string input = chain_key + std::to_string(message_number);
        return derive_key(input, "MessageKey", 32);
        
    } catch (const std::exception& e) {
        return "";
    }
}

std::vector<std::string> EncryptionManager::get_supported_algorithms() const {
    std::vector<std::string> algorithms;
    for (auto algo : supported_algorithms_) {
        algorithms.push_back(algorithm_to_string(algo));
    }
    return algorithms;
}

void EncryptionManager::cleanup_expired_keys() {
    auto now = std::chrono::system_clock::now();
    
    // Clean up expired key pairs
    {
        std::lock_guard<std::mutex> lock(key_pairs_mutex_);
        for (auto it = key_pairs_.begin(); it != key_pairs_.end();) {
            if (it->second.is_expired()) {
                it = key_pairs_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    // Clean up expired session keys
    {
        std::lock_guard<std::mutex> lock(session_keys_mutex_);
        for (auto it = session_keys_.begin(); it != session_keys_.end();) {
            if (it->second.is_expired()) {
                const auto& session_key = it->second;
                
                // Remove from indexes
                auto chat_it = chat_session_keys_.find(session_key.chat_id);
                if (chat_it != chat_session_keys_.end()) {
                    chat_it->second.erase(session_key.session_id);
                    if (chat_it->second.empty()) {
                        chat_session_keys_.erase(chat_it);
                    }
                }
                
                auto user_it = user_session_keys_.find(session_key.user_id);
                if (user_it != user_session_keys_.end()) {
                    user_it->second.erase(session_key.session_id);
                    if (user_it->second.empty()) {
                        user_session_keys_.erase(user_it);
                    }
                }
                
                it = session_keys_.erase(it);
            } else {
                ++it;
            }
        }
    }
    // After cleanup, persist
    try {
        std::lock_guard<std::mutex> lock(session_keys_mutex_);
        Json::Value root;
        root["session_keys"] = Json::arrayValue;
        for (const auto& [id, sk] : session_keys_) {
            Json::Value j;
            j["session_id"] = sk.second.session_id;
            j["chat_id"] = sk.second.chat_id;
            j["user_id"] = sk.second.user_id;
            j["algorithm"] = static_cast<int>(sk.second.algorithm);
            j["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(sk.second.created_at.time_since_epoch()).count();
            j["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(sk.second.expires_at.time_since_epoch()).count();
            j["message_count"] = sk.second.message_count;
            j["max_messages"] = sk.second.max_messages;
            root["session_keys"].append(j);
        }
        auto path = get_session_keys_path();
        ensure_parent_dir(path);
        std::ofstream out(path, std::ios::trunc);
        Json::StreamWriterBuilder wb;
        out << Json::writeString(wb, root);
    } catch (...) { /* ignore */ }
}

size_t EncryptionManager::get_key_size(EncryptionAlgorithm algorithm) const {
    switch (algorithm) {
        case EncryptionAlgorithm::AES_256_GCM:
            return 32; // 256 bits
        case EncryptionAlgorithm::CHACHA20_POLY1305:
        case EncryptionAlgorithm::X25519_CHACHA20_POLY1305:
            return 32; // 256 bits
        default:
            return 32; // Default to 256 bits
    }
}

std::string EncryptionManager::compute_shared_secret(const std::string& our_private_key,
                                                    const std::string& their_public_key) {
    try {
        // Decode keys from hex
        std::string private_key_bin, public_key_bin;
        CryptoPP::StringSource(our_private_key, true,
            new CryptoPP::HexDecoder(new CryptoPP::StringSink(private_key_bin)));
        CryptoPP::StringSource(their_public_key, true,
            new CryptoPP::HexDecoder(new CryptoPP::StringSink(public_key_bin)));
        
        // Perform X25519 ECDH
        CryptoPP::x25519 curve;
        CryptoPP::SecByteBlock shared_secret(curve.AgreedValueLength());
        
        if (!curve.Agree(shared_secret,
                        reinterpret_cast<const CryptoPP::byte*>(private_key_bin.data()),
                        reinterpret_cast<const CryptoPP::byte*>(public_key_bin.data()))) {
            return "";
        }
        
        return std::string(shared_secret.begin(), shared_secret.end());
        
    } catch (const std::exception& e) {
        return "";
    }
}

std::string EncryptionManager::derive_key(const std::string& input_key_material,
                                         const std::string& info,
                                         size_t output_length) {
    try {
        // Deterministic salt derived from (info || IKM) to avoid empty salt
        CryptoPP::SHA256 sha;
        std::string salt_input = info + input_key_material;
        std::string salt;
        salt.resize(CryptoPP::SHA256::DIGESTSIZE);
        sha.CalculateDigest(reinterpret_cast<CryptoPP::byte*>(&salt[0]),
                            reinterpret_cast<const CryptoPP::byte*>(salt_input.data()),
                            salt_input.size());
        
        CryptoPP::HKDF<CryptoPP::SHA256> hkdf;
        CryptoPP::SecByteBlock derived_key(output_length);
        
        hkdf.DeriveKey(
            derived_key,
            derived_key.size(),
            reinterpret_cast<const CryptoPP::byte*>(input_key_material.data()),
            input_key_material.size(),
            reinterpret_cast<const CryptoPP::byte*>(salt.data()),
            salt.size(),
            reinterpret_cast<const CryptoPP::byte*>(info.data()),
            info.size()
        );
        
        return std::string(derived_key.begin(), derived_key.end());
        
    } catch (const std::exception& e) {
        return "";
    }
}

std::string EncryptionManager::base64_encode(const std::string& input) {
    std::string encoded;
    CryptoPP::StringSource ss(input, true,
        new CryptoPP::Base64Encoder(new CryptoPP::StringSink(encoded), false));
    return encoded;
}

std::string EncryptionManager::base64_decode(const std::string& input) {
    std::string decoded;
    CryptoPP::StringSource ss(input, true,
        new CryptoPP::Base64Decoder(new CryptoPP::StringSink(decoded)));
    return decoded;
}

std::string EncryptionManager::generate_key_id() {
    return generate_random_id("key");
}

std::string EncryptionManager::generate_session_id() {
    return generate_random_id("sess");
}

std::string EncryptionManager::generate_message_id() {
    return generate_random_id("msg");
}

std::string EncryptionManager::generate_state_id() {
    return generate_random_id("state");
}

std::string EncryptionManager::generate_random_id(const std::string& prefix) {
    // 128-bit CSPRNG ID -> hex
    CryptoPP::SecByteBlock rnd(16);
    rng_->GenerateBlock(rnd, rnd.size());
    std::ostringstream oss;
    oss << prefix << "_";
    for (size_t i = 0; i < rnd.size(); ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(static_cast<unsigned char>(rnd[i]));
    }
    return oss.str();
}

std::string EncryptionManager::algorithm_to_string(EncryptionAlgorithm algorithm) {
    switch (algorithm) {
        case EncryptionAlgorithm::AES_256_GCM:
            return "AES-256-GCM";
        case EncryptionAlgorithm::CHACHA20_POLY1305:
            return "ChaCha20-Poly1305";
        case EncryptionAlgorithm::X25519_CHACHA20_POLY1305:
            return "X25519-ChaCha20-Poly1305";
        default:
            return "Unknown";
    }
}

// Helper methods for key size
size_t EncryptionManager::get_key_size(EncryptionAlgorithm algorithm) {
    switch (algorithm) {
        case EncryptionAlgorithm::AES_256_GCM:
            return 32; // 256 bits
        case EncryptionAlgorithm::CHACHA20_POLY1305:
            return 32; // 256 bits
        case EncryptionAlgorithm::X25519_CHACHA20_POLY1305:
            return 32; // 256 bits
        default:
            return 32;
    }
}

// Double Ratchet helper methods
std::string EncryptionManager::derive_chain_key(const std::string& input_key, const std::string& info) {
    return derive_key(input_key, info, 32);
}

std::string EncryptionManager::derive_message_key_from_chain(const std::string& chain_key, uint32_t message_number) {
    return derive_key(chain_key, "MessageKey" + std::to_string(message_number), 32);
}

void EncryptionManager::ratchet_sending_chain(DoubleRatchetState& state) {
    // Derive new chain key and message key
    state.sending_chain_key = derive_chain_key(state.sending_chain_key, "SendingChain");
    state.sending_chain_length++;
    state.sending_message_number++;
    state.messages_since_rekey++;
}

void EncryptionManager::ratchet_receiving_chain(DoubleRatchetState& state) {
    // Derive new chain key and message key
    state.receiving_chain_key = derive_chain_key(state.receiving_chain_key, "ReceivingChain");
    state.receiving_chain_length++;
    state.receiving_message_number++;
}

bool EncryptionManager::should_perform_dh_ratchet(const DoubleRatchetState& state) {
    return state.should_ratchet() || state.should_rekey();
}

void EncryptionManager::perform_rekey_if_needed(DoubleRatchetState& state) {
    if (state.should_rekey()) {
        // Generate new ratchet key pair
        auto new_ratchet_key_pair = generate_key_pair(EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
        
        // Update state with new keys
        state.our_ratchet_private_key = new_ratchet_key_pair.serialize_private_key();
        state.our_ratchet_public_key = new_ratchet_key_pair.serialize_public_key();
        
        // Reset counters
        state.messages_since_rekey = 0;
        state.last_ratchet = std::chrono::system_clock::now();
    }
}

// Advanced Double Ratchet methods
bool EncryptionManager::advance_sending_chain(const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    // Check if we need to perform DH ratchet
    if (should_perform_dh_ratchet(state)) {
        // Generate new ratchet key pair
        auto new_ratchet_key_pair = generate_key_pair(EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
        
        // Update state
        state.our_ratchet_private_key = new_ratchet_key_pair.serialize_private_key();
        state.our_ratchet_public_key = new_ratchet_key_pair.serialize_public_key();
        state.last_ratchet = std::chrono::system_clock::now();
        state.messages_since_rekey = 0;
    }
    
    // Advance the sending chain
    ratchet_sending_chain(state);
    
    return true;
}

bool EncryptionManager::advance_receiving_chain(const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    // Advance the receiving chain
    ratchet_receiving_chain(state);
    
    return true;
}

std::string EncryptionManager::get_sending_message_key(const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return "";
    }
    
    auto& state = state_it->second;
    
    // Derive message key from current chain key
    std::string message_key = derive_message_key_from_chain(state.sending_chain_key, state.sending_message_number);
    
    // Advance the chain for next use
    ratchet_sending_chain(state);
    
    return message_key;
}

std::string EncryptionManager::get_receiving_message_key(const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return "";
    }
    
    auto& state = state_it->second;
    
    // Check if we have a skipped key for this message number
    auto skipped_it = state.skipped_message_keys.find(state.receiving_message_number);
    if (skipped_it != state.skipped_message_keys.end()) {
        // Use the skipped key and remove it
        std::string key = skipped_it->second;
        state.skipped_message_keys.erase(skipped_it);
        return key;
    }
    
    // Derive message key from current chain key
    std::string message_key = derive_message_key_from_chain(state.receiving_chain_key, state.receiving_message_number);
    
    // Advance the chain for next use
    ratchet_receiving_chain(state);
    
    return message_key;
}

bool EncryptionManager::store_skipped_message_key(const std::string& chat_id, uint32_t message_number, const std::string& key) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    // Store the skipped key
    state.skipped_message_keys[message_number] = key;
    
    // Clean up old keys if needed
    state.cleanup_old_skipped_keys();
    
    return true;
}

std::string EncryptionManager::get_skipped_message_key(const std::string& chat_id, uint32_t message_number) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return "";
    }
    
    auto& state = state_it->second;
    
    auto key_it = state.skipped_message_keys.find(message_number);
    if (key_it != state.skipped_message_keys.end()) {
        std::string key = key_it->second;
        state.skipped_message_keys.erase(key_it);
        return key;
    }
    
    return "";
}

void EncryptionManager::cleanup_expired_ratchet_states() {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> expired_chats;
    
    for (const auto& [chat_id, state] : ratchet_states_) {
        // Consider a state expired if it hasn't been used in 30 days
        auto time_since_last_use = now - state.last_ratchet;
        if (time_since_last_use > std::chrono::days(30)) {
            expired_chats.push_back(chat_id);
        }
    }
    
    // Remove expired states
    for (const auto& chat_id : expired_chats) {
        ratchet_states_.erase(chat_id);
    }
}

// Message handling with Double Ratchet
bool EncryptionManager::process_incoming_message(const std::string& chat_id, uint32_t message_number, const std::string& encrypted_content) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    // Check if this is an out-of-order message
    if (message_number < state.receiving_message_number) {
        // This is a late message, check if we have the key
        auto skipped_key = get_skipped_message_key(chat_id, message_number);
        if (skipped_key.empty()) {
            // We don't have the key for this message, can't decrypt
            return false;
        }
        // Key found, message can be processed
        return true;
    }
    
    // Check if we need to skip ahead to this message number
    if (message_number > state.receiving_message_number) {
        // Generate keys for skipped messages and store them
        for (uint32_t i = state.receiving_message_number; i < message_number; i++) {
            std::string skipped_key = derive_message_key_from_chain(state.receiving_chain_key, i);
            store_skipped_message_key(chat_id, i, skipped_key);
        }
    }
    
    // Advance receiving chain to current message
    while (state.receiving_message_number < message_number) {
        ratchet_receiving_chain(state);
    }
    
    return true;
}

std::string EncryptionManager::prepare_outgoing_message(const std::string& chat_id, const std::string& plaintext) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return "";
    }
    
    auto& state = state_it->second;
    
    // Check if we need to perform DH ratchet
    if (should_perform_dh_ratchet(state)) {
        // Generate new ratchet key pair
        auto new_ratchet_key_pair = generate_key_pair(EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
        
        // Update state
        state.our_ratchet_private_key = new_ratchet_key_pair.serialize_private_key();
        state.our_ratchet_public_key = new_ratchet_key_pair.serialize_public_key();
        state.last_ratchet = std::chrono::system_clock::now();
        state.messages_since_rekey = 0;
    }
    
    // Get the current sending message key
    std::string message_key = derive_message_key_from_chain(state.sending_chain_key, state.sending_message_number);
    
    // Advance the sending chain
    ratchet_sending_chain(state);
    
    // For now, return the message key (in a real implementation, this would encrypt the plaintext)
    // The actual encryption would happen in the encrypt_message method
    return message_key;
}

// Security and recovery methods
bool EncryptionManager::mark_key_compromised(const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    // Clear all sensitive key material
    secure_zero_string(state.root_key);
    secure_zero_string(state.sending_chain_key);
    secure_zero_string(state.receiving_chain_key);
    secure_zero_string(state.our_ratchet_private_key);
    state.skipped_message_keys.clear();
    
    return true;
}

bool EncryptionManager::recover_from_compromise(const std::string& chat_id, const std::string& new_identity_key) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    // Generate new ratchet key pair
    auto new_ratchet_key_pair = generate_key_pair(EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
    
    // Update identity key
    state.our_identity_key = new_identity_key;
    
    // Generate new root key (this would typically come from a new key exchange)
    // For now, we'll generate a random one
    CryptoPP::SecByteBlock new_root_key(32);
    rng_->GenerateBlock(new_root_key, new_root_key.size());
    state.root_key.assign(new_root_key.begin(), new_root_key.end());
    
    // Reset chain keys
    state.sending_chain_key = derive_key(state.root_key, "SendingChain", 32);
    state.receiving_chain_key = derive_key(state.root_key, "ReceivingChain", 32);
    
    // Update ratchet keys
    state.our_ratchet_private_key = new_ratchet_key_pair.serialize_private_key();
    
    // Reset counters
    state.sending_message_number = 0;
    state.receiving_message_number = 0;
    state.sending_chain_length = 0;
    state.receiving_chain_length = 0;
    state.messages_since_rekey = 0;
    state.last_ratchet = std::chrono::system_clock::now();
    
    // Clear skipped keys
    state.skipped_message_keys.clear();
    
    return true;
}

std::string EncryptionManager::export_ratchet_state(const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return "";
    }
    
    const auto& state = state_it->second;
    
    // Export state to JSON (excluding sensitive private keys)
    Json::Value json;
    json["chat_id"] = state.chat_id;
    json["our_identity_key"] = state.our_identity_key;
    json["their_identity_key"] = state.their_identity_key;
    json["our_ratchet_public_key"] = state.our_ratchet_public_key;
    json["their_ratchet_public_key"] = state.their_ratchet_public_key;
    json["sending_message_number"] = state.sending_message_number;
    json["receiving_message_number"] = state.receiving_message_number;
    json["sending_chain_length"] = state.sending_chain_length;
    json["receiving_chain_length"] = state.receiving_chain_length;
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        state.created_at.time_since_epoch()).count();
    json["last_ratchet"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        state.last_ratchet.time_since_epoch()).count();
    
    // Convert to string
    Json::StreamWriterBuilder wb;
    return Json::writeString(wb, json);
}

bool EncryptionManager::import_ratchet_state(const std::string& chat_id, const std::string& state_data) {
    try {
        Json::Value json;
        Json::CharReaderBuilder rb;
        std::string errs;
        std::unique_ptr<Json::CharReader> reader(rb.newCharReader());
        
        if (!reader->parse(state_data.data(), state_data.data() + state_data.size(), &json, &errs)) {
            return false;
        }
        
        // Create new state
        DoubleRatchetState state;
        state.chat_id = chat_id;
        state.our_identity_key = json["our_identity_key"].asString();
        state.their_identity_key = json["their_identity_key"].asString();
        state.our_ratchet_public_key = json["our_ratchet_public_key"].asString();
        state.their_ratchet_public_key = json["their_ratchet_public_key"].asString();
        state.sending_message_number = json["sending_message_number"].asUInt();
        state.receiving_message_number = json["receiving_message_number"].asUInt();
        state.sending_chain_length = json["sending_chain_length"].asUInt();
        state.receiving_chain_length = json["receiving_chain_length"].asUInt();
        
        auto created_ms = json["created_at"].asInt64();
        state.created_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(created_ms));
        
        auto last_ratchet_ms = json["last_ratchet"].asInt64();
        state.last_ratchet = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(last_ratchet_ms));
        
        // Generate new private keys and derive chain keys
        auto new_ratchet_key_pair = generate_key_pair(EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
        state.our_ratchet_private_key = new_ratchet_key_pair.serialize_private_key();
        
        // Generate new root key and chain keys
        CryptoPP::SecByteBlock new_root_key(32);
        rng_->GenerateBlock(new_root_key, new_root_key.size());
        state.root_key.assign(new_root_key.begin(), new_root_key.end());
        
        state.sending_chain_key = derive_key(state.root_key, "SendingChain", 32);
        state.receiving_chain_key = derive_key(state.root_key, "ReceivingChain", 32);
        
        // Store the state
        std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
        ratchet_states_[chat_id] = state;
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

// Group chat encryption (interim implementation)
bool EncryptionManager::add_group_member(const std::string& chat_id, const std::string& user_id, const std::string& public_key) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    // For now, we'll just store the member info
    // In a full implementation, this would:
    // 1. Generate a new group key
    // 2. Encrypt it with the new member's public key
    // 3. Distribute the encrypted group key to all members
    
    // This is a placeholder for the group member management
    // The actual implementation would be more complex with proper group key management
    
    return true;
}

bool EncryptionManager::remove_group_member(const std::string& chat_id, const std::string& user_id) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return false;
    }
    
    auto& state = state_it->second;
    
    // For now, we'll just return success
    // In a full implementation, this would:
    // 1. Generate a new group key
    // 2. Re-encrypt all existing messages with the new key
    // 3. Distribute the new key to remaining members
    
    return true;
}

std::vector<std::string> EncryptionManager::get_group_members(const std::string& chat_id) {
    std::lock_guard<std::mutex> lock(ratchet_states_mutex_);
    
    auto state_it = ratchet_states_.find(chat_id);
    if (state_it == ratchet_states_.end()) {
        return {};
    }
    
    // For now, return empty list
    // In a full implementation, this would return the actual group members
    // stored in the ratchet state or a separate group management structure
    
    return {};
}

} // namespace sonet::messaging::encryption
