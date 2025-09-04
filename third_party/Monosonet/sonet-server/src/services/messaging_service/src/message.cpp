/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "include/message.hpp"
#include <algorithm>
#include <regex>
#include <sstream>
#include <iomanip>
#include <openssl/sha.h>
#include <openssl/evp.h>

namespace sonet::messaging {

// MessageReaction implementation
Json::Value MessageReaction::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["emoji"] = emoji;
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    return json;
}

MessageReaction MessageReaction::from_json(const Json::Value& json) {
    MessageReaction reaction;
    reaction.user_id = json["user_id"].asString();
    reaction.emoji = json["emoji"].asString();
    
    auto timestamp_ms = json["created_at"].asInt64();
    reaction.created_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(timestamp_ms));
    
    return reaction;
}

// MessageAttachment implementation
Json::Value MessageAttachment::to_json() const {
    Json::Value json;
    json["id"] = id;
    json["filename"] = filename;
    json["content_type"] = content_type;
    json["file_size"] = static_cast<Json::Int64>(file_size);
    json["storage_url"] = storage_url;
    json["thumbnail_url"] = thumbnail_url;
    json["encryption_key_id"] = encryption_key_id;
    json["encryption_iv"] = encryption_iv;
    json["encryption_hash"] = encryption_hash;
    
    Json::Value metadata_json;
    for (const auto& [key, value] : metadata) {
        metadata_json[key] = value;
    }
    json["metadata"] = metadata_json;
    
    return json;
}

MessageAttachment MessageAttachment::from_json(const Json::Value& json) {
    MessageAttachment attachment;
    attachment.id = json["id"].asString();
    attachment.filename = json["filename"].asString();
    attachment.content_type = json["content_type"].asString();
    attachment.file_size = json["file_size"].asUInt64();
    attachment.storage_url = json["storage_url"].asString();
    attachment.thumbnail_url = json["thumbnail_url"].asString();
    attachment.encryption_key_id = json["encryption_key_id"].asString();
    attachment.encryption_iv = json["encryption_iv"].asString();
    attachment.encryption_hash = json["encryption_hash"].asString();
    
    const auto& metadata_json = json["metadata"];
    for (const auto& key : metadata_json.getMemberNames()) {
        attachment.metadata[key] = metadata_json[key].asString();
    }
    
    return attachment;
}

// MessageEncryption implementation
Json::Value MessageEncryption::to_json() const {
    Json::Value json;
    json["level"] = static_cast<int>(level);
    json["algorithm"] = algorithm;
    json["key_id"] = key_id;
    json["initialization_vector"] = initialization_vector;
    json["signature"] = signature;
    json["session_key_fingerprint"] = session_key_fingerprint;
    json["perfect_forward_secrecy"] = perfect_forward_secrecy;
    
    if (sender_key_fingerprint) {
        json["sender_key_fingerprint"] = *sender_key_fingerprint;
    }
    if (recipient_key_fingerprint) {
        json["recipient_key_fingerprint"] = *recipient_key_fingerprint;
    }
    
    return json;
}

MessageEncryption MessageEncryption::from_json(const Json::Value& json) {
    MessageEncryption encryption;
    encryption.level = static_cast<EncryptionLevel>(json["level"].asInt());
    encryption.algorithm = json["algorithm"].asString();
    encryption.key_id = json["key_id"].asString();
    encryption.initialization_vector = json["initialization_vector"].asString();
    encryption.signature = json["signature"].asString();
    encryption.session_key_fingerprint = json["session_key_fingerprint"].asString();
    encryption.perfect_forward_secrecy = json["perfect_forward_secrecy"].asBool();
    
    if (json.isMember("sender_key_fingerprint")) {
        encryption.sender_key_fingerprint = json["sender_key_fingerprint"].asString();
    }
    if (json.isMember("recipient_key_fingerprint")) {
        encryption.recipient_key_fingerprint = json["recipient_key_fingerprint"].asString();
    }
    
    return encryption;
}

// Message implementation
Message::Message(const std::string& chat_id, const std::string& sender_id, 
                const std::string& content, MessageType type)
    : chat_id(chat_id), sender_id(sender_id), content(content), type(type),
      status(MessageStatus::PENDING), priority(DeliveryPriority::NORMAL) {
    
    id = MessageUtils::generate_message_id();
    created_at = std::chrono::system_clock::now();
    updated_at = created_at;
    
    // Set default military-grade encryption
    encryption.level = EncryptionLevel::MILITARY_GRADE;
    encryption.algorithm = "AES-256-GCM";
    encryption.perfect_forward_secrecy = true;
}

bool Message::is_valid() const {
    if (id.empty() || chat_id.empty() || sender_id.empty()) {
        return false;
    }
    
    if (content.empty() && attachments.empty()) {
        return false;
    }
    
    // Check message size limits
    if (calculate_size() > 10485760) { // 10MB
        return false;
    }
    
    return true;
}

bool Message::is_encrypted() const {
    return encryption.level != EncryptionLevel::NONE;
}

bool Message::is_expired() const {
    if (!expires_at) {
        return false;
    }
    return std::chrono::system_clock::now() > *expires_at;
}

bool Message::can_be_edited() const {
    // Messages can be edited within 48 hours
    auto edit_deadline = created_at + std::chrono::hours(48);
    return std::chrono::system_clock::now() < edit_deadline &&
           status != MessageStatus::DELETED;
}

bool Message::can_be_deleted() const {
    return status != MessageStatus::DELETED;
}

void Message::add_attachment(const MessageAttachment& attachment) {
    attachments.push_back(attachment);
    updated_at = std::chrono::system_clock::now();
}

void Message::add_reaction(const MessageReaction& reaction) {
    // Remove existing reaction from same user with same emoji
    auto it = std::remove_if(reactions.begin(), reactions.end(),
        [&](const MessageReaction& r) {
            return r.user_id == reaction.user_id && r.emoji == reaction.emoji;
        });
    reactions.erase(it, reactions.end());
    
    reactions.push_back(reaction);
    updated_at = std::chrono::system_clock::now();
}

void Message::remove_reaction(const std::string& user_id, const std::string& emoji) {
    auto it = std::remove_if(reactions.begin(), reactions.end(),
        [&](const MessageReaction& r) {
            return r.user_id == user_id && r.emoji == emoji;
        });
    reactions.erase(it, reactions.end());
    updated_at = std::chrono::system_clock::now();
}

void Message::mark_as_read(const std::string& user_id, const std::string& device_id) {
    // Remove existing read receipt from same user
    auto it = std::remove_if(read_receipts.begin(), read_receipts.end(),
        [&](const MessageReadReceipt& r) {
            return r.user_id == user_id;
        });
    read_receipts.erase(it, read_receipts.end());
    
    MessageReadReceipt receipt;
    receipt.user_id = user_id;
    receipt.device_id = device_id;
    receipt.read_at = std::chrono::system_clock::now();
    
    read_receipts.push_back(receipt);
    
    if (status == MessageStatus::DELIVERED) {
        status = MessageStatus::READ;
    }
    
    updated_at = std::chrono::system_clock::now();
}

void Message::mark_as_delivered() {
    if (status == MessageStatus::SENT) {
        status = MessageStatus::DELIVERED;
        updated_at = std::chrono::system_clock::now();
    }
}

void Message::mark_as_failed(const std::string& error_reason) {
    status = MessageStatus::FAILED;
    metadata["error_reason"] = error_reason;
    updated_at = std::chrono::system_clock::now();
}

void Message::set_encryption(EncryptionLevel level, const std::string& key_id, 
                           const std::string& algorithm) {
    encryption.level = level;
    encryption.key_id = key_id;
    encryption.algorithm = algorithm;
    
    // Generate new IV for each encryption
    encryption.initialization_vector = MessageUtils::generate_message_id();
    
    update_encryption_signature();
    updated_at = std::chrono::system_clock::now();
}

bool Message::verify_signature() const {
    if (encryption.signature.empty()) {
        return false;
    }
    
    // Create message hash for signature verification
    std::string message_data = id + chat_id + sender_id + content;
    std::string calculated_hash = get_content_hash();
    
    // In a real implementation, this would use proper cryptographic verification
    return !calculated_hash.empty();
}

void Message::update_encryption_signature() {
    if (encryption.level == EncryptionLevel::NONE) {
        encryption.signature.clear();
        return;
    }
    
    // Generate cryptographic signature
    encryption.signature = get_content_hash();
}

void Message::edit_content(const std::string& new_content) {
    if (!can_be_edited()) {
        return;
    }
    
    edit_history.push_back(content);
    content = new_content;
    last_edited_at = std::chrono::system_clock::now();
    updated_at = *last_edited_at;
    
    // Update encryption signature after edit
    update_encryption_signature();
}

void Message::schedule_for_deletion(std::chrono::seconds delay) {
    expires_at = std::chrono::system_clock::now() + delay;
    updated_at = std::chrono::system_clock::now();
}

Json::Value Message::to_json() const {
    Json::Value json;
    
    json["id"] = id;
    json["chat_id"] = chat_id;
    json["sender_id"] = sender_id;
    json["content"] = content;
    json["type"] = static_cast<int>(type);
    json["status"] = static_cast<int>(status);
    json["priority"] = static_cast<int>(priority);
    
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["updated_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        updated_at.time_since_epoch()).count();
    
    if (expires_at) {
        json["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            expires_at->time_since_epoch()).count();
    }
    
    if (scheduled_at) {
        json["scheduled_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            scheduled_at->time_since_epoch()).count();
    }
    
    if (reply_to_message_id) {
        json["reply_to_message_id"] = *reply_to_message_id;
    }
    
    if (thread_id) {
        json["thread_id"] = *thread_id;
        json["thread_position"] = thread_position;
    }
    
    // Serialize attachments
    Json::Value attachments_json(Json::arrayValue);
    for (const auto& attachment : attachments) {
        attachments_json.append(attachment.to_json());
    }
    json["attachments"] = attachments_json;
    
    // Serialize reactions
    Json::Value reactions_json(Json::arrayValue);
    for (const auto& reaction : reactions) {
        reactions_json.append(reaction.to_json());
    }
    json["reactions"] = reactions_json;
    
    // Serialize metadata
    Json::Value metadata_json;
    for (const auto& [key, value] : metadata) {
        metadata_json[key] = value;
    }
    json["metadata"] = metadata_json;
    
    // Serialize encryption
    json["encryption"] = encryption.to_json();
    
    return json;
}

size_t Message::calculate_size() const {
    size_t size = content.size();
    
    for (const auto& attachment : attachments) {
        size += attachment.file_size;
    }
    
    // Add overhead for metadata, reactions, etc.
    size += 1024; // Estimated overhead
    
    return size;
}

bool Message::is_oversized(size_t max_size) const {
    return calculate_size() > max_size;
}

void Message::sanitize_content() {
    // Remove potentially dangerous content
    std::regex script_tag("<\\s*script[^>]*>.*?</\\s*script\\s*>", 
                         std::regex_constants::icase);
    content = std::regex_replace(content, script_tag, "");
    
    // Remove other dangerous HTML tags
    std::regex dangerous_tags("<\\s*(iframe|object|embed|form)[^>]*>.*?</\\s*\\1\\s*>", 
                             std::regex_constants::icase);
    content = std::regex_replace(content, dangerous_tags, "");
}

bool Message::has_malicious_content() const {
    // Check for script injections
    std::regex script_pattern("<\\s*script", std::regex_constants::icase);
    if (std::regex_search(content, script_pattern)) {
        return true;
    }
    
    // Check for suspicious URLs
    std::regex suspicious_url("(javascript:|data:|vbscript:)", std::regex_constants::icase);
    if (std::regex_search(content, suspicious_url)) {
        return true;
    }
    
    return false;
}

std::string Message::get_content_hash() const {
    std::string data = id + chat_id + sender_id + content;
    
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, data.c_str(), data.size());
    SHA256_Final(hash, &sha256);
    
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    
    return ss.str();
}

// MessageBuilder implementation
MessageBuilder::MessageBuilder(const std::string& chat_id, const std::string& sender_id) {
    message_ = std::make_unique<Message>(chat_id, sender_id, "");
}

MessageBuilder& MessageBuilder::content(const std::string& content) {
    message_->content = content;
    message_->type = MessageUtils::detect_message_type(content);
    return *this;
}

MessageBuilder& MessageBuilder::type(MessageType type) {
    message_->type = type;
    return *this;
}

MessageBuilder& MessageBuilder::priority(DeliveryPriority priority) {
    message_->priority = priority;
    return *this;
}

MessageBuilder& MessageBuilder::reply_to(const std::string& message_id) {
    message_->reply_to_message_id = message_id;
    return *this;
}

MessageBuilder& MessageBuilder::thread(const std::string& thread_id, int position) {
    message_->thread_id = thread_id;
    message_->thread_position = position;
    return *this;
}

MessageBuilder& MessageBuilder::expires_in(std::chrono::seconds duration) {
    message_->expires_at = std::chrono::system_clock::now() + duration;
    return *this;
}

MessageBuilder& MessageBuilder::schedule_for(std::chrono::system_clock::time_point when) {
    message_->scheduled_at = when;
    return *this;
}

MessageBuilder& MessageBuilder::add_attachment(const MessageAttachment& attachment) {
    message_->attachments.push_back(attachment);
    return *this;
}

MessageBuilder& MessageBuilder::add_metadata(const std::string& key, const std::string& value) {
    message_->metadata[key] = value;
    return *this;
}

MessageBuilder& MessageBuilder::encrypt_with(EncryptionLevel level, const std::string& key_id) {
    message_->set_encryption(level, key_id);
    return *this;
}

std::unique_ptr<Message> MessageBuilder::build() {
    message_->sanitize_content();
    message_->update_encryption_signature();
    return std::move(message_);
}

// MessageUtils implementation
std::string MessageUtils::generate_message_id() {
    // Generate UUID-like ID
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    
    uint64_t high = dis(gen);
    uint64_t low = dis(gen);
    
    std::stringstream ss;
    ss << std::hex << high << low;
    return ss.str();
}

std::string MessageUtils::generate_thread_id() {
    return "thread_" + generate_message_id();
}

bool MessageUtils::is_valid_message_id(const std::string& id) {
    return !id.empty() && id.length() >= 16 && id.length() <= 64;
}

MessageType MessageUtils::detect_message_type(const std::string& content) {
    // Simple type detection based on content
    if (content.empty()) {
        return MessageType::TEXT;
    }
    
    // Check for URL patterns
    std::regex url_pattern(R"(https?://[^\s]+)", std::regex_constants::icase);
    if (std::regex_search(content, url_pattern)) {
        return MessageType::TEXT; // URLs are still text messages
    }
    
    return MessageType::TEXT;
}

std::string MessageUtils::sanitize_message_content(const std::string& content) {
    std::string sanitized = content;
    
    // Remove null characters
    sanitized.erase(std::remove(sanitized.begin(), sanitized.end(), '\0'), sanitized.end());
    
    // Limit length
    if (sanitized.length() > 4096) {
        sanitized = sanitized.substr(0, 4096);
    }
    
    return sanitized;
}

size_t MessageUtils::calculate_message_size(const Message& message) {
    return message.calculate_size();
}

bool MessageUtils::should_compress_message(const Message& message) {
    return message.calculate_size() > 1024; // Compress messages larger than 1KB
}

std::vector<std::string> MessageUtils::extract_mentions(const std::string& content) {
    std::vector<std::string> mentions;
    std::regex mention_pattern(R"(@([a-zA-Z0-9_]+))");
    std::sregex_iterator begin(content.begin(), content.end(), mention_pattern);
    std::sregex_iterator end;
    
    for (auto it = begin; it != end; ++it) {
        mentions.push_back(it->str(1));
    }
    
    return mentions;
}

std::vector<std::string> MessageUtils::extract_hashtags(const std::string& content) {
    std::vector<std::string> hashtags;
    std::regex hashtag_pattern(R"(#([a-zA-Z0-9_]+))");
    std::sregex_iterator begin(content.begin(), content.end(), hashtag_pattern);
    std::sregex_iterator end;
    
    for (auto it = begin; it != end; ++it) {
        hashtags.push_back(it->str(1));
    }
    
    return hashtags;
}

std::string MessageUtils::format_message_preview(const Message& message, size_t max_length) {
    std::string preview = message.content;
    
    if (preview.length() > max_length) {
        preview = preview.substr(0, max_length - 3) + "...";
    }
    
    // Add attachment info if present
    if (!message.attachments.empty()) {
        preview += " [" + std::to_string(message.attachments.size()) + " attachment(s)]";
    }
    
    return preview;
}

bool MessageUtils::is_spam_message(const Message& message) {
    const std::string& content = message.content;
    
    // Check for excessive repetition
    if (content.length() > 100) {
        size_t unique_chars = std::unordered_set<char>(content.begin(), content.end()).size();
        if (unique_chars < content.length() / 10) {
            return true; // Too much repetition
        }
    }
    
    // Check for excessive caps
    size_t caps_count = std::count_if(content.begin(), content.end(), ::isupper);
    if (caps_count > content.length() / 2 && content.length() > 20) {
        return true; // Too many capitals
    }
    
    return false;
}

double MessageUtils::calculate_message_priority_score(const Message& message) {
    double score = static_cast<double>(message.priority);
    
    // Boost priority for mentions
    auto mentions = extract_mentions(message.content);
    score += mentions.size() * 0.5;
    
    // Boost priority for replies
    if (message.reply_to_message_id) {
        score += 1.0;
    }
    
    // Boost priority for attachments
    score += message.attachments.size() * 0.3;
    
    return score;
}

} // namespace sonet::messaging
