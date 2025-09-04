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
#include <json/json.h>

namespace sonet::messaging {

enum class MessageType {
    TEXT,
    IMAGE,
    VIDEO,
    AUDIO,
    FILE,
    LOCATION,
    VOICE_NOTE,
    STICKER,
    SYSTEM_MESSAGE
};

enum class MessageStatus {
    PENDING,
    SENT,
    DELIVERED,
    READ,
    FAILED,
    DELETED
};

enum class EncryptionLevel {
    NONE,
    SERVER_SIDE,
    END_TO_END,
    MILITARY_GRADE
};

enum class DeliveryPriority {
    LOW = 0,
    NORMAL = 1,
    HIGH = 2,
    URGENT = 3,
    CRITICAL = 4
};

struct MessageReaction {
    std::string user_id;
    std::string emoji;
    std::chrono::system_clock::time_point created_at;
    
    Json::Value to_json() const;
    static MessageReaction from_json(const Json::Value& json);
};

struct MessageAttachment {
    std::string id;
    std::string filename;
    std::string content_type;
    size_t file_size;
    std::string storage_url;
    std::string thumbnail_url;
    std::unordered_map<std::string, std::string> metadata;
    
    // Encryption info for attachments
    std::string encryption_key_id;
    std::string encryption_iv;
    std::string encryption_hash;
    
    Json::Value to_json() const;
    static MessageAttachment from_json(const Json::Value& json);
};

struct MessageForwardInfo {
    std::string original_message_id;
    std::string original_sender_id;
    std::string original_chat_id;
    std::chrono::system_clock::time_point original_timestamp;
    int forward_count = 1;
    
    Json::Value to_json() const;
    static MessageForwardInfo from_json(const Json::Value& json);
};

struct MessageEncryption {
    EncryptionLevel level;
    std::string algorithm;
    std::string key_id;
    std::string initialization_vector;
    std::string signature;
    std::string session_key_fingerprint;
    std::optional<std::string> sender_key_fingerprint;
    std::optional<std::string> recipient_key_fingerprint;
    bool perfect_forward_secrecy = true;
    
    Json::Value to_json() const;
    static MessageEncryption from_json(const Json::Value& json);
};

struct MessageReadReceipt {
    std::string user_id;
    std::chrono::system_clock::time_point read_at;
    std::string device_id;
    
    Json::Value to_json() const;
    static MessageReadReceipt from_json(const Json::Value& json);
};

class Message {
public:
    // Core message data
    std::string id;
    std::string chat_id;
    std::string sender_id;
    std::string content;
    MessageType type;
    MessageStatus status;
    DeliveryPriority priority;
    
    // Timestamps
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::optional<std::chrono::system_clock::time_point> expires_at;
    std::optional<std::chrono::system_clock::time_point> scheduled_at;
    
    // Thread and reply support
    std::optional<std::string> reply_to_message_id;
    std::optional<std::string> thread_id;
    int thread_position = 0;
    
    // Rich content
    std::vector<MessageAttachment> attachments;
    std::vector<MessageReaction> reactions;
    std::unordered_map<std::string, std::string> metadata;
    
    // Forward information
    std::optional<MessageForwardInfo> forward_info;
    
    // Encryption data
    MessageEncryption encryption;
    
    // Read receipts
    std::vector<MessageReadReceipt> read_receipts;
    
    // Edit history
    std::vector<std::string> edit_history;
    std::optional<std::chrono::system_clock::time_point> last_edited_at;
    
    // Message validation
    bool is_valid() const;
    bool is_encrypted() const;
    bool is_expired() const;
    bool can_be_edited() const;
    bool can_be_deleted() const;
    
    // Content operations
    void add_attachment(const MessageAttachment& attachment);
    void add_reaction(const MessageReaction& reaction);
    void remove_reaction(const std::string& user_id, const std::string& emoji);
    void mark_as_read(const std::string& user_id, const std::string& device_id = "");
    void mark_as_delivered();
    void mark_as_failed(const std::string& error_reason = "");
    
    // Encryption operations
    void set_encryption(EncryptionLevel level, const std::string& key_id, 
                       const std::string& algorithm = "AES-256-GCM");
    bool verify_signature() const;
    void update_encryption_signature();
    
    // Content modification
    void edit_content(const std::string& new_content);
    void schedule_for_deletion(std::chrono::seconds delay);
    
    // Serialization
    Json::Value to_json() const;
    static std::unique_ptr<Message> from_json(const Json::Value& json);
    
    // Database operations
    std::string to_sql_insert() const;
    std::string to_sql_update() const;
    static std::unique_ptr<Message> from_sql_row(const std::vector<std::string>& row);
    
    // Size and performance
    size_t calculate_size() const;
    bool is_oversized(size_t max_size = 10485760) const; // 10MB default
    
    // Security
    void sanitize_content();
    bool has_malicious_content() const;
    std::string get_content_hash() const;
    
    // Constructors
    Message() = default;
    Message(const std::string& chat_id, const std::string& sender_id, 
            const std::string& content, MessageType type = MessageType::TEXT);
    
    // Comparison operators
    bool operator==(const Message& other) const;
    bool operator<(const Message& other) const;
};

// Message builder for complex message construction
class MessageBuilder {
private:
    std::unique_ptr<Message> message_;
    
public:
    MessageBuilder(const std::string& chat_id, const std::string& sender_id);
    
    MessageBuilder& content(const std::string& content);
    MessageBuilder& type(MessageType type);
    MessageBuilder& priority(DeliveryPriority priority);
    MessageBuilder& reply_to(const std::string& message_id);
    MessageBuilder& thread(const std::string& thread_id, int position = 0);
    MessageBuilder& expires_in(std::chrono::seconds duration);
    MessageBuilder& schedule_for(std::chrono::system_clock::time_point when);
    MessageBuilder& add_attachment(const MessageAttachment& attachment);
    MessageBuilder& add_metadata(const std::string& key, const std::string& value);
    MessageBuilder& encrypt_with(EncryptionLevel level, const std::string& key_id);
    MessageBuilder& forward_from(const Message& original);
    
    std::unique_ptr<Message> build();
};

// Message utilities
class MessageUtils {
public:
    static std::string generate_message_id();
    static std::string generate_thread_id();
    static bool is_valid_message_id(const std::string& id);
    static MessageType detect_message_type(const std::string& content);
    static std::string sanitize_message_content(const std::string& content);
    static size_t calculate_message_size(const Message& message);
    static bool should_compress_message(const Message& message);
    static std::vector<std::string> extract_mentions(const std::string& content);
    static std::vector<std::string> extract_hashtags(const std::string& content);
    static std::string format_message_preview(const Message& message, size_t max_length = 100);
    static bool is_spam_message(const Message& message);
    static double calculate_message_priority_score(const Message& message);
};

} // namespace sonet::messaging
