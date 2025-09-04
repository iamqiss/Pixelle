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
#include <functional>
#include <mutex>
#include <atomic>
#include <thread>
#include <json/json.h>
#include <grpcpp/grpcpp.h>
#include "message.hpp"
#include "chat.hpp"
#include "crypto_engine.hpp"
#include "encryption_manager.hpp"
#include "websocket_manager.hpp"
#include "../../user_service/include/jwt_manager.h"

namespace sonet::messaging {

struct AttachmentUpload {
    std::string id;
    std::string filename;
    std::string content_type;
    std::vector<uint8_t> data;
    std::string uploader_id;
    std::chrono::system_clock::time_point uploaded_at;
    std::unordered_map<std::string, std::string> metadata;
    
    // Encryption info
    std::string encryption_key_id;
    std::vector<uint8_t> encrypted_data;
    std::vector<uint8_t> encryption_iv;
    
    Json::Value to_json() const;
    static std::unique_ptr<AttachmentUpload> from_json(const Json::Value& json);
    size_t get_size() const;
    bool is_valid() const;
};

struct MessageSearchQuery {
    std::string query_text;
    std::vector<std::string> chat_ids;
    std::vector<std::string> sender_ids;
    std::optional<MessageType> message_type;
    std::optional<std::chrono::system_clock::time_point> start_date;
    std::optional<std::chrono::system_clock::time_point> end_date;
    bool include_attachments = false;
    uint32_t limit = 50;
    uint32_t offset = 0;
    
    Json::Value to_json() const;
    static MessageSearchQuery from_json(const Json::Value& json);
    bool is_valid() const;
};

struct MessageSearchResult {
    std::vector<std::unique_ptr<Message>> messages;
    uint32_t total_count;
    uint32_t page_count;
    bool has_more;
    std::chrono::milliseconds search_time;
    
    Json::Value to_json() const;
    static std::unique_ptr<MessageSearchResult> from_json(const Json::Value& json);
};

struct MessagingStats {
    uint64_t total_messages = 0;
    uint64_t total_chats = 0;
    uint64_t total_users = 0;
    uint64_t active_users_today = 0;
    uint64_t messages_today = 0;
    uint64_t storage_used_bytes = 0;
    uint32_t realtime_connections = 0;
    double average_message_size = 0.0;
    std::chrono::milliseconds average_response_time;
    std::unordered_map<MessageType, uint64_t> message_type_counts;
    std::unordered_map<std::string, uint64_t> chat_activity;
    
    Json::Value to_json() const;
    void update_message_stats(const Message& message);
    void update_chat_stats(const Chat& chat);
};

class MessagingController {
private:
    // Core components
    std::unique_ptr<ChatManager> chat_manager_;
    std::unique_ptr<encryption::EncryptionManager> encryption_manager_;
    std::unique_ptr<realtime::WebSocketManager> websocket_manager_;
    std::unique_ptr<sonet::user::JWTManager> jwt_manager_;
    
    // Database and storage
    std::string database_connection_string_;
    std::string redis_connection_string_;
    std::string storage_base_path_;
    
    // Message storage
    std::unordered_map<std::string, std::vector<std::unique_ptr<Message>>> chat_messages_;
    std::mutex messages_mutex_;
    
    // Attachment storage
    std::unordered_map<std::string, std::unique_ptr<AttachmentUpload>> attachments_;
    std::mutex attachments_mutex_;
    
    // Performance monitoring
    MessagingStats stats_;
    std::mutex stats_mutex_;
    std::thread stats_update_thread_;
    std::atomic<bool> running_;
    
    // Configuration
    uint64_t max_message_size_;
    uint64_t max_attachment_size_;
    uint32_t message_retention_days_;
    bool encryption_enabled_;
    bool disappearing_messages_enabled_;
    std::chrono::seconds default_message_ttl_;
    
    // Rate limiting
    std::unordered_map<std::string, std::chrono::system_clock::time_point> user_last_message_;
    std::unordered_map<std::string, uint32_t> user_message_counts_;
    std::mutex rate_limiting_mutex_;
    uint32_t messages_per_minute_limit_;
    
    // Background tasks
    std::thread cleanup_thread_;
    std::thread encryption_key_rotation_thread_;
    
    // Replay protection (in-memory, short-lived)
    std::mutex replay_mutex_;
    // key -> seen_at; key format: chatId|userId|iv|tag
    std::unordered_map<std::string, std::chrono::system_clock::time_point> replay_seen_;
    std::chrono::seconds replay_ttl_{600}; // 10 minutes
    void replay_cleanup_locked_();
    bool check_and_mark_replay_locked_(const std::string& chat_id, const std::string& user_id,
                                       const std::string& iv_b64, const std::string& tag_b64);

    // Internal methods
    bool validate_message_content(const Message& message);
    bool check_rate_limits(const std::string& user_id);
    void update_user_activity(const std::string& user_id);
    void cleanup_expired_messages();
    void cleanup_old_attachments();
    void rotate_encryption_keys();
    void update_statistics();
    bool store_message_to_database(const Message& message);
    bool store_attachment_to_storage(const AttachmentUpload& attachment);
    std::vector<std::unique_ptr<Message>> load_messages_from_database(
        const std::string& chat_id, uint32_t limit, uint32_t offset);
    
public:
    MessagingController();
    ~MessagingController();
    
    // Initialization and configuration
    bool initialize(const Json::Value& config);
    bool start();
    void shutdown();
    bool is_running() const;
    
    // HTTP endpoint handlers
    boost::beast::http::response<boost::beast::http::string_body> handle_chats_endpoint(const boost::beast::http::request<boost::beast::http::string_body>& req);
    
    // Message operations
    std::unique_ptr<Message> send_message(
        const std::string& chat_id,
        const std::string& sender_id,
        const std::string& content,
        MessageType type = MessageType::TEXT,
        const std::optional<std::string>& reply_to_message_id = std::nullopt
    );
    
    std::unique_ptr<Message> send_encrypted_message(
        const std::string& chat_id,
        const std::string& sender_id,
        const std::string& content,
        MessageType type = MessageType::TEXT,
        const std::optional<std::string>& reply_to_message_id = std::nullopt
    );
    
    bool edit_message(
        const std::string& message_id,
        const std::string& user_id,
        const std::string& new_content
    );
    
    bool delete_message(
        const std::string& message_id,
        const std::string& user_id,
        bool delete_for_everyone = false
    );
    
    bool mark_message_as_read(
        const std::string& message_id,
        const std::string& user_id,
        const std::string& device_id = ""
    );
    
    std::unique_ptr<Message> forward_message(
        const std::string& message_id,
        const std::string& target_chat_id,
        const std::string& sender_id
    );
    
    // Message retrieval
    std::vector<std::unique_ptr<Message>> get_chat_messages(
        const std::string& chat_id,
        const std::string& user_id,
        uint32_t limit = 50,
        uint32_t offset = 0
    );
    
    std::unique_ptr<Message> get_message(
        const std::string& message_id,
        const std::string& user_id
    );
    
    std::vector<std::unique_ptr<Message>> get_messages_after(
        const std::string& chat_id,
        const std::string& user_id,
        const std::string& after_message_id,
        uint32_t limit = 50
    );
    
    std::vector<std::unique_ptr<Message>> get_messages_before(
        const std::string& chat_id,
        const std::string& user_id,
        const std::string& before_message_id,
        uint32_t limit = 50
    );
    
    // Message search
    std::unique_ptr<MessageSearchResult> search_messages(
        const MessageSearchQuery& query,
        const std::string& user_id
    );
    
    std::vector<std::unique_ptr<Message>> search_messages_in_chat(
        const std::string& chat_id,
        const std::string& query_text,
        const std::string& user_id,
        uint32_t limit = 50
    );
    
    // Chat operations
    std::unique_ptr<Chat> create_direct_message(
        const std::string& user1_id,
        const std::string& user2_id
    );
    
    std::unique_ptr<Chat> create_group_chat(
        const std::string& name,
        const std::string& owner_id,
        const std::vector<std::string>& participant_ids,
        const ChatSettings& settings = ChatSettings()
    );
    
    std::unique_ptr<Chat> create_secret_chat(
        const std::string& user1_id,
        const std::string& user2_id,
        std::chrono::seconds message_ttl = std::chrono::seconds(3600)
    );
    
    std::shared_ptr<Chat> get_chat(
        const std::string& chat_id,
        const std::string& user_id
    );
    
    std::vector<std::shared_ptr<Chat>> get_user_chats(const std::string& user_id);
    
    bool add_participant_to_chat(
        const std::string& chat_id,
        const std::string& user_id,
        const std::string& added_by,
        ParticipantRole role = ParticipantRole::MEMBER
    );
    
    bool remove_participant_from_chat(
        const std::string& chat_id,
        const std::string& user_id,
        const std::string& removed_by
    );
    
    bool leave_chat(const std::string& chat_id, const std::string& user_id);
    
    bool update_chat_settings(
        const std::string& chat_id,
        const std::string& user_id,
        const ChatSettings& new_settings
    );
    
    // Attachment operations
    std::string upload_attachment(
        const std::string& filename,
        const std::string& content_type,
        const std::vector<uint8_t>& data,
        const std::string& uploader_id,
        bool encrypt = true
    );
    
    std::unique_ptr<AttachmentUpload> get_attachment(
        const std::string& attachment_id,
        const std::string& user_id
    );
    
    bool delete_attachment(
        const std::string& attachment_id,
        const std::string& user_id
    );
    
    std::vector<uint8_t> download_attachment(
        const std::string& attachment_id,
        const std::string& user_id
    );
    
    // Reaction operations
    bool add_reaction(
        const std::string& message_id,
        const std::string& user_id,
        const std::string& emoji
    );
    
    bool remove_reaction(
        const std::string& message_id,
        const std::string& user_id,
        const std::string& emoji
    );
    
    std::vector<MessageReaction> get_message_reactions(const std::string& message_id);
    
    // Real-time operations
    void start_typing(const std::string& user_id, const std::string& chat_id);
    void stop_typing(const std::string& user_id, const std::string& chat_id);
    std::vector<std::string> get_typing_users(const std::string& chat_id);
    
    void set_online_status(const std::string& user_id, realtime::OnlineStatus status);
    realtime::OnlineStatus get_user_status(const std::string& user_id);
    std::vector<std::string> get_online_users_in_chat(const std::string& chat_id);
    
    // Encryption operations
    bool setup_e2e_encryption(
        const std::string& user_id,
        const crypto::CryptoKey& identity_key,
        const crypto::CryptoKey& signed_prekey,
        const std::vector<crypto::CryptoKey>& one_time_prekeys
    );
    
    std::string initiate_secure_session(
        const std::string& sender_id,
        const std::string& recipient_id
    );
    
    bool verify_session_fingerprint(
        const std::string& session_id,
        const std::string& expected_fingerprint
    );
    
    // Statistics and monitoring
    MessagingStats get_messaging_stats();
    Json::Value get_detailed_metrics();
    Json::Value get_chat_analytics(const std::string& chat_id);
    Json::Value get_user_analytics(const std::string& user_id);
    
    // Configuration
    void set_max_message_size(uint64_t size);
    void set_max_attachment_size(uint64_t size);
    void set_message_retention_period(uint32_t days);
    void set_encryption_enabled(bool enabled);
    void set_rate_limits(uint32_t messages_per_minute);
    void enable_disappearing_messages(bool enabled, std::chrono::seconds default_ttl);
    
    // Health and diagnostics
    Json::Value get_health_status();
    bool perform_health_check();
    Json::Value get_system_info();
    void force_cleanup();
    void rebuild_indexes();
    
    // Utilities
    std::string generate_message_id();
    std::string generate_chat_id();
    std::string generate_attachment_id();
    bool is_valid_user_id(const std::string& user_id);
    bool can_user_access_chat(const std::string& user_id, const std::string& chat_id);
    std::vector<std::string> get_user_permissions(const std::string& user_id, const std::string& chat_id);
};

// HTTP/REST API handlers
class MessagingAPIHandler {
private:
    std::shared_ptr<MessagingController> controller_;
    
public:
    MessagingAPIHandler(std::shared_ptr<MessagingController> controller);
    
    // Message endpoints
    Json::Value handle_send_message(const Json::Value& request, const std::string& user_id);
    Json::Value handle_get_messages(const Json::Value& request, const std::string& user_id);
    Json::Value handle_edit_message(const Json::Value& request, const std::string& user_id);
    Json::Value handle_delete_message(const Json::Value& request, const std::string& user_id);
    Json::Value handle_search_messages(const Json::Value& request, const std::string& user_id);
    
    // Chat endpoints
    Json::Value handle_create_chat(const Json::Value& request, const std::string& user_id);
    Json::Value handle_get_chats(const Json::Value& request, const std::string& user_id);
    Json::Value handle_get_chat(const Json::Value& request, const std::string& user_id);
    Json::Value handle_update_chat(const Json::Value& request, const std::string& user_id);
    Json::Value handle_add_participant(const Json::Value& request, const std::string& user_id);
    Json::Value handle_remove_participant(const Json::Value& request, const std::string& user_id);
    
    // Attachment endpoints
    Json::Value handle_upload_attachment(const Json::Value& request, const std::string& user_id);
    Json::Value handle_download_attachment(const Json::Value& request, const std::string& user_id);
    Json::Value handle_delete_attachment(const Json::Value& request, const std::string& user_id);
    
    // Utility endpoints
    Json::Value handle_health_check();
    Json::Value handle_get_metrics();
    Json::Value handle_get_stats();
    
    // Error handling
    Json::Value create_error_response(const std::string& error, const std::string& details = "");
    Json::Value create_success_response(const Json::Value& data = Json::Value::null);
};

} // namespace sonet::messaging
