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
#include <unordered_set>
#include <optional>
#include <json/json.h>
#include "message.hpp"

namespace sonet::messaging {

enum class ChatType {
    DIRECT_MESSAGE,
    GROUP_CHAT,
    CHANNEL,
    BROADCAST,
    SECRET_CHAT,
    TEMPORARY_CHAT
};

enum class ParticipantRole {
    OWNER,
    ADMIN,
    MODERATOR,
    MEMBER,
    RESTRICTED,
    BANNED
};

enum class ChatStatus {
    ACTIVE,
    ARCHIVED,
    DELETED,
    SUSPENDED,
    READONLY
};

enum class ChatPermission {
    SEND_MESSAGES,
    SEND_MEDIA,
    SEND_STICKERS,
    SEND_POLLS,
    EMBED_LINKS,
    ADD_PARTICIPANTS,
    REMOVE_PARTICIPANTS,
    CHANGE_INFO,
    PIN_MESSAGES,
    DELETE_MESSAGES,
    MANAGE_VIDEO_CALLS,
    READ_MESSAGE_HISTORY
};

struct ChatParticipant {
    std::string user_id;
    std::string display_name;
    ParticipantRole role;
    std::chrono::system_clock::time_point joined_at;
    std::optional<std::chrono::system_clock::time_point> last_read_at;
    std::optional<std::chrono::system_clock::time_point> last_active_at;
    std::unordered_set<ChatPermission> permissions;
    std::string invitation_link;
    std::string invited_by;
    bool notifications_enabled = true;
    std::string custom_title;
    
    Json::Value to_json() const;
    static ChatParticipant from_json(const Json::Value& json);
    bool has_permission(ChatPermission permission) const;
    void grant_permission(ChatPermission permission);
    void revoke_permission(ChatPermission permission);
};

struct ChatSettings {
    bool encryption_enabled = true;
    EncryptionLevel encryption_level = EncryptionLevel::MILITARY_GRADE;
    bool disappearing_messages = false;
    std::chrono::seconds message_ttl = std::chrono::seconds(0);
    bool read_receipts_enabled = true;
    bool typing_indicators_enabled = true;
    bool link_previews_enabled = true;
    bool auto_delete_media = false;
    std::chrono::hours media_ttl = std::chrono::hours(24 * 30);
    uint32_t max_participants = 1000;
    uint64_t max_message_size = 10485760; // 10MB
    uint64_t max_file_size = 104857600;   // 100MB
    std::string welcome_message;
    std::vector<std::string> pinned_message_ids;
    std::unordered_map<std::string, std::string> custom_settings;
    
    Json::Value to_json() const;
    static ChatSettings from_json(const Json::Value& json);
    bool is_valid() const;
};

struct ChatAnalytics {
    uint64_t total_messages = 0;
    uint64_t total_participants = 0;
    uint64_t active_participants_today = 0;
    uint64_t active_participants_week = 0;
    uint64_t media_messages = 0;
    uint64_t text_messages = 0;
    std::chrono::system_clock::time_point last_activity;
    std::chrono::system_clock::time_point peak_activity_time;
    uint32_t messages_per_day_avg = 0;
    uint64_t storage_used_bytes = 0;
    std::unordered_map<std::string, uint32_t> participant_message_counts;
    std::unordered_map<MessageType, uint32_t> message_type_counts;
    
    Json::Value to_json() const;
    static ChatAnalytics from_json(const Json::Value& json);
    void update_message_stats(const Message& message);
    void update_participant_activity(const std::string& user_id);
};

class Chat {
public:
    // Core chat data
    std::string id;
    std::string name;
    std::string description;
    ChatType type;
    ChatStatus status;
    std::string owner_id;
    
    // Timestamps
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::optional<std::chrono::system_clock::time_point> last_message_at;
    std::optional<std::chrono::system_clock::time_point> archived_at;
    
    // Participants and permissions
    std::vector<ChatParticipant> participants;
    std::unordered_map<ParticipantRole, std::unordered_set<ChatPermission>> role_permissions;
    
    // Chat configuration
    ChatSettings settings;
    std::string avatar_url;
    std::string invitation_link;
    std::optional<std::string> parent_chat_id;
    std::vector<std::string> child_chat_ids;
    
    // Analytics and metrics
    ChatAnalytics analytics;
    
    // Validation
    bool is_valid() const;
    bool can_send_messages() const;
    bool is_participant(const std::string& user_id) const;
    bool is_admin(const std::string& user_id) const;
    bool is_owner(const std::string& user_id) const;
    
    // Participant management
    bool add_participant(const ChatParticipant& participant);
    bool remove_participant(const std::string& user_id, const std::string& removed_by);
    bool update_participant_role(const std::string& user_id, ParticipantRole new_role);
    bool update_participant_permissions(const std::string& user_id, 
                                       const std::unordered_set<ChatPermission>& permissions);
    ChatParticipant* find_participant(const std::string& user_id);
    const ChatParticipant* find_participant(const std::string& user_id) const;
    std::vector<ChatParticipant> get_participants_with_role(ParticipantRole role) const;
    std::vector<ChatParticipant> get_active_participants(std::chrono::minutes within = std::chrono::minutes(15)) const;
    
    // Permission checking
    bool has_permission(const std::string& user_id, ChatPermission permission) const;
    bool can_manage_chat(const std::string& user_id) const;
    bool can_delete_messages(const std::string& user_id) const;
    bool can_add_participants(const std::string& user_id) const;
    
    // Message operations
    bool can_send_message(const std::string& user_id, MessageType type) const;
    void update_last_message_time();
    void update_participant_last_read(const std::string& user_id);
    void update_participant_activity(const std::string& user_id);
    
    // Chat settings
    void update_settings(const ChatSettings& new_settings);
    void enable_disappearing_messages(std::chrono::seconds ttl);
    void disable_disappearing_messages();
    void set_encryption_level(EncryptionLevel level);
    void pin_message(const std::string& message_id);
    void unpin_message(const std::string& message_id);
    
    // Analytics
    void update_analytics(const Message& message);
    uint32_t get_unread_count(const std::string& user_id) const;
    std::vector<std::string> get_active_user_ids(std::chrono::minutes within = std::chrono::minutes(15)) const;
    
    // Moderation
    bool mute_participant(const std::string& user_id, std::chrono::minutes duration);
    bool unmute_participant(const std::string& user_id);
    bool ban_participant(const std::string& user_id, const std::string& reason = "");
    bool unban_participant(const std::string& user_id);
    bool is_participant_muted(const std::string& user_id) const;
    bool is_participant_banned(const std::string& user_id) const;
    
    // Archive and cleanup
    void archive();
    void unarchive();
    void delete_chat();
    void cleanup_old_messages(std::chrono::hours older_than);
    void cleanup_old_media(std::chrono::hours older_than);
    
    // Serialization
    Json::Value to_json() const;
    static std::unique_ptr<Chat> from_json(const Json::Value& json);
    
    // Database operations
    std::string to_sql_insert() const;
    std::string to_sql_update() const;
    static std::unique_ptr<Chat> from_sql_row(const std::vector<std::string>& row);
    
    // Utilities
    std::string generate_invitation_link();
    bool validate_invitation_link(const std::string& link) const;
    size_t calculate_storage_usage() const;
    
    // Constructors
    Chat() = default;
    Chat(const std::string& name, ChatType type, const std::string& owner_id);
    Chat(const std::string& participant1_id, const std::string& participant2_id); // DM constructor
    
    // Comparison operators
    bool operator==(const Chat& other) const;
    bool operator<(const Chat& other) const;
};

class ChatManager {
private:
    std::unordered_map<std::string, std::unique_ptr<Chat>> chats_;
    std::unordered_map<std::string, std::unordered_set<std::string>> user_chats_;
    std::mutex chats_mutex_;
    
    // Configuration
    uint32_t max_chats_per_user_;
    uint32_t max_participants_per_chat_;
    std::chrono::hours chat_inactivity_threshold_;
    
public:
    ChatManager();
    ~ChatManager();
    
    // Chat creation
    std::unique_ptr<Chat> create_direct_message(
        const std::string& user1_id,
        const std::string& user2_id
    );
    
    std::unique_ptr<Chat> create_group_chat(
        const std::string& name,
        const std::string& owner_id,
        const std::vector<std::string>& participant_ids = {},
        const ChatSettings& settings = ChatSettings()
    );
    
    std::unique_ptr<Chat> create_channel(
        const std::string& name,
        const std::string& owner_id,
        const std::string& description = "",
        const ChatSettings& settings = ChatSettings()
    );
    
    std::unique_ptr<Chat> create_secret_chat(
        const std::string& user1_id,
        const std::string& user2_id,
        std::chrono::seconds message_ttl = std::chrono::seconds(3600)
    );
    
    // Chat retrieval
    std::shared_ptr<Chat> get_chat(const std::string& chat_id);
    std::vector<std::shared_ptr<Chat>> get_user_chats(const std::string& user_id);
    std::vector<std::shared_ptr<Chat>> get_user_direct_messages(const std::string& user_id);
    std::vector<std::shared_ptr<Chat>> get_user_group_chats(const std::string& user_id);
    std::shared_ptr<Chat> find_direct_message(const std::string& user1_id, const std::string& user2_id);
    
    // Chat management
    bool update_chat(const std::string& chat_id, const Chat& updated_chat);
    bool delete_chat(const std::string& chat_id, const std::string& deleted_by);
    bool archive_chat(const std::string& chat_id, const std::string& archived_by);
    bool unarchive_chat(const std::string& chat_id, const std::string& unarchived_by);
    
    // Participant management
    bool add_participant_to_chat(const std::string& chat_id, const ChatParticipant& participant);
    bool remove_participant_from_chat(const std::string& chat_id, const std::string& user_id, 
                                     const std::string& removed_by);
    bool update_participant_role(const std::string& chat_id, const std::string& user_id, 
                                ParticipantRole new_role);
    bool leave_chat(const std::string& chat_id, const std::string& user_id);
    
    // Search and filtering
    std::vector<std::shared_ptr<Chat>> search_chats(
        const std::string& query,
        const std::string& user_id,
        ChatType type_filter = ChatType::DIRECT_MESSAGE
    );
    
    std::vector<std::shared_ptr<Chat>> get_chats_by_type(
        const std::string& user_id,
        ChatType type
    );
    
    std::vector<std::shared_ptr<Chat>> get_active_chats(
        const std::string& user_id,
        std::chrono::hours within = std::chrono::hours(24)
    );
    
    // Analytics and monitoring
    Json::Value get_chat_analytics(const std::string& chat_id);
    Json::Value get_user_chat_analytics(const std::string& user_id);
    uint32_t get_total_chats();
    uint32_t get_active_chats_count(std::chrono::hours within = std::chrono::hours(24));
    
    // Maintenance
    void cleanup_inactive_chats(std::chrono::hours inactive_for);
    void cleanup_deleted_chats();
    void optimize_memory_usage();
    void rebuild_user_chat_index();
    
    // Configuration
    void set_max_chats_per_user(uint32_t max_chats);
    void set_max_participants_per_chat(uint32_t max_participants);
    void set_inactivity_threshold(std::chrono::hours threshold);
    
    // Utilities
    std::string generate_chat_id();
    bool is_valid_chat_id(const std::string& chat_id);
    std::vector<std::string> get_chat_participant_ids(const std::string& chat_id);
    
    // Cache management
    void cache_chat(std::unique_ptr<Chat> chat);
    void remove_from_cache(const std::string& chat_id);
    void clear_cache();
    size_t get_cache_size();
};

// Chat utilities
class ChatUtils {
public:
    static std::string generate_chat_id();
    static std::string generate_invitation_link(const std::string& chat_id);
    static bool is_valid_chat_name(const std::string& name);
    static bool is_valid_chat_description(const std::string& description);
    static ChatType detect_chat_type(const std::vector<std::string>& participant_ids);
    static std::vector<ChatPermission> get_default_permissions(ParticipantRole role);
    static std::string format_chat_preview(const Chat& chat);
    static size_t calculate_chat_storage_usage(const Chat& chat);
    static bool should_archive_chat(const Chat& chat, std::chrono::hours inactivity_threshold);
    static double calculate_chat_activity_score(const Chat& chat);
    static std::string sanitize_chat_name(const std::string& name);
    static std::string sanitize_chat_description(const std::string& description);
};

} // namespace sonet::messaging
