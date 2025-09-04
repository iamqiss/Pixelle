/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "include/chat.hpp"
#include <algorithm>
#include <random>
#include <sstream>
#include <regex>
#include <openssl/sha.h>

namespace sonet::messaging {

// ChatParticipant implementation
Json::Value ChatParticipant::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["display_name"] = display_name;
    json["role"] = static_cast<int>(role);
    json["joined_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        joined_at.time_since_epoch()).count();
    
    if (last_read_at) {
        json["last_read_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            last_read_at->time_since_epoch()).count();
    }
    
    if (last_active_at) {
        json["last_active_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            last_active_at->time_since_epoch()).count();
    }
    
    Json::Value permissions_json(Json::arrayValue);
    for (const auto& permission : permissions) {
        permissions_json.append(static_cast<int>(permission));
    }
    json["permissions"] = permissions_json;
    
    json["invitation_link"] = invitation_link;
    json["invited_by"] = invited_by;
    json["notifications_enabled"] = notifications_enabled;
    json["custom_title"] = custom_title;
    
    return json;
}

ChatParticipant ChatParticipant::from_json(const Json::Value& json) {
    ChatParticipant participant;
    participant.user_id = json["user_id"].asString();
    participant.display_name = json["display_name"].asString();
    participant.role = static_cast<ParticipantRole>(json["role"].asInt());
    
    auto joined_ms = json["joined_at"].asInt64();
    participant.joined_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(joined_ms));
    
    if (json.isMember("last_read_at") && !json["last_read_at"].isNull()) {
        auto read_ms = json["last_read_at"].asInt64();
        participant.last_read_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(read_ms));
    }
    
    if (json.isMember("last_active_at") && !json["last_active_at"].isNull()) {
        auto active_ms = json["last_active_at"].asInt64();
        participant.last_active_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(active_ms));
    }
    
    const auto& permissions_json = json["permissions"];
    for (const auto& perm : permissions_json) {
        participant.permissions.insert(static_cast<ChatPermission>(perm.asInt()));
    }
    
    participant.invitation_link = json["invitation_link"].asString();
    participant.invited_by = json["invited_by"].asString();
    participant.notifications_enabled = json["notifications_enabled"].asBool();
    participant.custom_title = json["custom_title"].asString();
    
    return participant;
}

bool ChatParticipant::has_permission(ChatPermission permission) const {
    return permissions.find(permission) != permissions.end();
}

void ChatParticipant::grant_permission(ChatPermission permission) {
    permissions.insert(permission);
}

void ChatParticipant::revoke_permission(ChatPermission permission) {
    permissions.erase(permission);
}

// ChatSettings implementation
Json::Value ChatSettings::to_json() const {
    Json::Value json;
    json["encryption_enabled"] = encryption_enabled;
    json["encryption_level"] = static_cast<int>(encryption_level);
    json["disappearing_messages"] = disappearing_messages;
    json["message_ttl"] = static_cast<Json::Int64>(message_ttl.count());
    json["read_receipts_enabled"] = read_receipts_enabled;
    json["typing_indicators_enabled"] = typing_indicators_enabled;
    json["link_previews_enabled"] = link_previews_enabled;
    json["auto_delete_media"] = auto_delete_media;
    json["media_ttl"] = static_cast<Json::Int64>(media_ttl.count());
    json["max_participants"] = max_participants;
    json["max_message_size"] = static_cast<Json::UInt64>(max_message_size);
    json["max_file_size"] = static_cast<Json::UInt64>(max_file_size);
    json["welcome_message"] = welcome_message;
    
    Json::Value pinned_json(Json::arrayValue);
    for (const auto& pinned_id : pinned_message_ids) {
        pinned_json.append(pinned_id);
    }
    json["pinned_message_ids"] = pinned_json;
    
    Json::Value custom_json;
    for (const auto& [key, value] : custom_settings) {
        custom_json[key] = value;
    }
    json["custom_settings"] = custom_json;
    
    return json;
}

ChatSettings ChatSettings::from_json(const Json::Value& json) {
    ChatSettings settings;
    settings.encryption_enabled = json["encryption_enabled"].asBool();
    settings.encryption_level = static_cast<EncryptionLevel>(json["encryption_level"].asInt());
    settings.disappearing_messages = json["disappearing_messages"].asBool();
    settings.message_ttl = std::chrono::seconds(json["message_ttl"].asInt64());
    settings.read_receipts_enabled = json["read_receipts_enabled"].asBool();
    settings.typing_indicators_enabled = json["typing_indicators_enabled"].asBool();
    settings.link_previews_enabled = json["link_previews_enabled"].asBool();
    settings.auto_delete_media = json["auto_delete_media"].asBool();
    settings.media_ttl = std::chrono::hours(json["media_ttl"].asInt64());
    settings.max_participants = json["max_participants"].asUInt();
    settings.max_message_size = json["max_message_size"].asUInt64();
    settings.max_file_size = json["max_file_size"].asUInt64();
    settings.welcome_message = json["welcome_message"].asString();
    
    const auto& pinned_json = json["pinned_message_ids"];
    for (const auto& pinned : pinned_json) {
        settings.pinned_message_ids.push_back(pinned.asString());
    }
    
    const auto& custom_json = json["custom_settings"];
    for (const auto& key : custom_json.getMemberNames()) {
        settings.custom_settings[key] = custom_json[key].asString();
    }
    
    return settings;
}

bool ChatSettings::is_valid() const {
    if (max_participants == 0 || max_participants > 100000) {
        return false;
    }
    
    if (max_message_size == 0 || max_message_size > 104857600) { // 100MB max
        return false;
    }
    
    if (max_file_size == 0 || max_file_size > 1073741824) { // 1GB max
        return false;
    }
    
    return true;
}

// ChatAnalytics implementation
Json::Value ChatAnalytics::to_json() const {
    Json::Value json;
    json["total_messages"] = static_cast<Json::UInt64>(total_messages);
    json["total_participants"] = static_cast<Json::UInt64>(total_participants);
    json["active_participants_today"] = static_cast<Json::UInt64>(active_participants_today);
    json["active_participants_week"] = static_cast<Json::UInt64>(active_participants_week);
    json["media_messages"] = static_cast<Json::UInt64>(media_messages);
    json["text_messages"] = static_cast<Json::UInt64>(text_messages);
    
    json["last_activity"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_activity.time_since_epoch()).count();
    json["peak_activity_time"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        peak_activity_time.time_since_epoch()).count();
    
    json["messages_per_day_avg"] = messages_per_day_avg;
    json["storage_used_bytes"] = static_cast<Json::UInt64>(storage_used_bytes);
    
    Json::Value participant_counts_json;
    for (const auto& [user_id, count] : participant_message_counts) {
        participant_counts_json[user_id] = count;
    }
    json["participant_message_counts"] = participant_counts_json;
    
    Json::Value type_counts_json;
    for (const auto& [type, count] : message_type_counts) {
        type_counts_json[std::to_string(static_cast<int>(type))] = count;
    }
    json["message_type_counts"] = type_counts_json;
    
    return json;
}

void ChatAnalytics::update_message_stats(const Message& message) {
    total_messages++;
    
    if (message.type == MessageType::TEXT) {
        text_messages++;
    } else {
        media_messages++;
    }
    
    message_type_counts[message.type]++;
    participant_message_counts[message.sender_id]++;
    
    storage_used_bytes += message.calculate_size();
    last_activity = std::chrono::system_clock::now();
    
    // Update average messages per day (simplified calculation)
    auto days_since_creation = std::chrono::duration_cast<std::chrono::hours>(
        last_activity - peak_activity_time).count() / 24;
    if (days_since_creation > 0) {
        messages_per_day_avg = static_cast<uint32_t>(total_messages / days_since_creation);
    }
}

void ChatAnalytics::update_participant_activity(const std::string& user_id) {
    auto now = std::chrono::system_clock::now();
    auto today_start = std::chrono::system_clock::time_point(
        std::chrono::duration_cast<std::chrono::days>(now.time_since_epoch()));
    
    // This is simplified - in production you'd track actual daily activity
    last_activity = now;
}

// Chat implementation
Chat::Chat(const std::string& name, ChatType type, const std::string& owner_id)
    : name(name), type(type), owner_id(owner_id), status(ChatStatus::ACTIVE) {
    
    id = ChatUtils::generate_chat_id();
    created_at = std::chrono::system_clock::now();
    updated_at = created_at;
    
    // Set default permissions based on chat type
    setup_default_permissions();
    
    // Add owner as first participant
    ChatParticipant owner;
    owner.user_id = owner_id;
    owner.display_name = ""; // Will be set from user service
    owner.role = ParticipantRole::OWNER;
    owner.joined_at = created_at;
    owner.permissions = get_all_permissions();
    
    participants.push_back(owner);
    analytics.total_participants = 1;
}

Chat::Chat(const std::string& participant1_id, const std::string& participant2_id)
    : type(ChatType::DIRECT_MESSAGE), status(ChatStatus::ACTIVE) {
    
    id = ChatUtils::generate_chat_id();
    name = ""; // DMs don't have names
    description = "";
    owner_id = participant1_id; // First participant is nominal owner
    created_at = std::chrono::system_clock::now();
    updated_at = created_at;
    
    // Set encryption enabled by default for DMs
    settings.encryption_enabled = true;
    settings.encryption_level = EncryptionLevel::MILITARY_GRADE;
    
    // Add both participants
    ChatParticipant p1, p2;
    p1.user_id = participant1_id;
    p1.role = ParticipantRole::MEMBER;
    p1.joined_at = created_at;
    p1.permissions = get_default_dm_permissions();
    
    p2.user_id = participant2_id;
    p2.role = ParticipantRole::MEMBER;
    p2.joined_at = created_at;
    p2.permissions = get_default_dm_permissions();
    
    participants = {p1, p2};
    analytics.total_participants = 2;
}

bool Chat::is_valid() const {
    if (id.empty() || owner_id.empty()) {
        return false;
    }
    
    if (type == ChatType::DIRECT_MESSAGE && participants.size() != 2) {
        return false;
    }
    
    if (participants.empty()) {
        return false;
    }
    
    // Check if owner is in participants
    bool owner_found = false;
    for (const auto& participant : participants) {
        if (participant.user_id == owner_id) {
            owner_found = true;
            break;
        }
    }
    
    return owner_found && settings.is_valid();
}

bool Chat::can_send_messages() const {
    return status == ChatStatus::ACTIVE && 
           type != ChatType::READONLY;
}

bool Chat::is_participant(const std::string& user_id) const {
    return find_participant(user_id) != nullptr;
}

bool Chat::is_admin(const std::string& user_id) const {
    const auto* participant = find_participant(user_id);
    return participant && (participant->role == ParticipantRole::ADMIN ||
                          participant->role == ParticipantRole::OWNER);
}

bool Chat::is_owner(const std::string& user_id) const {
    return owner_id == user_id;
}

bool Chat::add_participant(const ChatParticipant& participant) {
    if (participants.size() >= settings.max_participants) {
        return false;
    }
    
    // Check if user is already a participant
    if (is_participant(participant.user_id)) {
        return false;
    }
    
    participants.push_back(participant);
    analytics.total_participants++;
    updated_at = std::chrono::system_clock::now();
    
    return true;
}

bool Chat::remove_participant(const std::string& user_id, const std::string& removed_by) {
    auto it = std::find_if(participants.begin(), participants.end(),
        [&user_id](const ChatParticipant& p) {
            return p.user_id == user_id;
        });
    
    if (it == participants.end()) {
        return false;
    }
    
    // Can't remove the owner
    if (user_id == owner_id) {
        return false;
    }
    
    participants.erase(it);
    analytics.total_participants--;
    updated_at = std::chrono::system_clock::now();
    
    return true;
}

bool Chat::update_participant_role(const std::string& user_id, ParticipantRole new_role) {
    auto* participant = find_participant(user_id);
    if (!participant) {
        return false;
    }
    
    // Can't change owner role
    if (user_id == owner_id && new_role != ParticipantRole::OWNER) {
        return false;
    }
    
    participant->role = new_role;
    participant->permissions = get_role_permissions(new_role);
    updated_at = std::chrono::system_clock::now();
    
    return true;
}

ChatParticipant* Chat::find_participant(const std::string& user_id) {
    auto it = std::find_if(participants.begin(), participants.end(),
        [&user_id](const ChatParticipant& p) {
            return p.user_id == user_id;
        });
    
    return (it != participants.end()) ? &(*it) : nullptr;
}

const ChatParticipant* Chat::find_participant(const std::string& user_id) const {
    auto it = std::find_if(participants.begin(), participants.end(),
        [&user_id](const ChatParticipant& p) {
            return p.user_id == user_id;
        });
    
    return (it != participants.end()) ? &(*it) : nullptr;
}

bool Chat::has_permission(const std::string& user_id, ChatPermission permission) const {
    const auto* participant = find_participant(user_id);
    return participant && participant->has_permission(permission);
}

bool Chat::can_send_message(const std::string& user_id, MessageType type) const {
    if (!can_send_messages() || !is_participant(user_id)) {
        return false;
    }
    
    if (!has_permission(user_id, ChatPermission::SEND_MESSAGES)) {
        return false;
    }
    
    switch (type) {
        case MessageType::IMAGE:
        case MessageType::VIDEO:
        case MessageType::AUDIO:
        case MessageType::FILE:
            return has_permission(user_id, ChatPermission::SEND_MEDIA);
        case MessageType::STICKER:
            return has_permission(user_id, ChatPermission::SEND_STICKERS);
        default:
            return true;
    }
}

void Chat::update_last_message_time() {
    last_message_at = std::chrono::system_clock::now();
    updated_at = *last_message_at;
}

void Chat::update_participant_last_read(const std::string& user_id) {
    auto* participant = find_participant(user_id);
    if (participant) {
        participant->last_read_at = std::chrono::system_clock::now();
    }
}

void Chat::update_participant_activity(const std::string& user_id) {
    auto* participant = find_participant(user_id);
    if (participant) {
        participant->last_active_at = std::chrono::system_clock::now();
    }
    
    analytics.update_participant_activity(user_id);
}

void Chat::update_analytics(const Message& message) {
    analytics.update_message_stats(message);
    update_last_message_time();
}

Json::Value Chat::to_json() const {
    Json::Value json;
    
    json["id"] = id;
    json["name"] = name;
    json["description"] = description;
    json["type"] = static_cast<int>(type);
    json["status"] = static_cast<int>(status);
    json["owner_id"] = owner_id;
    
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["updated_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        updated_at.time_since_epoch()).count();
    
    if (last_message_at) {
        json["last_message_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            last_message_at->time_since_epoch()).count();
    }
    
    // Serialize participants
    Json::Value participants_json(Json::arrayValue);
    for (const auto& participant : participants) {
        participants_json.append(participant.to_json());
    }
    json["participants"] = participants_json;
    
    // Serialize settings
    json["settings"] = settings.to_json();
    
    json["avatar_url"] = avatar_url;
    json["invitation_link"] = invitation_link;
    
    if (parent_chat_id) {
        json["parent_chat_id"] = *parent_chat_id;
    }
    
    Json::Value child_chats_json(Json::arrayValue);
    for (const auto& child_id : child_chat_ids) {
        child_chats_json.append(child_id);
    }
    json["child_chat_ids"] = child_chats_json;
    
    // Serialize analytics
    json["analytics"] = analytics.to_json();
    
    return json;
}

// Helper methods
void Chat::setup_default_permissions() {
    // Set up role-based permissions
    role_permissions[ParticipantRole::OWNER] = get_all_permissions();
    role_permissions[ParticipantRole::ADMIN] = get_admin_permissions();
    role_permissions[ParticipantRole::MODERATOR] = get_moderator_permissions();
    role_permissions[ParticipantRole::MEMBER] = get_member_permissions();
    role_permissions[ParticipantRole::RESTRICTED] = get_restricted_permissions();
}

std::unordered_set<ChatPermission> Chat::get_all_permissions() const {
    return {
        ChatPermission::SEND_MESSAGES,
        ChatPermission::SEND_MEDIA,
        ChatPermission::SEND_STICKERS,
        ChatPermission::SEND_POLLS,
        ChatPermission::EMBED_LINKS,
        ChatPermission::ADD_PARTICIPANTS,
        ChatPermission::REMOVE_PARTICIPANTS,
        ChatPermission::CHANGE_INFO,
        ChatPermission::PIN_MESSAGES,
        ChatPermission::DELETE_MESSAGES,
        ChatPermission::MANAGE_VIDEO_CALLS,
        ChatPermission::READ_MESSAGE_HISTORY
    };
}

std::unordered_set<ChatPermission> Chat::get_admin_permissions() const {
    return {
        ChatPermission::SEND_MESSAGES,
        ChatPermission::SEND_MEDIA,
        ChatPermission::SEND_STICKERS,
        ChatPermission::SEND_POLLS,
        ChatPermission::EMBED_LINKS,
        ChatPermission::ADD_PARTICIPANTS,
        ChatPermission::REMOVE_PARTICIPANTS,
        ChatPermission::CHANGE_INFO,
        ChatPermission::PIN_MESSAGES,
        ChatPermission::DELETE_MESSAGES,
        ChatPermission::MANAGE_VIDEO_CALLS,
        ChatPermission::READ_MESSAGE_HISTORY
    };
}

std::unordered_set<ChatPermission> Chat::get_moderator_permissions() const {
    return {
        ChatPermission::SEND_MESSAGES,
        ChatPermission::SEND_MEDIA,
        ChatPermission::SEND_STICKERS,
        ChatPermission::SEND_POLLS,
        ChatPermission::EMBED_LINKS,
        ChatPermission::PIN_MESSAGES,
        ChatPermission::DELETE_MESSAGES,
        ChatPermission::READ_MESSAGE_HISTORY
    };
}

std::unordered_set<ChatPermission> Chat::get_member_permissions() const {
    return {
        ChatPermission::SEND_MESSAGES,
        ChatPermission::SEND_MEDIA,
        ChatPermission::SEND_STICKERS,
        ChatPermission::EMBED_LINKS,
        ChatPermission::READ_MESSAGE_HISTORY
    };
}

std::unordered_set<ChatPermission> Chat::get_restricted_permissions() const {
    return {
        ChatPermission::READ_MESSAGE_HISTORY
    };
}

std::unordered_set<ChatPermission> Chat::get_default_dm_permissions() const {
    return {
        ChatPermission::SEND_MESSAGES,
        ChatPermission::SEND_MEDIA,
        ChatPermission::SEND_STICKERS,
        ChatPermission::EMBED_LINKS,
        ChatPermission::READ_MESSAGE_HISTORY
    };
}

std::unordered_set<ChatPermission> Chat::get_role_permissions(ParticipantRole role) const {
    auto it = role_permissions.find(role);
    if (it != role_permissions.end()) {
        return it->second;
    }
    return get_member_permissions(); // Default fallback
}

// ChatUtils implementation
std::string ChatUtils::generate_chat_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    
    uint64_t high = dis(gen);
    uint64_t low = dis(gen);
    
    std::stringstream ss;
    ss << "chat_" << std::hex << high << low;
    return ss.str();
}

std::string ChatUtils::generate_invitation_link(const std::string& chat_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 61);
    
    const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    std::string token;
    token.reserve(32);
    
    for (int i = 0; i < 32; ++i) {
        token += chars[dis(gen)];
    }
    
    return "https://sonet.app/invite/" + token;
}

bool ChatUtils::is_valid_chat_name(const std::string& name) {
    if (name.empty() || name.length() > 100) {
        return false;
    }
    
    // Check for invalid characters
    std::regex invalid_chars(R"([<>\"'&])");
    return !std::regex_search(name, invalid_chars);
}

bool ChatUtils::is_valid_chat_description(const std::string& description) {
    if (description.length() > 1000) {
        return false;
    }
    
    // Check for invalid characters
    std::regex invalid_chars(R"([<>\"'&])");
    return !std::regex_search(description, invalid_chars);
}

ChatType ChatUtils::detect_chat_type(const std::vector<std::string>& participant_ids) {
    if (participant_ids.size() == 2) {
        return ChatType::DIRECT_MESSAGE;
    } else if (participant_ids.size() <= 10) {
        return ChatType::GROUP_CHAT;
    } else {
        return ChatType::CHANNEL;
    }
}

std::string ChatUtils::sanitize_chat_name(const std::string& name) {
    std::string sanitized = name;
    
    // Remove dangerous characters
    std::regex dangerous_chars(R"([<>\"'&])");
    sanitized = std::regex_replace(sanitized, dangerous_chars, "");
    
    // Trim whitespace
    sanitized.erase(0, sanitized.find_first_not_of(" \t\n\r"));
    sanitized.erase(sanitized.find_last_not_of(" \t\n\r") + 1);
    
    // Limit length
    if (sanitized.length() > 100) {
        sanitized = sanitized.substr(0, 100);
    }
    
    return sanitized;
}

} // namespace sonet::messaging
