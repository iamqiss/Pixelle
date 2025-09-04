/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Implementation of the notification model. I spent a lot of time thinking about
 * how to make notifications feel natural and not spammy. Every method here serves
 * the goal of keeping users engaged without overwhelming them.
 */

#include "notification.h"
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <random>
#include <regex>

namespace sonet {
namespace notification_service {
namespace models {

// Notification Implementation

Notification::Notification() {
    initialize_defaults();
}

Notification::Notification(const std::string& user_id, const std::string& sender_id, 
                          NotificationType type, const std::string& title, 
                          const std::string& message) 
    : user_id(user_id), sender_id(sender_id), type(type), title(title), message(message) {
    initialize_defaults();
    tracking_id = generate_tracking_id();
}

void Notification::initialize_defaults() {
    delivery_channels = static_cast<int>(DeliveryChannel::IN_APP);
    priority = NotificationPriority::NORMAL;
    created_at = std::chrono::system_clock::now();
    scheduled_at = created_at;
    expires_at = created_at + std::chrono::hours(24 * 30); // 30 days default
    status = DeliveryStatus::PENDING;
    delivery_attempts = 0;
    is_batched = false;
    respect_quiet_hours = true;
    allow_bundling = true;
    metadata = nlohmann::json::object();
    template_data = nlohmann::json::object();
    analytics_data = nlohmann::json::object();
}

std::string Notification::generate_tracking_id() const {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(100000, 999999);
    
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    return "notif_" + std::to_string(timestamp) + "_" + std::to_string(dis(gen));
}

void Notification::add_delivery_channel(DeliveryChannel channel) {
    delivery_channels |= static_cast<int>(channel);
}

void Notification::remove_delivery_channel(DeliveryChannel channel) {
    delivery_channels &= ~static_cast<int>(channel);
}

bool Notification::has_delivery_channel(DeliveryChannel channel) const {
    return (delivery_channels & static_cast<int>(channel)) != 0;
}

std::vector<DeliveryChannel> Notification::get_delivery_channels() const {
    std::vector<DeliveryChannel> channels;
    
    if (has_delivery_channel(DeliveryChannel::IN_APP)) {
        channels.push_back(DeliveryChannel::IN_APP);
    }
    if (has_delivery_channel(DeliveryChannel::PUSH_NOTIFICATION)) {
        channels.push_back(DeliveryChannel::PUSH_NOTIFICATION);
    }
    if (has_delivery_channel(DeliveryChannel::EMAIL)) {
        channels.push_back(DeliveryChannel::EMAIL);
    }
    if (has_delivery_channel(DeliveryChannel::SMS)) {
        channels.push_back(DeliveryChannel::SMS);
    }
    if (has_delivery_channel(DeliveryChannel::WEBHOOK)) {
        channels.push_back(DeliveryChannel::WEBHOOK);
    }
    
    return channels;
}

void Notification::mark_as_sent() {
    status = DeliveryStatus::SENT;
    update_delivery_attempt();
}

void Notification::mark_as_delivered() {
    status = DeliveryStatus::DELIVERED;
    delivered_at = std::chrono::system_clock::now();
}

void Notification::mark_as_read() {
    status = DeliveryStatus::READ;
    read_at = std::chrono::system_clock::now();
    record_view();
}

void Notification::mark_as_failed(const std::string& reason) {
    status = DeliveryStatus::FAILED;
    failure_reason = reason;
    update_delivery_attempt();
}

void Notification::update_delivery_attempt() {
    delivery_attempts++;
}

bool Notification::is_expired() const {
    return std::chrono::system_clock::now() > expires_at;
}

bool Notification::is_readable() const {
    return status == DeliveryStatus::DELIVERED || status == DeliveryStatus::READ;
}

void Notification::set_group_key(const std::string& key) {
    group_key = key;
}

bool Notification::can_be_grouped_with(const Notification& other) const {
    if (!allow_bundling || !other.allow_bundling) {
        return false;
    }
    
    return user_id == other.user_id && 
           type == other.type && 
           sender_id == other.sender_id &&
           !group_key.empty() && 
           group_key == other.group_key;
}

void Notification::set_template(const std::string& template_id, const nlohmann::json& data) {
    this->template_id = template_id;
    this->template_data = data;
}

std::string Notification::render_message() const {
    if (!template_id.empty()) {
        return process_template_variables(message);
    }
    return message;
}

std::string Notification::render_title() const {
    if (!template_id.empty()) {
        return process_template_variables(title);
    }
    return title;
}

std::string Notification::process_template_variables(const std::string& template_str) const {
    std::string result = template_str;
    auto context = get_template_context();
    
    // Simple template variable replacement: {{variable_name}}
    std::regex var_regex(R"(\{\{(\w+)\}\})");
    std::smatch match;
    
    while (std::regex_search(result, match, var_regex)) {
        std::string var_name = match[1].str();
        std::string replacement = "";
        
        if (context.contains(var_name)) {
            if (context[var_name].is_string()) {
                replacement = context[var_name].get<std::string>();
            } else {
                replacement = context[var_name].dump();
            }
        }
        
        result.replace(match.position(), match.length(), replacement);
    }
    
    return result;
}

nlohmann::json Notification::get_template_context() const {
    nlohmann::json context = template_data;
    
    // Add standard context variables
    context["user_id"] = user_id;
    context["sender_id"] = sender_id;
    context["type"] = notification_type_to_string(type);
    context["note_id"] = note_id;
    context["comment_id"] = comment_id;
    context["tracking_id"] = tracking_id;
    
    return context;
}

nlohmann::json Notification::to_json() const {
    nlohmann::json json;
    
    json["id"] = id;
    json["user_id"] = user_id;
    json["sender_id"] = sender_id;
    json["type"] = static_cast<int>(type);
    json["title"] = title;
    json["message"] = message;
    json["action_url"] = action_url;
    json["note_id"] = note_id;
    json["comment_id"] = comment_id;
    json["conversation_id"] = conversation_id;
    json["delivery_channels"] = delivery_channels;
    json["priority"] = static_cast<int>(priority);
    json["status"] = static_cast<int>(status);
    json["delivery_attempts"] = delivery_attempts;
    json["group_key"] = group_key;
    json["batch_id"] = batch_id;
    json["is_batched"] = is_batched;
    json["template_id"] = template_id;
    json["tracking_id"] = tracking_id;
    json["respect_quiet_hours"] = respect_quiet_hours;
    json["allow_bundling"] = allow_bundling;
    json["failure_reason"] = failure_reason;
    json["metadata"] = metadata;
    json["template_data"] = template_data;
    json["analytics_data"] = analytics_data;
    
    // Timestamps
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["scheduled_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        scheduled_at.time_since_epoch()).count();
    json["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        expires_at.time_since_epoch()).count();
    
    if (status == DeliveryStatus::DELIVERED || status == DeliveryStatus::READ) {
        json["delivered_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            delivered_at.time_since_epoch()).count();
    }
    
    if (status == DeliveryStatus::READ) {
        json["read_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            read_at.time_since_epoch()).count();
    }
    
    return json;
}

void Notification::from_json(const nlohmann::json& json) {
    if (json.contains("id")) id = json["id"];
    if (json.contains("user_id")) user_id = json["user_id"];
    if (json.contains("sender_id")) sender_id = json["sender_id"];
    if (json.contains("type")) type = static_cast<NotificationType>(json["type"]);
    if (json.contains("title")) title = json["title"];
    if (json.contains("message")) message = json["message"];
    if (json.contains("action_url")) action_url = json["action_url"];
    if (json.contains("note_id")) note_id = json["note_id"];
    if (json.contains("comment_id")) comment_id = json["comment_id"];
    if (json.contains("conversation_id")) conversation_id = json["conversation_id"];
    if (json.contains("delivery_channels")) delivery_channels = json["delivery_channels"];
    if (json.contains("priority")) priority = static_cast<NotificationPriority>(json["priority"]);
    if (json.contains("status")) status = static_cast<DeliveryStatus>(json["status"]);
    if (json.contains("delivery_attempts")) delivery_attempts = json["delivery_attempts"];
    if (json.contains("group_key")) group_key = json["group_key"];
    if (json.contains("batch_id")) batch_id = json["batch_id"];
    if (json.contains("is_batched")) is_batched = json["is_batched"];
    if (json.contains("template_id")) template_id = json["template_id"];
    if (json.contains("tracking_id")) tracking_id = json["tracking_id"];
    if (json.contains("respect_quiet_hours")) respect_quiet_hours = json["respect_quiet_hours"];
    if (json.contains("allow_bundling")) allow_bundling = json["allow_bundling"];
    if (json.contains("failure_reason")) failure_reason = json["failure_reason"];
    if (json.contains("metadata")) metadata = json["metadata"];
    if (json.contains("template_data")) template_data = json["template_data"];
    if (json.contains("analytics_data")) analytics_data = json["analytics_data"];
    
    // Timestamps
    if (json.contains("created_at")) {
        created_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(json["created_at"]));
    }
    if (json.contains("scheduled_at")) {
        scheduled_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(json["scheduled_at"]));
    }
    if (json.contains("expires_at")) {
        expires_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(json["expires_at"]));
    }
    if (json.contains("delivered_at")) {
        delivered_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(json["delivered_at"]));
    }
    if (json.contains("read_at")) {
        read_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(json["read_at"]));
    }
}

bool Notification::is_valid() const {
    return !user_id.empty() && 
           !title.empty() && 
           !message.empty() &&
           delivery_channels > 0;
}

std::vector<std::string> Notification::validate() const {
    std::vector<std::string> errors;
    
    if (user_id.empty()) {
        errors.push_back("user_id is required");
    }
    if (title.empty()) {
        errors.push_back("title is required");
    }
    if (message.empty()) {
        errors.push_back("message is required");
    }
    if (delivery_channels == 0) {
        errors.push_back("at least one delivery channel must be specified");
    }
    if (is_expired()) {
        errors.push_back("notification has expired");
    }
    
    return errors;
}

std::string Notification::get_display_text() const {
    return render_title() + ": " + render_message();
}

std::string Notification::get_summary() const {
    std::stringstream ss;
    ss << notification_type_to_string(type) << " notification for " << user_id;
    if (!sender_id.empty()) {
        ss << " from " << sender_id;
    }
    return ss.str();
}

std::chrono::milliseconds Notification::get_age() const {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - created_at);
}

bool Notification::should_send_now() const {
    auto now = std::chrono::system_clock::now();
    return now >= scheduled_at && !is_expired() && status == DeliveryStatus::PENDING;
}

void Notification::record_click(const std::string& element_id) {
    if (!analytics_data.contains("clicks")) {
        analytics_data["clicks"] = nlohmann::json::array();
    }
    
    nlohmann::json click_event;
    click_event["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    click_event["element_id"] = element_id;
    
    analytics_data["clicks"].push_back(click_event);
}

void Notification::record_view() {
    analytics_data["viewed_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    if (!analytics_data.contains("view_count")) {
        analytics_data["view_count"] = 0;
    }
    analytics_data["view_count"] = analytics_data["view_count"].get<int>() + 1;
}

void Notification::record_dismiss() {
    analytics_data["dismissed_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

nlohmann::json Notification::get_analytics_summary() const {
    nlohmann::json summary;
    summary["tracking_id"] = tracking_id;
    summary["type"] = notification_type_to_string(type);
    summary["status"] = status_to_string(status);
    summary["delivery_attempts"] = delivery_attempts;
    summary["age_ms"] = get_age().count();
    
    if (analytics_data.contains("view_count")) {
        summary["view_count"] = analytics_data["view_count"];
    }
    if (analytics_data.contains("clicks")) {
        summary["click_count"] = analytics_data["clicks"].size();
    }
    
    return summary;
}

bool Notification::operator==(const Notification& other) const {
    return id == other.id;
}

bool Notification::operator!=(const Notification& other) const {
    return !(*this == other);
}

bool Notification::operator<(const Notification& other) const {
    // Sort by priority first, then by creation time
    if (priority != other.priority) {
        return static_cast<int>(priority) > static_cast<int>(other.priority);
    }
    return created_at < other.created_at;
}

std::size_t Notification::Hash::operator()(const Notification& notification) const {
    return std::hash<std::string>{}(notification.id);
}

// NotificationBatch Implementation

NotificationBatch::NotificationBatch() {
    created_at = std::chrono::system_clock::now();
    scheduled_at = created_at;
    status = DeliveryStatus::PENDING;
    total_count = 0;
    delivered_count = 0;
    failed_count = 0;
}

NotificationBatch::NotificationBatch(const std::string& user_id) 
    : target_user_id(user_id) {
    created_at = std::chrono::system_clock::now();
    scheduled_at = created_at;
    status = DeliveryStatus::PENDING;
    total_count = 0;
    delivered_count = 0;
    failed_count = 0;
    
    // Generate batch ID
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(100000, 999999);
    batch_id = "batch_" + std::to_string(dis(gen));
}

void NotificationBatch::add_notification(std::shared_ptr<Notification> notification) {
    if (can_add_notification(*notification)) {
        notification->batch_id = batch_id;
        notification->is_batched = true;
        notifications.push_back(notification);
        total_count++;
        
        if (notifications.size() == 1) {
            common_type = notification->type;
        }
    }
}

void NotificationBatch::remove_notification(const std::string& notification_id) {
    auto it = std::remove_if(notifications.begin(), notifications.end(),
        [&notification_id](const std::shared_ptr<Notification>& notif) {
            return notif->id == notification_id;
        });
    
    if (it != notifications.end()) {
        notifications.erase(it, notifications.end());
        total_count = notifications.size();
    }
}

bool NotificationBatch::can_add_notification(const Notification& notification) const {
    if (target_user_id.empty()) {
        return true;
    }
    
    return notification.user_id == target_user_id && 
           notification.allow_bundling &&
           (notifications.empty() || notification.type == common_type);
}

void NotificationBatch::mark_as_processed() {
    status = DeliveryStatus::SENT;
}

void NotificationBatch::mark_notification_delivered(const std::string& notification_id) {
    auto it = std::find_if(notifications.begin(), notifications.end(),
        [&notification_id](const std::shared_ptr<Notification>& notif) {
            return notif->id == notification_id;
        });
    
    if (it != notifications.end()) {
        (*it)->mark_as_delivered();
        delivered_count++;
        
        if (is_complete()) {
            status = DeliveryStatus::DELIVERED;
        }
    }
}

void NotificationBatch::mark_notification_failed(const std::string& notification_id, 
                                                 const std::string& reason) {
    auto it = std::find_if(notifications.begin(), notifications.end(),
        [&notification_id](const std::shared_ptr<Notification>& notif) {
            return notif->id == notification_id;
        });
    
    if (it != notifications.end()) {
        (*it)->mark_as_failed(reason);
        failed_count++;
    }
}

std::string NotificationBatch::get_summary_message() const {
    if (notifications.empty()) {
        return "Empty notification batch";
    }
    
    std::stringstream ss;
    ss << "You have " << notifications.size() << " new ";
    ss << notification_type_to_string(common_type) << " notifications";
    
    return ss.str();
}

nlohmann::json NotificationBatch::get_batch_analytics() const {
    nlohmann::json analytics;
    analytics["batch_id"] = batch_id;
    analytics["total_count"] = total_count;
    analytics["delivered_count"] = delivered_count;
    analytics["failed_count"] = failed_count;
    analytics["success_rate"] = total_count > 0 ? 
        static_cast<double>(delivered_count) / total_count : 0.0;
    analytics["common_type"] = notification_type_to_string(common_type);
    analytics["status"] = status_to_string(status);
    
    return analytics;
}

bool NotificationBatch::is_complete() const {
    return delivered_count + failed_count >= total_count;
}

nlohmann::json NotificationBatch::to_json() const {
    nlohmann::json json;
    json["batch_id"] = batch_id;
    json["target_user_id"] = target_user_id;
    json["common_type"] = static_cast<int>(common_type);
    json["status"] = static_cast<int>(status);
    json["total_count"] = total_count;
    json["delivered_count"] = delivered_count;
    json["failed_count"] = failed_count;
    
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["scheduled_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        scheduled_at.time_since_epoch()).count();
    
    json["notifications"] = nlohmann::json::array();
    for (const auto& notif : notifications) {
        json["notifications"].push_back(notif->to_json());
    }
    
    return json;
}

void NotificationBatch::from_json(const nlohmann::json& json) {
    if (json.contains("batch_id")) batch_id = json["batch_id"];
    if (json.contains("target_user_id")) target_user_id = json["target_user_id"];
    if (json.contains("common_type")) common_type = static_cast<NotificationType>(json["common_type"]);
    if (json.contains("status")) status = static_cast<DeliveryStatus>(json["status"]);
    if (json.contains("total_count")) total_count = json["total_count"];
    if (json.contains("delivered_count")) delivered_count = json["delivered_count"];
    if (json.contains("failed_count")) failed_count = json["failed_count"];
    
    if (json.contains("created_at")) {
        created_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(json["created_at"]));
    }
    if (json.contains("scheduled_at")) {
        scheduled_at = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(json["scheduled_at"]));
    }
    
    if (json.contains("notifications")) {
        notifications.clear();
        for (const auto& notif_json : json["notifications"]) {
            auto notification = std::make_shared<Notification>();
            notification->from_json(notif_json);
            notifications.push_back(notification);
        }
    }
}

// NotificationPreferences Implementation

NotificationPreferences::NotificationPreferences() {
    apply_defaults();
}

NotificationPreferences::NotificationPreferences(const std::string& user_id) 
    : user_id(user_id) {
    apply_defaults();
}

void NotificationPreferences::apply_defaults() {
    // Default channel preferences - in-app and push for most types
    int default_channels = static_cast<int>(DeliveryChannel::IN_APP) | 
                          static_cast<int>(DeliveryChannel::PUSH_NOTIFICATION);
    
    channel_preferences[NotificationType::LIKE] = default_channels;
    channel_preferences[NotificationType::COMMENT] = default_channels;
    channel_preferences[NotificationType::FOLLOW] = default_channels;
    channel_preferences[NotificationType::MENTION] = default_channels | 
                                                   static_cast<int>(DeliveryChannel::EMAIL);
    channel_preferences[NotificationType::REPLY] = default_channels;
    channel_preferences[NotificationType::RETWEET] = default_channels;
    channel_preferences[NotificationType::QUOTE_TWEET] = default_channels;
    channel_preferences[NotificationType::DIRECT_MESSAGE] = default_channels | 
                                                          static_cast<int>(DeliveryChannel::EMAIL);
    channel_preferences[NotificationType::SYSTEM_ALERT] = default_channels | 
                                                        static_cast<int>(DeliveryChannel::EMAIL);
    
    // Default type enablement
    for (auto& [type, _] : channel_preferences) {
        type_enabled[type] = true;
    }
    
    // Quiet hours: 10 PM to 8 AM
    enable_quiet_hours = true;
    auto now = std::chrono::system_clock::now();
    auto today = std::chrono::floor<std::chrono::days>(now);
    quiet_start = today + std::chrono::hours(22); // 10 PM
    quiet_end = today + std::chrono::hours(32);   // 8 AM next day
    
    timezone = "UTC";
    
    // Frequency limits (per hour)
    frequency_limits[NotificationType::LIKE] = 50;
    frequency_limits[NotificationType::COMMENT] = 20;
    frequency_limits[NotificationType::FOLLOW] = 10;
    frequency_limits[NotificationType::MENTION] = 20;
    frequency_limits[NotificationType::REPLY] = 20;
    frequency_limits[NotificationType::RETWEET] = 30;
    frequency_limits[NotificationType::QUOTE_TWEET] = 15;
    frequency_limits[NotificationType::DIRECT_MESSAGE] = 100;
    frequency_limits[NotificationType::SYSTEM_ALERT] = 5;
    
    // Batching preferences
    enable_batching = true;
    batch_interval = std::chrono::minutes(15);
    
    // Privacy preferences
    show_preview_in_lock_screen = true;
    show_sender_name = true;
    enable_read_receipts = true;
}

void NotificationPreferences::set_channel_preference(NotificationType type, int channels) {
    channel_preferences[type] = channels;
}

int NotificationPreferences::get_channel_preference(NotificationType type) const {
    auto it = channel_preferences.find(type);
    if (it != channel_preferences.end()) {
        return it->second;
    }
    return static_cast<int>(DeliveryChannel::IN_APP); // Default
}

bool NotificationPreferences::is_channel_enabled(NotificationType type, DeliveryChannel channel) const {
    int channels = get_channel_preference(type);
    return (channels & static_cast<int>(channel)) != 0;
}

void NotificationPreferences::set_type_enabled(NotificationType type, bool enabled) {
    type_enabled[type] = enabled;
}

bool NotificationPreferences::is_type_enabled(NotificationType type) const {
    auto it = type_enabled.find(type);
    if (it != type_enabled.end()) {
        return it->second;
    }
    return true; // Default enabled
}

void NotificationPreferences::add_blocked_sender(const std::string& sender_id) {
    auto it = std::find(blocked_senders.begin(), blocked_senders.end(), sender_id);
    if (it == blocked_senders.end()) {
        blocked_senders.push_back(sender_id);
    }
}

void NotificationPreferences::remove_blocked_sender(const std::string& sender_id) {
    auto it = std::find(blocked_senders.begin(), blocked_senders.end(), sender_id);
    if (it != blocked_senders.end()) {
        blocked_senders.erase(it);
    }
}

bool NotificationPreferences::is_sender_blocked(const std::string& sender_id) const {
    return std::find(blocked_senders.begin(), blocked_senders.end(), sender_id) 
           != blocked_senders.end();
}

bool NotificationPreferences::is_in_quiet_hours() const {
    if (!enable_quiet_hours) {
        return false;
    }
    
    auto now = std::chrono::system_clock::now();
    auto today = std::chrono::floor<std::chrono::days>(now);
    auto time_of_day = now - today;
    
    auto quiet_start_time = quiet_start - std::chrono::floor<std::chrono::days>(quiet_start);
    auto quiet_end_time = quiet_end - std::chrono::floor<std::chrono::days>(quiet_end);
    
    // Handle overnight quiet hours
    if (quiet_start_time > quiet_end_time) {
        return time_of_day >= quiet_start_time || time_of_day <= quiet_end_time;
    } else {
        return time_of_day >= quiet_start_time && time_of_day <= quiet_end_time;
    }
}

bool NotificationPreferences::should_batch_notifications() const {
    return enable_batching;
}

bool NotificationPreferences::can_send_notification(NotificationType type) const {
    return is_type_enabled(type);
}

nlohmann::json NotificationPreferences::to_json() const {
    nlohmann::json json;
    json["user_id"] = user_id;
    json["enable_quiet_hours"] = enable_quiet_hours;
    json["timezone"] = timezone;
    json["enable_batching"] = enable_batching;
    json["batch_interval_minutes"] = batch_interval.count();
    json["show_preview_in_lock_screen"] = show_preview_in_lock_screen;
    json["show_sender_name"] = show_sender_name;
    json["enable_read_receipts"] = enable_read_receipts;
    json["blocked_senders"] = blocked_senders;
    json["priority_senders"] = priority_senders;
    
    // Convert maps to JSON objects
    nlohmann::json channel_prefs;
    for (const auto& [type, channels] : channel_preferences) {
        channel_prefs[std::to_string(static_cast<int>(type))] = channels;
    }
    json["channel_preferences"] = channel_prefs;
    
    nlohmann::json freq_limits;
    for (const auto& [type, limit] : frequency_limits) {
        freq_limits[std::to_string(static_cast<int>(type))] = limit;
    }
    json["frequency_limits"] = freq_limits;
    
    nlohmann::json type_enabled_json;
    for (const auto& [type, enabled] : type_enabled) {
        type_enabled_json[std::to_string(static_cast<int>(type))] = enabled;
    }
    json["type_enabled"] = type_enabled_json;
    
    return json;
}

void NotificationPreferences::from_json(const nlohmann::json& json) {
    if (json.contains("user_id")) user_id = json["user_id"];
    if (json.contains("enable_quiet_hours")) enable_quiet_hours = json["enable_quiet_hours"];
    if (json.contains("timezone")) timezone = json["timezone"];
    if (json.contains("enable_batching")) enable_batching = json["enable_batching"];
    if (json.contains("batch_interval_minutes")) {
        batch_interval = std::chrono::minutes(json["batch_interval_minutes"]);
    }
    if (json.contains("show_preview_in_lock_screen")) {
        show_preview_in_lock_screen = json["show_preview_in_lock_screen"];
    }
    if (json.contains("show_sender_name")) show_sender_name = json["show_sender_name"];
    if (json.contains("enable_read_receipts")) enable_read_receipts = json["enable_read_receipts"];
    if (json.contains("blocked_senders")) blocked_senders = json["blocked_senders"];
    if (json.contains("priority_senders")) priority_senders = json["priority_senders"];
    
    // Convert JSON objects back to maps
    if (json.contains("channel_preferences")) {
        channel_preferences.clear();
        for (const auto& [key, value] : json["channel_preferences"].items()) {
            NotificationType type = static_cast<NotificationType>(std::stoi(key));
            channel_preferences[type] = value;
        }
    }
    
    if (json.contains("frequency_limits")) {
        frequency_limits.clear();
        for (const auto& [key, value] : json["frequency_limits"].items()) {
            NotificationType type = static_cast<NotificationType>(std::stoi(key));
            frequency_limits[type] = value;
        }
    }
    
    if (json.contains("type_enabled")) {
        type_enabled.clear();
        for (const auto& [key, value] : json["type_enabled"].items()) {
            NotificationType type = static_cast<NotificationType>(std::stoi(key));
            type_enabled[type] = value;
        }
    }
}

bool NotificationPreferences::is_valid() const {
    return !user_id.empty();
}

// Utility Functions

std::string notification_type_to_string(NotificationType type) {
    switch (type) {
        case NotificationType::LIKE: return "like";
        case NotificationType::COMMENT: return "comment";
        case NotificationType::FOLLOW: return "follow";
        case NotificationType::MENTION: return "mention";
        case NotificationType::REPLY: return "reply";
        case NotificationType::RENOTE: return "renote";
        case NotificationType::QUOTE_NOTE: return "quote_note";
        case NotificationType::DIRECT_MESSAGE: return "direct_message";
        case NotificationType::SYSTEM_ALERT: return "system_alert";
        case NotificationType::PROMOTION: return "promotion";
        case NotificationType::TRENDING_NOTE: return "trending_note";
        case NotificationType::FOLLOWER_MILESTONE: return "follower_milestone";
        case NotificationType::NOTE_MILESTONE: return "note_milestone";
        default: return "unknown";
    }
}

NotificationType string_to_notification_type(const std::string& type_str) {
    if (type_str == "like") return NotificationType::LIKE;
    if (type_str == "comment") return NotificationType::COMMENT;
    if (type_str == "follow") return NotificationType::FOLLOW;
    if (type_str == "mention") return NotificationType::MENTION;
    if (type_str == "reply") return NotificationType::REPLY;
    if (type_str == "renote") return NotificationType::RENOTE;
    if (type_str == "quote_note") return NotificationType::QUOTE_NOTE;
    if (type_str == "direct_message") return NotificationType::DIRECT_MESSAGE;
    if (type_str == "system_alert") return NotificationType::SYSTEM_ALERT;
    if (type_str == "promotion") return NotificationType::PROMOTION;
    if (type_str == "trending_note") return NotificationType::TRENDING_NOTE;
    if (type_str == "follower_milestone") return NotificationType::FOLLOWER_MILESTONE;
    if (type_str == "note_milestone") return NotificationType::NOTE_MILESTONE;
    return NotificationType::SYSTEM_ALERT; // Safe default
}

std::string delivery_channel_to_string(DeliveryChannel channel) {
    switch (channel) {
        case DeliveryChannel::IN_APP: return "in_app";
        case DeliveryChannel::PUSH_NOTIFICATION: return "push";
        case DeliveryChannel::EMAIL: return "email";
        case DeliveryChannel::SMS: return "sms";
        case DeliveryChannel::WEBHOOK: return "webhook";
        default: return "unknown";
    }
}

DeliveryChannel string_to_delivery_channel(const std::string& channel_str) {
    if (channel_str == "in_app") return DeliveryChannel::IN_APP;
    if (channel_str == "push") return DeliveryChannel::PUSH_NOTIFICATION;
    if (channel_str == "email") return DeliveryChannel::EMAIL;
    if (channel_str == "sms") return DeliveryChannel::SMS;
    if (channel_str == "webhook") return DeliveryChannel::WEBHOOK;
    return DeliveryChannel::IN_APP; // Default
}

std::string priority_to_string(NotificationPriority priority) {
    switch (priority) {
        case NotificationPriority::LOW: return "low";
        case NotificationPriority::NORMAL: return "normal";
        case NotificationPriority::HIGH: return "high";
        case NotificationPriority::URGENT: return "urgent";
        default: return "normal";
    }
}

NotificationPriority string_to_priority(const std::string& priority_str) {
    if (priority_str == "low") return NotificationPriority::LOW;
    if (priority_str == "normal") return NotificationPriority::NORMAL;
    if (priority_str == "high") return NotificationPriority::HIGH;
    if (priority_str == "urgent") return NotificationPriority::URGENT;
    return NotificationPriority::NORMAL; // Default
}

std::string status_to_string(DeliveryStatus status) {
    switch (status) {
        case DeliveryStatus::PENDING: return "pending";
        case DeliveryStatus::SENT: return "sent";
        case DeliveryStatus::DELIVERED: return "delivered";
        case DeliveryStatus::READ: return "read";
        case DeliveryStatus::FAILED: return "failed";
        case DeliveryStatus::CANCELLED: return "cancelled";
        default: return "unknown";
    }
}

DeliveryStatus string_to_status(const std::string& status_str) {
    if (status_str == "pending") return DeliveryStatus::PENDING;
    if (status_str == "sent") return DeliveryStatus::SENT;
    if (status_str == "delivered") return DeliveryStatus::DELIVERED;
    if (status_str == "read") return DeliveryStatus::READ;
    if (status_str == "failed") return DeliveryStatus::FAILED;
    if (status_str == "cancelled") return DeliveryStatus::CANCELLED;
    return DeliveryStatus::PENDING; // Default
}

// Helper Functions for Notification Creation

std::shared_ptr<Notification> create_like_notification(
    const std::string& recipient_id, const std::string& liker_id, 
    const std::string& note_id) {
    
    auto notification = std::make_shared<Notification>(
        recipient_id, liker_id, NotificationType::LIKE,
        "New like on your note",
        "{{sender_name}} liked your note"
    );
    
    notification->note_id = note_id;
    notification->action_url = "/notes/" + note_id;
    notification->set_group_key("like_" + note_id);
    notification->template_data["sender_name"] = liker_id; // Will be resolved to actual name
    
    return notification;
}

std::shared_ptr<Notification> create_follow_notification(
    const std::string& recipient_id, const std::string& follower_id) {
    
    auto notification = std::make_shared<Notification>(
        recipient_id, follower_id, NotificationType::FOLLOW,
        "New follower",
        "{{sender_name}} started following you"
    );
    
    notification->action_url = "/users/" + follower_id;
    notification->template_data["sender_name"] = follower_id; // Would be resolved to actual name
    
    return notification;
}

std::shared_ptr<Notification> create_comment_notification(
    const std::string& recipient_id, const std::string& commenter_id,
    const std::string& note_id, const std::string& comment_id) {
    
    auto notification = std::make_shared<Notification>(
        recipient_id, commenter_id, NotificationType::COMMENT,
        "New comment on your note",
        "{{sender_name}} commented on your note: {{comment_preview}}"
    );
    
    notification->note_id = note_id;
    notification->comment_id = comment_id;
    notification->action_url = "/notes/" + note_id + "#comment-" + comment_id;
    notification->set_group_key("comment_" + note_id);
    notification->template_data["sender_name"] = commenter_id;
    notification->template_data["comment_preview"] = "..."; // Will contain actual comment preview
    
    return notification;
}

std::shared_ptr<Notification> create_mention_notification(
    const std::string& recipient_id, const std::string& mentioner_id,
    const std::string& note_id) {
    
    auto notification = std::make_shared<Notification>(
        recipient_id, mentioner_id, NotificationType::MENTION,
        "You were mentioned",
        "{{sender_name}} mentioned you in a note"
    );
    
    notification->note_id = note_id;
    notification->action_url = "/notes/" + note_id;
    notification->priority = NotificationPriority::HIGH;
    notification->add_delivery_channel(DeliveryChannel::EMAIL); // Mentions are important
    notification->template_data["sender_name"] = mentioner_id;
    
    return notification;
}

std::shared_ptr<Notification> create_renote_notification(
    const std::string& recipient_id, const std::string& renoter_id,
    const std::string& note_id) {
    
    auto notification = std::make_shared<Notification>(
        recipient_id, renoter_id, NotificationType::RENOTE,
        "Your note was renoted",
        "{{sender_name}} renoted your note"
    );
    
    notification->note_id = note_id;
    notification->action_url = "/notes/" + note_id;
    notification->set_group_key("renote_" + note_id);
    notification->template_data["sender_name"] = renoter_id;
    
    return notification;
}

std::shared_ptr<Notification> create_system_notification(
    const std::string& recipient_id, const std::string& title,
    const std::string& message, NotificationPriority priority) {
    
    auto notification = std::make_shared<Notification>(
        recipient_id, "system", NotificationType::SYSTEM_ALERT,
        title, message
    );
    
    notification->priority = priority;
    notification->add_delivery_channel(DeliveryChannel::EMAIL);
    notification->allow_bundling = false; // System notifications shouldn't be bundled
    
    return notification;
}

} // namespace models
} // namespace notification_service
} // namespace sonet
