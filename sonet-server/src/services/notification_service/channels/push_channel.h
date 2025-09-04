/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This is the push notification channel for mobile and web notifications.
 * I built this to send timely, engaging push notifications that bring users
 * back to Sonet when something interesting happens with their notes.
 */

#pragma once

#include "../models/notification.h"
#include <string>
#include <vector>
#include <memory>
#include <future>
#include <unordered_map>
#include <chrono>
#include <nlohmann/json.hpp>

namespace sonet {
namespace notification_service {
namespace channels {

/**
 * Push notification payload structure
 * I designed this to work across iOS, Android, and web platforms
 */
struct PushNotification {
    std::string title;
    std::string body;
    std::string icon;
    std::string image;
    std::string badge;
    std::string sound = "default";
    std::string click_action;
    std::string category;
    bool silent = false;
    int badge_count = 0;
    std::chrono::system_clock::time_point expires_at;
    
    // Platform-specific data
    nlohmann::json ios_data;    // APNS specific fields
    nlohmann::json android_data; // FCM Android specific fields
    nlohmann::json web_data;    // Web push specific fields
    nlohmann::json custom_data; // App-specific data
    
    // Behavior settings
    bool collapse_id_enabled = false;
    std::string collapse_id;
    bool mutable_content = false;
    int priority = 1; // 0=low, 1=normal, 2=high
    bool content_available = false;
    
    nlohmann::json to_json() const {
        return nlohmann::json{
            {"title", title},
            {"body", body},
            {"icon", icon},
            {"image", image},
            {"badge", badge},
            {"sound", sound},
            {"click_action", click_action},
            {"category", category},
            {"silent", silent},
            {"badge_count", badge_count},
            {"expires_at", std::chrono::duration_cast<std::chrono::seconds>(
                expires_at.time_since_epoch()).count()},
            {"ios_data", ios_data},
            {"android_data", android_data},
            {"web_data", web_data},
            {"custom_data", custom_data},
            {"collapse_id_enabled", collapse_id_enabled},
            {"collapse_id", collapse_id},
            {"mutable_content", mutable_content},
            {"priority", priority},
            {"content_available", content_available}
        };
    }
};

/**
 * Device registration for push notifications
 * I track this to know where to send notifications for each user
 */
struct DeviceRegistration {
    std::string device_id;
    std::string user_id;
    std::string push_token;
    std::string platform; // "ios", "android", "web"
    std::string app_version;
    std::string os_version;
    std::string device_model;
    std::string timezone;
    std::string language;
    bool is_active = true;
    std::chrono::system_clock::time_point registered_at;
    std::chrono::system_clock::time_point last_seen;
    std::chrono::system_clock::time_point token_updated_at;
    nlohmann::json device_capabilities;
    
    bool is_expired() const {
        auto now = std::chrono::system_clock::now();
        auto days_since_update = std::chrono::duration_cast<std::chrono::hours>(
            now - token_updated_at).count() / 24;
        return days_since_update > 90; // Tokens expire after 90 days
    }
    
    nlohmann::json to_json() const {
        return nlohmann::json{
            {"device_id", device_id},
            {"user_id", user_id},
            {"push_token", push_token},
            {"platform", platform},
            {"app_version", app_version},
            {"os_version", os_version},
            {"device_model", device_model},
            {"timezone", timezone},
            {"language", language},
            {"is_active", is_active},
            {"registered_at", std::chrono::duration_cast<std::chrono::seconds>(
                registered_at.time_since_epoch()).count()},
            {"last_seen", std::chrono::duration_cast<std::chrono::seconds>(
                last_seen.time_since_epoch()).count()},
            {"token_updated_at", std::chrono::duration_cast<std::chrono::seconds>(
                token_updated_at.time_since_epoch()).count()},
            {"device_capabilities", device_capabilities}
        };
    }
};

/**
 * Push delivery result for tracking success/failure
 * I track these to improve delivery rates and handle token updates
 */
struct PushDeliveryResult {
    bool success = false;
    std::string message_id;
    std::string error_code;
    std::string error_message;
    std::string device_id;
    std::string push_token;
    bool token_invalid = false;
    bool should_retry = false;
    std::chrono::system_clock::time_point sent_at;
    std::chrono::milliseconds delivery_time{0};
    int retry_count = 0;
    nlohmann::json provider_response;
    
    nlohmann::json to_json() const {
        return nlohmann::json{
            {"success", success},
            {"message_id", message_id},
            {"error_code", error_code},
            {"error_message", error_message},
            {"device_id", device_id},
            {"push_token", push_token},
            {"token_invalid", token_invalid},
            {"should_retry", should_retry},
            {"sent_at", std::chrono::duration_cast<std::chrono::seconds>(
                sent_at.time_since_epoch()).count()},
            {"delivery_time_ms", delivery_time.count()},
            {"retry_count", retry_count},
            {"provider_response", provider_response}
        };
    }
};

/**
 * Push notification template for different notification types
 * I use these to create consistent, engaging push notifications
 */
struct PushTemplate {
    models::NotificationType type;
    std::string title_template;
    std::string body_template;
    std::string icon;
    std::string sound = "default";
    std::string category;
    std::string click_action;
    bool use_badge = true;
    bool use_image = false;
    std::string image_template;
    std::unordered_map<std::string, std::string> default_data;
    
    // Platform customizations
    nlohmann::json ios_customization;
    nlohmann::json android_customization;
    nlohmann::json web_customization;
    
    bool is_valid() const {
        return !title_template.empty() && !body_template.empty();
    }
};

/**
 * Push notification channel interface
 * I keep this abstract to support different push providers
 */
class PushChannel {
public:
    virtual ~PushChannel() = default;
    
    // Core sending methods
    virtual std::future<PushDeliveryResult> send_notification_push(
        const models::Notification& notification,
        const DeviceRegistration& device,
        const models::NotificationPreferences& user_preferences) = 0;
    
    virtual std::future<std::vector<PushDeliveryResult>> send_batch_push(
        const std::vector<models::Notification>& notifications,
        const std::vector<DeviceRegistration>& devices,
        const std::unordered_map<std::string, models::NotificationPreferences>& user_preferences) = 0;
    
    virtual std::future<PushDeliveryResult> send_to_user(
        const models::Notification& notification,
        const std::string& user_id,
        const models::NotificationPreferences& user_preferences) = 0;
    
    virtual std::future<std::vector<PushDeliveryResult>> send_to_users(
        const models::Notification& notification,
        const std::vector<std::string>& user_ids,
        const std::unordered_map<std::string, models::NotificationPreferences>& user_preferences) = 0;
    
    // Device management
    virtual std::future<bool> register_device(const DeviceRegistration& device) = 0;
    virtual std::future<bool> update_device(const DeviceRegistration& device) = 0;
    virtual std::future<bool> unregister_device(const std::string& device_id) = 0;
    virtual std::future<bool> unregister_user_devices(const std::string& user_id) = 0;
    virtual std::future<std::vector<DeviceRegistration>> get_user_devices(const std::string& user_id) = 0;
    virtual std::future<std::optional<DeviceRegistration>> get_device(const std::string& device_id) = 0;
    
    // Token management
    virtual std::future<bool> update_push_token(const std::string& device_id, const std::string& new_token) = 0;
    virtual std::future<bool> validate_push_token(const std::string& token, const std::string& platform) = 0;
    virtual std::future<int> cleanup_expired_tokens() = 0;
    virtual std::future<int> cleanup_invalid_tokens() = 0;
    
    // Template management
    virtual bool register_template(models::NotificationType type, const PushTemplate& tmpl) = 0;
    virtual bool update_template(models::NotificationType type, const PushTemplate& tmpl) = 0;
    virtual bool remove_template(models::NotificationType type) = 0;
    virtual std::optional<PushTemplate> get_template(models::NotificationType type) const = 0;
    
    // Rendering
    virtual PushNotification render_push_notification(
        const models::Notification& notification,
        const PushTemplate& tmpl,
        const DeviceRegistration& device) const = 0;
    
    // Testing and validation
    virtual std::future<bool> send_test_push(const std::string& device_id,
                                            const std::string& title,
                                            const std::string& message) = 0;
    virtual bool validate_push_payload(const PushNotification& push) const = 0;
    
    // Analytics and monitoring
    virtual nlohmann::json get_delivery_stats() const = 0;
    virtual nlohmann::json get_device_stats() const = 0;
    virtual nlohmann::json get_health_status() const = 0;
    virtual void reset_stats() = 0;
    
    // Badge management
    virtual std::future<bool> update_badge_count(const std::string& user_id, int count) = 0;
    virtual std::future<bool> clear_badge(const std::string& user_id) = 0;
    virtual std::future<int> get_badge_count(const std::string& user_id) = 0;
    
    // Configuration
    virtual bool configure(const nlohmann::json& config) = 0;
    virtual nlohmann::json get_config() const = 0;
};

/**
 * Firebase Cloud Messaging (FCM) implementation
 * I use FCM because it works for both Android and iOS
 */
class FCMPushChannel : public PushChannel {
public:
    struct Config {
        std::string project_id;
        std::string server_key;
        std::string service_account_json;
        std::string apns_certificate_path;
        std::string apns_key_path;
        std::string apns_key_id;
        std::string apns_team_id;
        
        // Connection settings
        std::chrono::seconds connection_timeout{30};
        std::chrono::seconds send_timeout{60};
        int max_connections = 20;
        int retry_attempts = 3;
        std::chrono::seconds retry_delay{5};
        
        // Rate limiting
        int max_requests_per_minute = 1000;
        int max_requests_per_hour = 10000;
        
        // Environment
        bool use_apns_sandbox = false;
        std::string fcm_endpoint = "https://fcm.googleapis.com/v1/projects";
        std::string apns_endpoint = "api.push.apple.com";
        
        // Features
        bool enable_batch_sending = true;
        int batch_size = 500;
        bool enable_token_validation = true;
        bool auto_cleanup_invalid_tokens = true;
        std::chrono::hours token_cleanup_interval{24};
    };
    
    explicit FCMPushChannel(const Config& config);
    virtual ~FCMPushChannel();
    
    // Implement PushChannel interface
    std::future<PushDeliveryResult> send_notification_push(
        const models::Notification& notification,
        const DeviceRegistration& device,
        const models::NotificationPreferences& user_preferences) override;
    
    std::future<std::vector<PushDeliveryResult>> send_batch_push(
        const std::vector<models::Notification>& notifications,
        const std::vector<DeviceRegistration>& devices,
        const std::unordered_map<std::string, models::NotificationPreferences>& user_preferences) override;
    
    std::future<PushDeliveryResult> send_to_user(
        const models::Notification& notification,
        const std::string& user_id,
        const models::NotificationPreferences& user_preferences) override;
    
    std::future<std::vector<PushDeliveryResult>> send_to_users(
        const models::Notification& notification,
        const std::vector<std::string>& user_ids,
        const std::unordered_map<std::string, models::NotificationPreferences>& user_preferences) override;
    
    std::future<bool> register_device(const DeviceRegistration& device) override;
    std::future<bool> update_device(const DeviceRegistration& device) override;
    std::future<bool> unregister_device(const std::string& device_id) override;
    std::future<bool> unregister_user_devices(const std::string& user_id) override;
    std::future<std::vector<DeviceRegistration>> get_user_devices(const std::string& user_id) override;
    std::future<std::optional<DeviceRegistration>> get_device(const std::string& device_id) override;
    
    std::future<bool> update_push_token(const std::string& device_id, const std::string& new_token) override;
    std::future<bool> validate_push_token(const std::string& token, const std::string& platform) override;
    std::future<int> cleanup_expired_tokens() override;
    std::future<int> cleanup_invalid_tokens() override;
    
    bool register_template(models::NotificationType type, const PushTemplate& tmpl) override;
    bool update_template(models::NotificationType type, const PushTemplate& tmpl) override;
    bool remove_template(models::NotificationType type) override;
    std::optional<PushTemplate> get_template(models::NotificationType type) const override;
    
    PushNotification render_push_notification(
        const models::Notification& notification,
        const PushTemplate& tmpl,
        const DeviceRegistration& device) const override;
    
    std::future<bool> send_test_push(const std::string& device_id,
                                    const std::string& title,
                                    const std::string& message) override;
    bool validate_push_payload(const PushNotification& push) const override;
    
    nlohmann::json get_delivery_stats() const override;
    nlohmann::json get_device_stats() const override;
    nlohmann::json get_health_status() const override;
    void reset_stats() override;
    
    std::future<bool> update_badge_count(const std::string& user_id, int count) override;
    std::future<bool> clear_badge(const std::string& user_id) override;
    std::future<int> get_badge_count(const std::string& user_id) override;
    
    bool configure(const nlohmann::json& config) override;
    nlohmann::json get_config() const override;
    
    // FCM-specific methods
    std::future<bool> subscribe_to_topic(const std::string& token, const std::string& topic);
    std::future<bool> unsubscribe_from_topic(const std::string& token, const std::string& topic);
    std::future<PushDeliveryResult> send_to_topic(const std::string& topic, const PushNotification& push);
    std::future<std::string> get_access_token();
    bool test_connection();
    
private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    // Internal helper methods
    std::string build_fcm_payload(const PushNotification& push, const std::string& token) const;
    std::string build_apns_payload(const PushNotification& push, const std::string& token) const;
    std::future<PushDeliveryResult> send_fcm_message(const std::string& payload);
    std::future<PushDeliveryResult> send_apns_message(const std::string& payload, const std::string& token);
    std::string replace_template_variables(const std::string& template,
                                         const std::unordered_map<std::string, std::string>& variables) const;
    std::unordered_map<std::string, std::string> extract_template_variables(
        const models::Notification& notification, const DeviceRegistration& device) const;
    void track_delivery_attempt(const std::string& platform);
    void track_delivery_success(const std::string& platform);
    void track_delivery_failure(const std::string& platform, const std::string& error);
    void track_token_invalid(const std::string& platform);
    void cleanup_old_stats();
};

/**
 * Factory for creating push channels
 * I use this to support different push providers
 */
class PushChannelFactory {
public:
    enum class ChannelType {
        FCM,
        APNS,
        WEB_PUSH,
        MOCK // For testing
    };
    
    static std::unique_ptr<PushChannel> create(ChannelType type, const nlohmann::json& config);
    static std::unique_ptr<PushChannel> create_fcm(const FCMPushChannel::Config& config);
    static std::unique_ptr<PushChannel> create_mock(); // For testing
    
    // Template helpers
    static PushTemplate create_like_template();
    static PushTemplate create_comment_template();
    static PushTemplate create_follow_template();
    static PushTemplate create_mention_template();
    static PushTemplate create_renote_template();
    static PushTemplate create_dm_template();
    static std::unordered_map<models::NotificationType, PushTemplate> create_default_templates();
};

} // namespace channels
} // namespace notification_service
} // namespace sonet