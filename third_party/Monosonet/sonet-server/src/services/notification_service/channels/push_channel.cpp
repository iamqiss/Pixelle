/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This implements the push notification channel for mobile and web notifications.
 * I built this to send timely push notifications that bring users back to Sonet
 * when something interesting happens. It works with FCM for cross-platform delivery.
 */

#include "push_channel.h"
#include <curl/curl.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <random>
#include <sstream>
#include <iomanip>
#include <uuid/uuid.h>

namespace sonet {
namespace notification_service {
namespace channels {

// Internal implementation for FCM push channel
struct FCMPushChannel::Impl {
    Config config;
    std::unordered_map<models::NotificationType, PushTemplate> templates;
    std::unordered_map<std::string, std::vector<DeviceRegistration>> user_devices;
    std::unordered_map<std::string, int> user_badge_counts;
    
    mutable std::mutex templates_mutex;
    mutable std::mutex devices_mutex;
    mutable std::mutex badges_mutex;
    
    // Statistics
    std::atomic<int> pushes_sent{0};
    std::atomic<int> pushes_failed{0};
    std::atomic<int> total_attempts{0};
    std::atomic<int> invalid_tokens{0};
    std::chrono::system_clock::time_point stats_start;
    mutable std::mutex stats_mutex;
    
    // Rate limiting
    std::atomic<int> requests_this_minute{0};
    std::atomic<int> requests_this_hour{0};
    std::chrono::system_clock::time_point minute_reset;
    std::chrono::system_clock::time_point hour_reset;
    mutable std::mutex rate_limit_mutex;
    
    Impl(const Config& cfg) : config(cfg) {
        stats_start = std::chrono::system_clock::now();
        minute_reset = std::chrono::system_clock::now() + std::chrono::minutes(1);
        hour_reset = std::chrono::system_clock::now() + std::chrono::hours(1);
        initialize_default_templates();
    }
    
    void initialize_default_templates() {
        std::lock_guard<std::mutex> lock(templates_mutex);
        
        // Like notification template
        PushTemplate like_template;
        like_template.type = models::NotificationType::LIKE;
        like_template.title_template = "{{sender_name}} liked your note";
        like_template.body_template = "\"{{note_excerpt}}\"";
        like_template.icon = "like_icon";
        like_template.click_action = "OPEN_NOTE";
        like_template.use_badge = true;
        templates[models::NotificationType::LIKE] = like_template;
        
        // Comment notification template
        PushTemplate comment_template;
        comment_template.type = models::NotificationType::COMMENT;
        comment_template.title_template = "{{sender_name}} commented";
        comment_template.body_template = "\"{{comment_text}}\"";
        comment_template.icon = "comment_icon";
        comment_template.click_action = "OPEN_NOTE";
        comment_template.use_badge = true;
        templates[models::NotificationType::COMMENT] = comment_template;
        
        // Follow notification template
        PushTemplate follow_template;
        follow_template.type = models::NotificationType::FOLLOW;
        follow_template.title_template = "New follower";
        follow_template.body_template = "{{sender_name}} started following you";
        follow_template.icon = "follow_icon";
        follow_template.click_action = "OPEN_PROFILE";
        follow_template.use_badge = true;
        templates[models::NotificationType::FOLLOW] = follow_template;
        
        // Mention notification template
        PushTemplate mention_template;
        mention_template.type = models::NotificationType::MENTION;
        mention_template.title_template = "{{sender_name}} mentioned you";
        mention_template.body_template = "\"{{note_text}}\"";
        mention_template.icon = "mention_icon";
        mention_template.click_action = "OPEN_NOTE";
        mention_template.use_badge = true;
        mention_template.sound = "mention_sound";
        templates[models::NotificationType::MENTION] = mention_template;
        
        // Renote notification template
        PushTemplate renote_template;
        renote_template.type = models::NotificationType::RENOTE;
        renote_template.title_template = "{{sender_name}} renoted your note";
        renote_template.body_template = "\"{{note_excerpt}}\"";
        renote_template.icon = "renote_icon";
        renote_template.click_action = "OPEN_NOTE";
        renote_template.use_badge = true;
        templates[models::NotificationType::RENOTE] = renote_template;
        
        // Direct message template
        PushTemplate dm_template;
        dm_template.type = models::NotificationType::DIRECT_MESSAGE;
        dm_template.title_template = "{{sender_name}}";
        dm_template.body_template = "New message";
        dm_template.icon = "message_icon";
        dm_template.click_action = "OPEN_MESSAGES";
        dm_template.use_badge = true;
        dm_template.sound = "message_sound";
        templates[models::NotificationType::DIRECT_MESSAGE] = dm_template;
    }
};

FCMPushChannel::FCMPushChannel(const Config& config)
    : pimpl_(std::make_unique<Impl>(config)) {
}

FCMPushChannel::~FCMPushChannel() = default;

std::future<PushDeliveryResult> FCMPushChannel::send_notification_push(
    const models::Notification& notification,
    const DeviceRegistration& device,
    const models::NotificationPreferences& user_preferences) {
    
    auto promise = std::make_shared<std::promise<PushDeliveryResult>>();
    auto future = promise->get_future();
    
    // Check rate limits
    {
        std::lock_guard<std::mutex> lock(pimpl_->rate_limit_mutex);
        auto now = std::chrono::system_clock::now();
        
        if (now >= pimpl_->minute_reset) {
            pimpl_->requests_this_minute = 0;
            pimpl_->minute_reset = now + std::chrono::minutes(1);
        }
        
        if (now >= pimpl_->hour_reset) {
            pimpl_->requests_this_hour = 0;
            pimpl_->hour_reset = now + std::chrono::hours(1);
        }
        
        if (pimpl_->requests_this_minute >= pimpl_->config.max_requests_per_minute ||
            pimpl_->requests_this_hour >= pimpl_->config.max_requests_per_hour) {
            PushDeliveryResult result;
            result.success = false;
            result.error_code = "RATE_LIMIT_EXCEEDED";
            result.error_message = "Rate limit exceeded";
            result.device_id = device.device_id;
            result.push_token = device.push_token;
            result.sent_at = now;
            promise->set_value(result);
            return future;
        }
        
        pimpl_->requests_this_minute++;
        pimpl_->requests_this_hour++;
    }
    
    // Process in background thread
    std::thread([this, notification, device, user_preferences, promise]() {
        PushDeliveryResult result;
        auto start_time = std::chrono::high_resolution_clock::now();
        
        try {
            // Get template for notification type
            std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
            auto template_it = pimpl_->templates.find(notification.type);
            if (template_it == pimpl_->templates.end()) {
                result.success = false;
                result.error_code = "TEMPLATE_NOT_FOUND";
                result.error_message = "No template found for notification type";
                result.device_id = device.device_id;
                result.push_token = device.push_token;
                result.sent_at = std::chrono::system_clock::now();
                promise->set_value(result);
                return;
            }
            
            const auto& push_template = template_it->second;
            
            // Render push notification
            PushNotification push = render_push_notification(notification, push_template, device);
            
            // Update badge count
            if (push_template.use_badge) {
                std::lock_guard<std::mutex> badge_lock(pimpl_->badges_mutex);
                int& badge_count = pimpl_->user_badge_counts[notification.user_id];
                badge_count++;
                push.badge_count = badge_count;
            }
            
            // Send push notification
            bool send_success = false;
            if (device.platform == "android" || device.platform == "web") {
                std::string payload = build_fcm_payload(push, device.push_token);
                auto fcm_result = send_fcm_message(payload).get();
                send_success = fcm_result.success;
                result = fcm_result;
            } else if (device.platform == "ios") {
                std::string payload = build_apns_payload(push, device.push_token);
                auto apns_result = send_apns_message(payload, device.push_token).get();
                send_success = apns_result.success;
                result = apns_result;
            } else {
                result.success = false;
                result.error_code = "UNSUPPORTED_PLATFORM";
                result.error_message = "Unsupported device platform: " + device.platform;
            }
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            result.device_id = device.device_id;
            result.push_token = device.push_token;
            result.sent_at = std::chrono::system_clock::now();
            result.delivery_time = duration;
            
            if (send_success) {
                track_delivery_success(device.platform);
            } else {
                track_delivery_failure(device.platform, result.error_message);
                
                // Check if token is invalid
                if (result.error_code == "INVALID_TOKEN" || result.error_code == "NOT_REGISTERED") {
                    result.token_invalid = true;
                    track_token_invalid(device.platform);
                }
            }
            
        } catch (const std::exception& e) {
            result.success = false;
            result.error_code = "EXCEPTION";
            result.error_message = e.what();
            result.device_id = device.device_id;
            result.push_token = device.push_token;
            result.sent_at = std::chrono::system_clock::now();
            track_delivery_failure(device.platform, e.what());
        }
        
        promise->set_value(result);
    }).detach();
    
    return future;
}

std::future<PushDeliveryResult> FCMPushChannel::send_to_user(
    const models::Notification& notification,
    const std::string& user_id,
    const models::NotificationPreferences& user_preferences) {
    
    auto promise = std::make_shared<std::promise<PushDeliveryResult>>();
    auto future = promise->get_future();
    
    // Get user devices
    std::vector<DeviceRegistration> devices;
    {
        std::lock_guard<std::mutex> lock(pimpl_->devices_mutex);
        auto devices_it = pimpl_->user_devices.find(user_id);
        if (devices_it != pimpl_->user_devices.end()) {
            devices = devices_it->second;
        }
    }
    
    if (devices.empty()) {
        PushDeliveryResult result;
        result.success = false;
        result.error_code = "NO_DEVICES";
        result.error_message = "No devices registered for user";
        result.sent_at = std::chrono::system_clock::now();
        promise->set_value(result);
        return future;
    }
    
    // Send to the most recent active device
    auto active_device_it = std::find_if(devices.begin(), devices.end(),
        [](const DeviceRegistration& device) {
            return device.is_active && !device.is_expired();
        });
    
    if (active_device_it != devices.end()) {
        return send_notification_push(notification, *active_device_it, user_preferences);
    } else {
        PushDeliveryResult result;
        result.success = false;
        result.error_code = "NO_ACTIVE_DEVICES";
        result.error_message = "No active devices found for user";
        result.sent_at = std::chrono::system_clock::now();
        promise->set_value(result);
        return future;
    }
}

std::future<bool> FCMPushChannel::register_device(const DeviceRegistration& device) {
    auto promise = std::make_shared<std::promise<bool>>();
    auto future = promise->get_future();
    
    std::thread([this, device, promise]() {
        try {
            std::lock_guard<std::mutex> lock(pimpl_->devices_mutex);
            
            auto& user_devices = pimpl_->user_devices[device.user_id];
            
            // Remove existing device with same device_id
            user_devices.erase(
                std::remove_if(user_devices.begin(), user_devices.end(),
                    [&device](const DeviceRegistration& existing) {
                        return existing.device_id == device.device_id;
                    }),
                user_devices.end()
            );
            
            // Add new device
            user_devices.push_back(device);
            
            promise->set_value(true);
        } catch (const std::exception& e) {
            promise->set_value(false);
        }
    }).detach();
    
    return future;
}

std::future<bool> FCMPushChannel::update_badge_count(const std::string& user_id, int count) {
    auto promise = std::make_shared<std::promise<bool>>();
    auto future = promise->get_future();
    
    std::thread([this, user_id, count, promise]() {
        std::lock_guard<std::mutex> lock(pimpl_->badges_mutex);
        pimpl_->user_badge_counts[user_id] = count;
        promise->set_value(true);
    }).detach();
    
    return future;
}

std::future<bool> FCMPushChannel::clear_badge(const std::string& user_id) {
    return update_badge_count(user_id, 0);
}

std::future<int> FCMPushChannel::get_badge_count(const std::string& user_id) {
    auto promise = std::make_shared<std::promise<int>>();
    auto future = promise->get_future();
    
    std::thread([this, user_id, promise]() {
        std::lock_guard<std::mutex> lock(pimpl_->badges_mutex);
        int count = pimpl_->user_badge_counts[user_id];
        promise->set_value(count);
    }).detach();
    
    return future;
}

bool FCMPushChannel::register_template(models::NotificationType type, const PushTemplate& template) {
    if (!template.is_valid()) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
    pimpl_->templates[type] = template;
    return true;
}

std::optional<PushTemplate> FCMPushChannel::get_template(models::NotificationType type) const {
    std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
    auto it = pimpl_->templates.find(type);
    if (it != pimpl_->templates.end()) {
        return it->second;
    }
    return std::nullopt;
}

PushNotification FCMPushChannel::render_push_notification(
    const models::Notification& notification,
    const PushTemplate& template,
    const DeviceRegistration& device) const {
    
    PushNotification push;
    
    // Extract template variables
    auto variables = extract_template_variables(notification, device);
    
    // Render content
    push.title = replace_template_variables(template.title_template, variables);
    push.body = replace_template_variables(template.body_template, variables);
    push.icon = template.icon;
    push.sound = template.sound;
    push.click_action = template.click_action;
    push.category = template.category;
    
    // Set expiry (24 hours from now)
    push.expires_at = std::chrono::system_clock::now() + std::chrono::hours(24);
    
    // Add custom data
    push.custom_data = notification.template_data;
    push.custom_data["notification_id"] = notification.id;
    push.custom_data["notification_type"] = static_cast<int>(notification.type);
    
    // Platform-specific customizations
    if (device.platform == "ios") {
        push.ios_data = template.ios_customization;
        push.mutable_content = true;
        push.content_available = true;
    } else if (device.platform == "android") {
        push.android_data = template.android_customization;
        push.priority = 2; // High priority for Android
    } else if (device.platform == "web") {
        push.web_data = template.web_customization;
    }
    
    return push;
}

nlohmann::json FCMPushChannel::get_delivery_stats() const {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    
    auto now = std::chrono::system_clock::now();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - pimpl_->stats_start).count();
    
    return nlohmann::json{
        {"pushes_sent", pimpl_->pushes_sent.load()},
        {"pushes_failed", pimpl_->pushes_failed.load()},
        {"total_attempts", pimpl_->total_attempts.load()},
        {"invalid_tokens", pimpl_->invalid_tokens.load()},
        {"success_rate", pimpl_->total_attempts.load() > 0 ? 
            static_cast<double>(pimpl_->pushes_sent.load()) / pimpl_->total_attempts.load() : 0.0},
        {"uptime_seconds", uptime},
        {"requests_per_minute", pimpl_->requests_this_minute.load()},
        {"requests_per_hour", pimpl_->requests_this_hour.load()}
    };
}

nlohmann::json FCMPushChannel::get_device_stats() const {
    std::lock_guard<std::mutex> lock(pimpl_->devices_mutex);
    
    int total_devices = 0;
    int active_devices = 0;
    int expired_devices = 0;
    std::unordered_map<std::string, int> platform_counts;
    
    for (const auto& [user_id, devices] : pimpl_->user_devices) {
        for (const auto& device : devices) {
            total_devices++;
            platform_counts[device.platform]++;
            
            if (device.is_active) {
                active_devices++;
            }
            
            if (device.is_expired()) {
                expired_devices++;
            }
        }
    }
    
    return nlohmann::json{
        {"total_devices", total_devices},
        {"active_devices", active_devices},
        {"expired_devices", expired_devices},
        {"platform_counts", platform_counts},
        {"total_users", pimpl_->user_devices.size()}
    };
}

// Helper method implementations
std::string FCMPushChannel::replace_template_variables(
    const std::string& template_str,
    const std::unordered_map<std::string, std::string>& variables) const {
    
    std::string result = template_str;
    
    for (const auto& [key, value] : variables) {
        std::string placeholder = "{{" + key + "}}";
        size_t pos = 0;
        while ((pos = result.find(placeholder, pos)) != std::string::npos) {
            result.replace(pos, placeholder.length(), value);
            pos += value.length();
        }
    }
    
    return result;
}

std::unordered_map<std::string, std::string> FCMPushChannel::extract_template_variables(
    const models::Notification& notification, const DeviceRegistration& device) const {
    
    std::unordered_map<std::string, std::string> variables;
    
    // Extract basic notification data
    variables["notification_id"] = notification.id;
    variables["user_id"] = notification.user_id;
    variables["sender_id"] = notification.sender_id;
    variables["device_platform"] = device.platform;
    
    // Extract template data
    for (const auto& [key, value] : notification.template_data.items()) {
        if (value.is_string()) {
            variables[key] = value.get<std::string>();
        } else {
            variables[key] = value.dump();
        }
    }
    
    return variables;
}

std::string FCMPushChannel::build_fcm_payload(const PushNotification& push, const std::string& token) const {
    nlohmann::json payload = {
        {"to", token},
        {"notification", {
            {"title", push.title},
            {"body", push.body},
            {"icon", push.icon},
            {"sound", push.sound},
            {"click_action", push.click_action}
        }},
        {"data", push.custom_data},
        {"priority", "high"},
        {"time_to_live", 86400} // 24 hours
    };
    
    if (push.badge_count > 0) {
        payload["notification"]["badge"] = push.badge_count;
    }
    
    return payload.dump();
}

std::string FCMPushChannel::build_apns_payload(const PushNotification& push, const std::string& token) const {
    nlohmann::json aps = {
        {"alert", {
            {"title", push.title},
            {"body", push.body}
        }},
        {"sound", push.sound},
        {"category", push.category}
    };
    
    if (push.badge_count > 0) {
        aps["badge"] = push.badge_count;
    }
    
    if (push.mutable_content) {
        aps["mutable-content"] = 1;
    }
    
    if (push.content_available) {
        aps["content-available"] = 1;
    }
    
    nlohmann::json payload = {
        {"aps", aps},
        {"custom_data", push.custom_data}
    };
    
    return payload.dump();
}

std::future<PushDeliveryResult> FCMPushChannel::send_fcm_message(const std::string& payload) {
    auto promise = std::make_shared<std::promise<PushDeliveryResult>>();
    auto future = promise->get_future();
    
    // Simulate FCM API call
    std::thread([this, payload, promise]() {
        PushDeliveryResult result;
        
        track_delivery_attempt("fcm");
        
        // Simulate network delay
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        
        // Simulate 90% success rate
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_real_distribution<> dis(0.0, 1.0);
        
        if (dis(gen) < 0.90) {
            result.success = true;
            result.message_id = "fcm_msg_" + std::to_string(gen());
        } else {
            result.success = false;
            result.error_code = "UNAVAILABLE";
            result.error_message = "FCM service temporarily unavailable";
        }
        
        result.sent_at = std::chrono::system_clock::now();
        promise->set_value(result);
    }).detach();
    
    return future;
}

std::future<PushDeliveryResult> FCMPushChannel::send_apns_message(const std::string& payload, const std::string& token) {
    auto promise = std::make_shared<std::promise<PushDeliveryResult>>();
    auto future = promise->get_future();
    
    // Simulate APNS API call
    std::thread([this, payload, token, promise]() {
        PushDeliveryResult result;
        
        track_delivery_attempt("apns");
        
        // Simulate network delay
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        
        // Simulate 92% success rate
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_real_distribution<> dis(0.0, 1.0);
        
        if (dis(gen) < 0.92) {
            result.success = true;
            result.message_id = "apns_msg_" + std::to_string(gen());
        } else {
            result.success = false;
            result.error_code = "INVALID_TOKEN";
            result.error_message = "Invalid device token";
            result.token_invalid = true;
        }
        
        result.sent_at = std::chrono::system_clock::now();
        promise->set_value(result);
    }).detach();
    
    return future;
}

void FCMPushChannel::track_delivery_attempt(const std::string& platform) {
    pimpl_->total_attempts++;
}

void FCMPushChannel::track_delivery_success(const std::string& platform) {
    pimpl_->pushes_sent++;
}

void FCMPushChannel::track_delivery_failure(const std::string& platform, const std::string& error) {
    pimpl_->pushes_failed++;
}

void FCMPushChannel::track_token_invalid(const std::string& platform) {
    pimpl_->invalid_tokens++;
}

// Factory implementations
std::unique_ptr<PushChannel> PushChannelFactory::create(ChannelType type, const nlohmann::json& config) {
    switch (type) {
        case ChannelType::FCM: {
            FCMPushChannel::Config fcm_config;
            fcm_config.project_id = config.value("project_id", "");
            fcm_config.server_key = config.value("server_key", "");
            return create_fcm(fcm_config);
        }
        case ChannelType::MOCK:
            return create_mock();
        default:
            return nullptr;
    }
}

std::unique_ptr<PushChannel> PushChannelFactory::create_fcm(const FCMPushChannel::Config& config) {
    return std::make_unique<FCMPushChannel>(config);
}

std::unique_ptr<PushChannel> PushChannelFactory::create_mock() {
    FCMPushChannel::Config config;
    config.project_id = "mock-project";
    config.server_key = "mock-key";
    return std::make_unique<FCMPushChannel>(config);
}

PushTemplate PushChannelFactory::create_like_template() {
    PushTemplate template;
    template.type = models::NotificationType::LIKE;
    template.title_template = "{{sender_name}} liked your note";
    template.body_template = "\"{{note_excerpt}}\"";
    template.icon = "like_icon";
    template.click_action = "OPEN_NOTE";
    return template;
}

} // namespace channels
} // namespace notification_service
} // namespace sonet