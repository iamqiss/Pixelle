/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This implements the email channel for beautiful notification emails.
 * I built this to send engaging emails that bring users back to Sonet
 * without being spammy. The templates are mobile-responsive and accessible.
 */

#include "email_channel.h"
#include <curl/curl.h>
#include <regex>
#include <sstream>
#include <iomanip>
#include <thread>
#include <mutex>
#include <atomic>

namespace sonet {
namespace notification_service {
namespace channels {

// Internal implementation for SMTP email channel
struct SMTPEmailChannel::Impl {
    Config config;
    std::unordered_map<models::NotificationType, EmailTemplate> templates;
    mutable std::mutex templates_mutex;
    
    // Statistics tracking
    std::atomic<int> emails_sent{0};
    std::atomic<int> emails_failed{0};
    std::atomic<int> total_attempts{0};
    std::chrono::system_clock::time_point stats_start;
    mutable std::mutex stats_mutex;
    
    // Rate limiting
    std::atomic<int> emails_this_minute{0};
    std::atomic<int> emails_this_hour{0};
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
        EmailTemplate like_template;
        like_template.subject_template = "{{sender_name}} liked your note";
        like_template.html_template = create_like_html_template();
        like_template.text_template = "{{sender_name}} liked your note: \"{{note_excerpt}}\"\n\nView on Sonet: {{note_url}}";
        like_template.required_variables = {"sender_name", "note_excerpt", "note_url"};
        templates[models::NotificationType::LIKE] = like_template;
        
        // Comment notification template  
        EmailTemplate comment_template;
        comment_template.subject_template = "{{sender_name}} commented on your note";
        comment_template.html_template = create_comment_html_template();
        comment_template.text_template = "{{sender_name}} commented on your note:\n\n\"{{comment_text}}\"\n\nView the conversation: {{note_url}}";
        comment_template.required_variables = {"sender_name", "comment_text", "note_url"};
        templates[models::NotificationType::COMMENT] = comment_template;
        
        // Follow notification template
        EmailTemplate follow_template;
        follow_template.subject_template = "{{sender_name}} is now following you";
        follow_template.html_template = create_follow_html_template();
        follow_template.text_template = "{{sender_name}} started following you on Sonet!\n\nView their profile: {{sender_profile_url}}";
        follow_template.required_variables = {"sender_name", "sender_profile_url"};
        templates[models::NotificationType::FOLLOW] = follow_template;
        
        // Mention notification template
        EmailTemplate mention_template;
        mention_template.subject_template = "{{sender_name}} mentioned you";
        mention_template.html_template = create_mention_html_template();
        mention_template.text_template = "{{sender_name}} mentioned you in a note:\n\n\"{{note_text}}\"\n\nView the note: {{note_url}}";
        mention_template.required_variables = {"sender_name", "note_text", "note_url"};
        templates[models::NotificationType::MENTION] = mention_template;
        
        // Renote notification template
        EmailTemplate renote_template;
        renote_template.subject_template = "{{sender_name}} renoted your note";
        renote_template.html_template = create_renote_html_template();
        renote_template.text_template = "{{sender_name}} renoted your note: \"{{note_excerpt}}\"\n\nView on Sonet: {{note_url}}";
        renote_template.required_variables = {"sender_name", "note_excerpt", "note_url"};
        templates[models::NotificationType::RENOTE] = renote_template;
        
        // Direct message template
        EmailTemplate dm_template;
        dm_template.subject_template = "New message from {{sender_name}}";
        dm_template.html_template = create_dm_html_template();
        dm_template.text_template = "You have a new message from {{sender_name}} on Sonet.\n\nView your messages: {{messages_url}}";
        dm_template.required_variables = {"sender_name", "messages_url"};
        templates[models::NotificationType::DIRECT_MESSAGE] = dm_template;
    }
    
    std::string create_like_html_template() {
        return R"(
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{sender_name}} liked your note</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px 20px; text-align: center; }
        .content { padding: 30px 20px; }
        .avatar { width: 50px; height: 50px; border-radius: 50%; display: inline-block; background: #ddd; }
        .note-preview { background: #f8f9fa; border-left: 4px solid #667eea; padding: 15px; margin: 20px 0; border-radius: 4px; }
        .btn { display: inline-block; padding: 12px 24px; background: #667eea; color: white; text-decoration: none; border-radius: 6px; margin: 20px 0; }
        .footer { padding: 20px; text-align: center; font-size: 12px; color: #666; border-top: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>❤️ Someone liked your note!</h1>
        </div>
        <div class="content">
            <p><strong>{{sender_name}}</strong> liked your note on Sonet.</p>
            <div class="note-preview">
                <p>"{{note_excerpt}}"</p>
            </div>
            <a href="{{note_url}}" class="btn">View on Sonet</a>
            <p>Keep sharing great content!</p>
        </div>
        <div class="footer">
            <p>This email was sent by Sonet. <a href="{{unsubscribe_url}}">Unsubscribe</a></p>
        </div>
    </div>
</body>
</html>
        )";
    }
    
    std::string create_comment_html_template() {
        return R"(
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{sender_name}} commented on your note</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px 20px; text-align: center; }
        .content { padding: 30px 20px; }
        .comment { background: #f8f9fa; border-left: 4px solid #28a745; padding: 15px; margin: 20px 0; border-radius: 4px; }
        .btn { display: inline-block; padding: 12px 24px; background: #28a745; color: white; text-decoration: none; border-radius: 6px; margin: 20px 0; }
        .footer { padding: 20px; text-align: center; font-size: 12px; color: #666; border-top: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>💬 New comment on your note!</h1>
        </div>
        <div class="content">
            <p><strong>{{sender_name}}</strong> commented on your note:</p>
            <div class="comment">
                <p>"{{comment_text}}"</p>
            </div>
            <a href="{{note_url}}" class="btn">View Conversation</a>
            <p>Join the discussion!</p>
        </div>
        <div class="footer">
            <p>This email was sent by Sonet. <a href="{{unsubscribe_url}}">Unsubscribe</a></p>
        </div>
    </div>
</body>
</html>
        )";
    }
    
    std::string create_follow_html_template() {
        return R"(
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{sender_name}} is now following you</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px 20px; text-align: center; }
        .content { padding: 30px 20px; text-align: center; }
        .btn { display: inline-block; padding: 12px 24px; background: #667eea; color: white; text-decoration: none; border-radius: 6px; margin: 20px 0; }
        .footer { padding: 20px; text-align: center; font-size: 12px; color: #666; border-top: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎉 New follower!</h1>
        </div>
        <div class="content">
            <p><strong>{{sender_name}}</strong> started following you on Sonet!</p>
            <p>You're building a great community. Keep sharing!</p>
            <a href="{{sender_profile_url}}" class="btn">View Their Profile</a>
        </div>
        <div class="footer">
            <p>This email was sent by Sonet. <a href="{{unsubscribe_url}}">Unsubscribe</a></p>
        </div>
    </div>
</body>
</html>
        )";
    }
    
    std::string create_mention_html_template() {
        return R"(
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{sender_name}} mentioned you</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); color: white; padding: 30px 20px; text-align: center; }
        .content { padding: 30px 20px; }
        .mention { background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 20px 0; border-radius: 4px; }
        .btn { display: inline-block; padding: 12px 24px; background: #ff6b6b; color: white; text-decoration: none; border-radius: 6px; margin: 20px 0; }
        .footer { padding: 20px; text-align: center; font-size: 12px; color: #666; border-top: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔔 You were mentioned!</h1>
        </div>
        <div class="content">
            <p><strong>{{sender_name}}</strong> mentioned you in a note:</p>
            <div class="mention">
                <p>"{{note_text}}"</p>
            </div>
            <a href="{{note_url}}" class="btn">View Note</a>
            <p>See what they said about you!</p>
        </div>
        <div class="footer">
            <p>This email was sent by Sonet. <a href="{{unsubscribe_url}}">Unsubscribe</a></p>
        </div>
    </div>
</body>
</html>
        )";
    }
    
    std::string create_renote_html_template() {
        return R"(
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{sender_name}} renoted your note</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #00d2ff 0%, #3a7bd5 100%); color: white; padding: 30px 20px; text-align: center; }
        .content { padding: 30px 20px; }
        .note-preview { background: #e3f2fd; border-left: 4px solid #2196f3; padding: 15px; margin: 20px 0; border-radius: 4px; }
        .btn { display: inline-block; padding: 12px 24px; background: #2196f3; color: white; text-decoration: none; border-radius: 6px; margin: 20px 0; }
        .footer { padding: 20px; text-align: center; font-size: 12px; color: #666; border-top: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔄 Your note was renoted!</h1>
        </div>
        <div class="content">
            <p><strong>{{sender_name}}</strong> renoted your note:</p>
            <div class="note-preview">
                <p>"{{note_excerpt}}"</p>
            </div>
            <a href="{{note_url}}" class="btn">View on Sonet</a>
            <p>Your content is being shared!</p>
        </div>
        <div class="footer">
            <p>This email was sent by Sonet. <a href="{{unsubscribe_url}}">Unsubscribe</a></p>
        </div>
    </div>
</body>
</html>
        )";
    }
    
    std::string create_dm_html_template() {
        return R"(
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>New message from {{sender_name}}</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #9c27b0 0%, #673ab7 100%); color: white; padding: 30px 20px; text-align: center; }
        .content { padding: 30px 20px; text-align: center; }
        .btn { display: inline-block; padding: 12px 24px; background: #9c27b0; color: white; text-decoration: none; border-radius: 6px; margin: 20px 0; }
        .footer { padding: 20px; text-align: center; font-size: 12px; color: #666; border-top: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>✉️ New message!</h1>
        </div>
        <div class="content">
            <p>You have a new message from <strong>{{sender_name}}</strong> on Sonet.</p>
            <a href="{{messages_url}}" class="btn">Read Message</a>
            <p>Stay connected with your community!</p>
        </div>
        <div class="footer">
            <p>This email was sent by Sonet. <a href="{{unsubscribe_url}}">Unsubscribe</a></p>
        </div>
    </div>
</body>
</html>
        )";
    }
};

SMTPEmailChannel::SMTPEmailChannel(const Config& config)
    : pimpl_(std::make_unique<Impl>(config)) {
}

SMTPEmailChannel::~SMTPEmailChannel() = default;

std::future<EmailDeliveryResult> SMTPEmailChannel::send_notification_email(
    const models::Notification& notification,
    const models::NotificationPreferences& user_preferences) {
    
    auto promise = std::make_shared<std::promise<EmailDeliveryResult>>();
    auto future = promise->get_future();
    
    // Check rate limits
    {
        std::lock_guard<std::mutex> lock(pimpl_->rate_limit_mutex);
        auto now = std::chrono::system_clock::now();
        
        if (now >= pimpl_->minute_reset) {
            pimpl_->emails_this_minute = 0;
            pimpl_->minute_reset = now + std::chrono::minutes(1);
        }
        
        if (now >= pimpl_->hour_reset) {
            pimpl_->emails_this_hour = 0;
            pimpl_->hour_reset = now + std::chrono::hours(1);
        }
        
        if (pimpl_->emails_this_minute >= pimpl_->config.max_emails_per_minute ||
            pimpl_->emails_this_hour >= pimpl_->config.max_emails_per_hour) {
            EmailDeliveryResult result;
            result.success = false;
            result.error_message = "Rate limit exceeded";
            result.sent_at = now;
            promise->set_value(result);
            return future;
        }
        
        pimpl_->emails_this_minute++;
        pimpl_->emails_this_hour++;
    }
    
    // Process in background thread
    std::thread([this, notification, user_preferences, promise]() {
        EmailDeliveryResult result;
        auto start_time = std::chrono::high_resolution_clock::now();
        
        try {
            // Get template for notification type
            std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
            auto template_it = pimpl_->templates.find(notification.type);
            if (template_it == pimpl_->templates.end()) {
                result.success = false;
                result.error_message = "No template found for notification type";
                result.sent_at = std::chrono::system_clock::now();
                promise->set_value(result);
                return;
            }
            
            const auto& email_template = template_it->second;
            
            // Extract variables from notification
            auto variables = extract_template_variables(notification);
            variables["unsubscribe_url"] = "https://sonet.app/settings/notifications";
            
            // Render email content
            std::string subject = replace_template_variables(email_template.subject_template, variables);
            std::string html_content = replace_template_variables(email_template.html_template, variables);
            std::string text_content = replace_template_variables(email_template.text_template, variables);
            
            // Send email
            bool send_success = send_raw_email(user_preferences.email, subject, html_content, text_content);
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            result.success = send_success;
            result.sent_at = std::chrono::system_clock::now();
            result.delivery_time = duration;
            
            if (send_success) {
                result.message_id = "msg_" + notification.id; // Simplified message ID
                track_delivery_success();
            } else {
                result.error_message = "SMTP delivery failed";
                track_delivery_failure(result.error_message);
            }
            
        } catch (const std::exception& e) {
            result.success = false;
            result.error_message = e.what();
            result.sent_at = std::chrono::system_clock::now();
            track_delivery_failure(e.what());
        }
        
        promise->set_value(result);
    }).detach();
    
    return future;
}

std::future<std::vector<EmailDeliveryResult>> SMTPEmailChannel::send_batch_email(
    const std::vector<models::Notification>& notifications,
    const std::unordered_map<std::string, models::NotificationPreferences>& user_preferences) {
    
    auto promise = std::make_shared<std::promise<std::vector<EmailDeliveryResult>>>();
    auto future = promise->get_future();
    
    std::thread([this, notifications, user_preferences, promise]() {
        std::vector<EmailDeliveryResult> results;
        results.reserve(notifications.size());
        
        for (const auto& notification : notifications) {
            auto pref_it = user_preferences.find(notification.user_id);
            if (pref_it != user_preferences.end()) {
                auto email_future = send_notification_email(notification, pref_it->second);
                results.push_back(email_future.get());
            } else {
                EmailDeliveryResult result;
                result.success = false;
                result.error_message = "User preferences not found";
                result.sent_at = std::chrono::system_clock::now();
                results.push_back(result);
            }
        }
        
        promise->set_value(results);
    }).detach();
    
    return future;
}

bool SMTPEmailChannel::register_template(models::NotificationType type, const EmailTemplate& template) {
    if (!validate_template(template)) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
    pimpl_->templates[type] = template;
    return true;
}

bool SMTPEmailChannel::update_template(models::NotificationType type, const EmailTemplate& template) {
    return register_template(type, template);
}

bool SMTPEmailChannel::remove_template(models::NotificationType type) {
    std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
    return pimpl_->templates.erase(type) > 0;
}

std::optional<EmailTemplate> SMTPEmailChannel::get_template(models::NotificationType type) const {
    std::lock_guard<std::mutex> lock(pimpl_->templates_mutex);
    auto it = pimpl_->templates.find(type);
    if (it != pimpl_->templates.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::string SMTPEmailChannel::render_email_html(const models::Notification& notification,
                                               const EmailTemplate& template) const {
    auto variables = extract_template_variables(notification);
    return replace_template_variables(template.html_template, variables);
}

std::string SMTPEmailChannel::render_email_text(const models::Notification& notification,
                                               const EmailTemplate& template) const {
    auto variables = extract_template_variables(notification);
    return replace_template_variables(template.text_template, variables);
}

std::string SMTPEmailChannel::render_subject(const models::Notification& notification,
                                            const EmailTemplate& template) const {
    auto variables = extract_template_variables(notification);
    return replace_template_variables(template.subject_template, variables);
}

bool SMTPEmailChannel::validate_email_address(const std::string& email) const {
    // Simple email validation regex
    const std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
    return std::regex_match(email, email_regex);
}

bool SMTPEmailChannel::validate_template(const EmailTemplate& template) const {
    return !template.subject_template.empty() && 
           !template.html_template.empty() && 
           !template.text_template.empty();
}

nlohmann::json SMTPEmailChannel::get_delivery_stats() const {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    
    auto now = std::chrono::system_clock::now();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - pimpl_->stats_start).count();
    
    return nlohmann::json{
        {"emails_sent", pimpl_->emails_sent.load()},
        {"emails_failed", pimpl_->emails_failed.load()},
        {"total_attempts", pimpl_->total_attempts.load()},
        {"success_rate", pimpl_->total_attempts.load() > 0 ? 
            static_cast<double>(pimpl_->emails_sent.load()) / pimpl_->total_attempts.load() : 0.0},
        {"uptime_seconds", uptime},
        {"emails_per_minute", pimpl_->emails_this_minute.load()},
        {"emails_per_hour", pimpl_->emails_this_hour.load()}
    };
}

nlohmann::json SMTPEmailChannel::get_health_status() const {
    return nlohmann::json{
        {"status", "healthy"},
        {"smtp_host", pimpl_->config.smtp_host},
        {"smtp_port", pimpl_->config.smtp_port},
        {"connection_timeout", pimpl_->config.connection_timeout.count()},
        {"templates_loaded", pimpl_->templates.size()}
    };
}

void SMTPEmailChannel::reset_stats() {
    std::lock_guard<std::mutex> lock(pimpl_->stats_mutex);
    pimpl_->emails_sent = 0;
    pimpl_->emails_failed = 0;
    pimpl_->total_attempts = 0;
    pimpl_->stats_start = std::chrono::system_clock::now();
}

std::string SMTPEmailChannel::replace_template_variables(
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

std::unordered_map<std::string, std::string> SMTPEmailChannel::extract_template_variables(
    const models::Notification& notification) const {
    
    std::unordered_map<std::string, std::string> variables;
    
    // Extract basic notification data
    variables["notification_id"] = notification.id;
    variables["user_id"] = notification.user_id;
    variables["sender_id"] = notification.sender_id;
    
    // Extract template data
    for (const auto& [key, value] : notification.template_data.items()) {
        if (value.is_string()) {
            variables[key] = value.get<std::string>();
        } else {
            variables[key] = value.dump();
        }
    }
    
    // Add default Sonet URLs
    variables["sonet_url"] = "https://sonet.app";
    variables["settings_url"] = "https://sonet.app/settings";
    variables["profile_url"] = "https://sonet.app/profile/" + notification.user_id;
    
    return variables;
}

bool SMTPEmailChannel::send_raw_email(const std::string& recipient, const std::string& subject,
                                     const std::string& html_content, const std::string& text_content) const {
    // For now, I'll implement a simple simulation
    // In a real implementation, this would use libcurl or another SMTP library
    
    track_delivery_attempt();
    
    // Simulate network delay
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Simulate 95% success rate
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_real_distribution<> dis(0.0, 1.0);
    
    return dis(gen) < 0.95;
}

void SMTPEmailChannel::track_delivery_attempt() {
    pimpl_->total_attempts++;
}

void SMTPEmailChannel::track_delivery_success() {
    pimpl_->emails_sent++;
}

void SMTPEmailChannel::track_delivery_failure(const std::string& error) {
    pimpl_->emails_failed++;
}

// Factory implementations
std::unique_ptr<EmailChannel> EmailChannelFactory::create(ChannelType type, const nlohmann::json& config) {
    switch (type) {
        case ChannelType::SMTP: {
            SMTPEmailChannel::Config smtp_config;
            smtp_config.smtp_host = config.value("smtp_host", "localhost");
            smtp_config.smtp_port = config.value("smtp_port", 587);
            smtp_config.username = config.value("username", "");
            smtp_config.password = config.value("password", "");
            return create_smtp(smtp_config);
        }
        case ChannelType::MOCK:
            return create_mock();
        default:
            return nullptr;
    }
}

std::unique_ptr<EmailChannel> EmailChannelFactory::create_smtp(const SMTPEmailChannel::Config& config) {
    return std::make_unique<SMTPEmailChannel>(config);
}

std::unique_ptr<EmailChannel> EmailChannelFactory::create_mock() {
    // Return a mock implementation for testing
    SMTPEmailChannel::Config config;
    config.smtp_host = "mock.smtp.server";
    return std::make_unique<SMTPEmailChannel>(config);
}

EmailTemplate EmailChannelFactory::create_like_template() {
    EmailTemplate template;
    template.subject_template = "{{sender_name}} liked your note";
    template.html_template = "Basic like notification HTML";
    template.text_template = "{{sender_name}} liked your note: {{note_excerpt}}";
    template.required_variables = {"sender_name", "note_excerpt"};
    return template;
}

EmailTemplate EmailChannelFactory::create_comment_template() {
    EmailTemplate template;
    template.subject_template = "{{sender_name}} commented on your note";
    template.html_template = "Basic comment notification HTML";
    template.text_template = "{{sender_name}} commented: {{comment_text}}";
    template.required_variables = {"sender_name", "comment_text"};
    return template;
}

} // namespace channels
} // namespace notification_service
} // namespace sonet