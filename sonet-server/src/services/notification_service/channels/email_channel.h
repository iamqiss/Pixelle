/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This is the email channel for sending notification emails. I built this to
 * send beautiful, responsive emails that work great on both desktop and mobile.
 * The templates are designed to engage users without being spammy.
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
 * Email template data for rendering notifications
 * I use this to create personalized, engaging email content
 */
struct EmailTemplate {
    std::string subject_template;
    std::string html_template;
    std::string text_template;
    std::string sender_name = "Sonet";
    std::string sender_email = "notifications@sonet.app";
    std::string reply_to_email = "noreply@sonet.app";
    bool include_unsubscribe_link = true;
    bool include_branding = true;
    std::unordered_map<std::string, std::string> custom_headers;
    
    // Template variables that get replaced
    std::vector<std::string> required_variables;
    std::unordered_map<std::string, std::string> default_variables;
};

/**
 * Email delivery result for tracking success/failure
 * I track these details to improve delivery rates
 */
struct EmailDeliveryResult {
    bool success = false;
    std::string message_id;
    std::string error_message;
    std::chrono::system_clock::time_point sent_at;
    std::chrono::milliseconds delivery_time{0};
    std::string provider_response;
    int retry_count = 0;
    
    nlohmann::json to_json() const {
        return nlohmann::json{
            {"success", success},
            {"message_id", message_id},
            {"error_message", error_message},
            {"sent_at", std::chrono::duration_cast<std::chrono::seconds>(
                sent_at.time_since_epoch()).count()},
            {"delivery_time_ms", delivery_time.count()},
            {"provider_response", provider_response},
            {"retry_count", retry_count}
        };
    }
};

/**
 * Email channel interface for sending notification emails
 * I keep this abstract so we can use different email providers
 */
class EmailChannel {
public:
    virtual ~EmailChannel() = default;
    
    // Core sending methods
    virtual std::future<EmailDeliveryResult> send_notification_email(
        const models::Notification& notification,
        const models::NotificationPreferences& user_preferences) = 0;
    
    virtual std::future<std::vector<EmailDeliveryResult>> send_batch_email(
        const std::vector<models::Notification>& notifications,
        const std::unordered_map<std::string, models::NotificationPreferences>& user_preferences) = 0;
    
    // Template management
    virtual bool register_template(models::NotificationType type, const EmailTemplate& tmpl) = 0;
    virtual bool update_template(models::NotificationType type, const EmailTemplate& tmpl) = 0;
    virtual bool remove_template(models::NotificationType type) = 0;
    virtual std::optional<EmailTemplate> get_template(models::NotificationType type) const = 0;
    
    // Rendering and preview
    virtual std::string render_email_html(const models::Notification& notification,
                                         const EmailTemplate& tmpl) const = 0;
    virtual std::string render_email_text(const models::Notification& notification,
                                         const EmailTemplate& tmpl) const = 0;
    virtual std::string render_subject(const models::Notification& notification,
                                     const EmailTemplate& tmpl) const = 0;
    
    // Testing and validation
    virtual std::future<bool> send_test_email(const std::string& recipient,
                                             const std::string& subject,
                                             const std::string& content) = 0;
    virtual bool validate_email_address(const std::string& email) const = 0;
    virtual bool validate_template(const EmailTemplate& tmpl) const = 0;
    
    // Analytics and monitoring
    virtual nlohmann::json get_delivery_stats() const = 0;
    virtual nlohmann::json get_health_status() const = 0;
    virtual void reset_stats() = 0;
    
    // Configuration
    virtual bool configure(const nlohmann::json& config) = 0;
    virtual nlohmann::json get_config() const = 0;
};

/**
 * SMTP email channel implementation
 * I built this to work with most email providers like SendGrid, Mailgun, etc.
 */
class SMTPEmailChannel : public EmailChannel {
public:
    struct Config {
        std::string smtp_host;
        int smtp_port = 587;
        bool use_tls = true;
        bool use_ssl = false;
        std::string username;
        std::string password;
        std::string sender_email = "notifications@sonet.app";
        std::string sender_name = "Sonet";
        
        // Connection settings
        std::chrono::seconds connection_timeout{30};
        std::chrono::seconds send_timeout{60};
        int max_connections = 10;
        int retry_attempts = 3;
        std::chrono::seconds retry_delay{5};
        
        // Rate limiting
        int max_emails_per_minute = 100;
        int max_emails_per_hour = 1000;
        
        // Content settings
        std::string charset = "UTF-8";
        std::string encoding = "quoted-printable";
        bool include_message_id = true;
        bool include_date_header = true;
        
        // Security
        bool verify_ssl_certificate = true;
        std::string ssl_ca_file;
        std::string ssl_cert_file;
        std::string ssl_key_file;
    };
    
    explicit SMTPEmailChannel(const Config& config);
    virtual ~SMTPEmailChannel();
    
    // Implement EmailChannel interface
    std::future<EmailDeliveryResult> send_notification_email(
        const models::Notification& notification,
        const models::NotificationPreferences& user_preferences) override;
    
    std::future<std::vector<EmailDeliveryResult>> send_batch_email(
        const std::vector<models::Notification>& notifications,
        const std::unordered_map<std::string, models::NotificationPreferences>& user_preferences) override;
    
    bool register_template(models::NotificationType type, const EmailTemplate& tmpl) override;
    bool update_template(models::NotificationType type, const EmailTemplate& tmpl) override;
    bool remove_template(models::NotificationType type) override;
    std::optional<EmailTemplate> get_template(models::NotificationType type) const override;
    
    std::string render_email_html(const models::Notification& notification,
                                 const EmailTemplate& tmpl) const override;
    std::string render_email_text(const models::Notification& notification,
                                 const EmailTemplate& tmpl) const override;
    std::string render_subject(const models::Notification& notification,
                              const EmailTemplate& tmpl) const override;
    
    std::future<bool> send_test_email(const std::string& recipient,
                                     const std::string& subject,
                                     const std::string& content) override;
    bool validate_email_address(const std::string& email) const override;
    bool validate_template(const EmailTemplate& tmpl) const override;
    
    nlohmann::json get_delivery_stats() const override;
    nlohmann::json get_health_status() const override;
    void reset_stats() override;
    
    bool configure(const nlohmann::json& config) override;
    nlohmann::json get_config() const override;
    
    // SMTP-specific methods
    bool test_connection() const;
    void reload_templates();
    void clear_template_cache();
    
private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    // Internal helper methods
    std::string build_email_message(const models::Notification& notification,
                                   const EmailTemplate& tmpl,
                                   const models::NotificationPreferences& preferences) const;
    std::string replace_template_variables(const std::string& tmpl,
                                         const std::unordered_map<std::string, std::string>& variables) const;
    std::unordered_map<std::string, std::string> extract_template_variables(
        const models::Notification& notification) const;
    bool send_raw_email(const std::string& recipient, const std::string& subject,
                       const std::string& html_content, const std::string& text_content) const;
    void track_delivery_attempt();
    void track_delivery_success();
    void track_delivery_failure(const std::string& error);
    void cleanup_old_stats();
};

/**
 * Factory for creating email channels
 * I use this to support different email providers easily
 */
class EmailChannelFactory {
public:
    enum class ChannelType {
        SMTP,
        SENDGRID,
        MAILGUN,
        AWS_SES,
        MOCK // For testing
    };
    
    static std::unique_ptr<EmailChannel> create(ChannelType type, const nlohmann::json& config);
    static std::unique_ptr<EmailChannel> create_smtp(const SMTPEmailChannel::Config& config);
    static std::unique_ptr<EmailChannel> create_mock(); // For testing
    
    // Template helpers
    static EmailTemplate create_like_template();
    static EmailTemplate create_comment_template();
    static EmailTemplate create_follow_template();
    static EmailTemplate create_mention_template();
    static EmailTemplate create_renote_template();
    static EmailTemplate create_dm_template();
    static EmailTemplate create_digest_template();
    static std::unordered_map<models::NotificationType, EmailTemplate> create_default_templates();
};

} // namespace channels
} // namespace notification_service
} // namespace sonet