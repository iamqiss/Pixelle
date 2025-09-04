/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <memory>
#include <future>
#include <functional>
#include <map>

namespace sonet::user::email {

struct EmailTemplate {
    std::string subject;
    std::string html_body;
    std::string text_body;
    std::map<std::string, std::string> variables;
};

struct EmailMessage {
    std::string to_email;
    std::string to_name;
    std::string subject;
    std::string html_body;
    std::string text_body;
    std::string from_email = "noreply@sonet.com";
    std::string from_name = "Sonet";
    int priority = 1; // 1=high, 2=normal, 3=low
};

enum class EmailProvider {
    SMTP,
    SENDGRID,
    AWS_SES,
    MAILGUN
};

class EmailService {
public:
    EmailService(EmailProvider provider = EmailProvider::SMTP);
    ~EmailService();

    // Initialize email service with configuration
    bool initialize(const std::map<std::string, std::string>& config);
    
    // Send verification email
    std::future<bool> send_verification_email(
        const std::string& email,
        const std::string& username,
        const std::string& verification_token,
        const std::string& verification_url);
    
    // Send password reset email
    std::future<bool> send_password_reset_email(
        const std::string& email,
        const std::string& username,
        const std::string& reset_token,
        const std::string& reset_url);
    
    // Send welcome email
    std::future<bool> send_welcome_email(
        const std::string& email,
        const std::string& username);
    
    // Send security alert email
    std::future<bool> send_security_alert_email(
        const std::string& email,
        const std::string& username,
        const std::string& alert_type,
        const std::string& device_info,
        const std::string& location);
    
    // Send notification email
    std::future<bool> send_notification_email(
        const std::string& email,
        const std::string& username,
        const std::string& notification_type,
        const std::map<std::string, std::string>& data);
    
    // Generic email sending
    std::future<bool> send_email(const EmailMessage& message);
    
    // Template management
    void register_template(const std::string& template_name, const EmailTemplate& email_template);
    std::future<bool> send_template_email(
        const std::string& template_name,
        const std::string& to_email,
        const std::string& to_name,
        const std::map<std::string, std::string>& variables);
    
    // Configuration
    void set_smtp_config(const std::string& host, int port, const std::string& username, 
                        const std::string& password, bool use_tls = true);
    void set_sendgrid_config(const std::string& api_key);
    void set_aws_ses_config(const std::string& access_key, const std::string& secret_key, 
                           const std::string& region);
    void set_mailgun_config(const std::string& api_key, const std::string& domain);
    
    // Status and health
    bool is_healthy() const;
    std::string get_status() const;
    
    // Rate limiting and queue management
    void set_rate_limit(int emails_per_minute);
    size_t get_queue_size() const;
    void flush_queue();

private:
    class Impl;
    std::unique_ptr<Impl> pimpl_;
};

// Email template builder utility
class EmailTemplateBuilder {
public:
    EmailTemplateBuilder& set_subject(const std::string& subject);
    EmailTemplateBuilder& set_html_body(const std::string& html_body);
    EmailTemplateBuilder& set_text_body(const std::string& text_body);
    EmailTemplateBuilder& add_variable(const std::string& key, const std::string& default_value);
    
    EmailTemplate build() const;

private:
    EmailTemplate template_;
};

// Utility functions
std::string render_template(const std::string& template_str, 
                           const std::map<std::string, std::string>& variables);
bool is_valid_email_address(const std::string& email);
std::string generate_verification_url(const std::string& base_url, const std::string& token);
std::string generate_reset_url(const std::string& base_url, const std::string& token);

} // namespace sonet::user::email
