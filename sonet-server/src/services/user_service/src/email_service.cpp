/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "../include/email_service.h"
#include <curl/curl.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <regex>
#include <fstream>
#include <sstream>

namespace sonet::user::email {

// SMTP implementation using libcurl
class SMTPSender {
public:
    struct Config {
        std::string host;
        int port = 587;
        std::string username;
        std::string password;
        bool use_tls = true;
    };

    SMTPSender(const Config& config) : config_(config) {
        curl_global_init(CURL_GLOBAL_DEFAULT);
    }

    ~SMTPSender() {
        curl_global_cleanup();
    }

    bool send_email(const EmailMessage& message) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            spdlog::error("Failed to initialize CURL for SMTP");
            return false;
        }

        CURLcode res = CURLE_OK;
        
        try {
            // Set SMTP server and port
            std::string url = "smtps://" + config_.host + ":" + std::to_string(config_.port);
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            
            // Authentication
            curl_easy_setopt(curl, CURLOPT_USERNAME, config_.username.c_str());
            curl_easy_setopt(curl, CURLOPT_PASSWORD, config_.password.c_str());
            
            // Use TLS
            if (config_.use_tls) {
                curl_easy_setopt(curl, CURLOPT_USE_SSL, CURLUSESSL_ALL);
            }
            
            // Set sender
            curl_easy_setopt(curl, CURLOPT_MAIL_FROM, message.from_email.c_str());
            
            // Set recipient
            struct curl_slist* recipients = nullptr;
            recipients = curl_slist_append(recipients, message.to_email.c_str());
            curl_easy_setopt(curl, CURLOPT_MAIL_RCPT, recipients);
            
            // Email content
            std::string email_content = build_email_content(message);
            curl_easy_setopt(curl, CURLOPT_READDATA, &email_content);
            curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
            curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
            
            // Perform the send
            res = curl_easy_perform(curl);
            
            curl_slist_free_all(recipients);
            curl_easy_cleanup(curl);
            
            if (res != CURLE_OK) {
                spdlog::error("SMTP send failed: {}", curl_easy_strerror(res));
                return false;
            }
            
            spdlog::info("Email sent successfully to {}", message.to_email);
            return true;
            
        } catch (const std::exception& e) {
            spdlog::error("SMTP send exception: {}", e.what());
            curl_easy_cleanup(curl);
            return false;
        }
    }

private:
    Config config_;
    
    static size_t read_callback(void* ptr, size_t size, size_t nmemb, void* userp) {
        std::string* email_content = static_cast<std::string*>(userp);
        size_t bytes_to_copy = std::min(size * nmemb, email_content->length());
        
        if (bytes_to_copy > 0) {
            memcpy(ptr, email_content->c_str(), bytes_to_copy);
            email_content->erase(0, bytes_to_copy);
        }
        
        return bytes_to_copy;
    }
    
    std::string build_email_content(const EmailMessage& message) {
        std::ostringstream content;
        
        content << "From: " << message.from_name << " <" << message.from_email << ">\r\n";
        content << "To: " << message.to_name << " <" << message.to_email << ">\r\n";
        content << "Subject: " << message.subject << "\r\n";
        content << "MIME-Version: 1.0\r\n";
        content << "Content-Type: multipart/alternative; boundary=\"boundary123\"\r\n";
        content << "\r\n";
        
        // Text part
        content << "--boundary123\r\n";
        content << "Content-Type: text/plain; charset=UTF-8\r\n";
        content << "\r\n";
        content << message.text_body << "\r\n";
        
        // HTML part
        content << "--boundary123\r\n";
        content << "Content-Type: text/html; charset=UTF-8\r\n";
        content << "\r\n";
        content << message.html_body << "\r\n";
        
        content << "--boundary123--\r\n";
        
        return content.str();
    }
};

// SendGrid implementation
class SendGridSender {
public:
    SendGridSender(const std::string& api_key) : api_key_(api_key) {}

    bool send_email(const EmailMessage& message) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            spdlog::error("Failed to initialize CURL for SendGrid");
            return false;
        }

        try {
            // Prepare JSON payload
            nlohmann::json payload;
            payload["personalizations"][0]["to"][0]["email"] = message.to_email;
            payload["personalizations"][0]["to"][0]["name"] = message.to_name;
            payload["personalizations"][0]["subject"] = message.subject;
            
            payload["from"]["email"] = message.from_email;
            payload["from"]["name"] = message.from_name;
            
            payload["content"][0]["type"] = "text/plain";
            payload["content"][0]["value"] = message.text_body;
            payload["content"][1]["type"] = "text/html";
            payload["content"][1]["value"] = message.html_body;
            
            std::string json_string = payload.dump();
            
            // Set headers
            struct curl_slist* headers = nullptr;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            std::string auth_header = "Authorization: Bearer " + api_key_;
            headers = curl_slist_append(headers, auth_header.c_str());
            
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curl, CURLOPT_URL, "https://api.sendgrid.com/v3/mail/send");
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_string.c_str());
            
            // Response handling
            std::string response;
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
            
            CURLcode res = curl_easy_perform(curl);
            long response_code;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
            
            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
            
            if (res != CURLE_OK || response_code >= 400) {
                spdlog::error("SendGrid send failed: {} (HTTP {})", curl_easy_strerror(res), response_code);
                return false;
            }
            
            spdlog::info("Email sent successfully via SendGrid to {}", message.to_email);
            return true;
            
        } catch (const std::exception& e) {
            spdlog::error("SendGrid send exception: {}", e.what());
            curl_easy_cleanup(curl);
            return false;
        }
    }

private:
    std::string api_key_;
    
    static size_t write_callback(void* contents, size_t size, size_t nmemb, std::string* userp) {
        userp->append((char*)contents, size * nmemb);
        return size * nmemb;
    }
};

// EmailService implementation
class EmailService::Impl {
public:
    Impl(EmailProvider provider) : provider_(provider), running_(false) {}
    
    ~Impl() {
        stop_queue_processor();
    }
    
    bool initialize(const std::map<std::string, std::string>& config) {
        try {
            switch (provider_) {
                case EmailProvider::SMTP: {
                    SMTPSender::Config smtp_config;
                    smtp_config.host = config.at("host");
                    smtp_config.port = std::stoi(config.at("port"));
                    smtp_config.username = config.at("username");
                    smtp_config.password = config.at("password");
                    smtp_config.use_tls = config.count("use_tls") ? config.at("use_tls") == "true" : true;
                    
                    smtp_sender_ = std::make_unique<SMTPSender>(smtp_config);
                    break;
                }
                case EmailProvider::SENDGRID: {
                    sendgrid_sender_ = std::make_unique<SendGridSender>(config.at("api_key"));
                    break;
                }
                default:
                    spdlog::error("Unsupported email provider");
                    return false;
            }
            
            load_default_templates();
            start_queue_processor();
            
            spdlog::info("Email service initialized successfully");
            return true;
            
        } catch (const std::exception& e) {
            spdlog::error("Failed to initialize email service: {}", e.what());
            return false;
        }
    }
    
    std::future<bool> send_verification_email(const std::string& email, const std::string& username, 
                                            const std::string& verification_token, const std::string& verification_url) {
        std::map<std::string, std::string> variables = {
            {"username", username},
            {"verification_url", verification_url},
            {"verification_token", verification_token}
        };
        
        return send_template_email("email_verification", email, username, variables);
    }
    
    std::future<bool> send_password_reset_email(const std::string& email, const std::string& username,
                                               const std::string& reset_token, const std::string& reset_url) {
        std::map<std::string, std::string> variables = {
            {"username", username},
            {"reset_url", reset_url},
            {"reset_token", reset_token}
        };
        
        return send_template_email("password_reset", email, username, variables);
    }
    
    std::future<bool> send_welcome_email(const std::string& email, const std::string& username) {
        std::map<std::string, std::string> variables = {
            {"username", username}
        };
        
        return send_template_email("welcome", email, username, variables);
    }
    
    std::future<bool> send_security_alert_email(const std::string& email, const std::string& username,
                                               const std::string& alert_type, const std::string& device_info,
                                               const std::string& location) {
        std::map<std::string, std::string> variables = {
            {"username", username},
            {"alert_type", alert_type},
            {"device_info", device_info},
            {"location", location},
            {"timestamp", std::to_string(std::time(nullptr))}
        };
        
        return send_template_email("security_alert", email, username, variables);
    }
    
    std::future<bool> send_email(const EmailMessage& message) {
        return std::async(std::launch::async, [this, message]() {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            email_queue_.push(message);
            queue_cv_.notify_one();
            return true;
        });
    }
    
    std::future<bool> send_template_email(const std::string& template_name, const std::string& to_email,
                                         const std::string& to_name, const std::map<std::string, std::string>& variables) {
        return std::async(std::launch::async, [this, template_name, to_email, to_name, variables]() {
            auto template_it = templates_.find(template_name);
            if (template_it == templates_.end()) {
                spdlog::error("Email template not found: {}", template_name);
                return false;
            }
            
            EmailMessage message;
            message.to_email = to_email;
            message.to_name = to_name;
            message.subject = render_template(template_it->second.subject, variables);
            message.html_body = render_template(template_it->second.html_body, variables);
            message.text_body = render_template(template_it->second.text_body, variables);
            
            std::lock_guard<std::mutex> lock(queue_mutex_);
            email_queue_.push(message);
            queue_cv_.notify_one();
            return true;
        });
    }
    
    void register_template(const std::string& template_name, const EmailTemplate& email_template) {
        std::lock_guard<std::mutex> lock(templates_mutex_);
        templates_[template_name] = email_template;
    }
    
    size_t get_queue_size() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return email_queue_.size();
    }
    
    bool is_healthy() const {
        return running_ && (provider_ == EmailProvider::SMTP ? smtp_sender_ != nullptr : sendgrid_sender_ != nullptr);
    }

private:
    EmailProvider provider_;
    std::unique_ptr<SMTPSender> smtp_sender_;
    std::unique_ptr<SendGridSender> sendgrid_sender_;
    
    std::map<std::string, EmailTemplate> templates_;
    mutable std::mutex templates_mutex_;
    
    std::queue<EmailMessage> email_queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::thread queue_processor_;
    std::atomic<bool> running_;
    
    void start_queue_processor() {
        running_ = true;
        queue_processor_ = std::thread([this]() {
            while (running_) {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cv_.wait(lock, [this]() { return !email_queue_.empty() || !running_; });
                
                if (!running_) break;
                
                EmailMessage message = email_queue_.front();
                email_queue_.pop();
                lock.unlock();
                
                // Send the email
                bool success = false;
                switch (provider_) {
                    case EmailProvider::SMTP:
                        if (smtp_sender_) success = smtp_sender_->send_email(message);
                        break;
                    case EmailProvider::SENDGRID:
                        if (sendgrid_sender_) success = sendgrid_sender_->send_email(message);
                        break;
                    default:
                        spdlog::error("Unsupported email provider for sending");
                        break;
                }
                
                if (!success) {
                    spdlog::error("Failed to send email to {}", message.to_email);
                }
                
                // Rate limiting - sleep for a bit between emails
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }
    
    void stop_queue_processor() {
        running_ = false;
        queue_cv_.notify_all();
        if (queue_processor_.joinable()) {
            queue_processor_.join();
        }
    }
    
    void load_default_templates() {
        // Email verification template
        EmailTemplate verification_template;
        verification_template.subject = "Verify your Sonet account";
        verification_template.html_body = R"(
            <html>
                <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                        <h2 style="color: #1DA1F2;">Welcome to Sonet, {{username}}!</h2>
                        <p>Thank you for joining Sonet. To complete your registration, please verify your email address by clicking the button below:</p>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="{{verification_url}}" style="background-color: #1DA1F2; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block;">Verify Email Address</a>
                        </div>
                        <p>If the button doesn't work, you can copy and paste this link into your browser:</p>
                        <p style="word-break: break-all; color: #666;">{{verification_url}}</p>
                        <p>This verification link will expire in 24 hours for security reasons.</p>
                        <p>If you didn't create this account, please ignore this email.</p>
                        <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
                        <p style="color: #666; font-size: 12px;">© 2025 Sonet. All rights reserved.</p>
                    </div>
                </body>
            </html>
        )";
        verification_template.text_body = R"(
            Welcome to Sonet, {{username}}!
            
            Thank you for joining Sonet. To complete your registration, please verify your email address by visiting:
            
            {{verification_url}}
            
            This verification link will expire in 24 hours for security reasons.
            
            If you didn't create this account, please ignore this email.
            
            © 2025 Sonet. All rights reserved.
        )";
        register_template("email_verification", verification_template);
        
        // Password reset template
        EmailTemplate reset_template;
        reset_template.subject = "Reset your Sonet password";
        reset_template.html_body = R"(
            <html>
                <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                        <h2 style="color: #1DA1F2;">Password Reset Request</h2>
                        <p>Hi {{username}},</p>
                        <p>We received a request to reset your password for your Sonet account. Click the button below to reset it:</p>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="{{reset_url}}" style="background-color: #E1306C; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block;">Reset Password</a>
                        </div>
                        <p>If the button doesn't work, you can copy and paste this link into your browser:</p>
                        <p style="word-break: break-all; color: #666;">{{reset_url}}</p>
                        <p>This password reset link will expire in 1 hour for security reasons.</p>
                        <p>If you didn't request this password reset, please ignore this email. Your password will remain unchanged.</p>
                        <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
                        <p style="color: #666; font-size: 12px;">© 2025 Sonet. All rights reserved.</p>
                    </div>
                </body>
            </html>
        )";
        register_template("password_reset", reset_template);
        
        // Welcome email template  
        EmailTemplate welcome_template;
        welcome_template.subject = "Welcome to Sonet!";
        welcome_template.html_body = R"(
            <html>
                <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                        <h2 style="color: #1DA1F2;">Welcome to Sonet, {{username}}! 🎉</h2>
                        <p>Your account has been successfully verified and you're now part of the Sonet community!</p>
                        <p>Here are some things you can do to get started:</p>
                        <ul>
                            <li>Complete your profile with a photo and bio</li>
                            <li>Find and follow friends</li>
                            <li>Share your first note with the world</li>
                            <li>Discover trending topics and conversations</li>
                        </ul>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="https://sonet.com/dashboard" style="background-color: #1DA1F2; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block;">Get Started</a>
                        </div>
                        <p>We're excited to see what you'll share!</p>
                        <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
                        <p style="color: #666; font-size: 12px;">© 2025 Sonet. All rights reserved.</p>
                    </div>
                </body>
            </html>
        )";
        register_template("welcome", welcome_template);
        
        // Security alert template
        EmailTemplate security_template;
        security_template.subject = "Security Alert - New Login to Your Account";
        security_template.html_body = R"(
            <html>
                <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                        <h2 style="color: #E1306C;">Security Alert</h2>
                        <p>Hi {{username}},</p>
                        <p>We detected a new login to your Sonet account:</p>
                        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0;">
                            <strong>Alert Type:</strong> {{alert_type}}<br>
                            <strong>Device:</strong> {{device_info}}<br>
                            <strong>Location:</strong> {{location}}<br>
                            <strong>Time:</strong> {{timestamp}}
                        </div>
                        <p>If this was you, you can ignore this email. If you don't recognize this activity, please secure your account immediately:</p>
                        <ul>
                            <li>Change your password</li>
                            <li>Review your active sessions</li>
                            <li>Enable two-factor authentication</li>
                        </ul>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="https://sonet.com/security" style="background-color: #E1306C; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block;">Secure My Account</a>
                        </div>
                        <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
                        <p style="color: #666; font-size: 12px;">© 2025 Sonet. All rights reserved.</p>
                    </div>
                </body>
            </html>
        )";
        register_template("security_alert", security_template);
    }
};

// EmailService public methods
EmailService::EmailService(EmailProvider provider) : pimpl_(std::make_unique<Impl>(provider)) {}
EmailService::~EmailService() = default;

bool EmailService::initialize(const std::map<std::string, std::string>& config) {
    return pimpl_->initialize(config);
}

std::future<bool> EmailService::send_verification_email(const std::string& email, const std::string& username,
                                                       const std::string& verification_token, const std::string& verification_url) {
    return pimpl_->send_verification_email(email, username, verification_token, verification_url);
}

std::future<bool> EmailService::send_password_reset_email(const std::string& email, const std::string& username,
                                                         const std::string& reset_token, const std::string& reset_url) {
    return pimpl_->send_password_reset_email(email, username, reset_token, reset_url);
}

std::future<bool> EmailService::send_welcome_email(const std::string& email, const std::string& username) {
    return pimpl_->send_welcome_email(email, username);
}

std::future<bool> EmailService::send_security_alert_email(const std::string& email, const std::string& username,
                                                         const std::string& alert_type, const std::string& device_info,
                                                         const std::string& location) {
    return pimpl_->send_security_alert_email(email, username, alert_type, device_info, location);
}

std::future<bool> EmailService::send_email(const EmailMessage& message) {
    return pimpl_->send_email(message);
}

std::future<bool> EmailService::send_template_email(const std::string& template_name, const std::string& to_email,
                                                   const std::string& to_name, const std::map<std::string, std::string>& variables) {
    return pimpl_->send_template_email(template_name, to_email, to_name, variables);
}

void EmailService::register_template(const std::string& template_name, const EmailTemplate& email_template) {
    pimpl_->register_template(template_name, email_template);
}

size_t EmailService::get_queue_size() const {
    return pimpl_->get_queue_size();
}

bool EmailService::is_healthy() const {
    return pimpl_->is_healthy();
}

// Utility functions
std::string render_template(const std::string& template_str, const std::map<std::string, std::string>& variables) {
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

bool is_valid_email_address(const std::string& email) {
    const std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
    return std::regex_match(email, email_regex);
}

std::string generate_verification_url(const std::string& base_url, const std::string& token) {
    return base_url + "/verify-email?token=" + token;
}

std::string generate_reset_url(const std::string& base_url, const std::string& token) {
    return base_url + "/reset-password?token=" + token;
}

} // namespace sonet::user::email
