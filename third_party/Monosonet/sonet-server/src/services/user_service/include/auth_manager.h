/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "user_types.h"
#include "password_manager.h"
#include "jwt_manager.h"
#include "session_manager.h"
#include "rate_limiter.h"
#include <memory>
#include <optional>

namespace sonet::user {

/**
 * Authentication Manager - The central hub for all authentication
 * 
 * This is where all the magic happens. I've orchestrated all the components
 * to work together seamlessly. Security is my top priority, but I also
 * want this to be fast and user-friendly.
 */
class AuthManager {
public:
    AuthManager(
        std::shared_ptr<PasswordManager> password_manager,
        std::shared_ptr<JWTManager> jwt_manager,
        std::shared_ptr<SessionManager> session_manager,
        std::shared_ptr<RateLimiter> rate_limiter
    );
    
    ~AuthManager() = default;

    // Primary authentication methods
    AuthResult authenticate_user(const AuthCredentials& credentials, UserSession& session);
    AuthResult register_user(const RegistrationRequest& request, User& user);
    
    // Token-based authentication
    std::optional<User> authenticate_token(const std::string& token);
    bool refresh_authentication(const std::string& refresh_token, std::string& new_access_token);
    
    // Session management
    bool create_session(const User& user, const std::string& device_info, const std::string& ip_address, UserSession& session);
    bool terminate_session(const std::string& session_id);
    bool terminate_all_sessions(const std::string& user_id);
    std::vector<UserSession> get_active_sessions(const std::string& user_id);
    
    // Two-factor authentication
    bool setup_2fa(const std::string& user_id, TwoFactorAuth& tfa_config);
    bool verify_2fa(const std::string& user_id, const std::string& code);
    bool disable_2fa(const std::string& user_id, const std::string& password);
    std::vector<std::string> generate_backup_codes(const std::string& user_id);
    
    // Password operations
    bool change_password(const std::string& user_id, const std::string& old_password, const std::string& new_password);
    bool reset_password(const std::string& email, std::string& reset_token);
    bool confirm_password_reset(const std::string& reset_token, const std::string& new_password);
    
    // Account verification
    bool send_verification_email(const std::string& user_id);
    bool verify_email(const std::string& verification_token);
    
    // Security features
    bool is_account_locked(const std::string& user_id);
    bool unlock_account(const std::string& user_id);
    void lock_account(const std::string& user_id, const std::string& reason);
    
    // Suspicious activity detection
    bool is_login_suspicious(const AuthCredentials& credentials, const User& user);
    void log_security_event(const std::string& user_id, SecurityEventType type, const std::string& details);
    std::vector<SecurityEvent> get_security_events(const std::string& user_id, size_t limit = 50);
    
    // Administrative functions
    bool force_password_reset(const std::string& user_id);
    bool suspend_account(const std::string& user_id, const std::string& reason);
    bool unsuspend_account(const std::string& user_id);

private:
    std::shared_ptr<PasswordManager> password_manager_;
    std::shared_ptr<JWTManager> jwt_manager_;
    std::shared_ptr<SessionManager> session_manager_;
    std::shared_ptr<RateLimiter> rate_limiter_;
    
    // Configuration
    RateLimitConfig rate_limit_config_;
    
    // Helper methods for authentication flow
    bool validate_credentials(const AuthCredentials& credentials);
    bool check_account_status(const User& user);
    bool verify_user_password(const std::string& user_id, const std::string& password);
    
    // Security helpers
    bool is_ip_blocked(const std::string& ip_address);
    bool is_device_trusted(const std::string& user_id, const std::string& device_id);
    std::string generate_device_fingerprint(const std::string& user_agent, const std::string& ip_address);
    
    // Notification helpers
    void send_login_notification(const User& user, const UserSession& session);
    void send_security_alert(const User& user, const std::string& alert_type, const std::string& details);
    
    // Audit logging
    void log_authentication_attempt(const AuthCredentials& credentials, AuthResult result);
    void log_session_event(const std::string& user_id, const std::string& session_id, const std::string& event);
    
    // Rate limiting helpers
    bool check_login_rate_limit(const std::string& ip_address, const std::string& user_id);
    bool check_registration_rate_limit(const std::string& ip_address);
    bool check_password_reset_rate_limit(const std::string& ip_address);
};

} // namespace sonet::user
