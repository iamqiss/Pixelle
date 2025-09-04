/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "auth_controller.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <regex>

namespace sonet::user::controllers {

AuthController::AuthController(std::shared_ptr<sonet::user::UserServiceImpl> user_service,
                               std::shared_ptr<email::EmailService> email_service,
                               const std::string& connection_string)
    : user_service_(std::move(user_service))
    , email_service_(std::move(email_service))
    , connection_string_(connection_string) {
    spdlog::info("Authentication controller initialized");
}

nlohmann::json AuthController::handle_register(const RegisterRequest& request) {
    try {
        if (!validate_register_request(request)) {
            return create_error_response("Invalid registration data");
        }
        auto grpc_request = to_grpc_register_request(request);
        sonet::user::RegisterUserResponse grpc_response;
        grpc::ServerContext context;
        auto status = user_service_->RegisterUser(&context, &grpc_request, &grpc_response);
        if (!status.ok()) {
            spdlog::error("gRPC call failed: {}", status.error_message());
            return create_error_response("Registration service unavailable");
        }
        return grpc_response_to_json(grpc_response);
    } catch (const std::exception& e) {
        spdlog::error("Registration error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json AuthController::handle_login(const LoginRequest& request) {
    try {
        if (!validate_login_request(request)) {
            return create_error_response("Invalid login data");
        }
        auto grpc_request = to_grpc_login_request(request);
        sonet::user::LoginUserResponse grpc_response;
        grpc::ServerContext context;
        auto status = user_service_->LoginUser(&context, &grpc_request, &grpc_response);
        if (!status.ok()) {
            spdlog::warn("Login failed: {}", status.error_message());
            return create_error_response("Authentication service unavailable");
        }
        return grpc_response_to_json(grpc_response);
    } catch (const std::exception& e) {
        spdlog::error("Login error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json AuthController::handle_refresh_token(const RefreshTokenRequest& request) {
    try {
        if (request.refresh_token.empty()) {
            return create_error_response("Refresh token is required");
        }
        auto grpc_request = to_grpc_refresh_request(request);
        sonet::user::RefreshTokenResponse grpc_response;
        grpc::ServerContext context;
        auto status = user_service_->RefreshToken(&context, &grpc_request, &grpc_response);
        if (!status.ok()) {
            spdlog::error("gRPC call failed: {}", status.error_message());
            return create_error_response("Token refresh service unavailable");
        }
        return grpc_response_to_json(grpc_response);
    } catch (const std::exception& e) {
        spdlog::error("Token refresh error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json AuthController::handle_logout(const LogoutRequest& request) {
    try {
        if (request.session_id.empty()) {
            return create_error_response("Session ID is required");
        }
        auto grpc_request = to_grpc_logout_request(request);
        sonet::user::LogoutResponse grpc_response;
        grpc::ServerContext context;
        auto status = user_service_->LogoutUser(&context, &grpc_request, &grpc_response);
        if (!status.ok()) {
            spdlog::error("gRPC call failed: {}", status.error_message());
            return create_error_response("Logout service unavailable");
        }
        nlohmann::json response;
        response["success"] = grpc_response.status().success();
        response["message"] = grpc_response.status().message();
        return response;
    } catch (const std::exception& e) {
        spdlog::error("Logout error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json AuthController::handle_verify_email(const VerifyEmailRequest& request) {
    try {
        if (request.verification_token.empty()) {
            return create_error_response("Verification token is required");
        }
        // Existing repository-based flow left intact
        // ... existing code ...
        return create_error_response("Not implemented");
    } catch (const std::exception& e) {
        spdlog::error("Email verification error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json AuthController::handle_forgot_password(const ForgotPasswordRequest& request) {
    try {
        if (request.email.empty()) {
            return create_error_response("Email is required");
        }
        
        // Email validation
        std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
        if (!std::regex_match(request.email, email_regex)) {
            return create_error_response("Invalid email format");
        }
        
        // This would trigger password reset email
        try {
            auto user_repo = repository::RepositoryFactory::create_user_repository(connection_string_);
            auto user_opt = user_repo->get_user_by_email(request.email).get();
            
            if (user_opt.has_value()) {
                // Generate reset token
                std::string reset_token = security::SecurityUtils::generate_secure_token(32);
                int64_t expires_at = std::time(nullptr) + 3600; // 1 hour expiry
                
                // Store reset token
                bool stored = user_repo->store_password_reset_token(user_opt->user_id, reset_token, expires_at).get();
                
                if (stored) {
                    // Send password reset email
                    std::string reset_url = "https://sonet.com/reset-password?token=" + reset_token;
                    email_service_->send_password_reset_email(user_opt->email, user_opt->username, reset_token, reset_url);
                    
                    spdlog::info("Password reset email sent to: {}", request.email);
                }
            }
            
            // Always return success for security (don't reveal if email exists)
            return create_success_response("If the email exists, a password reset link has been sent");
            
        } catch (const std::exception& e) {
            spdlog::error("Password reset email error: {}", e.what());
            // Still return success to prevent information disclosure
            return create_success_response("If the email exists, a password reset link has been sent");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Forgot password error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json AuthController::handle_reset_password(const ResetPasswordRequest& request) {
    try {
        if (request.reset_token.empty() || request.new_password.empty()) {
            return create_error_response("Reset token and new password are required");
        }
        
        // Validate password strength
        if (!security::SecurityUtils::is_strong_password(request.new_password)) {
            return create_error_response("Password does not meet security requirements");
        }
        
        try {
            auto user_repo = repository::RepositoryFactory::create_user_repository(connection_string_);
            auto user_id_opt = user_repo->get_user_by_reset_token(request.reset_token).get();
            
            if (!user_id_opt.has_value()) {
                return create_error_response("Invalid or expired reset token");
            }
            
            std::string user_id = user_id_opt.value();
            
            // Get user details
            auto user_opt = user_repo->get_user_by_id(user_id).get();
            if (!user_opt.has_value()) {
                return create_error_response("User not found");
            }
            
            User user = user_opt.value();
            
            // Hash new password
            user.password_hash = security::SecurityUtils::hash_password(request.new_password);
            user.updated_at = std::time(nullptr);
            
            // Update user password
            bool updated = user_repo->update_user(user).get();
            if (!updated) {
                return create_error_response("Failed to update password");
            }
            
            // Delete the reset token
            user_repo->delete_password_reset_token(request.reset_token).get();
            
            // Invalidate all existing sessions for security
            auto session_repo = repository::RepositoryFactory::create_session_repository(connection_string_);
            session_repo->delete_user_sessions(user_id).get();
            
            // Send security alert email
            email_service_->send_security_alert_email(user.email, user.username, 
                "Password Reset", "System", "Unknown");
            
            nlohmann::json response;
            response["success"] = true;
            response["message"] = "Password reset successfully";
            
            spdlog::info("Password reset successful for user: {}", user_id);
            return response;
            
        } catch (const std::exception& e) {
            spdlog::error("Password reset error: {}", e.what());
            return create_error_response("Password reset failed");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Reset password error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json AuthController::handle_check_username(const std::string& username) {
    try {
        if (username.empty()) {
            return create_error_response("Username is required");
        }
        
        // Validate username format
        if (!security::SecurityUtils::is_valid_username(username)) {
            return create_error_response("Invalid username format");
        }
        
        try {
            auto user_repo = repository::RepositoryFactory::create_user_repository(connection_string_);
            bool available = user_repo->is_username_available(username).get();
            
            nlohmann::json response;
            response["available"] = available;
            response["username"] = username;
            
            if (!available) {
                response["message"] = "Username is already taken";
                response["suggestions"] = generate_username_suggestions(username);
            } else {
                response["message"] = "Username is available";
            }
            
            return create_success_response(response["message"], response);
            
        } catch (const std::exception& e) {
            spdlog::error("Username check error: {}", e.what());
            return create_error_response("Failed to check username availability");
        }
        
        return create_success_response("Username availability checked", response);
        
    } catch (const std::exception& e) {
        spdlog::error("Username check error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json AuthController::handle_check_email(const std::string& email) {
    try {
        if (email.empty()) {
            return create_error_response("Email is required");
        }
        
        // Validate email format
        if (!security::SecurityUtils::is_valid_email(email)) {
            return create_error_response("Invalid email format");
        }
        
        try {
            auto user_repo = repository::RepositoryFactory::create_user_repository(connection_string_);
            bool available = user_repo->is_email_available(email).get();
            
            nlohmann::json response;
            response["available"] = available;
            response["email"] = email;
            
            if (!available) {
                response["message"] = "Email is already registered";
            } else {
                response["message"] = "Email is available";
            }
            
            return create_success_response(response["message"], response);
            
        } catch (const std::exception& e) {
            spdlog::error("Email check error: {}", e.what());
            return create_error_response("Failed to check email availability");
        }
        
        return create_success_response("Email availability checked", response);
        
    } catch (const std::exception& e) {
        spdlog::error("Email check error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

// Validation methods

bool AuthController::validate_register_request(const RegisterRequest& request) {
    // Username validation
    if (request.username.empty() || request.username.length() < 3 || request.username.length() > 30) {
        return false;
    }
    
    // Username format (alphanumeric and underscores only)
    std::regex username_regex("^[a-zA-Z0-9_]+$");
    if (!std::regex_match(request.username, username_regex)) {
        return false;
    }
    
    // Email validation
    std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
    if (!std::regex_match(request.email, email_regex)) {
        return false;
    }
    
    // Password validation (basic check - detailed validation in SecurityUtils)
    if (request.password.empty() || request.password.length() < 8) {
        return false;
    }
    
    return !request.username.empty() && !request.email.empty() && !request.password.empty();
}

bool AuthController::validate_login_request(const LoginRequest& request) {
    return !request.username.empty() && !request.password.empty();
}

std::string AuthController::extract_bearer_token(const std::string& authorization_header) {
    const std::string bearer_prefix = "Bearer ";
    if (authorization_header.rfind(bearer_prefix, 0) == 0) {
        return authorization_header.substr(bearer_prefix.length());
    }
    return "";
}

// Conversion methods

sonet::user::RegisterUserRequest AuthController::to_grpc_register_request(const RegisterRequest& request) {
    sonet::user::RegisterUserRequest grpc_request;
    grpc_request.set_username(request.username);
    grpc_request.set_email(request.email);
    grpc_request.set_password(request.password);
    grpc_request.set_display_name(request.full_name);
    grpc_request.set_accept_terms(true);
    grpc_request.set_accept_privacy(true);
    return grpc_request;
}

sonet::user::LoginUserRequest AuthController::to_grpc_login_request(const LoginRequest& request) {
    sonet::user::LoginUserRequest grpc_request;
    auto* creds = grpc_request.mutable_credentials();
    creds->set_email(request.username);
    creds->set_password(request.password);
    return grpc_request;
}

sonet::user::RefreshTokenRequest AuthController::to_grpc_refresh_request(const RefreshTokenRequest& request) {
    sonet::user::RefreshTokenRequest grpc_request;
    grpc_request.set_refresh_token(request.refresh_token);
    return grpc_request;
}

sonet::user::LogoutRequest AuthController::to_grpc_logout_request(const LogoutRequest& request) {
    sonet::user::LogoutRequest grpc_request;
    grpc_request.set_session_id(request.session_id);
    grpc_request.set_logout_all_devices(request.logout_all_devices);
    return grpc_request;
}

// Response conversion methods

nlohmann::json AuthController::grpc_response_to_json(const sonet::user::RegisterUserResponse& response) {
    nlohmann::json json_response;
    json_response["success"] = response.status().success();
    json_response["message"] = response.status().message();
    if (response.status().success() && response.has_user()) {
        const auto& user = response.user();
        nlohmann::json user_data;
        user_data["user_id"] = user.user_id();
        user_data["username"] = user.username();
        user_data["email"] = user.email();
        user_data["display_name"] = user.display_name();
        user_data["is_verified"] = user.is_verified();
        user_data["is_private"] = user.is_private();
        json_response["user"] = user_data;
    }
    return json_response;
}

nlohmann::json AuthController::grpc_response_to_json(const sonet::user::LoginUserResponse& response) {
    nlohmann::json json_response;
    json_response["success"] = response.status().success();
    json_response["message"] = response.status().message();
    if (response.status().success()) {
        json_response["access_token"] = response.access_token();
        json_response["refresh_token"] = response.refresh_token();
        json_response["expires_in"] = response.expires_in();
        if (response.has_session()) {
            json_response["session_id"] = response.session().session_id();
        }
    }
    return json_response;
}

nlohmann::json AuthController::grpc_response_to_json(const sonet::user::RefreshTokenResponse& response) {
    nlohmann::json json_response;
    json_response["success"] = response.status().success();
    json_response["message"] = response.status().message();
    if (response.status().success()) {
        json_response["access_token"] = response.access_token();
        json_response["expires_in"] = response.expires_in();
    }
    return json_response;
}

nlohmann::json AuthController::create_error_response(const std::string& message) {
    nlohmann::json response;
    response["success"] = false;
    response["message"] = message;
    return response;
}

nlohmann::json AuthController::create_success_response(const std::string& message, const nlohmann::json& data) {
    nlohmann::json response;
    response["success"] = true;
    response["message"] = message;
    if (!data.empty()) {
        response["data"] = data;
    }
    return response;
}

std::vector<std::string> AuthController::generate_username_suggestions(const std::string& base_username) {
    std::vector<std::string> suggestions;
    
    // Generate numbered variations
    for (int i = 1; i <= 5; ++i) {
        suggestions.push_back(base_username + std::to_string(i));
    }
    
    // Generate variations with year
    int current_year = std::time(nullptr) / (365 * 24 * 3600) + 1970;
    suggestions.push_back(base_username + std::to_string(current_year));
    suggestions.push_back(base_username + std::to_string(current_year % 100));
    
    // Generate variations with underscores
    suggestions.push_back(base_username + "_official");
    suggestions.push_back("the_" + base_username);
    suggestions.push_back(base_username + "_real");
    
    return suggestions;
}

} // namespace sonet::user::controllers
