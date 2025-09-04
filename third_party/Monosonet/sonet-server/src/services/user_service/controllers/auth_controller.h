/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../include/user_service.h"
#include "services/user.grpc.pb.h"
#include "../include/email_service.h"
#include "../include/repository.h"
#include <grpcpp/server_context.h>
#include <memory>
#include <string>
#include <vector>

namespace sonet::user::controllers {

/**
 * Authentication controller handling REST API endpoints for user authentication.
 * This provides HTTP/REST interface on top of the gRPC service.
 */
class AuthController {
public:
    explicit AuthController(std::shared_ptr<sonet::user::UserServiceImpl> user_service,
                           std::shared_ptr<email::EmailService> email_service = nullptr,
                           const std::string& connection_string = "");
    ~AuthController() = default;

    // REST endpoint handlers
    struct RegisterRequest {
        std::string username;
        std::string email;
        std::string password;
        std::string full_name; // mapped to display_name in proto
        std::string bio;
        std::string device_fingerprint;
        int device_type;
    };

    struct LoginRequest {
        std::string username;  // email for proto credentials.email; keep name for compatibility
        std::string password;
        std::string device_fingerprint;
        int device_type;
    };

    struct RefreshTokenRequest { std::string refresh_token; };

    struct LogoutRequest {
        std::string session_id; // proto expects session_id
        bool logout_all_devices{false};
    };

    struct VerifyEmailRequest { std::string verification_token; };
    struct ForgotPasswordRequest { std::string email; };
    struct ResetPasswordRequest { std::string reset_token; std::string new_password; };

    // Response structures
    struct AuthResponse {
        bool success;
        std::string message;
        std::string access_token;
        std::string refresh_token;
        std::string user_id;
        int64_t access_token_expires_at{0};
        int64_t refresh_token_expires_at{0};
        nlohmann::json user_data;
    };

    struct StandardResponse { bool success; std::string message; nlohmann::json data; };

    // HTTP endpoint handlers
    nlohmann::json handle_register(const RegisterRequest& request);
    nlohmann::json handle_login(const LoginRequest& request);
    nlohmann::json handle_refresh_token(const RefreshTokenRequest& request);
    nlohmann::json handle_logout(const LogoutRequest& request);
    nlohmann::json handle_verify_email(const VerifyEmailRequest& request);
    nlohmann::json handle_forgot_password(const ForgotPasswordRequest& request);
    nlohmann::json handle_reset_password(const ResetPasswordRequest& request);
    nlohmann::json handle_check_username(const std::string& username);
    nlohmann::json handle_check_email(const std::string& email);

        // Utility methods
    bool validate_register_request(const RegisterRequest& request);
    bool validate_login_request(const LoginRequest& request);
    std::string extract_bearer_token(const std::string& authorization_header);
    std::vector<std::string> generate_username_suggestions(const std::string& base_username);

private:
    std::shared_ptr<sonet::user::UserServiceImpl> user_service_;
    std::shared_ptr<email::EmailService> email_service_;
    std::string connection_string_;

private:
    // Converters to proto
    sonet::user::RegisterUserRequest to_grpc_register_request(const RegisterRequest& request);
    sonet::user::LoginUserRequest to_grpc_login_request(const LoginRequest& request);
    sonet::user::RefreshTokenRequest to_grpc_refresh_request(const RefreshTokenRequest& request);
    sonet::user::LogoutRequest to_grpc_logout_request(const LogoutRequest& request);

    // Response mappers
    nlohmann::json grpc_response_to_json(const sonet::user::RegisterUserResponse& response);
    nlohmann::json grpc_response_to_json(const sonet::user::LoginUserResponse& response);
    nlohmann::json grpc_response_to_json(const sonet::user::RefreshTokenResponse& response);
    nlohmann::json create_error_response(const std::string& message);
    nlohmann::json create_success_response(const std::string& message, const nlohmann::json& data = nlohmann::json{});
};

} // namespace sonet::user::controllers
