/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "user_types.h"
#include "auth_manager.h"
#include "password_manager.h"
#include "jwt_manager.h"
#include "session_manager.h"
#include "rate_limiter.h"

// Include generated gRPC code
#include "services/user.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <memory>

namespace sonet::user {

/**
 * User Service Implementation - The main gRPC service
 * 
 * This is the entry point for all user-related operations.
 * I've designed this to be clean and focused - it handles the gRPC
 * protocol details and delegates the real work to specialized managers.
 */
class UserServiceImpl final : public sonet::user::UserService::Service {
public:
    UserServiceImpl();
    ~UserServiceImpl() = default;

    // Core authentication operations
    grpc::Status RegisterUser(
        grpc::ServerContext* context,
        const sonet::user::RegisterUserRequest* request,
        sonet::user::RegisterUserResponse* response) override;

    grpc::Status LoginUser(
        grpc::ServerContext* context,
        const sonet::user::LoginUserRequest* request,
        sonet::user::LoginUserResponse* response) override;

    grpc::Status LogoutUser(
        grpc::ServerContext* context,
        const sonet::user::LogoutRequest* request,
        sonet::user::LogoutResponse* response) override;

    // Token operations
    grpc::Status VerifyToken(
        grpc::ServerContext* context,
        const sonet::user::VerifyTokenRequest* request,
        sonet::user::VerifyTokenResponse* response) override;

    grpc::Status RefreshToken(
        grpc::ServerContext* context,
        const sonet::user::RefreshTokenRequest* request,
        sonet::user::RefreshTokenResponse* response) override;

    // Password management
    grpc::Status ChangePassword(
        grpc::ServerContext* context,
        const sonet::user::ChangePasswordRequest* request,
        sonet::user::ChangePasswordResponse* response) override;

    grpc::Status ResetPassword(
        grpc::ServerContext* context,
        const sonet::user::ResetPasswordRequest* request,
        sonet::user::ResetPasswordResponse* response) override;

    // Account verification
    grpc::Status VerifyEmail(
        grpc::ServerContext* context,
        const sonet::user::VerifyEmailRequest* request,
        sonet::user::VerifyEmailResponse* response) override;

    grpc::Status ResendVerification(
        grpc::ServerContext* context,
        const sonet::user::ResendVerificationRequest* request,
        sonet::user::ResendVerificationResponse* response) override;

    // Two-factor authentication
    grpc::Status SetupTwoFactor(
        grpc::ServerContext* context,
        const sonet::user::SetupTwoFactorRequest* request,
        sonet::user::SetupTwoFactorResponse* response) override;

    grpc::Status VerifyTwoFactor(
        grpc::ServerContext* context,
        const sonet::user::VerifyTwoFactorRequest* request,
        sonet::user::VerifyTwoFactorResponse* response) override;

    // Session management
    grpc::Status GetActiveSessions(
        grpc::ServerContext* context,
        const sonet::user::GetActiveSessionsRequest* request,
        sonet::user::GetActiveSessionsResponse* response) override;

    grpc::Status TerminateSession(
        grpc::ServerContext* context,
        const sonet::user::TerminateSessionRequest* request,
        sonet::user::TerminateSessionResponse* response) override;

    // User profile operations
    grpc::Status GetUserProfile(
        grpc::ServerContext* context,
        const sonet::user::GetUserProfileRequest* request,
        sonet::user::GetUserProfileResponse* response) override;

    grpc::Status UpdateUserProfile(
        grpc::ServerContext* context,
        const sonet::user::UpdateUserProfileRequest* request,
        sonet::user::UpdateUserProfileResponse* response) override;

private:
    // Core components - the heart of our authentication system
    std::shared_ptr<AuthManager> auth_manager_;
    std::shared_ptr<PasswordManager> password_manager_;
    std::shared_ptr<JWTManager> jwt_manager_;
    std::shared_ptr<SessionManager> session_manager_;
    std::shared_ptr<RateLimiter> rate_limiter_;
    
    // Helper methods for gRPC context handling
    std::string extract_client_info(grpc::ServerContext* context);
    std::string extract_ip_address(grpc::ServerContext* context);
    std::string extract_user_id_from_context(grpc::ServerContext* context);
    
    // Utility methods
    std::string get_auth_result_message(AuthResult result);
    User get_user_by_email(const std::string& email);
    
    // Validation helpers
    bool validate_email_format(const std::string& email);
    bool validate_username_format(const std::string& username);
    
    // Response builders
    void build_user_response(const User& user, sonet::user::UserProfile* profile);
    void build_session_response(const UserSession& session, sonet::user::Session* session_proto);
};

} // namespace sonet::user
