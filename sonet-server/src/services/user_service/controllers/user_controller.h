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
#include "../include/file_upload_service.h"
#include "../include/repository.h"
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <vector>

namespace sonet::user::controllers {

/**
 * User controller handling REST API endpoints for user management and profiles.
 * Provides HTTP interface for user operations beyond authentication.
 */
class UserController {
public:
    explicit UserController(std::shared_ptr<sonet::user::UserServiceImpl> user_service,
                           std::shared_ptr<storage::FileUploadService> file_service = nullptr,
                           const std::string& connection_string = "");
    ~UserController() = default;

    // Request structures
    struct GetProfileRequest {
        std::string access_token;
        std::string user_id;  // If empty, get current user's profile
    };

    struct UpdateProfileRequest {
        std::string access_token;
        std::string full_name;
        std::string bio;
        std::string location;
        std::string website;
        std::string avatar_url;
        std::string banner_url;
        bool is_private;
    };

    struct ChangePasswordRequest {
        std::string access_token;
        std::string current_password;
        std::string new_password;
    };

    struct UpdateSettingsRequest {
        std::string access_token;
        nlohmann::json privacy_settings;
        nlohmann::json notification_settings;
        nlohmann::json security_settings;
    };

    struct GetSessionsRequest {
        std::string access_token;
    };

    struct TerminateSessionRequest {
        std::string access_token;
        std::string session_id;
    };

    struct DeactivateAccountRequest {
        std::string access_token;
        std::string password;
        std::string reason;
    };

    struct SearchUsersRequest {
        std::string access_token;
        std::string query;
        int limit;
        int offset;
    };

    // HTTP endpoint handlers
    nlohmann::json handle_get_profile(const GetProfileRequest& request);
    nlohmann::json handle_update_profile(const UpdateProfileRequest& request);
    nlohmann::json handle_change_password(const ChangePasswordRequest& request);
    nlohmann::json handle_update_settings(const UpdateSettingsRequest& request);
    nlohmann::json handle_get_sessions(const GetSessionsRequest& request);
    nlohmann::json handle_terminate_session(const TerminateSessionRequest& request);
    nlohmann::json handle_terminate_all_sessions(const std::string& access_token);
    nlohmann::json handle_deactivate_account(const DeactivateAccountRequest& request);
    nlohmann::json handle_search_users(const SearchUsersRequest& request);
    nlohmann::json handle_get_user_stats(const std::string& access_token, const std::string& user_id);
    
    // File upload handlers
    nlohmann::json handle_upload_avatar(const std::string& access_token, 
                                       const std::vector<uint8_t>& file_data,
                                       const std::string& filename,
                                       const std::string& content_type);
    nlohmann::json handle_upload_banner(const std::string& access_token,
                                       const std::vector<uint8_t>& file_data,
                                       const std::string& filename,
                                       const std::string& content_type);
    nlohmann::json handle_terminate_all_sessions(const std::string& access_token);
    nlohmann::json handle_deactivate_account(const DeactivateAccountRequest& request);
    nlohmann::json handle_search_users(const SearchUsersRequest& request);
    nlohmann::json handle_get_user_stats(const std::string& access_token, const std::string& user_id);
    nlohmann::json handle_upload_avatar(const std::string& access_token, const std::vector<uint8_t>& image_data);
    nlohmann::json handle_upload_banner(const std::string& access_token, const std::vector<uint8_t>& image_data);

    // Validation helpers
    bool validate_update_profile_request(const UpdateProfileRequest& request);
    bool validate_change_password_request(const ChangePasswordRequest& request);
    std::string extract_bearer_token(const std::string& authorization_header);

private:
    std::shared_ptr<sonet::user::UserServiceImpl> user_service_;
    std::shared_ptr<storage::FileUploadService> file_service_;
    std::string connection_string_;
    
    // Helper methods
    nlohmann::json create_error_response(const std::string& message);
    nlohmann::json create_success_response(const std::string& message, const nlohmann::json& data = nlohmann::json{});
    nlohmann::json user_data_to_json(const sonet::user::UserProfile& user);
    nlohmann::json session_data_to_json(const sonet::user::Session& session);
    
    // Image processing helpers
    bool is_valid_image_format(const std::vector<uint8_t>& data);
    std::string save_uploaded_image(const std::vector<uint8_t>& data, const std::string& user_id, const std::string& type);
};

} // namespace sonet::user::controllers
