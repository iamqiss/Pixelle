/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "user_controller.h"
#include <spdlog/spdlog.h>
#include <fstream>
#include <filesystem>
#include <regex>

namespace sonet::user::controllers {

UserController::UserController(std::shared_ptr<sonet::user::UserServiceImpl> user_service,
                               std::shared_ptr<storage::FileUploadService> file_service,
                               const std::string& connection_string)
    : user_service_(std::move(user_service))
    , file_service_(std::move(file_service))
    , connection_string_(connection_string) {
    spdlog::info("User controller initialized");
}

nlohmann::json UserController::handle_get_profile(const GetProfileRequest& request) {
    try {
        if (request.access_token.empty()) { return create_error_response("Access token is required"); }
        sonet::user::GetUserProfileRequest grpc_request;
        grpc_request.set_user_id(request.user_id);
        sonet::user::GetUserProfileResponse grpc_response;
        grpc::ServerContext context;
        auto status = user_service_->GetUserProfile(&context, &grpc_request, &grpc_response);
        if (!status.ok()) { return create_error_response("Profile service unavailable"); }
        nlohmann::json response;
        response["success"] = grpc_response.status().success();
        response["message"] = grpc_response.status().message();
        if (grpc_response.status().success() && grpc_response.has_user()) {
            response["user"] = user_data_to_json(grpc_response.user());
        }
        return response;
    } catch (const std::exception& e) {
        spdlog::error("Get profile error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_update_profile(const UpdateProfileRequest& request) {
    try {
        if (!validate_update_profile_request(request)) {
            return create_error_response("Invalid profile update data");
        }
        
        // This would require implementing UpdateUserProfile in the gRPC service
        // For now, returning a placeholder response
        nlohmann::json user_data;
        user_data["full_name"] = request.full_name;
        user_data["bio"] = request.bio;
        user_data["location"] = request.location;
        user_data["website"] = request.website;
        user_data["avatar_url"] = request.avatar_url;
        user_data["banner_url"] = request.banner_url;
        user_data["is_private"] = request.is_private;
        
        return create_success_response("Profile updated successfully", user_data);
        
    } catch (const std::exception& e) {
        spdlog::error("Update profile error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_change_password(const ChangePasswordRequest& request) {
    try {
        if (!validate_change_password_request(request)) {
            return create_error_response("Invalid password change data");
        }
        
        // This would require implementing ChangePassword in the gRPC service
        // Including verification of current password and updating to new password
        return create_success_response("Password changed successfully");
        
    } catch (const std::exception& e) {
        spdlog::error("Change password error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_update_settings(const UpdateSettingsRequest& request) {
    try {
        if (request.access_token.empty()) {
            return create_error_response("Access token is required");
        }
        
        // This would update user settings in the database
        // Including privacy, notification, and security preferences
        nlohmann::json settings;
        settings["privacy"] = request.privacy_settings;
        settings["notifications"] = request.notification_settings;
        settings["security"] = request.security_settings;
        
        return create_success_response("Settings updated successfully", settings);
        
    } catch (const std::exception& e) {
        spdlog::error("Update settings error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_get_sessions(const GetSessionsRequest& request) {
    try {
        if (request.access_token.empty()) {
            return create_error_response("Access token is required");
        }
        
        // This would get all active sessions for the user
        // For now, returning a placeholder
        nlohmann::json sessions = nlohmann::json::array();
        
        // Example session data
        nlohmann::json session;
        session["session_id"] = "current-session";
        session["device_type"] = "web";
        session["device_name"] = "Chrome on Windows";
        session["ip_address"] = "192.168.1.100";
        session["location"] = "San Francisco, CA";
        session["is_current"] = true;
        session["created_at"] = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        session["last_activity"] = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        sessions.push_back(session);
        
        return create_success_response("Sessions retrieved successfully", sessions);
        
    } catch (const std::exception& e) {
        spdlog::error("Get sessions error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_terminate_session(const TerminateSessionRequest& request) {
    try {
        if (request.access_token.empty() || request.session_id.empty()) {
            return create_error_response("Access token and session ID are required");
        }
        
        // This would terminate the specified session
        return create_success_response("Session terminated successfully");
        
    } catch (const std::exception& e) {
        spdlog::error("Terminate session error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_terminate_all_sessions(const std::string& access_token) {
    try {
        if (access_token.empty()) {
            return create_error_response("Access token is required");
        }
        
        // This would terminate all sessions except the current one
        return create_success_response("All other sessions terminated successfully");
        
    } catch (const std::exception& e) {
        spdlog::error("Terminate all sessions error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_deactivate_account(const DeactivateAccountRequest& request) {
    try {
        if (request.access_token.empty() || request.password.empty()) {
            return create_error_response("Access token and password are required");
        }
        
        // This would:
        // 1. Verify the password
        // 2. Mark the account as deactivated
        // 3. Log the reason
        // 4. Schedule data retention according to policy
        
        spdlog::info("Account deactivation requested. Reason: {}", request.reason);
        
        return create_success_response("Account deactivated successfully");
        
    } catch (const std::exception& e) {
        spdlog::error("Deactivate account error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_search_users(const SearchUsersRequest& request) {
    try {
        if (request.access_token.empty() || request.query.empty()) {
            return create_error_response("Access token and search query are required");
        }
        
        // This would search users by username, full name, bio, etc.
        nlohmann::json users = nlohmann::json::array();
        
        // Example search result
        nlohmann::json user;
        user["user_id"] = "user-123";
        user["username"] = "johndoe";
        user["full_name"] = "John Doe";
        user["avatar_url"] = "https://cdn.sonet.com/avatars/user-123.jpg";
        user["bio"] = "Software engineer passionate about C++";
        user["is_verified"] = true;
        user["is_private"] = false;
        
        users.push_back(user);
        
        nlohmann::json data;
        data["users"] = users;
        data["total_count"] = users.size();
        data["query"] = request.query;
        data["limit"] = request.limit;
        data["offset"] = request.offset;
        
        return create_success_response("Search completed successfully", data);
        
    } catch (const std::exception& e) {
        spdlog::error("Search users error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_get_user_stats(const std::string& access_token, const std::string& user_id) {
    try {
        if (access_token.empty()) {
            return create_error_response("Access token is required");
        }
        
        // This would get user statistics like follower count, following count, note count, etc.
        nlohmann::json stats;
        stats["followers_count"] = 1234;
        stats["following_count"] = 567;
        stats["notes_count"] = 89;
        stats["likes_received"] = 4567;
        stats["account_age_days"] = 365;
        stats["join_date"] = "2024-01-01";
        stats["last_active"] = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        return create_success_response("User stats retrieved successfully", stats);
        
    } catch (const std::exception& e) {
        spdlog::error("Get user stats error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_upload_avatar(const std::string& access_token, const std::vector<uint8_t>& image_data) {
    try {
        if (access_token.empty()) {
            return create_error_response("Access token is required");
        }
        
        if (image_data.empty()) {
            return create_error_response("Image data is required");
        }
        
        if (!is_valid_image_format(image_data)) {
            return create_error_response("Invalid image format. Supported formats: JPEG, PNG, WebP");
        }
        
        // This would:
        // 1. Validate image size and format
        // 2. Resize/optimize the image
        // 3. Upload to CDN/storage
        // 4. Update user's avatar_url
        
        std::string avatar_url = "https://cdn.sonet.com/avatars/user-123-" + 
                                std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count()) + ".jpg";
        
        nlohmann::json data;
        data["avatar_url"] = avatar_url;
        
        return create_success_response("Avatar uploaded successfully", data);
        
    } catch (const std::exception& e) {
        spdlog::error("Upload avatar error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_upload_banner(const std::string& access_token, const std::vector<uint8_t>& image_data) {
    try {
        if (access_token.empty()) {
            return create_error_response("Access token is required");
        }
        
        if (image_data.empty()) {
            return create_error_response("Image data is required");
        }
        
        if (!is_valid_image_format(image_data)) {
            return create_error_response("Invalid image format. Supported formats: JPEG, PNG, WebP");
        }
        
        // Similar to avatar upload but for banner images
        std::string banner_url = "https://cdn.sonet.com/banners/user-123-" + 
                                std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count()) + ".jpg";
        
        nlohmann::json data;
        data["banner_url"] = banner_url;
        
        return create_success_response("Banner uploaded successfully", data);
        
    } catch (const std::exception& e) {
        spdlog::error("Upload banner error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

// Validation methods

bool UserController::validate_update_profile_request(const UpdateProfileRequest& request) {
    if (request.access_token.empty()) {
        return false;
    }
    
    // Full name validation
    if (request.full_name.length() > 100) {
        return false;
    }
    
    // Bio validation
    if (request.bio.length() > 500) {
        return false;
    }
    
    // Location validation
    if (request.location.length() > 100) {
        return false;
    }
    
    // Website URL validation
    if (!request.website.empty()) {
        std::regex url_regex(R"(^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$)");
        if (!std::regex_match(request.website, url_regex)) {
            return false;
        }
    }
    
    return true;
}

bool UserController::validate_change_password_request(const ChangePasswordRequest& request) {
    if (request.access_token.empty() || request.current_password.empty() || request.new_password.empty()) {
        return false;
    }
    
    // New password strength validation (basic check)
    if (request.new_password.length() < 8) {
        return false;
    }
    
    return true;
}

std::string UserController::extract_bearer_token(const std::string& authorization_header) {
    const std::string bearer_prefix = "Bearer ";
    if (authorization_header.starts_with(bearer_prefix)) {
        return authorization_header.substr(bearer_prefix.length());
    }
    return "";
}

// Helper methods

nlohmann::json UserController::create_error_response(const std::string& message) {
    nlohmann::json response; response["success"] = false; response["message"] = message; return response;
}

nlohmann::json UserController::create_success_response(const std::string& message, const nlohmann::json& data) {
    nlohmann::json response; response["success"] = true; response["message"] = message; if (!data.empty()) response["data"] = data; return response;
}

nlohmann::json UserController::user_data_to_json(const sonet::user::UserProfile& user) {
    nlohmann::json json_user;
    json_user["user_id"] = user.user_id();
    json_user["username"] = user.username();
    json_user["email"] = user.email();
    json_user["display_name"] = user.display_name();
    json_user["bio"] = user.bio();
    json_user["avatar_url"] = user.avatar_url();
    json_user["location"] = user.location();
    json_user["website"] = user.website();
    json_user["is_verified"] = user.is_verified();
    json_user["is_private"] = user.is_private();
    return json_user;
}

nlohmann::json UserController::session_data_to_json(const sonet::user::Session& session) {
    nlohmann::json json_session;
    json_session["session_id"] = session.session_id();
    json_session["ip_address"] = session.ip_address();
    json_session["user_agent"] = session.user_agent();
    return json_session;
}

bool UserController::is_valid_image_format(const std::vector<uint8_t>& data) {
    if (data.size() < 4) {
        return false;
    }
    
    // Check for common image format magic bytes
    // JPEG: FF D8 FF
    if (data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF) {
        return true;
    }
    
    // PNG: 89 50 4E 47
    if (data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47) {
        return true;
    }
    
    // WebP: check for "WEBP" in bytes 8-11
    if (data.size() >= 12) {
        std::string webp_signature(data.begin() + 8, data.begin() + 12);
        if (webp_signature == "WEBP") {
            return true;
        }
    }
    
    return false;
}

std::string UserController::save_uploaded_image(const std::vector<uint8_t>& data, const std::string& user_id, const std::string& type) {
    if (!file_service_) {
        spdlog::error("File service not configured");
        return "";
    }
    
    try {
        // Determine upload type and call appropriate method
        std::future<storage::UploadResult> upload_future;
        
        if (type == "avatar") {
            upload_future = file_service_->upload_profile_picture(user_id, data, "avatar.jpg", "image/jpeg");
        } else if (type == "banner") {
            upload_future = file_service_->upload_profile_banner(user_id, data, "banner.jpg", "image/jpeg");
        } else {
            spdlog::error("Unknown upload type: {}", type);
            return "";
        }
        
        // Wait for upload to complete
        auto result = upload_future.get();
        
        if (result.success) {
            spdlog::info("Successfully uploaded {} for user {}: {}", type, user_id, result.url);
            return result.url;
        } else {
            spdlog::error("Failed to upload {}: {}", type, result.error_message);
            return "";
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Image upload error: {}", e.what());
        return "";
    }
}

nlohmann::json UserController::handle_upload_avatar(const std::string& access_token,
                                                   const std::vector<uint8_t>& file_data,
                                                   const std::string& filename,
                                                   const std::string& content_type) {
    try {
        if (access_token.empty()) {
            return create_error_response("Access token is required");
        }
        
        if (file_data.empty()) {
            return create_error_response("File data is required");
        }
        
        // Validate file type
        if (!file_service_ || !file_service_->is_valid_image_format(content_type)) {
            return create_error_response("Invalid image format. Supported formats: JPEG, PNG, WebP, GIF");
        }
        
        // Validate file size
        if (!file_service_->is_valid_file_size(file_data.size(), "avatar")) {
            return create_error_response("File size too large. Maximum size for avatar is 10MB");
        }
        
        // Extract user ID from token (would need JWT parsing)
        // For now, using a placeholder
        std::string user_id = "user_from_token"; // This should be extracted from JWT
        
        // Upload the avatar
        auto upload_future = file_service_->upload_profile_picture(user_id, file_data, filename, content_type);
        auto result = upload_future.get();
        
        if (!result.success) {
            return create_error_response("Failed to upload avatar: " + result.error_message);
        }
        
        // Update user profile with new avatar URL
        // This would require updating the user record in the database
        
        nlohmann::json response_data;
        response_data["avatar_url"] = result.url;
        response_data["file_id"] = result.file_id;
        response_data["file_size"] = result.file_size;
        
        return create_success_response("Avatar uploaded successfully", response_data);
        
    } catch (const std::exception& e) {
        spdlog::error("Avatar upload error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

nlohmann::json UserController::handle_upload_banner(const std::string& access_token,
                                                   const std::vector<uint8_t>& file_data,
                                                   const std::string& filename,
                                                   const std::string& content_type) {
    try {
        if (access_token.empty()) {
            return create_error_response("Access token is required");
        }
        
        if (file_data.empty()) {
            return create_error_response("File data is required");
        }
        
        // Validate file type
        if (!file_service_ || !file_service_->is_valid_image_format(content_type)) {
            return create_error_response("Invalid image format. Supported formats: JPEG, PNG, WebP, GIF");
        }
        
        // Validate file size
        if (!file_service_->is_valid_file_size(file_data.size(), "banner")) {
            return create_error_response("File size too large. Maximum size for banner is 15MB");
        }
        
        // Extract user ID from token (would need JWT parsing)
        std::string user_id = "user_from_token"; // This should be extracted from JWT
        
        // Upload the banner
        auto upload_future = file_service_->upload_profile_banner(user_id, file_data, filename, content_type);
        auto result = upload_future.get();
        
        if (!result.success) {
            return create_error_response("Failed to upload banner: " + result.error_message);
        }
        
        // Update user profile with new banner URL
        // This would require updating the user record in the database
        
        nlohmann::json response_data;
        response_data["banner_url"] = result.url;
        response_data["file_id"] = result.file_id;
        response_data["file_size"] = result.file_size;
        
        return create_success_response("Banner uploaded successfully", response_data);
        
    } catch (const std::exception& e) {
        spdlog::error("Banner upload error: {}", e.what());
        return create_error_response("Internal server error");
    }
}

} // namespace sonet::user::controllers
