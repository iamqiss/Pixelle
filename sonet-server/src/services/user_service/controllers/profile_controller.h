/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../include/user_service.h"
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <vector>

namespace sonet::user::controllers {

/**
 * Profile controller for advanced profile management features.
 * Handles profile customization, privacy settings, and advanced user features.
 */
class ProfileController {
public:
    explicit ProfileController(std::shared_ptr<sonet::user::UserServiceImpl> user_service);
    ~ProfileController() = default;

    // Request structures for profile operations
    struct GetPublicProfileRequest {
        std::string username;
        std::string viewer_token;  // Optional - affects what data is visible
    };

    struct UpdatePrivacySettingsRequest {
        std::string access_token;
        bool is_private_account;
        bool allow_message_requests;
        bool show_activity_status;
        bool show_read_receipts;
        std::vector<std::string> blocked_users;
        std::vector<std::string> muted_users;
    };

    struct BlockUserRequest {
        std::string access_token;
        std::string user_id_to_block;
    };

    struct ReportUserRequest {
        std::string access_token;
        std::string reported_user_id;
        std::string reason;
        std::string description;
    };

    struct GetFollowersRequest {
        std::string access_token;
        std::string user_id;  // If empty, get current user's followers
        int limit;
        int offset;
    };

    struct GetFollowingRequest {
        std::string access_token;
        std::string user_id;  // If empty, get current user's following
        int limit;
        int offset;
    };

    struct UpdateNotificationSettingsRequest {
        std::string access_token;
        bool email_notifications;
        bool push_notifications;
        bool sms_notifications;
        nlohmann::json notification_types;  // Which types of notifications to receive
    };

    struct GetActivityLogRequest {
        std::string access_token;
        std::string activity_type;  // login, profile_update, etc.
        int limit;
        int offset;
    };

    struct ExportDataRequest {
        std::string access_token;
        std::vector<std::string> data_types;  // profile, notes, messages, etc.
    };

    // HTTP endpoint handlers
    nlohmann::json handle_get_public_profile(const GetPublicProfileRequest& request);
    nlohmann::json handle_update_privacy_settings(const UpdatePrivacySettingsRequest& request);
    nlohmann::json handle_block_user(const BlockUserRequest& request);
    nlohmann::json handle_unblock_user(const std::string& access_token, const std::string& user_id);
    nlohmann::json handle_mute_user(const std::string& access_token, const std::string& user_id);
    nlohmann::json handle_unmute_user(const std::string& access_token, const std::string& user_id);
    nlohmann::json handle_report_user(const ReportUserRequest& request);
    nlohmann::json handle_get_followers(const GetFollowersRequest& request);
    nlohmann::json handle_get_following(const GetFollowingRequest& request);
    nlohmann::json handle_update_notification_settings(const UpdateNotificationSettingsRequest& request);
    nlohmann::json handle_get_activity_log(const GetActivityLogRequest& request);
    nlohmann::json handle_export_data(const ExportDataRequest& request);
    nlohmann::json handle_delete_activity_log(const std::string& access_token, const std::string& activity_id);
    nlohmann::json handle_get_blocked_users(const std::string& access_token);
    nlohmann::json handle_get_muted_users(const std::string& access_token);

    // Profile verification and badges
    nlohmann::json handle_request_verification(const std::string& access_token, const nlohmann::json& verification_data);
    nlohmann::json handle_get_verification_status(const std::string& access_token);

    // Profile analytics (for the user's own profile)
    nlohmann::json handle_get_profile_analytics(const std::string& access_token, const std::string& time_range);

private:
    std::shared_ptr<sonet::user::UserServiceImpl> user_service_;
    
    // Helper methods
    nlohmann::json create_error_response(const std::string& message);
    nlohmann::json create_success_response(const std::string& message, const nlohmann::json& data = nlohmann::json{});
    
    // Privacy filtering
    nlohmann::json filter_profile_data(const nlohmann::json& profile_data, bool is_private, bool is_viewer_following);
    bool can_view_private_profile(const std::string& viewer_id, const std::string& profile_user_id);
    
    // Validation
    bool validate_privacy_settings(const UpdatePrivacySettingsRequest& request);
    bool validate_report_request(const ReportUserRequest& request);
    
    // Data formatting
    nlohmann::json format_user_list(const std::vector<std::string>& user_ids);
    nlohmann::json format_activity_log_entry(const std::string& activity_type, const nlohmann::json& details, int64_t timestamp);
};

} // namespace sonet::user::controllers
