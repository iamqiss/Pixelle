/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "http_handler.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <regex>
#include <sstream>

namespace sonet::user::handlers {

HttpHandler::HttpHandler(
    std::shared_ptr<controllers::AuthController> auth_controller,
    std::shared_ptr<controllers::UserController> user_controller,
    std::shared_ptr<controllers::ProfileController> profile_controller)
    : auth_controller_(std::move(auth_controller))
    , user_controller_(std::move(user_controller))
    , profile_controller_(std::move(profile_controller)) {
    
    spdlog::info("HTTP handler initialized with all controllers");
}

HttpHandler::HttpResponse HttpHandler::handle_request(const HttpRequest& request) {
    auto start_time = std::chrono::steady_clock::now();
    
    try {
        // CORS preflight handling
        if (request.method == "OPTIONS") {
            return create_options_response();
        }
        
        // Basic request validation
        if (!validate_request(request)) {
            return create_error_response(400, "Invalid request format");
        }
        
        // Rate limiting
        if (is_rate_limited(request.client_ip, request.path)) {
            return create_error_response(429, "Rate limit exceeded");
        }
        
        HttpResponse response;
        
        // Route to appropriate handler based on path
        if (request.path.starts_with("/api/v1/auth/")) {
            response = handle_auth_routes(request);
        } else if (request.path.starts_with("/api/v1/users/") || request.path.starts_with("/api/v1/user/")) {
            response = handle_user_routes(request);
        } else if (request.path.starts_with("/api/v1/profile/")) {
            response = handle_profile_routes(request);
        } else if (request.path == "/api/v1/health") {
            // Health check endpoint
            nlohmann::json health_data;
            health_data["status"] = "healthy";
            health_data["service"] = "user-service";
            health_data["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            response = create_json_response(200, health_data);
        } else {
            response = create_error_response(404, "Endpoint not found");
        }
        
        // Add security headers
        add_security_headers(response);
        
        // Log the request
        log_request(request, response);
        
        return response;
        
    } catch (const std::exception& e) {
        spdlog::error("Error handling request: {}", e.what());
        auto response = create_error_response(500, "Internal server error");
        add_security_headers(response);
        log_request(request, response);
        return response;
    }
}

HttpHandler::HttpResponse HttpHandler::handle_auth_routes(const HttpRequest& request) {
    nlohmann::json response_data;
    
    if (request.path == "/api/v1/auth/register" && request.method == "NOTE") {
        auto body = parse_json_body(request.body);
        
        controllers::AuthController::RegisterRequest auth_request;
        auth_request.username = body.value("username", "");
        auth_request.email = body.value("email", "");
        auth_request.password = body.value("password", "");
        auth_request.full_name = body.value("full_name", "");
        auth_request.bio = body.value("bio", "");
        auth_request.device_fingerprint = body.value("device_fingerprint", "");
        auth_request.device_type = body.value("device_type", 0);
        
        response_data = auth_controller_->handle_register(auth_request);
        
    } else if (request.path == "/api/v1/auth/login" && request.method == "NOTE") {
        auto body = parse_json_body(request.body);
        
        controllers::AuthController::LoginRequest auth_request;
        auth_request.username = body.value("username", "");
        auth_request.password = body.value("password", "");
        auth_request.device_fingerprint = body.value("device_fingerprint", "");
        auth_request.device_type = body.value("device_type", 0);
        
        response_data = auth_controller_->handle_login(auth_request);
        
    } else if (request.path == "/api/v1/auth/refresh" && request.method == "NOTE") {
        auto body = parse_json_body(request.body);
        
        controllers::AuthController::RefreshTokenRequest auth_request;
        auth_request.refresh_token = body.value("refresh_token", "");
        
        response_data = auth_controller_->handle_refresh_token(auth_request);
        
    } else if (request.path == "/api/v1/auth/logout" && request.method == "NOTE") {
        auto body = parse_json_body(request.body);
        auto auth_header = request.headers.find("Authorization");
        
        controllers::AuthController::LogoutRequest auth_request;
        if (auth_header != request.headers.end()) {
            auth_request.access_token = auth_controller_->extract_bearer_token(auth_header->second);
        }
        auth_request.refresh_token = body.value("refresh_token", "");
        
        response_data = auth_controller_->handle_logout(auth_request);
        
    } else if (request.path == "/api/v1/auth/verify-email" && request.method == "NOTE") {
        auto body = parse_json_body(request.body);
        
        controllers::AuthController::VerifyEmailRequest auth_request;
        auth_request.verification_token = body.value("verification_token", "");
        
        response_data = auth_controller_->handle_verify_email(auth_request);
        
    } else if (request.path == "/api/v1/auth/forgot-password" && request.method == "NOTE") {
        auto body = parse_json_body(request.body);
        
        controllers::AuthController::ForgotPasswordRequest auth_request;
        auth_request.email = body.value("email", "");
        
        response_data = auth_controller_->handle_forgot_password(auth_request);
        
    } else if (request.path == "/api/v1/auth/reset-password" && request.method == "NOTE") {
        auto body = parse_json_body(request.body);
        
        controllers::AuthController::ResetPasswordRequest auth_request;
        auth_request.reset_token = body.value("reset_token", "");
        auth_request.new_password = body.value("new_password", "");
        
        response_data = auth_controller_->handle_reset_password(auth_request);
        
    } else {
        return create_error_response(404, "Auth endpoint not found");
    }
    
    int status_code = response_data.value("success", false) ? 200 : 400;
    return create_json_response(status_code, response_data);
}

HttpHandler::HttpResponse HttpHandler::handle_user_routes(const HttpRequest& request) {
    nlohmann::json response_data;
    auto auth_header = request.headers.find("Authorization");
    std::string access_token;
    
    if (auth_header != request.headers.end()) {
        access_token = user_controller_->extract_bearer_token(auth_header->second);
    }
    
    if (request.path == "/api/v1/user/profile" && request.method == "GET") {
        controllers::UserController::GetProfileRequest user_request;
        user_request.access_token = access_token;
        
        response_data = user_controller_->handle_get_profile(user_request);
        
    } else if (request.path == "/api/v1/user/profile" && request.method == "PUT") {
        auto body = parse_json_body(request.body);
        
        controllers::UserController::UpdateProfileRequest user_request;
        user_request.access_token = access_token;
        user_request.full_name = body.value("full_name", "");
        user_request.bio = body.value("bio", "");
        user_request.location = body.value("location", "");
        user_request.website = body.value("website", "");
        user_request.avatar_url = body.value("avatar_url", "");
        user_request.banner_url = body.value("banner_url", "");
        user_request.is_private = body.value("is_private", false);
        
        response_data = user_controller_->handle_update_profile(user_request);
        
    } else if (request.path == "/api/v1/user/password" && request.method == "PUT") {
        auto body = parse_json_body(request.body);
        
        controllers::UserController::ChangePasswordRequest user_request;
        user_request.access_token = access_token;
        user_request.current_password = body.value("current_password", "");
        user_request.new_password = body.value("new_password", "");
        
        response_data = user_controller_->handle_change_password(user_request);
        
    } else if (request.path == "/api/v1/user/sessions" && request.method == "GET") {
        controllers::UserController::GetSessionsRequest user_request;
        user_request.access_token = access_token;
        
        response_data = user_controller_->handle_get_sessions(user_request);
        
    } else if (request.path.starts_with("/api/v1/user/sessions/") && request.method == "DELETE") {
        std::string session_id = extract_path_parameter(request.path, "/api/v1/user/sessions/([^/]+)", "session_id");
        
        controllers::UserController::TerminateSessionRequest user_request;
        user_request.access_token = access_token;
        user_request.session_id = session_id;
        
        response_data = user_controller_->handle_terminate_session(user_request);
        
    } else if (request.path == "/api/v1/user/sessions" && request.method == "DELETE") {
        response_data = user_controller_->handle_terminate_all_sessions(access_token);
        
    } else if (request.path == "/api/v1/user/search" && request.method == "GET") {
        auto params = parse_query_params(request.query_string);
        
        controllers::UserController::SearchUsersRequest user_request;
        user_request.access_token = access_token;
        user_request.query = params["q"];
        user_request.limit = std::stoi(params.value("limit", "20"));
        user_request.offset = std::stoi(params.value("offset", "0"));
        
        response_data = user_controller_->handle_search_users(user_request);
        
    } else {
        return create_error_response(404, "User endpoint not found");
    }
    
    int status_code = response_data.value("success", false) ? 200 : 400;
    return create_json_response(status_code, response_data);
}

HttpHandler::HttpResponse HttpHandler::handle_profile_routes(const HttpRequest& request) {
    nlohmann::json response_data;
    auto auth_header = request.headers.find("Authorization");
    std::string access_token;
    
    if (auth_header != request.headers.end()) {
        access_token = profile_controller_->extract_bearer_token(auth_header->second);
    }
    
    if (request.path.starts_with("/api/v1/profile/") && request.method == "GET") {
        std::string username = extract_path_parameter(request.path, "/api/v1/profile/([^/]+)", "username");
        
        controllers::ProfileController::GetPublicProfileRequest profile_request;
        profile_request.username = username;
        profile_request.viewer_token = access_token;
        
        response_data = profile_controller_->handle_get_public_profile(profile_request);
        
    } else if (request.path == "/api/v1/profile/privacy" && request.method == "PUT") {
        auto body = parse_json_body(request.body);
        
        controllers::ProfileController::UpdatePrivacySettingsRequest profile_request;
        profile_request.access_token = access_token;
        profile_request.is_private_account = body.value("is_private_account", false);
        profile_request.allow_message_requests = body.value("allow_message_requests", true);
        profile_request.show_activity_status = body.value("show_activity_status", true);
        profile_request.show_read_receipts = body.value("show_read_receipts", true);
        
        response_data = profile_controller_->handle_update_privacy_settings(profile_request);
        
    } else if (request.path == "/api/v1/profile/block" && request.method == "NOTE") {
        auto body = parse_json_body(request.body);
        
        controllers::ProfileController::BlockUserRequest profile_request;
        profile_request.access_token = access_token;
        profile_request.user_id_to_block = body.value("user_id", "");
        
        response_data = profile_controller_->handle_block_user(profile_request);
        
    } else if (request.path == "/api/v1/profile/followers" && request.method == "GET") {
        auto params = parse_query_params(request.query_string);
        
        controllers::ProfileController::GetFollowersRequest profile_request;
        profile_request.access_token = access_token;
        profile_request.user_id = params.value("user_id", "");
        profile_request.limit = std::stoi(params.value("limit", "20"));
        profile_request.offset = std::stoi(params.value("offset", "0"));
        
        response_data = profile_controller_->handle_get_followers(profile_request);
        
    } else {
        return create_error_response(404, "Profile endpoint not found");
    }
    
    int status_code = response_data.value("success", false) ? 200 : 400;
    return create_json_response(status_code, response_data);
}

// Utility methods

std::map<std::string, std::string> HttpHandler::parse_query_params(const std::string& query_string) {
    std::map<std::string, std::string> params;
    
    if (query_string.empty()) {
        return params;
    }
    
    std::stringstream ss(query_string);
    std::string pair;
    
    while (std::getline(ss, pair, '&')) {
        size_t eq_pos = pair.find('=');
        if (eq_pos != std::string::npos) {
            std::string key = pair.substr(0, eq_pos);
            std::string value = pair.substr(eq_pos + 1);
            
            // URL decode (basic implementation)
            // In production, use a proper URL decode function
            std::replace(value.begin(), value.end(), '+', ' ');
            
            params[key] = value;
        }
    }
    
    return params;
}

nlohmann::json HttpHandler::parse_json_body(const std::string& body) {
    try {
        if (body.empty()) {
            return nlohmann::json{};
        }
        return nlohmann::json::parse(body);
    } catch (const std::exception& e) {
        spdlog::warn("Failed to parse JSON body: {}", e.what());
        return nlohmann::json{};
    }
}

std::string HttpHandler::extract_path_parameter(const std::string& path, const std::string& pattern, const std::string& param_name) {
    std::regex path_regex(pattern);
    std::smatch match;
    
    if (std::regex_match(path, match, path_regex) && match.size() > 1) {
        return match[1].str();
    }
    
    return "";
}

// Response builders

HttpHandler::HttpResponse HttpHandler::create_json_response(int status_code, const nlohmann::json& data) {
    HttpResponse response;
    response.status_code = status_code;
    response.content_type = "application/json";
    response.body = data.dump();
    response.headers["Cache-Control"] = "no-store";
    return response;
}

HttpHandler::HttpResponse HttpHandler::create_error_response(int status_code, const std::string& message) {
    nlohmann::json error_data;
    error_data["success"] = false;
    error_data["message"] = message;
    error_data["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    return create_json_response(status_code, error_data);
}

HttpHandler::HttpResponse HttpHandler::create_options_response() {
    HttpResponse response;
    response.status_code = 200;
    response.content_type = "text/plain";
    response.body = "";
    response.headers["Access-Control-Allow-Origin"] = "*";
    response.headers["Access-Control-Allow-Methods"] = "GET, NOTE, PUT, DELETE, OPTIONS";
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization";
    response.headers["Access-Control-Max-Age"] = "86400";
    return response;
}

bool HttpHandler::validate_request(const HttpRequest& request) {
    // Basic validation
    if (request.method.empty() || request.path.empty()) {
        return false;
    }
    
    // Validate JSON body for NOTE/PUT requests
    if ((request.method == "NOTE" || request.method == "PUT") && !request.body.empty()) {
        return is_valid_json(request.body);
    }
    
    return true;
}

bool HttpHandler::is_valid_json(const std::string& body) {
    try {
        nlohmann::json::parse(body);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool HttpHandler::is_rate_limited(const std::string& client_ip, const std::string& endpoint) {
    // Basic rate limiting implementation
    // In production, this would use Redis or similar
    
    // For now, allow all requests
    return false;
}

void HttpHandler::add_security_headers(HttpResponse& response) {
    response.headers["X-Content-Type-Options"] = "nosniff";
    response.headers["X-Frame-Options"] = "DENY";
    response.headers["X-XSS-Protection"] = "1; mode=block";
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains";
    response.headers["Content-Security-Policy"] = "default-src 'self'";
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin";
    response.headers["Access-Control-Allow-Origin"] = "*";  // Configure appropriately for production
}

void HttpHandler::log_request(const HttpRequest& request, const HttpResponse& response) {
    spdlog::info("HTTP {} {} - {} - {} bytes from {}",
        request.method,
        request.path,
        response.status_code,
        response.body.length(),
        request.client_ip
    );
}

} // namespace sonet::user::handlers
