/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "controllers/auth_controller.h"
#include "controllers/user_controller.h"
#include "controllers/profile_controller.h"
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <functional>

namespace sonet::user::handlers {

/**
 * HTTP request handler for the User Service REST API.
 * Routes HTTP requests to appropriate controllers and manages request/response flow.
 */
class HttpHandler {
public:
    HttpHandler(
        std::shared_ptr<controllers::AuthController> auth_controller,
        std::shared_ptr<controllers::UserController> user_controller,
        std::shared_ptr<controllers::ProfileController> profile_controller
    );
    ~HttpHandler() = default;

    // HTTP request structure
    struct HttpRequest {
        std::string method;          // GET, NOTE, PUT, DELETE, etc.
        std::string path;           // /api/v1/auth/login
        std::string query_string;   // ?param=value
        std::string body;           // JSON request body
        std::map<std::string, std::string> headers;
        std::string client_ip;
        std::string user_agent;
    };

    // HTTP response structure
    struct HttpResponse {
        int status_code;
        std::string content_type;
        std::string body;
        std::map<std::string, std::string> headers;
    };

    // Main request handler
    HttpResponse handle_request(const HttpRequest& request);

    // Route handlers
    HttpResponse handle_auth_routes(const HttpRequest& request);
    HttpResponse handle_user_routes(const HttpRequest& request);
    HttpResponse handle_profile_routes(const HttpRequest& request);

    // Utility methods
    std::map<std::string, std::string> parse_query_params(const std::string& query_string);
    nlohmann::json parse_json_body(const std::string& body);
    std::string extract_path_parameter(const std::string& path, const std::string& pattern, const std::string& param_name);

private:
    std::shared_ptr<controllers::AuthController> auth_controller_;
    std::shared_ptr<controllers::UserController> user_controller_;
    std::shared_ptr<controllers::ProfileController> profile_controller_;

    // Response builders
    HttpResponse create_json_response(int status_code, const nlohmann::json& data);
    HttpResponse create_error_response(int status_code, const std::string& message);
    HttpResponse create_cors_response();
    HttpResponse create_options_response();

    // Request validation
    bool validate_request(const HttpRequest& request);
    bool is_valid_json(const std::string& body);
    
    // Rate limiting
    bool is_rate_limited(const std::string& client_ip, const std::string& endpoint);
    
    // Security headers
    void add_security_headers(HttpResponse& response);
    
    // Logging
    void log_request(const HttpRequest& request, const HttpResponse& response);
};

} // namespace sonet::user::handlers
