/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "follow_controller.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <regex>
#include <algorithm>

namespace sonet::follow::controllers {

using namespace std::chrono;
using json = nlohmann::json;

// ========== CONSTRUCTOR & INITIALIZATION ==========

FollowController::FollowController(std::shared_ptr<FollowService> follow_service,
                                   const json& config)
    : follow_service_(follow_service),
      config_(config),
      start_time_(steady_clock::now()),
      request_count_(0),
      avg_response_time_(0.0) {
    
    spdlog::info("🌐 Initializing Twitter-Scale Follow Controller...");
    
    // Initialize configuration
    max_request_size_ = config_.value("max_request_size", 1024 * 1024); // 1MB
    rate_limit_per_minute_ = config_.value("rate_limit_per_minute", 1000);
    enable_cors_ = config_.value("enable_cors", true);
    require_auth_ = config_.value("require_auth", true);
    
    // Initialize rate limiting
    rate_limiter_ = std::make_unique<RateLimiter>(rate_limit_per_minute_);
    
    // Initialize request tracking
    operation_counts_.clear();
    operation_times_.clear();
    
    spdlog::info("✅ Follow Controller initialized: rate_limit={}/min, cors={}, auth={}", 
                rate_limit_per_minute_, enable_cors_, require_auth_);
}

// ========== CORE FOLLOW ENDPOINTS ==========

HttpResponse FollowController::follow_user(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📝 NOTE /api/v1/follow/{} from {}", request.path_params.at("user_id"), 
                     request.headers.value("user-id", "unknown"));
        
        // Authentication and validation
        auto auth_result = authenticate_request(request);
        if (!auth_result.success) {
            return create_error_response(401, "UNAUTHORIZED", auth_result.message);
        }
        
        auto rate_limit_result = check_rate_limit(request);
        if (!rate_limit_result.success) {
            return create_error_response(429, "RATE_LIMITED", rate_limit_result.message);
        }
        
        // Extract parameters
        std::string follower_id = auth_result.user_id;
        std::string following_id = request.path_params.at("user_id");
        
        // Validate input
        auto validation_result = validate_user_ids(follower_id, following_id);
        if (!validation_result.success) {
            return create_error_response(400, "INVALID_INPUT", validation_result.message);
        }
        
        // Parse request body
        json request_body;
        if (!request.body.empty()) {
            try {
                request_body = json::parse(request.body);
            } catch (const std::exception& e) {
                return create_error_response(400, "INVALID_JSON", "Invalid JSON in request body");
            }
        }
        
        std::string follow_type = request_body.value("type", "standard");
        std::string source = request_body.value("source", "api");
        
        // Call service
        auto result = follow_service_->follow_user(follower_id, following_id, follow_type, source);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("follow_user", duration);
        
        if (result.value("success", false)) {
            spdlog::info("✅ Follow successful: {} -> {} in {}μs", follower_id, following_id, duration);
            return create_success_response(200, result);
        } else {
            spdlog::warn("⚠️ Follow failed: {} -> {} - {}", follower_id, following_id, 
                        result.value("error_code", "UNKNOWN"));
            return create_error_response(400, result.value("error_code", "FOLLOW_FAILED"), 
                                       result.value("message", "Follow operation failed"));
        }
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Follow endpoint error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

HttpResponse FollowController::unfollow_user(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🗑️ DELETE /api/v1/follow/{} from {}", request.path_params.at("user_id"), 
                     request.headers.value("user-id", "unknown"));
        
        // Authentication and validation
        auto auth_result = authenticate_request(request);
        if (!auth_result.success) {
            return create_error_response(401, "UNAUTHORIZED", auth_result.message);
        }
        
        auto rate_limit_result = check_rate_limit(request);
        if (!rate_limit_result.success) {
            return create_error_response(429, "RATE_LIMITED", rate_limit_result.message);
        }
        
        // Extract parameters
        std::string follower_id = auth_result.user_id;
        std::string following_id = request.path_params.at("user_id");
        
        // Validate input
        auto validation_result = validate_user_ids(follower_id, following_id);
        if (!validation_result.success) {
            return create_error_response(400, "INVALID_INPUT", validation_result.message);
        }
        
        // Call service
        auto result = follow_service_->unfollow_user(follower_id, following_id);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("unfollow_user", duration);
        
        if (result.value("success", false)) {
            spdlog::info("✅ Unfollow successful: {} -> {} in {}μs", follower_id, following_id, duration);
            return create_success_response(200, result);
        } else {
            spdlog::warn("⚠️ Unfollow failed: {} -> {} - {}", follower_id, following_id, 
                        result.value("error_code", "UNKNOWN"));
            return create_error_response(400, result.value("error_code", "UNFOLLOW_FAILED"), 
                                       result.value("message", "Unfollow operation failed"));
        }
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Unfollow endpoint error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

HttpResponse FollowController::get_followers(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📋 GET /api/v1/users/{}/followers from {}", request.path_params.at("user_id"), 
                     request.headers.value("user-id", "unknown"));
        
        // Authentication
        auto auth_result = authenticate_request(request);
        if (!auth_result.success) {
            return create_error_response(401, "UNAUTHORIZED", auth_result.message);
        }
        
        // Extract parameters
        std::string user_id = request.path_params.at("user_id");
        std::string requester_id = auth_result.user_id;
        
        // Parse query parameters
        int limit = parse_int_param(request.query_params, "limit", 50);
        std::string cursor = request.query_params.value("cursor", "");
        
        // Validate parameters
        if (limit <= 0 || limit > 1000) {
            return create_error_response(400, "INVALID_LIMIT", "Limit must be between 1 and 1000");
        }
        
        if (!is_valid_user_id(user_id)) {
            return create_error_response(400, "INVALID_USER_ID", "Invalid user ID format");
        }
        
        // Get followers
        auto followers = follow_service_->get_followers(user_id, limit, cursor, requester_id);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_followers", duration);
        
        spdlog::debug("✅ Followers retrieved for {}: {} results in {}μs", 
                     user_id, followers.value("count", 0), duration);
        
        return create_success_response(200, followers);
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get followers error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

HttpResponse FollowController::get_following(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📋 GET /api/v1/users/{}/following from {}", request.path_params.at("user_id"), 
                     request.headers.value("user-id", "unknown"));
        
        // Authentication
        auto auth_result = authenticate_request(request);
        if (!auth_result.success) {
            return create_error_response(401, "UNAUTHORIZED", auth_result.message);
        }
        
        // Extract parameters
        std::string user_id = request.path_params.at("user_id");
        std::string requester_id = auth_result.user_id;
        
        // Parse query parameters
        int limit = parse_int_param(request.query_params, "limit", 50);
        std::string cursor = request.query_params.value("cursor", "");
        
        // Validate parameters
        if (limit <= 0 || limit > 1000) {
            return create_error_response(400, "INVALID_LIMIT", "Limit must be between 1 and 1000");
        }
        
        if (!is_valid_user_id(user_id)) {
            return create_error_response(400, "INVALID_USER_ID", "Invalid user ID format");
        }
        
        // Get following
        auto following = follow_service_->get_following(user_id, limit, cursor, requester_id);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_following", duration);
        
        spdlog::debug("✅ Following retrieved for {}: {} results in {}μs", 
                     user_id, following.value("count", 0), duration);
        
        return create_success_response(200, following);
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get following error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

// ========== UTILITY & HELPER METHODS ==========

AuthResult FollowController::authenticate_request(const HttpRequest& request) {
    try {
        if (!require_auth_) {
            // For development/testing - extract user ID from header
            std::string user_id = request.headers.value("user-id", "");
            if (user_id.empty()) {
                return {false, "", "User ID required in header"};
            }
            return {true, user_id, ""};
        }
        
        // Extract Authorization header
        std::string auth_header = request.headers.value("authorization", "");
        if (auth_header.empty()) {
            return {false, "", "Authorization header required"};
        }
        
        // Validate Bearer token format
        if (auth_header.substr(0, 7) != "Bearer ") {
            return {false, "", "Invalid authorization format"};
        }
        
        std::string token = auth_header.substr(7);
        if (token.empty()) {
            return {false, "", "Token is required"};
        }
        
        // Validate JWT token (simplified - would use actual JWT library)
        auto user_id = validate_jwt_token(token);
        if (user_id.empty()) {
            return {false, "", "Invalid or expired token"};
        }
        
        return {true, user_id, ""};
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Authentication error: {}", e.what());
        return {false, "", "Authentication failed"};
    }
}

RateLimitResult FollowController::check_rate_limit(const HttpRequest& request) {
    try {
        std::string client_id = request.headers.value("user-id", request.headers.value("x-forwarded-for", "unknown"));
        
        if (rate_limiter_->is_allowed(client_id)) {
            return {true, ""};
        } else {
            auto reset_time = rate_limiter_->get_reset_time(client_id);
            return {false, "Rate limit exceeded. Try again in " + std::to_string(reset_time) + " seconds"};
        }
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Rate limit check error: {}", e.what());
        return {true, ""}; // Allow request on error
    }
}

ValidationResult FollowController::validate_user_ids(const std::string& user1_id, const std::string& user2_id) {
    if (user1_id.empty() || user2_id.empty()) {
        return {false, "User IDs cannot be empty"};
    }
    
    if (user1_id == user2_id) {
        return {false, "User IDs cannot be the same"};
    }
    
    if (!is_valid_user_id(user1_id) || !is_valid_user_id(user2_id)) {
        return {false, "Invalid user ID format"};
    }
    
    return {true, ""};
}

bool FollowController::is_valid_user_id(const std::string& user_id) {
    // Validate user ID format (alphanumeric, underscore, hyphen, 3-64 characters)
    std::regex user_id_pattern("^[a-zA-Z0-9_-]{3,64}$");
    return std::regex_match(user_id, user_id_pattern);
}

int FollowController::parse_int_param(const std::unordered_map<std::string, std::string>& params,
                                     const std::string& key, int default_value) {
    auto it = params.find(key);
    if (it != params.end()) {
        try {
            return std::stoi(it->second);
        } catch (const std::exception&) {
            return default_value;
        }
    }
    return default_value;
}

std::string FollowController::validate_jwt_token(const std::string& token) {
    // Simplified JWT validation - in real implementation would use proper JWT library
    // For now, just check if token looks valid and extract user ID
    
    if (token.length() < 10) {
        return "";
    }
    
    // Simulate token validation and user ID extraction
    // In real implementation: verify signature, check expiration, extract claims
    std::hash<std::string> hasher;
    auto hash = hasher(token);
    
    return "user_" + std::to_string(hash % 1000000);
}

HttpResponse FollowController::create_success_response(int status_code, const json& data) {
    json response_body = {
        {"success", true},
        {"timestamp", duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()},
        {"data", data}
    };
    
    HttpResponse response;
    response.status_code = status_code;
    response.body = response_body.dump();
    response.headers["Content-Type"] = "application/json";
    
    if (enable_cors_) {
        response.headers["Access-Control-Allow-Origin"] = "*";
        response.headers["Access-Control-Allow-Methods"] = "GET, NOTE, PUT, DELETE, OPTIONS";
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With";
    }
    
    return response;
}

HttpResponse FollowController::create_error_response(int status_code, const std::string& error_code, 
                                                    const std::string& message) {
    json response_body = {
        {"success", false},
        {"error_code", error_code},
        {"message", message},
        {"timestamp", duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()}
    };
    
    HttpResponse response;
    response.status_code = status_code;
    response.body = response_body.dump();
    response.headers["Content-Type"] = "application/json";
    
    if (enable_cors_) {
        response.headers["Access-Control-Allow-Origin"] = "*";
        response.headers["Access-Control-Allow-Methods"] = "GET, NOTE, PUT, DELETE, OPTIONS";
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With";
    }
    
    return response;
}

void FollowController::track_operation_performance(const std::string& operation, int64_t duration_us) {
    request_count_++;
    operation_counts_[operation]++;
    
    // Update running average
    if (operation_times_.find(operation) == operation_times_.end()) {
        operation_times_[operation] = static_cast<double>(duration_us);
    } else {
        operation_times_[operation] = (operation_times_[operation] + duration_us) / 2.0;
    }
    
    // Update overall average response time
    avg_response_time_ = (avg_response_time_ + duration_us) / 2.0;
}

json FollowController::get_controller_metrics() const {
    auto uptime = duration_cast<seconds>(steady_clock::now() - start_time_).count();
    
    json metrics = {
        {"controller_name", "follow_controller"},
        {"uptime_seconds", uptime},
        {"total_requests", request_count_.load()},
        {"avg_response_time_us", avg_response_time_.load()},
        {"requests_per_second", request_count_.load() / std::max(1.0, static_cast<double>(uptime))},
        {"operation_metrics", json::object()}
    };
    
    for (const auto& [operation, count] : operation_counts_) {
        metrics["operation_metrics"][operation] = {
            {"count", count},
            {"avg_duration_us", operation_times_.at(operation)}
        };
    }
    
    return metrics;
}

// ========== RATE LIMITER IMPLEMENTATION ==========

RateLimiter::RateLimiter(int max_requests_per_minute) 
    : max_requests_per_minute_(max_requests_per_minute) {
}

bool RateLimiter::is_allowed(const std::string& client_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto now = steady_clock::now();
    auto& bucket = buckets_[client_id];
    
    // Clean old entries
    bucket.erase(std::remove_if(bucket.begin(), bucket.end(),
                               [now](const auto& timestamp) {
                                   return duration_cast<minutes>(now - timestamp).count() >= 1;
                               }), bucket.end());
    
    // Check if under limit
    if (bucket.size() < static_cast<size_t>(max_requests_per_minute_)) {
        bucket.push_back(now);
        return true;
    }
    
    return false;
}

int RateLimiter::get_reset_time(const std::string& client_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = buckets_.find(client_id);
    if (it == buckets_.end() || it->second.empty()) {
        return 0;
    }
    
    auto oldest = *std::min_element(it->second.begin(), it->second.end());
    auto reset_time = oldest + minutes(1);
    auto now = steady_clock::now();
    
    if (reset_time > now) {
        return duration_cast<seconds>(reset_time - now).count();
    }
    
    return 0;
}

} // namespace sonet::follow::controllers
