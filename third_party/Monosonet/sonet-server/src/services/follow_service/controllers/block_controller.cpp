/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "block_controller.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <regex>
#include <algorithm>

namespace sonet::follow::controllers {

using namespace std::chrono;
using json = nlohmann::json;

// ========== CONSTRUCTOR & INITIALIZATION ==========

BlockController::BlockController(std::shared_ptr<FollowService> follow_service,
                                const json& config)
    : follow_service_(follow_service),
      config_(config),
      start_time_(steady_clock::now()),
      request_count_(0),
      avg_response_time_(0.0) {
    
    spdlog::info("🚫 Initializing Twitter-Scale Block Controller...");
    
    // Initialize configuration
    max_request_size_ = config_.value("max_request_size", 1024 * 1024); // 1MB
    rate_limit_per_minute_ = config_.value("rate_limit_per_minute", 100); // Lower for blocking
    enable_cors_ = config_.value("enable_cors", true);
    require_auth_ = config_.value("require_auth", true);
    
    // Initialize rate limiting
    rate_limiter_ = std::make_unique<RateLimiter>(rate_limit_per_minute_);
    
    // Initialize request tracking
    operation_counts_.clear();
    operation_times_.clear();
    
    spdlog::info("✅ Block Controller initialized: rate_limit={}/min, cors={}, auth={}", 
                rate_limit_per_minute_, enable_cors_, require_auth_);
}

// ========== CORE BLOCKING ENDPOINTS ==========

HttpResponse BlockController::block_user(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🚫 NOTE /api/v1/block/{} from {}", request.path_params.at("user_id"), 
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
        std::string blocker_id = auth_result.user_id;
        std::string blocked_id = request.path_params.at("user_id");
        
        // Validate input
        auto validation_result = validate_user_ids(blocker_id, blocked_id);
        if (!validation_result.success) {
            return create_error_response(400, "INVALID_INPUT", validation_result.message);
        }
        
        // Parse request body for reason and options
        json request_body;
        std::string block_reason = "user_initiated";
        bool report_spam = false;
        
        if (!request.body.empty()) {
            try {
                request_body = json::parse(request.body);
                block_reason = request_body.value("reason", "user_initiated");
                report_spam = request_body.value("report_spam", false);
            } catch (const std::exception& e) {
                return create_error_response(400, "INVALID_JSON", "Invalid JSON in request body");
            }
        }
        
        // Call service to block user
        auto result = follow_service_->block_user(blocker_id, blocked_id, block_reason);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("block_user", duration);
        
        if (result.value("success", false)) {
            spdlog::info("✅ Block successful: {} blocked {} (reason: {}) in {}μs", 
                        blocker_id, blocked_id, block_reason, duration);
            
            // Additional processing for spam reports
            if (report_spam) {
                auto spam_report_result = report_user_for_spam(blocker_id, blocked_id, request_body);
                result["spam_report"] = spam_report_result;
            }
            
            return create_success_response(200, result);
        } else {
            spdlog::warn("⚠️ Block failed: {} -> {} - {}", blocker_id, blocked_id, 
                        result.value("error_code", "UNKNOWN"));
            return create_error_response(400, result.value("error_code", "BLOCK_FAILED"), 
                                       result.value("message", "Block operation failed"));
        }
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Block endpoint error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

HttpResponse BlockController::unblock_user(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🔓 DELETE /api/v1/block/{} from {}", request.path_params.at("user_id"), 
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
        std::string blocker_id = auth_result.user_id;
        std::string blocked_id = request.path_params.at("user_id");
        
        // Validate input
        auto validation_result = validate_user_ids(blocker_id, blocked_id);
        if (!validation_result.success) {
            return create_error_response(400, "INVALID_INPUT", validation_result.message);
        }
        
        // Call service to unblock user
        auto result = follow_service_->unblock_user(blocker_id, blocked_id);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("unblock_user", duration);
        
        if (result.value("success", false)) {
            spdlog::info("✅ Unblock successful: {} unblocked {} in {}μs", 
                        blocker_id, blocked_id, duration);
            return create_success_response(200, result);
        } else {
            spdlog::warn("⚠️ Unblock failed: {} -> {} - {}", blocker_id, blocked_id, 
                        result.value("error_code", "UNKNOWN"));
            return create_error_response(400, result.value("error_code", "UNBLOCK_FAILED"), 
                                       result.value("message", "Unblock operation failed"));
        }
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Unblock endpoint error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

HttpResponse BlockController::get_blocked_users(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📋 GET /api/v1/blocked-users from {}", 
                     request.headers.value("user-id", "unknown"));
        
        // Authentication
        auto auth_result = authenticate_request(request);
        if (!auth_result.success) {
            return create_error_response(401, "UNAUTHORIZED", auth_result.message);
        }
        
        // Parse query parameters
        int limit = parse_int_param(request.query_params, "limit", 50);
        std::string cursor = request.query_params.value("cursor", "");
        
        // Validate parameters
        if (limit <= 0 || limit > 200) {
            return create_error_response(400, "INVALID_LIMIT", "Limit must be between 1 and 200");
        }
        
        // Get blocked users
        auto blocked_users = follow_service_->get_blocked_users(auth_result.user_id, limit, cursor);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_blocked_users", duration);
        
        spdlog::debug("✅ Blocked users retrieved for {}: {} results in {}μs", 
                     auth_result.user_id, blocked_users.value("count", 0), duration);
        
        return create_success_response(200, blocked_users);
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get blocked users error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

HttpResponse BlockController::check_block_status(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🔍 GET /api/v1/block-status/{} from {}", request.path_params.at("user_id"), 
                     request.headers.value("user-id", "unknown"));
        
        // Authentication
        auto auth_result = authenticate_request(request);
        if (!auth_result.success) {
            return create_error_response(401, "UNAUTHORIZED", auth_result.message);
        }
        
        // Extract parameters
        std::string user1_id = auth_result.user_id;
        std::string user2_id = request.path_params.at("user_id");
        
        // Validate input
        auto validation_result = validate_user_ids(user1_id, user2_id);
        if (!validation_result.success) {
            return create_error_response(400, "INVALID_INPUT", validation_result.message);
        }
        
        // Get block status
        auto block_status = follow_service_->get_block_status(user1_id, user2_id);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("check_block_status", duration);
        
        spdlog::debug("✅ Block status check: {} <-> {} in {}μs", user1_id, user2_id, duration);
        
        return create_success_response(200, block_status);
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Block status check error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

// ========== MUTING ENDPOINTS ==========

HttpResponse BlockController::mute_user(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🔇 NOTE /api/v1/mute/{} from {}", request.path_params.at("user_id"), 
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
        std::string muter_id = auth_result.user_id;
        std::string muted_id = request.path_params.at("user_id");
        
        // Validate input
        auto validation_result = validate_user_ids(muter_id, muted_id);
        if (!validation_result.success) {
            return create_error_response(400, "INVALID_INPUT", validation_result.message);
        }
        
        // Parse request body for mute options
        json request_body;
        std::string mute_duration = "permanent";
        bool include_retweets = true;
        
        if (!request.body.empty()) {
            try {
                request_body = json::parse(request.body);
                mute_duration = request_body.value("duration", "permanent");
                include_retweets = request_body.value("include_retweets", true);
            } catch (const std::exception& e) {
                return create_error_response(400, "INVALID_JSON", "Invalid JSON in request body");
            }
        }
        
        // Call service to mute user
        auto result = follow_service_->mute_user(muter_id, muted_id, mute_duration, include_retweets);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("mute_user", duration);
        
        if (result.value("success", false)) {
            spdlog::info("✅ Mute successful: {} muted {} (duration: {}) in {}μs", 
                        muter_id, muted_id, mute_duration, duration);
            return create_success_response(200, result);
        } else {
            spdlog::warn("⚠️ Mute failed: {} -> {} - {}", muter_id, muted_id, 
                        result.value("error_code", "UNKNOWN"));
            return create_error_response(400, result.value("error_code", "MUTE_FAILED"), 
                                       result.value("message", "Mute operation failed"));
        }
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Mute endpoint error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

HttpResponse BlockController::unmute_user(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🔊 DELETE /api/v1/mute/{} from {}", request.path_params.at("user_id"), 
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
        std::string muter_id = auth_result.user_id;
        std::string muted_id = request.path_params.at("user_id");
        
        // Validate input
        auto validation_result = validate_user_ids(muter_id, muted_id);
        if (!validation_result.success) {
            return create_error_response(400, "INVALID_INPUT", validation_result.message);
        }
        
        // Call service to unmute user
        auto result = follow_service_->unmute_user(muter_id, muted_id);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("unmute_user", duration);
        
        if (result.value("success", false)) {
            spdlog::info("✅ Unmute successful: {} unmuted {} in {}μs", 
                        muter_id, muted_id, duration);
            return create_success_response(200, result);
        } else {
            spdlog::warn("⚠️ Unmute failed: {} -> {} - {}", muter_id, muted_id, 
                        result.value("error_code", "UNKNOWN"));
            return create_error_response(400, result.value("error_code", "UNMUTE_FAILED"), 
                                       result.value("message", "Unmute operation failed"));
        }
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Unmute endpoint error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

HttpResponse BlockController::get_muted_users(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📋 GET /api/v1/muted-users from {}", 
                     request.headers.value("user-id", "unknown"));
        
        // Authentication
        auto auth_result = authenticate_request(request);
        if (!auth_result.success) {
            return create_error_response(401, "UNAUTHORIZED", auth_result.message);
        }
        
        // Parse query parameters
        int limit = parse_int_param(request.query_params, "limit", 50);
        std::string cursor = request.query_params.value("cursor", "");
        
        // Validate parameters
        if (limit <= 0 || limit > 200) {
            return create_error_response(400, "INVALID_LIMIT", "Limit must be between 1 and 200");
        }
        
        // Get muted users
        auto muted_users = follow_service_->get_muted_users(auth_result.user_id, limit, cursor);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_muted_users", duration);
        
        spdlog::debug("✅ Muted users retrieved for {}: {} results in {}μs", 
                     auth_result.user_id, muted_users.value("count", 0), duration);
        
        return create_success_response(200, muted_users);
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get muted users error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

// ========== BULK OPERATIONS ==========

HttpResponse BlockController::bulk_block(const HttpRequest& request) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📦 NOTE /api/v1/block/bulk from {}", 
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
        
        // Parse request body
        json request_body;
        try {
            request_body = json::parse(request.body);
        } catch (const std::exception& e) {
            return create_error_response(400, "INVALID_JSON", "Invalid JSON in request body");
        }
        
        // Validate bulk request
        if (!request_body.contains("user_ids") || !request_body["user_ids"].is_array()) {
            return create_error_response(400, "MISSING_USER_IDS", "user_ids array is required");
        }
        
        auto user_ids_json = request_body["user_ids"];
        if (user_ids_json.size() > 50) { // Lower limit for blocking
            return create_error_response(400, "TOO_MANY_USERS", "Maximum 50 users per bulk block operation");
        }
        
        std::vector<std::string> user_ids;
        for (const auto& id : user_ids_json) {
            if (id.is_string()) {
                user_ids.push_back(id.get<std::string>());
            }
        }
        
        if (user_ids.empty()) {
            return create_error_response(400, "EMPTY_USER_LIST", "At least one user ID is required");
        }
        
        std::string block_reason = request_body.value("reason", "bulk_operation");
        
        // Call service
        auto result = follow_service_->bulk_block(auth_result.user_id, user_ids, block_reason);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("bulk_block", duration);
        
        spdlog::info("✅ Bulk block completed for {}: {}/{} successful in {}μs", 
                    auth_result.user_id, result.value("successful", 0), user_ids.size(), duration);
        
        return create_success_response(200, result);
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Bulk block error: {} ({}μs)", e.what(), duration);
        return create_error_response(500, "INTERNAL_ERROR", "Internal server error");
    }
}

// ========== SPAM REPORTING ==========

json BlockController::report_user_for_spam(const std::string& reporter_id, 
                                          const std::string& reported_id, 
                                          const json& report_details) {
    try {
        spdlog::info("🚨 Spam report: {} reporting {} for spam", reporter_id, reported_id);
        
        // This would integrate with a spam detection service
        // For now, just log and return success
        
        json spam_report = {
            {"success", true},
            {"report_id", "spam_" + std::to_string(std::time(nullptr))},
            {"reporter_id", reporter_id},
            {"reported_id", reported_id},
            {"status", "submitted"},
            {"details", report_details.value("spam_details", json::object())},
            {"timestamp", duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()}
        };
        
        // TODO: Queue for spam analysis team
        // TODO: Update user reputation scores
        // TODO: Check for patterns across multiple reports
        
        return spam_report;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to report spam: {} -> {}: {}", reporter_id, reported_id, e.what());
        return json{{"success", false}, {"error", e.what()}};
    }
}

// ========== UTILITY & HELPER METHODS ==========

AuthResult BlockController::authenticate_request(const HttpRequest& request) {
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

RateLimitResult BlockController::check_rate_limit(const HttpRequest& request) {
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

ValidationResult BlockController::validate_user_ids(const std::string& user1_id, const std::string& user2_id) {
    if (user1_id.empty() || user2_id.empty()) {
        return {false, "User IDs cannot be empty"};
    }
    
    if (user1_id == user2_id) {
        return {false, "Cannot block/mute yourself"};
    }
    
    if (!is_valid_user_id(user1_id) || !is_valid_user_id(user2_id)) {
        return {false, "Invalid user ID format"};
    }
    
    return {true, ""};
}

bool BlockController::is_valid_user_id(const std::string& user_id) {
    // Validate user ID format (alphanumeric, underscore, hyphen, 3-64 characters)
    std::regex user_id_pattern("^[a-zA-Z0-9_-]{3,64}$");
    return std::regex_match(user_id, user_id_pattern);
}

int BlockController::parse_int_param(const std::unordered_map<std::string, std::string>& params,
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

std::string BlockController::validate_jwt_token(const std::string& token) {
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

HttpResponse BlockController::create_success_response(int status_code, const json& data) {
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

HttpResponse BlockController::create_error_response(int status_code, const std::string& error_code, 
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

void BlockController::track_operation_performance(const std::string& operation, int64_t duration_us) {
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

json BlockController::get_controller_metrics() const {
    auto uptime = duration_cast<seconds>(steady_clock::now() - start_time_).count();
    
    json metrics = {
        {"controller_name", "block_controller"},
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
