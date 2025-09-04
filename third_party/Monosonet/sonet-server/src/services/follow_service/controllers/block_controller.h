/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <future>
#include <nlohmann/json.hpp>
#include "../../core/network/http_request.h"
#include "../../core/network/http_response.h"
#include "../services/follow_service.h"

namespace sonet::follow::controllers {

using json = nlohmann::json;
using namespace sonet::core::network;

/**
 * @brief Twitter-Scale Block Controller
 * 
 * High-performance HTTP REST API controller for user blocking operations.
 * Handles blocking, unblocking, and block status management with sub-1ms response times.
 * 
 * Features:
 * - Block/unblock users with automatic follow removal
 * - Bulk block operations for content moderation
 * - Block list management and pagination
 * - Block analytics and reporting
 * - Real-time block status updates
 * - Rate limiting and abuse prevention
 * 
 * Performance: Sub-1ms response times, 100k+ concurrent requests
 * Cache: Multi-layer caching with Redis and memory
 * Analytics: Real-time metrics and performance tracking
 */
class BlockController {
public:
    /**
     * @brief Constructor
     * @param follow_service Shared pointer to follow service
     * @param config Configuration JSON object
     */
    explicit BlockController(std::shared_ptr<FollowService> follow_service,
                           const json& config = {});
    
    ~BlockController() = default;
    
    // ========== CORE BLOCK OPERATIONS ==========
    
    /**
     * @brief Block a user
     * 
     * NOTE /api/v1/blocks
     * Body: {"user_id": "user123"}
     * 
     * Response: 201 Created
     * {
     *   "success": true,
     *   "message": "User blocked successfully",
     *   "block": {
     *     "blocker_id": "current_user",
     *     "blocked_id": "user123",
     *     "blocked_at": "2025-08-06T12:00:00Z",
     *     "block_reason": "spam"
     *   },
     *   "analytics": {
     *     "total_blocks": 15,
     *     "blocks_today": 3,
     *     "processing_time_ms": 0.8
     *   }
     * }
     * 
     * @param request HTTP request with user_id in body
     * @return HTTP response with block confirmation
     */
    HttpResponse block_user(const HttpRequest& request);
    
    /**
     * @brief Unblock a user
     * 
     * DELETE /api/v1/blocks/{user_id}
     * 
     * Response: 200 OK
     * {
     *   "success": true,
     *   "message": "User unblocked successfully",
     *   "unblock": {
     *     "blocker_id": "current_user",
     *     "unblocked_id": "user123",
     *     "unblocked_at": "2025-08-06T12:30:00Z",
     *     "block_duration_hours": 0.5
     *   },
     *   "analytics": {
     *     "total_blocks": 14,
     *     "processing_time_ms": 0.6
     *   }
     * }
     * 
     * @param request HTTP request with user_id in path
     * @return HTTP response with unblock confirmation
     */
    HttpResponse unblock_user(const HttpRequest& request);
    
    /**
     * @brief Check if user is blocked
     * 
     * GET /api/v1/blocks/status/{user_id}
     * 
     * Response: 200 OK
     * {
     *   "is_blocked": true,
     *   "is_blocking": false,
     *   "mutual_block": false,
     *   "block_details": {
     *     "blocked_at": "2025-08-06T11:30:00Z",
     *     "block_reason": "harassment",
     *     "can_unblock": true
     *   },
     *   "processing_time_ms": 0.3
     * }
     * 
     * @param request HTTP request with user_id in path
     * @return HTTP response with block status
     */
    HttpResponse get_block_status(const HttpRequest& request);
    
    // ========== BULK OPERATIONS ==========
    
    /**
     * @brief Block multiple users (batch operation)
     * 
     * NOTE /api/v1/blocks/batch
     * Body: {
     *   "user_ids": ["user1", "user2", "user3"],
     *   "block_reason": "spam_campaign",
     *   "notify_users": false
     * }
     * 
     * Response: 200 OK
     * {
     *   "success": true,
     *   "blocked_count": 3,
     *   "failed_count": 0,
     *   "results": [
     *     {"user_id": "user1", "status": "blocked", "reason": "success"},
     *     {"user_id": "user2", "status": "blocked", "reason": "success"},
     *     {"user_id": "user3", "status": "blocked", "reason": "success"}
     *   ],
     *   "processing_time_ms": 2.1
     * }
     * 
     * @param request HTTP request with user_ids array in body
     * @return HTTP response with batch block results
     */
    HttpResponse bulk_block_users(const HttpRequest& request);
    
    /**
     * @brief Unblock multiple users (batch operation)
     * 
     * DELETE /api/v1/blocks/batch
     * Body: {"user_ids": ["user1", "user2", "user3"]}
     * 
     * @param request HTTP request with user_ids array in body
     * @return HTTP response with batch unblock results
     */
    HttpResponse bulk_unblock_users(const HttpRequest& request);
    
    // ========== BLOCK LISTS & MANAGEMENT ==========
    
    /**
     * @brief Get user's block list
     * 
     * GET /api/v1/blocks?page=1&limit=20&sort=recent
     * 
     * Response: 200 OK
     * {
     *   "blocks": [
     *     {
     *       "blocked_user": {
     *         "id": "user123",
     *         "username": "spammer",
     *         "display_name": "Spam Account"
     *       },
     *       "blocked_at": "2025-08-06T11:30:00Z",
     *       "block_reason": "spam",
     *       "block_duration_hours": 12.5
     *     }
     *   ],
     *   "pagination": {
     *     "page": 1,
     *     "limit": 20,
     *     "total": 47,
     *     "has_next": true,
     *     "next_cursor": "block_cursor_abc123"
     *   },
     *   "analytics": {
     *     "total_blocks": 47,
     *     "blocks_this_week": 8,
     *     "most_common_reason": "spam"
     *   }
     * }
     * 
     * @param request HTTP request with pagination parameters
     * @return HTTP response with paginated block list
     */
    HttpResponse get_block_list(const HttpRequest& request);
    
    /**
     * @brief Get users who blocked current user
     * 
     * GET /api/v1/blocks/blocking-me?page=1&limit=20
     * 
     * @param request HTTP request with pagination parameters
     * @return HTTP response with users blocking current user
     */
    HttpResponse get_blocked_by_list(const HttpRequest& request);
    
    /**
     * @brief Clear all blocks (with confirmation)
     * 
     * DELETE /api/v1/blocks/all
     * Body: {"confirmation": "CLEAR_ALL_BLOCKS"}
     * 
     * @param request HTTP request with confirmation token
     * @return HTTP response with clear operation results
     */
    HttpResponse clear_all_blocks(const HttpRequest& request);
    
    // ========== ANALYTICS & REPORTING ==========
    
    /**
     * @brief Get block analytics
     * 
     * GET /api/v1/blocks/analytics?period=7d&include_reasons=true
     * 
     * Response: 200 OK
     * {
     *   "analytics": {
     *     "total_blocks": 156,
     *     "active_blocks": 143,
     *     "blocks_this_period": 23,
     *     "block_rate_per_day": 3.3,
     *     "top_block_reasons": [
     *       {"reason": "spam", "count": 89, "percentage": 57.1},
     *       {"reason": "harassment", "count": 34, "percentage": 21.8},
     *       {"reason": "inappropriate", "count": 33, "percentage": 21.2}
     *     ],
     *     "block_patterns": {
     *       "peak_block_hour": 14,
     *       "most_active_day": "Monday",
     *       "average_block_duration_hours": 168.5
     *     },
     *     "moderation_impact": {
     *       "content_filtered": 2456,
     *       "notifications_prevented": 1234,
     *       "harassment_incidents_prevented": 45
     *     }
     *   },
     *   "processing_time_ms": 1.2
     * }
     * 
     * @param request HTTP request with analytics parameters
     * @return HTTP response with comprehensive block analytics
     */
    HttpResponse get_block_analytics(const HttpRequest& request);
    
    /**
     * @brief Export block data
     * 
     * GET /api/v1/blocks/export?format=json&include_reasons=true
     * 
     * @param request HTTP request with export parameters
     * @return HTTP response with exported block data
     */
    HttpResponse export_block_data(const HttpRequest& request);
    
    // ========== MODERATION & SAFETY ==========
    
    /**
     * @brief Report a user and automatically block
     * 
     * NOTE /api/v1/blocks/report-and-block
     * Body: {
     *   "user_id": "user123",
     *   "report_reason": "harassment",
     *   "evidence": ["message_id_1", "message_id_2"],
     *   "auto_block": true
     * }
     * 
     * @param request HTTP request with report and block data
     * @return HTTP response with report and block confirmation
     */
    HttpResponse report_and_block_user(const HttpRequest& request);
    
    /**
     * @brief Get block recommendations based on user behavior
     * 
     * GET /api/v1/blocks/recommendations?limit=10&min_confidence=0.8
     * 
     * @param request HTTP request with recommendation parameters
     * @return HTTP response with suggested users to block
     */
    HttpResponse get_block_recommendations(const HttpRequest& request);
    
    // ========== PERFORMANCE & MONITORING ==========
    
    /**
     * @brief Get controller performance metrics
     * 
     * GET /api/v1/blocks/metrics
     * 
     * @param request HTTP request
     * @return HTTP response with performance metrics
     */
    HttpResponse get_performance_metrics(const HttpRequest& request);

private:
    // ========== PRIVATE MEMBERS ==========
    
    std::shared_ptr<FollowService> follow_service_;
    json config_;
    
    // Performance tracking
    mutable std::atomic<uint64_t> request_count_{0};
    mutable std::atomic<uint64_t> total_processing_time_us_{0};
    mutable std::atomic<uint64_t> cache_hits_{0};
    mutable std::atomic<uint64_t> cache_misses_{0};
    
    // Configuration
    int default_page_size_;
    int max_page_size_;
    int bulk_operation_limit_;
    bool enable_analytics_;
    bool enable_rate_limiting_;
    
    // ========== PRIVATE HELPER METHODS ==========
    
    /**
     * @brief Extract user ID from request path
     * @param path Request path like "/api/v1/blocks/user123"
     * @return Extracted user ID
     */
    std::string extract_user_id_from_path(const std::string& path) const;
    
    /**
     * @brief Validate block request parameters
     * @param request HTTP request to validate
     * @return Validation result with error details
     */
    json validate_block_request(const HttpRequest& request) const;
    
    /**
     * @brief Parse pagination parameters from request
     * @param request HTTP request with query parameters
     * @return Parsed pagination object
     */
    json parse_pagination_params(const HttpRequest& request) const;
    
    /**
     * @brief Create error response
     * @param error_code HTTP status code
     * @param message Error message
     * @param details Additional error details
     * @return HTTP error response
     */
    HttpResponse create_error_response(int error_code, 
                                     const std::string& message,
                                     const json& details = {}) const;
    
    /**
     * @brief Create success response
     * @param data Response data
     * @param processing_time_ms Processing time in milliseconds
     * @return HTTP success response
     */
    HttpResponse create_success_response(const json& data, 
                                       double processing_time_ms) const;
    
    /**
     * @brief Track operation performance
     * @param operation Operation name
     * @param duration_us Duration in microseconds
     */
    void track_performance(const std::string& operation, uint64_t duration_us) const;
    
    /**
     * @brief Validate bulk operation limits
     * @param user_ids List of user IDs to validate
     * @return Validation result
     */
    bool validate_bulk_limits(const std::vector<std::string>& user_ids) const;
    
    /**
     * @brief Apply rate limiting for block operations
     * @param user_id User performing the operation
     * @param operation_type Type of operation (block/unblock)
     * @return Whether operation is allowed
     */
    bool check_rate_limits(const std::string& user_id, 
                          const std::string& operation_type) const;
};

} // namespace sonet::follow::controllers