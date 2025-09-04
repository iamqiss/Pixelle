/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../ghost_reply_service.h"
#include "../repositories/ghost_reply_repository.h"
#include "../validators/ghost_reply_validator.h"
#include "../services/ghost_reply_moderator.h"

// HTTP Framework includes
#include "../../../core/network/http_server.h"
#include "../../../core/network/http_request.h"
#include "../../../core/network/http_response.h"
#include "../../../core/network/websocket_server.h"
#include "../../../core/network/websocket_connection.h"

// Security and validation
#include "../../../core/validation/input_validator.h"
#include "../../../core/security/auth_middleware.h"
#include "../../../core/security/rate_limiter.h"
#include "../../../core/cache/redis_client.h"

#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <unordered_map>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>

namespace sonet::ghost_reply::controllers {

using namespace sonet::ghost_reply::repositories;
using json = nlohmann::json;

/**
 * @brief Ghost Reply RESTful Controller
 * 
 * Provides comprehensive HTTP REST API for anonymous ghost reply operations including:
 * - Anonymous ghost reply creation with custom avatars
 * - Ghost reply retrieval and search
 * - Moderation and content management
 * - Analytics and statistics
 * - Rate limiting and abuse prevention
 * - Real-time updates via WebSocket
 */
class GhostReplyController {
public:
    // Constructor with comprehensive service injection
    explicit GhostReplyController(
        std::shared_ptr<GhostReplyRepository> repository,
        std::shared_ptr<GhostReplyService> ghost_reply_service,
        std::shared_ptr<GhostReplyValidator> validator,
        std::shared_ptr<GhostReplyModerator> moderator,
        std::shared_ptr<sonet::core::cache::RedisClient> redis_client,
        std::shared_ptr<sonet::core::security::RateLimiter> rate_limiter
    );

    // HTTP Route Registration for REST API
    void register_http_routes(std::shared_ptr<core::network::HttpServer> server);
    void register_websocket_handlers(std::shared_ptr<core::network::WebSocketServer> ws_server);

    // ========== CORE GHOST REPLY OPERATIONS ==========
    
    /**
     * @brief Create a new ghost reply
     * POST /api/v1/ghost-replies
     * 
     * Features:
     * - Anonymous reply creation
     * - Custom ghost avatar selection
     * - Ephemeral ID generation
     * - Content moderation and safety checks
     * - Spam and duplicate detection
     * 
     * Rate limit: 10 requests/minute per IP (anonymous)
     */
    core::network::HttpResponse create_ghost_reply(const core::network::HttpRequest& request);

    /**
     * @brief Get a specific ghost reply by ID
     * GET /api/v1/ghost-replies/{id}
     * 
     * Features:
     * - Ghost reply retrieval by UUID
     * - Anonymous user data (no personal info)
     * - Media attachment support
     * - Engagement metrics
     */
    core::network::HttpResponse get_ghost_reply(const core::network::HttpRequest& request);

    /**
     * @brief Get ghost replies for a specific thread
     * GET /api/v1/threads/{thread_id}/ghost-replies
     * 
     * Features:
     * - Paginated ghost reply retrieval
     * - Cursor-based pagination
     * - Sorting options (newest, oldest, most liked)
     * - Filtering by moderation status
     */
    core::network::HttpResponse get_ghost_replies_for_thread(const core::network::HttpRequest& request);

    /**
     * @brief Get ghost replies for a specific note
     * GET /api/v1/notes/{note_id}/ghost-replies
     * 
     * Features:
     * - Ghost replies for a specific note
     * - Pagination and sorting
     * - Moderation status filtering
     */
    core::network::HttpResponse get_ghost_replies_for_note(const core::network::HttpRequest& request);

    /**
     * @brief Search ghost replies
     * GET /api/v1/ghost-replies/search
     * 
     * Features:
     * - Full-text search across ghost reply content
     * - Tag-based search
     * - Language filtering
     * - Relevance scoring
     */
    core::network::HttpResponse search_ghost_replies(const core::network::HttpRequest& request);

    // ========== GHOST REPLY MANAGEMENT ==========
    
    /**
     * @brief Delete a ghost reply (soft delete)
     * DELETE /api/v1/ghost-replies/{id}
     * 
     * Features:
     * - Soft deletion for moderation
     * - Audit trail maintenance
     * - Content preservation for compliance
     */
    core::network::HttpResponse delete_ghost_reply(const core::network::HttpRequest& request);

    /**
     * @brief Hide a ghost reply
     * PATCH /api/v1/ghost-replies/{id}/hide
     * 
     * Features:
     * - Hide from public view
     * - Maintains data integrity
     * - Reversible action
     */
    core::network::HttpResponse hide_ghost_reply(const core::network::HttpRequest& request);

    /**
     * @brief Flag a ghost reply for moderation
     * POST /api/v1/ghost-replies/{id}/flag
     * 
     * Features:
     * - User reporting system
     * - Moderation queue management
     * - Abuse pattern detection
     */
    core::network::HttpResponse flag_ghost_reply(const core::network::HttpRequest& request);

    // ========== MODERATION OPERATIONS ==========
    
    /**
     * @brief Get pending moderation queue
     * GET /api/v1/moderation/ghost-replies/pending
     * 
     * Features:
     * - Moderator access only
     * - Prioritized content review
     * - Bulk moderation actions
     */
    core::network::HttpResponse get_pending_moderation(const core::network::HttpRequest& request);

    /**
     * @brief Moderate a ghost reply
     * POST /api/v1/moderation/ghost-replies/{id}/moderate
     * 
     * Features:
     * - Approve/reject/hide actions
     * - Reason tracking
     * - Moderation history
     */
    core::network::HttpResponse moderate_ghost_reply(const core::network::HttpRequest& request);

    /**
     * @brief Get flagged ghost replies
     * GET /api/v1/moderation/ghost-replies/flagged
     * 
     * Features:
     * - User-reported content
     * - Priority-based review
     * - Pattern analysis
     */
    core::network::HttpResponse get_flagged_ghost_replies(const core::network::HttpRequest& request);

    // ========== ANALYTICS AND STATISTICS ==========
    
    /**
     * @brief Get ghost reply statistics for a thread
     * GET /api/v1/threads/{thread_id}/ghost-replies/stats
     * 
     * Features:
     * - Engagement metrics
     * - Content quality scores
     * - Usage patterns
     */
    core::network::HttpResponse get_thread_ghost_reply_stats(const core::network::HttpRequest& request);

    /**
     * @brief Get ghost reply analytics
     * GET /api/v1/ghost-replies/{id}/analytics
     * 
     * Features:
     * - View count tracking
     * - Engagement metrics
     * - Geographic data (anonymous)
     */
    core::network::HttpResponse get_ghost_reply_analytics(const core::network::HttpRequest& request);

    /**
     * @brief Get ghost avatar usage statistics
     * GET /api/v1/ghost-replies/avatars/stats
     * 
     * Features:
     * - Avatar popularity
     * - Usage patterns
     * - Category preferences
     */
    core::network::HttpResponse get_ghost_avatar_stats(const core::network::HttpRequest& request);

    // ========== GHOST AVATAR MANAGEMENT ==========
    
    /**
     * @brief Get available ghost avatars
     * GET /api/v1/ghost-replies/avatars
     * 
     * Features:
     * - Available avatar list
     * - Category filtering
     * - Usage statistics
     */
    core::network::HttpResponse get_available_ghost_avatars(const core::network::HttpRequest& request);

    /**
     * @brief Get random ghost avatar
     * GET /api/v1/ghost-replies/avatars/random
     * 
     * Features:
     * - Random avatar selection
     * - Category-based selection
     * - Usage balancing
     */
    core::network::HttpResponse get_random_ghost_avatar(const core::network::HttpRequest& request);

    // ========== ENGAGEMENT OPERATIONS ==========
    
    /**
     * @brief Like a ghost reply
     * POST /api/v1/ghost-replies/{id}/like
     * 
     * Features:
     * - Anonymous like tracking
     * - Rate limiting
     * - Engagement metrics
     */
    core::network::HttpResponse like_ghost_reply(const core::network::HttpRequest& request);

    /**
     * @brief Unlike a ghost reply
     * DELETE /api/v1/ghost-replies/{id}/like
     * 
     * Features:
     * - Remove anonymous like
     * - Update engagement metrics
     */
    core::network::HttpResponse unlike_ghost_reply(const core::network::HttpRequest& request);

    // ========== MEDIA OPERATIONS ==========
    
    /**
     * @brief Add media to ghost reply
     * POST /api/v1/ghost-replies/{id}/media
     * 
     * Features:
     * - Media attachment support
     * - File validation
     * - Processing and optimization
     */
    core::network::HttpResponse add_media_to_ghost_reply(const core::network::HttpRequest& request);

    /**
     * @brief Remove media from ghost reply
     * DELETE /api/v1/ghost-replies/{id}/media/{media_id}
     * 
     * Features:
     * - Media removal
     * - Order reindexing
     */
    core::network::HttpResponse remove_media_from_ghost_reply(const core::network::HttpRequest& request);

    // ========== ADMINISTRATIVE OPERATIONS ==========
    
    /**
     * @brief Get service health status
     * GET /api/v1/ghost-replies/health
     * 
     * Features:
     * - Service health monitoring
     * - Database connectivity
     * - Performance metrics
     */
    core::network::HttpResponse get_service_health(const core::network::HttpRequest& request);

    /**
     * @brief Cleanup old data
     * POST /api/v1/ghost-replies/admin/cleanup
     * 
     * Features:
     * - Data retention management
     * - Performance optimization
     * - Storage management
     */
    core::network::HttpResponse cleanup_old_data(const core::network::HttpRequest& request);

private:
    // Service dependencies
    std::shared_ptr<GhostReplyRepository> repository_;
    std::shared_ptr<GhostReplyService> ghost_reply_service_;
    std::shared_ptr<GhostReplyValidator> validator_;
    std::shared_ptr<GhostReplyModerator> moderator_;
    std::shared_ptr<sonet::core::cache::RedisClient> redis_client_;
    std::shared_ptr<sonet::core::security::RateLimiter> rate_limiter_;

    // Helper methods
    bool validate_ghost_reply_request(const json& request_data, std::string& error_message);
    bool check_rate_limit_for_anonymous_user(const std::string& ip_address);
    bool check_content_safety(const std::string& content);
    json build_ghost_reply_response(const GhostReply& ghost_reply);
    json build_ghost_replies_response(const std::vector<GhostReply>& ghost_replies);
    json build_error_response(const std::string& error, int status_code);
    json build_success_response(const std::string& message, const json& data = {});
    
    // Authentication and authorization
    bool is_moderator(const core::network::HttpRequest& request);
    bool is_admin(const core::network::HttpRequest& request);
    std::string get_client_ip(const core::network::HttpRequest& request);
    
    // Cache management
    void cache_ghost_reply(const GhostReply& ghost_reply);
    void invalidate_ghost_reply_cache(const std::string& ghost_reply_id);
    void invalidate_thread_cache(const std::string& thread_id);
    
    // Logging and monitoring
    void log_api_request(const std::string& endpoint, const std::string& method, int status_code);
    void log_ghost_reply_action(const std::string& action, const std::string& ghost_reply_id);
    
    // WebSocket event handlers
    void handle_ghost_reply_created(const GhostReply& ghost_reply);
    void handle_ghost_reply_updated(const GhostReply& ghost_reply);
    void handle_ghost_reply_deleted(const std::string& ghost_reply_id);
    void broadcast_to_thread_subscribers(const std::string& thread_id, const json& event_data);
};

} // namespace sonet::ghost_reply::controllers