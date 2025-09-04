/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/note.h"
#include "../models/attachment.h"
#include "../repositories/note_repository.h"
#include "../validators/note_validator.h"
#include "../service.h"
#include "../services/timeline_service.h"
#include "../services/notification_service.h"
#include "../services/analytics_service.h"

// HTTP Framework includes
#include "../../core/network/http_server.h"
#include "../../core/network/http_request.h"
#include "../../core/network/http_response.h"
#include "../../core/network/websocket_server.h"
#include "../../core/network/websocket_connection.h"

// Security and validation
#include "../../core/validation/input_validator.h"
#include "../../core/security/auth_middleware.h"
#include "../../core/security/rate_limiter.h"
#include "../../core/cache/redis_client.h"

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

namespace sonet::note::controllers {

using namespace sonet::note::models;
using namespace sonet::note::repositories;
using json = nlohmann::json;

/**
 * @brief Twitter-Scale RESTful Controller for Note Management
 * 
 * Provides comprehensive HTTP REST API, WebSocket real-time features, and integration
 * with gRPC services for high-performance note operations including:
 * - CRUD operations for notes with Twitter-like features
 * - Real-time timeline updates via WebSocket
 * - Renote (retweet) functionality
 * - Advanced search and discovery
 * - Analytics and engagement metrics
 * - Content moderation and safety
 * - Rate limiting and abuse prevention
 * - Horizontal scaling support
 */
class NoteController {
public:
    // Constructor with comprehensive service injection
    explicit NoteController(
        std::shared_ptr<NoteRepository> repository,
        std::shared_ptr<sonet::note::NoteService> note_service,
        std::shared_ptr<services::TimelineService> timeline_service,
        std::shared_ptr<services::NotificationService> notification_service,
        std::shared_ptr<services::AnalyticsService> analytics_service,
        std::shared_ptr<sonet::core::cache::RedisClient> redis_client,
        std::shared_ptr<sonet::core::security::RateLimiter> rate_limiter
    );

    // HTTP Route Registration for REST API
    void register_http_routes(std::shared_ptr<core::network::HttpServer> server);
    void register_websocket_handlers(std::shared_ptr<core::network::WebSocketServer> ws_server);

    // ========== CORE NOTE OPERATIONS ==========
    
    /**
     * @brief Create a new note
     * NOTE /api/v1/notes
     * 
     * Features:
     * - 300 character limit (Twitter-like)
     * - Multiple attachments support (images, videos, GIFs, polls, etc.)
     * - Rich media processing
     * - Auto-hashtag and mention detection
     * - Content moderation and safety checks
     * - Spam and duplicate detection
     * 
     * Rate limit: 25 requests/minute per user
     */
    core::network::HttpResponse create_note(const core::network::HttpRequest& request);

    /**
     * @brief Get a specific note by ID
     * GET /api/v1/notes/:note_id
     * 
     * Features:
     * - Privacy-aware content filtering
     * - View count tracking
     * - Related content suggestions
     * - Thread context loading
     */
    core::network::HttpResponse get_note(const core::network::HttpRequest& request);

    /**
     * @brief Update an existing note (within 30-minute edit window)
     * PUT /api/v1/notes/:note_id
     * 
     * Features:
     * - Edit history tracking
     * - Notification to engagers about edits
     * - Attachment modification support
     */
    core::network::HttpResponse update_note(const core::network::HttpRequest& request);

    /**
     * @brief Delete a note
     * DELETE /api/v1/notes/:note_id
     * 
     * Features:
     * - Cascade deletion of renotes and replies
     * - Analytics data preservation for insights
     * - Timeline cleanup and cache invalidation
     */
    core::network::HttpResponse delete_note(const core::network::HttpRequest& request);

    /**
     * @brief Get note conversation thread
     * GET /api/v1/notes/:note_id/thread
     * 
     * Features:
     * - Intelligent thread reconstruction
     * - Reply sorting and pagination
     * - Hidden/muted content filtering
     */
    core::network::HttpResponse get_note_thread(const core::network::HttpRequest& request);

    // ========== RENOTE (RETWEET) OPERATIONS ==========
    
    /**
     * @brief Renote (retweet) a note
     * NOTE /api/v1/notes/:note_id/renote
     * 
     * Rate limit: 150 requests/minute per user
     */
    core::network::HttpResponse renote(const core::network::HttpRequest& request);

    /**
     * @brief Undo renote
     * DELETE /api/v1/notes/:note_id/renote
     */
    core::network::HttpResponse undo_renote(const core::network::HttpRequest& request);

    /**
     * @brief Quote renote (retweet with comment)
     * NOTE /api/v1/notes/:note_id/quote
     * 
     * Features:
     * - Original note embedding
     * - Additional commentary (300 chars)
     * - Preserves engagement metrics separately
     */
    core::network::HttpResponse quote_renote(const core::network::HttpRequest& request);

    /**
     * @brief Get users who renoted a note
     * GET /api/v1/notes/:note_id/renotes
     */
    core::network::HttpResponse get_renotes(const core::network::HttpRequest& request);

    // ========== ENGAGEMENT OPERATIONS ==========
    
    /**
     * @brief Like a note
     * NOTE /api/v1/notes/:note_id/like
     * 
     * Rate limit: 300 requests/minute per user
     */
    core::network::HttpResponse like_note(const core::network::HttpRequest& request);

    /**
     * @brief Unlike a note
     * DELETE /api/v1/notes/:note_id/like
     */
    core::network::HttpResponse unlike_note(const core::network::HttpRequest& request);

    /**
     * @brief Bookmark a note for later reading
     * NOTE /api/v1/notes/:note_id/bookmark
     */
    core::network::HttpResponse bookmark_note(const core::network::HttpRequest& request);

    /**
     * @brief Remove bookmark
     * DELETE /api/v1/notes/:note_id/bookmark
     */
    core::network::HttpResponse unbookmark_note(const core::network::HttpRequest& request);

    /**
     * @brief Report a note for policy violations
     * NOTE /api/v1/notes/:note_id/report
     * 
     * Features:
     * - Automatic content review triggering
     * - Reporter anonymity protection
     * - Appeal process integration
     */
    core::network::HttpResponse report_note(const core::network::HttpRequest& request);

    // ========== TIMELINE OPERATIONS ==========
    
    /**
     * @brief Get user's personalized home timeline
     * GET /api/v1/timelines/home
     * 
     * Features:
     * - ML-powered content ranking
     * - Real-time updates via cursor pagination
     * - Promoted content integration
     * - Content diversity algorithms
     * 
     * Rate limit: 300 requests/minute per user
     */
    core::network::HttpResponse get_home_timeline(const core::network::HttpRequest& request);

    /**
     * @brief Get user's profile timeline (their notes)
     * GET /api/v1/timelines/user/:user_id
     * 
     * Features:
     * - Pinned notes support
     * - Privacy filtering based on relationship
     * - Reply hiding options
     */
    core::network::HttpResponse get_user_timeline(const core::network::HttpRequest& request);

    /**
     * @brief Get global public timeline
     * GET /api/v1/timelines/public
     * 
     * Features:
     * - Geographic content filtering
     * - Language preferences
     * - Trending content boosting
     */
    core::network::HttpResponse get_public_timeline(const core::network::HttpRequest& request);

    /**
     * @brief Get trending notes (Twitter's trending topics equivalent)
     * GET /api/v1/timelines/trending
     * 
     * Features:
     * - Real-time trend detection
     * - Geographic and demographic targeting
     * - Trend explanation and context
     */
    core::network::HttpResponse get_trending_timeline(const core::network::HttpRequest& request);

    /**
     * @brief Get user's mentions timeline
     * GET /api/v1/timelines/mentions
     */
    core::network::HttpResponse get_mentions_timeline(const core::network::HttpRequest& request);

    /**
     * @brief Get user's bookmarks timeline
     * GET /api/v1/timelines/bookmarks
     */
    core::network::HttpResponse get_bookmarks_timeline(const core::network::HttpRequest& request);

    // ========== SEARCH AND DISCOVERY ==========
    
    /**
     * @brief Advanced note search with filters
     * GET /api/v1/search/notes
     * 
     * Features:
     * - Full-text search with relevance ranking
     * - Advanced filters (date, location, engagement, etc.)
     * - Real-time search suggestions
     * - Search result personalization
     * 
     * Rate limit: 180 requests/minute per user
     */
    core::network::HttpResponse search_notes(const core::network::HttpRequest& request);

    /**
     * @brief Get trending hashtags and topics
     * GET /api/v1/search/trending
     * 
     * Features:
     * - Real-time trend calculation
     * - Geographic trend variations
     * - Trend momentum tracking
     */
    core::network::HttpResponse get_trending_hashtags(const core::network::HttpRequest& request);

    /**
     * @brief Get notes by specific hashtag
     * GET /api/v1/search/hashtag/:tag
     * 
     * Features:
     * - Related hashtag suggestions
     * - Hashtag engagement analytics
     * - Content quality filtering
     */
    core::network::HttpResponse get_notes_by_hashtag(const core::network::HttpRequest& request);

    /**
     * @brief Advanced search with AI-powered relevance
     * NOTE /api/v1/search/advanced
     */
    core::network::HttpResponse advanced_search(const core::network::HttpRequest& request);

    // ========== ANALYTICS AND INSIGHTS ==========
    
    /**
     * @brief Get comprehensive note analytics
     * GET /api/v1/notes/:note_id/analytics
     * 
     * Features:
     * - Engagement metrics over time
     * - Audience demographics
     * - Performance compared to user's average
     * - Reach and impression analytics
     */
    core::network::HttpResponse get_note_analytics(const core::network::HttpRequest& request);

    /**
     * @brief Get user's note performance statistics
     * GET /api/v1/users/:user_id/note-stats
     * 
     * Features:
     * - Publishing patterns analysis
     * - Engagement rate trends
     * - Content performance insights
     * - Follower growth correlation
     */
    core::network::HttpResponse get_user_note_stats(const core::network::HttpRequest& request);

    /**
     * @brief Get real-time engagement metrics
     * GET /api/v1/notes/:note_id/engagement/live
     */
    core::network::HttpResponse get_live_engagement(const core::network::HttpRequest& request);

    // ========== BULK AND BATCH OPERATIONS ==========
    
    /**
     * @brief Get multiple notes by IDs (for efficient loading)
     * NOTE /api/v1/notes/batch
     * 
     * Features:
     * - Parallel data retrieval
     * - Privacy filtering per note
     * - Missing note handling
     */
    core::network::HttpResponse get_notes_batch(const core::network::HttpRequest& request);

    /**
     * @brief Bulk delete notes (for cleanup operations)
     * DELETE /api/v1/notes/batch
     */
    core::network::HttpResponse delete_notes_batch(const core::network::HttpRequest& request);

    /**
     * @brief Bulk update note settings (privacy, etc.)
     * PATCH /api/v1/notes/batch
     */
    core::network::HttpResponse update_notes_batch(const core::network::HttpRequest& request);

    // ========== CONTENT MANAGEMENT ==========
    
    /**
     * @brief Schedule note for future publishing
     * NOTE /api/v1/notes/schedule
     */
    core::network::HttpResponse schedule_note(const core::network::HttpRequest& request);

    /**
     * @brief Get user's scheduled notes
     * GET /api/v1/notes/scheduled
     */
    core::network::HttpResponse get_scheduled_notes(const core::network::HttpRequest& request);

    /**
     * @brief Save note as draft
     * NOTE /api/v1/notes/draft
     */
    core::network::HttpResponse save_draft(const core::network::HttpRequest& request);

    /**
     * @brief Get user's drafts
     * GET /api/v1/notes/drafts
     */
    core::network::HttpResponse get_drafts(const core::network::HttpRequest& request);

    // ========== WEBSOCKET REAL-TIME FEATURES ==========
    
    /**
     * @brief Handle WebSocket connection for real-time updates
     * 
     * Features:
     * - Timeline streaming
     * - Live engagement updates
     * - Typing indicators
     * - Push notifications
     * - Connection health monitoring
     */
    void handle_websocket_connection(std::shared_ptr<core::network::WebSocketConnection> connection);

    /**
     * @brief Handle timeline subscription for real-time updates
     * Message: {"type": "subscribe", "timeline": "home|public|user", "user_id": "..."}
     */
    void handle_timeline_subscription(std::shared_ptr<core::network::WebSocketConnection> connection, 
                                    const json& message);

    /**
     * @brief Handle typing indicators for conversations
     * Message: {"type": "typing", "note_id": "...", "is_typing": true}
     */
    void handle_typing_indicator(std::shared_ptr<core::network::WebSocketConnection> connection,
                               const json& message);

    /**
     * @brief Handle live engagement updates subscription
     * Message: {"type": "subscribe_engagement", "note_id": "..."}
     */
    void handle_engagement_subscription(std::shared_ptr<core::network::WebSocketConnection> connection,
                                      const json& message);

    /**
     * @brief Broadcast new note to timeline subscribers
     */
    void broadcast_note_created(const models::Note& note);

    /**
     * @brief Broadcast note update to subscribers
     */
    void broadcast_note_updated(const models::Note& note, const std::string& change_type);

    /**
     * @brief Broadcast note deletion to subscribers
     */
    void broadcast_note_deleted(const std::string& note_id, const std::string& user_id);

    /**
     * @brief Broadcast live engagement updates
     */
    void broadcast_engagement_update(const std::string& note_id, const std::string& engagement_type, int count);

private:
    // ========== SERVICE DEPENDENCIES ==========
    std::shared_ptr<NoteRepository> note_repository_;
    std::shared_ptr<sonet::note::NoteService> note_service_;
    std::shared_ptr<services::TimelineService> timeline_service_;
    std::shared_ptr<services::NotificationService> notification_service_;
    std::shared_ptr<services::AnalyticsService> analytics_service_;
    std::shared_ptr<sonet::core::cache::RedisClient> redis_client_;
    std::shared_ptr<sonet::core::security::RateLimiter> rate_limiter_;

    // ========== WEBSOCKET CONNECTION MANAGEMENT ==========
    std::unordered_map<std::string, std::vector<std::shared_ptr<core::network::WebSocketConnection>>> timeline_subscribers_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<core::network::WebSocketConnection>>> engagement_subscribers_;
    std::unordered_map<std::string, std::shared_ptr<core::network::WebSocketConnection>> user_connections_;
    std::mutex connections_mutex_;

    // ========== PERFORMANCE MONITORING ==========
    std::atomic<uint64_t> request_count_{0};
    std::atomic<uint64_t> active_connections_{0};
    std::atomic<uint64_t> total_processed_notes_{0};

    // ========== HELPER METHODS ==========
    
    // Authentication and authorization
    std::string extract_user_id(const core::network::HttpRequest& request);
    bool validate_authentication(const core::network::HttpRequest& request);
    bool validate_user_permissions(const std::string& note_id, const std::string& user_id, const std::string& operation);

    // Rate limiting and abuse prevention
    bool check_rate_limit(const std::string& user_id, const std::string& endpoint);
    bool detect_spam_patterns(const std::string& content, const std::string& user_id);
    bool check_content_policy_violations(const std::string& content);

    // Response building
    json build_note_response(const models::Note& note, const std::string& viewer_id);
    json build_timeline_response(const std::vector<models::Note>& notes, const std::string& viewer_id, 
                                const std::string& cursor = "", bool has_more = false);
    json build_analytics_response(const std::string& note_id, const std::string& timeframe);
    json build_search_response(const std::vector<models::Note>& notes, const std::string& query, 
                              const std::string& viewer_id, int total_count);

    // Request validation
    bool validate_note_request(const json& request_body, std::string& error_message);
    bool validate_search_query(const std::string& query, std::string& error_message);
    bool validate_timeline_params(const core::network::HttpRequest& request, std::string& error_message);

    // Pagination handling
    struct PaginationParams {
        int limit = 20;
        std::string cursor;
        std::string max_id;
        std::string since_id;
        int offset = 0;
    };
    PaginationParams extract_pagination_params(const core::network::HttpRequest& request);

    // Privacy and content filtering
    bool can_access_note(const models::Note& note, const std::string& viewer_id);
    bool should_filter_sensitive_content(const models::Note& note, const std::string& viewer_id);
    void apply_privacy_filters(std::vector<models::Note>& notes, const std::string& viewer_id);
    void sanitize_note_content(models::Note& note, const std::string& viewer_id);

    // Error handling
    core::network::HttpResponse create_error_response(int status_code, const std::string& error_code, 
                                                    const std::string& message, const json& details = json::object());
    core::network::HttpResponse create_success_response(const json& data, int status_code = 200, 
                                                       const json& meta = json::object());
    void log_request_metrics(const core::network::HttpRequest& request, const std::string& user_id, 
                           const std::string& action, std::chrono::milliseconds duration);

    // WebSocket helpers
    void subscribe_to_timeline(std::shared_ptr<core::network::WebSocketConnection> connection,
                              const std::string& timeline_type, const std::string& user_id);
    void subscribe_to_engagement(std::shared_ptr<core::network::WebSocketConnection> connection,
                               const std::string& note_id);
    void unsubscribe_from_all(std::shared_ptr<core::network::WebSocketConnection> connection);
    void broadcast_to_subscribers(const std::string& subscription_type, const json& message, 
                                const std::string& exclude_user_id = "");
    void handle_websocket_message(std::shared_ptr<core::network::WebSocketConnection> connection,
                                const std::string& message);
    void cleanup_dead_connections();

    // Cache management
    void invalidate_user_caches(const std::string& user_id);
    void invalidate_timeline_caches(const std::vector<std::string>& user_ids);
    void update_trending_cache(const models::Note& note);
    std::string generate_cache_key(const std::string& prefix, const std::vector<std::string>& params);

    // Content processing
    void extract_content_features(models::Note& note);
    void process_mentions_and_hashtags(models::Note& note);
    void trigger_content_moderation(const models::Note& note);
    void calculate_content_quality_score(models::Note& note);

    // Analytics helpers
    void track_user_engagement(const std::string& user_id, const std::string& action, 
                             const std::string& note_id = "");
    void update_real_time_metrics(const std::string& note_id, const std::string& metric_type);
    json get_engagement_analytics(const std::string& note_id, const std::string& timeframe);
    json get_reach_analytics(const std::string& note_id, const std::string& timeframe);

    // Performance optimizations
    void preload_user_relationships(const std::vector<std::string>& user_ids, const std::string& viewer_id);
    void batch_load_engagement_data(std::vector<models::Note>& notes, const std::string& viewer_id);
    void apply_timeline_algorithms(std::vector<models::Note>& notes, const std::string& user_id, 
                                 const std::string& timeline_type);

    // ========== TWITTER-SCALE CONSTANTS ==========
    
    // Content limits
    static constexpr int MAX_CONTENT_LENGTH = 300;
    static constexpr int MAX_HASHTAGS = 10;
    static constexpr int MAX_MENTIONS = 10;
    static constexpr int MAX_URLS = 5;
    static constexpr int NOTE_EDIT_WINDOW_MINUTES = 30;
    
    // Pagination limits
    static constexpr int DEFAULT_TIMELINE_LIMIT = 20;
    static constexpr int MAX_TIMELINE_LIMIT = 200;
    static constexpr int DEFAULT_SEARCH_LIMIT = 20;
    static constexpr int MAX_SEARCH_LIMIT = 100;
    static constexpr int MAX_BATCH_SIZE = 100;
    
    // Rate limiting (requests per minute)
    static constexpr int CREATE_NOTE_RATE_LIMIT = 25;
    static constexpr int LIKE_RATE_LIMIT = 300;
    static constexpr int RENOTE_RATE_LIMIT = 150;
    static constexpr int SEARCH_RATE_LIMIT = 180;
    static constexpr int TIMELINE_RATE_LIMIT = 300;
    static constexpr int BULK_OPERATION_RATE_LIMIT = 10;
    
    // WebSocket limits
    static constexpr int MAX_CONNECTIONS_PER_USER = 5;
    static constexpr int MAX_SUBSCRIPTIONS_PER_CONNECTION = 20;
    static constexpr int WEBSOCKET_HEARTBEAT_INTERVAL_SECONDS = 30;
    
    // Cache TTL (seconds)
    static constexpr int TIMELINE_CACHE_TTL = 300;     // 5 minutes
    static constexpr int NOTE_CACHE_TTL = 3600;        // 1 hour
    static constexpr int TRENDING_CACHE_TTL = 600;     // 10 minutes
    static constexpr int ANALYTICS_CACHE_TTL = 1800;   // 30 minutes
};

} // namespace sonet::note::controllers