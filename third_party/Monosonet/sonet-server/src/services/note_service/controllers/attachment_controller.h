/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/attachment.h"
#include "../../core/cache/cache_manager.h"
#include "../../core/logging/logger.h"
#include "../../core/validation/request_validator.h"
#include "../../core/security/rate_limiter.h"
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <optional>

namespace sonet::note::controllers {

using json = nlohmann::json;
using namespace sonet::note::models;

/**
 * @brief HTTP status codes for attachment operations
 */
enum class AttachmentHttpStatus {
    OK = 200,
    CREATED = 201,
    NO_CONTENT = 204,
    BAD_REQUEST = 400,
    UNAUTHORIZED = 401,
    FORBIDDEN = 403,
    NOT_FOUND = 404,
    CONFLICT = 409,
    PAYLOAD_TOO_LARGE = 413,
    UNSUPPORTED_MEDIA_TYPE = 415,
    UNPROCESSABLE_ENTITY = 422,
    TOO_MANY_REQUESTS = 429,
    INTERNAL_SERVER_ERROR = 500
};

/**
 * @brief Request structures for attachment operations
 */
struct UploadImageRequest {
    std::string filename;
    std::string mime_type;
    size_t file_size;
    std::string alt_text;
    std::string caption;
    bool is_sensitive = false;
    
    static UploadImageRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct UploadVideoRequest {
    std::string filename;
    std::string mime_type;
    size_t file_size;
    double duration;
    std::string alt_text;
    std::string caption;
    bool is_sensitive = false;
    
    static UploadVideoRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct TenorGifRequest {
    std::string tenor_id;
    std::string search_term;
    std::string alt_text;
    
    static TenorGifRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct LinkPreviewRequest {
    std::string url;
    
    static LinkPreviewRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct CreatePollRequest {
    std::string question;
    std::vector<std::string> options;
    bool multiple_choice = false;
    bool anonymous = true;
    int expires_in_hours = 24;
    
    static CreatePollRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct LocationRequest {
    std::string place_id;
    std::string name;
    std::string address;
    double latitude;
    double longitude;
    std::string city;
    std::string country;
    
    static LocationRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

/**
 * @brief Response structures for attachment operations
 */
struct AttachmentResponse {
    Attachment attachment;
    std::string upload_url;      // Pre-signed URL for upload
    std::string callback_url;    // Webhook URL for processing completion
    bool requires_processing = false;
    
    json to_json() const;
};

struct TenorSearchResponse {
    std::vector<TenorGifData> gifs;
    std::string next_cursor;
    int total_results;
    
    json to_json() const;
};

/**
 * @brief Controller for attachment-related HTTP endpoints
 * 
 * Handles media upload, processing, and management operations including:
 * - Image and video upload with processing
 * - Tenor GIF integration
 * - Link preview generation
 * - Poll creation and management
 * - Location attachment
 * - Media processing status tracking
 */
class AttachmentController {
public:
    AttachmentController(
        std::shared_ptr<core::cache::CacheManager> cache_manager,
        std::shared_ptr<core::security::RateLimiter> rate_limiter
    );
    
    ~AttachmentController() = default;
    
    // Image operations
    
    /**
     * @brief Upload an image attachment
     * @param request_data JSON request data
     * @param user_id ID of the requesting user
     * @param file_data Binary file data (placeholder for actual implementation)
     * @return JSON response with attachment data or error
     */
    json upload_image(const json& request_data, const std::string& user_id, const std::string& file_data = "");
    
    /**
     * @brief Get image processing status
     * @param attachment_id Attachment identifier
     * @param user_id ID of the requesting user
     * @return JSON response with processing status
     */
    json get_image_status(const std::string& attachment_id, const std::string& user_id);
    
    // Video operations
    
    /**
     * @brief Upload a video attachment
     * @param request_data JSON request data
     * @param user_id ID of the requesting user
     * @param file_data Binary file data (placeholder for actual implementation)
     * @return JSON response with attachment data or error
     */
    json upload_video(const json& request_data, const std::string& user_id, const std::string& file_data = "");
    
    /**
     * @brief Get video processing status
     * @param attachment_id Attachment identifier
     * @param user_id ID of the requesting user
     * @return JSON response with processing status
     */
    json get_video_status(const std::string& attachment_id, const std::string& user_id);
    
    // GIF operations
    
    /**
     * @brief Search Tenor GIFs
     * @param query Search query
     * @param limit Number of results to return
     * @param cursor Pagination cursor
     * @param user_id ID of the requesting user (for rate limiting)
     * @return JSON response with GIF search results
     */
    json search_tenor_gifs(const std::string& query, int limit = 20, const std::string& cursor = "", const std::string& user_id = "");
    
    /**
     * @brief Add Tenor GIF to note
     * @param request_data JSON request data with Tenor GIF information
     * @param user_id ID of the requesting user
     * @return JSON response with attachment data
     */
    json add_tenor_gif(const json& request_data, const std::string& user_id);
    
    /**
     * @brief Get trending GIFs from Tenor
     * @param limit Number of results to return
     * @param user_id ID of the requesting user (for rate limiting)
     * @return JSON response with trending GIFs
     */
    json get_trending_gifs(int limit = 20, const std::string& user_id = "");
    
    // Link preview operations
    
    /**
     * @brief Generate link preview
     * @param request_data JSON request data with URL
     * @param user_id ID of the requesting user
     * @return JSON response with link preview data
     */
    json generate_link_preview(const json& request_data, const std::string& user_id);
    
    /**
     * @brief Get cached link preview
     * @param url URL to get preview for
     * @param user_id ID of the requesting user
     * @return JSON response with link preview or null if not cached
     */
    json get_link_preview(const std::string& url, const std::string& user_id = "");
    
    // Poll operations
    
    /**
     * @brief Create a poll attachment
     * @param request_data JSON request data with poll information
     * @param user_id ID of the requesting user
     * @return JSON response with poll attachment data
     */
    json create_poll(const json& request_data, const std::string& user_id);
    
    /**
     * @brief Vote on a poll
     * @param attachment_id Poll attachment identifier
     * @param option_ids Vector of option IDs to vote for
     * @param user_id ID of the voting user
     * @return JSON response with updated poll data
     */
    json vote_on_poll(const std::string& attachment_id, const std::vector<std::string>& option_ids, const std::string& user_id);
    
    /**
     * @brief Get poll results
     * @param attachment_id Poll attachment identifier
     * @param user_id ID of the requesting user
     * @return JSON response with poll results
     */
    json get_poll_results(const std::string& attachment_id, const std::string& user_id = "");
    
    // Location operations
    
    /**
     * @brief Add location attachment
     * @param request_data JSON request data with location information
     * @param user_id ID of the requesting user
     * @return JSON response with location attachment data
     */
    json add_location(const json& request_data, const std::string& user_id);
    
    /**
     * @brief Search for places
     * @param query Search query
     * @param latitude Optional latitude for location-based search
     * @param longitude Optional longitude for location-based search
     * @param user_id ID of the requesting user
     * @return JSON response with place search results
     */
    json search_places(const std::string& query, std::optional<double> latitude = std::nullopt, 
                      std::optional<double> longitude = std::nullopt, const std::string& user_id = "");
    
    // General attachment operations
    
    /**
     * @brief Get attachment by ID
     * @param attachment_id Attachment identifier
     * @param user_id ID of the requesting user
     * @return JSON response with attachment data
     */
    json get_attachment(const std::string& attachment_id, const std::string& user_id = "");
    
    /**
     * @brief Update attachment metadata
     * @param attachment_id Attachment identifier
     * @param request_data JSON request data with updates
     * @param user_id ID of the requesting user
     * @return JSON response with updated attachment data
     */
    json update_attachment(const std::string& attachment_id, const json& request_data, const std::string& user_id);
    
    /**
     * @brief Delete an attachment
     * @param attachment_id Attachment identifier
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json delete_attachment(const std::string& attachment_id, const std::string& user_id);
    
    /**
     * @brief Get attachments by user
     * @param user_id User identifier
     * @param type Optional attachment type filter
     * @param limit Number of attachments to return
     * @param offset Offset for pagination
     * @param requesting_user_id ID of the requesting user
     * @return JSON response with user's attachments
     */
    json get_user_attachments(const std::string& user_id, std::optional<AttachmentType> type = std::nullopt,
                             int limit = 20, int offset = 0, const std::string& requesting_user_id = "");
    
    // Analytics and metrics
    
    /**
     * @brief Get attachment analytics
     * @param attachment_id Attachment identifier
     * @param user_id ID of the requesting user
     * @return JSON response with analytics data
     */
    json get_attachment_analytics(const std::string& attachment_id, const std::string& user_id);
    
    /**
     * @brief Record attachment interaction
     * @param attachment_id Attachment identifier
     * @param interaction_type Type of interaction (view, download, share)
     * @param user_id ID of the interacting user
     * @return JSON response with status
     */
    json record_interaction(const std::string& attachment_id, const std::string& interaction_type, const std::string& user_id);
    
    // Processing and moderation
    
    /**
     * @brief Get processing status for multiple attachments
     * @param attachment_ids Vector of attachment identifiers
     * @param user_id ID of the requesting user
     * @return JSON response with processing statuses
     */
    json get_processing_status(const std::vector<std::string>& attachment_ids, const std::string& user_id);
    
    /**
     * @brief Retry failed processing
     * @param attachment_id Attachment identifier
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json retry_processing(const std::string& attachment_id, const std::string& user_id);
    
    /**
     * @brief Report attachment content
     * @param attachment_id Attachment identifier
     * @param reason Report reason
     * @param user_id ID of the reporting user
     * @return JSON response with status
     */
    json report_attachment(const std::string& attachment_id, const std::string& reason, const std::string& user_id);
    
    // Utility methods
    
    /**
     * @brief Health check endpoint
     * @return JSON response with service status
     */
    json health_check();
    
    /**
     * @brief Get API information
     * @return JSON response with API details
     */
    json get_api_info();

private:
    // Dependencies
    std::shared_ptr<core::cache::CacheManager> cache_manager_;
    std::shared_ptr<core::security::RateLimiter> rate_limiter_;
    
    // Service placeholders (to be injected from dedicated media service)
    // These would be actual service implementations in production
    struct MediaProcessingService {
        // Placeholder for media processing service integration
        std::string process_image(const std::string& file_data, const UploadImageRequest& request);
        std::string process_video(const std::string& file_data, const UploadVideoRequest& request);
        bool get_processing_status(const std::string& job_id, ProcessingStatus& status, std::string& error);
    };
    
    struct TenorService {
        // Placeholder for Tenor API integration
        TenorSearchResponse search_gifs(const std::string& query, int limit, const std::string& cursor);
        TenorSearchResponse get_trending_gifs(int limit);
        TenorGifData get_gif_details(const std::string& tenor_id);
    };
    
    struct LinkPreviewService {
        // Placeholder for link preview service
        LinkPreview generate_preview(const std::string& url);
        bool is_url_supported(const std::string& url);
    };
    
    struct LocationService {
        // Placeholder for location service
        std::vector<LocationData> search_places(const std::string& query, std::optional<double> lat, std::optional<double> lng);
        LocationData get_place_details(const std::string& place_id);
    };
    
    MediaProcessingService media_service_;
    TenorService tenor_service_;
    LinkPreviewService link_service_;
    LocationService location_service_;
    
    // Request handlers and validation
    
    /**
     * @brief Validate and parse request data
     * @tparam T Request type
     * @param request_data JSON request data
     * @return Parsed request object or nullopt if invalid
     */
    template<typename T>
    std::optional<T> parse_and_validate_request(const json& request_data);
    
    /**
     * @brief Check rate limits for a user
     * @param user_id User identifier
     * @param operation Operation type
     * @return True if within limits, false otherwise
     */
    bool check_rate_limit(const std::string& user_id, const std::string& operation);
    
    /**
     * @brief Create error response
     * @param status HTTP status code
     * @param message Error message
     * @param details Additional error details
     * @return JSON error response
     */
    json create_error_response(AttachmentHttpStatus status, const std::string& message, const json& details = json::object());
    
    /**
     * @brief Create success response
     * @param data Response data
     * @param status HTTP status code
     * @return JSON success response
     */
    json create_success_response(const json& data, AttachmentHttpStatus status = AttachmentHttpStatus::OK);
    
    /**
     * @brief Validate attachment permissions
     * @param attachment_id Attachment identifier
     * @param user_id User identifier
     * @param operation Required operation type
     * @return True if user has permission
     */
    bool check_attachment_permission(const std::string& attachment_id, const std::string& user_id, const std::string& operation);
    
    /**
     * @brief Generate upload URL for attachment
     * @param attachment Attachment object
     * @return Pre-signed upload URL
     */
    std::string generate_upload_url(const Attachment& attachment);
    
    /**
     * @brief Cache attachment data
     * @param attachment_id Attachment identifier
     * @param data Data to cache
     * @param ttl_seconds Cache TTL in seconds
     */
    void cache_attachment_data(const std::string& attachment_id, const json& data, int ttl_seconds = 300);
    
    /**
     * @brief Get cached attachment data
     * @param attachment_id Attachment identifier
     * @return Cached data or nullopt if not found
     */
    std::optional<json> get_cached_attachment_data(const std::string& attachment_id);
    
    /**
     * @brief Invalidate attachment cache
     * @param attachment_id Attachment identifier
     */
    void invalidate_attachment_cache(const std::string& attachment_id);
    
    /**
     * @brief Log controller operation
     * @param operation Operation name
     * @param user_id User identifier
     * @param attachment_id Attachment identifier (optional)
     * @param status Operation status
     */
    void log_operation(const std::string& operation, const std::string& user_id, 
                      const std::string& attachment_id = "", const std::string& status = "success");
    
    /**
     * @brief Validate attachment ID format
     * @param attachment_id Attachment identifier
     * @return True if valid format
     */
    bool is_valid_attachment_id(const std::string& attachment_id) const;
    
    /**
     * @brief Validate user ID format
     * @param user_id User identifier
     * @return True if valid format
     */
    bool is_valid_user_id(const std::string& user_id) const;
    
    /**
     * @brief Get pagination info
     * @param total_count Total number of items
     * @param limit Items per page
     * @param offset Current offset
     * @return Pagination information
     */
    json get_pagination_info(int total_count, int limit, int offset) const;
    
    /**
     * @brief Simulate media processing (placeholder)
     * @param attachment Attachment to process
     * @return Processing job ID
     */
    std::string simulate_media_processing(const Attachment& attachment);
    
    /**
     * @brief Simulate content moderation (placeholder)
     * @param attachment Attachment to moderate
     * @return Content safety score
     */
    double simulate_content_moderation(const Attachment& attachment);
};

} // namespace sonet::note::controllers
