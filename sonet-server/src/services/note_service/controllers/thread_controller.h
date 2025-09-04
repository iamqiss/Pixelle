/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/thread.h"
#include "../models/note.h"
#include "../repositories/thread_repository.h"
#include "../repositories/note_repository.h"
#include "../services/thread_service.h"
#include "../security/thread_security.h"
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
using namespace sonet::note::repositories;
using namespace sonet::note::services;
using namespace sonet::note::security;

/**
 * @brief HTTP status codes for thread operations
 */
enum class ThreadHttpStatus {
    OK = 200,
    CREATED = 201,
    NO_CONTENT = 204,
    BAD_REQUEST = 400,
    UNAUTHORIZED = 401,
    FORBIDDEN = 403,
    NOT_FOUND = 404,
    CONFLICT = 409,
    UNPROCESSABLE_ENTITY = 422,
    TOO_MANY_REQUESTS = 429,
    INTERNAL_SERVER_ERROR = 500
};

/**
 * @brief Request/Response structures for thread operations
 */
struct CreateThreadRequest {
    std::string starter_note_id;
    std::string title;
    std::string description;
    std::vector<std::string> tags;
    bool is_public = true;
    bool allow_replies = true;
    bool allow_renotes = true;
    
    static CreateThreadRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct UpdateThreadRequest {
    std::optional<std::string> title;
    std::optional<std::string> description;
    std::optional<std::vector<std::string>> tags;
    std::optional<bool> is_locked;
    std::optional<bool> is_pinned;
    std::optional<bool> allow_replies;
    std::optional<bool> allow_renotes;
    
    static UpdateThreadRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct AddNoteToThreadRequest {
    std::string note_id;
    std::optional<int> position;
    
    static AddNoteToThreadRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct ThreadResponse {
    Thread thread;
    std::vector<Note> notes;
    ThreadStatistics statistics;
    bool can_edit = false;
    bool can_moderate = false;
    
    static ThreadResponse from_thread(const Thread& thread);
    json to_json() const;
};

struct ThreadSearchRequest {
    std::optional<std::string> query;
    std::optional<std::string> author_id;
    std::optional<std::vector<std::string>> tags;
    std::optional<Visibility> visibility;
    std::optional<std::string> sort_by = "created_at";
    std::optional<std::string> sort_order = "desc";
    int limit = 20;
    int offset = 0;
    
    static ThreadSearchRequest from_json(const json& j);
    json to_json() const;
    bool validate() const;
};

struct ThreadListResponse {
    std::vector<ThreadResponse> threads;
    int total_count = 0;
    int page = 1;
    int per_page = 20;
    bool has_more = false;
    
    json to_json() const;
};

/**
 * @brief Controller for thread-related HTTP endpoints
 * 
 * Handles REST API operations for Twitter-style threads including:
 * - Thread creation and management
 * - Note addition/removal from threads
 * - Thread discovery and search
 * - Thread moderation and permissions
 * - Thread analytics and statistics
 */
class ThreadController {
public:
    ThreadController(
        std::shared_ptr<ThreadRepository> thread_repo,
        std::shared_ptr<NoteRepository> note_repo,
        std::shared_ptr<ThreadService> thread_service,
        std::shared_ptr<ThreadSecurity> thread_security,
        std::shared_ptr<core::cache::CacheManager> cache_manager,
        std::shared_ptr<core::security::RateLimiter> rate_limiter
    );
    
    ~ThreadController() = default;
    
    // Core thread operations
    
    /**
     * @brief Create a new thread
     * @param request_data JSON request data
     * @param user_id ID of the requesting user
     * @return JSON response with thread data or error
     */
    json create_thread(const json& request_data, const std::string& user_id);
    
    /**
     * @brief Get thread by ID
     * @param thread_id Thread identifier
     * @param user_id ID of the requesting user (optional)
     * @param include_notes Whether to include note content
     * @return JSON response with thread data or error
     */
    json get_thread(const std::string& thread_id, const std::string& user_id = "", bool include_notes = true);
    
    /**
     * @brief Update thread metadata
     * @param thread_id Thread identifier
     * @param request_data JSON request data
     * @param user_id ID of the requesting user
     * @return JSON response with updated thread or error
     */
    json update_thread(const std::string& thread_id, const json& request_data, const std::string& user_id);
    
    /**
     * @brief Delete a thread
     * @param thread_id Thread identifier
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json delete_thread(const std::string& thread_id, const std::string& user_id);
    
    // Thread structure operations
    
    /**
     * @brief Add a note to a thread
     * @param thread_id Thread identifier
     * @param request_data JSON request data with note information
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json add_note_to_thread(const std::string& thread_id, const json& request_data, const std::string& user_id);
    
    /**
     * @brief Remove a note from a thread
     * @param thread_id Thread identifier
     * @param note_id Note identifier
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json remove_note_from_thread(const std::string& thread_id, const std::string& note_id, const std::string& user_id);
    
    /**
     * @brief Reorder notes in a thread
     * @param thread_id Thread identifier
     * @param request_data JSON request data with new order
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json reorder_thread_notes(const std::string& thread_id, const json& request_data, const std::string& user_id);
    
    /**
     * @brief Get notes in a thread
     * @param thread_id Thread identifier
     * @param user_id ID of the requesting user (optional)
     * @param include_hidden Whether to include hidden notes
     * @return JSON response with notes
     */
    json get_thread_notes(const std::string& thread_id, const std::string& user_id = "", bool include_hidden = false);
    
    // Thread discovery and search
    
    /**
     * @brief Search threads by criteria
     * @param request_data JSON request data with search criteria
     * @param user_id ID of the requesting user (optional)
     * @return JSON response with search results
     */
    json search_threads(const json& request_data, const std::string& user_id = "");
    
    /**
     * @brief Get threads by author
     * @param author_id Author identifier
     * @param user_id ID of the requesting user (optional)
     * @param limit Number of threads to return
     * @param offset Offset for pagination
     * @return JSON response with threads
     */
    json get_threads_by_author(const std::string& author_id, const std::string& user_id = "", int limit = 20, int offset = 0);
    
    /**
     * @brief Get threads by tag
     * @param tag Tag to search for
     * @param user_id ID of the requesting user (optional)
     * @param limit Number of threads to return
     * @param offset Offset for pagination
     * @return JSON response with threads
     */
    json get_threads_by_tag(const std::string& tag, const std::string& user_id = "", int limit = 20, int offset = 0);
    
    /**
     * @brief Get trending threads
     * @param user_id ID of the requesting user (optional)
     * @param timeframe Timeframe for trending calculation
     * @param limit Number of threads to return
     * @return JSON response with trending threads
     */
    json get_trending_threads(const std::string& user_id = "", const std::string& timeframe = "24h", int limit = 20);
    
    // Thread engagement
    
    /**
     * @brief Record a thread view
     * @param thread_id Thread identifier
     * @param user_id ID of the viewing user
     * @return JSON response with status
     */
    json record_thread_view(const std::string& thread_id, const std::string& user_id);
    
    /**
     * @brief Get thread statistics
     * @param thread_id Thread identifier
     * @param user_id ID of the requesting user
     * @return JSON response with statistics
     */
    json get_thread_statistics(const std::string& thread_id, const std::string& user_id);
    
    /**
     * @brief Get thread participants
     * @param thread_id Thread identifier
     * @param user_id ID of the requesting user
     * @param limit Number of participants to return
     * @return JSON response with participants
     */
    json get_thread_participants(const std::string& thread_id, const std::string& user_id, int limit = 50);
    
    // Thread moderation
    
    /**
     * @brief Lock/unlock a thread
     * @param thread_id Thread identifier
     * @param lock Whether to lock (true) or unlock (false)
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json set_thread_lock(const std::string& thread_id, bool lock, const std::string& user_id);
    
    /**
     * @brief Pin/unpin a thread
     * @param thread_id Thread identifier
     * @param pin Whether to pin (true) or unpin (false)
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json set_thread_pin(const std::string& thread_id, bool pin, const std::string& user_id);
    
    /**
     * @brief Add a moderator to a thread
     * @param thread_id Thread identifier
     * @param moderator_id ID of the user to add as moderator
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json add_thread_moderator(const std::string& thread_id, const std::string& moderator_id, const std::string& user_id);
    
    /**
     * @brief Remove a moderator from a thread
     * @param thread_id Thread identifier
     * @param moderator_id ID of the moderator to remove
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json remove_thread_moderator(const std::string& thread_id, const std::string& moderator_id, const std::string& user_id);
    
    /**
     * @brief Block a user from a thread
     * @param thread_id Thread identifier
     * @param blocked_user_id ID of the user to block
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json block_user_from_thread(const std::string& thread_id, const std::string& blocked_user_id, const std::string& user_id);
    
    /**
     * @brief Unblock a user from a thread
     * @param thread_id Thread identifier
     * @param blocked_user_id ID of the user to unblock
     * @param user_id ID of the requesting user
     * @return JSON response with status
     */
    json unblock_user_from_thread(const std::string& thread_id, const std::string& blocked_user_id, const std::string& user_id);
    
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
    std::shared_ptr<ThreadRepository> thread_repo_;
    std::shared_ptr<NoteRepository> note_repo_;
    std::shared_ptr<ThreadService> thread_service_;
    std::shared_ptr<ThreadSecurity> thread_security_;
    std::shared_ptr<core::cache::CacheManager> cache_manager_;
    std::shared_ptr<core::security::RateLimiter> rate_limiter_;
    
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
    json create_error_response(ThreadHttpStatus status, const std::string& message, const json& details = json::object());
    
    /**
     * @brief Create success response
     * @param data Response data
     * @param status HTTP status code
     * @return JSON success response
     */
    json create_success_response(const json& data, ThreadHttpStatus status = ThreadHttpStatus::OK);
    
    /**
     * @brief Get thread with permission check
     * @param thread_id Thread identifier
     * @param user_id User identifier
     * @param required_permission Required permission level
     * @return Thread object or nullopt if not found/no permission
     */
    std::optional<Thread> get_thread_with_permission(
        const std::string& thread_id, 
        const std::string& user_id, 
        ThreadPermission required_permission = ThreadPermission::READ
    );
    
    /**
     * @brief Load thread notes with filtering
     * @param thread Thread object
     * @param user_id User identifier
     * @param include_hidden Whether to include hidden notes
     * @return Vector of notes
     */
    std::vector<Note> load_thread_notes_filtered(const Thread& thread, const std::string& user_id, bool include_hidden);
    
    /**
     * @brief Create thread response with permissions
     * @param thread Thread object
     * @param user_id User identifier
     * @param include_notes Whether to include note content
     * @return ThreadResponse object
     */
    ThreadResponse create_thread_response(const Thread& thread, const std::string& user_id, bool include_notes = true);
    
    /**
     * @brief Cache thread data
     * @param thread_id Thread identifier
     * @param data Data to cache
     * @param ttl_seconds Cache TTL in seconds
     */
    void cache_thread_data(const std::string& thread_id, const json& data, int ttl_seconds = 300);
    
    /**
     * @brief Get cached thread data
     * @param thread_id Thread identifier
     * @return Cached data or nullopt if not found
     */
    std::optional<json> get_cached_thread_data(const std::string& thread_id);
    
    /**
     * @brief Invalidate thread cache
     * @param thread_id Thread identifier
     */
    void invalidate_thread_cache(const std::string& thread_id);
    
    /**
     * @brief Log controller operation
     * @param operation Operation name
     * @param user_id User identifier
     * @param thread_id Thread identifier (optional)
     * @param status Operation status
     */
    void log_operation(const std::string& operation, const std::string& user_id, 
                      const std::string& thread_id = "", const std::string& status = "success");
    
    /**
     * @brief Validate thread ID format
     * @param thread_id Thread identifier
     * @return True if valid format
     */
    bool is_valid_thread_id(const std::string& thread_id) const;
    
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
};

} // namespace sonet::note::controllers
