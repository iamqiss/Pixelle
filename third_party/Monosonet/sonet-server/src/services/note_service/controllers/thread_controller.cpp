/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "thread_controller.h"
#include "../../core/utils/id_generator.h"
#include "../../core/utils/time_utils.h"
#include "../../core/validation/input_sanitizer.h"
#include <spdlog/spdlog.h>
#include <regex>
#include <chrono>
#include <algorithm>

namespace sonet::note::controllers {

// Request/Response struct implementations

CreateThreadRequest CreateThreadRequest::from_json(const json& j) {
    CreateThreadRequest req;
    
    if (j.contains("starter_note_id") && j["starter_note_id"].is_string()) {
        req.starter_note_id = j["starter_note_id"];
    }
    
    if (j.contains("title") && j["title"].is_string()) {
        req.title = j["title"];
    }
    
    if (j.contains("description") && j["description"].is_string()) {
        req.description = j["description"];
    }
    
    if (j.contains("tags") && j["tags"].is_array()) {
        for (const auto& tag : j["tags"]) {
            if (tag.is_string()) {
                req.tags.push_back(tag);
            }
        }
    }
    
    if (j.contains("is_public") && j["is_public"].is_boolean()) {
        req.is_public = j["is_public"];
    }
    
    if (j.contains("allow_replies") && j["allow_replies"].is_boolean()) {
        req.allow_replies = j["allow_replies"];
    }
    
    if (j.contains("allow_renotes") && j["allow_renotes"].is_boolean()) {
        req.allow_renotes = j["allow_renotes"];
    }
    
    return req;
}

json CreateThreadRequest::to_json() const {
    return json{
        {"starter_note_id", starter_note_id},
        {"title", title},
        {"description", description},
        {"tags", tags},
        {"is_public", is_public},
        {"allow_replies", allow_replies},
        {"allow_renotes", allow_renotes}
    };
}

bool CreateThreadRequest::validate() const {
    if (starter_note_id.empty()) {
        spdlog::warn("Create thread request missing starter_note_id");
        return false;
    }
    
    if (title.empty() || title.length() > 500) {
        spdlog::warn("Create thread request invalid title length: {}", title.length());
        return false;
    }
    
    if (description.length() > 10000) {
        spdlog::warn("Create thread request description too long: {}", description.length());
        return false;
    }
    
    if (tags.size() > 50) {
        spdlog::warn("Create thread request too many tags: {}", tags.size());
        return false;
    }
    
    for (const auto& tag : tags) {
        if (tag.empty() || tag.length() > 100) {
            spdlog::warn("Create thread request invalid tag: {}", tag);
            return false;
        }
    }
    
    return true;
}

UpdateThreadRequest UpdateThreadRequest::from_json(const json& j) {
    UpdateThreadRequest req;
    
    if (j.contains("title") && j["title"].is_string()) {
        req.title = j["title"];
    }
    
    if (j.contains("description") && j["description"].is_string()) {
        req.description = j["description"];
    }
    
    if (j.contains("tags") && j["tags"].is_array()) {
        std::vector<std::string> tags;
        for (const auto& tag : j["tags"]) {
            if (tag.is_string()) {
                tags.push_back(tag);
            }
        }
        req.tags = tags;
    }
    
    if (j.contains("is_locked") && j["is_locked"].is_boolean()) {
        req.is_locked = j["is_locked"];
    }
    
    if (j.contains("is_pinned") && j["is_pinned"].is_boolean()) {
        req.is_pinned = j["is_pinned"];
    }
    
    if (j.contains("allow_replies") && j["allow_replies"].is_boolean()) {
        req.allow_replies = j["allow_replies"];
    }
    
    if (j.contains("allow_renotes") && j["allow_renotes"].is_boolean()) {
        req.allow_renotes = j["allow_renotes"];
    }
    
    return req;
}

bool UpdateThreadRequest::validate() const {
    if (title.has_value() && (title->empty() || title->length() > 500)) {
        return false;
    }
    
    if (description.has_value() && description->length() > 10000) {
        return false;
    }
    
    if (tags.has_value()) {
        if (tags->size() > 50) {
            return false;
        }
        for (const auto& tag : *tags) {
            if (tag.empty() || tag.length() > 100) {
                return false;
            }
        }
    }
    
    return true;
}

ThreadResponse ThreadResponse::from_thread(const Thread& thread) {
    ThreadResponse response;
    response.thread = thread;
    return response;
}

json ThreadResponse::to_json() const {
    json j = thread.to_json();
    j["notes"] = json::array();
    
    for (const auto& note : notes) {
        j["notes"].push_back(note.to_json());
    }
    
    j["statistics"] = json{
        {"total_notes", statistics.total_notes},
        {"total_views", statistics.total_views},
        {"total_engagement", statistics.total_engagement},
        {"engagement_rate", statistics.engagement_rate},
        {"average_time_between_notes", statistics.average_time_between_notes},
        {"total_thread_duration", statistics.total_thread_duration},
        {"calculated_at", statistics.calculated_at}
    };
    
    j["permissions"] = json{
        {"can_edit", can_edit},
        {"can_moderate", can_moderate}
    };
    
    return j;
}

// ThreadController implementation

ThreadController::ThreadController(
    std::shared_ptr<ThreadRepository> thread_repo,
    std::shared_ptr<NoteRepository> note_repo,
    std::shared_ptr<ThreadService> thread_service,
    std::shared_ptr<ThreadSecurity> thread_security,
    std::shared_ptr<core::cache::CacheManager> cache_manager,
    std::shared_ptr<core::security::RateLimiter> rate_limiter)
    : thread_repo_(thread_repo),
      note_repo_(note_repo),
      thread_service_(thread_service),
      thread_security_(thread_security),
      cache_manager_(cache_manager),
      rate_limiter_(rate_limiter) {
    
    spdlog::info("ThreadController initialized");
}

// Core thread operations

json ThreadController::create_thread(const json& request_data, const std::string& user_id) {
    try {
        // Rate limiting
        if (!check_rate_limit(user_id, "create_thread")) {
            return create_error_response(ThreadHttpStatus::TOO_MANY_REQUESTS, "Rate limit exceeded");
        }
        
        // Validate user ID
        if (!is_valid_user_id(user_id)) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "Invalid user ID");
        }
        
        // Parse and validate request
        auto req_opt = parse_and_validate_request<CreateThreadRequest>(request_data);
        if (!req_opt) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "Invalid request data");
        }
        
        CreateThreadRequest req = req_opt.value();
        
        // Verify starter note exists and user has permission
        auto starter_note = note_repo_->get_note_by_id(req.starter_note_id);
        if (!starter_note) {
            return create_error_response(ThreadHttpStatus::NOT_FOUND, "Starter note not found");
        }
        
        if (starter_note->author_id != user_id) {
            return create_error_response(ThreadHttpStatus::FORBIDDEN, "Cannot create thread from another user's note");
        }
        
        // Create thread object
        Thread thread;
        thread.thread_id = core::utils::generate_thread_id();
        thread.starter_note_id = req.starter_note_id;
        thread.author_id = user_id;
        thread.author_username = starter_note->author_username; // Would come from user service
        thread.title = core::validation::sanitize_input(req.title);
        thread.description = core::validation::sanitize_input(req.description);
        
        // Process tags
        for (const auto& tag : req.tags) {
            std::string clean_tag = core::validation::sanitize_hashtag(tag);
            if (!clean_tag.empty()) {
                thread.tags.push_back(clean_tag);
            }
        }
        
        thread.total_notes = 1; // Starting with the initial note
        thread.max_depth = 1;
        thread.is_locked = false;
        thread.is_pinned = false;
        thread.is_published = true;
        thread.allow_replies = req.allow_replies;
        thread.allow_renotes = req.allow_renotes;
        thread.visibility = req.is_public ? Visibility::PUBLIC : Visibility::FOLLOWERS;
        
        // Set timestamps
        auto now = std::time(nullptr);
        thread.created_at = now;
        thread.updated_at = now;
        thread.last_activity_at = now;
        
        // Add the starter note to the thread
        thread.note_ids.push_back(req.starter_note_id);
        
        // Save thread
        if (!thread_repo_->create_thread(thread)) {
            return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Failed to create thread");
        }
        
        // Create response
        ThreadResponse response = create_thread_response(thread, user_id);
        
        log_operation("create_thread", user_id, thread.thread_id);
        return create_success_response(response.to_json(), ThreadHttpStatus::CREATED);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception creating thread for user {}: {}", user_id, e.what());
        return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Internal server error");
    }
}

json ThreadController::get_thread(const std::string& thread_id, const std::string& user_id, bool include_notes) {
    try {
        // Validate thread ID
        if (!is_valid_thread_id(thread_id)) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "Invalid thread ID");
        }
        
        // Check cache first
        auto cached_data = get_cached_thread_data(thread_id);
        if (cached_data && !user_id.empty()) {
            // Return cached data if available and user context allows
            return create_success_response(cached_data.value());
        }
        
        // Get thread with permission check
        auto thread_opt = get_thread_with_permission(thread_id, user_id, ThreadPermission::READ);
        if (!thread_opt) {
            return create_error_response(ThreadHttpStatus::NOT_FOUND, "Thread not found");
        }
        
        Thread thread = thread_opt.value();
        
        // Record view if user is specified
        if (!user_id.empty()) {
            thread_repo_->record_thread_view(thread_id, user_id);
        }
        
        // Create response
        ThreadResponse response = create_thread_response(thread, user_id, include_notes);
        json response_data = response.to_json();
        
        // Cache the response
        cache_thread_data(thread_id, response_data);
        
        log_operation("get_thread", user_id, thread_id);
        return create_success_response(response_data);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting thread {}: {}", thread_id, e.what());
        return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Internal server error");
    }
}

json ThreadController::update_thread(const std::string& thread_id, const json& request_data, const std::string& user_id) {
    try {
        // Rate limiting
        if (!check_rate_limit(user_id, "update_thread")) {
            return create_error_response(ThreadHttpStatus::TOO_MANY_REQUESTS, "Rate limit exceeded");
        }
        
        // Validate IDs
        if (!is_valid_thread_id(thread_id) || !is_valid_user_id(user_id)) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "Invalid thread or user ID");
        }
        
        // Parse and validate request
        auto req_opt = parse_and_validate_request<UpdateThreadRequest>(request_data);
        if (!req_opt) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "Invalid request data");
        }
        
        UpdateThreadRequest req = req_opt.value();
        
        // Get thread with edit permission
        auto thread_opt = get_thread_with_permission(thread_id, user_id, ThreadPermission::EDIT);
        if (!thread_opt) {
            return create_error_response(ThreadHttpStatus::NOT_FOUND, "Thread not found or no permission");
        }
        
        Thread thread = thread_opt.value();
        
        // Apply updates
        bool updated = false;
        
        if (req.title.has_value()) {
            thread.title = core::validation::sanitize_input(req.title.value());
            updated = true;
        }
        
        if (req.description.has_value()) {
            thread.description = core::validation::sanitize_input(req.description.value());
            updated = true;
        }
        
        if (req.tags.has_value()) {
            thread.tags.clear();
            for (const auto& tag : req.tags.value()) {
                std::string clean_tag = core::validation::sanitize_hashtag(tag);
                if (!clean_tag.empty()) {
                    thread.tags.push_back(clean_tag);
                }
            }
            updated = true;
        }
        
        if (req.is_locked.has_value()) {
            thread.is_locked = req.is_locked.value();
            updated = true;
        }
        
        if (req.is_pinned.has_value()) {
            thread.is_pinned = req.is_pinned.value();
            updated = true;
        }
        
        if (req.allow_replies.has_value()) {
            thread.allow_replies = req.allow_replies.value();
            updated = true;
        }
        
        if (req.allow_renotes.has_value()) {
            thread.allow_renotes = req.allow_renotes.value();
            updated = true;
        }
        
        if (!updated) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "No valid updates provided");
        }
        
        // Update timestamps
        thread.updated_at = std::time(nullptr);
        
        // Save changes
        if (!thread_repo_->update_thread(thread)) {
            return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Failed to update thread");
        }
        
        // Invalidate cache
        invalidate_thread_cache(thread_id);
        
        // Create response
        ThreadResponse response = create_thread_response(thread, user_id);
        
        log_operation("update_thread", user_id, thread_id);
        return create_success_response(response.to_json());
        
    } catch (const std::exception& e) {
        spdlog::error("Exception updating thread {}: {}", thread_id, e.what());
        return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Internal server error");
    }
}

json ThreadController::delete_thread(const std::string& thread_id, const std::string& user_id) {
    try {
        // Rate limiting
        if (!check_rate_limit(user_id, "delete_thread")) {
            return create_error_response(ThreadHttpStatus::TOO_MANY_REQUESTS, "Rate limit exceeded");
        }
        
        // Validate IDs
        if (!is_valid_thread_id(thread_id) || !is_valid_user_id(user_id)) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "Invalid thread or user ID");
        }
        
        // Get thread with delete permission
        auto thread_opt = get_thread_with_permission(thread_id, user_id, ThreadPermission::DELETE);
        if (!thread_opt) {
            return create_error_response(ThreadHttpStatus::NOT_FOUND, "Thread not found or no permission");
        }
        
        // Delete thread
        if (!thread_repo_->delete_thread(thread_id)) {
            return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Failed to delete thread");
        }
        
        // Invalidate cache
        invalidate_thread_cache(thread_id);
        
        log_operation("delete_thread", user_id, thread_id);
        return create_success_response(json{{"message", "Thread deleted successfully"}}, ThreadHttpStatus::NO_CONTENT);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception deleting thread {}: {}", thread_id, e.what());
        return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Internal server error");
    }
}

// Thread structure operations

json ThreadController::add_note_to_thread(const std::string& thread_id, const json& request_data, const std::string& user_id) {
    try {
        // Rate limiting
        if (!check_rate_limit(user_id, "add_note_to_thread")) {
            return create_error_response(ThreadHttpStatus::TOO_MANY_REQUESTS, "Rate limit exceeded");
        }
        
        // Validate IDs
        if (!is_valid_thread_id(thread_id) || !is_valid_user_id(user_id)) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "Invalid thread or user ID");
        }
        
        // Parse request
        auto req_opt = parse_and_validate_request<AddNoteToThreadRequest>(request_data);
        if (!req_opt) {
            return create_error_response(ThreadHttpStatus::BAD_REQUEST, "Invalid request data");
        }
        
        AddNoteToThreadRequest req = req_opt.value();
        
        // Get thread with edit permission
        auto thread_opt = get_thread_with_permission(thread_id, user_id, ThreadPermission::EDIT);
        if (!thread_opt) {
            return create_error_response(ThreadHttpStatus::NOT_FOUND, "Thread not found or no permission");
        }
        
        Thread thread = thread_opt.value();
        
        // Check if thread is locked
        if (thread.is_locked && !thread_security_->can_moderate_thread(user_id, thread)) {
            return create_error_response(ThreadHttpStatus::FORBIDDEN, "Thread is locked");
        }
        
        // Verify note exists and user has permission
        auto note = note_repo_->get_note_by_id(req.note_id);
        if (!note) {
            return create_error_response(ThreadHttpStatus::NOT_FOUND, "Note not found");
        }
        
        if (note->author_id != user_id && thread.author_id != user_id) {
            return create_error_response(ThreadHttpStatus::FORBIDDEN, "Cannot add another user's note to thread");
        }
        
        // Check if note is already in thread
        auto it = std::find(thread.note_ids.begin(), thread.note_ids.end(), req.note_id);
        if (it != thread.note_ids.end()) {
            return create_error_response(ThreadHttpStatus::CONFLICT, "Note already in thread");
        }
        
        // Determine position
        int position = req.position.value_or(static_cast<int>(thread.note_ids.size()));
        position = std::max(0, std::min(position, static_cast<int>(thread.note_ids.size())));
        
        // Add note to thread
        if (!thread_repo_->add_note_to_thread(thread_id, req.note_id, position)) {
            return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Failed to add note to thread");
        }
        
        // Update thread metadata
        thread.total_notes++;
        thread.updated_at = std::time(nullptr);
        thread.last_activity_at = std::time(nullptr);
        
        // Update the thread
        thread_repo_->update_thread(thread);
        
        // Invalidate cache
        invalidate_thread_cache(thread_id);
        
        log_operation("add_note_to_thread", user_id, thread_id);
        return create_success_response(json{{"message", "Note added to thread successfully"}});
        
    } catch (const std::exception& e) {
        spdlog::error("Exception adding note to thread {}: {}", thread_id, e.what());
        return create_error_response(ThreadHttpStatus::INTERNAL_SERVER_ERROR, "Internal server error");
    }
}

json ThreadController::health_check() {
    json health_data = json{
        {"status", "healthy"},
        {"service", "thread_controller"},
        {"timestamp", std::time(nullptr)},
        {"checks", json::object()}
    };
    
    // Check dependencies
    if (thread_repo_) {
        health_data["checks"]["thread_repository"] = "connected";
    } else {
        health_data["checks"]["thread_repository"] = "disconnected";
        health_data["status"] = "unhealthy";
    }
    
    if (note_repo_) {
        health_data["checks"]["note_repository"] = "connected";
    } else {
        health_data["checks"]["note_repository"] = "disconnected";
        health_data["status"] = "unhealthy";
    }
    
    if (cache_manager_) {
        health_data["checks"]["cache_manager"] = "connected";
    } else {
        health_data["checks"]["cache_manager"] = "disconnected";
    }
    
    if (rate_limiter_) {
        health_data["checks"]["rate_limiter"] = "connected";
    } else {
        health_data["checks"]["rate_limiter"] = "disconnected";
    }
    
    return create_success_response(health_data);
}

json ThreadController::get_api_info() {
    json api_info = json{
        {"service", "sonet_thread_service"},
        {"version", "1.0.0"},
        {"description", "Twitter-style thread management service"},
        {"author", "Neo Qiss"},
        {"endpoints", json::array({
            "/threads",
            "/threads/{id}",
            "/threads/{id}/notes",
            "/threads/search",
            "/threads/trending",
            "/users/{id}/threads"
        })},
        {"features", json::array({
            "Thread creation and management",
            "Note addition and ordering",
            "Thread discovery and search",
            "Thread moderation",
            "Real-time analytics"
        })}
    };
    
    return create_success_response(api_info);
}

// Private helper methods

template<typename T>
std::optional<T> ThreadController::parse_and_validate_request(const json& request_data) {
    try {
        T request = T::from_json(request_data);
        if (request.validate()) {
            return request;
        }
        return std::nullopt;
    } catch (const std::exception& e) {
        spdlog::warn("Failed to parse request: {}", e.what());
        return std::nullopt;
    }
}

bool ThreadController::check_rate_limit(const std::string& user_id, const std::string& operation) {
    if (!rate_limiter_) {
        return true; // No rate limiting if not configured
    }
    
    std::string key = "thread_" + operation + "_" + user_id;
    return rate_limiter_->check_limit(key, 60, 100); // 100 requests per minute
}

json ThreadController::create_error_response(ThreadHttpStatus status, const std::string& message, const json& details) {
    return json{
        {"error", true},
        {"status", static_cast<int>(status)},
        {"message", message},
        {"details", details},
        {"timestamp", std::time(nullptr)}
    };
}

json ThreadController::create_success_response(const json& data, ThreadHttpStatus status) {
    return json{
        {"error", false},
        {"status", static_cast<int>(status)},
        {"data", data},
        {"timestamp", std::time(nullptr)}
    };
}

std::optional<Thread> ThreadController::get_thread_with_permission(
    const std::string& thread_id, 
    const std::string& user_id, 
    ThreadPermission required_permission) {
    
    auto thread_opt = thread_repo_->get_thread_by_id(thread_id);
    if (!thread_opt) {
        return std::nullopt;
    }
    
    Thread thread = thread_opt.value();
    
    // Check permission if user ID is provided
    if (!user_id.empty() && thread_security_) {
        if (!thread_security_->check_permission(user_id, thread, required_permission)) {
            return std::nullopt;
        }
    }
    
    return thread;
}

ThreadResponse ThreadController::create_thread_response(const Thread& thread, const std::string& user_id, bool include_notes) {
    ThreadResponse response = ThreadResponse::from_thread(thread);
    
    // Load notes if requested
    if (include_notes) {
        response.notes = load_thread_notes_filtered(thread, user_id, false);
    }
    
    // Load statistics
    response.statistics = thread_repo_->get_thread_statistics(thread.thread_id);
    
    // Set permissions
    if (!user_id.empty() && thread_security_) {
        response.can_edit = thread_security_->check_permission(user_id, thread, ThreadPermission::EDIT);
        response.can_moderate = thread_security_->check_permission(user_id, thread, ThreadPermission::MODERATE);
    }
    
    return response;
}

std::vector<Note> ThreadController::load_thread_notes_filtered(const Thread& thread, const std::string& user_id, bool include_hidden) {
    std::vector<Note> notes;
    
    for (const auto& note_id : thread.note_ids) {
        auto note_opt = note_repo_->get_note_by_id(note_id);
        if (note_opt) {
            Note note = note_opt.value();
            
            // Filter based on permissions and visibility
            if (!include_hidden && note.is_hidden) {
                continue;
            }
            
            // Add additional filtering logic here based on user permissions
            notes.push_back(note);
        }
    }
    
    return notes;
}

void ThreadController::cache_thread_data(const std::string& thread_id, const json& data, int ttl_seconds) {
    if (cache_manager_) {
        std::string cache_key = "thread_" + thread_id;
        cache_manager_->set(cache_key, data.dump(), ttl_seconds);
    }
}

std::optional<json> ThreadController::get_cached_thread_data(const std::string& thread_id) {
    if (!cache_manager_) {
        return std::nullopt;
    }
    
    std::string cache_key = "thread_" + thread_id;
    auto cached_str = cache_manager_->get(cache_key);
    
    if (cached_str) {
        try {
            return json::parse(cached_str.value());
        } catch (const std::exception& e) {
            spdlog::warn("Failed to parse cached thread data: {}", e.what());
        }
    }
    
    return std::nullopt;
}

void ThreadController::invalidate_thread_cache(const std::string& thread_id) {
    if (cache_manager_) {
        std::string cache_key = "thread_" + thread_id;
        cache_manager_->remove(cache_key);
    }
}

void ThreadController::log_operation(const std::string& operation, const std::string& user_id, 
                                   const std::string& thread_id, const std::string& status) {
    spdlog::info("Thread operation: {} by user {} on thread {} - {}", 
                operation, user_id, thread_id, status);
}

bool ThreadController::is_valid_thread_id(const std::string& thread_id) const {
    if (thread_id.empty() || thread_id.length() > 100) {
        return false;
    }
    
    // Basic format validation - adjust based on your ID format
    std::regex id_pattern("^[a-zA-Z0-9_-]+$");
    return std::regex_match(thread_id, id_pattern);
}

bool ThreadController::is_valid_user_id(const std::string& user_id) const {
    if (user_id.empty() || user_id.length() > 100) {
        return false;
    }
    
    // Basic format validation - adjust based on your ID format
    std::regex id_pattern("^[a-zA-Z0-9_-]+$");
    return std::regex_match(user_id, id_pattern);
}

json ThreadController::get_pagination_info(int total_count, int limit, int offset) const {
    int page = (offset / limit) + 1;
    int total_pages = (total_count + limit - 1) / limit;
    bool has_more = (offset + limit) < total_count;
    
    return json{
        {"total_count", total_count},
        {"page", page},
        {"per_page", limit},
        {"total_pages", total_pages},
        {"has_more", has_more}
    };
}

} // namespace sonet::note::controllers
