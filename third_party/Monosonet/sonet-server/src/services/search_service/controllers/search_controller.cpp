/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Implementation of the search controller for Twitter-scale search operations.
 * This handles HTTP/gRPC requests, authentication, rate limiting, and response caching.
 */

#include "search_controller.h"
#include <regex>
#include <algorithm>
#include <sstream>
#include <random>
#include <chrono>
#include <future>
#include <unordered_set>
#include <iomanip>
#include <cmath>

namespace sonet {
namespace search_service {
namespace controllers {

/**
 * SearchRequestContext implementation
 */
SearchRequestContext SearchRequestContext::from_http_request(const std::string& user_id, const std::string& session_id, const std::string& ip_address, const std::map<std::string, std::string>& headers) {
    SearchRequestContext context;
    
    context.user_id = user_id;
    context.session_id = session_id;
    context.ip_address = ip_address;
    context.timestamp = std::chrono::system_clock::now();
    
    // Extract user agent
    auto ua_it = headers.find("user-agent");
    if (ua_it != headers.end()) {
        context.user_agent = ua_it->second;
    }
    
    // Extract referer
    auto ref_it = headers.find("referer");
    if (ref_it != headers.end()) {
        context.referer = ref_it->second;
    }
    
    // Extract accept language
    auto lang_it = headers.find("accept-language");
    if (lang_it != headers.end()) {
        context.accept_language = lang_it->second;
    }
    
    // Extract authorization
    auto auth_it = headers.find("authorization");
    if (auth_it != headers.end()) {
        context.authorization_header = auth_it->second;
    }
    
    // Set request ID
    context.request_id = generate_request_id();
    
    return context;
}

bool SearchRequestContext::is_authenticated() const {
    return !user_id.empty() && !authorization_header.empty();
}

std::string SearchRequestContext::get_rate_limit_key() const {
    if (is_authenticated()) {
        return "user:" + user_id;
    } else {
        return "ip:" + ip_address;
    }
}

std::string SearchRequestContext::get_cache_key_prefix() const {
    return "search:" + (is_authenticated() ? user_id : ip_address);
}

std::string SearchRequestContext::generate_request_id() {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1000, 9999);
    
    return std::to_string(timestamp) + "_" + std::to_string(dis(gen));
}

/**
 * SearchResponse implementation
 */
nlohmann::json SearchResponse::to_json() const {
    nlohmann::json response = {
        {"success", success},
        {"message", message},
        {"request_id", request_id},
        {"processing_time_ms", processing_time_ms},
        {"cached", cached},
        {"timestamp", std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()).count()}
    };
    
    if (success && search_result.has_value()) {
        response["result"] = search_result->to_json();
    }
    
    if (!error_code.empty()) {
        response["error_code"] = error_code;
    }
    
    if (!debug_info.empty()) {
        response["debug"] = debug_info;
    }
    
    return response;
}

SearchResponse SearchResponse::success_response(const models::SearchResult& result, const std::string& request_id, int processing_time_ms, bool cached) {
    SearchResponse response;
    response.success = true;
    response.search_result = result;
    response.request_id = request_id;
    response.processing_time_ms = processing_time_ms;
    response.cached = cached;
    response.timestamp = std::chrono::system_clock::now();
    return response;
}

SearchResponse SearchResponse::error_response(const std::string& error_code, const std::string& message, const std::string& request_id) {
    SearchResponse response;
    response.success = false;
    response.error_code = error_code;
    response.message = message;
    response.request_id = request_id;
    response.timestamp = std::chrono::system_clock::now();
    return response;
}

/**
 * RateLimiter implementation
 */
RateLimiter::RateLimiter(int requests_per_minute, int burst_capacity)
    : requests_per_minute_(requests_per_minute), burst_capacity_(burst_capacity) {
}

bool RateLimiter::is_allowed(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    auto& bucket = buckets_[key];
    
    // Refill tokens based on time elapsed
    auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - bucket.last_refill).count();
    if (time_elapsed > 0) {
        double tokens_to_add = (static_cast<double>(requests_per_minute_) / 60000.0) * time_elapsed;
        bucket.tokens = std::min(static_cast<double>(burst_capacity_), bucket.tokens + tokens_to_add);
        bucket.last_refill = now;
    }
    
    // Check if request can be allowed
    if (bucket.tokens >= 1.0) {
        bucket.tokens -= 1.0;
        bucket.request_count++;
        return true;
    }
    
    return false;
}

RateLimitInfo RateLimiter::get_rate_limit_info(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = buckets_.find(key);
    if (it == buckets_.end()) {
        return {burst_capacity_, requests_per_minute_, 0, std::chrono::steady_clock::now()};
    }
    
    const auto& bucket = it->second;
    return {
        static_cast<int>(bucket.tokens),
        requests_per_minute_,
        bucket.request_count,
        bucket.last_refill
    };
}

void RateLimiter::reset_bucket(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    buckets_.erase(key);
}

void RateLimiter::cleanup_old_buckets() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    auto cutoff = now - std::chrono::hours{1};  // Remove buckets older than 1 hour
    
    for (auto it = buckets_.begin(); it != buckets_.end();) {
        if (it->second.last_refill < cutoff) {
            it = buckets_.erase(it);
        } else {
            ++it;
        }
    }
}

/**
 * ResponseCache implementation
 */
ResponseCache::ResponseCache(size_t max_size, std::chrono::minutes ttl)
    : max_size_(max_size), ttl_(ttl) {
}

std::optional<models::SearchResult> ResponseCache::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        return std::nullopt;
    }
    
    // Check if expired
    auto now = std::chrono::system_clock::now();
    if (now - it->second.timestamp > ttl_) {
        cache_.erase(it);
        return std::nullopt;
    }
    
    // Update access time for LRU
    it->second.last_access = now;
    
    return it->second.result;
}

void ResponseCache::put(const std::string& key, const models::SearchResult& result) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Evict if at capacity
    if (cache_.size() >= max_size_) {
        evict_lru();
    }
    
    auto now = std::chrono::system_clock::now();
    cache_[key] = {result, now, now};
}

void ResponseCache::invalidate(const std::string& pattern) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::regex regex_pattern(pattern);
    
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (std::regex_match(it->first, regex_pattern)) {
            it = cache_.erase(it);
        } else {
            ++it;
        }
    }
}

void ResponseCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
}

size_t ResponseCache::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.size();
}

void ResponseCache::cleanup_expired() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto now = std::chrono::system_clock::now();
    
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (now - it->second.timestamp > ttl_) {
            it = cache_.erase(it);
        } else {
            ++it;
        }
    }
}

void ResponseCache::evict_lru() {
    if (cache_.empty()) return;
    
    auto oldest_it = cache_.begin();
    for (auto it = cache_.begin(); it != cache_.end(); ++it) {
        if (it->second.last_access < oldest_it->second.last_access) {
            oldest_it = it;
        }
    }
    
    cache_.erase(oldest_it);
}

/**
 * AuthenticationHandler implementation
 */
AuthenticationResult AuthenticationHandler::authenticate(const SearchRequestContext& context) {
    AuthenticationResult result;
    
    if (!context.is_authenticated()) {
        result.authenticated = false;
        result.user_id = "";
        result.permissions = {"public_search"};
        return result;
    }
    
    // Extract token from authorization header
    std::string token = extract_token(context.authorization_header);
    if (token.empty()) {
        result.authenticated = false;
        result.error = "Invalid authorization header";
        return result;
    }
    
    // Validate token (simplified implementation)
    auto user_info = validate_token(token);
    if (!user_info.valid) {
        result.authenticated = false;
        result.error = "Invalid or expired token";
        return result;
    }
    
    result.authenticated = true;
    result.user_id = user_info.user_id;
    result.permissions = user_info.permissions;
    result.rate_limit_tier = user_info.rate_limit_tier;
    
    return result;
}

std::string AuthenticationHandler::extract_token(const std::string& auth_header) {
    // Expected format: "Bearer <token>"
    const std::string bearer_prefix = "Bearer ";
    
    if (auth_header.substr(0, bearer_prefix.length()) != bearer_prefix) {
        return "";
    }
    
    return auth_header.substr(bearer_prefix.length());
}

AuthenticationHandler::TokenInfo AuthenticationHandler::validate_token(const std::string& token) {
    // This is a simplified implementation
    // In production, you would validate JWT tokens, check against a database, etc.
    
    TokenInfo info;
    info.valid = false;
    
    // Basic token format validation
    if (token.length() < 32) {
        return info;
    }
    
    // Simulate token validation
    // In reality, this would involve JWT verification, database lookup, etc.
    if (token.substr(0, 4) == "test") {
        info.valid = true;
        info.user_id = "test_user_" + token.substr(4, 8);
        info.permissions = {"public_search", "advanced_search", "export_results"};
        info.rate_limit_tier = "standard";
        
        // Check for premium users
        if (token.find("premium") != std::string::npos) {
            info.permissions.push_back("real_time_search");
            info.permissions.push_back("analytics");
            info.rate_limit_tier = "premium";
        }
    }
    
    return info;
}

/**
 * SearchController::Impl - Private implementation
 */
struct SearchController::Impl {
    std::shared_ptr<engines::ElasticsearchEngine> engine;
    SearchControllerConfig config;
    
    // Components
    std::unique_ptr<RateLimiter> rate_limiter;
    std::unique_ptr<ResponseCache> response_cache;
    std::unique_ptr<AuthenticationHandler> auth_handler;
    
    // Metrics
    mutable std::mutex metrics_mutex;
    SearchControllerMetrics metrics;
    
    // Trending data cache
    mutable std::mutex trending_mutex;
    std::vector<models::TrendingItem> trending_hashtags;
    std::vector<models::TrendingItem> trending_users;
    std::chrono::system_clock::time_point last_trending_update;
    
    // Suggestion cache
    mutable std::mutex suggestions_mutex;
    std::unordered_map<std::string, std::vector<std::string>> suggestion_cache;
    std::chrono::system_clock::time_point last_suggestions_update;
    
    Impl(std::shared_ptr<engines::ElasticsearchEngine> eng, const SearchControllerConfig& cfg)
        : engine(std::move(eng)), config(cfg) {
        
        // Initialize rate limiter based on config
        int rpm = config.authenticated_rate_limit_rpm;
        int burst = config.authenticated_burst_capacity;
        rate_limiter = std::make_unique<RateLimiter>(rpm, burst);
        
        // Initialize response cache
        response_cache = std::make_unique<ResponseCache>(
            config.cache_max_size,
            std::chrono::minutes{config.cache_ttl_minutes}
        );
        
        // Initialize authentication handler
        auth_handler = std::make_unique<AuthenticationHandler>();
        
        // Initialize metrics
        metrics.last_reset = std::chrono::system_clock::now();
        
        // Initialize trending data
        last_trending_update = std::chrono::system_clock::time_point{};
        last_suggestions_update = std::chrono::system_clock::time_point{};
    }
};

/**
 * SearchController implementation
 */
SearchController::SearchController(std::shared_ptr<engines::ElasticsearchEngine> engine, const SearchControllerConfig& config)
    : pimpl_(std::make_unique<Impl>(std::move(engine), config)) {
}

SearchController::~SearchController() = default;

SearchResponse SearchController::search_notes(const models::SearchQuery& query, const SearchRequestContext& context) {
    auto start_time = std::chrono::steady_clock::now();
    
    // Authenticate request
    auto auth_result = pimpl_->auth_handler->authenticate(context);
    if (!auth_result.authenticated && !has_permission(auth_result.permissions, "public_search")) {
        return SearchResponse::error_response("AUTHENTICATION_REQUIRED", "Authentication required for search", context.request_id);
    }
    
    // Check rate limiting
    std::string rate_limit_key = context.get_rate_limit_key();
    if (!pimpl_->rate_limiter->is_allowed(rate_limit_key)) {
        update_metrics("search_notes", false, "RATE_LIMIT_EXCEEDED");
        return SearchResponse::error_response("RATE_LIMIT_EXCEEDED", "Rate limit exceeded", context.request_id);
    }
    
    // Generate cache key
    std::string cache_key = generate_cache_key("notes", query, context);
    
    // Check cache
    if (pimpl_->config.enable_caching) {
        auto cached_result = pimpl_->response_cache->get(cache_key);
        if (cached_result.has_value()) {
            auto end_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            update_metrics("search_notes", true, "SUCCESS");
            return SearchResponse::success_response(cached_result.value(), context.request_id, static_cast<int>(duration.count()), true);
        }
    }
    
    try {
        // Perform search
        auto search_future = pimpl_->engine->search_notes(query);
        auto search_result = search_future.get();
        
        // Apply note-processing filters
        search_result = apply_personalization(search_result, auth_result);
        search_result = apply_content_filters(search_result, context);
        
        // Cache result
        if (pimpl_->config.enable_caching && !search_result.notes.empty()) {
            pimpl_->response_cache->put(cache_key, search_result);
        }
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        update_metrics("search_notes", true, "SUCCESS");
        return SearchResponse::success_response(search_result, context.request_id, static_cast<int>(duration.count()), false);
        
    } catch (const std::exception& e) {
        update_metrics("search_notes", false, "SEARCH_ERROR");
        return SearchResponse::error_response("SEARCH_ERROR", std::string("Search failed: ") + e.what(), context.request_id);
    }
}

SearchResponse SearchController::search_users(const models::SearchQuery& query, const SearchRequestContext& context) {
    auto start_time = std::chrono::steady_clock::now();
    
    // Authenticate request
    auto auth_result = pimpl_->auth_handler->authenticate(context);
    if (!auth_result.authenticated && !has_permission(auth_result.permissions, "public_search")) {
        return SearchResponse::error_response("AUTHENTICATION_REQUIRED", "Authentication required for search", context.request_id);
    }
    
    // Check rate limiting
    std::string rate_limit_key = context.get_rate_limit_key();
    if (!pimpl_->rate_limiter->is_allowed(rate_limit_key)) {
        update_metrics("search_users", false, "RATE_LIMIT_EXCEEDED");
        return SearchResponse::error_response("RATE_LIMIT_EXCEEDED", "Rate limit exceeded", context.request_id);
    }
    
    // Generate cache key
    std::string cache_key = generate_cache_key("users", query, context);
    
    // Check cache
    if (pimpl_->config.enable_caching) {
        auto cached_result = pimpl_->response_cache->get(cache_key);
        if (cached_result.has_value()) {
            auto end_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            update_metrics("search_users", true, "SUCCESS");
            return SearchResponse::success_response(cached_result.value(), context.request_id, static_cast<int>(duration.count()), true);
        }
    }
    
    try {
        // Perform search
        auto search_future = pimpl_->engine->search_users(query);
        auto search_result = search_future.get();
        
        // Apply note-processing filters
        search_result = apply_user_filters(search_result, auth_result, context);
        
        // Cache result
        if (pimpl_->config.enable_caching && !search_result.users.empty()) {
            pimpl_->response_cache->put(cache_key, search_result);
        }
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        update_metrics("search_users", true, "SUCCESS");
        return SearchResponse::success_response(search_result, context.request_id, static_cast<int>(duration.count()), false);
        
    } catch (const std::exception& e) {
        update_metrics("search_users", false, "SEARCH_ERROR");
        return SearchResponse::error_response("SEARCH_ERROR", std::string("Search failed: ") + e.what(), context.request_id);
    }
}

SearchResponse SearchController::get_trending_hashtags(const SearchRequestContext& context) {
    auto start_time = std::chrono::steady_clock::now();
    
    // Check rate limiting
    std::string rate_limit_key = context.get_rate_limit_key();
    if (!pimpl_->rate_limiter->is_allowed(rate_limit_key)) {
        update_metrics("trending_hashtags", false, "RATE_LIMIT_EXCEEDED");
        return SearchResponse::error_response("RATE_LIMIT_EXCEEDED", "Rate limit exceeded", context.request_id);
    }
    
    try {
        // Get trending hashtags
        auto trending_data = get_trending_data();
        
        // Create search result
        models::SearchResult result;
        result.trending_hashtags = trending_data.first;
        result.total_results = static_cast<int>(trending_data.first.size());
        result.processing_time_ms = 0;  // Will be updated
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        update_metrics("trending_hashtags", true, "SUCCESS");
        return SearchResponse::success_response(result, context.request_id, static_cast<int>(duration.count()), false);
        
    } catch (const std::exception& e) {
        update_metrics("trending_hashtags", false, "ERROR");
        return SearchResponse::error_response("ERROR", std::string("Failed to get trending hashtags: ") + e.what(), context.request_id);
    }
}

SearchResponse SearchController::get_trending_users(const SearchRequestContext& context) {
    auto start_time = std::chrono::steady_clock::now();
    
    // Check rate limiting
    std::string rate_limit_key = context.get_rate_limit_key();
    if (!pimpl_->rate_limiter->is_allowed(rate_limit_key)) {
        update_metrics("trending_users", false, "RATE_LIMIT_EXCEEDED");
        return SearchResponse::error_response("RATE_LIMIT_EXCEEDED", "Rate limit exceeded", context.request_id);
    }
    
    try {
        // Get trending users
        auto trending_data = get_trending_data();
        
        // Create search result
        models::SearchResult result;
        result.trending_users = trending_data.second;
        result.total_results = static_cast<int>(trending_data.second.size());
        result.processing_time_ms = 0;  // Will be updated
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        update_metrics("trending_users", true, "SUCCESS");
        return SearchResponse::success_response(result, context.request_id, static_cast<int>(duration.count()), false);
        
    } catch (const std::exception& e) {
        update_metrics("trending_users", false, "ERROR");
        return SearchResponse::error_response("ERROR", std::string("Failed to get trending users: ") + e.what(), context.request_id);
    }
}

SearchResponse SearchController::get_suggestions(const std::string& prefix, const SearchRequestContext& context) {
    auto start_time = std::chrono::steady_clock::now();
    
    // Check rate limiting (more lenient for suggestions)
    std::string rate_limit_key = context.get_rate_limit_key();
    if (!pimpl_->rate_limiter->is_allowed(rate_limit_key)) {
        update_metrics("suggestions", false, "RATE_LIMIT_EXCEEDED");
        return SearchResponse::error_response("RATE_LIMIT_EXCEEDED", "Rate limit exceeded", context.request_id);
    }
    
    if (prefix.length() < 2) {
        return SearchResponse::error_response("INVALID_QUERY", "Prefix must be at least 2 characters", context.request_id);
    }
    
    try {
        // Get suggestions
        auto suggestions = get_search_suggestions(prefix, context);
        
        // Create search result
        models::SearchResult result;
        result.suggestions = suggestions;
        result.total_results = static_cast<int>(suggestions.size());
        result.processing_time_ms = 0;  // Will be updated
        
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        update_metrics("suggestions", true, "SUCCESS");
        return SearchResponse::success_response(result, context.request_id, static_cast<int>(duration.count()), false);
        
    } catch (const std::exception& e) {
        update_metrics("suggestions", false, "ERROR");
        return SearchResponse::error_response("ERROR", std::string("Failed to get suggestions: ") + e.what(), context.request_id);
    }
}

SearchResponse SearchController::autocomplete(const std::string& query, const SearchRequestContext& context) {
    // Autocomplete is similar to suggestions but with different logic
    return get_suggestions(query, context);
}

SearchControllerMetrics SearchController::get_metrics() const {
    std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
    return pimpl_->metrics;
}

void SearchController::clear_cache() {
    pimpl_->response_cache->clear();
    
    std::lock_guard<std::mutex> suggestions_lock(pimpl_->suggestions_mutex);
    pimpl_->suggestion_cache.clear();
}

void SearchController::update_rate_limits(const std::string& tier, int requests_per_minute, int burst_capacity) {
    // This would update rate limits for specific tiers
    // For now, we just recreate the rate limiter
    pimpl_->rate_limiter = std::make_unique<RateLimiter>(requests_per_minute, burst_capacity);
}

std::string SearchController::generate_cache_key(const std::string& search_type, const models::SearchQuery& query, const SearchRequestContext& context) {
    std::ostringstream oss;
    oss << context.get_cache_key_prefix() << ":"
        << search_type << ":"
        << std::hash<std::string>{}(query.to_string()) << ":"
        << query.offset << ":"
        << query.limit;
    
    return oss.str();
}

bool SearchController::has_permission(const std::vector<std::string>& permissions, const std::string& required_permission) {
    return std::find(permissions.begin(), permissions.end(), required_permission) != permissions.end();
}

models::SearchResult SearchController::apply_personalization(const models::SearchResult& result, const AuthenticationResult& auth) {
    if (!auth.authenticated) {
        return result;  // No personalization for unauthenticated users
    }
    
    models::SearchResult personalized = result;
    
    // Apply personalization logic here
    // This could include:
    // - Boosting results from followed users
    // - Filtering based on user preferences
    // - Reranking based on user's interaction history
    
    return personalized;
}

models::SearchResult SearchController::apply_content_filters(const models::SearchResult& result, const SearchRequestContext& context) {
    models::SearchResult filtered = result;
    
    // Apply content filtering
    auto it = std::remove_if(filtered.notes.begin(), filtered.notes.end(), [](const models::NoteResult& note) {
        // Filter out NSFW content for unauthenticated users
        // Filter out content from suspended users
        // Apply other content policies
        return note.nsfw || note.user_suspended;
    });
    
    filtered.notes.erase(it, filtered.notes.end());
    filtered.total_results = static_cast<int>(filtered.notes.size());
    
    return filtered;
}

models::SearchResult SearchController::apply_user_filters(const models::SearchResult& result, const AuthenticationResult& auth, const SearchRequestContext& context) {
    models::SearchResult filtered = result;
    
    // Apply user filtering
    auto it = std::remove_if(filtered.users.begin(), filtered.users.end(), [](const models::UserResult& user) {
        // Filter out suspended or deleted users
        // Filter out private accounts for non-followers
        return user.is_suspended || user.is_deleted;
    });
    
    filtered.users.erase(it, filtered.users.end());
    filtered.total_results = static_cast<int>(filtered.users.size());
    
    return filtered;
}

std::pair<std::vector<models::TrendingItem>, std::vector<models::TrendingItem>> SearchController::get_trending_data() {
    std::lock_guard<std::mutex> lock(pimpl_->trending_mutex);
    
    auto now = std::chrono::system_clock::now();
    auto update_interval = std::chrono::minutes{5};  // Update every 5 minutes
    
    if (now - pimpl_->last_trending_update > update_interval) {
        // Update trending data
        pimpl_->trending_hashtags = fetch_trending_hashtags();
        pimpl_->trending_users = fetch_trending_users();
        pimpl_->last_trending_update = now;
    }
    
    return {pimpl_->trending_hashtags, pimpl_->trending_users};
}

std::vector<models::TrendingItem> SearchController::fetch_trending_hashtags() {
    // This would fetch real trending data from Elasticsearch
    // For now, return mock data
    std::vector<models::TrendingItem> trending;
    
    std::vector<std::string> mock_hashtags = {
        "technology", "ai", "programming", "socialmedia", "trending",
        "news", "sports", "music", "art", "photography"
    };
    
    for (size_t i = 0; i < mock_hashtags.size(); ++i) {
        models::TrendingItem item;
        item.text = mock_hashtags[i];
        item.volume = 10000 - (i * 1000);  // Decreasing volume
        item.change_percentage = 5.0f + (i * 0.5f);  // Mock growth
        trending.push_back(item);
    }
    
    return trending;
}

std::vector<models::TrendingItem> SearchController::fetch_trending_users() {
    // This would fetch real trending users from Elasticsearch
    // For now, return mock data
    std::vector<models::TrendingItem> trending;
    
    std::vector<std::string> mock_users = {
        "tech_guru", "ai_researcher", "social_media_expert", "news_anchor", "sports_fan"
    };
    
    for (size_t i = 0; i < mock_users.size(); ++i) {
        models::TrendingItem item;
        item.text = mock_users[i];
        item.volume = 5000 - (i * 500);  // Decreasing volume
        item.change_percentage = 3.0f + (i * 0.3f);  // Mock growth
        trending.push_back(item);
    }
    
    return trending;
}

std::vector<std::string> SearchController::get_search_suggestions(const std::string& prefix, const SearchRequestContext& context) {
    std::lock_guard<std::mutex> lock(pimpl_->suggestions_mutex);
    
    auto now = std::chrono::system_clock::now();
    auto update_interval = std::chrono::minutes{10};  // Update every 10 minutes
    
    std::string cache_key = "suggestions:" + prefix.substr(0, 2);  // Cache by first 2 characters
    
    if (now - pimpl_->last_suggestions_update > update_interval || pimpl_->suggestion_cache.find(cache_key) == pimpl_->suggestion_cache.end()) {
        // Update suggestions
        pimpl_->suggestion_cache[cache_key] = fetch_suggestions(prefix);
        pimpl_->last_suggestions_update = now;
    }
    
    // Filter suggestions by prefix
    std::vector<std::string> filtered_suggestions;
    const auto& cached_suggestions = pimpl_->suggestion_cache[cache_key];
    
    for (const auto& suggestion : cached_suggestions) {
        if (suggestion.substr(0, prefix.length()) == prefix) {
            filtered_suggestions.push_back(suggestion);
        }
    }
    
    return filtered_suggestions;
}

std::vector<std::string> SearchController::fetch_suggestions(const std::string& prefix) {
    // This would fetch real suggestions from Elasticsearch
    // For now, return mock data
    std::vector<std::string> suggestions;
    
    // Add some common prefixes
    if (prefix.substr(0, 2) == "te") {
        suggestions = {"technology", "tech", "tesla", "testing", "team", "technical", "television", "tennis"};
    } else if (prefix.substr(0, 2) == "ai") {
        suggestions = {"artificial intelligence", "ai", "airport", "air", "airbnb", "airline"};
    } else if (prefix.substr(0, 2) == "so") {
        suggestions = {"social media", "software", "sonet", "soccer", "solution", "society", "sound", "source"};
    } else {
        // Generic suggestions
        suggestions = {prefix + "1", prefix + "2", prefix + "3"};
    }
    
    return suggestions;
}

void SearchController::update_metrics(const std::string& operation, bool success, const std::string& status) {
    std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
    
    pimpl_->metrics.total_requests++;
    
    if (success) {
        pimpl_->metrics.successful_requests++;
    } else {
        pimpl_->metrics.failed_requests++;
    }
    
    // Update operation-specific metrics
    if (operation == "search_notes") {
        pimpl_->metrics.note_searches++;
    } else if (operation == "search_users") {
        pimpl_->metrics.user_searches++;
    } else if (operation == "trending_hashtags" || operation == "trending_users") {
        pimpl_->metrics.trending_requests++;
    } else if (operation == "suggestions") {
        pimpl_->metrics.suggestion_requests++;
    }
    
    // Update status counters
    if (status == "RATE_LIMIT_EXCEEDED") {
        pimpl_->metrics.rate_limited_requests++;
    } else if (status == "AUTHENTICATION_REQUIRED") {
        pimpl_->metrics.authentication_failures++;
    }
}

/**
 * SearchControllerMetrics implementation
 */
nlohmann::json SearchControllerMetrics::to_json() const {
    auto now = std::chrono::system_clock::now();
    auto uptime_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - last_reset).count();
    
    return nlohmann::json{
        {"total_requests", total_requests.load()},
        {"successful_requests", successful_requests.load()},
        {"failed_requests", failed_requests.load()},
        {"note_searches", note_searches.load()},
        {"user_searches", user_searches.load()},
        {"trending_requests", trending_requests.load()},
        {"suggestion_requests", suggestion_requests.load()},
        {"rate_limited_requests", rate_limited_requests.load()},
        {"authentication_failures", authentication_failures.load()},
        {"cache_hits", cache_hits.load()},
        {"cache_misses", cache_misses.load()},
        {"average_response_time_ms", get_average_response_time_ms()},
        {"success_rate", get_success_rate()},
        {"cache_hit_rate", get_cache_hit_rate()},
        {"uptime_seconds", uptime_seconds}
    };
}

void SearchControllerMetrics::reset() {
    total_requests = 0;
    successful_requests = 0;
    failed_requests = 0;
    note_searches = 0;
    user_searches = 0;
    trending_requests = 0;
    suggestion_requests = 0;
    rate_limited_requests = 0;
    authentication_failures = 0;
    cache_hits = 0;
    cache_misses = 0;
    total_response_time_ms = 0;
    last_reset = std::chrono::system_clock::now();
}

double SearchControllerMetrics::get_success_rate() const {
    long total = total_requests.load();
    if (total == 0) return 0.0;
    
    return static_cast<double>(successful_requests.load()) / total;
}

double SearchControllerMetrics::get_cache_hit_rate() const {
    long total_cache_requests = cache_hits.load() + cache_misses.load();
    if (total_cache_requests == 0) return 0.0;
    
    return static_cast<double>(cache_hits.load()) / total_cache_requests;
}

double SearchControllerMetrics::get_average_response_time_ms() const {
    long total = total_requests.load();
    if (total == 0) return 0.0;
    
    return static_cast<double>(total_response_time_ms.load()) / total;
}

} // namespace controllers
} // namespace search_service
} // namespace sonet