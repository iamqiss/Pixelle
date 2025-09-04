/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Search controller for handling Twitter-scale search requests.
 * This orchestrates all search operations with intelligent routing,
 * caching, rate limiting, and real-time responses.
 */

#pragma once

#include "../engines/elasticsearch_engine.h"
#include "../models/search_query.h"
#include "../models/search_result.h"
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <memory>
#include <future>
#include <chrono>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <optional>

namespace sonet {
namespace search_service {
namespace controllers {

struct SearchRequestContext {
    std::string user_id;
    std::string session_id;
    std::string ip_address;
    std::string authorization_header;
    std::string user_agent;
    std::string referer;
    std::string accept_language;
    std::string request_id;
    std::chrono::system_clock::time_point timestamp;

    static SearchRequestContext from_http_request(
        const std::string& user_id,
        const std::string& session_id,
        const std::string& ip_address,
        const std::map<std::string, std::string>& headers
    );

    bool is_authenticated() const;
    std::string get_rate_limit_key() const;
    std::string get_cache_key_prefix() const;
    static std::string generate_request_id();
};

struct SearchResponse {
    bool success = false;
    std::string message;
    std::string request_id;
    int processing_time_ms = 0;
    bool cached = false;
    std::chrono::system_clock::time_point timestamp;

    std::optional<models::SearchResult> search_result;
    std::string error_code;
    nlohmann::json debug_info;

    nlohmann::json to_json() const;

    static SearchResponse success_response(
        const models::SearchResult& result,
        const std::string& request_id,
        int processing_time_ms,
        bool cached
    );

    static SearchResponse error_response(
        const std::string& error_code,
        const std::string& message,
        const std::string& request_id
    );
};

struct SearchControllerMetrics {
    std::atomic<long> total_requests{0};
    std::atomic<long> successful_requests{0};
    std::atomic<long> failed_requests{0};
    std::atomic<long> rate_limited_requests{0};
    std::atomic<long> authentication_failures{0};
    std::atomic<long> cache_hits{0};
    std::atomic<long> cache_misses{0};

    std::atomic<long> note_searches{0};
    std::atomic<long> user_searches{0};
    std::atomic<long> suggestion_requests{0};
    std::atomic<long> trending_requests{0};

    std::atomic<long> total_response_time_ms{0};

    std::chrono::system_clock::time_point last_reset;

    nlohmann::json to_json() const;
    void reset();
    double get_average_response_time_ms() const;
    double get_success_rate() const;
    double get_cache_hit_rate() const;
};

class RateLimiter {
public:
    struct RateLimitInfo {
        int remaining_tokens;
        int rate_per_minute;
        long request_count;
        std::chrono::steady_clock::time_point last_refill;
    };

    RateLimiter(int requests_per_minute, int burst_capacity);

    bool is_allowed(const std::string& key);
    RateLimitInfo get_rate_limit_info(const std::string& key);
    void reset_bucket(const std::string& key);
    void cleanup_old_buckets();

private:
    int requests_per_minute_;
    int burst_capacity_;
    struct Bucket {
        double tokens = 0.0;
        long request_count = 0;
        std::chrono::steady_clock::time_point last_refill = std::chrono::steady_clock::now();
    };
    std::unordered_map<std::string, Bucket> buckets_;
    std::mutex mutex_;
};

class ResponseCache {
public:
    ResponseCache(size_t max_size, std::chrono::minutes ttl);

    std::optional<models::SearchResult> get(const std::string& key);
    void put(const std::string& key, const models::SearchResult& result);
    void invalidate(const std::string& pattern);
    void clear();
    size_t size() const;
    void cleanup_expired();

private:
    struct Entry {
        models::SearchResult result;
        std::chrono::system_clock::time_point timestamp;
        std::chrono::system_clock::time_point last_access;
    };
    size_t max_size_;
    std::chrono::minutes ttl_;
    std::unordered_map<std::string, Entry> cache_;
    mutable std::mutex mutex_;

    void evict_lru();
};

class AuthenticationHandler {
public:
    struct TokenInfo {
        bool valid = false;
        std::string user_id;
        std::vector<std::string> permissions;
        std::string rate_limit_tier;
    };

    struct AuthenticationResult {
        bool authenticated = false;
        std::string user_id;
        std::vector<std::string> permissions;
        std::string rate_limit_tier;
        std::string error;
    };

    AuthenticationResult authenticate(const SearchRequestContext& context);

private:
    static std::string extract_token(const std::string& auth_header);
    static TokenInfo validate_token(const std::string& token);
};

struct SearchControllerConfig {
    bool enable_caching = true;
    int cache_max_size = 10000;
    int cache_ttl_minutes = 5;

    bool enable_rate_limiting = true;
    int authenticated_rate_limit_rpm = 1000;
    int authenticated_burst_capacity = 50;
};

class SearchController {
public:
    explicit SearchController(
        std::shared_ptr<engines::ElasticsearchEngine> engine,
        const SearchControllerConfig& config
    );
    ~SearchController();

    SearchResponse search_notes(const models::SearchQuery& query, const SearchRequestContext& context);
    SearchResponse search_users(const models::SearchQuery& query, const SearchRequestContext& context);
    SearchResponse get_trending_hashtags(const SearchRequestContext& context);
    SearchResponse get_trending_users(const SearchRequestContext& context);
    SearchResponse get_suggestions(const std::string& prefix, const SearchRequestContext& context);
    SearchResponse autocomplete(const std::string& query, const SearchRequestContext& context);

    SearchControllerMetrics get_metrics() const;
    void clear_cache();
    void update_rate_limits(const std::string& tier, int requests_per_minute, int burst_capacity);

private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
};

} // namespace controllers
} // namespace search_service
} // namespace sonet