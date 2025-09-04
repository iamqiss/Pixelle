/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This defines the search query model for our Twitter-scale search service.
 * I designed this to handle complex search queries with filtering, ranking,
 * and real-time search capabilities that can scale to billions of notes.
 */

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <optional>
#include <nlohmann/json.hpp>

namespace sonet {
namespace search_service {
namespace models {

/**
 * Search types supported by the search service
 */
enum class SearchType {
    NOTES,           // Search through notes/tweets
    USERS,           // Search for users
    HASHTAGS,        // Search for trending hashtags  
    MENTIONS,        // Search for mentions
    MIXED,           // Search across all content types
    MEDIA,           // Search for media content
    LIVE             // Live/real-time search
};

/**
 * Search result ordering options
 */
enum class SortOrder {
    RELEVANCE,       // Sort by relevance score (default)
    RECENCY,         // Sort by creation time (newest first)
    POPULARITY,      // Sort by engagement metrics
    TRENDING,        // Sort by trending score
    MIXED_SIGNALS    // Combine relevance + recency + popularity
};

/**
 * Search filters for advanced queries
 */
struct SearchFilters {
    // Time-based filters
    std::optional<std::chrono::system_clock::time_point> from_date;
    std::optional<std::chrono::system_clock::time_point> to_date;
    std::optional<std::chrono::hours> last_hours;
    
    // User-based filters
    std::optional<std::string> from_user;
    std::vector<std::string> mentioned_users;
    std::vector<std::string> exclude_users;
    
    // Content filters
    std::vector<std::string> hashtags;
    std::vector<std::string> exclude_hashtags;
    std::optional<bool> has_media;
    std::optional<bool> has_links;
    std::optional<bool> is_verified_user;
    
    // Engagement filters
    std::optional<int> min_likes;
    std::optional<int> min_renotes;
    std::optional<int> min_replies;
    
    // Geographic filters
    std::optional<std::string> location;
    std::optional<double> latitude;
    std::optional<double> longitude;
    std::optional<double> radius_km;
    
    // Language and content type
    std::optional<std::string> language;
    std::vector<std::string> content_types;
    
    /**
     * Convert filters to Elasticsearch query
     */
    nlohmann::json to_elasticsearch_query() const;
    
    /**
     * Check if any filters are applied
     */
    bool has_filters() const;
    
    /**
     * Serialize to JSON
     */
    nlohmann::json to_json() const;
    
    /**
     * Deserialize from JSON
     */
    static SearchFilters from_json(const nlohmann::json& json);
};

/**
 * Search query configuration
 */
struct SearchConfig {
    // Pagination
    int offset = 0;
    int limit = 20;
    int max_limit = 100;
    
    // Search behavior
    bool enable_autocomplete = true;
    bool enable_spell_correction = true;
    bool enable_fuzzy_matching = true;
    bool enable_stemming = true;
    
    // Performance settings
    std::chrono::milliseconds timeout = std::chrono::milliseconds{5000};
    bool use_cache = true;
    std::chrono::minutes cache_ttl = std::chrono::minutes{5};
    
    // Ranking weights
    double relevance_weight = 1.0;
    double recency_weight = 0.3;
    double popularity_weight = 0.5;
    double user_reputation_weight = 0.2;
    
    // Content preferences
    std::vector<std::string> preferred_languages;
    bool include_deleted = false;
    bool include_private = false;
    
    /**
     * Validate configuration
     */
    bool is_valid() const;
    
    /**
     * Get default configuration
     */
    static SearchConfig default_config();
    
    /**
     * Configuration for real-time search
     */
    static SearchConfig realtime_config();
    
    /**
     * Configuration for analytics/trending
     */
    static SearchConfig trending_config();
};

/**
 * Main search query class
 * 
 * This represents a complete search request with query text, filters,
 * configuration, and metadata. I designed this to be highly flexible
 * while maintaining performance for Twitter-scale search operations.
 */
class SearchQuery {
public:
    /**
     * Constructor
     */
    SearchQuery() = default;
    explicit SearchQuery(const std::string& query_text);
    SearchQuery(const std::string& query_text, SearchType type);
    
    // Core query properties
    std::string query_text;
    SearchType search_type = SearchType::MIXED;
    SortOrder sort_order = SortOrder::RELEVANCE;
    
    // Advanced search features
    SearchFilters filters;
    SearchConfig config;
    
    // Request metadata
    std::string user_id;
    std::string session_id;
    std::string client_ip;
    std::string user_agent;
    std::chrono::system_clock::time_point created_at;
    
    // Search personalization
    std::vector<std::string> user_interests;
    std::vector<std::string> following_users;
    std::optional<std::string> user_location;
    std::optional<std::string> user_language;
    
    /**
     * Set basic text query
     */
    SearchQuery& set_query(const std::string& text);
    
    /**
     * Set search type
     */
    SearchQuery& set_type(SearchType type);
    
    /**
     * Set sort order
     */
    SearchQuery& set_sort(SortOrder order);
    
    /**
     * Add pagination
     */
    SearchQuery& set_pagination(int offset, int limit);
    
    /**
     * Add time filter
     */
    SearchQuery& set_time_range(
        const std::optional<std::chrono::system_clock::time_point>& from,
        const std::optional<std::chrono::system_clock::time_point>& to
    );
    
    /**
     * Add user filter
     */
    SearchQuery& set_from_user(const std::string& username);
    
    /**
     * Add hashtag filter
     */
    SearchQuery& add_hashtag(const std::string& hashtag);
    
    /**
     * Add mention filter
     */
    SearchQuery& add_mention(const std::string& username);
    
    /**
     * Add engagement filter
     */
    SearchQuery& set_min_engagement(int min_likes, int min_renotes = 0, int min_replies = 0);
    
    /**
     * Add location filter
     */
    SearchQuery& set_location(double lat, double lon, double radius_km);
    
    /**
     * Set user context for personalization
     */
    SearchQuery& set_user_context(
        const std::string& user_id,
        const std::vector<std::string>& interests = {},
        const std::vector<std::string>& following = {}
    );
    
    /**
     * Parse query from natural language
     * 
     * Examples:
     * "from:@john coffee since:2024-01-01"
     * "machine learning #AI min_likes:100"
     * "@elonmusk tesla near:\"San Francisco\" within:50km"
     */
    static SearchQuery parse_natural_language(const std::string& query);
    
    /**
     * Generate Elasticsearch query
     */
    nlohmann::json to_elasticsearch_query() const;
    
    /**
     * Get query fingerprint for caching
     */
    std::string get_cache_key() const;
    
    /**
     * Validate query
     */
    bool is_valid() const;
    
    /**
     * Get estimated result count (for pagination)
     */
    std::optional<int> estimate_result_count() const;
    
    /**
     * Check if this is a trending/analytics query
     */
    bool is_trending_query() const;
    
    /**
     * Check if this is a real-time search
     */
    bool is_realtime_query() const;
    
    /**
     * Get query complexity score (for performance tuning)
     */
    double get_complexity_score() const;
    
    /**
     * Serialize to JSON
     */
    nlohmann::json to_json() const;
    
    /**
     * Deserialize from JSON
     */
    static SearchQuery from_json(const nlohmann::json& json);
    
    /**
     * Create query for autocomplete suggestions
     */
    static SearchQuery create_autocomplete_query(const std::string& partial_text);
    
    /**
     * Create query for trending topics
     */
    static SearchQuery create_trending_query(
        const std::chrono::hours& time_window = std::chrono::hours{24}
    );
    
    /**
     * Create query for user recommendations
     */
    static SearchQuery create_user_recommendation_query(
        const std::string& user_id,
        const std::vector<std::string>& interests
    );

private:
    /**
     * Parse advanced query operators
     */
    void parse_operators(const std::string& query);
    
    /**
     * Extract hashtags from query text
     */
    std::vector<std::string> extract_hashtags() const;
    
    /**
     * Extract mentions from query text  
     */
    std::vector<std::string> extract_mentions() const;
    
    /**
     * Build personalization boost
     */
    nlohmann::json build_personalization_boost() const;
    
    /**
     * Generate query ID for tracking
     */
    std::string generate_query_id() const;
};

/**
 * Search query builder for fluent interface
 */
class SearchQueryBuilder {
public:
    SearchQueryBuilder() = default;
    
    SearchQueryBuilder& query(const std::string& text);
    SearchQueryBuilder& type(SearchType search_type);
    SearchQueryBuilder& sort(SortOrder order);
    SearchQueryBuilder& limit(int limit);
    SearchQueryBuilder& offset(int offset);
    SearchQueryBuilder& from_user(const std::string& username);
    SearchQueryBuilder& hashtag(const std::string& hashtag);
    SearchQueryBuilder& mention(const std::string& username);
    SearchQueryBuilder& since(const std::chrono::system_clock::time_point& time);
    SearchQueryBuilder& until(const std::chrono::system_clock::time_point& time);
    SearchQueryBuilder& min_likes(int likes);
    SearchQueryBuilder& near(double lat, double lon, double radius_km);
    SearchQueryBuilder& language(const std::string& lang);
    SearchQueryBuilder& with_media();
    SearchQueryBuilder& verified_only();
    SearchQueryBuilder& user_context(const std::string& user_id);
    
    SearchQuery build();

private:
    SearchQuery query_;
};

/**
 * Utility functions for search queries
 */
namespace query_utils {
    /**
     * Parse time expressions like "1h", "3d", "2w"
     */
    std::optional<std::chrono::system_clock::time_point> parse_relative_time(const std::string& expr);
    
    /**
     * Parse absolute time expressions
     */
    std::optional<std::chrono::system_clock::time_point> parse_absolute_time(const std::string& expr);
    
    /**
     * Extract query operators from text
     */
    std::unordered_map<std::string, std::string> extract_operators(const std::string& query);
    
    /**
     * Clean query text by removing operators
     */
    std::string clean_query_text(const std::string& query);
    
    /**
     * Validate username format
     */
    bool is_valid_username(const std::string& username);
    
    /**
     * Validate hashtag format
     */
    bool is_valid_hashtag(const std::string& hashtag);
    
    /**
     * Generate search suggestions
     */
    std::vector<std::string> generate_suggestions(const std::string& partial_query);
}

} // namespace models
} // namespace search_service  
} // namespace sonet