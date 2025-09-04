/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This defines the search result models for our Twitter-scale search service.
 * I designed this to handle rich search results with highlighting, aggregations,
 * and metadata that helps users find exactly what they're looking for.
 */

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <optional>
#include <nlohmann/json.hpp>
#include "search_query.h"

namespace sonet {
namespace search_service {
namespace models {

/**
 * Search result types
 */
enum class ResultType {
    NOTE,           // Individual note/tweet result
    USER,           // User profile result
    HASHTAG,        // Hashtag result with stats
    TREND,          // Trending topic result
    SUGGESTION,     // Search suggestion
    AGGREGATION     // Aggregated/grouped result
};

/**
 * Individual note result from search
 */
struct NoteResult {
    // Basic note information
    std::string note_id;
    std::string content;
    std::string author_id;
    std::string author_username;
    std::string author_display_name;
    std::optional<std::string> author_avatar_url;
    bool author_verified = false;
    std::chrono::system_clock::time_point created_at;
    
    // Engagement metrics
    int likes_count = 0;
    int renotes_count = 0;
    int replies_count = 0;
    int views_count = 0;
    double engagement_rate = 0.0;
    
    // Content analysis
    std::vector<std::string> hashtags;
    std::vector<std::string> mentions;
    std::vector<std::string> urls;
    std::optional<std::string> language;
    std::optional<std::string> sentiment;  // positive, negative, neutral
    
    // Media information
    bool has_media = false;
    std::vector<std::string> media_urls;
    std::vector<std::string> media_types;  // image, video, gif
    
    // Thread information
    std::optional<std::string> reply_to_note_id;
    std::optional<std::string> thread_id;
    bool is_thread_starter = false;
    int thread_position = 0;
    
    // Search-specific data
    double relevance_score = 0.0;
    std::unordered_map<std::string, std::vector<std::string>> highlights;
    std::vector<std::string> matched_fields;
    
    /**
     * Convert to JSON for API response
     */
    nlohmann::json to_json() const;
    
    /**
     * Create from Elasticsearch document
     */
    static NoteResult from_elasticsearch_doc(const nlohmann::json& doc);
    
    /**
     * Get snippet of content for display
     */
    std::string get_content_snippet(int max_length = 280) const;
    
    /**
     * Check if this is a renote
     */
    bool is_renote() const;
    
    /**
     * Get display timestamp
     */
    std::string get_display_timestamp() const;
};

/**
 * User result from search
 */
struct UserResult {
    // Basic user information
    std::string user_id;
    std::string username;
    std::string display_name;
    std::optional<std::string> bio;
    std::optional<std::string> avatar_url;
    std::optional<std::string> banner_url;
    std::optional<std::string> location;
    std::optional<std::string> website;
    bool verified = false;
    std::chrono::system_clock::time_point created_at;
    
    // Social metrics
    int followers_count = 0;
    int following_count = 0;
    int notes_count = 0;
    int listed_count = 0;
    double engagement_rate = 0.0;
    
    // Activity information
    std::chrono::system_clock::time_point last_active;
    std::optional<std::string> last_note_content;
    std::chrono::system_clock::time_point last_note_time;
    
    // Search-specific data
    double relevance_score = 0.0;
    std::unordered_map<std::string, std::vector<std::string>> highlights;
    std::vector<std::string> matched_fields;
    std::optional<std::string> match_reason;  // "followed_by_friends", "similar_interests", etc.
    
    // User relationship (relative to searching user)
    bool is_following = false;
    bool is_followed_by = false;
    bool is_blocked = false;
    bool is_muted = false;
    
    /**
     * Convert to JSON for API response
     */
    nlohmann::json to_json() const;
    
    /**
     * Create from Elasticsearch document
     */
    static UserResult from_elasticsearch_doc(const nlohmann::json& doc);
    
    /**
     * Get display bio snippet
     */
    std::string get_bio_snippet(int max_length = 160) const;
    
    /**
     * Calculate user reputation score
     */
    double get_reputation_score() const;
};

/**
 * Hashtag result with trending information
 */
struct HashtagResult {
    std::string hashtag;
    std::string display_hashtag;  // With proper casing
    
    // Usage statistics
    int total_uses = 0;
    int recent_uses_1h = 0;
    int recent_uses_24h = 0;
    int recent_uses_7d = 0;
    
    // Trending information
    double trending_score = 0.0;
    int trending_rank = 0;
    double velocity = 0.0;  // Rate of growth
    
    // Content samples
    std::vector<std::string> sample_note_ids;
    std::vector<std::string> top_contributors;
    
    // Search-specific data
    double relevance_score = 0.0;
    std::unordered_map<std::string, std::vector<std::string>> highlights;
    
    /**
     * Convert to JSON for API response
     */
    nlohmann::json to_json() const;
    
    /**
     * Create from aggregation data
     */
    static HashtagResult from_aggregation(const nlohmann::json& agg_data);
    
    /**
     * Get trending status
     */
    std::string get_trending_status() const;  // "hot", "rising", "stable", "declining"
};

/**
 * Search suggestion result
 */
struct SuggestionResult {
    std::string suggestion_text;
    std::string completion_text;
    ResultType suggestion_type;
    double confidence_score = 0.0;
    int estimated_results = 0;
    
    // Context information
    std::optional<std::string> context;  // "trending", "recent", "popular"
    std::vector<std::string> related_terms;
    
    /**
     * Convert to JSON for API response
     */
    nlohmann::json to_json() const;
};

/**
 * Search aggregation data
 */
struct SearchAggregations {
    // Time-based distribution
    std::unordered_map<std::string, int> time_distribution;  // "2024-01-01" -> count
    
    // User distribution
    std::unordered_map<std::string, int> top_users;  // username -> count
    
    // Hashtag distribution
    std::unordered_map<std::string, int> top_hashtags;  // hashtag -> count
    
    // Language distribution
    std::unordered_map<std::string, int> language_distribution;  // "en" -> count
    
    // Media type distribution
    std::unordered_map<std::string, int> media_types;  // "image" -> count
    
    // Engagement ranges
    std::unordered_map<std::string, int> engagement_ranges;  // "0-10" -> count
    
    /**
     * Convert to JSON for API response
     */
    nlohmann::json to_json() const;
    
    /**
     * Create from Elasticsearch aggregations
     */
    static SearchAggregations from_elasticsearch_aggs(const nlohmann::json& aggs);
};

/**
 * Search metadata and performance information
 */
struct SearchMetadata {
    // Query information
    std::string query_id;
    SearchQuery original_query;
    std::string processed_query_text;
    
    // Performance metrics
    std::chrono::milliseconds took;
    std::chrono::milliseconds elasticsearch_time;
    std::chrono::milliseconds cache_time;
    bool served_from_cache = false;
    
    // Result information
    int total_results = 0;
    int returned_results = 0;
    int offset = 0;
    bool has_more_results = false;
    double max_score = 0.0;
    
    // Search quality
    std::vector<std::string> applied_corrections;  // Spell corrections applied
    std::vector<std::string> suggestions;  // "Did you mean?" suggestions
    std::optional<std::string> rewritten_query;  // Query rewrite for better results
    
    // Debug information (only in development)
    std::optional<nlohmann::json> debug_info;
    
    /**
     * Convert to JSON for API response
     */
    nlohmann::json to_json() const;
};

/**
 * Complete search result containing all data
 * 
 * This is the main result class that contains everything needed
 * to display search results to users. I designed it to be
 * comprehensive yet efficient for Twitter-scale search.
 */
class SearchResult {
public:
    SearchResult() = default;
    explicit SearchResult(const SearchQuery& query);
    
    // Result metadata
    SearchMetadata metadata;
    
    // Result collections
    std::vector<NoteResult> notes;
    std::vector<UserResult> users;
    std::vector<HashtagResult> hashtags;
    std::vector<SuggestionResult> suggestions;
    
    // Aggregations and analytics
    std::optional<SearchAggregations> aggregations;
    
    // Mixed results (when search_type is MIXED)
    std::vector<std::pair<ResultType, size_t>> mixed_results;  // type -> index in respective vector
    
    /**
     * Add a note result
     */
    void add_note(const NoteResult& note);
    void add_note(NoteResult&& note);
    
    /**
     * Add a user result
     */
    void add_user(const UserResult& user);
    void add_user(UserResult&& user);
    
    /**
     * Add a hashtag result
     */
    void add_hashtag(const HashtagResult& hashtag);
    void add_hashtag(HashtagResult&& hashtag);
    
    /**
     * Add a suggestion
     */
    void add_suggestion(const SuggestionResult& suggestion);
    void add_suggestion(SuggestionResult&& suggestion);
    
    /**
     * Set aggregations data
     */
    void set_aggregations(const SearchAggregations& aggs);
    void set_aggregations(SearchAggregations&& aggs);
    
    /**
     * Get total number of results across all types
     */
    int get_total_results() const;
    
    /**
     * Check if there are any results
     */
    bool has_results() const;
    
    /**
     * Check if search was successful
     */
    bool is_successful() const;
    
    /**
     * Get results sorted by relevance (mixed mode)
     */
    std::vector<std::pair<ResultType, size_t>> get_sorted_mixed_results() const;
    
    /**
     * Apply result filters note-search
     */
    void apply_content_filter(const std::function<bool(const NoteResult&)>& filter);
    void apply_user_filter(const std::function<bool(const UserResult&)>& filter);
    
    /**
     * Sort results by custom criteria
     */
    void sort_notes_by(const std::function<bool(const NoteResult&, const NoteResult&)>& comparator);
    void sort_users_by(const std::function<bool(const UserResult&, const UserResult&)>& comparator);
    
    /**
     * Get paginated subset of results
     */
    SearchResult get_page(int offset, int limit) const;
    
    /**
     * Merge with another search result
     */
    void merge_with(const SearchResult& other);
    
    /**
     * Convert to JSON for API response
     */
    nlohmann::json to_json() const;
    
    /**
     * Create from Elasticsearch response
     */
    static SearchResult from_elasticsearch_response(
        const nlohmann::json& es_response,
        const SearchQuery& original_query
    );
    
    /**
     * Create error result
     */
    static SearchResult create_error(
        const SearchQuery& query,
        const std::string& error_message,
        const std::string& error_code = "SEARCH_ERROR"
    );
    
    /**
     * Create empty result
     */
    static SearchResult create_empty(const SearchQuery& query);

private:
    /**
     * Generate result ID for tracking
     */
    std::string generate_result_id() const;
    
    /**
     * Update mixed results index
     */
    void update_mixed_results_index();
    
    /**
     * Calculate relevance scores
     */
    void calculate_relevance_scores();
};

/**
 * Result formatting utilities
 */
namespace result_utils {
    /**
     * Format engagement count for display
     */
    std::string format_count(int count);
    
    /**
     * Format timestamp for display
     */
    std::string format_relative_time(const std::chrono::system_clock::time_point& time);
    
    /**
     * Format relevance score for display
     */
    std::string format_relevance_score(double score);
    
    /**
     * Truncate text with ellipsis
     */
    std::string truncate_text(const std::string& text, int max_length);
    
    /**
     * Extract highlighted text
     */
    std::string extract_highlight(
        const std::unordered_map<std::string, std::vector<std::string>>& highlights,
        const std::string& field,
        const std::string& fallback = ""
    );
    
    /**
     * Clean HTML from highlighted text
     */
    std::string clean_highlight_html(const std::string& highlighted_text);
    
    /**
     * Generate snippet from full text
     */
    std::string generate_snippet(
        const std::string& full_text,
        const std::string& query,
        int max_length = 280
    );
    
    /**
     * Calculate text similarity score
     */
    double calculate_similarity(const std::string& text1, const std::string& text2);
    
    /**
     * Detect language of text
     */
    std::string detect_language(const std::string& text);
    
    /**
     * Analyze sentiment of text
     */
    std::string analyze_sentiment(const std::string& text);
}

/**
 * Result caching utilities
 */
namespace result_cache {
    /**
     * Generate cache key for search result
     */
    std::string generate_cache_key(const SearchQuery& query);
    
    /**
     * Serialize result for caching
     */
    std::string serialize_result(const SearchResult& result);
    
    /**
     * Deserialize result from cache
     */
    std::optional<SearchResult> deserialize_result(const std::string& cached_data);
    
    /**
     * Check if result is still valid
     */
    bool is_result_valid(const SearchResult& result, std::chrono::minutes max_age);
}

} // namespace models
} // namespace search_service
} // namespace sonet