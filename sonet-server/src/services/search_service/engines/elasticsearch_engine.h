/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This defines the Elasticsearch engine for our Twitter-scale search service.
 * I designed this to handle billions of documents with real-time indexing,
 * intelligent ranking, and sub-second response times that scale globally.
 */

#pragma once

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

namespace sonet {
namespace search_service {
namespace engines {

/**
 * Elasticsearch connection configuration
 */
struct ElasticsearchConfig {
    // Connection settings
    std::vector<std::string> hosts = {"http://localhost:9200"};
    std::string username;
    std::string password;
    std::string api_key;
    bool use_ssl = false;
    bool verify_ssl = true;
    std::chrono::seconds connection_timeout = std::chrono::seconds{30};
    std::chrono::seconds request_timeout = std::chrono::seconds{60};
    
    // Connection pooling
    int max_connections = 100;
    int max_connections_per_host = 20;
    int connection_pool_timeout_ms = 5000;
    
    // Index settings
    std::string notes_index = "sonet_notes";
    std::string users_index = "sonet_users";
    std::string hashtags_index = "sonet_hashtags";
    std::string suggestions_index = "sonet_suggestions";
    
    // Index templates and patterns
    std::string notes_index_pattern = "sonet_notes_*";
    std::string users_index_pattern = "sonet_users_*";
    std::string time_based_index_format = "yyyy.MM.dd";  // For time-based indices
    
    // Performance settings
    int bulk_index_size = 1000;
    std::chrono::milliseconds bulk_flush_interval = std::chrono::milliseconds{5000};
    int max_retry_attempts = 3;
    std::chrono::milliseconds retry_delay = std::chrono::milliseconds{1000};
    
    // Search settings
    int default_search_timeout_ms = 5000;
    int max_result_window = 10000;
    bool enable_source_filtering = true;
    bool enable_highlighting = true;
    
    // Caching
    bool enable_request_cache = true;
    std::chrono::minutes cache_ttl = std::chrono::minutes{5};
    
    // Monitoring
    bool enable_slow_query_logging = true;
    std::chrono::milliseconds slow_query_threshold = std::chrono::milliseconds{1000};
    bool enable_metrics_collection = true;
    
    /**
     * Validate configuration
     */
    bool is_valid() const;
    
    /**
     * Get default production configuration
     */
    static ElasticsearchConfig production_config();
    
    /**
     * Get development configuration
     */
    static ElasticsearchConfig development_config();
    
    /**
     * Load from environment variables
     */
    static ElasticsearchConfig from_environment();
};

/**
 * Index mapping configuration for different document types
 */
struct IndexMappings {
    /**
     * Get note index mapping
     */
    static nlohmann::json get_notes_mapping();
    
    /**
     * Get user index mapping
     */
    static nlohmann::json get_users_mapping();
    
    /**
     * Get hashtag index mapping
     */
    static nlohmann::json get_hashtags_mapping();
    
    /**
     * Get suggestions index mapping
     */
    static nlohmann::json get_suggestions_mapping();
    
    /**
     * Get index settings with analyzers
     */
    static nlohmann::json get_index_settings();
    
    /**
     * Get search template for notes
     */
    static nlohmann::json get_notes_search_template();
    
    /**
     * Get search template for users
     */
    static nlohmann::json get_users_search_template();
};

/**
 * Search performance metrics
 */
struct SearchMetrics {
    std::atomic<long> total_searches{0};
    std::atomic<long> successful_searches{0};
    std::atomic<long> failed_searches{0};
    std::atomic<long> cached_searches{0};
    std::atomic<long> slow_searches{0};
    
    std::atomic<long> total_query_time_ms{0};
    std::atomic<long> total_elasticsearch_time_ms{0};
    std::atomic<long> total_cache_time_ms{0};
    
    std::atomic<long> total_documents_searched{0};
    std::atomic<long> total_results_returned{0};
    
    std::chrono::system_clock::time_point last_reset;
    
    /**
     * Get metrics as JSON
     */
    nlohmann::json to_json() const;
    
    /**
     * Reset metrics
     */
    void reset();
    
    /**
     * Calculate average query time
     */
    double get_average_query_time_ms() const;
    
    /**
     * Calculate success rate
     */
    double get_success_rate() const;
    
    /**
     * Calculate cache hit rate
     */
    double get_cache_hit_rate() const;
};

/**
 * Bulk indexing operation
 */
struct BulkOperation {
    enum class Type {
        INDEX,
        UPDATE,
        DELETE
    };
    
    Type operation_type;
    std::string index_name;
    std::string document_id;
    nlohmann::json document;
    
    /**
     * Convert to Elasticsearch bulk format
     */
    std::string to_bulk_format() const;
};

/**
 * Main Elasticsearch engine class
 * 
 * This is the core search engine that handles all interactions with
 * Elasticsearch. I designed it to be high-performance, fault-tolerant,
 * and capable of handling Twitter-scale search loads.
 */
class ElasticsearchEngine {
public:
    /**
     * Constructor
     */
    explicit ElasticsearchEngine(const ElasticsearchConfig& config);
    
    /**
     * Destructor - ensures clean shutdown
     */
    ~ElasticsearchEngine();
    
    // Non-copyable and non-movable
    ElasticsearchEngine(const ElasticsearchEngine&) = delete;
    ElasticsearchEngine& operator=(const ElasticsearchEngine&) = delete;
    ElasticsearchEngine(ElasticsearchEngine&&) = delete;
    ElasticsearchEngine& operator=(ElasticsearchEngine&&) = delete;
    
    /**
     * Initialize the engine and create indices
     */
    std::future<bool> initialize();
    
    /**
     * Shutdown the engine gracefully
     */
    void shutdown();
    
    /**
     * Check if engine is ready for operations
     */
    bool is_ready() const;
    
    /**
     * Get cluster health status
     */
    std::future<nlohmann::json> get_cluster_health();
    
    /**
     * SEARCH OPERATIONS
     */
    
    /**
     * Execute search query
     */
    std::future<models::SearchResult> search(const models::SearchQuery& query);
    
    /**
     * Execute multi-search (for different result types)
     */
    std::future<models::SearchResult> multi_search(const models::SearchQuery& query);
    
    /**
     * Get search suggestions/autocomplete
     */
    std::future<std::vector<std::string>> get_suggestions(
        const std::string& partial_text,
        int max_suggestions = 10
    );
    
    /**
     * Get trending hashtags
     */
    std::future<std::vector<models::HashtagResult>> get_trending_hashtags(
        std::chrono::hours time_window = std::chrono::hours{24},
        int limit = 20
    );
    
    /**
     * Execute scroll search for large result sets
     */
    std::future<models::SearchResult> scroll_search(
        const models::SearchQuery& query,
        const std::string& scroll_id = "",
        std::chrono::minutes keep_alive = std::chrono::minutes{5}
    );
    
    /**
     * Count documents matching query
     */
    std::future<long> count_documents(const models::SearchQuery& query);
    
    /**
     * INDEXING OPERATIONS
     */
    
    /**
     * Index a single note
     */
    std::future<bool> index_note(
        const std::string& note_id,
        const nlohmann::json& note_document
    );
    
    /**
     * Index a single user
     */
    std::future<bool> index_user(
        const std::string& user_id,
        const nlohmann::json& user_document
    );
    
    /**
     * Update note metrics (likes, renotes, etc.)
     */
    std::future<bool> update_note_metrics(
        const std::string& note_id,
        const nlohmann::json& metrics_update
    );
    
    /**
     * Delete note from index
     */
    std::future<bool> delete_note(const std::string& note_id);
    
    /**
     * Delete user from index
     */
    std::future<bool> delete_user(const std::string& user_id);
    
    /**
     * BULK OPERATIONS
     */
    
    /**
     * Execute bulk operations
     */
    std::future<nlohmann::json> bulk_execute(const std::vector<BulkOperation>& operations);
    
    /**
     * Add operation to bulk queue (asynchronous)
     */
    void queue_bulk_operation(const BulkOperation& operation);
    
    /**
     * Flush pending bulk operations
     */
    std::future<bool> flush_bulk_queue();
    
    /**
     * REAL-TIME OPERATIONS
     */
    
    /**
     * Refresh indices to make recent changes searchable
     */
    std::future<bool> refresh_indices();
    
    /**
     * Force merge indices for performance
     */
    std::future<bool> force_merge_indices(int max_num_segments = 1);
    
    /**
     * ANALYTICS AND AGGREGATIONS
     */
    
    /**
     * Get search analytics
     */
    std::future<nlohmann::json> get_search_analytics(
        const std::chrono::system_clock::time_point& from,
        const std::chrono::system_clock::time_point& to
    );
    
    /**
     * Get hashtag usage statistics
     */
    std::future<nlohmann::json> get_hashtag_stats(
        const std::vector<std::string>& hashtags,
        std::chrono::hours time_window = std::chrono::hours{24}
    );
    
    /**
     * Get user activity aggregations
     */
    std::future<nlohmann::json> get_user_activity_stats(
        const std::string& user_id,
        std::chrono::hours time_window = std::chrono::hours{168}  // 1 week
    );
    
    /**
     * INDEX MANAGEMENT
     */
    
    /**
     * Create indices with proper mappings
     */
    std::future<bool> create_indices();
    
    /**
     * Update index mappings
     */
    std::future<bool> update_mappings();
    
    /**
     * Create index templates
     */
    std::future<bool> create_templates();
    
    /**
     * Rotate time-based indices
     */
    std::future<bool> rotate_time_based_indices();
    
    /**
     * Get index statistics
     */
    std::future<nlohmann::json> get_index_stats();
    
    /**
     * MONITORING AND DEBUGGING
     */
    
    /**
     * Get engine performance metrics
     */
    SearchMetrics get_metrics() const;
    
    /**
     * Get detailed engine status
     */
    nlohmann::json get_engine_status() const;
    
    /**
     * Enable/disable debug mode
     */
    void set_debug_mode(bool enabled);
    
    /**
     * Get recent slow queries
     */
    std::vector<nlohmann::json> get_slow_queries(int limit = 10) const;
    
    /**
     * Test connection to Elasticsearch
     */
    std::future<bool> test_connection();
    
    /**
     * CACHING
     */
    
    /**
     * Clear search cache
     */
    std::future<bool> clear_cache();
    
    /**
     * Get cache statistics
     */
    nlohmann::json get_cache_stats() const;
    
    /**
     * UTILITIES
     */
    
    /**
     * Validate query syntax
     */
    bool validate_query(const models::SearchQuery& query) const;
    
    /**
     * Optimize query for better performance
     */
    models::SearchQuery optimize_query(const models::SearchQuery& query) const;
    
    /**
     * Get query explanation (for debugging)
     */
    std::future<nlohmann::json> explain_query(
        const models::SearchQuery& query,
        const std::string& document_id
    );

private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    /**
     * Internal HTTP client operations
     */
    std::future<nlohmann::json> execute_request(
        const std::string& method,
        const std::string& path,
        const nlohmann::json& body = nlohmann::json{},
        const std::unordered_map<std::string, std::string>& params = {}
    );
    
    /**
     * Build Elasticsearch URL
     */
    std::string build_url(
        const std::string& path,
        const std::unordered_map<std::string, std::string>& params = {}
    ) const;
    
    /**
     * Handle Elasticsearch errors
     */
    void handle_error(const nlohmann::json& response) const;
    
    /**
     * Update metrics
     */
    void update_metrics(
        bool success,
        std::chrono::milliseconds query_time,
        std::chrono::milliseconds es_time,
        bool from_cache,
        long documents_searched,
        long results_returned
    );
    
    /**
     * Log slow query
     */
    void log_slow_query(
        const models::SearchQuery& query,
        std::chrono::milliseconds execution_time
    );
};

/**
 * Factory for creating Elasticsearch engines
 */
class ElasticsearchEngineFactory {
public:
    /**
     * Create production engine
     */
    static std::unique_ptr<ElasticsearchEngine> create_production(
        const ElasticsearchConfig& config
    );
    
    /**
     * Create development engine
     */
    static std::unique_ptr<ElasticsearchEngine> create_development(
        const ElasticsearchConfig& config
    );
    
    /**
     * Create testing engine (with mocked responses)
     */
    static std::unique_ptr<ElasticsearchEngine> create_testing();
    
    /**
     * Create engine from environment
     */
    static std::unique_ptr<ElasticsearchEngine> create_from_environment();
};

/**
 * Elasticsearch utilities
 */
namespace elasticsearch_utils {
    /**
     * Generate time-based index name
     */
    std::string generate_time_based_index_name(
        const std::string& base_name,
        const std::chrono::system_clock::time_point& time,
        const std::string& format = "yyyy.MM.dd"
    );
    
    /**
     * Parse Elasticsearch version
     */
    std::tuple<int, int, int> parse_version(const std::string& version_string);
    
    /**
     * Check if feature is supported in version
     */
    bool is_feature_supported(
        const std::tuple<int, int, int>& version,
        const std::string& feature
    );
    
    /**
     * Escape special characters in query
     */
    std::string escape_query_string(const std::string& query);
    
    /**
     * Build date range filter
     */
    nlohmann::json build_date_range_filter(
        const std::string& field,
        const std::optional<std::chrono::system_clock::time_point>& from,
        const std::optional<std::chrono::system_clock::time_point>& to
    );
    
    /**
     * Build geo distance filter
     */
    nlohmann::json build_geo_distance_filter(
        const std::string& field,
        double latitude,
        double longitude,
        double distance_km
    );
    
    /**
     * Convert search type to index names
     */
    std::vector<std::string> get_target_indices(
        models::SearchType search_type,
        const ElasticsearchConfig& config
    );
    
    /**
     * Generate document routing key
     */
    std::string generate_routing_key(const std::string& user_id);
    
    /**
     * Calculate optimal shard count
     */
    int calculate_optimal_shard_count(long estimated_documents, long avg_document_size_bytes);
}

} // namespace engines
} // namespace search_service
} // namespace sonet