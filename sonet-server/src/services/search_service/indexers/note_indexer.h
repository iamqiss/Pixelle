/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Real-time note indexer for Twitter-scale search operations.
 * This handles indexing millions of notes per second with intelligent
 * content analysis, trending detection, and engagement tracking.
 */

#pragma once

#include "../engines/elasticsearch_engine.h"
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <memory>
#include <future>
#include <chrono>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>
#include <condition_variable>
#include <functional>

namespace sonet {
namespace search_service {
namespace indexers {

/**
 * Note document structure for Elasticsearch
 */
struct NoteDocument {
    // Core note data
    std::string id;
    std::string user_id;
    std::string username;
    std::string display_name;
    std::string content;
    std::vector<std::string> hashtags;
    std::vector<std::string> mentions;
    std::vector<std::string> media_urls;
    std::string language;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    
    // Location data
    std::optional<std::pair<double, double>> location;  // lat, lon
    std::string place_name;
    
    // Thread and reply data
    bool is_reply = false;
    std::string reply_to_id;
    bool is_renote = false;
    std::string renote_of_id;
    std::string thread_id;
    
    // Visibility and content flags
    std::string visibility = "public";  // public, unlisted, followers, private
    bool nsfw = false;
    bool sensitive = false;
    
    // Engagement metrics
    struct Metrics {
        int likes_count = 0;
        int renotes_count = 0;
        int replies_count = 0;
        long views_count = 0;
        float engagement_score = 0.0f;
        float virality_score = 0.0f;
        float trending_score = 0.0f;
    } metrics;
    
    // User context for ranking
    struct UserMetrics {
        int followers_count = 0;
        int following_count = 0;
        float reputation_score = 0.0f;
        std::string verification_level = "none";  // none, email, phone, identity, organization
    } user_metrics;
    
    // Boost factors for ranking
    struct BoostFactors {
        float recency_boost = 1.0f;
        float engagement_boost = 1.0f;
        float author_boost = 1.0f;
        float content_quality_boost = 1.0f;
    } boost_factors;
    
    // Indexing metadata
    struct IndexingMetadata {
        std::chrono::system_clock::time_point indexed_at;
        int version = 1;
        std::string source = "realtime";  // realtime, bulk, migration
    } indexing_metadata;
    
    /**
     * Convert to Elasticsearch JSON document
     */
    nlohmann::json to_elasticsearch_document() const;
    
    /**
     * Create from database row or API response
     */
    static NoteDocument from_json(const nlohmann::json& json);
    
    /**
     * Calculate engagement score
     */
    float calculate_engagement_score() const;
    
    /**
     * Calculate virality score
     */
    float calculate_virality_score() const;
    
    /**
     * Calculate trending score
     */
    float calculate_trending_score() const;
    
    /**
     * Extract hashtags from content
     */
    static std::vector<std::string> extract_hashtags(const std::string& content);
    
    /**
     * Extract mentions from content
     */
    static std::vector<std::string> extract_mentions(const std::string& content);
    
    /**
     * Detect language from content
     */
    static std::string detect_language(const std::string& content);
    
    /**
     * Calculate content quality score
     */
    float calculate_content_quality_score() const;
    
    /**
     * Check if content should be indexed
     */
    bool should_be_indexed() const;
    
    /**
     * Generate routing key for sharding
     */
    std::string get_routing_key() const;
};

/**
 * Real-time indexing configuration
 */
struct IndexingConfig {
    // Performance settings
    int batch_size = 1000;
    std::chrono::milliseconds batch_timeout = std::chrono::milliseconds{5000};
    int max_concurrent_batches = 5;
    int max_retry_attempts = 3;
    std::chrono::milliseconds retry_delay = std::chrono::milliseconds{1000};
    
    // Content processing
    bool enable_content_analysis = true;
    bool enable_language_detection = true;
    bool enable_hashtag_extraction = true;
    bool enable_mention_extraction = true;
    bool enable_media_extraction = true;
    bool enable_spam_detection = true;
    
    // Ranking and scoring
    bool enable_engagement_scoring = true;
    bool enable_virality_scoring = true;
    bool enable_trending_scoring = true;
    bool enable_quality_scoring = true;
    
    // Real-time features
    bool enable_real_time_indexing = true;
    bool enable_priority_indexing = true;  // For verified users, trending content
    std::chrono::milliseconds real_time_delay = std::chrono::milliseconds{100};
    
    // Filtering
    bool index_private_notes = false;
    bool index_nsfw_content = true;
    bool index_spam_content = false;
    std::vector<std::string> blocked_users;
    std::vector<std::string> blocked_hashtags;
    
    // Memory management
    int max_queue_size = 100000;
    int memory_warning_threshold_mb = 500;
    int memory_limit_threshold_mb = 1000;
    
    // Monitoring
    bool enable_metrics_collection = true;
    std::chrono::minutes metrics_reporting_interval = std::chrono::minutes{5};
    
    /**
     * Get default production configuration
     */
    static IndexingConfig production_config();
    
    /**
     * Get development configuration
     */
    static IndexingConfig development_config();
    
    /**
     * Validate configuration
     */
    bool is_valid() const;
};

/**
 * Indexing metrics and statistics
 */
struct IndexingMetrics {
    std::atomic<long> notes_processed{0};
    std::atomic<long> notes_indexed{0};
    std::atomic<long> notes_updated{0};
    std::atomic<long> notes_deleted{0};
    std::atomic<long> notes_skipped{0};
    std::atomic<long> notes_failed{0};
    
    std::atomic<long> batches_processed{0};
    std::atomic<long> batches_failed{0};
    std::atomic<long> retries_attempted{0};
    
    std::atomic<long> total_processing_time_ms{0};
    std::atomic<long> total_indexing_time_ms{0};
    std::atomic<long> total_queue_time_ms{0};
    
    std::atomic<long> content_analysis_time_ms{0};
    std::atomic<long> language_detection_time_ms{0};
    std::atomic<long> scoring_time_ms{0};
    
    std::atomic<int> current_queue_size{0};
    std::atomic<int> current_memory_usage_mb{0};
    std::atomic<int> active_worker_threads{0};
    
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
     * Calculate processing rate (notes per second)
     */
    double get_processing_rate() const;
    
    /**
     * Calculate success rate
     */
    double get_success_rate() const;
    
    /**
     * Calculate average processing time
     */
    double get_average_processing_time_ms() const;
    
    /**
     * Check if memory usage is critical
     */
    bool is_memory_critical(const IndexingConfig& config) const;
};

/**
 * Indexing operation types
 */
enum class IndexingOperation {
    CREATE,
    UPDATE,
    DELETE,
    UPDATE_METRICS  // For updating engagement metrics only
};

/**
 * Indexing task for the queue
 */
struct IndexingTask {
    IndexingOperation operation;
    NoteDocument note;
    std::chrono::system_clock::time_point queued_at;
    std::chrono::system_clock::time_point scheduled_at;
    int priority = 0;  // Higher number = higher priority
    int retry_count = 0;
    std::string correlation_id;
    
    /**
     * Check if task should be retried
     */
    bool should_retry(const IndexingConfig& config) const;
    
    /**
     * Calculate task priority based on content and user
     */
    static int calculate_priority(const NoteDocument& note);
    
    /**
     * Get next retry delay
     */
    std::chrono::milliseconds get_retry_delay(const IndexingConfig& config) const;
};

/**
 * Priority queue comparator for indexing tasks
 */
struct TaskPriorityComparator {
    bool operator()(const IndexingTask& a, const IndexingTask& b) const {
        // Higher priority first, then earlier scheduled time
        if (a.priority != b.priority) {
            return a.priority < b.priority;
        }
        return a.scheduled_at > b.scheduled_at;
    }
};

/**
 * Content analyzer for notes
 */
class ContentAnalyzer {
public:
    /**
     * Analyze note content for indexing
     */
    struct AnalysisResult {
        std::vector<std::string> hashtags;
        std::vector<std::string> mentions;
        std::vector<std::string> media_urls;
        std::string language;
        float content_quality_score;
        float spam_score;
        bool is_nsfw;
        bool is_sensitive;
        std::vector<std::string> topics;
        std::string sentiment;  // positive, negative, neutral
    };
    
    /**
     * Perform comprehensive content analysis
     */
    static AnalysisResult analyze_content(const std::string& content);
    
    /**
     * Extract hashtags with position information
     */
    static std::vector<std::pair<std::string, size_t>> extract_hashtags_with_positions(const std::string& content);
    
    /**
     * Extract mentions with position information
     */
    static std::vector<std::pair<std::string, size_t>> extract_mentions_with_positions(const std::string& content);
    
    /**
     * Extract URLs from content
     */
    static std::vector<std::string> extract_urls(const std::string& content);
    
    /**
     * Detect and extract media URLs
     */
    static std::vector<std::string> extract_media_urls(const std::string& content);
    
    /**
     * Perform language detection
     */
    static std::string detect_language_advanced(const std::string& content);
    
    /**
     * Calculate content quality score (0-1)
     */
    static float calculate_content_quality(const std::string& content);
    
    /**
     * Calculate spam probability (0-1)
     */
    static float calculate_spam_score(const std::string& content, const std::string& user_id);
    
    /**
     * Detect NSFW content
     */
    static bool is_nsfw_content(const std::string& content);
    
    /**
     * Detect sensitive content
     */
    static bool is_sensitive_content(const std::string& content);
    
    /**
     * Extract topic tags from content
     */
    static std::vector<std::string> extract_topics(const std::string& content);
    
    /**
     * Perform sentiment analysis
     */
    static std::string analyze_sentiment(const std::string& content);
    
    /**
     * Clean and normalize content for indexing
     */
    static std::string normalize_content(const std::string& content);

private:
    // Content analysis patterns and classifiers
    static const std::vector<std::string> SPAM_PATTERNS;
    static const std::vector<std::string> NSFW_PATTERNS;
    static const std::vector<std::string> SENSITIVE_PATTERNS;
    static const std::unordered_map<std::string, std::vector<std::string>> TOPIC_KEYWORDS;
};

/**
 * Main note indexer class
 * 
 * This handles real-time indexing of notes with intelligent content analysis,
 * trending detection, and high-performance batch processing for Twitter-scale loads.
 */
class NoteIndexer {
public:
    /**
     * Constructor
     */
    explicit NoteIndexer(
        std::shared_ptr<engines::ElasticsearchEngine> engine,
        const IndexingConfig& config = IndexingConfig::production_config()
    );
    
    /**
     * Destructor - ensures clean shutdown
     */
    ~NoteIndexer();
    
    // Non-copyable and non-movable
    NoteIndexer(const NoteIndexer&) = delete;
    NoteIndexer& operator=(const NoteIndexer&) = delete;
    NoteIndexer(NoteIndexer&&) = delete;
    NoteIndexer& operator=(NoteIndexer&&) = delete;
    
    /**
     * Start the indexer
     */
    std::future<bool> start();
    
    /**
     * Stop the indexer gracefully
     */
    void stop();
    
    /**
     * Check if indexer is running
     */
    bool is_running() const;
    
    /**
     * REAL-TIME INDEXING OPERATIONS
     */
    
    /**
     * Queue note for indexing (asynchronous)
     */
    bool queue_note_for_indexing(const NoteDocument& note, int priority = 0);
    
    /**
     * Index note immediately (synchronous)
     */
    std::future<bool> index_note_immediately(const NoteDocument& note);
    
    /**
     * Update note in index
     */
    std::future<bool> update_note(const NoteDocument& note);
    
    /**
     * Update note engagement metrics only
     */
    std::future<bool> update_note_metrics(
        const std::string& note_id,
        int likes_count,
        int renotes_count,
        int replies_count,
        long views_count
    );
    
    /**
     * Delete note from index
     */
    std::future<bool> delete_note(const std::string& note_id);
    
    /**
     * BATCH OPERATIONS
     */
    
    /**
     * Index multiple notes in batch
     */
    std::future<bool> index_notes_batch(const std::vector<NoteDocument>& notes);
    
    /**
     * Reindex all notes for a user
     */
    std::future<bool> reindex_user_notes(const std::string& user_id);
    
    /**
     * Bulk delete notes by user
     */
    std::future<bool> delete_user_notes(const std::string& user_id);
    
    /**
     * TRENDING AND ANALYTICS
     */
    
    /**
     * Update trending scores for recent notes
     */
    std::future<bool> update_trending_scores();
    
    /**
     * Reindex notes with updated engagement data
     */
    std::future<bool> refresh_engagement_scores(std::chrono::hours lookback = std::chrono::hours{24});
    
    /**
     * Index historical notes (for migration)
     */
    std::future<bool> index_historical_notes(
        const std::vector<NoteDocument>& notes,
        const std::function<void(float)>& progress_callback = nullptr
    );
    
    /**
     * QUEUE MANAGEMENT
     */
    
    /**
     * Get current queue size
     */
    int get_queue_size() const;
    
    /**
     * Clear the indexing queue
     */
    void clear_queue();
    
    /**
     * Process pending queue immediately
     */
    std::future<bool> flush_queue();
    
    /**
     * Pause/resume queue processing
     */
    void pause_processing();
    void resume_processing();
    bool is_paused() const;
    
    /**
     * MONITORING AND DEBUGGING
     */
    
    /**
     * Get indexing metrics
     */
    IndexingMetrics get_metrics() const;
    
    /**
     * Get detailed indexer status
     */
    nlohmann::json get_status() const;
    
    /**
     * Enable/disable debug mode
     */
    void set_debug_mode(bool enabled);
    
    /**
     * Get recent failed operations
     */
    std::vector<nlohmann::json> get_failed_operations(int limit = 10) const;
    
    /**
     * Health check
     */
    bool health_check() const;
    
    /**
     * CONFIGURATION
     */
    
    /**
     * Update configuration (will restart workers)
     */
    void update_config(const IndexingConfig& new_config);
    
    /**
     * Get current configuration
     */
    IndexingConfig get_config() const;

private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    /**
     * Worker thread functions
     */
    void indexing_worker_loop();
    void metrics_reporter_loop();
    void memory_monitor_loop();
    
    /**
     * Process a batch of indexing tasks
     */
    bool process_task_batch(const std::vector<IndexingTask>& tasks);
    
    /**
     * Process single indexing task
     */
    bool process_task(const IndexingTask& task);
    
    /**
     * Handle failed task (retry or discard)
     */
    void handle_failed_task(const IndexingTask& task, const std::string& error);
    
    /**
     * Update metrics
     */
    void update_metrics(
        IndexingOperation operation,
        bool success,
        std::chrono::milliseconds processing_time,
        std::chrono::milliseconds indexing_time
    );
    
    /**
     * Check memory usage and take action if needed
     */
    void check_memory_usage();
};

/**
 * Factory for creating note indexers
 */
class NoteIndexerFactory {
public:
    /**
     * Create production indexer
     */
    static std::unique_ptr<NoteIndexer> create_production(
        std::shared_ptr<engines::ElasticsearchEngine> engine
    );
    
    /**
     * Create development indexer
     */
    static std::unique_ptr<NoteIndexer> create_development(
        std::shared_ptr<engines::ElasticsearchEngine> engine
    );
    
    /**
     * Create testing indexer (with mocked engine)
     */
    static std::unique_ptr<NoteIndexer> create_testing();
    
    /**
     * Create indexer from configuration
     */
    static std::unique_ptr<NoteIndexer> create_from_config(
        std::shared_ptr<engines::ElasticsearchEngine> engine,
        const IndexingConfig& config
    );
};

/**
 * Indexing utilities
 */
namespace indexing_utils {
    /**
     * Generate note document ID
     */
    std::string generate_note_id(const std::string& user_id, const std::string& content_hash);
    
    /**
     * Calculate content hash for deduplication
     */
    std::string calculate_content_hash(const std::string& content);
    
    /**
     * Validate note document
     */
    bool validate_note_document(const NoteDocument& note, std::string& error_message);
    
    /**
     * Estimate document size in bytes
     */
    size_t estimate_document_size(const NoteDocument& note);
    
    /**
     * Check if note is eligible for indexing
     */
    bool is_indexable(const NoteDocument& note, const IndexingConfig& config);
    
    /**
     * Generate index name with rotation
     */
    std::string generate_index_name(
        const std::string& base_name,
        const std::chrono::system_clock::time_point& timestamp
    );
    
    /**
     * Calculate optimal batch size based on memory and network
     */
    int calculate_optimal_batch_size(
        int avg_document_size_bytes,
        int available_memory_mb,
        int network_bandwidth_mbps
    );
}

} // namespace indexers
} // namespace search_service
} // namespace sonet