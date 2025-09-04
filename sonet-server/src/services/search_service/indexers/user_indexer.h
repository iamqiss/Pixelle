/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Real-time user indexer for Twitter-scale search operations.
 * This handles indexing millions of user profiles with intelligent
 * profile analysis, reputation scoring, and relationship tracking.
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
#include <optional>

namespace sonet {
namespace search_service {
namespace indexers {

/**
 * User document structure for Elasticsearch
 */
struct UserDocument {
    // Core user data
    std::string id;
    std::string username;
    std::string display_name;
    std::string bio;
    std::string email;  // Not indexed for privacy
    std::string phone;  // Not indexed for privacy
    
    // Profile information
    std::string avatar_url;
    std::string banner_url;
    std::string website;
    std::optional<std::pair<double, double>> location;  // lat, lon
    std::string location_name;
    std::string timezone;
    std::vector<std::string> languages;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
    std::chrono::system_clock::time_point last_active_at;
    
    // Verification and trust
    struct Verification {
        bool is_verified = false;
        std::string verification_type = "none";  // none, email, phone, identity, organization, government
        std::chrono::system_clock::time_point verified_at;
        std::string verification_badge = "none";  // none, blue, gold, gray
    } verification;
    
    // Social metrics
    struct Metrics {
        int followers_count = 0;
        int following_count = 0;
        int notes_count = 0;
        long likes_given = 0;
        long likes_received = 0;
        long renotes_given = 0;
        long renotes_received = 0;
        float reputation_score = 0.0f;
        float influence_score = 0.0f;
        float engagement_rate = 0.0f;
        float authenticity_score = 1.0f;  // Anti-bot score
    } metrics;
    
    // Interests and topics
    std::vector<std::string> interests;
    std::vector<std::string> favorite_hashtags;
    std::vector<std::string> frequently_mentioned_users;
    
    // Privacy and security
    struct Privacy {
        bool is_private = false;
        bool searchable = true;
        bool indexable = true;
        bool show_activity = true;
        bool show_followers = true;
        bool show_following = true;
        std::vector<std::string> blocked_users;
        std::vector<std::string> muted_users;
    } privacy;
    
    // Account status
    bool is_active = true;
    bool is_suspended = false;
    bool is_deleted = false;
    bool is_bot = false;
    std::string account_type = "personal";  // personal, business, organization, bot
    
    // Activity patterns
    struct ActivityScore {
        float daily_activity = 0.0f;
        float weekly_activity = 0.0f;
        float monthly_activity = 0.0f;
        std::chrono::system_clock::time_point last_calculated;
    } activity_score;
    
    // Content quality metrics
    float content_quality_score = 0.0f;
    float spam_score = 0.0f;
    int reported_count = 0;
    int warning_count = 0;
    
    // Indexing metadata
    struct IndexingMetadata {
        std::chrono::system_clock::time_point indexed_at;
        int version = 1;
        std::string source = "realtime";  // realtime, bulk, migration
        std::string index_name;
    } indexing_metadata;
    
    /**
     * Convert to Elasticsearch JSON document
     */
    nlohmann::json to_elasticsearch_document() const;
    
    /**
     * Create from database row or API response
     */
    static UserDocument from_json(const nlohmann::json& json);
    
    /**
     * Calculate reputation score based on various factors
     */
    float calculate_reputation_score() const;
    
    /**
     * Calculate influence score (followers, engagement, etc.)
     */
    float calculate_influence_score() const;
    
    /**
     * Calculate authenticity score (anti-bot detection)
     */
    float calculate_authenticity_score() const;
    
    /**
     * Calculate content quality score
     */
    float calculate_content_quality_score() const;
    
    /**
     * Calculate engagement rate
     */
    float calculate_engagement_rate() const;
    
    /**
     * Extract interests from bio and activity
     */
    static std::vector<std::string> extract_interests(const std::string& bio, const std::vector<std::string>& hashtags);
    
    /**
     * Normalize username for search
     */
    static std::string normalize_username(const std::string& username);
    
    /**
     * Validate user document
     */
    bool is_valid() const;
    
    /**
     * Check if user should be indexed
     */
    bool should_be_indexed() const;
    
    /**
     * Generate routing key for sharding
     */
    std::string get_routing_key() const;
    
    /**
     * Generate search suggestions for this user
     */
    std::vector<std::string> generate_search_suggestions() const;
};

/**
 * User indexing configuration
 */
struct UserIndexingConfig {
    // Performance settings
    int batch_size = 500;
    std::chrono::milliseconds batch_timeout = std::chrono::milliseconds{10000};
    int max_concurrent_batches = 3;
    int max_retry_attempts = 3;
    std::chrono::milliseconds retry_delay = std::chrono::milliseconds{2000};
    
    // Profile processing
    bool enable_bio_analysis = true;
    bool enable_interest_extraction = true;
    bool enable_reputation_scoring = true;
    bool enable_influence_scoring = true;
    bool enable_authenticity_scoring = true;
    bool enable_activity_tracking = true;
    
    // Privacy and filtering
    bool index_private_users = false;
    bool index_suspended_users = false;
    bool index_deleted_users = false;
    bool index_bot_accounts = true;
    bool respect_searchable_flag = true;
    bool respect_indexable_flag = true;
    
    // Data freshness
    std::chrono::hours metrics_refresh_interval = std::chrono::hours{6};
    std::chrono::hours activity_score_refresh_interval = std::chrono::hours{24};
    std::chrono::hours full_reindex_interval = std::chrono::hours{168};  // 1 week
    
    // Suggestions and autocomplete
    bool enable_suggestion_generation = true;
    int max_suggestions_per_user = 10;
    bool include_bio_in_suggestions = true;
    bool include_interests_in_suggestions = true;
    
    // Memory management
    int max_queue_size = 50000;
    int memory_warning_threshold_mb = 300;
    int memory_limit_threshold_mb = 500;
    
    // Monitoring
    bool enable_metrics_collection = true;
    std::chrono::minutes metrics_reporting_interval = std::chrono::minutes{10};
    
    /**
     * Get default production configuration
     */
    static UserIndexingConfig production_config();
    
    /**
     * Get development configuration
     */
    static UserIndexingConfig development_config();
    
    /**
     * Validate configuration
     */
    bool is_valid() const;
};

/**
 * User indexing metrics
 */
struct UserIndexingMetrics {
    std::atomic<long> users_processed{0};
    std::atomic<long> users_indexed{0};
    std::atomic<long> users_updated{0};
    std::atomic<long> users_deleted{0};
    std::atomic<long> users_skipped{0};
    std::atomic<long> users_failed{0};
    
    std::atomic<long> suggestions_generated{0};
    std::atomic<long> reputation_calculations{0};
    std::atomic<long> influence_calculations{0};
    std::atomic<long> authenticity_checks{0};
    
    std::atomic<long> total_processing_time_ms{0};
    std::atomic<long> total_indexing_time_ms{0};
    std::atomic<long> bio_analysis_time_ms{0};
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
     * Calculate processing rate (users per second)
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
};

/**
 * User indexing operation types
 */
enum class UserIndexingOperation {
    CREATE,
    UPDATE,
    DELETE,
    UPDATE_METRICS,     // Update social metrics only
    UPDATE_ACTIVITY,    // Update activity scores only
    FULL_REFRESH        // Complete reindex of user
};

/**
 * User indexing task
 */
struct UserIndexingTask {
    UserIndexingOperation operation;
    UserDocument user;
    std::chrono::system_clock::time_point queued_at;
    std::chrono::system_clock::time_point scheduled_at;
    int priority = 0;  // Higher number = higher priority
    int retry_count = 0;
    std::string correlation_id;
    std::unordered_map<std::string, std::string> metadata;
    
    /**
     * Check if task should be retried
     */
    bool should_retry(const UserIndexingConfig& config) const;
    
    /**
     * Calculate task priority
     */
    static int calculate_priority(const UserDocument& user, UserIndexingOperation operation);
    
    /**
     * Get next retry delay
     */
    std::chrono::milliseconds get_retry_delay(const UserIndexingConfig& config) const;
};

/**
 * Profile analyzer for advanced user analysis
 */
class ProfileAnalyzer {
public:
    /**
     * Profile analysis result
     */
    struct AnalysisResult {
        std::vector<std::string> interests;
        std::vector<std::string> topics;
        std::string profession;
        std::string industry;
        std::vector<std::string> skills;
        float personality_openness = 0.0f;
        float personality_conscientiousness = 0.0f;
        float personality_extraversion = 0.0f;
        float personality_agreeableness = 0.0f;
        float personality_neuroticism = 0.0f;
        std::string sentiment = "neutral";
        bool likely_bot = false;
        float bot_probability = 0.0f;
        std::string account_purpose = "personal";  // personal, business, entertainment, news, etc.
    };
    
    /**
     * Perform comprehensive profile analysis
     */
    static AnalysisResult analyze_profile(const UserDocument& user);
    
    /**
     * Extract interests from bio text
     */
    static std::vector<std::string> extract_interests_from_bio(const std::string& bio);
    
    /**
     * Detect profession/occupation from bio
     */
    static std::string detect_profession(const std::string& bio);
    
    /**
     * Detect industry from bio and interests
     */
    static std::string detect_industry(const std::string& bio, const std::vector<std::string>& interests);
    
    /**
     * Perform personality analysis from bio
     */
    static std::unordered_map<std::string, float> analyze_personality(const std::string& bio);
    
    /**
     * Detect if account is likely a bot
     */
    static std::pair<bool, float> detect_bot_account(const UserDocument& user);
    
    /**
     * Classify account purpose
     */
    static std::string classify_account_purpose(const UserDocument& user);
    
    /**
     * Calculate content authenticity score
     */
    static float calculate_authenticity_score(const UserDocument& user);
    
    /**
     * Analyze profile completeness
     */
    static float calculate_profile_completeness(const UserDocument& user);

private:
    // Analysis patterns and classifiers
    static const std::vector<std::string> BOT_INDICATORS;
    static const std::unordered_map<std::string, std::vector<std::string>> PROFESSION_KEYWORDS;
    static const std::unordered_map<std::string, std::vector<std::string>> INDUSTRY_KEYWORDS;
    static const std::unordered_map<std::string, std::vector<std::string>> INTEREST_KEYWORDS;
};

/**
 * Reputation calculator for user scoring
 */
class ReputationCalculator {
public:
    /**
     * Calculate comprehensive reputation score (0-100)
     */
    static float calculate_reputation(const UserDocument& user);
    
    /**
     * Calculate influence score based on network effects
     */
    static float calculate_influence(const UserDocument& user);
    
    /**
     * Calculate engagement quality score
     */
    static float calculate_engagement_quality(const UserDocument& user);
    
    /**
     * Calculate content quality score
     */
    static float calculate_content_quality(const UserDocument& user);
    
    /**
     * Calculate trust score
     */
    static float calculate_trust_score(const UserDocument& user);
    
    /**
     * Calculate activity score
     */
    static float calculate_activity_score(const UserDocument& user);
    
    /**
     * Calculate follower quality score
     */
    static float calculate_follower_quality(const UserDocument& user);
    
    /**
     * Detect and penalize spam behavior
     */
    static float calculate_spam_penalty(const UserDocument& user);

private:
    // Scoring weights and thresholds
    struct ScoringWeights {
        float verification_weight = 0.2f;
        float follower_quality_weight = 0.25f;
        float content_quality_weight = 0.2f;
        float engagement_weight = 0.15f;
        float activity_weight = 0.1f;
        float trust_weight = 0.1f;
    };
    
    static const ScoringWeights DEFAULT_WEIGHTS;
};

/**
 * Main user indexer class
 * 
 * This handles real-time indexing of user profiles with intelligent analysis,
 * reputation scoring, and high-performance batch processing for Twitter-scale loads.
 */
class UserIndexer {
public:
    /**
     * Constructor
     */
    explicit UserIndexer(
        std::shared_ptr<engines::ElasticsearchEngine> engine,
        const UserIndexingConfig& config = UserIndexingConfig::production_config()
    );
    
    /**
     * Destructor - ensures clean shutdown
     */
    ~UserIndexer();
    
    // Non-copyable and non-movable
    UserIndexer(const UserIndexer&) = delete;
    UserIndexer& operator=(const UserIndexer&) = delete;
    UserIndexer(UserIndexer&&) = delete;
    UserIndexer& operator=(UserIndexer&&) = delete;
    
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
     * Queue user for indexing (asynchronous)
     */
    bool queue_user_for_indexing(const UserDocument& user, int priority = 0);
    
    /**
     * Index user immediately (synchronous)
     */
    std::future<bool> index_user_immediately(const UserDocument& user);
    
    /**
     * Update user profile in index
     */
    std::future<bool> update_user(const UserDocument& user);
    
    /**
     * Update user social metrics only
     */
    std::future<bool> update_user_metrics(
        const std::string& user_id,
        int followers_count,
        int following_count,
        int notes_count,
        long likes_received,
        long renotes_received
    );
    
    /**
     * Update user activity scores
     */
    std::future<bool> update_user_activity(
        const std::string& user_id,
        float daily_activity,
        float weekly_activity,
        float monthly_activity
    );
    
    /**
     * Delete user from index
     */
    std::future<bool> delete_user(const std::string& user_id);
    
    /**
     * BATCH OPERATIONS
     */
    
    /**
     * Index multiple users in batch
     */
    std::future<bool> index_users_batch(const std::vector<UserDocument>& users);
    
    /**
     * Refresh reputation scores for all users
     */
    std::future<bool> refresh_reputation_scores(const std::vector<std::string>& user_ids = {});
    
    /**
     * Update verification status for users
     */
    std::future<bool> update_verification_batch(
        const std::vector<std::pair<std::string, UserDocument::Verification>>& verifications
    );
    
    /**
     * SUGGESTIONS AND AUTOCOMPLETE
     */
    
    /**
     * Generate and index search suggestions for user
     */
    std::future<bool> generate_user_suggestions(const std::string& user_id);
    
    /**
     * Refresh suggestions for all users
     */
    std::future<bool> refresh_all_suggestions();
    
    /**
     * Update suggestion weights based on popularity
     */
    std::future<bool> update_suggestion_weights();
    
    /**
     * ANALYTICS AND INSIGHTS
     */
    
    /**
     * Calculate and update influence scores
     */
    std::future<bool> update_influence_scores(std::chrono::hours lookback = std::chrono::hours{168});
    
    /**
     * Detect and flag potential bot accounts
     */
    std::future<std::vector<std::string>> detect_bot_accounts(float threshold = 0.7f);
    
    /**
     * Calculate trending users based on growth
     */
    std::future<std::vector<std::string>> calculate_trending_users(std::chrono::hours window = std::chrono::hours{24});
    
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
    UserIndexingMetrics get_metrics() const;
    
    /**
     * Get detailed indexer status
     */
    nlohmann::json get_status() const;
    
    /**
     * Enable/disable debug mode
     */
    void set_debug_mode(bool enabled);
    
    /**
     * Health check
     */
    bool health_check() const;
    
    /**
     * CONFIGURATION
     */
    
    /**
     * Update configuration
     */
    void update_config(const UserIndexingConfig& new_config);
    
    /**
     * Get current configuration
     */
    UserIndexingConfig get_config() const;

private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
    
    /**
     * Worker thread functions
     */
    void indexing_worker_loop();
    void metrics_reporter_loop();
    void reputation_calculator_loop();
    void suggestion_generator_loop();
    
    /**
     * Process a batch of indexing tasks
     */
    bool process_task_batch(const std::vector<UserIndexingTask>& tasks);
    
    /**
     * Process single indexing task
     */
    bool process_task(const UserIndexingTask& task);
    
    /**
     * Handle failed task
     */
    void handle_failed_task(const UserIndexingTask& task, const std::string& error);
    
    /**
     * Update metrics
     */
    void update_metrics(
        UserIndexingOperation operation,
        bool success,
        std::chrono::milliseconds processing_time
    );
};

/**
 * Factory for creating user indexers
 */
class UserIndexerFactory {
public:
    /**
     * Create production indexer
     */
    static std::unique_ptr<UserIndexer> create_production(
        std::shared_ptr<engines::ElasticsearchEngine> engine
    );
    
    /**
     * Create development indexer
     */
    static std::unique_ptr<UserIndexer> create_development(
        std::shared_ptr<engines::ElasticsearchEngine> engine
    );
    
    /**
     * Create testing indexer
     */
    static std::unique_ptr<UserIndexer> create_testing();
    
    /**
     * Create indexer from configuration
     */
    static std::unique_ptr<UserIndexer> create_from_config(
        std::shared_ptr<engines::ElasticsearchEngine> engine,
        const UserIndexingConfig& config
    );
};

/**
 * User indexing utilities
 */
namespace user_indexing_utils {
    /**
     * Validate user document for indexing
     */
    bool validate_user_document(const UserDocument& user, std::string& error_message);
    
    /**
     * Estimate user document size in bytes
     */
    size_t estimate_document_size(const UserDocument& user);
    
    /**
     * Check if user is eligible for indexing
     */
    bool is_indexable(const UserDocument& user, const UserIndexingConfig& config);
    
    /**
     * Normalize user data for consistent indexing
     */
    UserDocument normalize_user_data(const UserDocument& user);
    
    /**
     * Generate user search keywords
     */
    std::vector<std::string> generate_search_keywords(const UserDocument& user);
    
    /**
     * Calculate user document priority for indexing
     */
    int calculate_indexing_priority(const UserDocument& user, UserIndexingOperation operation);
    
    /**
     * Merge user document updates
     */
    UserDocument merge_user_updates(const UserDocument& existing, const UserDocument& updates);
}

} // namespace indexers
} // namespace search_service
} // namespace sonet