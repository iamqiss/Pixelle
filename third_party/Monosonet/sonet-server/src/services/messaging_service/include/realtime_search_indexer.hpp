/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <set>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <functional>
#include <future>
#include <thread>
#include <queue>
#include <json/json.h>
#include <regex>

namespace sonet::messaging::search {

/**
 * @brief Search scope for targeted queries
 */
enum class SearchScope {
    ALL_CONTENT = 0,      // Search all accessible content
    CURRENT_CHAT = 1,     // Search current chat only
    SPECIFIC_USER = 2,    // Search messages from specific user
    TIME_RANGE = 3,       // Search within time range
    THREADS_ONLY = 4,     // Search thread messages only
    MAIN_MESSAGES = 5,    // Search main chat messages only
    ATTACHMENTS = 6,      // Search attachment metadata
    MEDIA_CONTENT = 7,    // Search media descriptions/captions
    SHARED_FILES = 8      // Search shared file content
};

/**
 * @brief Search result type classification
 */
enum class SearchResultType {
    TEXT_MESSAGE = 0,     // Regular text message
    MEDIA_MESSAGE = 1,    // Image/video with caption
    FILE_MESSAGE = 2,     // Shared file/document
    VOICE_MESSAGE = 3,    // Voice note with transcription
    SYSTEM_MESSAGE = 4,   // System/notification message
    THREAD_MESSAGE = 5,   // Message in thread
    REPLY_MESSAGE = 6,    // Reply to another message
    FORWARD_MESSAGE = 7,  // Forwarded message
    EDITED_MESSAGE = 8,   // Edited message content
    ATTACHMENT_META = 9   // Attachment metadata/filename
};

/**
 * @brief Search ranking factors
 */
enum class SearchRankingFactor {
    EXACT_MATCH = 0,      // Exact phrase match
    PARTIAL_MATCH = 1,    // Partial word match
    RELEVANCE_SCORE = 2,  // TF-IDF relevance
    RECENCY = 3,          // Message timestamp
    USER_INTERACTION = 4, // Reactions, replies, etc.
    MESSAGE_IMPORTANCE = 5, // Pinned, starred messages
    CONTEXT_MATCH = 6,    // Context similarity
    SEMANTIC_MATCH = 7,   // Semantic similarity
    POPULARITY = 8,       // Message engagement
    PERSONAL_RELEVANCE = 9 // User-specific relevance
};

/**
 * @brief Advanced search filters
 */
struct SearchFilters {
    std::string query;
    SearchScope scope;
    
    // User filters
    std::vector<std::string> from_users;
    std::vector<std::string> exclude_users;
    
    // Time filters
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
    
    // Content filters
    std::vector<SearchResultType> include_types;
    std::vector<SearchResultType> exclude_types;
    bool include_deleted;
    bool include_edited;
    bool only_starred;
    bool only_pinned;
    
    // Context filters
    std::vector<std::string> in_chats;
    std::vector<std::string> in_threads;
    std::vector<std::string> with_attachments;
    std::vector<std::string> with_reactions;
    
    // Advanced filters
    uint32_t min_message_length;
    uint32_t max_message_length;
    std::vector<std::string> hashtags;
    std::vector<std::string> mentions;
    std::vector<std::string> file_types;
    
    // Ranking preferences
    std::map<SearchRankingFactor, double> ranking_weights;
    bool semantic_search_enabled;
    bool fuzzy_matching_enabled;
    double min_relevance_score;
    
    Json::Value to_json() const;
    static SearchFilters from_json(const Json::Value& json);
    static SearchFilters default_filters();
    bool matches_result_type(SearchResultType type) const;
    bool matches_time_range(const std::chrono::system_clock::time_point& timestamp) const;
};

/**
 * @brief Search result with comprehensive metadata
 */
struct SearchResult {
    std::string result_id;
    std::string message_id;
    std::string chat_id;
    std::string thread_id;
    std::string user_id;
    SearchResultType type;
    
    // Content
    std::string content;
    std::string original_content; // Before any processing
    std::string highlighted_content; // With search term highlighting
    std::vector<std::string> matched_terms;
    std::vector<std::pair<size_t, size_t>> match_positions; // start, length
    
    // Metadata
    std::chrono::system_clock::time_point timestamp;
    std::chrono::system_clock::time_point edited_at;
    bool is_deleted;
    bool is_edited;
    bool is_starred;
    bool is_pinned;
    
    // Context
    std::string reply_to_message_id;
    std::string forwarded_from_chat_id;
    std::vector<std::string> mentions;
    std::vector<std::string> hashtags;
    
    // Attachments
    std::vector<std::string> attachment_ids;
    std::vector<std::string> attachment_types;
    std::vector<std::string> attachment_names;
    
    // Reactions and engagement
    uint32_t reaction_count;
    uint32_t reply_count;
    uint32_t view_count;
    std::vector<std::string> reaction_types;
    
    // Scoring
    double relevance_score;
    double exact_match_score;
    double recency_score;
    double engagement_score;
    double final_score;
    std::map<SearchRankingFactor, double> factor_scores;
    
    // Context preview
    std::string before_context;
    std::string after_context;
    std::vector<SearchResult> thread_context; // Related thread messages
    
    Json::Value to_json() const;
    static SearchResult from_json(const Json::Value& json);
    bool is_relevant(double min_score) const;
    std::string get_display_content(size_t max_length = 200) const;
};

/**
 * @brief Search index entry for efficient querying
 */
struct SearchIndexEntry {
    std::string message_id;
    std::string chat_id;
    std::string user_id;
    std::string thread_id;
    SearchResultType type;
    
    // Processed content
    std::vector<std::string> words;
    std::vector<std::string> stemmed_words;
    std::unordered_map<std::string, uint32_t> word_frequencies;
    std::unordered_map<std::string, std::vector<size_t>> word_positions;
    
    // Metadata for ranking
    std::chrono::system_clock::time_point timestamp;
    uint32_t message_length;
    uint32_t engagement_score;
    bool is_important; // starred, pinned, etc.
    
    // Semantic vectors (for semantic search)
    std::vector<double> semantic_vector;
    std::string semantic_summary;
    
    // Quick access data
    std::unordered_set<std::string> unique_words;
    std::unordered_set<std::string> mentions;
    std::unordered_set<std::string> hashtags;
    
    Json::Value to_json() const;
    static SearchIndexEntry from_json(const Json::Value& json);
    double calculate_tf_idf_score(const std::string& term, 
                                 const std::unordered_map<std::string, uint32_t>& document_frequencies,
                                 uint32_t total_documents) const;
    bool matches_term(const std::string& term, bool exact_match = false) const;
};

/**
 * @brief Real-time search statistics
 */
struct SearchStatistics {
    std::chrono::system_clock::time_point collection_start;
    std::chrono::system_clock::time_point last_update;
    
    // Index statistics
    uint64_t total_indexed_messages;
    uint64_t total_indexed_words;
    uint64_t unique_words_count;
    uint64_t total_index_size_bytes;
    
    // Query statistics
    uint64_t total_queries_processed;
    uint64_t successful_queries;
    uint64_t failed_queries;
    std::chrono::milliseconds average_query_time;
    std::chrono::milliseconds fastest_query_time;
    std::chrono::milliseconds slowest_query_time;
    
    // Popular queries
    std::map<std::string, uint32_t> popular_terms;
    std::map<SearchScope, uint32_t> scope_usage;
    std::map<SearchResultType, uint32_t> result_type_distribution;
    
    // Performance metrics
    double index_update_rate; // Updates per second
    double query_success_rate;
    double average_results_per_query;
    uint32_t cache_hit_rate;
    
    // Real-time metrics
    uint32_t current_concurrent_queries;
    uint32_t pending_index_updates;
    std::chrono::milliseconds current_index_lag;
    
    Json::Value to_json() const;
    void reset();
    void update_query_time(std::chrono::milliseconds query_time);
    void record_query(const std::string& query, SearchScope scope, bool successful);
};

/**
 * @brief Search index configuration
 */
struct SearchIndexConfig {
    // Indexing settings
    bool real_time_indexing;
    std::chrono::milliseconds index_batch_interval;
    uint32_t max_batch_size;
    bool enable_stemming;
    bool enable_stop_words_removal;
    bool enable_semantic_indexing;
    
    // Storage settings
    std::string index_storage_path;
    bool persist_to_disk;
    uint32_t memory_cache_size_mb;
    std::chrono::hours max_cache_age;
    
    // Query settings
    uint32_t max_results_per_query;
    std::chrono::milliseconds query_timeout;
    bool enable_query_caching;
    bool enable_fuzzy_search;
    double fuzzy_threshold;
    
    // Content processing
    std::vector<std::string> ignored_file_types;
    std::vector<std::string> stop_words;
    uint32_t max_word_length;
    uint32_t min_word_length;
    
    // Language settings
    std::string primary_language;
    std::vector<std::string> supported_languages;
    bool auto_detect_language;
    
    Json::Value to_json() const;
    static SearchIndexConfig from_json(const Json::Value& json);
    static SearchIndexConfig default_config();
};

/**
 * @brief Advanced real-time search and indexing engine
 * 
 * Provides comprehensive search capabilities including:
 * - Real-time message indexing with incremental updates
 * - Full-text search with advanced filtering and ranking
 * - Semantic search using vector embeddings
 * - Encrypted content search with privacy preservation
 * - Multi-modal content search (text, files, media)
 * - Context-aware search with thread and chat scope
 * - Performance-optimized with caching and async processing
 * - Analytics and search pattern insights
 */
class RealTimeSearchIndexer {
public:
    RealTimeSearchIndexer(const SearchIndexConfig& config = SearchIndexConfig::default_config());
    ~RealTimeSearchIndexer();
    
    // Index management
    std::future<bool> initialize_index();
    std::future<bool> rebuild_index();
    std::future<bool> optimize_index();
    std::future<bool> backup_index(const std::string& backup_path);
    std::future<bool> restore_index(const std::string& backup_path);
    
    // Real-time indexing
    std::future<bool> index_message(const std::string& message_id,
                                   const std::string& chat_id,
                                   const std::string& user_id,
                                   const std::string& content,
                                   SearchResultType type = SearchResultType::TEXT_MESSAGE,
                                   const std::string& thread_id = "");
    
    std::future<bool> update_message_index(const std::string& message_id,
                                          const std::string& new_content);
    
    std::future<bool> remove_message_from_index(const std::string& message_id);
    
    std::future<bool> index_batch_messages(const std::vector<Json::Value>& message_batch);
    
    // Advanced content indexing
    std::future<bool> index_file_content(const std::string& message_id,
                                        const std::string& file_path,
                                        const std::string& file_type,
                                        const std::string& extracted_content);
    
    std::future<bool> index_media_metadata(const std::string& message_id,
                                          const Json::Value& media_metadata);
    
    std::future<bool> index_voice_transcription(const std::string& message_id,
                                               const std::string& transcription,
                                               double confidence_score);
    
    // Search operations
    std::future<std::vector<SearchResult>> search(const std::string& query,
                                                  const SearchFilters& filters = SearchFilters::default_filters(),
                                                  uint32_t max_results = 50);
    
    std::future<std::vector<SearchResult>> semantic_search(const std::string& query,
                                                           const SearchFilters& filters = SearchFilters::default_filters(),
                                                           uint32_t max_results = 20);
    
    std::future<std::vector<SearchResult>> fuzzy_search(const std::string& query,
                                                        double similarity_threshold = 0.7,
                                                        const SearchFilters& filters = SearchFilters::default_filters());
    
    std::future<std::vector<SearchResult>> search_with_context(const std::string& query,
                                                               const std::string& chat_id,
                                                               uint32_t context_messages = 3);
    
    // Advanced search features
    std::future<std::vector<std::string>> get_search_suggestions(const std::string& partial_query,
                                                                uint32_t max_suggestions = 10);
    
    std::future<std::vector<std::string>> get_popular_search_terms(uint32_t count = 20);
    
    std::future<std::unordered_map<std::string, uint32_t>> get_search_analytics(const std::chrono::system_clock::time_point& start,
                                                                               const std::chrono::system_clock::time_point& end);
    
    // Query assistance
    std::future<std::string> build_smart_query(const std::vector<std::string>& keywords,
                                              const std::vector<std::string>& context_clues);
    
    std::future<SearchFilters> suggest_search_filters(const std::string& query,
                                                      const std::string& user_id);
    
    // Index introspection
    std::future<SearchStatistics> get_index_statistics();
    std::future<std::vector<std::string>> get_indexed_chats();
    std::future<uint64_t> get_message_count_for_chat(const std::string& chat_id);
    std::future<std::vector<std::string>> get_most_frequent_words(uint32_t count = 100);
    
    // Real-time subscriptions
    void subscribe_to_search_updates(const std::string& subscriber_id,
                                    std::function<void(const std::string&)> callback);
    
    void subscribe_to_index_updates(const std::string& subscriber_id,
                                   std::function<void(const SearchStatistics&)> callback);
    
    void unsubscribe_from_search_updates(const std::string& subscriber_id);
    void unsubscribe_from_index_updates(const std::string& subscriber_id);
    
    // Configuration management
    void update_configuration(const SearchIndexConfig& new_config);
    SearchIndexConfig get_configuration() const;
    
    // Performance monitoring
    void enable_performance_monitoring(bool enabled);
    std::future<Json::Value> get_performance_metrics();
    
    // Cache management
    void clear_search_cache();
    void warm_up_cache(const std::vector<std::string>& common_queries);
    
    // Maintenance operations
    void start_background_optimization();
    void stop_background_optimization();
    void force_garbage_collection();
    
    // Privacy and security
    std::future<bool> enable_encrypted_search(const std::string& encryption_key);
    std::future<bool> disable_encrypted_search();
    bool is_encrypted_search_enabled() const;
    
    // Export and analysis
    std::future<Json::Value> export_search_patterns(const std::string& user_id,
                                                    const std::chrono::system_clock::time_point& start,
                                                    const std::chrono::system_clock::time_point& end);
    
    std::future<std::vector<std::string>> detect_trending_topics(uint32_t hours_back = 24);
    
private:
    // Core storage
    std::unordered_map<std::string, SearchIndexEntry> message_index_;
    std::unordered_map<std::string, std::unordered_set<std::string>> word_to_messages_;
    std::unordered_map<std::string, std::unordered_set<std::string>> chat_to_messages_;
    std::unordered_map<std::string, std::unordered_set<std::string>> user_to_messages_;
    std::unordered_map<std::string, uint32_t> document_frequencies_;
    
    // Semantic search
    std::unordered_map<std::string, std::vector<double>> semantic_vectors_;
    
    // Configuration
    SearchIndexConfig config_;
    std::atomic<bool> encrypted_search_enabled_;
    std::string encryption_key_;
    
    // Caching
    std::unordered_map<std::string, std::pair<std::vector<SearchResult>, std::chrono::system_clock::time_point>> query_cache_;
    std::unordered_map<std::string, std::vector<std::string>> suggestion_cache_;
    
    // Statistics
    SearchStatistics statistics_;
    
    // Thread safety
    mutable std::shared_mutex index_mutex_;
    mutable std::shared_mutex cache_mutex_;
    mutable std::shared_mutex statistics_mutex_;
    mutable std::shared_mutex subscriptions_mutex_;
    
    // Background processing
    std::thread indexing_thread_;
    std::thread optimization_thread_;
    std::queue<Json::Value> pending_updates_;
    mutable std::mutex pending_updates_mutex_;
    std::atomic<bool> background_running_;
    
    // Subscriptions
    std::unordered_map<std::string, std::function<void(const std::string&)>> search_subscribers_;
    std::unordered_map<std::string, std::function<void(const SearchStatistics&)>> index_subscribers_;
    
    // Text processing
    std::vector<std::string> tokenize_text(const std::string& text);
    std::vector<std::string> stem_words(const std::vector<std::string>& words);
    std::vector<std::string> remove_stop_words(const std::vector<std::string>& words);
    std::string normalize_text(const std::string& text);
    
    // Scoring and ranking
    double calculate_relevance_score(const SearchIndexEntry& entry,
                                   const std::vector<std::string>& query_terms,
                                   const SearchFilters& filters);
    
    double calculate_recency_score(const std::chrono::system_clock::time_point& timestamp);
    double calculate_engagement_score(const SearchResult& result);
    std::vector<SearchResult> rank_search_results(std::vector<SearchResult>& results,
                                                  const SearchFilters& filters);
    
    // Query processing
    std::vector<std::string> process_query(const std::string& query);
    std::vector<std::string> expand_query_terms(const std::vector<std::string>& terms);
    bool matches_filters(const SearchIndexEntry& entry, const SearchFilters& filters);
    
    // Semantic search utilities
    std::vector<double> generate_semantic_vector(const std::string& content);
    double calculate_semantic_similarity(const std::vector<double>& vec1,
                                        const std::vector<double>& vec2);
    
    // Background processing
    void run_indexing_loop();
    void run_optimization_loop();
    void process_pending_updates();
    
    // Cache management
    bool is_query_cached(const std::string& cache_key);
    void cache_query_result(const std::string& cache_key,
                           const std::vector<SearchResult>& results);
    std::vector<SearchResult> get_cached_result(const std::string& cache_key);
    std::string generate_cache_key(const std::string& query, const SearchFilters& filters);
    
    // Utility methods
    void notify_search_subscribers(const std::string& message);
    void notify_index_subscribers(const SearchStatistics& stats);
    void update_statistics(const std::string& query, SearchScope scope, bool successful, 
                          std::chrono::milliseconds query_time);
    
    void log_info(const std::string& message);
    void log_warning(const std::string& message);
    void log_error(const std::string& message);
};

/**
 * @brief Search query builder for advanced queries
 */
class SearchQueryBuilder {
public:
    SearchQueryBuilder();
    
    // Basic query building
    SearchQueryBuilder& with_text(const std::string& text);
    SearchQueryBuilder& from_user(const std::string& user_id);
    SearchQueryBuilder& in_chat(const std::string& chat_id);
    SearchQueryBuilder& in_thread(const std::string& thread_id);
    SearchQueryBuilder& of_type(SearchResultType type);
    
    // Time filters
    SearchQueryBuilder& after(const std::chrono::system_clock::time_point& time);
    SearchQueryBuilder& before(const std::chrono::system_clock::time_point& time);
    SearchQueryBuilder& in_last_days(uint32_t days);
    SearchQueryBuilder& in_last_hours(uint32_t hours);
    
    // Content filters
    SearchQueryBuilder& with_attachments();
    SearchQueryBuilder& with_reactions();
    SearchQueryBuilder& starred_only();
    SearchQueryBuilder& pinned_only();
    SearchQueryBuilder& include_deleted();
    
    // Advanced filters
    SearchQueryBuilder& min_length(uint32_t length);
    SearchQueryBuilder& max_length(uint32_t length);
    SearchQueryBuilder& with_hashtag(const std::string& hashtag);
    SearchQueryBuilder& with_mention(const std::string& user_id);
    SearchQueryBuilder& with_file_type(const std::string& file_type);
    
    // Ranking and scoring
    SearchQueryBuilder& set_ranking_weight(SearchRankingFactor factor, double weight);
    SearchQueryBuilder& enable_semantic_search();
    SearchQueryBuilder& enable_fuzzy_matching(double threshold = 0.7);
    SearchQueryBuilder& min_relevance(double score);
    
    // Build final query
    SearchFilters build() const;
    std::string to_query_string() const;
    
private:
    SearchFilters filters_;
};

/**
 * @brief Search utilities and helpers
 */
class SearchUtils {
public:
    // Text processing
    static std::string highlight_matches(const std::string& content,
                                       const std::vector<std::string>& terms);
    static std::vector<std::string> extract_keywords(const std::string& text,
                                                    uint32_t max_keywords = 10);
    static std::string clean_search_query(const std::string& query);
    
    // Query analysis
    static bool is_semantic_query(const std::string& query);
    static std::vector<std::string> detect_search_operators(const std::string& query);
    static std::pair<std::string, SearchFilters> parse_advanced_query(const std::string& query);
    
    // Result formatting
    static std::string format_search_summary(const std::vector<SearchResult>& results,
                                            const std::string& query);
    static Json::Value results_to_json(const std::vector<SearchResult>& results);
    static std::vector<SearchResult> results_from_json(const Json::Value& json);
    
    // Performance optimization
    static std::string optimize_query(const std::string& query);
    static bool should_use_semantic_search(const std::string& query);
    static uint32_t estimate_result_count(const std::string& query);
    
    // Analytics
    static std::map<std::string, uint32_t> extract_query_patterns(const std::vector<std::string>& queries);
    static double calculate_query_complexity(const std::string& query);
    static std::vector<std::string> suggest_query_improvements(const std::string& query);
};

} // namespace sonet::messaging::search
