/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/relationship.h"
#include "../../core/database/database_connection.h"
#include "../../core/cache/redis_client.h"
#include <nlohmann/json.hpp>
#include <memory>
#include <vector>
#include <string>
#include <optional>
#include <unordered_map>
#include <queue>
#include <set>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>

namespace sonet::follow::repositories {

using json = nlohmann::json;
using DatabaseConnection = sonet::core::database::DatabaseConnection;
using RedisClient = sonet::core::cache::RedisClient;

/**
 * @brief High-performance repository for managing user relationships
 * 
 * Handles bidirectional relationships including follows, blocks, and mutes
 * with advanced caching, connection pooling, and performance optimization.
 * 
 * Features:
 * - Multi-layer caching with Redis
 * - Connection pooling for high concurrency
 * - Bulk operations for efficiency
 * - Real-time performance tracking
 * - Relationship analytics and insights
 */
class RelationshipRepository {
public:
    /**
     * @brief Constructor
     * 
     * @param db_connection Database connection factory
     * @param redis_client Redis cache client
     * @param config Configuration JSON with cache settings, pool size, etc.
     */
    RelationshipRepository(std::shared_ptr<DatabaseConnection> db_connection,
                          std::shared_ptr<RedisClient> redis_client,
                          const json& config = json{});
    
    /**
     * @brief Destructor - cleans up connection pool
     */
    ~RelationshipRepository();

    // ========== CORE RELATIONSHIP OPERATIONS ==========

    /**
     * @brief Get relationship between two users
     * 
     * @param user1_id First user ID (perspective user)
     * @param user2_id Second user ID (target user)
     * @return Relationship object with normalized perspective
     */
    Relationship get_relationship(const std::string& user1_id, const std::string& user2_id);

    /**
     * @brief Create or update a relationship
     * 
     * @param relationship Relationship object to store
     * @return true if successful, false otherwise
     */
    bool create_or_update_relationship(const Relationship& relationship);

    /**
     * @brief Update follow status between users
     * 
     * @param follower_id User who is following
     * @param following_id User being followed
     * @param is_following true to follow, false to unfollow
     * @return true if successful, false otherwise
     */
    bool update_follow_status(const std::string& follower_id, 
                             const std::string& following_id, 
                             bool is_following);

    /**
     * @brief Update block status between users
     * 
     * @param blocker_id User who is blocking
     * @param blocked_id User being blocked
     * @param is_blocked true to block, false to unblock
     * @return true if successful, false otherwise
     */
    bool update_block_status(const std::string& blocker_id, 
                           const std::string& blocked_id, 
                           bool is_blocked);

    /**
     * @brief Update mute status between users
     * 
     * @param muter_id User who is muting
     * @param muted_id User being muted
     * @param is_muted true to mute, false to unmute
     * @return true if successful, false otherwise
     */
    bool update_mute_status(const std::string& muter_id, 
                          const std::string& muted_id, 
                          bool is_muted);

    // ========== BULK OPERATIONS ==========

    /**
     * @brief Get multiple relationships in a single query
     * 
     * @param user_pairs Vector of user ID pairs to check
     * @return Vector of relationships (empty relationships for non-existent pairs)
     */
    std::vector<Relationship> get_relationships_batch(
        const std::vector<std::pair<std::string, std::string>>& user_pairs);

    /**
     * @brief Bulk update follow statuses
     * 
     * @param operations Vector of follow operations (follower, following, status)
     * @return Number of successful operations
     */
    int bulk_update_follow_status(
        const std::vector<std::tuple<std::string, std::string, bool>>& operations);

    /**
     * @brief Bulk update block statuses
     * 
     * @param operations Vector of block operations (blocker, blocked, status)
     * @return Number of successful operations
     */
    int bulk_update_block_status(
        const std::vector<std::tuple<std::string, std::string, bool>>& operations);

    // ========== ANALYTICS & INSIGHTS ==========

    /**
     * @brief Get relationship statistics for a user
     * 
     * @param user_id User to analyze
     * @param days Number of days to look back
     * @return JSON with follower/following counts, mutual follows, etc.
     */
    json get_user_relationship_stats(const std::string& user_id, int days = 30);

    /**
     * @brief Get relationship activity timeline
     * 
     * @param user_id User to analyze
     * @param days Number of days to look back
     * @return JSON with daily activity counts
     */
    json get_relationship_activity_timeline(const std::string& user_id, int days = 7);

    /**
     * @brief Get mutual connection analysis
     * 
     * @param user1_id First user
     * @param user2_id Second user
     * @return JSON with mutual friends, connection strength, etc.
     */
    json get_mutual_connection_analysis(const std::string& user1_id, const std::string& user2_id);

    // ========== CACHE MANAGEMENT ==========

    /**
     * @brief Get relationship from cache
     * 
     * @param user1_id First user ID
     * @param user2_id Second user ID
     * @return Optional relationship if cached
     */
    std::optional<Relationship> get_cached_relationship(const std::string& user1_id, 
                                                       const std::string& user2_id);

    /**
     * @brief Cache a relationship
     * 
     * @param user1_id First user ID
     * @param user2_id Second user ID
     * @param relationship Relationship to cache
     */
    void cache_relationship(const std::string& user1_id, 
                          const std::string& user2_id, 
                          const Relationship& relationship);

    /**
     * @brief Invalidate cached relationship
     * 
     * @param user1_id First user ID
     * @param user2_id Second user ID
     */
    void invalidate_relationship_cache(const std::string& user1_id, const std::string& user2_id);

    /**
     * @brief Clear all cached relationships for a user
     * 
     * @param user_id User whose cache to clear
     */
    void clear_user_relationship_cache(const std::string& user_id);

    // ========== PERFORMANCE & MONITORING ==========

    /**
     * @brief Get repository performance metrics
     * 
     * @return JSON with cache hit rates, query times, connection pool status
     */
    json get_performance_metrics() const;

    /**
     * @brief Get cache statistics
     * 
     * @return JSON with hit/miss rates, eviction counts, etc.
     */
    json get_cache_statistics() const;

    /**
     * @brief Get connection pool status
     * 
     * @return JSON with active/idle connection counts
     */
    json get_connection_pool_status() const;

    /**
     * @brief Reset performance counters
     */
    void reset_performance_counters();

    // ========== HEALTH & MAINTENANCE ==========

    /**
     * @brief Check repository health
     * 
     * @return true if database and cache are accessible
     */
    bool health_check();

    /**
     * @brief Clean up expired cache entries
     * 
     * @return Number of entries cleaned
     */
    int cleanup_expired_cache();

    /**
     * @brief Optimize database indexes and statistics
     * 
     * @return true if optimization completed successfully
     */
    bool optimize_database();

private:
    // ========== PRIVATE MEMBERS ==========
    
    std::shared_ptr<DatabaseConnection> db_connection_;
    std::shared_ptr<RedisClient> redis_client_;
    json config_;
    
    // Cache configuration
    bool enable_cache_;
    int cache_ttl_seconds_;
    
    // Performance tracking
    mutable std::atomic<int> cache_hit_count_;
    mutable std::atomic<int> cache_miss_count_;
    mutable std::atomic<int> total_queries_;
    mutable std::unordered_map<std::string, double> operation_times_;
    mutable std::mutex performance_mutex_;
    
    // Connection pool
    int connection_pool_size_;
    int batch_size_;
    std::queue<std::shared_ptr<DatabaseConnection>> available_connections_;
    std::set<std::shared_ptr<DatabaseConnection>> busy_connections_;
    std::mutex connection_mutex_;
    std::condition_variable connection_cv_;
    
    // ========== PRIVATE METHODS ==========
    
    /**
     * @brief Initialize database connection pool
     */
    void initialize_connection_pool();
    
    /**
     * @brief Cleanup connection pool on shutdown
     */
    void cleanup_connection_pool();
    
    /**
     * @brief Get a connection from the pool
     * 
     * @return Database connection (blocks if none available)
     */
    std::shared_ptr<DatabaseConnection> get_connection();
    
    /**
     * @brief Return a connection to the pool
     * 
     * @param connection Connection to return
     */
    void return_connection(std::shared_ptr<DatabaseConnection> connection);
    
    /**
     * @brief Track operation performance
     * 
     * @param operation Operation name
     * @param duration_us Duration in microseconds
     */
    void track_operation_performance(const std::string& operation, int64_t duration_us);
    
    /**
     * @brief Generate normalized cache key for user pair
     * 
     * @param user1_id First user ID
     * @param user2_id Second user ID
     * @return Normalized cache key
     */
    std::string generate_cache_key(const std::string& user1_id, const std::string& user2_id) const;
    
    /**
     * @brief Execute query with retry logic
     * 
     * @param query SQL query
     * @param params Query parameters
     * @return Query result
     */
    template<typename... Args>
    auto execute_with_retry(const std::string& query, Args&&... params);
    
    /**
     * @brief Batch insert relationships
     * 
     * @param relationships Vector of relationships to insert
     * @return Number of successfully inserted relationships
     */
    int batch_insert_relationships(const std::vector<Relationship>& relationships);
    
    /**
     * @brief Update relationship interaction metrics
     * 
     * @param user1_id First user ID
     * @param user2_id Second user ID
     * @param interaction_type Type of interaction
     */
    void update_interaction_metrics(const std::string& user1_id, 
                                  const std::string& user2_id,
                                  const std::string& interaction_type);
};

} // namespace sonet::follow::repositories