/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "relationship_repository.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <algorithm>
#include <future>

namespace sonet::follow::repositories {

using namespace std::chrono;
using json = nlohmann::json;

// ========== CONSTRUCTOR & INITIALIZATION ==========

RelationshipRepository::RelationshipRepository(std::shared_ptr<DatabaseConnection> db_connection,
                                             std::shared_ptr<RedisClient> redis_client,
                                             const json& config)
    : db_connection_(db_connection),
      redis_client_(redis_client),
      config_(config),
      cache_ttl_seconds_(config_.value("cache_ttl_seconds", 3600)),
      batch_size_(config_.value("batch_size", 1000)),
      connection_pool_size_(config_.value("connection_pool_size", 20)) {
    
    spdlog::info("🔗 Initializing Twitter-Scale Relationship Repository...");
    
    // Initialize connection pool
    initialize_connection_pool();
    
    // Initialize cache settings
    enable_cache_ = config_.value("enable_cache", true);
    cache_hit_count_ = 0;
    cache_miss_count_ = 0;
    total_queries_ = 0;
    
    // Initialize performance tracking
    operation_times_.clear();
    
    spdlog::info("✅ Relationship Repository initialized: cache={}, ttl={}s, pool_size={}", 
                enable_cache_, cache_ttl_seconds_, connection_pool_size_);
}

RelationshipRepository::~RelationshipRepository() {
    cleanup_connection_pool();
}

// ========== CORE RELATIONSHIP OPERATIONS ==========

Relationship RelationshipRepository::get_relationship(const std::string& user1_id, const std::string& user2_id) {
    auto start = high_resolution_clock::now();
    total_queries_++;
    
    try {
        // Try cache first
        if (enable_cache_) {
            auto cached_relationship = get_cached_relationship(user1_id, user2_id);
            if (cached_relationship.has_value()) {
                cache_hit_count_++;
                auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
                track_operation_performance("get_relationship_cached", duration);
                
                spdlog::debug("✅ Relationship cache hit: {} <-> {} in {}μs", 
                             user1_id, user2_id, duration);
                return cached_relationship.value();
            }
            cache_miss_count_++;
        }
        
        // Get from database
        auto connection = get_connection();
        
        // Create bidirectional relationship key (ordered to avoid duplicates)
        std::string key1 = user1_id < user2_id ? user1_id + ":" + user2_id : user2_id + ":" + user1_id;
        
        std::string query = R"(
            SELECT 
                user1_id,
                user2_id,
                user1_follows_user2,
                user2_follows_user1,
                user1_blocks_user2,
                user2_blocks_user1,
                user1_mutes_user2,
                user2_mutes_user1,
                interaction_count,
                relationship_strength,
                last_interaction,
                created_at,
                updated_at
            FROM relationships 
            WHERE (user1_id = ? AND user2_id = ?) 
               OR (user1_id = ? AND user2_id = ?)
            LIMIT 1
        )";
        
        auto stmt = connection->prepare(query);
        stmt->bind(1, user1_id);
        stmt->bind(2, user2_id);
        stmt->bind(3, user2_id);
        stmt->bind(4, user1_id);
        
        auto result = stmt->execute();
        
        Relationship relationship;
        if (result->next()) {
            // Parse result into relationship object
            relationship.user1_id = result->getString("user1_id");
            relationship.user2_id = result->getString("user2_id");
            relationship.user1_follows_user2 = result->getBoolean("user1_follows_user2");
            relationship.user2_follows_user1 = result->getBoolean("user2_follows_user1");
            relationship.user1_blocks_user2 = result->getBoolean("user1_blocks_user2");
            relationship.user2_blocks_user1 = result->getBoolean("user2_blocks_user1");
            relationship.user1_mutes_user2 = result->getBoolean("user1_mutes_user2");
            relationship.user2_mutes_user1 = result->getBoolean("user2_mutes_user1");
            relationship.interaction_count = result->getInt("interaction_count");
            relationship.relationship_strength = result->getDouble("relationship_strength");
            relationship.last_interaction = result->getTimestamp("last_interaction");
            relationship.created_at = result->getTimestamp("created_at");
            relationship.updated_at = result->getTimestamp("updated_at");
            
            // Normalize perspective for requesting user
            relationship.normalize_perspective(user1_id, user2_id);
        } else {
            // Create empty relationship
            relationship = Relationship(user1_id, user2_id);
        }
        
        return_connection(connection);
        
        // Cache the result
        if (enable_cache_) {
            cache_relationship(user1_id, user2_id, relationship);
        }
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        track_operation_performance("get_relationship_db", duration);
        
        spdlog::debug("✅ Relationship retrieved: {} <-> {} in {}μs", 
                     user1_id, user2_id, duration);
        
        return relationship;
        
    } catch (const std::exception& e) {
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        spdlog::error("❌ Failed to get relationship {} <-> {}: {} ({}μs)", 
                     user1_id, user2_id, e.what(), duration);
        return Relationship(user1_id, user2_id); // Return empty relationship on error
    }
}

bool RelationshipRepository::create_or_update_relationship(const Relationship& relationship) {
    auto start = high_resolution_clock::now();
    total_queries_++;
    
    try {
        auto connection = get_connection();
        
        std::string query = R"(
            INSERT INTO relationships (
                user1_id, user2_id, user1_follows_user2, user2_follows_user1,
                user1_blocks_user2, user2_blocks_user1, user1_mutes_user2, user2_mutes_user1,
                interaction_count, relationship_strength, last_interaction, 
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                user1_follows_user2 = VALUES(user1_follows_user2),
                user2_follows_user1 = VALUES(user2_follows_user1),
                user1_blocks_user2 = VALUES(user1_blocks_user2),
                user2_blocks_user1 = VALUES(user2_blocks_user1),
                user1_mutes_user2 = VALUES(user1_mutes_user2),
                user2_mutes_user1 = VALUES(user2_mutes_user1),
                interaction_count = VALUES(interaction_count),
                relationship_strength = VALUES(relationship_strength),
                last_interaction = VALUES(last_interaction),
                updated_at = NOW()
        )";
        
        auto stmt = connection->prepare(query);
        
        // Ensure consistent ordering for user IDs
        std::string user1_id = relationship.user1_id;
        std::string user2_id = relationship.user2_id;
        bool user1_follows_user2 = relationship.user1_follows_user2;
        bool user2_follows_user1 = relationship.user2_follows_user1;
        bool user1_blocks_user2 = relationship.user1_blocks_user2;
        bool user2_blocks_user1 = relationship.user2_blocks_user1;
        bool user1_mutes_user2 = relationship.user1_mutes_user2;
        bool user2_mutes_user1 = relationship.user2_mutes_user1;
        
        if (user1_id > user2_id) {
            // Swap to maintain consistent ordering
            std::swap(user1_id, user2_id);
            std::swap(user1_follows_user2, user2_follows_user1);
            std::swap(user1_blocks_user2, user2_blocks_user1);
            std::swap(user1_mutes_user2, user2_mutes_user1);
        }
        
        stmt->bind(1, user1_id);
        stmt->bind(2, user2_id);
        stmt->bind(3, user1_follows_user2);
        stmt->bind(4, user2_follows_user1);
        stmt->bind(5, user1_blocks_user2);
        stmt->bind(6, user2_blocks_user1);
        stmt->bind(7, user1_mutes_user2);
        stmt->bind(8, user2_mutes_user1);
        stmt->bind(9, relationship.interaction_count);
        stmt->bind(10, relationship.relationship_strength);
        stmt->bind(11, relationship.last_interaction);
        
        auto result = stmt->execute();
        bool success = result->getAffectedRows() > 0;
        
        return_connection(connection);
        
        // Invalidate cache
        if (enable_cache_ && success) {
            invalidate_relationship_cache(relationship.user1_id, relationship.user2_id);
        }
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        track_operation_performance("create_or_update_relationship", duration);
        
        if (success) {
            spdlog::debug("✅ Relationship created/updated: {} <-> {} in {}μs", 
                         relationship.user1_id, relationship.user2_id, duration);
        } else {
            spdlog::warn("⚠️ Failed to create/update relationship: {} <-> {}", 
                        relationship.user1_id, relationship.user2_id);
        }
        
        return success;
        
    } catch (const std::exception& e) {
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        spdlog::error("❌ Failed to create/update relationship {} <-> {}: {} ({}μs)", 
                     relationship.user1_id, relationship.user2_id, e.what(), duration);
        return false;
    }
}

bool RelationshipRepository::update_follow_status(const std::string& follower_id, 
                                                const std::string& following_id, 
                                                bool is_following) {
    auto start = high_resolution_clock::now();
    total_queries_++;
    
    try {
        auto connection = get_connection();
        
        // Get current relationship
        auto current_relationship = get_relationship(follower_id, following_id);
        
        // Update follow status based on perspective
        if (current_relationship.user1_id == follower_id) {
            current_relationship.user1_follows_user2 = is_following;
        } else {
            current_relationship.user2_follows_user1 = is_following;
        }
        
        // Update interaction count and strength if following
        if (is_following) {
            current_relationship.interaction_count++;
            current_relationship.relationship_strength = std::min(1.0, 
                current_relationship.relationship_strength + 0.1);
            current_relationship.last_interaction = system_clock::now();
        }
        
        bool success = create_or_update_relationship(current_relationship);
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        track_operation_performance("update_follow_status", duration);
        
        spdlog::debug("✅ Follow status updated: {} {} {} in {}μs", 
                     follower_id, is_following ? "follows" : "unfollows", following_id, duration);
        
        return success;
        
    } catch (const std::exception& e) {
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        spdlog::error("❌ Failed to update follow status {} -> {}: {} ({}μs)", 
                     follower_id, following_id, e.what(), duration);
        return false;
    }
}

bool RelationshipRepository::update_block_status(const std::string& blocker_id, 
                                               const std::string& blocked_id, 
                                               bool is_blocked) {
    auto start = high_resolution_clock::now();
    total_queries_++;
    
    try {
        auto connection = get_connection();
        
        // Get current relationship
        auto current_relationship = get_relationship(blocker_id, blocked_id);
        
        // Update block status based on perspective
        if (current_relationship.user1_id == blocker_id) {
            current_relationship.user1_blocks_user2 = is_blocked;
            // If blocking, also unfollow
            if (is_blocked) {
                current_relationship.user1_follows_user2 = false;
                current_relationship.user2_follows_user1 = false; // Force mutual unfollow
            }
        } else {
            current_relationship.user2_blocks_user1 = is_blocked;
            // If blocking, also unfollow
            if (is_blocked) {
                current_relationship.user2_follows_user1 = false;
                current_relationship.user1_follows_user2 = false; // Force mutual unfollow
            }
        }
        
        // Update interaction tracking
        if (is_blocked) {
            current_relationship.relationship_strength = std::max(0.0, 
                current_relationship.relationship_strength - 0.5);
            current_relationship.last_interaction = system_clock::now();
        }
        
        bool success = create_or_update_relationship(current_relationship);
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        track_operation_performance("update_block_status", duration);
        
        spdlog::debug("✅ Block status updated: {} {} {} in {}μs", 
                     blocker_id, is_blocked ? "blocks" : "unblocks", blocked_id, duration);
        
        return success;
        
    } catch (const std::exception& e) {
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        spdlog::error("❌ Failed to update block status {} -> {}: {} ({}μs)", 
                     blocker_id, blocked_id, e.what(), duration);
        return false;
    }
}

bool RelationshipRepository::update_mute_status(const std::string& muter_id, 
                                              const std::string& muted_id, 
                                              bool is_muted) {
    auto start = high_resolution_clock::now();
    total_queries_++;
    
    try {
        auto connection = get_connection();
        
        // Get current relationship
        auto current_relationship = get_relationship(muter_id, muted_id);
        
        // Update mute status based on perspective
        if (current_relationship.user1_id == muter_id) {
            current_relationship.user1_mutes_user2 = is_muted;
        } else {
            current_relationship.user2_mutes_user1 = is_muted;
        }
        
        // Update interaction tracking
        if (is_muted) {
            current_relationship.last_interaction = system_clock::now();
        }
        
        bool success = create_or_update_relationship(current_relationship);
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        track_operation_performance("update_mute_status", duration);
        
        spdlog::debug("✅ Mute status updated: {} {} {} in {}μs", 
                     muter_id, is_muted ? "mutes" : "unmutes", muted_id, duration);
        
        return success;
        
    } catch (const std::exception& e) {
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        spdlog::error("❌ Failed to update mute status {} -> {}: {} ({}μs)", 
                     muter_id, muted_id, e.what(), duration);
        return false;
    }
}

// ========== BULK OPERATIONS ==========

std::vector<Relationship> RelationshipRepository::get_relationships_batch(
    const std::vector<std::pair<std::string, std::string>>& user_pairs) {
    
    auto start = high_resolution_clock::now();
    total_queries_++;
    
    std::vector<Relationship> relationships;
    
    try {
        if (user_pairs.empty()) {
            return relationships;
        }
        
        auto connection = get_connection();
        
        // Build query with multiple conditions
        std::string query = "SELECT * FROM relationships WHERE ";
        std::vector<std::string> conditions;
        
        for (const auto& pair : user_pairs) {
            conditions.push_back("((user1_id = ? AND user2_id = ?) OR (user1_id = ? AND user2_id = ?))");
        }
        
        query += "(" + std::accumulate(conditions.begin() + 1, conditions.end(), 
                                     conditions[0], [](const std::string& a, const std::string& b) {
                                         return a + " OR " + b;
                                     }) + ")";
        
        auto stmt = connection->prepare(query);
        
        // Bind parameters
        int param_index = 1;
        for (const auto& pair : user_pairs) {
            stmt->bind(param_index++, pair.first);
            stmt->bind(param_index++, pair.second);
            stmt->bind(param_index++, pair.second);
            stmt->bind(param_index++, pair.first);
        }
        
        auto result = stmt->execute();
        
        // Create map for quick lookup
        std::map<std::string, Relationship> relationship_map;
        
        while (result->next()) {
            Relationship rel;
            rel.user1_id = result->getString("user1_id");
            rel.user2_id = result->getString("user2_id");
            rel.user1_follows_user2 = result->getBoolean("user1_follows_user2");
            rel.user2_follows_user1 = result->getBoolean("user2_follows_user1");
            rel.user1_blocks_user2 = result->getBoolean("user1_blocks_user2");
            rel.user2_blocks_user1 = result->getBoolean("user2_blocks_user1");
            rel.user1_mutes_user2 = result->getBoolean("user1_mutes_user2");
            rel.user2_mutes_user1 = result->getBoolean("user2_mutes_user1");
            rel.interaction_count = result->getInt("interaction_count");
            rel.relationship_strength = result->getDouble("relationship_strength");
            rel.last_interaction = result->getTimestamp("last_interaction");
            rel.created_at = result->getTimestamp("created_at");
            rel.updated_at = result->getTimestamp("updated_at");
            
            // Store in map with normalized key
            std::string key = rel.user1_id < rel.user2_id ? 
                            rel.user1_id + ":" + rel.user2_id : 
                            rel.user2_id + ":" + rel.user1_id;
            relationship_map[key] = rel;
        }
        
        return_connection(connection);
        
        // Build result vector, creating empty relationships for missing pairs
        for (const auto& pair : user_pairs) {
            std::string key = pair.first < pair.second ? 
                            pair.first + ":" + pair.second : 
                            pair.second + ":" + pair.first;
            
            if (relationship_map.find(key) != relationship_map.end()) {
                auto rel = relationship_map[key];
                rel.normalize_perspective(pair.first, pair.second);
                relationships.push_back(rel);
            } else {
                relationships.push_back(Relationship(pair.first, pair.second));
            }
        }
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        track_operation_performance("get_relationships_batch", duration);
        
        spdlog::debug("✅ Batch relationships retrieved: {} pairs in {}μs", 
                     user_pairs.size(), duration);
        
        return relationships;
        
    } catch (const std::exception& e) {
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        spdlog::error("❌ Failed to get relationships batch: {} ({}μs)", e.what(), duration);
        return relationships;
    }
}

// ========== ANALYTICS & INSIGHTS ==========

json RelationshipRepository::get_user_relationship_stats(const std::string& user_id, int days) {
    auto start = high_resolution_clock::now();
    total_queries_++;
    
    try {
        auto connection = get_connection();
        
        std::string query = R"(
            SELECT 
                SUM(CASE WHEN user1_id = ? AND user1_follows_user2 = 1 THEN 1 
                         WHEN user2_id = ? AND user2_follows_user1 = 1 THEN 1 
                         ELSE 0 END) as following_count,
                SUM(CASE WHEN user1_id = ? AND user2_follows_user1 = 1 THEN 1 
                         WHEN user2_id = ? AND user1_follows_user2 = 1 THEN 1 
                         ELSE 0 END) as followers_count,
                SUM(CASE WHEN (user1_id = ? AND user1_follows_user2 = 1 AND user2_follows_user1 = 1)
                         OR   (user2_id = ? AND user2_follows_user1 = 1 AND user1_follows_user2 = 1)
                         THEN 1 ELSE 0 END) as mutual_follows_count,
                SUM(CASE WHEN user1_id = ? AND user1_blocks_user2 = 1 THEN 1 
                         WHEN user2_id = ? AND user2_blocks_user1 = 1 THEN 1 
                         ELSE 0 END) as blocked_count,
                SUM(CASE WHEN user1_id = ? AND user1_mutes_user2 = 1 THEN 1 
                         WHEN user2_id = ? AND user2_mutes_user1 = 1 THEN 1 
                         ELSE 0 END) as muted_count,
                AVG(CASE WHEN user1_id = ? OR user2_id = ? THEN relationship_strength 
                         ELSE NULL END) as avg_relationship_strength,
                COUNT(*) as total_relationships
            FROM relationships 
            WHERE (user1_id = ? OR user2_id = ?)
              AND updated_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
        )";
        
        auto stmt = connection->prepare(query);
        
        // Bind all the user_id parameters
        for (int i = 1; i <= 14; i++) {
            stmt->bind(i, user_id);
        }
        stmt->bind(15, days);
        
        auto result = stmt->execute();
        
        json stats = {
            {"user_id", user_id},
            {"period_days", days},
            {"following_count", 0},
            {"followers_count", 0},
            {"mutual_follows_count", 0},
            {"blocked_count", 0},
            {"muted_count", 0},
            {"avg_relationship_strength", 0.0},
            {"total_relationships", 0}
        };
        
        if (result->next()) {
            stats["following_count"] = result->getInt("following_count");
            stats["followers_count"] = result->getInt("followers_count");
            stats["mutual_follows_count"] = result->getInt("mutual_follows_count");
            stats["blocked_count"] = result->getInt("blocked_count");
            stats["muted_count"] = result->getInt("muted_count");
            stats["avg_relationship_strength"] = result->getDouble("avg_relationship_strength");
            stats["total_relationships"] = result->getInt("total_relationships");
        }
        
        return_connection(connection);
        
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        track_operation_performance("get_user_relationship_stats", duration);
        
        spdlog::debug("✅ Relationship stats retrieved for {} in {}μs", user_id, duration);
        
        return stats;
        
    } catch (const std::exception& e) {
        auto duration = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        spdlog::error("❌ Failed to get relationship stats for {}: {} ({}μs)", 
                     user_id, e.what(), duration);
        return json{{"error", e.what()}};
    }
}

// ========== CACHE MANAGEMENT ==========

std::optional<Relationship> RelationshipRepository::get_cached_relationship(
    const std::string& user1_id, const std::string& user2_id) {
    
    if (!enable_cache_ || !redis_client_) {
        return std::nullopt;
    }
    
    try {
        std::string cache_key = "relationship:" + 
                              (user1_id < user2_id ? user1_id + ":" + user2_id : user2_id + ":" + user1_id);
        
        auto cached_data = redis_client_->get(cache_key);
        if (cached_data.empty()) {
            return std::nullopt;
        }
        
        // Deserialize relationship from JSON
        auto relationship_json = json::parse(cached_data);
        Relationship relationship;
        relationship.from_json(relationship_json);
        relationship.normalize_perspective(user1_id, user2_id);
        
        return relationship;
        
    } catch (const std::exception& e) {
        spdlog::warn("⚠️ Cache retrieval failed for {} <-> {}: {}", user1_id, user2_id, e.what());
        return std::nullopt;
    }
}

void RelationshipRepository::cache_relationship(const std::string& user1_id, 
                                              const std::string& user2_id, 
                                              const Relationship& relationship) {
    
    if (!enable_cache_ || !redis_client_) {
        return;
    }
    
    try {
        std::string cache_key = "relationship:" + 
                              (user1_id < user2_id ? user1_id + ":" + user2_id : user2_id + ":" + user1_id);
        
        auto relationship_json = relationship.to_json();
        redis_client_->setex(cache_key, cache_ttl_seconds_, relationship_json.dump());
        
    } catch (const std::exception& e) {
        spdlog::warn("⚠️ Cache storage failed for {} <-> {}: {}", user1_id, user2_id, e.what());
    }
}

void RelationshipRepository::invalidate_relationship_cache(const std::string& user1_id, 
                                                         const std::string& user2_id) {
    
    if (!enable_cache_ || !redis_client_) {
        return;
    }
    
    try {
        std::string cache_key = "relationship:" + 
                              (user1_id < user2_id ? user1_id + ":" + user2_id : user2_id + ":" + user1_id);
        
        redis_client_->del(cache_key);
        
    } catch (const std::exception& e) {
        spdlog::warn("⚠️ Cache invalidation failed for {} <-> {}: {}", user1_id, user2_id, e.what());
    }
}

// ========== CONNECTION POOL MANAGEMENT ==========

void RelationshipRepository::initialize_connection_pool() {
    for (int i = 0; i < connection_pool_size_; i++) {
        auto connection = db_connection_->create_connection();
        available_connections_.push(connection);
    }
    spdlog::info("✅ Initialized connection pool with {} connections", connection_pool_size_);
}

void RelationshipRepository::cleanup_connection_pool() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    while (!available_connections_.empty()) {
        available_connections_.pop();
    }
    busy_connections_.clear();
}

std::shared_ptr<DatabaseConnection> RelationshipRepository::get_connection() {
    std::unique_lock<std::mutex> lock(connection_mutex_);
    
    // Wait for available connection
    connection_cv_.wait(lock, [this] { return !available_connections_.empty(); });
    
    auto connection = available_connections_.front();
    available_connections_.pop();
    busy_connections_.insert(connection);
    
    return connection;
}

void RelationshipRepository::return_connection(std::shared_ptr<DatabaseConnection> connection) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    
    busy_connections_.erase(connection);
    available_connections_.push(connection);
    
    connection_cv_.notify_one();
}

// ========== PERFORMANCE TRACKING ==========

void RelationshipRepository::track_operation_performance(const std::string& operation, int64_t duration_us) {
    std::lock_guard<std::mutex> lock(performance_mutex_);
    
    if (operation_times_.find(operation) == operation_times_.end()) {
        operation_times_[operation] = static_cast<double>(duration_us);
    } else {
        // Running average
        operation_times_[operation] = (operation_times_[operation] + duration_us) / 2.0;
    }
}

json RelationshipRepository::get_performance_metrics() const {
    std::lock_guard<std::mutex> lock(performance_mutex_);
    
    double cache_hit_rate = cache_hit_count_ + cache_miss_count_ > 0 ? 
                           static_cast<double>(cache_hit_count_) / (cache_hit_count_ + cache_miss_count_) : 0.0;
    
    json metrics = {
        {"repository_name", "relationship_repository"},
        {"total_queries", total_queries_.load()},
        {"cache_enabled", enable_cache_},
        {"cache_hit_count", cache_hit_count_.load()},
        {"cache_miss_count", cache_miss_count_.load()},
        {"cache_hit_rate", cache_hit_rate},
        {"connection_pool_size", connection_pool_size_},
        {"available_connections", available_connections_.size()},
        {"busy_connections", busy_connections_.size()},
        {"operation_metrics", json::object()}
    };
    
    for (const auto& [operation, avg_time] : operation_times_) {
        metrics["operation_metrics"][operation] = {
            {"avg_duration_us", avg_time}
        };
    }
    
    return metrics;
}

} // namespace sonet::follow::repositories
