/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "follow_repository.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <algorithm>
#include <sstream>
#include <thread>
#include <set>

namespace sonet::follow::repositories {

using namespace std::chrono;
using json = nlohmann::json;

// ========== CONSTRUCTOR & INITIALIZATION ==========

FollowRepository::FollowRepository(
    std::shared_ptr<void> db_primary,
    std::vector<std::shared_ptr<void>> db_replicas,
    std::shared_ptr<void> cache_client,
    const json& config
) : db_primary_(db_primary),
    db_replicas_(db_replicas),
    cache_client_(cache_client),
    config_(config),
    start_time_(system_clock::now()) {
    
    spdlog::info("🗄️ Initializing Twitter-Scale Follow Repository...");
    
    // Initialize performance tracking
    query_count_ = 0;
    cache_hits_ = 0;
    cache_misses_ = 0;
    avg_query_time_ = 0.0;
    
    operation_counts_.clear();
    operation_times_.clear();
    
    spdlog::info("✅ Follow Repository initialized with {} read replicas", db_replicas_.size());
}

// ========== CORE FOLLOW OPERATIONS ==========

std::future<models::Follow> FollowRepository::create_follow(
    const std::string& follower_id,
    const std::string& following_id,
    const std::string& follow_type
) {
    return std::async(std::launch::async, [this, follower_id, following_id, follow_type]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("📝 Creating follow: {} -> {} (type: {})", follower_id, following_id, follow_type);
            
            // Prepare follow object
            models::Follow follow;
            follow.follower_id = follower_id;
            follow.following_id = following_id;
            follow.follow_type = follow_type;
            follow.created_at = system_clock::now();
            follow.is_active = true;
            follow.interaction_count = 0;
            follow.last_interaction_at = follow.created_at;
            follow.follow_source = "api";
            
            // Simulate database insert with optimistic performance
            std::string query = R"(
                INSERT INTO follows (follower_id, following_id, follow_type, created_at, is_active, interaction_count, last_interaction_at, follow_source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (follower_id, following_id) 
                DO UPDATE SET 
                    follow_type = EXCLUDED.follow_type,
                    is_active = true,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING *
            )";
            
            json params = {
                {"follower_id", follower_id},
                {"following_id", following_id},
                {"follow_type", follow_type},
                {"created_at", duration_cast<milliseconds>(follow.created_at.time_since_epoch()).count()},
                {"is_active", true},
                {"interaction_count", 0},
                {"last_interaction_at", duration_cast<milliseconds>(follow.created_at.time_since_epoch()).count()},
                {"follow_source", "api"}
            };
            
            // Execute query (simulated)
            auto result = execute_query(query, params).get();
            
            // Invalidate relevant caches
            invalidate_user_cache(follower_id);
            invalidate_user_cache(following_id);
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("create_follow", duration);
            query_count_++;
            
            spdlog::debug("✅ Follow created: {} -> {} in {}μs", follower_id, following_id, duration);
            
            return follow;
            
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            spdlog::error("❌ Create follow failed: {} -> {} - {} ({}μs)", 
                         follower_id, following_id, e.what(), duration);
            throw;
        }
    });
}

std::future<bool> FollowRepository::remove_follow(
    const std::string& follower_id,
    const std::string& following_id
) {
    return std::async(std::launch::async, [this, follower_id, following_id]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("🗑️ Removing follow: {} -> {}", follower_id, following_id);
            
            // Use soft delete for analytics
            std::string query = R"(
                UPDATE follows 
                SET is_active = false, 
                    updated_at = CURRENT_TIMESTAMP,
                    deleted_at = CURRENT_TIMESTAMP
                WHERE follower_id = $1 AND following_id = $2 AND is_active = true
            )";
            
            json params = {
                {"follower_id", follower_id},
                {"following_id", following_id}
            };
            
            auto result = execute_query(query, params).get();
            
            // Check if any rows were affected
            bool success = result.value("rows_affected", 0) > 0;
            
            if (success) {
                // Invalidate caches
                invalidate_user_cache(follower_id);
                invalidate_user_cache(following_id);
            }
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("remove_follow", duration);
            query_count_++;
            
            spdlog::debug("✅ Follow removal {}: {} -> {} in {}μs", 
                         success ? "successful" : "failed", follower_id, following_id, duration);
            
            return success;
            
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            spdlog::error("❌ Remove follow failed: {} -> {} - {} ({}μs)", 
                         follower_id, following_id, e.what(), duration);
            return false;
        }
    });
}

std::future<bool> FollowRepository::is_following(
    const std::string& follower_id,
    const std::string& following_id
) {
    return std::async(std::launch::async, [this, follower_id, following_id]() {
        auto start = high_resolution_clock::now();
        try {
            // Deterministic: skip simulated cache; perform simple existence check via execute_query
            std::string query = R"(
                SELECT EXISTS(
                    SELECT 1 FROM follows 
                    WHERE follower_id = $1 AND following_id = $2 AND is_active = true
                ) as exists
            )";
            json params = {{"follower_id", follower_id}, {"following_id", following_id}};
            auto result = execute_query(query, params).get();
            bool exists = result.value("exists", false);
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            track_operation_performance("is_following", duration);
            spdlog::debug("✅ Follow check (db): {} -> {} = {} ({}μs)", follower_id, following_id, exists, duration);
            return exists;
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            spdlog::error("❌ Follow check failed: {} -> {} - {} ({}μs)", follower_id, following_id, e.what(), duration);
            return false;
        }
    });
}

std::future<std::optional<models::Follow>> FollowRepository::get_follow(
    const std::string& follower_id,
    const std::string& following_id
) {
    return std::async(std::launch::async, [this, follower_id, following_id]() -> std::optional<models::Follow> {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("🔍 Getting follow details: {} -> {}", follower_id, following_id);
            
            std::string query = R"(
                SELECT follower_id, following_id, follow_type, created_at, updated_at,
                       is_active, interaction_count, last_interaction_at, follow_source,
                       engagement_score, privacy_level
                FROM follows 
                WHERE follower_id = $1 AND following_id = $2 AND is_active = true
            )";
            
            json params = {
                {"follower_id", follower_id},
                {"following_id", following_id}
            };
            
            auto result = execute_query(query, params).get();
            
            if (result.contains("rows") && result["rows"].is_array() && !result["rows"].empty()) {
                auto row = result["rows"][0];
                
                models::Follow follow;
                follow.follower_id = row.value("follower_id", "");
                follow.following_id = row.value("following_id", "");
                follow.follow_type = row.value("follow_type", "standard");
                follow.created_at = system_clock::from_time_t(row.value("created_at", 0) / 1000);
                follow.updated_at = system_clock::from_time_t(row.value("updated_at", 0) / 1000);
                follow.is_active = row.value("is_active", true);
                follow.interaction_count = row.value("interaction_count", 0);
                follow.last_interaction_at = system_clock::from_time_t(row.value("last_interaction_at", 0) / 1000);
                follow.follow_source = row.value("follow_source", "api");
                follow.engagement_score = row.value("engagement_score", 0.0);
                follow.privacy_level = row.value("privacy_level", "public");
                
                auto end = high_resolution_clock::now();
                auto duration = duration_cast<microseconds>(end - start).count();
                
                track_operation_performance("get_follow", duration);
                query_count_++;
                
                spdlog::debug("✅ Follow details retrieved: {} -> {} in {}μs", 
                             follower_id, following_id, duration);
                
                return follow;
            }
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("get_follow", duration);
            query_count_++;
            
            spdlog::debug("❌ Follow not found: {} -> {} ({}μs)", follower_id, following_id, duration);
            
            return std::nullopt;
            
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            spdlog::error("❌ Get follow failed: {} -> {} - {} ({}μs)", 
                         follower_id, following_id, e.what(), duration);
            return std::nullopt;
        }
    });
}

// ========== RELATIONSHIP MANAGEMENT ==========

std::future<models::Relationship> FollowRepository::get_relationship(
    const std::string& user1_id,
    const std::string& user2_id
) {
    return std::async(std::launch::async, [this, user1_id, user2_id]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("🔗 Getting relationship: {} <-> {}", user1_id, user2_id);
            
            // Query both directions and blocking/muting status
            std::string query = R"(
                WITH relationship_data AS (
                    SELECT 
                        CASE WHEN follower_id = $1 AND following_id = $2 THEN 'user1_follows_user2'
                             WHEN follower_id = $2 AND following_id = $1 THEN 'user2_follows_user1'
                        END as relationship_type,
                        interaction_count,
                        last_interaction_at,
                        engagement_score
                    FROM follows 
                    WHERE ((follower_id = $1 AND following_id = $2) OR (follower_id = $2 AND following_id = $1))
                      AND is_active = true
                ),
                blocking_data AS (
                    SELECT 
                        CASE WHEN blocker_id = $1 AND blocked_id = $2 THEN 'user1_blocked_user2'
                             WHEN blocker_id = $2 AND blocked_id = $1 THEN 'user2_blocked_user1'
                        END as block_type
                    FROM user_blocks 
                    WHERE ((blocker_id = $1 AND blocked_id = $2) OR (blocker_id = $2 AND blocked_id = $1))
                      AND is_active = true
                ),
                muting_data AS (
                    SELECT 
                        CASE WHEN muter_id = $1 AND muted_id = $2 THEN 'user1_muted_user2'
                             WHEN muter_id = $2 AND muted_id = $1 THEN 'user2_muted_user1'
                        END as mute_type
                    FROM user_mutes 
                    WHERE ((muter_id = $1 AND muted_id = $2) OR (muter_id = $2 AND muted_id = $1))
                      AND is_active = true
                )
                SELECT * FROM relationship_data
                UNION ALL
                SELECT block_type as relationship_type, 0 as interaction_count, 
                       CURRENT_TIMESTAMP as last_interaction_at, 0.0 as engagement_score
                FROM blocking_data
                UNION ALL
                SELECT mute_type as relationship_type, 0 as interaction_count,
                       CURRENT_TIMESTAMP as last_interaction_at, 0.0 as engagement_score  
                FROM muting_data
            )";
            
            json params = {
                {"user1_id", user1_id},
                {"user2_id", user2_id}
            };
            
            auto result = execute_query(query, params).get();
            
            // Build relationship object
            models::Relationship relationship;
            relationship.user1_id = user1_id;
            relationship.user2_id = user2_id;
            relationship.user1_follows_user2 = false;
            relationship.user2_follows_user1 = false;
            relationship.user1_blocked_user2 = false;
            relationship.user2_blocked_user1 = false;
            relationship.user1_muted_user2 = false;
            relationship.user2_muted_user1 = false;
            relationship.created_at = system_clock::now();
            relationship.last_interaction_at = system_clock::now();
            
            if (result.contains("rows") && result["rows"].is_array()) {
                for (const auto& row : result["rows"]) {
                    std::string rel_type = row.value("relationship_type", "");
                    
                    if (rel_type == "user1_follows_user2") {
                        relationship.user1_follows_user2 = true;
                        relationship.user1_interaction_count = row.value("interaction_count", 0);
                    } else if (rel_type == "user2_follows_user1") {
                        relationship.user2_follows_user1 = true;
                        relationship.user2_interaction_count = row.value("interaction_count", 0);
                    } else if (rel_type == "user1_blocked_user2") {
                        relationship.user1_blocked_user2 = true;
                    } else if (rel_type == "user2_blocked_user1") {
                        relationship.user2_blocked_user1 = true;
                    } else if (rel_type == "user1_muted_user2") {
                        relationship.user1_muted_user2 = true;
                    } else if (rel_type == "user2_muted_user1") {
                        relationship.user2_muted_user1 = true;
                    }
                }
            }
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("get_relationship", duration);
            query_count_++;
            
            spdlog::debug("✅ Relationship retrieved: {} <-> {} in {}μs", user1_id, user2_id, duration);
            
            return relationship;
            
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            spdlog::error("❌ Get relationship failed: {} <-> {} - {} ({}μs)", 
                         user1_id, user2_id, e.what(), duration);
            
            // Return default relationship
            models::Relationship relationship;
            relationship.user1_id = user1_id;
            relationship.user2_id = user2_id;
            return relationship;
        }
    });
}

// ========== FOLLOWER/FOLLOWING LISTS ==========

std::future<json> FollowRepository::get_followers(
    const std::string& user_id,
    int limit,
    const std::string& cursor,
    const std::string& requester_id
) {
    return std::async(std::launch::async, [this, user_id, limit, cursor, requester_id]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("📋 Getting followers for {}: limit={}, cursor={}", user_id, limit, cursor);
            
            // Build query with cursor pagination
            std::ostringstream query_stream;
            query_stream << R"(
                SELECT f.follower_id, f.created_at, f.follow_type, f.interaction_count,
                       u.username, u.display_name, u.avatar_url, u.verified,
                       u.follower_count, u.following_count
                FROM follows f
                JOIN users u ON f.follower_id = u.user_id
                WHERE f.following_id = $1 AND f.is_active = true
            )";
            
            json params = {{"following_id", user_id}};
            int param_count = 1;
            
            // Add cursor condition
            if (!cursor.empty()) {
                query_stream << " AND f.created_at < $" << (++param_count);
                params["cursor_time"] = cursor;
            }
            
            // Add privacy filtering (simplified)
            if (requester_id != user_id) {
                query_stream << " AND u.privacy_level = 'public'";
            }
            
            query_stream << " ORDER BY f.created_at DESC LIMIT $" << (++param_count);
            params["limit"] = limit + 1; // Get one extra to determine if there are more
            
            auto result = execute_query(query_stream.str(), params).get();
            
            json followers_result = {
                {"user_id", user_id},
                {"count", 0},
                {"has_more", false},
                {"next_cursor", ""},
                {"followers", json::array()}
            };
            
            if (result.contains("rows") && result["rows"].is_array()) {
                auto rows = result["rows"];
                int returned_count = static_cast<int>(rows.size());
                
                // Check if there are more results
                if (returned_count > limit) {
                    followers_result["has_more"] = true;
                    rows.erase(rows.end() - 1); // Remove extra row
                    returned_count--;
                    
                    // Set next cursor to the last follow's created_at
                    if (!rows.empty()) {
                        followers_result["next_cursor"] = rows.back().value("created_at", "");
                    }
                }
                
                followers_result["count"] = returned_count;
                
                // Build followers array
                for (const auto& row : rows) {
                    json follower = {
                        {"user_id", row.value("follower_id", "")},
                        {"username", row.value("username", "")},
                        {"display_name", row.value("display_name", "")},
                        {"avatar_url", row.value("avatar_url", "")},
                        {"verified", row.value("verified", false)},
                        {"follower_count", row.value("follower_count", 0)},
                        {"following_count", row.value("following_count", 0)},
                        {"follow_type", row.value("follow_type", "standard")},
                        {"followed_at", row.value("created_at", "")},
                        {"interaction_count", row.value("interaction_count", 0)}
                    };
                    followers_result["followers"].push_back(follower);
                }
            }
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("get_followers", duration);
            query_count_++;
            
            spdlog::debug("✅ Followers retrieved for {}: {} results in {}μs", 
                         user_id, followers_result.value("count", 0), duration);
            
            return followers_result;
            
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            spdlog::error("❌ Get followers failed for {}: {} ({}μs)", user_id, e.what(), duration);
            
            return json{
                {"user_id", user_id},
                {"count", 0},
                {"has_more", false},
                {"next_cursor", ""},
                {"followers", json::array()},
                {"error", e.what()}
            };
        }
    });
}

// ========== BULK OPERATIONS ==========

std::future<json> FollowRepository::bulk_follow(
    const std::string& follower_id,
    const std::vector<std::string>& following_ids,
    const std::string& follow_type
) {
    return std::async(std::launch::async, [this, follower_id, following_ids, follow_type]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::info("📦 Bulk follow: {} -> {} users", follower_id, following_ids.size());
            
            json bulk_result = {
                {"follower_id", follower_id},
                {"total_requested", following_ids.size()},
                {"successful", 0},
                {"failed", 0},
                {"results", json::array()}
            };
            
            // Build batch insert query
            std::ostringstream query_stream;
            query_stream << R"(
                INSERT INTO follows (follower_id, following_id, follow_type, created_at, is_active, interaction_count, last_interaction_at, follow_source)
                VALUES 
            )";
            
            json params;
            auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            
            for (size_t i = 0; i < following_ids.size(); ++i) {
                if (i > 0) query_stream << ", ";
                query_stream << "($" << (i * 8 + 1) << ", $" << (i * 8 + 2) << ", $" << (i * 8 + 3) 
                           << ", $" << (i * 8 + 4) << ", $" << (i * 8 + 5) << ", $" << (i * 8 + 6) 
                           << ", $" << (i * 8 + 7) << ", $" << (i * 8 + 8) << ")";
                
                params["follower_id_" + std::to_string(i)] = follower_id;
                params["following_id_" + std::to_string(i)] = following_ids[i];
                params["follow_type_" + std::to_string(i)] = follow_type;
                params["created_at_" + std::to_string(i)] = now;
                params["is_active_" + std::to_string(i)] = true;
                params["interaction_count_" + std::to_string(i)] = 0;
                params["last_interaction_at_" + std::to_string(i)] = now;
                params["follow_source_" + std::to_string(i)] = "bulk_api";
            }
            
            query_stream << R"(
                ON CONFLICT (follower_id, following_id) 
                DO UPDATE SET 
                    follow_type = EXCLUDED.follow_type,
                    is_active = true,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING follower_id, following_id, created_at
            )";
            
            auto result = execute_query(query_stream.str(), params).get();
            
            // Process results
            int successful = 0;
            int failed = 0;
            
            std::set<std::string> successful_follows;
            if (result.contains("rows") && result["rows"].is_array()) {
                for (const auto& row : result["rows"]) {
                    successful_follows.insert(row.value("following_id", ""));
                }
                successful = successful_follows.size();
            }
            
            // Build detailed results
            for (const auto& following_id : following_ids) {
                bool success = successful_follows.count(following_id) > 0;
                
                json follow_result = {
                    {"following_id", following_id},
                    {"success", success},
                    {"follow_type", follow_type}
                };
                
                if (!success) {
                    follow_result["error"] = "Failed to create follow relationship";
                    failed++;
                }
                
                bulk_result["results"].push_back(follow_result);
            }
            
            bulk_result["successful"] = successful;
            bulk_result["failed"] = failed;
            
            // Invalidate caches for all affected users
            invalidate_user_cache(follower_id);
            for (const auto& following_id : following_ids) {
                invalidate_user_cache(following_id);
            }
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("bulk_follow", duration);
            query_count_++;
            
            spdlog::info("✅ Bulk follow completed: {}/{} successful in {}μs", 
                        successful, following_ids.size(), duration);
            
            return bulk_result;
            
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            spdlog::error("❌ Bulk follow failed: {} - {} ({}μs)", follower_id, e.what(), duration);
            
            return json{
                {"follower_id", follower_id},
                {"total_requested", following_ids.size()},
                {"successful", 0},
                {"failed", static_cast<int>(following_ids.size())},
                {"error", e.what()}
            };
        }
    });
}

// ========== ANALYTICS & METRICS ==========

std::future<int64_t> FollowRepository::get_follower_count(const std::string& user_id, bool /*use_cache*/) {
    return std::async(std::launch::async, [this, user_id]() {
        auto start = high_resolution_clock::now();
        try {
            std::string query = R"(
                SELECT COUNT(*) as follower_count
                FROM follows 
                WHERE following_id = $1 AND is_active = true
            )";
            json params = {{"following_id", user_id}};
            auto result = execute_query(query, params).get();
            int64_t count = 0;
            if (result.contains("rows") && result["rows"].is_array() && !result["rows"].empty()) {
                count = result["rows"][0].value("follower_count", 0);
            }
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            track_operation_performance("get_follower_count", duration);
            spdlog::debug("✅ Follower count (db): {} = {} ({}μs)", user_id, count, duration);
            return count;
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            spdlog::error("❌ Get follower count failed for {}: {} ({}μs)", user_id, e.what(), duration);
            return static_cast<int64_t>(0);
        }
    });
}

std::future<json> FollowRepository::get_follower_analytics(const std::string& user_id, int days) {
    return std::async(std::launch::async, [this, user_id, days]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("📊 Getting follower analytics for {}: {} days", user_id, days);
            
            // Complex analytics query
            std::string query = R"(
                WITH daily_stats AS (
                    SELECT 
                        DATE(created_at) as follow_date,
                        COUNT(*) as new_followers,
                        AVG(interaction_count) as avg_interaction
                    FROM follows 
                    WHERE following_id = $1 
                      AND is_active = true 
                      AND created_at >= CURRENT_DATE - INTERVAL '%d days'
                    GROUP BY DATE(created_at)
                ),
                total_stats AS (
                    SELECT 
                        COUNT(*) as total_followers,
                        AVG(interaction_count) as avg_total_interaction,
                        COUNT(DISTINCT follower_id) as unique_followers
                    FROM follows 
                    WHERE following_id = $1 AND is_active = true
                ),
                demographics AS (
                    SELECT 
                        u.country,
                        COUNT(*) as follower_count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                    FROM follows f
                    JOIN users u ON f.follower_id = u.user_id
                    WHERE f.following_id = $1 AND f.is_active = true
                    GROUP BY u.country
                    ORDER BY follower_count DESC
                    LIMIT 10
                )
                SELECT 
                    (SELECT json_agg(daily_stats) FROM daily_stats) as daily_growth,
                    (SELECT row_to_json(total_stats) FROM total_stats) as totals,
                    (SELECT json_agg(demographics) FROM demographics) as top_countries
            )";
            
            // Replace placeholder with actual days
            std::string formatted_query = query;
            size_t pos = formatted_query.find("%d");
            if (pos != std::string::npos) {
                formatted_query.replace(pos, 2, std::to_string(days));
            }
            
            json params = {{"user_id", user_id}};
            
            auto result = execute_query(formatted_query, params).get();
            
            // Build analytics response
            json analytics = {
                {"user_id", user_id},
                {"analysis_period_days", days},
                {"generated_at", duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()},
                {"daily_growth", json::array()},
                {"total_metrics", json::object()},
                {"demographics", json::object()}
            };
            
            if (result.contains("rows") && result["rows"].is_array() && !result["rows"].empty()) {
                auto row = result["rows"][0];
                
                if (row.contains("daily_growth") && row["daily_growth"].is_array()) {
                    analytics["daily_growth"] = row["daily_growth"];
                }
                
                if (row.contains("totals") && row["totals"].is_object()) {
                    analytics["total_metrics"] = row["totals"];
                }
                
                if (row.contains("top_countries") && row["top_countries"].is_array()) {
                    analytics["demographics"]["top_countries"] = row["top_countries"];
                }
            }
            
            // Add computed metrics
            if (analytics["daily_growth"].is_array() && !analytics["daily_growth"].empty()) {
                auto daily_data = analytics["daily_growth"];
                int total_new = 0;
                for (const auto& day : daily_data) {
                    total_new += day.value("new_followers", 0);
                }
                
                analytics["computed_metrics"] = {
                    {"total_new_followers_period", total_new},
                    {"avg_daily_growth", total_new / static_cast<double>(days)},
                    {"growth_rate_percentage", (total_new / static_cast<double>(analytics["total_metrics"].value("total_followers", 1))) * 100}
                };
            }
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("get_follower_analytics", duration);
            query_count_++;
            
            spdlog::debug("✅ Analytics retrieved for {} in {}μs", user_id, duration);
            
            return analytics;
            
        } catch (const std::exception& e) {
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            spdlog::error("❌ Get analytics failed for {}: {} ({}μs)", user_id, e.what(), duration);
            
            return json{
                {"user_id", user_id},
                {"error", e.what()},
                {"analysis_period_days", days}
            };
        }
    });
}

// ========== HELPER METHODS ==========

std::future<json> FollowRepository::execute_query(const std::string& /*query*/, const json& /*params*/) {
    return std::async(std::launch::async, [this]() {
        auto start = high_resolution_clock::now();
        try {
            // Deterministic stubbed result
            json result = {
                {"success", true},
                {"query_time_us", 0},
                {"rows_affected", 0},
                {"rows", json::array()}
            };
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            avg_query_time_ = (avg_query_time_ + duration) / 2.0;
            return result;
        } catch (const std::exception& e) {
            spdlog::error("❌ Query execution failed: {}", e.what());
            throw;
        }
    });
}

void FollowRepository::track_operation_performance(const std::string& operation, int64_t duration_us) {
    operation_counts_[operation]++;
    
    if (operation_times_.find(operation) == operation_times_.end()) {
        operation_times_[operation] = static_cast<double>(duration_us);
    } else {
        operation_times_[operation] = (operation_times_[operation] + duration_us) / 2.0;
    }
}

std::future<bool> FollowRepository::invalidate_user_cache(const std::string& user_id) {
    return std::async(std::launch::async, [user_id]() {
        try {
            std::vector<std::string> cache_keys = {
                "follower_count:" + user_id,
                "following_count:" + user_id,
                "followers:" + user_id,
                "following:" + user_id,
                "social_metrics:" + user_id
            };
            (void)cache_keys; // deterministic stub; no-op
            return true;
        } catch (const std::exception& e) {
            spdlog::warn("⚠️ Cache invalidation failed for {}: {}", user_id, e.what());
            return false;
        }
    });
}

json FollowRepository::get_health_status() const {
    auto uptime = duration_cast<seconds>(system_clock::now() - start_time_).count();
    
    double cache_hit_rate = (cache_hits_ + cache_misses_) > 0 ? 
        static_cast<double>(cache_hits_) / (cache_hits_ + cache_misses_) : 0.0;
    
    return {
        {"repository_name", "follow_repository"},
        {"status", "healthy"},
        {"uptime_seconds", uptime},
        {"total_queries", query_count_.load()},
        {"avg_query_time_us", avg_query_time_.load()},
        {"cache_hit_rate", cache_hit_rate},
        {"cache_hits", cache_hits_.load()},
        {"cache_misses", cache_misses_.load()},
        {"primary_db_status", "connected"},
        {"replica_count", db_replicas_.size()},
        {"cache_status", cache_client_ ? "connected" : "disconnected"}
    };
}

json FollowRepository::get_performance_metrics() const {
    json metrics = {
        {"repository_performance", "follow_repository"},
        {"total_operations", 0},
        {"operation_breakdown", json::object()}
    };
    
    uint64_t total_ops = 0;
    for (const auto& [operation, count] : operation_counts_) {
        total_ops += count;
        metrics["operation_breakdown"][operation] = {
            {"count", count},
            {"avg_duration_us", operation_times_.at(operation)}
        };
    }
    
    metrics["total_operations"] = total_ops;
    
    return metrics;
}

} // namespace sonet::follow::repositories
