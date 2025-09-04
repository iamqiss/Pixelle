/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "service.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <algorithm>
#include <random>
#include <set>
#include <unordered_set>

namespace sonet::follow {

using namespace std::chrono;
using json = nlohmann::json;

// ========== CONSTRUCTOR & INITIALIZATION ==========

FollowService::FollowService(
    std::shared_ptr<repositories::FollowRepository> repository,
    std::shared_ptr<graph::SocialGraph> social_graph,
    std::shared_ptr<void> cache_client,
    const json& config
) : repository_(repository), 
    social_graph_(social_graph), 
    cache_client_(cache_client),
    config_(config),
    start_time_(steady_clock::now()) {
    
    spdlog::info("🚀 Initializing Twitter-Scale Follow Service...");
    
    // Initialize performance tracking
    operation_counts_.clear();
    operation_times_.clear();
    
    // Load configuration
    max_following_limit_ = config_.value("max_following_limit", 7500); // Twitter-like limit
    max_followers_per_request_ = config_.value("max_followers_per_request", 1000);
    cache_ttl_seconds_ = config_.value("cache_ttl_seconds", 300);
    
    spdlog::info("✅ Follow Service initialized with config: max_following={}, cache_ttl={}s", 
                max_following_limit_, cache_ttl_seconds_);
}

// Add two-arg overload for follow_user to delegate to extended version
json FollowService::follow_user(const std::string& follower_id, const std::string& following_id) {
    return follow_user(follower_id, following_id, "standard", "api");
}

// ========== CORE FOLLOW OPERATIONS ==========

json FollowService::follow_user(const std::string& follower_id, const std::string& following_id, 
                                const std::string& follow_type, const std::string& source) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("👤 Processing follow: {} -> {} (type: {}, source: {})", 
                     follower_id, following_id, follow_type, source);
        
        // Validation
        if (follower_id == following_id) {
            return create_error_response("SELF_FOLLOW", "Cannot follow yourself");
        }
        
        if (follower_id.empty() || following_id.empty()) {
            return create_error_response("INVALID_INPUT", "User IDs cannot be empty");
        }
        
        // Check if already following
        auto existing_follow = repository_->get_follow(follower_id, following_id).get();
        if (existing_follow) {
                    json data = json::object();
        data["already_following"] = true;
        data["follow_date"] = std::chrono::duration_cast<std::chrono::milliseconds>(existing_follow->created_at.time_since_epoch()).count();
        data["follow_type"] = existing_follow->follow_type;
        return create_success_response("ALREADY_FOLLOWING", data);
        }
        
        // Check following limit
        auto following_count = repository_->get_following_count(follower_id, true).get();
        if (following_count >= max_following_limit_) {
            return create_error_response("FOLLOWING_LIMIT_EXCEEDED", 
                                       "Maximum following limit reached");
        }
        
        // Check if blocked
        if (is_blocked(following_id, follower_id)) {
            return create_error_response("USER_BLOCKED", "Cannot follow blocked user");
        }
        
        // Create follow relationship
        auto follow = repository_->create_follow(follower_id, following_id, follow_type).get();
        
        // Update social graph
        social_graph_->add_follow_relationship(follower_id, following_id);
        
        // Invalidate relevant caches
        invalidate_user_caches(follower_id);
        invalidate_user_caches(following_id);
        
        // Record analytics
        record_follow_event(follower_id, following_id, "follow", source);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("follow_user", duration);
        
        spdlog::info("✅ Follow successful: {} -> {} in {}μs", follower_id, following_id, duration);
        
        json data = json::object();
        data["follower_id"] = follower_id;
        data["following_id"] = following_id;
        data["follow_type"] = follow_type;
        data["follow_date"] = std::chrono::duration_cast<std::chrono::milliseconds>(follow.created_at.time_since_epoch()).count();
        data["source"] = source;
        data["processing_time_us"] = duration;
        return create_success_response("FOLLOW_SUCCESS", data);
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Follow failed: {} -> {} - {} ({}μs)", 
                     follower_id, following_id, e.what(), duration);
        
        return create_error_response("FOLLOW_FAILED", e.what());
    }
}

json FollowService::unfollow_user(const std::string& follower_id, const std::string& following_id) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("👤 Processing unfollow: {} -> {}", follower_id, following_id);
        
        // Validation
        if (follower_id == following_id) {
            return create_error_response("SELF_UNFOLLOW", "Cannot unfollow yourself");
        }
        
        // Check if currently following
        if (!repository_->is_following(follower_id, following_id).get()) {
            return create_success_response("NOT_FOLLOWING", {
                {"was_following", false},
                {"message", "User was not being followed"}
            });
        }
        
        // Remove follow relationship
        bool success = repository_->remove_follow(follower_id, following_id).get();
        
        if (success) {
            // Update social graph
            social_graph_->remove_follow_relationship(follower_id, following_id);
            
            // Invalidate caches
            invalidate_user_caches(follower_id);
            invalidate_user_caches(following_id);
            
            // Record analytics
            record_follow_event(follower_id, following_id, "unfollow", "manual");
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("unfollow_user", duration);
        
        spdlog::info("✅ Unfollow {}: {} -> {} in {}μs", 
                    success ? "successful" : "failed", follower_id, following_id, duration);
        
        return create_success_response(success ? "UNFOLLOW_SUCCESS" : "UNFOLLOW_FAILED", {
            {"follower_id", follower_id},
            {"following_id", following_id},
            {"success", success},
            {"processing_time_us", duration}
        });
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Unfollow failed: {} -> {} - {} ({}μs)", 
                     follower_id, following_id, e.what(), duration);
        
        return create_error_response("UNFOLLOW_FAILED", e.what());
    }
}

bool FollowService::is_following(const std::string& follower_id, const std::string& following_id) {
    auto start = high_resolution_clock::now();
    
    try {
        // Check cache first
        std::string cache_key = "following:" + follower_id + ":" + following_id;
        
        // Try to get from cache (simulated - would use actual cache client)
        // For now, go directly to repository
        bool result = repository_->is_following(follower_id, following_id).get();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("is_following", duration);
        
        spdlog::debug("🔍 Following check: {} -> {} = {} ({}μs)", 
                     follower_id, following_id, result, duration);
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Following check failed: {} -> {} - {}", 
                     follower_id, following_id, e.what());
        return false;
    }
}

json FollowService::get_relationship(const std::string& user1_id, const std::string& user2_id) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🔍 Getting relationship: {} <-> {}", user1_id, user2_id);
        
        // Get relationship from repository
        auto relationship = repository_->get_relationship(user1_id, user2_id).get();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_relationship", duration);
        
        json result = json::object();
        result["user1_id"] = user1_id;
        result["user2_id"] = user2_id;
        result["user1_follows_user2"] = relationship.user1_follows_user2;
        result["user2_follows_user1"] = relationship.user2_follows_user1;
        result["are_mutual_friends"] = relationship.are_mutual_friends();
        result["user1_blocked_user2"] = relationship.user1_blocked_user2;
        result["user2_blocked_user1"] = relationship.user2_blocked_user1;
        result["user1_muted_user2"] = relationship.user1_muted_user2;
        result["user2_muted_user1"] = relationship.user2_muted_user1;
        result["relationship_strength"] = relationship.calculate_strength();
        result["last_interaction"] = std::chrono::duration_cast<std::chrono::milliseconds>(relationship.last_interaction_at.time_since_epoch()).count();
        result["processing_time_us"] = duration;
        
        spdlog::debug("✅ Relationship retrieved: {} <-> {} in {}μs", user1_id, user2_id, duration);
        
        return result;
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get relationship failed: {} <-> {} - {} ({}μs)", 
                     user1_id, user2_id, e.what(), duration);
        
        return create_error_response("GET_RELATIONSHIP_FAILED", e.what());
    }
}

// ========== FOLLOWER/FOLLOWING LISTS ==========

json FollowService::get_followers(const std::string& user_id, int limit, const std::string& cursor, 
                                 const std::string& requester_id) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📋 Getting followers for user: {} (limit: {}, requester: {})", 
                     user_id, limit, requester_id);
        
        // Validate limit
        limit = std::min(limit, max_followers_per_request_);
        
        // Get followers from repository
        auto followers_data = repository_->get_followers(user_id, limit, cursor, requester_id).get();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_followers", duration);
        
        // Add performance metrics
        followers_data["processing_time_us"] = duration;
        followers_data["cache_hit"] = false; // Would be determined by cache layer
        
        spdlog::debug("✅ Followers retrieved for {}: {} results in {}μs", 
                     user_id, followers_data.value("count", 0), duration);
        
        return followers_data;
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get followers failed for {}: {} ({}μs)", user_id, e.what(), duration);
        
        return create_error_response("GET_FOLLOWERS_FAILED", e.what());
    }
}

json FollowService::get_following(const std::string& user_id, int limit, const std::string& cursor,
                                 const std::string& requester_id) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📋 Getting following for user: {} (limit: {}, requester: {})", 
                     user_id, limit, requester_id);
        
        // Validate limit
        limit = std::min(limit, max_followers_per_request_);
        
        // Get following from repository
        auto following_data = repository_->get_following(user_id, limit, cursor, requester_id).get();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_following", duration);
        
        // Add performance metrics
        following_data["processing_time_us"] = duration;
        following_data["cache_hit"] = false; // Would be determined by cache layer
        
        spdlog::debug("✅ Following retrieved for {}: {} results in {}μs", 
                     user_id, following_data.value("count", 0), duration);
        
        return following_data;
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get following failed for {}: {} ({}μs)", user_id, e.what(), duration);
        
        return create_error_response("GET_FOLLOWING_FAILED", e.what());
    }
}

std::vector<std::string> FollowService::get_mutual_friends(const std::string& user1_id, 
                                                           const std::string& user2_id, int limit) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("👥 Getting mutual friends: {} <-> {} (limit: {})", user1_id, user2_id, limit);
        
        // Get mutual followers from repository
        auto mutual_followers = repository_->get_mutual_followers(user1_id, user2_id, limit).get();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_mutual_friends", duration);
        
        spdlog::debug("✅ Mutual friends found: {} <-> {} = {} results in {}μs", 
                     user1_id, user2_id, mutual_followers.size(), duration);
        
        return mutual_followers;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Get mutual friends failed: {} <-> {} - {}", user1_id, user2_id, e.what());
        return {};
    }
}

bool FollowService::are_mutual_friends(const std::string& user1_id, const std::string& user2_id) {
    auto start = high_resolution_clock::now();
    
    try {
        bool user1_follows_user2 = is_following(user1_id, user2_id);
        bool user2_follows_user1 = is_following(user2_id, user1_id);
        
        bool result = user1_follows_user2 && user2_follows_user1;
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("are_mutual_friends", duration);
        
        spdlog::debug("👥 Mutual friends check: {} <-> {} = {} ({}μs)", 
                     user1_id, user2_id, result, duration);
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Mutual friends check failed: {} <-> {} - {}", user1_id, user2_id, e.what());
        return false;
    }
}

// ========== BULK OPERATIONS ==========

json FollowService::bulk_follow(const std::string& follower_id, 
                               const std::vector<std::string>& following_ids,
                               const std::string& follow_type) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::info("📦 Processing bulk follow: {} -> {} users (type: {})", 
                    follower_id, following_ids.size(), follow_type);
        
        // Validate bulk size
        if (following_ids.size() > 100) {
            return create_error_response("BULK_SIZE_EXCEEDED", "Maximum 100 users per bulk operation");
        }
        
        // Use repository bulk operation
        auto bulk_result = repository_->bulk_follow(follower_id, following_ids, follow_type).get();
        
        // Update social graph for successful follows
        for (const auto& following_id : following_ids) {
            // Check if this follow was successful in bulk result
            if (bulk_result.contains("results") && bulk_result["results"].is_array()) {
                for (const auto& result : bulk_result["results"]) {
                    if (result["following_id"] == following_id && result["success"] == true) {
                        social_graph_->add_follow_relationship(follower_id, following_id);
                    }
                }
            }
        }
        
        // Invalidate caches
        invalidate_user_caches(follower_id);
        for (const auto& following_id : following_ids) {
            invalidate_user_caches(following_id);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("bulk_follow", duration);
        
        bulk_result["processing_time_us"] = duration;
        
        spdlog::info("✅ Bulk follow completed: {} users in {}μs", following_ids.size(), duration);
        
        return bulk_result;
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Bulk follow failed: {} - {} ({}μs)", follower_id, e.what(), duration);
        
        return create_error_response("BULK_FOLLOW_FAILED", e.what());
    }
}

json FollowService::bulk_unfollow(const std::string& follower_id, 
                                 const std::vector<std::string>& following_ids) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::info("📦 Processing bulk unfollow: {} -> {} users", follower_id, following_ids.size());
        
        // Validate bulk size
        if (following_ids.size() > 100) {
            return create_error_response("BULK_SIZE_EXCEEDED", "Maximum 100 users per bulk operation");
        }
        
        // Use repository bulk operation
        auto bulk_result = repository_->bulk_unfollow(follower_id, following_ids).get();
        
        // Update social graph for successful unfollows
        for (const auto& following_id : following_ids) {
            // Check if this unfollow was successful in bulk result
            if (bulk_result.contains("results") && bulk_result["results"].is_array()) {
                for (const auto& result : bulk_result["results"]) {
                    if (result["following_id"] == following_id && result["success"] == true) {
                        social_graph_->remove_follow_relationship(follower_id, following_id);
                    }
                }
            }
        }
        
        // Invalidate caches
        invalidate_user_caches(follower_id);
        for (const auto& following_id : following_ids) {
            invalidate_user_caches(following_id);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("bulk_unfollow", duration);
        
        bulk_result["processing_time_us"] = duration;
        
        spdlog::info("✅ Bulk unfollow completed: {} users in {}μs", following_ids.size(), duration);
        
        return bulk_result;
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Bulk unfollow failed: {} - {} ({}μs)", follower_id, e.what(), duration);
        
        return create_error_response("BULK_UNFOLLOW_FAILED", e.what());
    }
}

// ========== BLOCKING & MUTING ==========

json FollowService::block_user(const std::string& blocker_id, const std::string& blocked_id) {
    auto start = high_resolution_clock::now();
    try {
        spdlog::debug("🚫 Processing block: {} -> {}", blocker_id, blocked_id);
        if (blocker_id == blocked_id) {
            return create_error_response("SELF_BLOCK", "Cannot block yourself");
        }
        bool success = repository_->block_user(blocker_id, blocked_id).get();
        if (success) {
            // Remove any existing follow relationships and await
            (void)repository_->remove_follow(blocker_id, blocked_id).get();
            (void)repository_->remove_follow(blocked_id, blocker_id).get();
            social_graph_->remove_follow_relationship(blocker_id, blocked_id);
            social_graph_->remove_follow_relationship(blocked_id, blocker_id);
            invalidate_user_caches(blocker_id);
            invalidate_user_caches(blocked_id);
        }
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        track_operation_performance("block_user", duration);
        spdlog::info("✅ Block {}: {} -> {} in {}μs", 
                    success ? "successful" : "failed", blocker_id, blocked_id, duration);
        return create_success_response(success ? "BLOCK_SUCCESS" : "BLOCK_FAILED", {
            {"blocker_id", blocker_id},
            {"blocked_id", blocked_id},
            {"success", success},
            {"processing_time_us", duration}
        });
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        spdlog::error("❌ Block failed: {} -> {} - {} ({}μs)", blocker_id, blocked_id, e.what(), duration);
        return create_error_response("BLOCK_FAILED", e.what());
    }
}

bool FollowService::is_blocked(const std::string& user_id, const std::string& potentially_blocked_id) {
    try {
        auto relationship = repository_->get_relationship(user_id, potentially_blocked_id).get();
        return relationship.user1_blocked_user2;
    } catch (const std::exception& e) {
        spdlog::error("❌ Block check failed: {} -> {} - {}", user_id, potentially_blocked_id, e.what());
        return false;
    }
}

// ========== FRIEND RECOMMENDATIONS ==========

json FollowService::get_friend_recommendations(const std::string& user_id, int limit, 
                                               const std::string& algorithm) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🎯 Getting recommendations for {}: algorithm={}, limit={}", 
                     user_id, algorithm, limit);
        
        json recommendations;
        
        if (algorithm == "mutual_friends" || algorithm == "hybrid") {
            auto mutual_recs = social_graph_->get_mutual_friend_recommendations(user_id, limit).get();
            recommendations["mutual_friends"] = mutual_recs;
        }
        
        if (algorithm == "interests" || algorithm == "hybrid") {
            auto interest_recs = social_graph_->get_interest_based_recommendations(user_id, limit).get();
            recommendations["interests"] = interest_recs;
        }
        
        if (algorithm == "trending" || algorithm == "hybrid") {
            auto trending_recs = social_graph_->get_trending_recommendations(user_id, limit).get();
            recommendations["trending"] = trending_recs;
        }
        
        // Combine and deduplicate for hybrid
        std::vector<json> final_recommendations;
        std::unordered_set<std::string> seen_users;
        
        if (algorithm == "hybrid") {
            // Merge different recommendation types with weights
            auto add_recommendations = [&](const json& recs, double weight) {
                if (recs.is_array()) {
                    for (const auto& rec : recs) {
                        std::string user_rec_id = rec.value("user_id", "");
                        if (!user_rec_id.empty() && seen_users.find(user_rec_id) == seen_users.end()) {
                            json weighted_rec = rec;
                            weighted_rec["score"] = rec.value("score", 0.0) * weight;
                            final_recommendations.push_back(weighted_rec);
                            seen_users.insert(user_rec_id);
                        }
                    }
                }
            };
            
            add_recommendations(recommendations.value("mutual_friends", json::array()), 1.0);
            add_recommendations(recommendations.value("interests", json::array()), 0.8);
            add_recommendations(recommendations.value("trending", json::array()), 0.6);
            
            // Sort by score
            std::sort(final_recommendations.begin(), final_recommendations.end(),
                     [](const json& a, const json& b) {
                         return a.value("score", 0.0) > b.value("score", 0.0);
                     });
            
            // Limit results
            if (final_recommendations.size() > static_cast<size_t>(limit)) {
                final_recommendations.resize(limit);
            }
        } else {
            // Single algorithm result
            auto single_result = recommendations.begin();
            if (single_result != recommendations.end() && single_result->is_array()) {
                for (const auto& rec : *single_result) {
                    final_recommendations.push_back(rec);
                    if (final_recommendations.size() >= static_cast<size_t>(limit)) break;
                }
            }
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_friend_recommendations", duration);
        
        json result = {
            {"user_id", user_id},
            {"algorithm", algorithm},
            {"count", final_recommendations.size()},
            {"recommendations", final_recommendations},
            {"processing_time_us", duration}
        };
        
        spdlog::debug("✅ Recommendations generated for {}: {} results in {}μs", 
                     user_id, final_recommendations.size(), duration);
        
        return result;
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get recommendations failed for {}: {} ({}μs)", user_id, e.what(), duration);
        
        return create_error_response("GET_RECOMMENDATIONS_FAILED", e.what());
    }
}

// ========== ANALYTICS ==========

json FollowService::get_follower_analytics(const std::string& user_id, const std::string& requester_id, int days) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📊 Getting follower analytics for {}: {} days", user_id, days);
        
        auto analytics = repository_->get_follower_analytics(user_id, days).get();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_follower_analytics", duration);
        
        analytics["processing_time_us"] = duration;
        analytics["requester_id"] = requester_id;
        
        spdlog::debug("✅ Analytics retrieved for {} in {}μs", user_id, duration);
        
        return analytics;
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get analytics failed for {}: {} ({}μs)", user_id, e.what(), duration);
        
        return create_error_response("GET_ANALYTICS_FAILED", e.what());
    }
}

json FollowService::get_social_metrics(const std::string& user_id) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📈 Getting social metrics for {}", user_id);
        
        auto metrics = repository_->get_social_metrics(user_id).get();
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("get_social_metrics", duration);
        
        metrics["processing_time_us"] = duration;
        
        spdlog::debug("✅ Social metrics retrieved for {} in {}μs", user_id, duration);
        
        return metrics;
        
    } catch (const std::exception& e) {
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        spdlog::error("❌ Get social metrics failed for {}: {} ({}μs)", user_id, e.what(), duration);
        
        return create_error_response("GET_SOCIAL_METRICS_FAILED", e.what());
    }
}

// ========== HELPER METHODS ==========

json FollowService::create_success_response(const std::string& status, const json& data) {
    json response = {
        {"success", true},
        {"status", status},
        {"timestamp", duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()}
    };
    
    if (!data.empty()) {
        response["data"] = data;
    }
    
    return response;
}

json FollowService::create_error_response(const std::string& error_code, const std::string& message) {
    return {
        {"success", false},
        {"error_code", error_code},
        {"message", message},
        {"timestamp", duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()}
    };
}

void FollowService::track_operation_performance(const std::string& operation, int64_t duration_us) {
    operation_counts_[operation]++;
    
    // Update running average
    if (operation_times_.find(operation) == operation_times_.end()) {
        operation_times_[operation] = static_cast<double>(duration_us);
    } else {
        operation_times_[operation] = (operation_times_[operation] + duration_us) / 2.0;
    }
}

void FollowService::invalidate_user_caches(const std::string& user_id) {
    try {
        repository_->invalidate_user_cache(user_id);
    } catch (const std::exception& e) {
        spdlog::warn("⚠️ Cache invalidation failed for {}: {}", user_id, e.what());
    }
}

void FollowService::record_follow_event(const std::string& follower_id, const std::string& following_id,
                                       const std::string& event_type, const std::string& /*source*/) {
    try {
        repository_->record_interaction(follower_id, following_id, event_type);
    } catch (const std::exception& e) {
        spdlog::warn("⚠️ Event recording failed: {} -> {} ({}) - {}", 
                    follower_id, following_id, event_type, e.what());
    }
}

json FollowService::get_service_metrics() const {
    auto uptime = duration_cast<seconds>(steady_clock::now() - start_time_).count();
    
    json metrics = {
        {"service_name", "follow_service"},
        {"uptime_seconds", uptime},
        {"total_operations", 0},
        {"operation_metrics", json::object()}
    };
    
    uint64_t total_ops = 0;
    for (const auto& [operation, count] : operation_counts_) {
        total_ops += count;
        metrics["operation_metrics"][operation] = {
            {"count", count},
            {"avg_duration_us", operation_times_.at(operation)}
        };
    }
    
    metrics["total_operations"] = total_ops;
    
    return metrics;
}

} // namespace sonet::follow
