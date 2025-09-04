/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../service.h"
#include "../models/follow.h"
#include "../models/relationship.h"
#include <nlohmann/json.hpp>
#include <string>
#include <memory>

namespace sonet::follow::controllers {

using json = nlohmann::json;
using namespace sonet::follow;
using namespace sonet::follow::models;

/**
 * @brief Twitter-Scale Follow Controller
 * 
 * RESTful HTTP API for follow service operations:
 * - Ultra-fast follow/unfollow operations (sub-1ms)
 * - Relationship management and queries
 * - Friend recommendations and discovery
 * - Social graph analytics
 * - Privacy controls and settings
 * - Real-time relationship updates
 * 
 * Performance Targets:
 * - Sub-1ms for follow/unfollow operations
 * - Sub-2ms for relationship checks
 * - Sub-5ms for follower/following lists
 * - Sub-10ms for friend recommendations
 * - Support 10K+ concurrent requests
 */
class FollowController {
public:
    // Constructor
    explicit FollowController(std::shared_ptr<FollowService> follow_service);
    
    // ========== CORE RELATIONSHIP OPERATIONS ==========
    
    /**
     * @brief NOTE /api/v1/follow/{user_id}
     * Follow a user
     * 
     * @param user_id User to follow
     * @param request_data Request body with optional parameters
     * @param requesting_user_id User making the request
     * @return JSON response with follow result
     */
    json follow_user(const std::string& user_id, const json& request_data, const std::string& requesting_user_id);
    
    /**
     * @brief DELETE /api/v1/follow/{user_id}
     * Unfollow a user
     * 
     * @param user_id User to unfollow
     * @param requesting_user_id User making the request
     * @return JSON response with unfollow result
     */
    json unfollow_user(const std::string& user_id, const std::string& requesting_user_id);
    
    /**
     * @brief NOTE /api/v1/block/{user_id}
     * Block a user
     * 
     * @param user_id User to block
     * @param request_data Request body with optional reason
     * @param requesting_user_id User making the request
     * @return JSON response with block result
     */
    json block_user(const std::string& user_id, const json& request_data, const std::string& requesting_user_id);
    
    /**
     * @brief DELETE /api/v1/block/{user_id}
     * Unblock a user
     * 
     * @param user_id User to unblock
     * @param requesting_user_id User making the request
     * @return JSON response with unblock result
     */
    json unblock_user(const std::string& user_id, const std::string& requesting_user_id);
    
    /**
     * @brief NOTE /api/v1/mute/{user_id}
     * Mute a user
     * 
     * @param user_id User to mute
     * @param request_data Request body with mute options
     * @param requesting_user_id User making the request
     * @return JSON response with mute result
     */
    json mute_user(const std::string& user_id, const json& request_data, const std::string& requesting_user_id);
    
    /**
     * @brief DELETE /api/v1/mute/{user_id}
     * Unmute a user
     * 
     * @param user_id User to unmute
     * @param requesting_user_id User making the request
     * @return JSON response with unmute result
     */
    json unmute_user(const std::string& user_id, const std::string& requesting_user_id);
    
    // ========== RELATIONSHIP QUERIES ==========
    
    /**
     * @brief GET /api/v1/relationship/{user_id}
     * Get relationship status with a user
     * 
     * @param user_id User to check relationship with
     * @param requesting_user_id User making the request
     * @return JSON response with relationship details
     */
    json get_relationship(const std::string& user_id, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/relationships/bulk
     * Get relationships with multiple users
     * 
     * @param user_ids Query parameter with comma-separated user IDs
     * @param requesting_user_id User making the request
     * @return JSON response with bulk relationship data
     */
    json get_bulk_relationships(const std::string& user_ids, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/friendship/check
     * Check if two users are friends (mutual follow)
     * 
     * @param user1_id First user ID (query parameter)
     * @param user2_id Second user ID (query parameter)
     * @param requesting_user_id User making the request
     * @return JSON response with friendship status
     */
    json check_friendship(const std::string& user1_id, const std::string& user2_id, const std::string& requesting_user_id);
    
    // ========== FOLLOWER/FOLLOWING LISTS ==========
    
    /**
     * @brief GET /api/v1/users/{user_id}/followers
     * Get user's followers with pagination
     * 
     * @param user_id Target user
     * @param limit Query parameter for limit (default: 50, max: 200)
     * @param cursor Query parameter for pagination cursor
     * @param requesting_user_id User making the request
     * @return JSON response with follower list
     */
    json get_followers(const std::string& user_id, const std::string& limit, const std::string& cursor, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/users/{user_id}/following
     * Get users that a user is following with pagination
     * 
     * @param user_id Target user
     * @param limit Query parameter for limit (default: 50, max: 200)
     * @param cursor Query parameter for pagination cursor
     * @param requesting_user_id User making the request
     * @return JSON response with following list
     */
    json get_following(const std::string& user_id, const std::string& limit, const std::string& cursor, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/users/{user_id}/mutual-friends/{other_user_id}
     * Get mutual friends between two users
     * 
     * @param user_id First user
     * @param other_user_id Second user
     * @param limit Query parameter for limit (default: 20, max: 100)
     * @param requesting_user_id User making the request
     * @return JSON response with mutual friends list
     */
    json get_mutual_friends(const std::string& user_id, const std::string& other_user_id, const std::string& limit, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/users/{user_id}/blocked
     * Get users blocked by a user (only accessible by the user themselves)
     * 
     * @param user_id User whose blocked list to retrieve
     * @param limit Query parameter for limit (default: 50)
     * @param cursor Query parameter for pagination cursor
     * @param requesting_user_id User making the request
     * @return JSON response with blocked users list
     */
    json get_blocked_users(const std::string& user_id, const std::string& limit, const std::string& cursor, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/users/{user_id}/muted
     * Get users muted by a user (only accessible by the user themselves)
     * 
     * @param user_id User whose muted list to retrieve
     * @param limit Query parameter for limit (default: 50)
     * @param cursor Query parameter for pagination cursor
     * @param requesting_user_id User making the request
     * @return JSON response with muted users list
     */
    json get_muted_users(const std::string& user_id, const std::string& limit, const std::string& cursor, const std::string& requesting_user_id);
    
    // ========== FRIEND RECOMMENDATIONS ==========
    
    /**
     * @brief GET /api/v1/recommendations/friends
     * Get friend recommendations for the authenticated user
     * 
     * @param limit Query parameter for limit (default: 20, max: 50)
     * @param algorithm Query parameter for algorithm (default, mutual, interests, trending)
     * @param requesting_user_id User making the request
     * @return JSON response with friend recommendations
     */
    json get_friend_recommendations(const std::string& limit, const std::string& algorithm, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/recommendations/mutual-friends
     * Get recommendations based on mutual friends
     * 
     * @param limit Query parameter for limit (default: 20)
     * @param requesting_user_id User making the request
     * @return JSON response with mutual friend recommendations
     */
    json get_mutual_friend_recommendations(const std::string& limit, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/recommendations/trending
     * Get trending users to follow
     * 
     * @param limit Query parameter for limit (default: 20)
     * @param category Query parameter for category filter
     * @param requesting_user_id User making the request
     * @return JSON response with trending users
     */
    json get_trending_users(const std::string& limit, const std::string& category, const std::string& requesting_user_id);
    
    // ========== ANALYTICS AND METRICS ==========
    
    /**
     * @brief GET /api/v1/analytics/followers/{user_id}
     * Get follower analytics for a user
     * 
     * @param user_id User to analyze
     * @param time_range Query parameter for time range (1d, 7d, 30d, 90d)
     * @param requesting_user_id User making the request
     * @return JSON response with follower analytics
     */
    json get_follower_analytics(const std::string& user_id, const std::string& time_range, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/analytics/social-metrics/{user_id}
     * Get social metrics for a user
     * 
     * @param user_id User to get metrics for
     * @param requesting_user_id User making the request
     * @return JSON response with social metrics
     */
    json get_social_metrics(const std::string& user_id, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/analytics/growth/{user_id}
     * Get growth metrics over time
     * 
     * @param user_id User to analyze
     * @param days Query parameter for number of days (default: 30, max: 365)
     * @param requesting_user_id User making the request
     * @return JSON response with growth metrics
     */
    json get_growth_metrics(const std::string& user_id, const std::string& days, const std::string& requesting_user_id);
    
    // ========== BULK OPERATIONS ==========
    
    /**
     * @brief NOTE /api/v1/follow/bulk
     * Follow multiple users in one request
     * 
     * @param request_data Request body with array of user IDs to follow
     * @param requesting_user_id User making the request
     * @return JSON response with bulk follow results
     */
    json bulk_follow(const json& request_data, const std::string& requesting_user_id);
    
    /**
     * @brief DELETE /api/v1/follow/bulk
     * Unfollow multiple users in one request
     * 
     * @param request_data Request body with array of user IDs to unfollow
     * @param requesting_user_id User making the request
     * @return JSON response with bulk unfollow results
     */
    json bulk_unfollow(const json& request_data, const std::string& requesting_user_id);
    
    // ========== PRIVACY AND SETTINGS ==========
    
    /**
     * @brief GET /api/v1/settings/privacy
     * Get user's follow privacy settings
     * 
     * @param requesting_user_id User making the request
     * @return JSON response with privacy settings
     */
    json get_privacy_settings(const std::string& requesting_user_id);
    
    /**
     * @brief PUT /api/v1/settings/privacy
     * Update user's follow privacy settings
     * 
     * @param request_data Request body with privacy settings
     * @param requesting_user_id User making the request
     * @return JSON response with updated settings
     */
    json update_privacy_settings(const json& request_data, const std::string& requesting_user_id);
    
    // ========== REAL-TIME OPERATIONS ==========
    
    /**
     * @brief GET /api/v1/users/{user_id}/follower-count/live
     * Get real-time follower count
     * 
     * @param user_id User to get count for
     * @param requesting_user_id User making the request
     * @return JSON response with live follower count
     */
    json get_live_follower_count(const std::string& user_id, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/activity/followers/recent
     * Get recent follower activity
     * 
     * @param limit Query parameter for limit (default: 10, max: 50)
     * @param requesting_user_id User making the request
     * @return JSON response with recent follower activities
     */
    json get_recent_follower_activity(const std::string& limit, const std::string& requesting_user_id);
    
    // ========== VALIDATION AND UTILITIES ==========
    
    /**
     * @brief NOTE /api/v1/validate/follow-request
     * Validate a follow request before attempting
     * 
     * @param request_data Request body with target user ID
     * @param requesting_user_id User making the request
     * @return JSON response with validation result
     */
    json validate_follow_request(const json& request_data, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/rate-limits/check
     * Check rate limits for user actions
     * 
     * @param action Query parameter for action type
     * @param requesting_user_id User making the request
     * @return JSON response with rate limit status
     */
    json check_rate_limits(const std::string& action, const std::string& requesting_user_id);
    
    // ========== SEARCH AND DISCOVERY ==========
    
    /**
     * @brief GET /api/v1/search/users/suggested
     * Get suggested users based on follow patterns
     * 
     * @param query Query parameter for search query (optional)
     * @param limit Query parameter for limit (default: 20)
     * @param requesting_user_id User making the request
     * @return JSON response with suggested users
     */
    json get_suggested_users(const std::string& query, const std::string& limit, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/discover/who-to-follow
     * Get "Who to Follow" recommendations
     * 
     * @param limit Query parameter for limit (default: 20)
     * @param refresh Query parameter to force refresh recommendations
     * @param requesting_user_id User making the request
     * @return JSON response with "Who to Follow" recommendations
     */
    json get_who_to_follow(const std::string& limit, const std::string& refresh, const std::string& requesting_user_id);
    
    // ========== SOCIAL GRAPH INSIGHTS ==========
    
    /**
     * @brief GET /api/v1/insights/network/{user_id}
     * Get network insights for a user
     * 
     * @param user_id User to analyze
     * @param requesting_user_id User making the request
     * @return JSON response with network insights
     */
    json get_network_insights(const std::string& user_id, const std::string& requesting_user_id);
    
    /**
     * @brief GET /api/v1/insights/influence/{user_id}
     * Get influence metrics for a user
     * 
     * @param user_id User to analyze
     * @param algorithm Query parameter for influence algorithm
     * @param requesting_user_id User making the request
     * @return JSON response with influence metrics
     */
    json get_influence_metrics(const std::string& user_id, const std::string& algorithm, const std::string& requesting_user_id);
    
private:
    std::shared_ptr<FollowService> follow_service_;
    
    // ========== HELPER METHODS ==========
    
    // Request validation
    bool validate_user_id(const std::string& user_id) const;
    bool validate_requesting_user(const std::string& requesting_user_id) const;
    bool validate_pagination_params(const std::string& limit, const std::string& cursor) const;
    bool validate_bulk_request(const json& request_data, int max_items = 100) const;
    
    // Response helpers
    json create_success_response(const json& data, const std::string& message = "Success") const;
    json create_error_response(const std::string& error_code, const std::string& message, int http_status = 400) const;
    json create_paginated_response(const json& data, const std::string& next_cursor, bool has_more) const;
    json create_rate_limit_response(const std::string& action, int limit, int remaining, int reset_time) const;
    
    // Parameter parsing
    int parse_limit_parameter(const std::string& limit, int default_value, int max_value) const;
    std::string parse_cursor_parameter(const std::string& cursor) const;
    std::vector<std::string> parse_user_ids_parameter(const std::string& user_ids) const;
    
    // Permission checks
    bool can_view_followers(const std::string& target_user_id, const std::string& requesting_user_id) const;
    bool can_view_following(const std::string& target_user_id, const std::string& requesting_user_id) const;
    bool can_view_blocked_list(const std::string& target_user_id, const std::string& requesting_user_id) const;
    bool can_view_muted_list(const std::string& target_user_id, const std::string& requesting_user_id) const;
    bool can_view_analytics(const std::string& target_user_id, const std::string& requesting_user_id) const;
    
    // Rate limiting
    bool check_follow_rate_limit(const std::string& user_id) const;
    bool check_api_rate_limit(const std::string& user_id, const std::string& endpoint) const;
    void track_api_usage(const std::string& user_id, const std::string& endpoint) const;
    
    // Caching
    void invalidate_related_caches(const std::string& user_id) const;
    std::string generate_cache_key(const std::string& endpoint, const std::vector<std::string>& params) const;
    
    // Analytics helpers
    json format_follower_analytics(const json& raw_analytics) const;
    json format_growth_metrics(const json& raw_metrics) const;
    json format_social_metrics(const json& raw_metrics) const;
    
    // ========== CONSTANTS ==========
    
    // API limits
    static constexpr int DEFAULT_FOLLOWERS_LIMIT = 50;
    static constexpr int MAX_FOLLOWERS_LIMIT = 200;
    static constexpr int DEFAULT_FOLLOWING_LIMIT = 50;
    static constexpr int MAX_FOLLOWING_LIMIT = 200;
    static constexpr int DEFAULT_RECOMMENDATIONS_LIMIT = 20;
    static constexpr int MAX_RECOMMENDATIONS_LIMIT = 50;
    static constexpr int DEFAULT_MUTUAL_FRIENDS_LIMIT = 20;
    static constexpr int MAX_MUTUAL_FRIENDS_LIMIT = 100;
    static constexpr int DEFAULT_ACTIVITY_LIMIT = 10;
    static constexpr int MAX_ACTIVITY_LIMIT = 50;
    static constexpr int MAX_BULK_OPERATIONS = 100;
    
    // Rate limits (per minute)
    static constexpr int FOLLOW_RATE_LIMIT = 50;
    static constexpr int UNFOLLOW_RATE_LIMIT = 100;
    static constexpr int BLOCK_RATE_LIMIT = 20;
    static constexpr int API_REQUEST_RATE_LIMIT = 1000;
    
    // Cache TTL (seconds)
    static constexpr int FOLLOWERS_CACHE_TTL = 300;         // 5 minutes
    static constexpr int FOLLOWING_CACHE_TTL = 300;         // 5 minutes
    static constexpr int RECOMMENDATIONS_CACHE_TTL = 3600;  // 1 hour
    static constexpr int ANALYTICS_CACHE_TTL = 1800;       // 30 minutes
};

} // namespace sonet::follow::controllers
