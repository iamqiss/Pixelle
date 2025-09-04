/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "models/follow.h"
#include "models/relationship.h"
#include "graph/social_graph.h"
#include "repositories/follow_repository.h"
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>

namespace sonet::follow {

using json = nlohmann::json;

class FollowService {
public:
    // Constructors
    FollowService(std::shared_ptr<repositories::FollowRepository> repository,
                  std::shared_ptr<graph::SocialGraph> social_graph,
                  std::shared_ptr<void> cache_client,
                  const json& config);

    // Quick-win overload to match existing main usage
    FollowService(std::shared_ptr<repositories::FollowRepository> repository,
                  std::shared_ptr<graph::SocialGraph> social_graph)
        : FollowService(repository, social_graph, nullptr, json::object()) {}

    // Core operations
    // Two-arg overload delegates to extended version
    json follow_user(const std::string& follower_id, const std::string& following_id);
    json follow_user(const std::string& follower_id, const std::string& following_id,
                     const std::string& follow_type, const std::string& source);
    json unfollow_user(const std::string& follower_id, const std::string& following_id);

    // Queries
    bool is_following(const std::string& follower_id, const std::string& following_id);
    json get_relationship(const std::string& user1_id, const std::string& user2_id);

    // Lists
    json get_followers(const std::string& user_id, int limit, const std::string& cursor,
                       const std::string& requester_id);
    json get_following(const std::string& user_id, int limit, const std::string& cursor,
                       const std::string& requester_id);
    std::vector<std::string> get_mutual_friends(const std::string& user1_id, const std::string& user2_id, int limit);
    bool are_mutual_friends(const std::string& user1_id, const std::string& user2_id);

    // Bulk
    json bulk_follow(const std::string& follower_id, const std::vector<std::string>& following_ids,
                     const std::string& follow_type);
    json bulk_unfollow(const std::string& follower_id, const std::vector<std::string>& following_ids);

    // Blocking & recommendations & analytics
    json block_user(const std::string& blocker_id, const std::string& blocked_id);
    bool is_blocked(const std::string& user_id, const std::string& potentially_blocked_id);

    json get_friend_recommendations(const std::string& user_id, int limit, const std::string& algorithm);

    json get_follower_analytics(const std::string& user_id, const std::string& requester_id, int days);
    json get_social_metrics(const std::string& user_id);

    // Metrics
    json get_service_metrics() const;

private:
    std::shared_ptr<repositories::FollowRepository> repository_;
    std::shared_ptr<graph::SocialGraph> social_graph_;
    std::shared_ptr<void> cache_client_;
    json config_{};

    // Perf
    std::chrono::steady_clock::time_point start_time_{};
    std::unordered_map<std::string, uint64_t> operation_counts_;
    std::unordered_map<std::string, double> operation_times_;

    // Config values
    int max_following_limit_{7500};
    int max_followers_per_request_{1000};
    int cache_ttl_seconds_{300};

    // Helpers
    json create_success_response(const std::string& status, const json& data);
    json create_error_response(const std::string& error_code, const std::string& message);
    void track_operation_performance(const std::string& operation, int64_t duration_us);
    void invalidate_user_caches(const std::string& user_id);
    void record_follow_event(const std::string& follower_id, const std::string& following_id,
                             const std::string& event_type, const std::string& source);
};

} // namespace sonet::follow
