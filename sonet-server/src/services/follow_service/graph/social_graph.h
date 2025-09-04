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
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <future>
#include <chrono>
#include <memory>
#include <nlohmann/json.hpp>

namespace sonet::follow::graph {

using json = nlohmann::json;
using Clock = std::chrono::steady_clock;

class SocialGraph {
public:
    SocialGraph();
    SocialGraph(std::shared_ptr<void> graph_store, const json& config);

    // Core graph operations used by service
    void add_follow_relationship(const std::string& follower_id, const std::string& following_id);
    void remove_follow_relationship(const std::string& follower_id, const std::string& following_id);
    bool has_follow_relationship(const std::string& follower_id, const std::string& following_id);

    // Recommendations (async)
    std::future<std::vector<json>> get_mutual_friend_recommendations(const std::string& user_id, int limit);
    std::future<std::vector<json>> get_interest_based_recommendations(const std::string& user_id, int limit);
    std::future<std::vector<json>> get_trending_recommendations(const std::string& user_id, int limit);

    // Graph analysis
    std::vector<std::string> find_shortest_path(const std::string& from_user, const std::string& to_user, int max_hops);
    double calculate_influence_score(const std::string& user_id);

    // Metrics
    json get_graph_metrics() const;

private:
    struct UserMetrics {
        int follower_count{0};
        int following_count{0};
        std::chrono::system_clock::time_point last_followed_at{};
    };

    struct RecommendationCacheEntry {
        std::vector<json> recommendations;
        Clock::time_point timestamp;
    };

    // External / config
    std::shared_ptr<void> graph_store_{};
    json config_{};

    // Data
    mutable std::shared_mutex graph_mutex_;
    std::unordered_map<std::string, std::unordered_set<std::string>> adjacency_list_;
    std::unordered_map<std::string, std::unordered_set<std::string>> reverse_adjacency_list_;
    std::unordered_map<std::string, UserMetrics> user_metrics_;

    // Caching
    int cache_ttl_seconds_{300};
    std::unordered_map<std::string, RecommendationCacheEntry> recommendation_cache_;

    // Tunables
    int max_recommendations_{100};
    bool enable_real_time_updates_{true};
    std::string graph_algorithm_type_{"hybrid"};
    double mutual_friend_weight_{1.0};
    double interest_weight_{0.8};
    double trending_weight_{0.6};
    double recency_decay_factor_{0.9};

    // Perf
    Clock::time_point start_time_{};
    std::unordered_map<std::string, uint64_t> operation_counts_;
    std::unordered_map<std::string, double> operation_times_;

    // Helpers
    void track_operation_performance(const std::string& operation, int64_t duration_us);
    void invalidate_user_cache(const std::string& user_id);

    std::vector<json> get_cached_recommendations(const std::string& cache_key);
    void cache_recommendations(const std::string& cache_key, const std::vector<json>& recommendations);
    std::vector<json> limit_recommendations(const std::vector<json>& recommendations, int limit);
    void update_recommendation_caches_async(const std::string& follower_id, const std::string& following_id);

    std::unordered_map<std::string, double> simulate_user_interests(const std::string& user_id);
    double simulate_engagement_score(const std::string& user_id);
};

} // namespace sonet::follow::graph
