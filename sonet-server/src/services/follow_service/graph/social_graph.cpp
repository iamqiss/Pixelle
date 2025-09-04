/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "social_graph.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <algorithm>
#include <random>
#include <queue>
#include <set>
#include <unordered_set>
#include <cmath>

namespace sonet::follow::graph {

using namespace std::chrono;
using json = nlohmann::json;

// ========== CONSTRUCTOR & INITIALIZATION ==========

SocialGraph::SocialGraph(std::shared_ptr<void> graph_store, const json& config)
    : graph_store_(graph_store), config_(config), start_time_(steady_clock::now()) {
    
    spdlog::info("🕸️ Initializing Twitter-Scale Social Graph Engine...");
    
    // Initialize configuration
    max_recommendations_ = config_.value("max_recommendations", 100);
    cache_ttl_seconds_ = config_.value("cache_ttl_seconds", 300);
    enable_real_time_updates_ = config_.value("enable_real_time_updates", true);
    graph_algorithm_type_ = config_.value("algorithm_type", "hybrid");
    
    // Initialize algorithm parameters
    mutual_friend_weight_ = config_.value("mutual_friend_weight", 1.0);
    interest_weight_ = config_.value("interest_weight", 0.8);
    trending_weight_ = config_.value("trending_weight", 0.6);
    recency_decay_factor_ = config_.value("recency_decay_factor", 0.9);
    
    // Initialize performance tracking
    operation_counts_.clear();
    operation_times_.clear();
    
    spdlog::info("✅ Social Graph initialized: algorithm={}, max_recs={}, real_time={}", 
                graph_algorithm_type_, max_recommendations_, enable_real_time_updates_);
}

// Default constructor delegates to the configurable one
SocialGraph::SocialGraph() : SocialGraph(nullptr, json::object()) {}

// ========== GRAPH OPERATIONS ==========

void SocialGraph::add_follow_relationship(const std::string& follower_id, const std::string& following_id) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("➕ Adding edge to social graph: {} -> {}", follower_id, following_id);
        
        // Add to in-memory graph structure
        {
            std::lock_guard<std::shared_mutex> lock(graph_mutex_);
            
            // Add follower -> following edge
            if (adjacency_list_.find(follower_id) == adjacency_list_.end()) {
                adjacency_list_[follower_id] = std::unordered_set<std::string>();
            }
            adjacency_list_[follower_id].insert(following_id);
            
            // Add to reverse index (for followers lookup)
            if (reverse_adjacency_list_.find(following_id) == reverse_adjacency_list_.end()) {
                reverse_adjacency_list_[following_id] = std::unordered_set<std::string>();
            }
            reverse_adjacency_list_[following_id].insert(follower_id);
            
            // Update user metrics
            user_metrics_[follower_id].following_count++;
            user_metrics_[following_id].follower_count++;
            user_metrics_[following_id].last_followed_at = system_clock::now();
        }
        
        // Invalidate relevant caches
        invalidate_user_cache(follower_id);
        invalidate_user_cache(following_id);
        
        // Update recommendation caches asynchronously
        if (enable_real_time_updates_) {
            update_recommendation_caches_async(follower_id, following_id);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("add_follow_relationship", duration);
        
        spdlog::debug("✅ Edge added: {} -> {} in {}μs", follower_id, following_id, duration);
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to add edge: {} -> {} - {}", follower_id, following_id, e.what());
    }
}

void SocialGraph::remove_follow_relationship(const std::string& follower_id, const std::string& following_id) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("➖ Removing edge from social graph: {} -> {}", follower_id, following_id);
        
        {
            std::lock_guard<std::shared_mutex> lock(graph_mutex_);
            
            // Remove from adjacency list
            if (adjacency_list_.find(follower_id) != adjacency_list_.end()) {
                adjacency_list_[follower_id].erase(following_id);
                if (adjacency_list_[follower_id].empty()) {
                    adjacency_list_.erase(follower_id);
                }
            }
            
            // Remove from reverse adjacency list
            if (reverse_adjacency_list_.find(following_id) != reverse_adjacency_list_.end()) {
                reverse_adjacency_list_[following_id].erase(follower_id);
                if (reverse_adjacency_list_[following_id].empty()) {
                    reverse_adjacency_list_.erase(following_id);
                }
            }
            
            // Update user metrics
            if (user_metrics_[follower_id].following_count > 0) {
                user_metrics_[follower_id].following_count--;
            }
            if (user_metrics_[following_id].follower_count > 0) {
                user_metrics_[following_id].follower_count--;
            }
        }
        
        // Invalidate relevant caches
        invalidate_user_cache(follower_id);
        invalidate_user_cache(following_id);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("remove_follow_relationship", duration);
        
        spdlog::debug("✅ Edge removed: {} -> {} in {}μs", follower_id, following_id, duration);
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to remove edge: {} -> {} - {}", follower_id, following_id, e.what());
    }
}

bool SocialGraph::has_follow_relationship(const std::string& follower_id, const std::string& following_id) {
    auto start = high_resolution_clock::now();
    
    try {
        std::shared_lock<std::shared_mutex> lock(graph_mutex_);
        
        bool result = false;
        auto it = adjacency_list_.find(follower_id);
        if (it != adjacency_list_.end()) {
            result = it->second.find(following_id) != it->second.end();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("has_follow_relationship", duration);
        
        spdlog::debug("🔍 Relationship check: {} -> {} = {} ({}μs)", 
                     follower_id, following_id, result, duration);
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Relationship check failed: {} -> {} - {}", follower_id, following_id, e.what());
        return false;
    }
}

// ========== RECOMMENDATION ALGORITHMS ==========

std::future<std::vector<json>> SocialGraph::get_mutual_friend_recommendations(const std::string& user_id, int limit) {
    return std::async(std::launch::async, [this, user_id, limit]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("🎯 Computing mutual friend recommendations for {}: limit={}", user_id, limit);
            
            // Check cache first
            std::string cache_key = "mutual_recs:" + user_id;
            auto cached = get_cached_recommendations(cache_key);
            if (!cached.empty()) {
                auto limited = limit_recommendations(cached, limit);
                spdlog::debug("🎯 Returned cached mutual friend recommendations: {} results", limited.size());
                return limited;
            }
            
            // Use simple double score per candidate
            std::unordered_map<std::string, double> candidate_scores;
            std::unordered_map<std::string, std::unordered_set<std::string>> candidate_mutuals;
            {
                std::shared_lock<std::shared_mutex> lock(graph_mutex_);
                auto user_following_it = adjacency_list_.find(user_id);
                if (user_following_it == adjacency_list_.end()) {
                    return std::vector<json>{};
                }
                const auto& user_following = user_following_it->second;
                for (const auto& following_id : user_following) {
                    auto following_following_it = adjacency_list_.find(following_id);
                    if (following_following_it == adjacency_list_.end()) continue;
                    for (const auto& candidate_id : following_following_it->second) {
                        if (candidate_id == user_id || user_following.count(candidate_id)) continue;
                        candidate_scores[candidate_id] += mutual_friend_weight_;
                        candidate_mutuals[candidate_id].insert(following_id);
                        auto metrics_it = user_metrics_.find(candidate_id);
                        if (metrics_it != user_metrics_.end()) {
                            candidate_scores[candidate_id] += std::log(metrics_it->second.follower_count + 1) * 0.1;
                        }
                    }
                }
            }
            std::vector<json> recommendations;
            for (const auto& [candidate_id, score] : candidate_scores) {
                json rec = {
                    {"user_id", candidate_id},
                    {"score", score},
                    {"mutual_friend_count", candidate_mutuals[candidate_id].size()},
                };
                recommendations.push_back(rec);
            }
            std::sort(recommendations.begin(), recommendations.end(),
                       [](const json& a, const json& b) { return a["score"].get<double>() > b["score"].get<double>(); });
            
            // Cache results
            cache_recommendations(cache_key, recommendations);
            
            auto result = limit_recommendations(recommendations, limit);
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("get_mutual_friend_recommendations", duration);
            
            spdlog::debug("✅ Mutual friend recommendations computed for {}: {} results in {}μs", 
                         user_id, result.size(), duration);
            
            return result;
            
        } catch (const std::exception& e) {
            spdlog::error("❌ Mutual friend recommendations failed for {}: {}", user_id, e.what());
            return std::vector<json>{};
        }
    });
}

std::future<std::vector<json>> SocialGraph::get_interest_based_recommendations(const std::string& user_id, int limit) {
    return std::async(std::launch::async, [this, user_id, limit]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("🎯 Computing interest-based recommendations for {}: limit={}", user_id, limit);
            
            // Check cache first
            std::string cache_key = "interest_recs:" + user_id;
            auto cached = get_cached_recommendations(cache_key);
            if (!cached.empty()) {
                auto limited = limit_recommendations(cached, limit);
                spdlog::debug("🎯 Returned cached interest recommendations: {} results", limited.size());
                return limited;
            }
            
            std::unordered_map<std::string, double> candidate_scores;
            {
                std::shared_lock<std::shared_mutex> lock(graph_mutex_);
                auto user_following_it = adjacency_list_.find(user_id);
                if (user_following_it == adjacency_list_.end()) {
                    return std::vector<json>{};
                }
                const auto& user_following = user_following_it->second;
                std::unordered_map<std::string, double> interest_weights;
                for (const auto& following_id : user_following) {
                    auto interests = simulate_user_interests(following_id);
                    for (const auto& [interest, weight] : interests) interest_weights[interest] += weight;
                }
                double total_weight = 0.0;
                for (const auto& [interest, weight] : interest_weights) total_weight += weight;
                if (total_weight > 0) {
                    for (auto& [interest, weight] : interest_weights) weight /= total_weight;
                }
                for (const auto& [candidate_id, metrics] : user_metrics_) {
                    if (candidate_id == user_id || user_following.count(candidate_id)) continue;
                    auto candidate_interests = simulate_user_interests(candidate_id);
                    double similarity_score = 0.0;
                    for (const auto& [interest, user_weight] : interest_weights) {
                        auto it = candidate_interests.find(interest);
                        if (it != candidate_interests.end()) similarity_score += user_weight * it->second;
                    }
                    if (similarity_score > 0.1) {
                        double score = similarity_score * interest_weight_;
                        score += std::log(metrics.follower_count + 1) * 0.05;
                        candidate_scores[candidate_id] = score;
                    }
                }
            }
            std::vector<json> recommendations;
            for (const auto& [candidate_id, score] : candidate_scores) {
                json rec = {{"user_id", candidate_id}, {"score", score}};
                recommendations.push_back(rec);
            }
            std::sort(recommendations.begin(), recommendations.end(),
                       [](const json& a, const json& b) { return a["score"].get<double>() > b["score"].get<double>(); });
            
            // Cache results
            cache_recommendations(cache_key, recommendations);
            
            auto result = limit_recommendations(recommendations, limit);
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("get_interest_based_recommendations", duration);
            
            spdlog::debug("✅ Interest recommendations computed for {}: {} results in {}μs", 
                         user_id, result.size(), duration);
            
            return result;
            
        } catch (const std::exception& e) {
            spdlog::error("❌ Interest recommendations failed for {}: {}", user_id, e.what());
            return std::vector<json>{};
        }
    });
}

std::future<std::vector<json>> SocialGraph::get_trending_recommendations(const std::string& user_id, int limit) {
    return std::async(std::launch::async, [this, user_id, limit]() {
        auto start = high_resolution_clock::now();
        
        try {
            spdlog::debug("🎯 Computing trending recommendations for {}: limit={}", user_id, limit);
            
            // Check cache first
            std::string cache_key = "trending_recs:" + user_id;
            auto cached = get_cached_recommendations(cache_key);
            if (!cached.empty()) {
                auto limited = limit_recommendations(cached, limit);
                spdlog::debug("🎯 Returned cached trending recommendations: {} results", limited.size());
                return limited;
            }
            
            std::vector<json> recommendations;
            auto now = system_clock::now();
            
            {
                std::shared_lock<std::shared_mutex> lock(graph_mutex_);
                
                // Get user's following list to exclude
                std::unordered_set<std::string> user_following;
                auto user_following_it = adjacency_list_.find(user_id);
                if (user_following_it != adjacency_list_.end()) {
                    user_following = user_following_it->second;
                }
                
                // Calculate trending scores for all users
                std::vector<std::pair<std::string, double>> trending_scores;
                
                for (const auto& [candidate_id, metrics] : user_metrics_) {
                    if (candidate_id == user_id || user_following.count(candidate_id)) {
                        continue; // Skip self and already following
                    }
                    
                    // Calculate trending score based on recent follower growth
                    double recency_hours = std::chrono::duration<double, std::ratio<3600>>(
                        now - metrics.last_followed_at).count();
                    
                    if (recency_hours > 168) continue; // Only consider users followed in last week
                    
                    // Exponential decay based on recency
                    double recency_factor = std::exp(-recency_hours / 24.0 * (1.0 - recency_decay_factor_));
                    
                    // Velocity score (follower growth rate)
                    double velocity_score = metrics.follower_count * recency_factor;
                    
                    // Engagement factor (simulated - would use real engagement metrics)
                    double engagement_factor = simulate_engagement_score(candidate_id);
                    
                    double trending_score = velocity_score * engagement_factor * trending_weight_;
                    
                    if (trending_score > 1.0) { // Minimum trending threshold
                        trending_scores.emplace_back(candidate_id, trending_score);
                    }
                }
                
                // Sort by trending score
                std::sort(trending_scores.begin(), trending_scores.end(),
                         [](const auto& a, const auto& b) {
                             return a.second > b.second;
                         });
                
                // Convert to JSON recommendations
                for (const auto& [candidate_id, score] : trending_scores) {
                    auto metrics_it = user_metrics_.find(candidate_id);
                    json rec = {
                        {"user_id", candidate_id},
                        {"score", score},
                        {"follower_count", metrics_it != user_metrics_.end() ? metrics_it->second.follower_count : 0},
                        {"reason", "trending"},
                        {"trending_factor", score / trending_weight_}
                    };
                    recommendations.push_back(rec);
                    
                    if (recommendations.size() >= static_cast<size_t>(limit * 2)) {
                        break; // Get extra for caching
                    }
                }
            }
            
            // Cache results
            cache_recommendations(cache_key, recommendations);
            
            auto result = limit_recommendations(recommendations, limit);
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start).count();
            
            track_operation_performance("get_trending_recommendations", duration);
            
            spdlog::debug("✅ Trending recommendations computed for {}: {} results in {}μs", 
                         user_id, result.size(), duration);
            
            return result;
            
        } catch (const std::exception& e) {
            spdlog::error("❌ Trending recommendations failed for {}: {}", user_id, e.what());
            return std::vector<json>{};
        }
    });
}

// ========== GRAPH ANALYSIS ==========

std::vector<std::string> SocialGraph::find_shortest_path(const std::string& from_user, const std::string& to_user, int max_hops) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("🔍 Finding shortest path: {} -> {} (max hops: {})", from_user, to_user, max_hops);
        
        std::shared_lock<std::shared_mutex> lock(graph_mutex_);
        
        if (from_user == to_user) {
            return {from_user};
        }
        
        // BFS to find shortest path
        std::queue<std::pair<std::string, std::vector<std::string>>> queue;
        std::unordered_set<std::string> visited;
        
        queue.push({from_user, {from_user}});
        visited.insert(from_user);
        
        while (!queue.empty()) {
            auto [current_user, path] = queue.front();
            queue.pop();
            
            if (path.size() > static_cast<size_t>(max_hops + 1)) {
                continue; // Exceeded max hops
            }
            
            auto following_it = adjacency_list_.find(current_user);
            if (following_it == adjacency_list_.end()) {
                continue;
            }
            
            for (const auto& next_user : following_it->second) {
                if (next_user == to_user) {
                    // Found target
                    auto result_path = path;
                    result_path.push_back(next_user);
                    
                    auto end = high_resolution_clock::now();
                    auto duration = duration_cast<microseconds>(end - start).count();
                    
                    track_operation_performance("find_shortest_path", duration);
                    
                    spdlog::debug("✅ Shortest path found: {} -> {} in {} hops ({}μs)", 
                                 from_user, to_user, result_path.size() - 1, duration);
                    
                    return result_path;
                }
                
                if (visited.find(next_user) == visited.end()) {
                    visited.insert(next_user);
                    auto new_path = path;
                    new_path.push_back(next_user);
                    queue.push({next_user, new_path});
                }
            }
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("find_shortest_path", duration);
        
        spdlog::debug("❌ No path found: {} -> {} within {} hops ({}μs)", 
                     from_user, to_user, max_hops, duration);
        
        return {}; // No path found
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Shortest path search failed: {} -> {} - {}", from_user, to_user, e.what());
        return {};
    }
}

double SocialGraph::calculate_influence_score(const std::string& user_id) {
    auto start = high_resolution_clock::now();
    
    try {
        spdlog::debug("📊 Calculating influence score for {}", user_id);
        
        std::shared_lock<std::shared_mutex> lock(graph_mutex_);
        
        auto metrics_it = user_metrics_.find(user_id);
        if (metrics_it == user_metrics_.end()) {
            return 0.0;
        }
        
        const auto& metrics = metrics_it->second;
        
        // Calculate influence based on multiple factors
        double follower_score = std::log(metrics.follower_count + 1);
        double following_ratio = metrics.following_count > 0 ? 
            static_cast<double>(metrics.follower_count) / metrics.following_count : metrics.follower_count;
        
        // Network centrality (simplified - would use PageRank in full implementation)
        double centrality_score = 0.0;
        auto following_it = adjacency_list_.find(user_id);
        if (following_it != adjacency_list_.end()) {
            for (const auto& following_id : following_it->second) {
                auto following_metrics_it = user_metrics_.find(following_id);
                if (following_metrics_it != user_metrics_.end()) {
                    centrality_score += std::log(following_metrics_it->second.follower_count + 1);
                }
            }
            centrality_score /= following_it->second.size();
        }
        
        // Engagement factor (simulated)
        double engagement_factor = simulate_engagement_score(user_id);
        
        double influence_score = (follower_score * 0.4 + 
                                following_ratio * 0.3 + 
                                centrality_score * 0.2 + 
                                engagement_factor * 0.1);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        track_operation_performance("calculate_influence_score", duration);
        
        spdlog::debug("✅ Influence score calculated for {}: {:.2f} ({}μs)", 
                     user_id, influence_score, duration);
        
        return influence_score;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Influence score calculation failed for {}: {}", user_id, e.what());
        return 0.0;
    }
}

// ========== HELPER METHODS ==========

void SocialGraph::track_operation_performance(const std::string& operation, int64_t duration_us) {
    operation_counts_[operation]++;
    
    if (operation_times_.find(operation) == operation_times_.end()) {
        operation_times_[operation] = static_cast<double>(duration_us);
    } else {
        operation_times_[operation] = (operation_times_[operation] + duration_us) / 2.0;
    }
}

void SocialGraph::invalidate_user_cache(const std::string& user_id) {
    // Invalidate recommendation caches for user
    recommendation_cache_.erase("mutual_recs:" + user_id);
    recommendation_cache_.erase("interest_recs:" + user_id);
    recommendation_cache_.erase("trending_recs:" + user_id);
}

std::vector<json> SocialGraph::get_cached_recommendations(const std::string& cache_key) {
    auto it = recommendation_cache_.find(cache_key);
    if (it != recommendation_cache_.end()) {
        auto now = steady_clock::now();
        if (duration_cast<seconds>(now - it->second.timestamp).count() < cache_ttl_seconds_) {
            return it->second.recommendations;
        } else {
            recommendation_cache_.erase(it);
        }
    }
    return {};
}

void SocialGraph::cache_recommendations(const std::string& cache_key, const std::vector<json>& recommendations) {
    recommendation_cache_[cache_key] = {
        .recommendations = recommendations,
        .timestamp = steady_clock::now()
    };
}

std::vector<json> SocialGraph::limit_recommendations(const std::vector<json>& recommendations, int limit) {
    if (recommendations.size() <= static_cast<size_t>(limit)) {
        return recommendations;
    }
    
    return std::vector<json>(recommendations.begin(), recommendations.begin() + limit);
}

void SocialGraph::update_recommendation_caches_async(const std::string& follower_id, const std::string& following_id) {
    // In real implementation, would use a background thread pool
    // For now, just invalidate caches
    invalidate_user_cache(follower_id);
    invalidate_user_cache(following_id);
}

std::unordered_map<std::string, double> SocialGraph::simulate_user_interests(const std::string& user_id) {
    // Simplified interest simulation - in real implementation would use ML/NLP
    std::hash<std::string> hasher;
    auto hash = hasher(user_id);
    
    std::unordered_map<std::string, double> interests;
    std::vector<std::string> categories = {"tech", "sports", "music", "politics", "entertainment", "science", "art"};
    
    for (size_t i = 0; i < categories.size(); ++i) {
        double weight = (hash % (100 + i * 10)) / 100.0;
        if (weight > 0.3) {
            interests[categories[i]] = weight;
        }
    }
    
    return interests;
}

double SocialGraph::simulate_engagement_score(const std::string& user_id) {
    // Simplified engagement simulation
    std::hash<std::string> hasher;
    auto hash = hasher(user_id + "engagement");
    return (hash % 100) / 100.0;
}

json SocialGraph::get_graph_metrics() const {
    auto uptime = duration_cast<seconds>(steady_clock::now() - start_time_).count();
    
    std::shared_lock<std::shared_mutex> lock(graph_mutex_);
    
    json metrics = {
        {"graph_name", "social_graph"},
        {"uptime_seconds", uptime},
        {"total_users", adjacency_list_.size()},
        {"total_relationships", 0},
        {"cache_size", recommendation_cache_.size()},
        {"operation_metrics", json::object()}
    };
    
    // Count total relationships
    size_t total_relationships = 0;
    for (const auto& [user_id, following_set] : adjacency_list_) {
        total_relationships += following_set.size();
    }
    metrics["total_relationships"] = total_relationships;
    
    // Add operation metrics
    for (const auto& [operation, count] : operation_counts_) {
        metrics["operation_metrics"][operation] = {
            {"count", count},
            {"avg_duration_us", operation_times_.at(operation)}
        };
    }
    
    return metrics;
}

} // namespace sonet::follow::graph
