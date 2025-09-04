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
#include <optional>
#include <chrono>
#include <nlohmann/json.hpp>

namespace sonet::follow::models {

using json = nlohmann::json;
using Clock = std::chrono::system_clock;

class Relationship {
public:
    // Core identification
    std::string user1_id;
    std::string user2_id;

    // Bidirectional relationship flags
    bool user1_follows_user2{false};
    bool user2_follows_user1{false};
    bool user1_blocked_user2{false};
    bool user2_blocked_user1{false};
    bool user1_muted_user2{false};
    bool user2_muted_user1{false};

    // Special relationship flags
    bool is_close_friends{false};
    bool is_verified_relationship{false};

    // Timestamps
    Clock::time_point created_at;
    Clock::time_point updated_at;
    Clock::time_point user1_followed_user2_at;
    Clock::time_point user2_followed_user1_at;
    Clock::time_point last_interaction_at;

    // Interaction counters
    int user1_interaction_count{0};
    int user2_interaction_count{0};
    double engagement_rate{0.0};

    // Mutual connection
    int mutual_followers_count{0};

    // Constructors
    Relationship();
    Relationship(const std::string& user1_id, const std::string& user2_id);

    // Relationship state management
    void set_follow_relationship(const std::string& follower_id, const std::string& following_id, bool is_following);
    void set_block_relationship(const std::string& blocker_id, const std::string& blocked_id, bool is_blocked);
    void set_mute_relationship(const std::string& muter_id, const std::string& muted_id, bool is_muted);

    // Queries
    bool are_mutual_friends() const;
    bool is_following(const std::string& follower_id, const std::string& following_id) const;
    bool is_blocked() const;
    bool is_blocked_by(const std::string& user_id) const;
    bool is_blocking(const std::string& user_id) const;
    bool is_muted() const;
    bool is_muted_by(const std::string& user_id) const;

    // Relationship type/status
    enum class RelationshipType {
        NONE,
        FOLLOWING,
        FOLLOWED_BY,
        MUTUAL,
        BLOCKED,
        BLOCKED_BY,
        MUTED,
        CLOSE_FRIENDS,
        PENDING_INCOMING,
        PENDING_OUTGOING,
        RESTRICTED
    };
    RelationshipType get_relationship_type() const;
    std::string get_relationship_status() const;

    // Interactions & metrics
    void record_interaction(const std::string& from_user_id, const std::string& interaction_type, double weight = 1.0);
    double calculate_strength() const;
    double get_engagement_rate() const;

    // Analytics
    json get_analytics_summary() const;
    json get_interaction_metrics() const;

    // Validation
    bool is_valid() const;

    // Serialization
    json to_json() const;
    static Relationship from_json(const json& j);

    // Comparisons
    bool operator==(const Relationship& other) const;
    bool operator!=(const Relationship& other) const { return !(*this == other); }
    bool operator<(const Relationship& other) const;

    // Utilities
    std::string get_display_name() const;
    bool has_any_interaction() const;
    bool is_recent_interaction(int hours) const;
    void update_mutual_followers_count(int count);
    void mark_as_verified();
    void unmark_as_verified();
};

} // namespace sonet::follow::models
