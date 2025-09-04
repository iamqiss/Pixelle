/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <optional>
#include <chrono>
#include <nlohmann/json.hpp>

namespace sonet::follow::models {

using json = nlohmann::json;
using Clock = std::chrono::system_clock;

class Follow {
public:
    // Core identification
    std::string follower_id;
    std::string following_id;

    // Relationship metadata
    std::string follow_type;
    bool is_active{true};

    // Timestamps
    Clock::time_point created_at;
    Clock::time_point updated_at;
    Clock::time_point last_interaction_at;

    // Engagement
    int interaction_count{0};
    double engagement_score{0.0};

    // Privacy and settings
    std::string privacy_level;         // "public" | "private" | "restricted"
    bool is_muted{false};
    bool show_retweets{true};
    bool show_replies{true};
    bool is_close_friend{false};
    std::string notification_level;    // "all" | "important" | "mentions" | "off"

    // Analytics metadata
    std::string follow_source;         // e.g., "api"

    // Constructors
    Follow();
    Follow(const std::string& follower_id, const std::string& following_id, const std::string& follow_type);

    // Engagement tracking
    void record_interaction(const std::string& interaction_type, double weight = 1.0);
    double calculate_relationship_strength() const;

    // Privacy & settings
    void update_privacy_settings(const json& settings);
    bool should_show_content(const std::string& content_type) const;
    void mark_as_close_friend();
    void unmark_as_close_friend();
    void mute();
    void unmute();

    // Validation
    bool is_valid() const;

    // Serialization
    json to_json() const;
    static Follow from_json(const json& j);

    // Analytics summary
    json get_analytics_summary() const;

    // Comparisons
    bool operator==(const Follow& other) const;
    bool operator!=(const Follow& other) const;
    bool operator<(const Follow& other) const;

    // Utilities
    std::string get_display_name() const;
    bool is_recent(int hours) const;
    bool is_active_recently(int hours) const;
    double get_activity_score() const;
};

} // namespace sonet::follow::models
