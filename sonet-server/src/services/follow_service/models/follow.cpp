/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "follow.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <cmath>

namespace sonet::follow::models {

using namespace std::chrono;
using json = nlohmann::json;

// ========== CONSTRUCTORS ==========

Follow::Follow()
    : follower_id("")
    , following_id("")
    , follow_type("standard")
    , is_active(true)
    , created_at(system_clock::now())
    , updated_at(system_clock::now())
    , last_interaction_at(system_clock::now())
    , interaction_count(0)
    , engagement_score(0.0)
    , privacy_level("public")
    , is_muted(false)
    , show_retweets(true)
    , show_replies(true)
    , is_close_friend(false)
    , notification_level("all")
    , follow_source("api") {
}

Follow::Follow(const std::string& follower_id, const std::string& following_id, 
               const std::string& follow_type)
    : follower_id(follower_id)
    , following_id(following_id)
    , follow_type(follow_type)
    , is_active(true)
    , created_at(system_clock::now())
    , updated_at(system_clock::now())
    , last_interaction_at(system_clock::now())
    , interaction_count(0)
    , engagement_score(0.0)
    , privacy_level("public")
    , is_muted(false)
    , show_retweets(true)
    , show_replies(true)
    , is_close_friend(false)
    , notification_level("all")
    , follow_source("api") {
    
    spdlog::debug("📝 Creating Follow: {} -> {} (type: {})", follower_id, following_id, follow_type);
}

// ========== ENGAGEMENT TRACKING ==========

void Follow::record_interaction(const std::string& interaction_type, double weight) {
    try {
        interaction_count++;
        last_interaction_at = system_clock::now();
        
        // Update engagement score with weighted contribution
        double interaction_weight = 1.0;
        if (interaction_type == "like") {
            interaction_weight = 1.0;
        } else if (interaction_type == "retweet") {
            interaction_weight = 2.0;
        } else if (interaction_type == "reply") {
            interaction_weight = 3.0;
        } else if (interaction_type == "mention") {
            interaction_weight = 2.5;
        } else if (interaction_type == "direct_message") {
            interaction_weight = 4.0;
        }
        
        // Calculate decay factor based on time since last interaction
        auto hours_since_last = duration_cast<hours>(system_clock::now() - last_interaction_at).count();
        double decay_factor = std::exp(-hours_since_last / 168.0); // Weekly decay
        
        // Update engagement score with exponential moving average
        double new_score = interaction_weight * weight * decay_factor;
        engagement_score = (engagement_score * 0.9) + (new_score * 0.1);
        
        // Ensure score stays within bounds
        engagement_score = std::max(0.0, std::min(100.0, engagement_score));
        
        updated_at = system_clock::now();
        
        spdlog::debug("📊 Interaction recorded: {} -> {} ({}, score: {:.2f})", 
                     follower_id, following_id, interaction_type, engagement_score);
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to record interaction: {} -> {} - {}", 
                     follower_id, following_id, e.what());
    }
}

double Follow::calculate_relationship_strength() const {
    try {
        // Base strength from follow duration
        auto follow_duration_days = duration_cast<hours>(system_clock::now() - created_at).count() / 24.0;
        double duration_factor = std::log(follow_duration_days + 1) / 10.0; // Logarithmic scaling
        
        // Interaction frequency factor
        double interaction_frequency = interaction_count / std::max(1.0, follow_duration_days);
        double frequency_factor = std::min(1.0, interaction_frequency / 10.0); // Cap at 10 interactions/day
        
        // Recency factor
        auto hours_since_interaction = duration_cast<hours>(system_clock::now() - last_interaction_at).count();
        double recency_factor = std::exp(-hours_since_interaction / 168.0); // Weekly decay
        
        // Engagement factor (normalized)
        double engagement_factor = engagement_score / 100.0;
        
        // Special relationship bonuses
        double special_bonus = 0.0;
        if (is_close_friend) special_bonus += 0.2;
        if (follow_type == "mutual") special_bonus += 0.15;
        if (notification_level == "all") special_bonus += 0.1;
        
        // Calculate final strength (0.0 to 1.0)
        double strength = (duration_factor * 0.2 + 
                          frequency_factor * 0.3 + 
                          recency_factor * 0.25 + 
                          engagement_factor * 0.25 + 
                          special_bonus);
        
        return std::max(0.0, std::min(1.0, strength));
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to calculate relationship strength: {} -> {} - {}", 
                     follower_id, following_id, e.what());
        return 0.0;
    }
}

// ========== PRIVACY & SETTINGS ==========

void Follow::update_privacy_settings(const json& settings) {
    try {
        if (settings.contains("is_muted")) {
            is_muted = settings["is_muted"].get<bool>();
        }
        
        if (settings.contains("show_retweets")) {
            show_retweets = settings["show_retweets"].get<bool>();
        }
        
        if (settings.contains("show_replies")) {
            show_replies = settings["show_replies"].get<bool>();
        }
        
        if (settings.contains("is_close_friend")) {
            is_close_friend = settings["is_close_friend"].get<bool>();
        }
        
        if (settings.contains("notification_level")) {
            std::string new_level = settings["notification_level"].get<std::string>();
            if (new_level == "all" || new_level == "important" || 
                new_level == "mentions" || new_level == "off") {
                notification_level = new_level;
            }
        }
        
        if (settings.contains("privacy_level")) {
            std::string new_privacy = settings["privacy_level"].get<std::string>();
            if (new_privacy == "public" || new_privacy == "private" || new_privacy == "restricted") {
                privacy_level = new_privacy;
            }
        }
        
        updated_at = system_clock::now();
        
        spdlog::debug("🔒 Privacy settings updated: {} -> {}", follower_id, following_id);
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to update privacy settings: {} -> {} - {}", 
                     follower_id, following_id, e.what());
    }
}

bool Follow::should_show_content(const std::string& content_type) const {
    try {
        if (is_muted) {
            return false;
        }
        
        if (content_type == "retweet" && !show_retweets) {
            return false;
        }
        
        if (content_type == "reply" && !show_replies) {
            return false;
        }
        
        if (privacy_level == "private" && !is_close_friend) {
            return false;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to check content visibility: {} -> {} - {}", 
                     follower_id, following_id, e.what());
        return false;
    }
}

// ========== ANALYTICS ==========

json Follow::get_analytics_summary() const {
    try {
        auto follow_duration_days = duration_cast<hours>(system_clock::now() - created_at).count() / 24.0;
        auto hours_since_interaction = duration_cast<hours>(system_clock::now() - last_interaction_at).count();
        
        return {
            {"follower_id", follower_id},
            {"following_id", following_id},
            {"follow_type", follow_type},
            {"follow_duration_days", follow_duration_days},
            {"interaction_count", interaction_count},
            {"hours_since_last_interaction", hours_since_interaction},
            {"engagement_score", engagement_score},
            {"relationship_strength", calculate_relationship_strength()},
            {"interaction_frequency", interaction_count / std::max(1.0, follow_duration_days)},
            {"is_active", is_active},
            {"is_close_friend", is_close_friend},
            {"is_muted", is_muted},
            {"notification_level", notification_level},
            {"privacy_level", privacy_level},
            {"follow_source", follow_source},
            {"created_at", duration_cast<milliseconds>(created_at.time_since_epoch()).count()},
            {"updated_at", duration_cast<milliseconds>(updated_at.time_since_epoch()).count()},
            {"last_interaction_at", duration_cast<milliseconds>(last_interaction_at.time_since_epoch()).count()}
        };
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to generate analytics summary: {} -> {} - {}", 
                     follower_id, following_id, e.what());
        return json{{"error", e.what()}};
    }
}

// ========== VALIDATION ==========

bool Follow::is_valid() const {
    try {
        // Basic validation
        if (follower_id.empty() || following_id.empty()) {
            return false;
        }
        
        // Cannot follow yourself
        if (follower_id == following_id) {
            return false;
        }
        
        // Valid follow types
        std::vector<std::string> valid_types = {"standard", "close_friend", "mutual", "pending", "requested"};
        if (std::find(valid_types.begin(), valid_types.end(), follow_type) == valid_types.end()) {
            return false;
        }
        
        // Valid notification levels
        std::vector<std::string> valid_notifications = {"all", "important", "mentions", "off"};
        if (std::find(valid_notifications.begin(), valid_notifications.end(), notification_level) == valid_notifications.end()) {
            return false;
        }
        
        // Valid privacy levels
        std::vector<std::string> valid_privacy = {"public", "private", "restricted"};
        if (std::find(valid_privacy.begin(), valid_privacy.end(), privacy_level) == valid_privacy.end()) {
            return false;
        }
        
        // Timestamps should be valid
        if (created_at > system_clock::now() || updated_at > system_clock::now()) {
            return false;
        }
        
        // Engagement score should be in valid range
        if (engagement_score < 0.0 || engagement_score > 100.0) {
            return false;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Follow validation failed: {} -> {} - {}", 
                     follower_id, following_id, e.what());
        return false;
    }
}

// ========== SERIALIZATION ==========

json Follow::to_json() const {
    try {
        return {
            {"follower_id", follower_id},
            {"following_id", following_id},
            {"follow_type", follow_type},
            {"created_at", duration_cast<milliseconds>(created_at.time_since_epoch()).count()},
            {"updated_at", duration_cast<milliseconds>(updated_at.time_since_epoch()).count()},
            {"is_active", is_active},
            {"interaction_count", interaction_count},
            {"last_interaction_at", duration_cast<milliseconds>(last_interaction_at.time_since_epoch()).count()},
            {"follow_source", follow_source},
            {"engagement_score", engagement_score},
            {"relationship_strength", calculate_relationship_strength()},
            {"privacy_level", privacy_level},
            {"settings", {
                {"is_muted", is_muted},
                {"show_retweets", show_retweets},
                {"show_replies", show_replies},
                {"is_close_friend", is_close_friend},
                {"notification_level", notification_level}
            }}
        };
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Follow serialization failed: {} -> {} - {}", 
                     follower_id, following_id, e.what());
        return json{{"error", e.what()}};
    }
}

Follow Follow::from_json(const json& j) {
    try {
        Follow follow;
        
        follow.follower_id = j.value("follower_id", "");
        follow.following_id = j.value("following_id", "");
        follow.follow_type = j.value("follow_type", "standard");
        follow.is_active = j.value("is_active", true);
        follow.interaction_count = j.value("interaction_count", 0);
        follow.follow_source = j.value("follow_source", "api");
        follow.engagement_score = j.value("engagement_score", 0.0);
        follow.privacy_level = j.value("privacy_level", "public");
        
        // Parse timestamps
        if (j.contains("created_at")) {
            follow.created_at = system_clock::from_time_t(j["created_at"].get<int64_t>() / 1000);
        }
        
        if (j.contains("updated_at")) {
            follow.updated_at = system_clock::from_time_t(j["updated_at"].get<int64_t>() / 1000);
        }
        
        if (j.contains("last_interaction_at")) {
            follow.last_interaction_at = system_clock::from_time_t(j["last_interaction_at"].get<int64_t>() / 1000);
        }
        
        // Parse settings
        if (j.contains("settings")) {
            const auto& settings = j["settings"];
            follow.is_muted = settings.value("is_muted", false);
            follow.show_retweets = settings.value("show_retweets", true);
            follow.show_replies = settings.value("show_replies", true);
            follow.is_close_friend = settings.value("is_close_friend", false);
            follow.notification_level = settings.value("notification_level", "all");
        }
        
        return follow;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Follow deserialization failed: {}", e.what());
        return Follow(); // Return default follow
    }
}

// ========== COMPARISON OPERATORS ==========

bool Follow::operator==(const Follow& other) const {
    return follower_id == other.follower_id && 
           following_id == other.following_id;
}

bool Follow::operator!=(const Follow& other) const {
    return !(*this == other);
}

bool Follow::operator<(const Follow& other) const {
    if (follower_id != other.follower_id) {
        return follower_id < other.follower_id;
    }
    return following_id < other.following_id;
}

// ========== UTILITY METHODS ==========

std::string Follow::get_display_name() const {
    return follower_id + " → " + following_id;
}

bool Follow::is_recent(int hours) const {
    auto time_diff = duration_cast<std::chrono::hours>(system_clock::now() - created_at).count();
    return time_diff <= hours;
}

bool Follow::is_active_recently(int hours) const {
    auto time_diff = duration_cast<std::chrono::hours>(system_clock::now() - last_interaction_at).count();
    return time_diff <= hours;
}

double Follow::get_activity_score() const {
    try {
        // Combine engagement score with recency
        auto hours_since_interaction = duration_cast<hours>(system_clock::now() - last_interaction_at).count();
        double recency_factor = std::exp(-hours_since_interaction / 168.0); // Weekly decay
        
        return (engagement_score / 100.0) * recency_factor;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to calculate activity score: {} -> {} - {}", 
                     follower_id, following_id, e.what());
        return 0.0;
    }
}

void Follow::mark_as_close_friend() {
    is_close_friend = true;
    notification_level = "all";
    updated_at = system_clock::now();
    
    spdlog::debug("👥 Marked as close friend: {} -> {}", follower_id, following_id);
}

void Follow::unmark_as_close_friend() {
    is_close_friend = false;
    updated_at = system_clock::now();
    
    spdlog::debug("👥 Unmarked as close friend: {} -> {}", follower_id, following_id);
}

void Follow::mute() {
    is_muted = true;
    notification_level = "off";
    updated_at = system_clock::now();
    
    spdlog::debug("🔇 Follow muted: {} -> {}", follower_id, following_id);
}

void Follow::unmute() {
    is_muted = false;
    notification_level = "all";
    updated_at = system_clock::now();
    
    spdlog::debug("🔊 Follow unmuted: {} -> {}", follower_id, following_id);
}

} // namespace sonet::follow::models
