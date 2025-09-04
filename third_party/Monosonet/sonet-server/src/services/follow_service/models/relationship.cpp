/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "relationship.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <cmath>
#include <algorithm>

namespace sonet::follow::models {

using namespace std::chrono;
using json = nlohmann::json;

// ========== CONSTRUCTORS ==========

Relationship::Relationship()
    : user1_id(""),
      user2_id(""),
      user1_follows_user2(false),
      user2_follows_user1(false),
      user1_blocked_user2(false),
      user2_blocked_user1(false),
      user1_muted_user2(false),
      user2_muted_user1(false),
      is_close_friends(false),
      is_verified_relationship(false),
      created_at(system_clock::now()),
      updated_at(system_clock::now()),
      user1_followed_user2_at(system_clock::time_point::min()),
      user2_followed_user1_at(system_clock::time_point::min()),
      last_interaction_at(system_clock::now()),
      user1_interaction_count(0),
      user2_interaction_count(0),
      mutual_followers_count(0) {
}

Relationship::Relationship(const std::string& user1_id, const std::string& user2_id)
    : user1_id(user1_id),
      user2_id(user2_id),
      user1_follows_user2(false),
      user2_follows_user1(false),
      user1_blocked_user2(false),
      user2_blocked_user1(false),
      user1_muted_user2(false),
      user2_muted_user1(false),
      is_close_friends(false),
      is_verified_relationship(false),
      created_at(system_clock::now()),
      updated_at(system_clock::now()),
      user1_followed_user2_at(system_clock::time_point::min()),
      user2_followed_user1_at(system_clock::time_point::min()),
      last_interaction_at(system_clock::now()),
      user1_interaction_count(0),
      user2_interaction_count(0),
      mutual_followers_count(0) {
    
    spdlog::debug("🔗 Creating Relationship: {} <-> {}", user1_id, user2_id);
}

// ========== RELATIONSHIP STATE MANAGEMENT ==========

void Relationship::set_follow_relationship(const std::string& follower_id, const std::string& following_id, bool is_following) {
    try {
        if (follower_id == user1_id && following_id == user2_id) {
            user1_follows_user2 = is_following;
            if (is_following) {
                user1_followed_user2_at = system_clock::now();
            }
        } else if (follower_id == user2_id && following_id == user1_id) {
            user2_follows_user1 = is_following;
            if (is_following) {
                user2_followed_user1_at = system_clock::now();
            }
        }
        
        // Update close friends status
        is_close_friends = user1_follows_user2 && user2_follows_user1;
        
        updated_at = system_clock::now();
        
        spdlog::debug("🔄 Follow relationship updated: {} -> {} = {}", follower_id, following_id, is_following);
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to set follow relationship: {} -> {} - {}", follower_id, following_id, e.what());
    }
}

void Relationship::set_block_relationship(const std::string& blocker_id, const std::string& blocked_id, bool is_blocked) {
    try {
        if (blocker_id == user1_id && blocked_id == user2_id) {
            user1_blocked_user2 = is_blocked;
            
            // Auto-unfollow when blocking
            if (is_blocked) {
                user1_follows_user2 = false;
                user2_follows_user1 = false;
                is_close_friends = false;
            }
        } else if (blocker_id == user2_id && blocked_id == user1_id) {
            user2_blocked_user1 = is_blocked;
            
            // Auto-unfollow when blocking
            if (is_blocked) {
                user1_follows_user2 = false;
                user2_follows_user1 = false;
                is_close_friends = false;
            }
        }
        
        updated_at = system_clock::now();
        
        spdlog::debug("🚫 Block relationship updated: {} -> {} = {}", blocker_id, blocked_id, is_blocked);
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to set block relationship: {} -> {} - {}", blocker_id, blocked_id, e.what());
    }
}

void Relationship::set_mute_relationship(const std::string& muter_id, const std::string& muted_id, bool is_muted) {
    try {
        if (muter_id == user1_id && muted_id == user2_id) {
            user1_muted_user2 = is_muted;
        } else if (muter_id == user2_id && muted_id == user1_id) {
            user2_muted_user1 = is_muted;
        }
        
        updated_at = system_clock::now();
        
        spdlog::debug("🔇 Mute relationship updated: {} -> {} = {}", muter_id, muted_id, is_muted);
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to set mute relationship: {} -> {} - {}", muter_id, muted_id, e.what());
    }
}

// ========== RELATIONSHIP QUERIES ==========

bool Relationship::are_mutual_friends() const {
    return user1_follows_user2 && user2_follows_user1 && !is_blocked() && !is_muted();
}

bool Relationship::is_following(const std::string& follower_id, const std::string& following_id) const {
    if (follower_id == user1_id && following_id == user2_id) {
        return user1_follows_user2;
    } else if (follower_id == user2_id && following_id == user1_id) {
        return user2_follows_user1;
    }
    return false;
}

bool Relationship::is_blocked() const {
    return user1_blocked_user2 || user2_blocked_user1;
}

bool Relationship::is_blocked_by(const std::string& user_id) const {
    if (user_id == user1_id) {
        return user2_blocked_user1;
    } else if (user_id == user2_id) {
        return user1_blocked_user2;
    }
    return false;
}

bool Relationship::is_blocking(const std::string& user_id) const {
    if (user_id == user1_id) {
        return user1_blocked_user2;
    } else if (user_id == user2_id) {
        return user2_blocked_user1;
    }
    return false;
}

bool Relationship::is_muted() const {
    return user1_muted_user2 || user2_muted_user1;
}

bool Relationship::is_muted_by(const std::string& user_id) const {
    if (user_id == user1_id) {
        return user1_muted_user2;
    } else if (user_id == user2_id) {
        return user2_muted_user1;
    }
    return false;
}

Relationship::RelationshipType Relationship::get_relationship_type() const {
    try {
        // Check for blocking first
        if (user1_blocked_user2) return RelationshipType::BLOCKED;
        if (user2_blocked_user1) return RelationshipType::BLOCKED_BY;
        
        // Check for following relationships
        if (user1_follows_user2 && user2_follows_user1) {
            if (is_close_friends) {
                return RelationshipType::CLOSE_FRIENDS;
            }
            return RelationshipType::MUTUAL;
        } else if (user1_follows_user2) {
            return RelationshipType::FOLLOWING;
        } else if (user2_follows_user1) {
            return RelationshipType::FOLLOWED_BY;
        }
        
        // Check for muting
        if (user1_muted_user2 || user2_muted_user1) {
            return RelationshipType::MUTED;
        }
        
        return RelationshipType::NONE;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to get relationship type: {} <-> {} - {}", user1_id, user2_id, e.what());
        return RelationshipType::NONE;
    }
}

std::string Relationship::get_relationship_status() const {
    try {
        auto type = get_relationship_type();
        
        switch (type) {
            case RelationshipType::NONE: return "none";
            case RelationshipType::FOLLOWING: return "following";
            case RelationshipType::FOLLOWED_BY: return "followed_by";
            case RelationshipType::MUTUAL: return "mutual";
            case RelationshipType::BLOCKED: return "blocked";
            case RelationshipType::BLOCKED_BY: return "blocked_by";
            case RelationshipType::MUTED: return "muted";
            case RelationshipType::CLOSE_FRIENDS: return "close_friends";
            case RelationshipType::PENDING_INCOMING: return "pending_incoming";
            case RelationshipType::PENDING_OUTGOING: return "pending_outgoing";
            case RelationshipType::RESTRICTED: return "restricted";
            default: return "unknown";
        }
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to get relationship status: {} <-> {} - {}", user1_id, user2_id, e.what());
        return "error";
    }
}

// ========== INTERACTION TRACKING ==========

void Relationship::record_interaction(const std::string& from_user_id, const std::string& interaction_type, double weight) {
    try {
        if (from_user_id == user1_id) {
            user1_interaction_count++;
        } else if (from_user_id == user2_id) {
            user2_interaction_count++;
        }
        
        last_interaction_at = system_clock::now();
        updated_at = system_clock::now();
        
        // Update relationship strength based on interaction
        double interaction_weight = 1.0;
        if (interaction_type == "like") interaction_weight = 1.0;
        else if (interaction_type == "retweet") interaction_weight = 2.0;
        else if (interaction_type == "reply") interaction_weight = 3.0;
        else if (interaction_type == "mention") interaction_weight = 2.5;
        else if (interaction_type == "direct_message") interaction_weight = 4.0;
        
        // Update engagement rate with exponential moving average
        double new_engagement = interaction_weight * weight;
        engagement_rate = (engagement_rate * 0.9) + (new_engagement * 0.1);
        
        spdlog::debug("💬 Interaction recorded: {} <-> {} ({}, weight: {:.2f})", 
                     user1_id, user2_id, interaction_type, interaction_weight);
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to record interaction: {} <-> {} - {}", user1_id, user2_id, e.what());
    }
}

double Relationship::calculate_strength() const {
    try {
        // Base strength from mutual following
        double base_strength = 0.0;
        if (are_mutual_friends()) {
            base_strength = 0.5;
        } else if (user1_follows_user2 || user2_follows_user1) {
            base_strength = 0.3;
        }
        
        // Interaction factor
        double total_interactions = user1_interaction_count + user2_interaction_count;
        double interaction_factor = std::min(1.0, total_interactions / 100.0); // Cap at 100 interactions
        
        // Recency factor
        auto hours_since_interaction = duration_cast<hours>(system_clock::now() - last_interaction_at).count();
        double recency_factor = std::exp(-hours_since_interaction / 168.0); // Weekly decay
        
        // Duration factor (how long they've been connected)
        auto relationship_duration_days = duration_cast<hours>(system_clock::now() - created_at).count() / 24.0;
        double duration_factor = std::log(relationship_duration_days + 1) / 10.0;
        
        // Mutual connections factor
        double mutual_factor = std::min(1.0, mutual_followers_count / 50.0); // Cap at 50 mutual followers
        
        // Special relationship bonuses
        double special_bonus = 0.0;
        if (is_close_friends) special_bonus += 0.2;
        if (is_verified_relationship) special_bonus += 0.1;
        
        // Penalties
        double penalty = 0.0;
        if (is_blocked()) penalty = -1.0; // Complete penalty
        if (is_muted()) penalty = -0.3;
        
        // Calculate final strength (0.0 to 1.0)
        double strength = base_strength + 
                         (interaction_factor * 0.25) + 
                         (recency_factor * 0.15) + 
                         (duration_factor * 0.1) + 
                         (mutual_factor * 0.1) + 
                         special_bonus + 
                         penalty;
        
        return std::max(0.0, std::min(1.0, strength));
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to calculate relationship strength: {} <-> {} - {}", user1_id, user2_id, e.what());
        return 0.0;
    }
}

double Relationship::get_engagement_rate() const {
    return engagement_rate;
}

// ========== ANALYTICS ==========

json Relationship::get_analytics_summary() const {
    try {
        auto relationship_duration_days = duration_cast<hours>(system_clock::now() - created_at).count() / 24.0;
        auto hours_since_interaction = duration_cast<hours>(system_clock::now() - last_interaction_at).count();
        
        return {
            {"user1_id", user1_id},
            {"user2_id", user2_id},
            {"relationship_type", get_relationship_status()},
            {"relationship_strength", calculate_strength()},
            {"engagement_rate", engagement_rate},
            {"relationship_duration_days", relationship_duration_days},
            {"total_interactions", user1_interaction_count + user2_interaction_count},
            {"hours_since_last_interaction", hours_since_interaction},
            {"mutual_followers_count", mutual_followers_count},
            {"flags", {
                {"user1_follows_user2", user1_follows_user2},
                {"user2_follows_user1", user2_follows_user1},
                {"are_mutual_friends", are_mutual_friends()},
                {"user1_blocked_user2", user1_blocked_user2},
                {"user2_blocked_user1", user2_blocked_user1},
                {"user1_muted_user2", user1_muted_user2},
                {"user2_muted_user1", user2_muted_user1},
                {"is_close_friends", is_close_friends},
                {"is_verified_relationship", is_verified_relationship}
            }},
            {"timestamps", {
                {"created_at", duration_cast<milliseconds>(created_at.time_since_epoch()).count()},
                {"updated_at", duration_cast<milliseconds>(updated_at.time_since_epoch()).count()},
                {"user1_followed_user2_at", user1_followed_user2_at != system_clock::time_point::min() ? 
                    duration_cast<milliseconds>(user1_followed_user2_at.time_since_epoch()).count() : 0},
                {"user2_followed_user1_at", user2_followed_user1_at != system_clock::time_point::min() ? 
                    duration_cast<milliseconds>(user2_followed_user1_at.time_since_epoch()).count() : 0},
                {"last_interaction_at", duration_cast<milliseconds>(last_interaction_at.time_since_epoch()).count()}
            }}
        };
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to generate analytics summary: {} <-> {} - {}", user1_id, user2_id, e.what());
        return json{{"error", e.what()}};
    }
}

json Relationship::get_interaction_metrics() const {
    try {
        auto relationship_duration_days = duration_cast<hours>(system_clock::now() - created_at).count() / 24.0;
        
        return {
            {"user1_interaction_count", user1_interaction_count},
            {"user2_interaction_count", user2_interaction_count},
            {"total_interactions", user1_interaction_count + user2_interaction_count},
            {"user1_interaction_rate", user1_interaction_count / std::max(1.0, relationship_duration_days)},
            {"user2_interaction_rate", user2_interaction_count / std::max(1.0, relationship_duration_days)},
            {"combined_interaction_rate", (user1_interaction_count + user2_interaction_count) / std::max(1.0, relationship_duration_days)},
            {"engagement_rate", engagement_rate},
            {"relationship_strength", calculate_strength()}
        };
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Failed to generate interaction metrics: {} <-> {} - {}", user1_id, user2_id, e.what());
        return json{{"error", e.what()}};
    }
}

// ========== VALIDATION ==========

bool Relationship::is_valid() const {
    try {
        // Basic validation
        if (user1_id.empty() || user2_id.empty()) {
            return false;
        }
        
        // Cannot have relationship with yourself
        if (user1_id == user2_id) {
            return false;
        }
        
        // Logical consistency checks
        if (user1_blocked_user2 && user1_follows_user2) {
            return false; // Cannot follow someone you've blocked
        }
        
        if (user2_blocked_user1 && user2_follows_user1) {
            return false; // Cannot follow someone you've blocked
        }
        
        if (is_close_friends && !are_mutual_friends()) {
            return false; // Close friends must be mutual
        }
        
        // Timestamps should be valid
        if (created_at > system_clock::now() || updated_at > system_clock::now()) {
            return false;
        }
        
        // Engagement rate should be in valid range
        if (engagement_rate < 0.0) {
            return false;
        }
        
        // Interaction counts should be non-negative
        if (user1_interaction_count < 0 || user2_interaction_count < 0) {
            return false;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Relationship validation failed: {} <-> {} - {}", user1_id, user2_id, e.what());
        return false;
    }
}

// ========== SERIALIZATION ==========

json Relationship::to_json() const {
    try {
        return {
            {"user1_id", user1_id},
            {"user2_id", user2_id},
            {"relationship_type", get_relationship_status()},
            {"relationship_strength", calculate_strength()},
            {"flags", {
                {"user1_follows_user2", user1_follows_user2},
                {"user2_follows_user1", user2_follows_user1},
                {"user1_blocked_user2", user1_blocked_user2},
                {"user2_blocked_user1", user2_blocked_user1},
                {"user1_muted_user2", user1_muted_user2},
                {"user2_muted_user1", user2_muted_user1},
                {"is_close_friends", is_close_friends},
                {"is_verified_relationship", is_verified_relationship}
            }},
            {"metrics", {
                {"user1_interaction_count", user1_interaction_count},
                {"user2_interaction_count", user2_interaction_count},
                {"total_interactions", user1_interaction_count + user2_interaction_count},
                {"engagement_rate", engagement_rate},
                {"mutual_followers_count", mutual_followers_count}
            }},
            {"timestamps", {
                {"created_at", duration_cast<milliseconds>(created_at.time_since_epoch()).count()},
                {"updated_at", duration_cast<milliseconds>(updated_at.time_since_epoch()).count()},
                {"user1_followed_user2_at", user1_followed_user2_at != system_clock::time_point::min() ? 
                    duration_cast<milliseconds>(user1_followed_user2_at.time_since_epoch()).count() : 0},
                {"user2_followed_user1_at", user2_followed_user1_at != system_clock::time_point::min() ? 
                    duration_cast<milliseconds>(user2_followed_user1_at.time_since_epoch()).count() : 0},
                {"last_interaction_at", duration_cast<milliseconds>(last_interaction_at.time_since_epoch()).count()}
            }}
        };
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Relationship serialization failed: {} <-> {} - {}", user1_id, user2_id, e.what());
        return json{{"error", e.what()}};
    }
}

Relationship Relationship::from_json(const json& j) {
    try {
        Relationship relationship;
        
        relationship.user1_id = j.value("user1_id", "");
        relationship.user2_id = j.value("user2_id", "");
        
        // Parse flags
        if (j.contains("flags")) {
            const auto& flags = j["flags"];
            relationship.user1_follows_user2 = flags.value("user1_follows_user2", false);
            relationship.user2_follows_user1 = flags.value("user2_follows_user1", false);
            relationship.user1_blocked_user2 = flags.value("user1_blocked_user2", false);
            relationship.user2_blocked_user1 = flags.value("user2_blocked_user1", false);
            relationship.user1_muted_user2 = flags.value("user1_muted_user2", false);
            relationship.user2_muted_user1 = flags.value("user2_muted_user1", false);
            relationship.is_close_friends = flags.value("is_close_friends", false);
            relationship.is_verified_relationship = flags.value("is_verified_relationship", false);
        }
        
        // Parse metrics
        if (j.contains("metrics")) {
            const auto& metrics = j["metrics"];
            relationship.user1_interaction_count = metrics.value("user1_interaction_count", 0);
            relationship.user2_interaction_count = metrics.value("user2_interaction_count", 0);
            relationship.engagement_rate = metrics.value("engagement_rate", 0.0);
            relationship.mutual_followers_count = metrics.value("mutual_followers_count", 0);
        }
        
        // Parse timestamps
        if (j.contains("timestamps")) {
            const auto& timestamps = j["timestamps"];
            
            if (timestamps.contains("created_at")) {
                relationship.created_at = system_clock::from_time_t(timestamps["created_at"].get<int64_t>() / 1000);
            }
            
            if (timestamps.contains("updated_at")) {
                relationship.updated_at = system_clock::from_time_t(timestamps["updated_at"].get<int64_t>() / 1000);
            }
            
            if (timestamps.contains("user1_followed_user2_at") && timestamps["user1_followed_user2_at"].get<int64_t>() > 0) {
                relationship.user1_followed_user2_at = system_clock::from_time_t(timestamps["user1_followed_user2_at"].get<int64_t>() / 1000);
            }
            
            if (timestamps.contains("user2_followed_user1_at") && timestamps["user2_followed_user1_at"].get<int64_t>() > 0) {
                relationship.user2_followed_user1_at = system_clock::from_time_t(timestamps["user2_followed_user1_at"].get<int64_t>() / 1000);
            }
            
            if (timestamps.contains("last_interaction_at")) {
                relationship.last_interaction_at = system_clock::from_time_t(timestamps["last_interaction_at"].get<int64_t>() / 1000);
            }
        }
        
        return relationship;
        
    } catch (const std::exception& e) {
        spdlog::error("❌ Relationship deserialization failed: {}", e.what());
        return Relationship(); // Return default relationship
    }
}

// ========== COMPARISON OPERATORS ==========

bool Relationship::operator==(const Relationship& other) const {
    return (user1_id == other.user1_id && user2_id == other.user2_id) ||
           (user1_id == other.user2_id && user2_id == other.user1_id);
}


bool Relationship::operator<(const Relationship& other) const {
    if (user1_id != other.user1_id) {
        return user1_id < other.user1_id;
    }
    return user2_id < other.user2_id;
}

// ========== UTILITY METHODS ==========

std::string Relationship::get_display_name() const {
    return user1_id + " ↔ " + user2_id;
}

bool Relationship::has_any_interaction() const {
    return (user1_interaction_count + user2_interaction_count) > 0;
}

bool Relationship::is_recent_interaction(int hours) const {
    auto time_diff = duration_cast<std::chrono::hours>(system_clock::now() - last_interaction_at).count();
    return time_diff <= hours;
}

void Relationship::update_mutual_followers_count(int count) {
    mutual_followers_count = std::max(0, count);
    updated_at = system_clock::now();
}

void Relationship::mark_as_verified() {
    is_verified_relationship = true;
    updated_at = system_clock::now();
    
    spdlog::debug("✅ Relationship marked as verified: {} <-> {}", user1_id, user2_id);
}

void Relationship::unmark_as_verified() {
    is_verified_relationship = false;
    updated_at = system_clock::now();
    
    spdlog::debug("❌ Relationship unmarked as verified: {} <-> {}", user1_id, user2_id);
}

} // namespace sonet::follow::models
