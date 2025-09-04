//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "implementations.h"
#include <iostream>
#include <sstream>
#include <chrono>
#include <algorithm>
#ifdef SONET_USE_REDIS_PLUS_PLUS
#include <sw/redis++/redis++.h>
#endif

namespace sonet::timeline {

namespace {
    // Helper to convert system_clock::time_point to protobuf timestamp
    ::sonet::common::Timestamp ToProtoTimestamp(std::chrono::system_clock::time_point tp) {
        ::sonet::common::Timestamp result;
        auto duration = tp.time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);
        
        result.set_seconds(seconds.count());
        result.set_nanos(static_cast<int32_t>(nanos.count()));
        return result;
    }

    // Helper to convert protobuf timestamp to system_clock::time_point
    std::chrono::system_clock::time_point FromProtoTimestamp(const ::sonet::common::Timestamp& ts) {
        auto duration = std::chrono::seconds(ts.seconds()) + std::chrono::nanoseconds(ts.nanos());
        return std::chrono::system_clock::time_point(duration);
    }

    // Simple JSON-like serialization for cache data
    std::string EscapeString(const std::string& str) {
        std::string escaped;
        escaped.reserve(str.length() + 20);
        
        for (char c : str) {
            switch (c) {
                case '"': escaped += "\\\""; break;
                case '\\': escaped += "\\\\"; break;
                case '\n': escaped += "\\n"; break;
                case '\r': escaped += "\\r"; break;
                case '\t': escaped += "\\t"; break;
                default: escaped += c; break;
            }
        }
        return escaped;
    }

    std::string UnescapeString(const std::string& str) {
        std::string unescaped;
        unescaped.reserve(str.length());
        
        for (size_t i = 0; i < str.length(); ++i) {
            if (str[i] == '\\' && i + 1 < str.length()) {
                switch (str[i + 1]) {
                    case '"': unescaped += '"'; ++i; break;
                    case '\\': unescaped += '\\'; ++i; break;
                    case 'n': unescaped += '\n'; ++i; break;
                    case 'r': unescaped += '\r'; ++i; break;
                    case 't': unescaped += '\t'; ++i; break;
                    default: unescaped += str[i]; break;
                }
            } else {
                unescaped += str[i];
            }
        }
        return unescaped;
    }
}

// ============= REDIS TIMELINE CACHE IMPLEMENTATION =============

RedisTimelineCache::RedisTimelineCache(const std::string& redis_host, int redis_port)
    : redis_host_(redis_host), redis_port_(redis_port) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    try {
        auto uri = std::string("tcp://") + redis_host_ + ":" + std::to_string(redis_port_);
        redis_ = std::make_unique<sw::redis::Redis>(uri);
        redis_available_ = true;
    } catch (...) {
        redis_available_ = false;
    }
#else
    const char* use_redis = std::getenv("SONET_USE_REDIS");
    if (use_redis && std::string(use_redis) == "1") {
        std::cout << "Attempting Redis connection at " << redis_host_ << ":" << redis_port_ << std::endl;
        redis_available_ = false;
    } else {
        redis_available_ = false;
    }
#endif
    
    std::cout << "Redis Timeline Cache initialized (" 
              << (redis_available_ ? "redis" : "fallback mode") << ") - Redis at " 
              << redis_host_ << ":" << redis_port_ << std::endl;
}

bool RedisTimelineCache::GetTimeline(
    const std::string& user_id,
    std::vector<RankedTimelineItem>& items
) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    if (redis_available_ && redis_) {
        try {
            auto key = TimelineKey(user_id);
            auto val = redis_->get(key);
            if (val) {
                items = DeserializeTimelineItems(*val);
                return true;
            }
        } catch (...) {
            // fall through to memory fallback
        }
    }
#endif
    std::lock_guard<std::mutex> lock(memory_cache_mutex_);
    // Purge expired entries lazily on access
    auto expiry_it = memory_timeline_expiry_.find(user_id);
    if (expiry_it != memory_timeline_expiry_.end()) {
        if (std::chrono::system_clock::now() >= expiry_it->second) {
            memory_timeline_cache_.erase(user_id);
            memory_timeline_expiry_.erase(expiry_it);
        }
    }
    auto it = memory_timeline_cache_.find(user_id);
    if (it != memory_timeline_cache_.end()) { items = it->second; return true; }
    return false;
}

void RedisTimelineCache::SetTimeline(
    const std::string& user_id,
    const std::vector<RankedTimelineItem>& items,
    std::chrono::seconds ttl
) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    if (redis_available_ && redis_) {
        try {
            auto key = TimelineKey(user_id);
            auto s = SerializeTimelineItems(items);
            redis_->setex(key, static_cast<long long>(ttl.count()), s);
            return;
        } catch (...) {}
    }
#endif
    std::lock_guard<std::mutex> lock(memory_cache_mutex_);
    memory_timeline_cache_[user_id] = items;
    memory_timeline_expiry_[user_id] = std::chrono::system_clock::now() + ttl;
}

void RedisTimelineCache::InvalidateTimeline(const std::string& user_id) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    if (redis_available_ && redis_) {
        try { redis_->del(TimelineKey(user_id)); } catch (...) {}
    }
#endif
    std::lock_guard<std::mutex> lock(memory_cache_mutex_);
    memory_timeline_cache_.erase(user_id);
    memory_timeline_expiry_.erase(user_id);
}

void RedisTimelineCache::InvalidateAuthorTimelines(const std::string& author_id) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    // For production, we'd use author->followers mapping keys; skip here
#endif
    std::lock_guard<std::mutex> lock(memory_cache_mutex_);
    for (auto it = memory_timeline_cache_.begin(); it != memory_timeline_cache_.end();) {
        bool has_author_content = false;
        for (const auto& item : it->second) {
            if (item.note.author_id() == author_id) { has_author_content = true; break; }
        }
        if (has_author_content) {
            memory_timeline_expiry_.erase(it->first);
            it = memory_timeline_cache_.erase(it);
        } else { ++it; }
    }
}

bool RedisTimelineCache::GetUserProfile(
    const std::string& user_id,
    UserEngagementProfile& profile
) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    if (redis_available_ && redis_) {
        try {
            auto val = redis_->get(ProfileKey(user_id));
            if (val) { profile = DeserializeUserProfile(*val); return true; }
        } catch (...) {}
    }
#endif
    std::lock_guard<std::mutex> lock(memory_cache_mutex_);
    auto it = memory_profile_cache_.find(user_id);
    if (it != memory_profile_cache_.end()) { profile = it->second; return true; }
    return false;
}

void RedisTimelineCache::SetUserProfile(
    const std::string& user_id,
    const UserEngagementProfile& profile
) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    if (redis_available_ && redis_) {
        try { redis_->set(ProfileKey(user_id), SerializeUserProfile(profile)); return; } catch (...) {}
    }
#endif
    std::lock_guard<std::mutex> lock(memory_cache_mutex_);
    memory_profile_cache_[user_id] = profile;
}

std::chrono::system_clock::time_point RedisTimelineCache::GetLastRead(const std::string& user_id) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    if (redis_available_ && redis_) {
        try {
            auto val = redis_->get(LastReadKey(user_id));
            if (val) {
                long long secs = std::stoll(*val);
                return std::chrono::system_clock::time_point(std::chrono::seconds(secs));
            }
        } catch (...) {}
    }
#endif
    std::lock_guard<std::mutex> lock(memory_cache_mutex_);
    auto it = memory_lastread_cache_.find(user_id);
    if (it != memory_lastread_cache_.end()) return it->second;
    return std::chrono::system_clock::time_point{};
}

void RedisTimelineCache::SetLastRead(const std::string& user_id, std::chrono::system_clock::time_point timestamp) {
#ifdef SONET_USE_REDIS_PLUS_PLUS
    if (redis_available_ && redis_) {
        try {
            auto secs = std::chrono::duration_cast<std::chrono::seconds>(timestamp.time_since_epoch()).count();
            redis_->set(LastReadKey(user_id), std::to_string(secs));
            return;
        } catch (...) {}
    }
#endif
    std::lock_guard<std::mutex> lock(memory_cache_mutex_);
    memory_lastread_cache_[user_id] = timestamp;
}

// ============= SERIALIZATION METHODS =============

std::string RedisTimelineCache::SerializeTimelineItems(const std::vector<RankedTimelineItem>& items) {
    std::ostringstream oss;
    oss << "[";
    
    for (size_t i = 0; i < items.size(); ++i) {
        if (i > 0) oss << ",";
        
        const auto& item = items[i];
        oss << "{"
            << "\"note_id\":\"" << EscapeString(item.note.id()) << "\"," 
            << "\"author_id\":\"" << EscapeString(item.note.author_id()) << "\"," 
            << "\"content\":\"" << EscapeString(item.note.content()) << "\"," 
            << "\"source\":" << static_cast<int>(item.source) << ","
            << "\"final_score\":" << item.final_score << ","
            << "\"injection_reason\":\"" << EscapeString(item.injection_reason) << "\"," 
            << "\"created_at\":" << item.note.created_at().seconds()
            << "}";
    }
    
    oss << "]";
    return oss.str();
}

std::vector<RankedTimelineItem> RedisTimelineCache::DeserializeTimelineItems(const std::string& /*data*/) {
    std::vector<RankedTimelineItem> items;
    
    // TODO: Implement proper JSON parsing
    // For now, return empty vector as placeholder
    // In production, use a JSON library like nlohmann/json or rapidjson
    
    return items;
}

std::string RedisTimelineCache::SerializeUserProfile(const UserEngagementProfile& profile) {
    std::ostringstream oss;
    oss << "{"
        << "\"user_id\":\"" << EscapeString(profile.user_id) << "\"," 
        << "\"avg_session_length_minutes\":" << profile.avg_session_length_minutes << ","
        << "\"daily_engagement_score\":" << profile.daily_engagement_score << ","
        << "\"last_updated\":" << std::chrono::duration_cast<std::chrono::seconds>(
               profile.last_updated.time_since_epoch()).count()
        << "}";
    
    return oss.str();
}

UserEngagementProfile RedisTimelineCache::DeserializeUserProfile(const std::string& /*data*/) {
    UserEngagementProfile profile;
    
    // TODO: Implement proper JSON parsing
    // For now, return empty profile as placeholder
    
    return profile;
}

// ============= KEY GENERATION METHODS =============

std::string RedisTimelineCache::TimelineKey(const std::string& user_id) {
    return "timeline:" + user_id;
}

std::string RedisTimelineCache::ProfileKey(const std::string& user_id) {
    return "profile:" + user_id;
}

std::string RedisTimelineCache::LastReadKey(const std::string& user_id) {
    return "lastread:" + user_id;
}

std::string RedisTimelineCache::AuthorFollowersKey(const std::string& author_id) {
    return "followers:" + author_id;
}

} // namespace sonet::timeline
