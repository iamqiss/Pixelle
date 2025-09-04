//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <atomic>
#include <mutex>
#include <random>
// Include service.h first to get grpc_stub.h and define the grpc namespace
#include "service.h"
// Now include stub_protos.h after the grpc namespace is defined
#include "../../../proto/services/stub_protos.h"
#include "clients/grpc_clients.h"

namespace sonet::timeline {

// ============= ML-BASED RANKING ENGINE =============

class MLRankingEngine : public RankingEngine {
public:
    MLRankingEngine();
    ~MLRankingEngine() override = default;

    std::vector<RankedTimelineItem> ScoreNotes(
        const std::vector<::sonet::note::Note>& notes,
        const std::string& user_id,
        const UserEngagementProfile& profile,
        const TimelineConfig& config
    ) override;

    void UpdateUserEngagement(
        const std::string& user_id,
        const std::string& note_id,
        const std::string& action,
        double duration_seconds = 0.0
    ) override;

    void TrainOnEngagementData(
        const std::vector<EngagementEvent>& events
    ) override;

private:
    // Author affinity scoring
    double CalculateAuthorAffinity(
        const std::string& user_id,
        const std::string& author_id,
        const UserEngagementProfile& profile
    );

    // Content quality assessment
    double CalculateContentQuality(
        const ::sonet::note::Note& note,
        const UserEngagementProfile& profile
    );

    // Engagement velocity prediction
    double CalculateEngagementVelocity(
        const ::sonet::note::Note& note
    );

    // Personalization score based on user history
    double CalculatePersonalizationScore(
        const ::sonet::note::Note& note,
        const UserEngagementProfile& profile
    );

    // Recency score with time decay
    double CalculateRecencyScore(
        const ::sonet::note::Note& note,
        double half_life_hours = 6.0
    );

    // Diversity enforcement
    void ApplyDiversityBoosts(
        std::vector<RankedTimelineItem>& items,
        double diversity_factor = 0.15
    );

    // Engagement tracking
    std::unordered_map<std::string, std::unordered_map<std::string, double>> user_author_affinity_;
    std::unordered_map<std::string, std::unordered_set<std::string>> user_engaged_hashtags_;
    std::unordered_map<std::string, double> global_author_scores_;
    std::unordered_map<std::string, std::chrono::system_clock::time_point> last_engagement_time_;
    
    // ML model parameters (simplified)
    double quality_text_length_weight_ = 0.1;
    double quality_media_boost_ = 0.15;
    double quality_link_penalty_ = -0.05;
    double quality_hashtag_boost_ = 0.08;
    double quality_mention_boost_ = 0.12;
    
    // Trending detection
    std::unordered_map<std::string, std::vector<double>> hashtag_velocity_;
    std::unordered_map<std::string, std::vector<double>> author_velocity_;
    
    mutable std::mutex affinity_mutex_;
    mutable std::mutex velocity_mutex_;
};

// ============= REDIS-BASED TIMELINE CACHE =============

class RedisTimelineCache : public TimelineCache {
public:
    explicit RedisTimelineCache(const std::string& redis_host = "localhost", int redis_port = 6379);
    ~RedisTimelineCache() override = default;

    bool GetTimeline(
        const std::string& user_id,
        std::vector<RankedTimelineItem>& items
    ) override;

    void SetTimeline(
        const std::string& user_id,
        const std::vector<RankedTimelineItem>& items,
        std::chrono::seconds ttl = std::chrono::seconds(3600)
    ) override;

    void InvalidateTimeline(const std::string& user_id) override;
    void InvalidateAuthorTimelines(const std::string& author_id) override;

    bool GetUserProfile(
        const std::string& user_id,
        UserEngagementProfile& profile
    ) override;

    void SetUserProfile(
        const std::string& user_id,
        const UserEngagementProfile& profile
    ) override;

    std::chrono::system_clock::time_point GetLastRead(const std::string& user_id) override;
    void SetLastRead(const std::string& user_id, std::chrono::system_clock::time_point timestamp) override;

private:
    std::string SerializeTimelineItems(const std::vector<RankedTimelineItem>& items);
    std::vector<RankedTimelineItem> DeserializeTimelineItems(const std::string& data);
    
    std::string SerializeUserProfile(const UserEngagementProfile& profile);
    UserEngagementProfile DeserializeUserProfile(const std::string& data);

    std::string TimelineKey(const std::string& user_id);
    std::string ProfileKey(const std::string& user_id);
    std::string LastReadKey(const std::string& user_id);
    std::string AuthorFollowersKey(const std::string& author_id);

    // Redis connection parameters
    std::string redis_host_;
    int redis_port_;
    
#ifdef SONET_USE_REDIS_PLUS_PLUS
    std::unique_ptr<sw::redis::Redis> redis_;
#endif
    
    // In-memory fallback cache for when Redis is unavailable
    std::unordered_map<std::string, std::vector<RankedTimelineItem>> memory_timeline_cache_;
    std::unordered_map<std::string, std::chrono::system_clock::time_point> memory_timeline_expiry_;
    std::unordered_map<std::string, UserEngagementProfile> memory_profile_cache_;
    std::unordered_map<std::string, std::chrono::system_clock::time_point> memory_lastread_cache_;
    mutable std::mutex memory_cache_mutex_;
    
    bool redis_available_ = false;
    mutable std::mutex redis_mutex_;
};

// ============= CONTENT FILTER IMPLEMENTATION =============

class AdvancedContentFilter : public ContentFilter {
public:
    AdvancedContentFilter();
    ~AdvancedContentFilter() override = default;

    std::vector<::sonet::note::Note> FilterNotes(
        const std::vector<::sonet::note::Note>& notes,
        const std::string& user_id,
        const UserEngagementProfile& profile
    ) override;

    void UpdateUserPreferences(
        const std::string& user_id,
        const ContentFilterPreferences& preferences
    ) override;

    void AddMutedUser(const std::string& user_id, const std::string& muted_user_id) override;
    void RemoveMutedUser(const std::string& user_id, const std::string& muted_user_id) override;

    void AddMutedKeyword(const std::string& user_id, const std::string& keyword) override;
    void RemoveMutedKeyword(const std::string& user_id, const std::string& keyword) override;

private:
    // Filter implementations
    bool IsUserMuted(const std::string& user_id, const std::string& author_id);
    bool ContainsMutedKeywords(const std::string& user_id, const ::sonet::note::Note& note);
    bool ViolatesContentPolicy(const ::sonet::note::Note& note);
    bool MeetsEngagementThreshold(const ::sonet::note::Note& note, const UserEngagementProfile& profile);
    bool IsAppropriateForUserAge(const ::sonet::note::Note& note, const UserEngagementProfile& profile);
    bool PassesSpamDetection(const ::sonet::note::Note& note);

    // User mute lists
    std::unordered_map<std::string, std::unordered_set<std::string>> muted_users_;
    std::unordered_map<std::string, std::unordered_set<std::string>> muted_keywords_;
    std::unordered_map<std::string, ContentFilterPreferences> user_preferences_;
    
    // Global content policy
    std::unordered_set<std::string> banned_keywords_;
    std::unordered_set<std::string> spam_patterns_;
    
    mutable std::mutex filter_mutex_;
};

// ============= WEBSOCKET REALTIME NOTIFIER =============

class WebSocketRealtimeNotifier : public RealtimeNotifier {
public:
    WebSocketRealtimeNotifier(int port = 8081);
    ~WebSocketRealtimeNotifier() override;

    void NotifyNewItems(
        const std::string& user_id,
        const std::vector<RankedTimelineItem>& items
    ) override;

    void NotifyItemUpdate(
        const std::string& user_id,
        const std::string& item_id,
        const ::sonet::timeline::TimelineUpdate& update
    ) override;

    void NotifyItemDeleted(
        const std::string& user_id,
        const std::string& item_id
    ) override;

    void Subscribe(const std::string& user_id, const std::string& connection_id) override;
    void Unsubscribe(const std::string& user_id, const std::string& connection_id) override;

    // WebSocket server management
    void Start();
    void Stop();
    bool IsRunning() const { return running_; }

private:
    void SendToUser(const std::string& user_id, const std::string& message);
    void BroadcastToAll(const std::string& message);
    
    // Connection management
    struct Connection {
        std::string connection_id;
        std::string user_id;
        std::chrono::system_clock::time_point last_activity;
        bool is_active = true;
    };
    
    std::unordered_map<std::string, std::vector<std::string>> user_connections_;
    std::unordered_map<std::string, Connection> connections_;
    
    int port_;
    std::atomic<bool> running_{false};
    std::thread server_thread_;
    mutable std::mutex connections_mutex_;
};

// ============= CONTENT SOURCE ADAPTERS =============

class FollowingContentAdapter : public ContentSourceAdapter {
public:
    explicit FollowingContentAdapter(std::shared_ptr<::sonet::note::NoteService::Stub> note_service);
    ~FollowingContentAdapter() override = default;

    std::vector<::sonet::note::Note> GetContent(
        const std::string& user_id,
        const TimelineConfig& config,
        std::chrono::system_clock::time_point since,
        int32_t limit
    ) override;

private:
    std::vector<std::string> GetFollowingList(const std::string& user_id);
    
    std::shared_ptr<::sonet::note::NoteService::Stub> note_service_;
    
    // Cache following lists for better performance
    std::unordered_map<std::string, std::vector<std::string>> following_cache_;
    std::unordered_map<std::string, std::chrono::system_clock::time_point> cache_timestamps_;
    mutable std::mutex cache_mutex_;
};

class RecommendedContentAdapter : public ContentSourceAdapter {
public:
    explicit RecommendedContentAdapter(
        std::shared_ptr<::sonet::note::NoteService::Stub> note_service,
        std::shared_ptr<MLRankingEngine> ranking_engine
    );
    ~RecommendedContentAdapter() override = default;

    std::vector<::sonet::note::Note> GetContent(
        const std::string& user_id,
        const TimelineConfig& config,
        std::chrono::system_clock::time_point since,
        int32_t limit
    ) override;

private:
    std::vector<::sonet::note::Note> FindSimilarContent(
        const std::string& user_id,
        const UserEngagementProfile& profile,
        int32_t limit
    );
    
    std::shared_ptr<::sonet::note::NoteService::Stub> note_service_;
    std::shared_ptr<MLRankingEngine> ranking_engine_;
};

// Separation-of-concerns: generic interface for trending providers
class ITrendingProvider {
public:
    virtual ~ITrendingProvider() = default;
    virtual void MaybeRefresh() = 0;
    virtual std::vector<::sonet::note::Note> Get(int32_t limit,
        std::chrono::system_clock::time_point since) = 0;
};

class TrendingHashtagsProvider : public ITrendingProvider {
public:
    TrendingHashtagsProvider();
    void MaybeRefresh() override;
    std::vector<::sonet::note::Note> Get(int32_t limit,
        std::chrono::system_clock::time_point since) override;
private:
    void UpdateTrendingHashtags();
    std::vector<std::string> trending_hashtags_;
    std::chrono::system_clock::time_point last_update_{};
    mutable std::mutex mutex_;
};

class TrendingTopicsProvider : public ITrendingProvider {
public:
    TrendingTopicsProvider();
    void MaybeRefresh() override;
    std::vector<::sonet::note::Note> Get(int32_t limit,
        std::chrono::system_clock::time_point since) override;
private:
    void UpdateTrendingTopics();
    std::vector<std::string> trending_topics_;
    std::chrono::system_clock::time_point last_update_{};
    mutable std::mutex mutex_;
};

class TrendingVideosProvider : public ITrendingProvider {
public:
    explicit TrendingVideosProvider(std::shared_ptr<::sonet::note::NoteService::Stub> note_service);
    void MaybeRefresh() override;
    std::vector<::sonet::note::Note> Get(int32_t limit,
        std::chrono::system_clock::time_point since) override;
private:
    void UpdateTrendingVideos();
    std::vector<std::string> trending_video_urls_;
    std::chrono::system_clock::time_point last_update_{};
    std::shared_ptr<::sonet::note::NoteService::Stub> note_service_;
    mutable std::mutex mutex_;
};

class TrendingContentAdapter : public ContentSourceAdapter {
public:
    explicit TrendingContentAdapter(std::shared_ptr<::sonet::note::NoteService::Stub> note_service);
    ~TrendingContentAdapter() override = default;

    std::vector<::sonet::note::Note> GetContent(
        const std::string& user_id,
        const TimelineConfig& config,
        std::chrono::system_clock::time_point since,
        int32_t limit
    ) override;

private:
    // Providers for different trend modalities
    std::unique_ptr<TrendingHashtagsProvider> hashtags_provider_;
    std::unique_ptr<TrendingTopicsProvider> topics_provider_;
    std::unique_ptr<TrendingVideosProvider> videos_provider_;
    std::shared_ptr<::sonet::note::NoteService::Stub> note_service_;
};

class RealFollowingContentAdapter : public ContentSourceAdapter {
public:
    RealFollowingContentAdapter(std::shared_ptr<clients::NoteClient> note_client,
                                std::shared_ptr<clients::FollowClient> follow_client)
        : note_client_(std::move(note_client)), follow_client_(std::move(follow_client)) {}

    std::vector<::sonet::note::Note> GetContent(
        const std::string& user_id,
        const TimelineConfig& /*config*/,
        std::chrono::system_clock::time_point since,
        int32_t limit
    ) override {
        if (!follow_client_ || !note_client_) return {};
        auto following = follow_client_->GetFollowing(user_id);
        if (following.empty()) return {};
        return note_client_->ListRecentNotesByAuthors(following, since, limit);
    }
private:
    std::shared_ptr<clients::NoteClient> note_client_;
    std::shared_ptr<clients::FollowClient> follow_client_;
};

class RealListsContentAdapter : public ContentSourceAdapter {
public:
    RealListsContentAdapter(std::shared_ptr<clients::NoteClient> note_client)
        : note_client_(std::move(note_client)) {}

    std::vector<::sonet::note::Note> GetContent(
        const std::string& user_id,
        const TimelineConfig& /*config*/,
        std::chrono::system_clock::time_point since,
        int32_t limit
    ) override {
        // Placeholder: in real system, fetch list memberships
        std::vector<std::string> list_authors = {"list_author_a","list_author_b"};
        (void)user_id;
        return note_client_->ListRecentNotesByAuthors(list_authors, since, limit);
    }
private:
    std::shared_ptr<clients::NoteClient> note_client_;
};

// ============= FACTORY FUNCTIONS =============

std::shared_ptr<TimelineServiceImpl> CreateTimelineService(
    const std::string& redis_host = "localhost",
    int redis_port = 6379,
    int websocket_port = 8081,
    std::shared_ptr<::sonet::note::NoteService::Stub> note_service = nullptr
);

} // namespace sonet::timeline
