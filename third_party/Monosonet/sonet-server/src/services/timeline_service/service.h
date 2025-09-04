//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <mutex>
#include <queue>
#include <future>
#include <atomic>
#include <shared_mutex>
#include <condition_variable>
#include <deque>

// Use real types; concrete proto types will be included where needed via CMake
namespace grpc { class Server; class ServerBuilder; class ServerContext; }

namespace sonet { namespace note { class Note; } }
namespace sonet { namespace timeline { struct ContentSource; struct RankingSignals; } }

namespace sonet::timeline {

// Forward declarations
class TimelineCache;
class RankingEngine;
class ContentFilter;
class RealtimeNotifier;
class ContentSourceAdapter;
class OverdriveClient; // Forward declaration for external ranker client

// Content filter preferences
struct ContentFilterPreferences {
    bool filter_nsfw = true;
    bool filter_spoilers = true;
    bool filter_violence = false;
    std::vector<std::string> blocked_keywords;
    std::vector<std::string> blocked_users;
};

// Timeline item with computed ranking data
struct RankedTimelineItem {
    ::sonet::note::Note note;
    ::sonet::timeline::ContentSource source;
    ::sonet::timeline::RankingSignals signals;
    double final_score = 0.0;
    std::chrono::system_clock::time_point injected_at{};
    std::string injection_reason;
    
    // For sorting by score
    bool operator<(const RankedTimelineItem& other) const {
        return final_score < other.final_score;
    }
};

// User engagement profile for personalization - defined in real protos

// Timeline generation configuration
struct TimelineConfig {
    ::sonet::timeline::TimelineAlgorithm algorithm = ::sonet::timeline::TIMELINE_ALGORITHM_HYBRID;
    int32_t max_items = 50;
    int32_t max_age_hours = 24;
    double min_score_threshold = 0.1;
    
    // Algorithm weights
    double recency_weight = 0.3;
    double engagement_weight = 0.25;
    double author_affinity_weight = 0.2;
    double content_quality_weight = 0.15;
    double diversity_weight = 0.1;
    
    // Content mix ratios
    double following_content_ratio = 0.7;
    double recommended_content_ratio = 0.2;
    double trending_content_ratio = 0.08;
    double lists_content_ratio = 0.02;

    // Per-source caps
    int32_t cap_following = 100;
    int32_t cap_recommended = 50;
    int32_t cap_trending = 30;
    int32_t cap_lists = 20;

    // A/B weighting parameters for source mixing
    double ab_following_weight = 1.0;
    double ab_recommended_weight = 1.0;
    double ab_trending_weight = 1.0;
    double ab_lists_weight = 1.0;
};

// Engagement event for ML training
struct EngagementEvent {
    std::string user_id;
    std::string author_id;
    std::string note_id;
    std::string action; // like, renote, reply, follow, hide
    double duration_seconds = 0.0;
    std::chrono::system_clock::time_point timestamp{};
};

// Content ranking engine interface
class RankingEngine {
public:
    virtual ~RankingEngine() = default;
    
    // Score a single note for a user (default implementation uses batch scorer)
    virtual double ScoreNote(
        const ::sonet::note::Note& note,
        const std::string& user_id,
        const UserEngagementProfile& profile,
        const TimelineConfig& config
    ) {
        std::vector<::sonet::note::Note> single{note};
        auto scored = ScoreNotes(single, user_id, profile, config);
        return scored.empty() ? 0.0 : scored.front().final_score;
    }
    
    // Batch score multiple notes
    virtual std::vector<RankedTimelineItem> ScoreNotes(
        const std::vector<::sonet::note::Note>& notes,
        const std::string& user_id,
        const UserEngagementProfile& profile,
        const TimelineConfig& config
    ) = 0;
    
    // Update ML models with user feedback
    virtual void UpdateUserEngagement(
        const std::string& user_id,
        const std::string& note_id,
        const std::string& action,  // "like", "renote", "reply", "follow", "hide"
        double duration_seconds
    ) = 0;

    // Train from historical engagement data
    virtual void TrainOnEngagementData(
        const std::vector<EngagementEvent>& events
    ) = 0;
};

// Content filtering for privacy, safety, and preferences
class ContentFilter {
public:
    virtual ~ContentFilter() = default;
    
    // Check if note should be shown to user (default allow)
    virtual bool ShouldShowNote(
        const ::sonet::note::Note& /*note*/,
        const std::string& /*user_id*/,
        const UserEngagementProfile& /*profile*/
    ) { return true; }
    
    // Filter out inappropriate content
    virtual std::vector<::sonet::note::Note> FilterNotes(
        const std::vector<::sonet::note::Note>& notes,
        const std::string& user_id,
        const UserEngagementProfile& profile
    ) = 0;

    // Preference/mute management
    virtual void UpdateUserPreferences(
        const std::string& user_id,
        const ContentFilterPreferences& preferences
    ) = 0;

    virtual void AddMutedUser(const std::string& user_id, const std::string& muted_user_id) = 0;
    virtual void RemoveMutedUser(const std::string& user_id, const std::string& muted_user_id) = 0;
    virtual void AddMutedKeyword(const std::string& user_id, const std::string& keyword) = 0;
    virtual void RemoveMutedKeyword(const std::string& user_id, const std::string& keyword) = 0;
};

// Timeline caching layer
class TimelineCache {
public:
    virtual ~TimelineCache() = default;
    
    // Cache operations
    virtual bool GetTimeline(const std::string& user_id, std::vector<RankedTimelineItem>& items) = 0;
    virtual void SetTimeline(
        const std::string& user_id,
        const std::vector<RankedTimelineItem>& items,
        std::chrono::seconds ttl = std::chrono::seconds(3600)
    ) = 0;
    virtual void InvalidateTimeline(const std::string& user_id) = 0;
    virtual void InvalidateAuthorTimelines(const std::string& author_id) = 0;
    
    // User profile cache
    virtual bool GetUserProfile(const std::string& user_id, UserEngagementProfile& profile) = 0;
    virtual void SetUserProfile(const std::string& user_id, const UserEngagementProfile& profile) = 0;
    
    // Timeline metadata
    virtual void SetLastRead(const std::string& user_id, std::chrono::system_clock::time_point timestamp) = 0;
    virtual std::chrono::system_clock::time_point GetLastRead(const std::string& user_id) = 0;
};

// Real-time timeline update notifications
class RealtimeNotifier {
public:
    virtual ~RealtimeNotifier() = default;
    
    // Subscribe user to real-time updates
    virtual void Subscribe(const std::string& user_id, const std::string& connection_id) = 0;
    virtual void Unsubscribe(const std::string& user_id, const std::string& connection_id) = 0;
    
    // Notify subscribers of timeline updates
    virtual void NotifyNewItems(const std::string& user_id, const std::vector<RankedTimelineItem>& items) = 0;
    virtual void NotifyItemUpdate(const std::string& user_id, const std::string& item_id, const ::sonet::timeline::TimelineUpdate& update) = 0;
    virtual void NotifyItemDeleted(const std::string& user_id, const std::string& note_id) = 0;
};

// Content source adapters
class ContentSourceAdapter {
public:
    virtual ~ContentSourceAdapter() = default;
    virtual std::vector<::sonet::note::Note> GetContent(
        const std::string& user_id,
        const TimelineConfig& config,
        std::chrono::system_clock::time_point since,
        int32_t limit
    ) = 0;
};

// Main Timeline Service Implementation
class TimelineServiceImpl final : public ::sonet::timeline::TimelineService::Service {
public:
    TimelineServiceImpl(
        std::shared_ptr<TimelineCache> cache,
        std::shared_ptr<RankingEngine> ranking_engine,
        std::shared_ptr<ContentFilter> content_filter,
        std::shared_ptr<RealtimeNotifier> realtime_notifier,
        std::unordered_map<::sonet::timeline::ContentSource, std::shared_ptr<ContentSourceAdapter>> content_sources,
        std::shared_ptr<::sonet::follow::FollowService::Stub> follow_service
    );
    ~TimelineServiceImpl();
    
    // Allow wiring an external Overdrive client at runtime
    void SetOverdriveClient(std::shared_ptr<OverdriveClient> client) { overdrive_client_ = std::move(client); }
    
    // gRPC service methods
    grpc::Status GetTimeline(
        grpc::ServerContext* context,
        const ::sonet::timeline::GetTimelineRequest* request,
        ::sonet::timeline::GetTimelineResponse* response
    ) override;
    
    grpc::Status GetUserTimeline(
        grpc::ServerContext* context,
        const ::sonet::timeline::GetUserTimelineRequest* request,
        ::sonet::timeline::GetUserTimelineResponse* response
    ) override;
    
    grpc::Status RefreshTimeline(
        grpc::ServerContext* context,
        const ::sonet::timeline::RefreshTimelineRequest* request,
        ::sonet::timeline::RefreshTimelineResponse* response
    ) override;
    
    grpc::Status MarkTimelineRead(
        grpc::ServerContext* context,
        const ::sonet::timeline::MarkTimelineReadRequest* request,
        ::sonet::timeline::MarkTimelineReadResponse* response
    ) override;
    
    grpc::Status UpdateTimelinePreferences(
        grpc::ServerContext* context,
        const ::sonet::timeline::UpdateTimelinePreferencesRequest* request,
        ::sonet::timeline::UpdateTimelinePreferencesResponse* response
    ) override;
    
    grpc::Status GetTimelinePreferences(
        grpc::ServerContext* context,
        const ::sonet::timeline::GetTimelinePreferencesRequest* request,
        ::sonet::timeline::GetTimelinePreferencesResponse* response
    ) override;
    
    grpc::Status SubscribeTimelineUpdates(
        grpc::ServerContext* context,
        const ::sonet::timeline::SubscribeTimelineUpdatesRequest* request,
        grpc::ServerWriter<::sonet::timeline::TimelineUpdate>* writer
    ) override;
    
    grpc::Status HealthCheck(
        grpc::ServerContext* context,
        const ::sonet::timeline::HealthCheckRequest* request,
        ::sonet::timeline::HealthCheckResponse* response
    ) override;

    grpc::Status RecordEngagement(
        grpc::ServerContext* context,
        const ::sonet::timeline::RecordEngagementRequest* request,
        ::sonet::timeline::RecordEngagementResponse* response
    ) override;

    grpc::Status GetForYouTimeline(
        grpc::ServerContext* context,
        const ::sonet::timeline::GetForYouTimelineRequest* request,
        ::sonet::timeline::GetForYouTimelineResponse* response
    ) override;

    grpc::Status GetFollowingTimeline(
        grpc::ServerContext* context,
        const ::sonet::timeline::GetFollowingTimelineRequest* request,
        ::sonet::timeline::GetFollowingTimelineResponse* response
    ) override;
    
    // Public methods for external services
    void OnNewNote(const ::sonet::note::Note& note);
    void OnNoteDeleted(const std::string& note_id, const std::string& author_id);
    void OnNoteUpdated(const ::sonet::note::Note& note);
    void OnFollowEvent(const std::string& follower_id, const std::string& following_id, bool is_follow);

    // Public access for testing
    std::shared_ptr<RankingEngine> ranking_engine_;

private:
    // Core timeline generation
    std::vector<RankedTimelineItem> GenerateTimeline(
        const std::string& user_id,
        const TimelineConfig& config,
        std::chrono::system_clock::time_point since,
        int32_t limit
    );
    
    // Content source integration
    std::vector<::sonet::note::Note> FetchFollowingContent(
        const std::string& user_id,
        const TimelineConfig& config,
        std::chrono::system_clock::time_point since,
        int32_t limit
    );
    
    std::vector<::sonet::note::Note> FetchRecommendedContent(
        const std::string& user_id,
        const UserEngagementProfile& profile,
        const TimelineConfig& config,
        int32_t limit
    );
    
    std::vector<::sonet::note::Note> FetchTrendingContent(
        const std::string& user_id,
        const TimelineConfig& config,
        int32_t limit
    );
    
    // User profile management
    UserEngagementProfile GetOrCreateUserProfile(const std::string& user_id);
    void UpdateUserEngagement(
        const std::string& user_id,
        const std::string& action,
        const std::string& target_id = ""
    );
    
    // Helper methods
    TimelineConfig GetUserTimelineConfig(const std::string& user_id);
    ::sonet::timeline::TimelineMetadata BuildTimelineMetadata(
        const std::vector<RankedTimelineItem>& items,
        const std::string& user_id,
        const TimelineConfig& config
    );
    
    std::string GetMetadataValue(grpc::ServerContext* context, const std::string& key);
    bool IsAuthorized(grpc::ServerContext* context, const std::string& user_id);
    
    void ApplyABOverridesFromMetadata(grpc::ServerContext* context, TimelineConfig& config);
    
    // Streaming updates support
    struct StreamSession {
        std::mutex mutex;
        std::condition_variable cv;
        std::deque<::sonet::timeline::TimelineUpdate> pending_updates;
        std::atomic<bool> open{true};
    };
    void PushUpdateToSubscribers(const std::string& user_id, const ::sonet::timeline::TimelineUpdate& update);
    std::unordered_map<std::string, std::vector<std::weak_ptr<StreamSession>>> stream_sessions_;
    std::mutex stream_mutex_;
    
    // Simple per-user token bucket rate limiter
    struct Bucket { double tokens = 0.0; std::chrono::steady_clock::time_point last_refill = std::chrono::steady_clock::now(); };
    std::unordered_map<std::string, Bucket> rate_buckets_;
    std::mutex rate_mutex_;
    int rate_rpm_ = 600; // default
    bool RateAllow(const std::string& key, int override_rpm = -1);
    
    // Fanout worker for new notes
    std::queue<::sonet::note::Note> fanout_queue_;
    std::mutex fanout_mutex_;
    std::condition_variable fanout_cv_;
    std::atomic<bool> fanout_running_{false};
    std::thread fanout_thread_;
    void FanoutLoop();
    
    // Components
    std::shared_ptr<TimelineCache> cache_;
    std::shared_ptr<ContentFilter> content_filter_;
    std::shared_ptr<RealtimeNotifier> realtime_notifier_;
    std::unordered_map<::sonet::timeline::ContentSource, std::shared_ptr<ContentSourceAdapter>> content_sources_;
    std::shared_ptr<::sonet::follow::FollowService::Stub> follow_service_;
    
    // Optional external ranker client (Overdrive)
    std::shared_ptr<OverdriveClient> overdrive_client_;
    
    // Configuration
    TimelineConfig default_config_;
    
    // Metrics and monitoring
    std::unordered_map<std::string, std::atomic<uint64_t>> metrics_;
    mutable std::mutex metrics_mutex_;
    
    // Internal user preferences storage (in-memory for now)
    std::unordered_map<std::string, ::sonet::timeline::TimelinePreferences> user_preferences_;
    mutable std::mutex preferences_mutex_;
    
    // Thread safety
    mutable std::shared_mutex service_mutex_;
};

} // namespace sonet::timeline
