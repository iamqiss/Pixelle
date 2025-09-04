#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <functional>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "proto/services/video_feed.grpc.pb.h"
#include "proto/common/video_types.pb.h"
#include "proto/common/user_types.pb.h"
#include "proto/common/ranking_types.pb.h"

namespace sonet {
namespace video_feed {

// Forward declarations
class VideoFeedRepository;
class VideoMLService;
class ContentFilteringService;
class UserEngagementService;
class RealTimeUpdateService;
class Cache;
class Database;
class Logger;

/**
 * @brief Video Feed Service - High-performance ML-powered video ranking and discovery
 * 
 * This service handles:
 * - ML-powered video ranking algorithms (HYBRID, RECENCY, ENGAGEMENT, PERSONALIZED)
 * - Content filtering and moderation
 * - User engagement tracking and personalization
 * - Real-time feed updates via gRPC streaming
 * - High-performance caching and database operations
 */
class VideoFeedService final : public proto::services::VideoFeedService::Service {
public:
    explicit VideoFeedService(
        std::shared_ptr<VideoFeedRepository> repository,
        std::shared_ptr<VideoMLService> mlService,
        std::shared_ptr<ContentFilteringService> contentFilter,
        std::shared_ptr<UserEngagementService> engagementService,
        std::shared_ptr<RealTimeUpdateService> realtimeService,
        std::shared_ptr<Cache> cache,
        std::shared_ptr<Database> database,
        std::shared_ptr<Logger> logger
    );

    ~VideoFeedService() override = default;

    // gRPC Service Methods
    grpc::Status GetVideoFeed(
        grpc::ServerContext* context,
        const proto::services::VideoFeedRequest* request,
        proto::services::VideoFeedResponse* response
    ) override;

    grpc::Status GetPersonalizedFeed(
        grpc::ServerContext* context,
        const proto::services::PersonalizedFeedRequest* request,
        proto::services::VideoFeedResponse* response
    ) override;

    grpc::Status TrackEngagement(
        grpc::ServerContext* context,
        const proto::services::EngagementEvent* request,
        proto::services::EngagementResponse* response
    ) override;

    grpc::Status GetFeedInsights(
        grpc::ServerContext* context,
        const proto::services::FeedInsightsRequest* request,
        proto::services::FeedInsightsResponse* response
    ) override;

    grpc::Status StreamVideoFeed(
        grpc::ServerContext* context,
        const proto::services::VideoFeedRequest* request,
        grpc::ServerWriter<proto::services::VideoFeedUpdate>* writer
    ) override;

    // Service Management
    void Start();
    void Stop();
    bool IsRunning() const { return running_; }

    // Health Check
    proto::common::HealthStatus GetHealthStatus() const;

private:
    // Core Services
    std::shared_ptr<VideoFeedRepository> repository_;
    std::shared_ptr<VideoMLService> mlService_;
    std::shared_ptr<ContentFilteringService> contentFilter_;
    std::shared_ptr<UserEngagementService> engagementService_;
    std::shared_ptr<RealTimeUpdateService> realtimeService_;
    std::shared_ptr<Cache> cache_;
    std::shared_ptr<Database> database_;
    std::shared_ptr<Logger> logger_;

    // Service State
    std::atomic<bool> running_{false};
    std::chrono::steady_clock::time_point startTime_;

    // Internal Methods
    grpc::Status ProcessVideoFeedRequest(
        const proto::services::VideoFeedRequest* request,
        proto::services::VideoFeedResponse* response
    );

    grpc::Status ProcessPersonalizedFeedRequest(
        const proto::services::PersonalizedFeedRequest* request,
        proto::services::VideoFeedResponse* response
    );

    std::vector<proto::common::VideoItem> RankVideoContent(
        const std::vector<proto::common::VideoCandidate>& candidates,
        const proto::services::VideoFeedRequest* request,
        const std::string& userId = ""
    );

    std::vector<proto::common::VideoItem> ApplyMLRanking(
        const std::vector<proto::common::VideoCandidate>& candidates,
        const std::string& userId,
        const proto::common::PersonalizationSettings& personalization
    );

    std::vector<proto::common::VideoItem> ApplyTrendingRanking(
        const std::vector<proto::common::VideoCandidate>& candidates
    );

    std::vector<proto::common::VideoItem> ApplyPersonalizedRanking(
        const std::vector<proto::common::VideoCandidate>& candidates,
        const std::string& userId,
        const proto::common::PersonalizationSettings& personalization
    );

    std::vector<proto::common::VideoItem> ApplyDefaultRanking(
        const std::vector<proto::common::VideoCandidate>& candidates
    );

    std::vector<proto::common::VideoItem> ApplyContentFiltering(
        const std::vector<proto::common::VideoItem>& items,
        const proto::services::VideoFeedRequest* request
    );

    std::vector<proto::common::VideoItem> OptimizeFeedDiversity(
        const std::vector<proto::common::VideoItem>& items,
        const proto::services::VideoFeedRequest* request
    );

    void ApplyDiversityBoosting(std::vector<proto::common::VideoItem>& items);
    void ApplyNoveltyBoosting(std::vector<proto::common::VideoItem>& items);

    double CalculateMLScore(
        const proto::common::MLPredictions& predictions,
        const proto::common::PersonalizationSettings& personalization
    );

    double CalculateTrendingScore(const proto::common::VideoCandidate& candidate);
    
    double CalculatePersonalizationScore(
        const proto::common::VideoCandidate& candidate,
        const std::vector<std::string>& userInterests,
        const std::unordered_map<std::string, double>& contentPreferences
    );

    double CalculateDefaultScore(const proto::common::VideoCandidate& candidate);

    proto::common::VideoItem TransformToVideoItem(
        const proto::common::VideoCandidate& candidate,
        double score,
        const proto::common::MLPredictions& mlPredictions = {}
    );

    proto::common::PaginationInfo GeneratePagination(
        const std::vector<proto::common::VideoItem>& items,
        const proto::common::PaginationRequest& request
    );

    proto::common::VideoStats GenerateVideoStats(
        const std::vector<proto::common::VideoCandidate>& candidates
    );

    std::unordered_map<std::string, double> GetRankingFactors(
        const std::vector<proto::common::VideoItem>& items
    );

    proto::common::PersonalizationSummary GetPersonalizationSummary(
        const std::vector<proto::common::VideoItem>& items,
        const proto::services::VideoFeedRequest* request
    );

    void SetupRealTimeUpdates(
        const std::string& userId,
        const std::string& algorithm
    );

    // Performance Monitoring
    void RecordMetrics(
        const std::string& operation,
        std::chrono::microseconds duration,
        const std::string& userId = ""
    );

    // Cache Management
    std::string GenerateCacheKey(
        const proto::services::VideoFeedRequest* request
    );

    bool TryGetFromCache(
        const std::string& cacheKey,
        proto::services::VideoFeedResponse* response
    );

    void CacheResponse(
        const std::string& cacheKey,
        const proto::services::VideoFeedResponse& response
    );

    // Error Handling
    grpc::Status HandleError(
        const std::string& operation,
        const std::exception& e
    );

    grpc::Status CreateErrorResponse(
        grpc::StatusCode code,
        const std::string& message
    );
};

} // namespace video_feed
} // namespace sonet