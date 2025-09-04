#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <optional>
#include <grpcpp/grpcpp.h>

#include "proto/common/video_types.pb.h"
#include "proto/common/pagination.proto"

namespace sonet {
namespace video_feed {

// Forward declarations
class Database;
class Cache;
class Logger;

/**
 * @brief Video Feed Repository - High-performance data access layer for video content
 * 
 * This repository provides:
 * - Optimized database queries with connection pooling
 * - Intelligent caching strategies
 * - Batch operations for performance
 * - Real-time data synchronization
 * - Query optimization and indexing
 */
class VideoFeedRepository {
public:
    explicit VideoFeedRepository(
        std::shared_ptr<Database> database,
        std::shared_ptr<Cache> cache,
        std::shared_ptr<Logger> logger
    );

    ~VideoFeedRepository() = default;

    // Core Video Operations
    std::vector<proto::common::VideoCandidate> GetVideos(
        const proto::common::VideoQueryParams& params
    );

    std::optional<proto::common::VideoCandidate> GetVideoById(
        const std::string& videoId
    );

    std::vector<proto::common::VideoCandidate> GetVideosByCreator(
        const std::string& creatorId,
        uint32_t limit = 20
    );

    std::vector<proto::common::VideoCandidate> GetTrendingVideos(
        uint32_t limit = 20,
        const std::string& timeWindow = "24h"
    );

    std::vector<proto::common::VideoCandidate> GetVideosByCategory(
        const std::string& category,
        uint32_t limit = 20
    );

    // Search Operations
    proto::common::VideoSearchResult SearchVideos(
        const std::string& query,
        uint32_t limit = 20
    );

    std::vector<proto::common::VideoCandidate> GetSimilarVideos(
        const std::string& videoId,
        uint32_t limit = 20
    );

    // Batch Operations
    std::vector<proto::common::VideoCandidate> GetVideosBatch(
        const std::vector<std::string>& videoIds
    );

    bool UpdateVideoMetrics(
        const std::string& videoId,
        const proto::common::EngagementMetrics& metrics
    );

    bool UpdateVideoFeatures(
        const std::string& videoId,
        const proto::common::VideoFeatures& features
    );

    // Analytics Operations
    proto::common::VideoAnalytics GetVideoAnalytics(
        const std::string& videoId,
        const std::string& timeWindow = "30d"
    );

    std::vector<proto::common::VideoRecommendation> GetVideoRecommendations(
        const std::string& userId,
        uint32_t limit = 20
    );

    // Cache Management
    void InvalidateVideoCache(const std::string& videoId);
    void InvalidateCreatorCache(const std::string& creatorId);
    void InvalidateCategoryCache(const std::string& category);
    void InvalidateTrendingCache();

    // Performance Monitoring
    struct QueryMetrics {
        std::chrono::microseconds query_time;
        uint32_t result_count;
        bool cache_hit;
        std::string query_type;
    };

    std::vector<QueryMetrics> GetQueryMetrics();
    void ResetQueryMetrics();

    // Health Check
    bool IsHealthy() const;
    std::string GetHealthStatus() const;

private:
    // Core Components
    std::shared_ptr<Database> database_;
    std::shared_ptr<Cache> cache_;
    std::shared_ptr<Logger> logger_;

    // Performance Tracking
    std::vector<QueryMetrics> query_metrics_;
    mutable std::mutex metrics_mutex_;

    // Internal Methods
    std::vector<proto::common::VideoCandidate> QueryVideosFromDatabase(
        const proto::common::VideoQueryParams& params
    );

    std::vector<proto::common::VideoCandidate> QueryTrendingVideosFromDatabase(
        uint32_t limit,
        const std::string& timeWindow
    );

    std::vector<proto::common::VideoCandidate> QueryVideosByCreatorFromDatabase(
        const std::string& creatorId,
        uint32_t limit
    );

    std::vector<proto::common::VideoCandidate> QueryVideosByCategoryFromDatabase(
        const std::string& category,
        uint32_t limit
    );

    proto::common::VideoSearchResult SearchVideosFromDatabase(
        const std::string& query,
        uint32_t limit
    );

    std::vector<proto::common::VideoCandidate> GetSimilarVideosFromDatabase(
        const std::string& videoId,
        uint32_t limit
    );

    // Cache Operations
    std::optional<std::vector<proto::common::VideoCandidate>> GetVideosFromCache(
        const std::string& cacheKey
    );

    void CacheVideos(
        const std::string& cacheKey,
        const std::vector<proto::common::VideoCandidate>& videos,
        uint32_t ttl = 300
    );

    std::optional<proto::common::VideoCandidate> GetVideoFromCache(
        const std::string& videoId
    );

    void CacheVideo(
        const std::string& videoId,
        const proto::common::VideoCandidate& video,
        uint32_t ttl = 600
    );

    // Database Query Builders
    std::string BuildVideoQuery(
        const proto::common::VideoQueryParams& params
    );

    std::string BuildTrendingQuery(
        uint32_t limit,
        const std::string& timeWindow
    );

    std::string BuildSearchQuery(
        const std::string& query,
        uint32_t limit
    );

    std::string BuildSimilarVideosQuery(
        const std::string& videoId,
        uint32_t limit
    );

    // Result Processing
    proto::common::VideoCandidate MapDatabaseRowToVideo(
        const std::unordered_map<std::string, std::string>& row
    );

    std::vector<proto::common::VideoCandidate> ProcessVideoResults(
        const std::vector<std::unordered_map<std::string, std::string>>& rows
    );

    // Utility Methods
    std::string GenerateCacheKey(
        const proto::common::VideoQueryParams& params
    );

    std::string GenerateVideoCacheKey(const std::string& videoId);
    std::string GenerateCreatorCacheKey(const std::string& creatorId);
    std::string GenerateCategoryCacheKey(const std::string& category);
    std::string GenerateTrendingCacheKey(const std::string& timeWindow);

    uint64_t GetTimeWindowMs(const std::string& timeWindow);
    std::string GetCurrentTimestamp();
    std::string FormatTimestamp(const std::chrono::system_clock::time_point& time);

    // Error Handling
    void LogDatabaseError(
        const std::string& operation,
        const std::exception& e
    );

    void LogCacheError(
        const std::string& operation,
        const std::exception& e
    );

    // Performance Optimization
    void OptimizeQuery(const std::string& query);
    void UpdateQueryStatistics(const QueryMetrics& metrics);
    void CleanupOldMetrics();
};

} // namespace video_feed
} // namespace sonet