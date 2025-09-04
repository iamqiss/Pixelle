#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <grpcpp/grpcpp.h>

#include "proto/common/video_types.pb.h"
#include "proto/common/user_types.pb.h"
#include "proto/common/ml_types.pb.h"

namespace sonet {
namespace video_feed {

// Forward declarations
class VideoFeatureExtractor;
class UserFeatureExtractor;
class MLModelClient;
class VideoAnalytics;
class Logger;

/**
 * @brief Video ML Service - High-performance AI-powered predictions for video content
 * 
 * This service provides:
 * - Real-time ML model inference for video ranking
 * - Feature extraction and normalization
 * - Multi-model ensemble predictions
 * - Performance optimization with model caching
 * - A/B testing support for algorithm validation
 */
class VideoMLService {
public:
    explicit VideoMLService(
        std::shared_ptr<VideoFeatureExtractor> featureExtractor,
        std::shared_ptr<UserFeatureExtractor> userFeatureExtractor,
        std::shared_ptr<MLModelClient> modelClient,
        std::shared_ptr<VideoAnalytics> analytics,
        std::shared_ptr<Logger> logger
    );

    ~VideoMLService() = default;

    // Core ML Methods
    proto::common::MLPredictionResponse GetPredictions(
        const proto::common::MLPredictionRequest& request
    );

    std::vector<proto::common::MLPredictionResponse> GetBatchPredictions(
        const std::vector<proto::common::MLPredictionRequest>& requests
    );

    // Feature Extraction
    proto::common::VideoFeatures ExtractVideoFeatures(
        const proto::common::VideoCandidate& video
    );

    proto::common::UserFeatures ExtractUserFeatures(
        const std::string& userId,
        const proto::common::UserContext& context
    );

    proto::common::ContextualFeatures ExtractContextualFeatures(
        const proto::common::RequestContext& context
    );

    // Model Management
    void LoadModel(const std::string& modelId, const std::string& modelPath);
    void UnloadModel(const std::string& modelId);
    bool IsModelLoaded(const std::string& modelId) const;

    // Performance Optimization
    void WarmupModel(const std::string& modelId);
    void SetModelCacheSize(size_t cacheSize);
    void EnableModelCaching(bool enable);

    // A/B Testing
    void SetExperimentVariant(const std::string& experimentId, const std::string& variant);
    std::string GetExperimentVariant(const std::string& experimentId) const;

    // Health and Monitoring
    proto::common::MLServiceHealth GetHealthStatus() const;
    proto::common::MLServiceMetrics GetMetrics() const;

private:
    // Core Components
    std::shared_ptr<VideoFeatureExtractor> featureExtractor_;
    std::shared_ptr<UserFeatureExtractor> userFeatureExtractor_;
    std::shared_ptr<MLModelClient> modelClient_;
    std::shared_ptr<VideoAnalytics> analytics_;
    std::shared_ptr<Logger> logger_;

    // Model Management
    std::unordered_map<std::string, std::shared_ptr<void>> loadedModels_;
    std::unordered_map<std::string, std::string> modelPaths_;
    std::unordered_map<std::string, bool> modelCacheEnabled_;

    // A/B Testing
    std::unordered_map<std::string, std::string> experimentVariants_;

    // Performance
    size_t modelCacheSize_{1000};
    bool modelCachingEnabled_{true};

    // Internal Methods
    proto::common::MLPredictionResponse ProcessPredictionRequest(
        const proto::common::MLPredictionRequest& request
    );

    std::vector<double> ExtractAndNormalizeFeatures(
        const proto::common::VideoFeatures& videoFeatures,
        const proto::common::UserFeatures& userFeatures,
        const proto::common::ContextualFeatures& contextualFeatures
    );

    proto::common::MLPredictionResponse GetModelPredictions(
        const std::vector<double>& features,
        const std::string& modelId
    );

    std::unordered_map<std::string, double> CalculateRankingFactors(
        const proto::common::MLPredictions& predictions,
        const proto::common::VideoFeatures& features
    );

    double CalculateTechnicalQuality(const proto::common::VideoFeatures& features);
    double CalculateVisualAppeal(const proto::common::VideoFeatures& features);
    double CalculateAudioQuality(const proto::common::VideoFeatures& features);
    double CalculateViralPotential(const proto::common::VideoFeatures& features);
    double CalculateInterestMatch(
        const proto::common::VideoFeatures& videoFeatures,
        const proto::common::UserFeatures& userFeatures
    );
    double CalculateCreatorPreference(
        const proto::common::VideoFeatures& videoFeatures,
        const proto::common::UserFeatures& userFeatures
    );
    double CalculateCategoryDiversity(
        const std::vector<proto::common::VideoFeatures>& features
    );
    double CalculateCreatorDiversity(
        const std::vector<proto::common::VideoFeatures>& features
    );
    double CalculateTrendingScore(const proto::common::VideoFeatures& features);
    double CalculateTimeliness(const proto::common::ContextualFeatures& features);
    double CalculateSeasonalRelevance(const proto::common::ContextualFeatures& features);
    double CalculateConfidence(
        const proto::common::MLPredictions& predictions,
        const proto::common::VideoFeatures& features
    );
    double CalculatePredictionVariance(const proto::common::MLPredictions& predictions);

    // Utility Methods
    double GetResolutionScore(const std::string& resolution);
    std::vector<double> NormalizeContextualFeatures(
        const proto::common::ContextualFeatures& features
    );
    proto::common::MLPredictionResponse GetFallbackPredictions(
        const proto::common::VideoFeatures& features
    );

    // Performance Monitoring
    void RecordPredictionLatency(
        const std::string& modelId,
        std::chrono::microseconds latency
    );

    void RecordPredictionAccuracy(
        const std::string& modelId,
        double accuracy
    );

    // Error Handling
    proto::common::MLPredictionResponse HandlePredictionError(
        const std::string& operation,
        const std::exception& e
    );
};

} // namespace video_feed
} // namespace sonet