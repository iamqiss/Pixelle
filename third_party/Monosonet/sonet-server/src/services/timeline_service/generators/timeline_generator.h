#pragma once

#include "../service.h"
#include <memory>
#include <vector>
#include <unordered_map>

namespace sonet {
namespace timeline {

class TimelineGenerator {
public:
    TimelineGenerator(
        std::shared_ptr<RankingEngine> ranking_engine,
        std::shared_ptr<ContentFilter> content_filter,
        std::unordered_map<::sonet::timeline::ContentSource, std::shared_ptr<ContentSourceAdapter>> content_sources
    );

    std::vector<RankedTimelineItem> Generate(
        const std::string& user_id,
        const TimelineConfig& config,
        std::chrono::system_clock::time_point since,
        int32_t limit
    );

private:
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

    std::shared_ptr<RankingEngine> ranking_engine_;
    std::shared_ptr<ContentFilter> content_filter_;
    std::unordered_map<::sonet::timeline::ContentSource, std::shared_ptr<ContentSourceAdapter>> content_sources_;
};

} // namespace timeline
} // namespace sonet