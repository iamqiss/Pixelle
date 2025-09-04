#include "timeline_generator.h"
#include <algorithm>
#include <unordered_set>
#include <iostream>

namespace sonet {
namespace timeline {

namespace {
    std::chrono::system_clock::time_point FromProtoTimestamp(const ::sonet::common::Timestamp& ts) {
        auto duration = std::chrono::seconds(ts.seconds()) + std::chrono::nanoseconds(ts.nanos());
        return std::chrono::system_clock::time_point(duration);
    }
}

TimelineGenerator::TimelineGenerator(
    std::shared_ptr<RankingEngine> ranking_engine,
    std::shared_ptr<ContentFilter> content_filter,
    std::unordered_map<::sonet::timeline::ContentSource, std::shared_ptr<ContentSourceAdapter>> content_sources
) : ranking_engine_(std::move(ranking_engine)),
    content_filter_(std::move(content_filter)),
    content_sources_(std::move(content_sources)) {}

std::vector<::sonet::note::Note> TimelineGenerator::FetchFollowingContent(
    const std::string& user_id,
    const TimelineConfig& config,
    std::chrono::system_clock::time_point since,
    int32_t limit
) {
    (void)config;
    auto it = content_sources_.find(::sonet::timeline::CONTENT_SOURCE_FOLLOWING);
    if (it != content_sources_.end()) {
        return it->second->GetContent(user_id, config, since, limit);
    }
    return {};
}

std::vector<::sonet::note::Note> TimelineGenerator::FetchRecommendedContent(
    const std::string& user_id,
    const UserEngagementProfile& /*profile*/,
    const TimelineConfig& config,
    int32_t limit
) {
    auto it = content_sources_.find(::sonet::timeline::CONTENT_SOURCE_RECOMMENDED);
    if (it != content_sources_.end()) {
        auto since = std::chrono::system_clock::now() - std::chrono::hours(24);
        return it->second->GetContent(user_id, config, since, limit);
    }
    return {};
}

std::vector<::sonet::note::Note> TimelineGenerator::FetchTrendingContent(
    const std::string& user_id,
    const TimelineConfig& config,
    int32_t limit
) {
    auto it = content_sources_.find(::sonet::timeline::CONTENT_SOURCE_TRENDING);
    if (it != content_sources_.end()) {
        auto since = std::chrono::system_clock::now() - std::chrono::hours(6);
        return it->second->GetContent(user_id, config, since, limit);
    }
    return {};
}

std::vector<RankedTimelineItem> TimelineGenerator::Generate(
    const std::string& user_id,
    const TimelineConfig& config,
    std::chrono::system_clock::time_point since,
    int32_t limit
) {
    // Collect content
    std::vector<::sonet::note::Note> all_notes;
    all_notes.reserve(static_cast<size_t>(limit) * 2);

    int32_t following_limit = static_cast<int32_t>(limit * config.following_content_ratio);
    if (following_limit > 0) {
        auto notes = FetchFollowingContent(user_id, config, since, following_limit);
        all_notes.insert(all_notes.end(), notes.begin(), notes.end());
    }

    int32_t recommended_limit = static_cast<int32_t>(limit * config.recommended_content_ratio);
    if (recommended_limit > 0) {
        UserEngagementProfile dummy_profile; dummy_profile.user_id = user_id;
        auto notes = FetchRecommendedContent(user_id, dummy_profile, config, recommended_limit);
        all_notes.insert(all_notes.end(), notes.begin(), notes.end());
    }

    int32_t trending_limit = static_cast<int32_t>(limit * config.trending_content_ratio);
    if (trending_limit > 0) {
        auto notes = FetchTrendingContent(user_id, config, trending_limit);
        all_notes.insert(all_notes.end(), notes.begin(), notes.end());
    }

    // Deduplicate
    std::unordered_set<std::string> seen_ids;
    std::vector<::sonet::note::Note> unique_notes;
    seen_ids.reserve(all_notes.size());
    unique_notes.reserve(all_notes.size());
    for (const auto& note : all_notes) {
        if (seen_ids.insert(note.id()).second) unique_notes.push_back(note);
    }

    // Filter
    if (content_filter_) {
        UserEngagementProfile dummy_profile; dummy_profile.user_id = user_id;
        unique_notes = content_filter_->FilterNotes(unique_notes, user_id, dummy_profile);
    }

    // Score
    if (!ranking_engine_) {
        std::vector<RankedTimelineItem> items;
        for (const auto& note : unique_notes) {
            RankedTimelineItem item; item.note = note; item.source = ::sonet::timeline::CONTENT_SOURCE_FOLLOWING;
            item.final_score = static_cast<double>(FromProtoTimestamp(note.created_at()).time_since_epoch().count());
            item.injected_at = std::chrono::system_clock::now();
            item.injection_reason = "chronological";
            items.push_back(item);
        }
        std::sort(items.begin(), items.end(), [](const RankedTimelineItem& a, const RankedTimelineItem& b){ return a.final_score > b.final_score; });
        if (static_cast<int32_t>(items.size()) > limit) items.resize(static_cast<size_t>(limit));
        return items;
    }

    std::vector<RankedTimelineItem> ranked = ranking_engine_->ScoreNotes(unique_notes, user_id, UserEngagementProfile{.user_id = user_id}, config);
    std::sort(ranked.begin(), ranked.end(), [](const RankedTimelineItem& a, const RankedTimelineItem& b){ return a.final_score > b.final_score; });
    if (static_cast<int32_t>(ranked.size()) > limit) ranked.resize(static_cast<size_t>(limit));
    return ranked;
}

} // namespace timeline
} // namespace sonet