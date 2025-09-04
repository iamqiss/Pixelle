#include "chronological_ranking_engine.h"
#include <algorithm>

namespace sonet {
namespace timeline {

namespace {
    std::chrono::system_clock::time_point FromProtoTimestamp(const ::sonet::common::Timestamp& ts) {
        auto duration = std::chrono::seconds(ts.seconds()) + std::chrono::nanoseconds(ts.nanos());
        return std::chrono::system_clock::time_point(duration);
    }
}

std::vector<RankedTimelineItem> ChronologicalRankingEngine::ScoreNotes(
    const std::vector<::sonet::note::Note>& notes,
    const std::string& user_id,
    const UserEngagementProfile& profile,
    const TimelineConfig& config
) {
    (void)user_id; (void)profile; (void)config;
    std::vector<RankedTimelineItem> items;
    items.reserve(notes.size());
    for (const auto& note : notes) {
        RankedTimelineItem item;
        item.note = note;
        item.source = ::sonet::timeline::CONTENT_SOURCE_FOLLOWING;
        auto tp = FromProtoTimestamp(note.created_at());
        item.final_score = static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count());
        item.injected_at = std::chrono::system_clock::now();
        item.injection_reason = "chronological";
        items.push_back(item);
    }
    std::sort(items.begin(), items.end(), [](const RankedTimelineItem& a, const RankedTimelineItem& b){ return a.final_score > b.final_score; });
    return items;
}

} // namespace timeline
} // namespace sonet