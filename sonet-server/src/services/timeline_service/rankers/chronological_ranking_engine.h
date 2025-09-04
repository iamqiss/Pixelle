#pragma once

#include "../service.h"

namespace sonet {
namespace timeline {

class ChronologicalRankingEngine : public RankingEngine {
public:
    ~ChronologicalRankingEngine() override = default;

    std::vector<RankedTimelineItem> ScoreNotes(
        const std::vector<::sonet::note::Note>& notes,
        const std::string& user_id,
        const UserEngagementProfile& profile,
        const TimelineConfig& config
    ) override;

    void UpdateUserEngagement(const std::string& user_id, const std::string& note_id, const std::string& action, double duration_seconds) override {}
    void TrainOnEngagementData(const std::vector<EngagementEvent>& events) override {}
};

} // namespace timeline
} // namespace sonet