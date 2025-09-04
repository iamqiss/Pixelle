#pragma once

#include "../service.h"
#include <memory>
#include <vector>
#include <optional>

namespace sonet {
namespace timeline {
namespace controllers {

struct HomeTimelineResult {
    std::vector<::sonet::timeline::TimelineItem> items;
    ::sonet::timeline::TimelineMetadata metadata;
    ::sonet::common::Pagination pagination;
    bool success = false;
    std::string error_message;
};

struct UserTimelineResult {
    std::vector<::sonet::timeline::TimelineItem> items;
    ::sonet::common::Pagination pagination;
    bool success = false;
    std::string error_message;
};

class TimelineController {
public:
    explicit TimelineController(std::shared_ptr<sonet::timeline::TimelineServiceImpl> service);

    HomeTimelineResult get_home_timeline(
        const std::string& user_id,
        int32_t offset,
        int32_t limit,
        bool include_ranking_signals = false
    );

    HomeTimelineResult get_for_you_timeline(
        const std::string& user_id,
        int32_t offset,
        int32_t limit,
        bool include_ranking_signals = false
    );

    HomeTimelineResult get_following_timeline(
        const std::string& user_id,
        int32_t offset,
        int32_t limit,
        bool include_ranking_signals = false
    );

    UserTimelineResult get_user_timeline(
        const std::string& target_user_id,
        const std::string& requesting_user_id,
        int32_t offset,
        int32_t limit,
        bool include_replies = false,
        bool include_renotes = true
    );

    bool refresh_timeline(const std::string& user_id, int32_t max_items);

    bool update_preferences(const std::string& user_id, const ::sonet::timeline::TimelinePreferences& prefs);

    std::optional<::sonet::timeline::TimelinePreferences> get_preferences(const std::string& user_id);

    bool record_engagement(const std::string& user_id, const std::string& note_id, const std::string& action, double duration_seconds = 0.0);

private:
    std::shared_ptr<sonet::timeline::TimelineServiceImpl> service_;
};

} // namespace controllers
} // namespace timeline
} // namespace sonet