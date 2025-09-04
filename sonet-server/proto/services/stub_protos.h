#pragma once

// Stub proto includes for compilation testing
// In production, these would be generated from proto files

#include <string>
#include <vector>
#include <memory>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <chrono>

namespace sonet {
namespace common {
    struct Timestamp {
        int64_t seconds_ = 0;
        int32_t nanos_ = 0;
        
        int64_t seconds() const { return seconds_; }
        int32_t nanos() const { return nanos_; }
        void set_seconds(int64_t s) { seconds_ = s; }
        void set_nanos(int32_t n) { nanos_ = n; }
    };
    
    struct Pagination {
        int32_t offset = 0;
        int32_t limit = 0;
        int32_t total_count = 0;
        bool has_next = false;
        bool has_previous = false;
        
        void set_offset(int32_t o) { offset = o; }
        void set_limit(int32_t l) { limit = l; }
        void set_total_count(int32_t t) { total_count = t; }
        void set_has_next(bool h) { has_next = h; }
        void set_has_previous(bool h) { has_previous = h; }
    };
}

namespace note {
    enum Visibility {
        VISIBILITY_PUBLIC = 0,
        VISIBILITY_FOLLOWERS = 1,
        VISIBILITY_FRIENDS = 2,
        VISIBILITY_PRIVATE = 3,
        VISIBILITY_MENTIONED = 4
    };
    
    struct NoteMetrics {
        int32_t views_ = 0;
        int32_t likes_ = 0;
        int32_t renotes_ = 0; // formerly renotes_
        int32_t replies_ = 0;
        int32_t quotes_ = 0;
        int32_t comments_ = 0; // Add comments field

        int32_t views() const { return views_; }
        int32_t likes() const { return likes_; }
        int32_t renotes() const { return renotes_; }
        int32_t replies() const { return replies_; }
        int32_t quotes() const { return quotes_; }
        int32_t comments() const { return comments_; } // Add comments getter

        void set_views(int32_t v) { views_ = v; }
        void set_likes(int32_t l) { likes_ = l; }
        void set_renotes(int32_t r) { renotes_ = r; }
        void set_replies(int32_t r) { replies_ = r; }
        void set_quotes(int32_t q) { quotes_ = q; }
        void set_comments(int32_t c) { comments_ = c; } // Add comments setter

        // Deprecated aliases for transition
        [[deprecated("Use renotes() instead")]] int32_t renotes() const { return renotes_; }
        [[deprecated("Use set_renotes() instead")]] void set_renotes(int32_t r) { renotes_ = r; }
    };
    
    struct MediaItem {
        std::string url;
        std::string type;
        int32_t items_size() const { return 1; }
    };
    
    struct Note {
        std::string id_;
        std::string author_id_;
        std::string content_;
        Visibility visibility_ = VISIBILITY_PUBLIC;
        std::string content_warning_;
        sonet::common::Timestamp created_at_;
        sonet::common::Timestamp updated_at_;
        NoteMetrics metrics_;
        MediaItem media_;
        
        std::string id() const { return id_; }
        std::string author_id() const { return author_id_; }
        std::string content() const { return content_; }
        Visibility visibility() const { return visibility_; }
        std::string content_warning() const { return content_warning_; }
        sonet::common::Timestamp created_at() const { return created_at_; }
        sonet::common::Timestamp updated_at() const { return updated_at_; }
        
        void set_id(const std::string& i) { id_ = i; }
        void set_author_id(const std::string& a) { author_id_ = a; }
        void set_content(const std::string& c) { content_ = c; }
        void set_visibility(Visibility v) { visibility_ = v; }
        
        sonet::common::Timestamp* mutable_created_at() { return &created_at_; }
        sonet::common::Timestamp* mutable_updated_at() { return &updated_at_; }
        NoteMetrics* mutable_metrics() { return &metrics_; }
        
        bool has_metrics() const { return true; }
        bool has_media() const { return true; }
        bool has_content_warning() const { return !content_warning_.empty(); }
        
        const NoteMetrics& metrics() const { return metrics_; }
        const MediaItem& media() const { return media_; }
    };
    
    // Stub service
    struct NoteService {
        struct Stub {
            // List recent notes by authors since a timestamp
            struct ListRecentNotesByAuthorsRequest {
                std::vector<std::string> author_ids;
                sonet::common::Timestamp since;
                int32_t limit = 50;
            };
            struct ListRecentNotesByAuthorsResponse {
                std::vector<Note> notes;
            };
            ListRecentNotesByAuthorsResponse ListRecentNotesByAuthors(const ListRecentNotesByAuthorsRequest& req) {
                // Generate synthetic notes deterministically for testing
                ListRecentNotesByAuthorsResponse resp;
                int idx = 1;
                for (const auto& aid : req.author_ids) {
                    for (int i = 0; i < 3 && static_cast<int>(resp.notes.size()) < req.limit; ++i) {
                        Note n;
                        n.set_id("auth_" + aid + "_" + std::to_string(idx++));
                        n.set_author_id(aid);
                        n.set_content("Recent note #" + std::to_string(i+1) + " by " + aid);
                        n.set_visibility(VISIBILITY_PUBLIC);
                        auto now = std::chrono::system_clock::now();
                        auto secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
                        n.mutable_created_at()->set_seconds(secs);
                        n.mutable_updated_at()->set_seconds(secs);
                        resp.notes.push_back(n);
                    }
                }
                return resp;
            }
        };
    };
}

namespace timeline {
    // User engagement profile for personalization
    struct UserEngagementProfile {
        std::string user_id;

        // Relationships and mutes
        std::unordered_set<std::string> following_ids;
        std::unordered_set<std::string> muted_users;
        std::unordered_set<std::string> muted_keywords;

        // Interests and affinities
        std::unordered_map<std::string, double> author_affinity;      // author_id -> affinity score
        std::unordered_map<std::string, double> hashtag_interests;    // hashtag -> interest score
        std::unordered_map<std::string, double> topic_interests;      // topic -> interest score

        // Engagement statistics
        std::chrono::system_clock::time_point last_updated{};
        double avg_session_length_minutes = 0.0;
        double daily_engagement_score = 0.0;
        double engagement_score = 0.0; // Add engagement_score field
        int32_t notes_per_day = 0;
        int32_t interactions_per_day = 0;
    };
}

namespace timeline {
    enum ContentSource {
        CONTENT_SOURCE_FOLLOWING = 0,
        CONTENT_SOURCE_RECOMMENDED = 1,
        CONTENT_SOURCE_TRENDING = 2,
        CONTENT_SOURCE_LISTS = 3
    };
    
    enum TimelineAlgorithm {
        TIMELINE_ALGORITHM_UNKNOWN = 0,
        TIMELINE_ALGORITHM_CHRONOLOGICAL = 1,
        TIMELINE_ALGORITHM_ALGORITHMIC = 2,
        TIMELINE_ALGORITHM_HYBRID = 3
    };
    
    enum TimelineUpdateType {
        UPDATE_TYPE_NEW_ITEM = 0,
        UPDATE_TYPE_ITEM_CHANGED = 1,
        UPDATE_TYPE_ITEM_DELETED = 2
    };
    
    struct RankingSignals {
        double author_affinity_score = 0.0;
        double content_quality_score = 0.0;
        double engagement_velocity_score = 0.0;
        double recency_score = 0.0;
        double personalization_score = 0.0;
        
        void set_author_affinity_score(double s) { author_affinity_score = s; }
        void set_content_quality_score(double s) { content_quality_score = s; }
        void set_engagement_velocity_score(double s) { engagement_velocity_score = s; }
        void set_recency_score(double s) { recency_score = s; }
        void set_personalization_score(double s) { personalization_score = s; }
    };
    
    struct TimelineUpdate {
        TimelineUpdateType update_type_ = UPDATE_TYPE_NEW_ITEM;
        TimelineUpdateType update_type() const { return update_type_; }
    };
    
    struct TimelineMetadata {
        int32_t total_items = 0;
        TimelineAlgorithm algorithm_used = TIMELINE_ALGORITHM_HYBRID;
        std::string timeline_version;
        sonet::common::Timestamp last_updated;
        sonet::common::Timestamp last_user_read;
        int32_t new_items_since_last_fetch = 0;
        std::map<std::string, double> algorithm_params_;
        
        void set_total_items(int32_t t) { total_items = t; }
        void set_algorithm_used(TimelineAlgorithm a) { algorithm_used = a; }
        void set_timeline_version(const std::string& v) { timeline_version = v; }
        void set_new_items_since_last_fetch(int32_t n) { new_items_since_last_fetch = n; }
        
        sonet::common::Timestamp* mutable_last_updated() { return &last_updated; }
        sonet::common::Timestamp* mutable_last_user_read() { return &last_user_read; }
        std::map<std::string, double>* mutable_algorithm_params() { return &algorithm_params_; }
    };

    struct TimelinePreferences {
        TimelineAlgorithm preferred_algorithm_ = TIMELINE_ALGORITHM_UNKNOWN;
        bool show_replies_ = true;
        bool show_renotes_ = true; // formerly show_renotes_
        bool show_recommended_content_ = true;
        bool show_trending_content_ = true;
        bool sensitive_content_warning_ = true;
        int32_t timeline_refresh_minutes_ = 5;

        TimelineAlgorithm algorithm() const { return preferred_algorithm_; }
        bool show_recommended_content() const { return show_recommended_content_; }
        bool show_trending_content() const { return show_trending_content_; }
        // Convenience for compatibility with service config builder
        int32_t max_items() const { return 0; }
        int32_t max_age_hours() const { return 0; }
        double min_score_threshold() const { return 0.0; }
        double recency_weight() const { return 0.0; }
        double engagement_weight() const { return 0.0; }
        double author_affinity_weight() const { return 0.0; }
        double content_quality_weight() const { return 0.0; }
        double diversity_weight() const { return 0.0; }
        double following_content_ratio() const { return 0.0; }
        double recommended_content_ratio() const { return 0.0; }
        double trending_content_ratio() const { return 0.0; }
    };
    
    // Stub gRPC service requests/responses
    struct GetTimelineRequest {
        std::string user_id_;
        TimelineAlgorithm algorithm_ = TIMELINE_ALGORITHM_UNKNOWN;
        sonet::common::Pagination pagination_;
        bool include_ranking_signals_ = false;
        
        std::string user_id() const { return user_id_; }
        TimelineAlgorithm algorithm() const { return algorithm_; }
        const sonet::common::Pagination& pagination() const { return pagination_; }
        bool include_ranking_signals() const { return include_ranking_signals_; }
    };
    
    struct TimelineItem {
        sonet::note::Note note_;
        ContentSource source_ = CONTENT_SOURCE_FOLLOWING;
        double final_score_ = 0.0;
        sonet::common::Timestamp injected_at_;
        std::string injection_reason_;
        RankingSignals ranking_signals_;
        
        sonet::note::Note* mutable_note() { return &note_; }
        void set_source(ContentSource s) { source_ = s; }
        void set_final_score(double s) { final_score_ = s; }
        void set_injection_reason(const std::string& r) { injection_reason_ = r; }
        sonet::common::Timestamp* mutable_injected_at() { return &injected_at_; }
        RankingSignals* mutable_ranking_signals() { return &ranking_signals_; }
    };
    
    struct GetTimelineResponse {
        std::vector<TimelineItem> items_;
        TimelineMetadata metadata_;
        sonet::common::Pagination pagination_;
        bool success_ = false;
        std::string error_message_;
        
        TimelineItem* add_items() { items_.emplace_back(); return &items_.back(); }
        TimelineMetadata* mutable_metadata() { return &metadata_; }
        sonet::common::Pagination* mutable_pagination() { return &pagination_; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& e) { error_message_ = e; }
    };
    
    // Other stub requests/responses
    struct RefreshTimelineRequest {
        std::string user_id_;
        sonet::common::Timestamp since_;
        int32_t max_items_ = 0;
        
        std::string user_id() const { return user_id_; }
        sonet::common::Timestamp since() const { return since_; }
        int32_t max_items() const { return max_items_; }
    };
    
    struct RefreshTimelineResponse {
        std::vector<TimelineItem> new_items_;
        int32_t total_new_items_ = 0;
        bool has_more_ = false;
        bool success_ = false;
        std::string error_message_;
        
        TimelineItem* add_new_items() { new_items_.emplace_back(); return &new_items_.back(); }
        void set_total_new_items(int32_t t) { total_new_items_ = t; }
        void set_has_more(bool h) { has_more_ = h; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& e) { error_message_ = e; }
    };
    
    struct MarkTimelineReadRequest {
        std::string user_id_;
        sonet::common::Timestamp read_until_;
        
        std::string user_id() const { return user_id_; }
        sonet::common::Timestamp read_until() const { return read_until_; }
    };
    
    struct MarkTimelineReadResponse {
        bool success_ = false;
        std::string error_message_;
        
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& e) { error_message_ = e; }
    };
    
    struct HealthCheckRequest {};
    
    struct HealthCheckResponse {
        std::string status_;
        std::map<std::string, std::string> details_;
        
        void set_status(const std::string& s) { status_ = s; }
        std::map<std::string, std::string>* mutable_details() { return &details_; }
    };
    
    // Additional stub requests for completeness
    struct GetUserTimelineRequest {
        std::string target_user_id_;
        std::string requesting_user_id_;
        sonet::common::Pagination pagination_;
        bool include_replies_ = false;
        bool include_renotes_ = true; // formerly include_renotes_
        
        std::string target_user_id() const { return target_user_id_; }
        std::string requesting_user_id() const { return requesting_user_id_; }
        const sonet::common::Pagination& pagination() const { return pagination_; }
        bool include_replies() const { return include_replies_; }
        bool include_renotes() const { return include_renotes_; }
        [[deprecated("Use include_renotes() instead")]] bool include_renotes() const { return include_renotes_; }
    };
    
    struct GetUserTimelineResponse {
        std::vector<TimelineItem> items_;
        sonet::common::Pagination pagination_;
        bool success_ = false;
        std::string error_message_;
        
        TimelineItem* add_items() { items_.emplace_back(); return &items_.back(); }
        sonet::common::Pagination* mutable_pagination() { return &pagination_; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& e) { error_message_ = e; }
    };

    struct UpdateTimelinePreferencesRequest {
        std::string user_id_;
        TimelinePreferences preferences_;
        std::string user_id() const { return user_id_; }
        const TimelinePreferences& preferences() const { return preferences_; }
    };

    struct UpdateTimelinePreferencesResponse { void set_success(bool) {} void set_error_message(const std::string&) {} };

    struct GetTimelinePreferencesRequest { std::string user_id_ ; std::string user_id() const { return user_id_; } };

    struct GetTimelinePreferencesResponse { TimelinePreferences preferences_; void set_success(bool) {} void set_error_message(const std::string&) {} };
    
    struct SubscribeTimelineUpdatesRequest { std::string user_id() const { return ""; } };

    struct RecordEngagementRequest {
        std::string user_id_;
        std::string note_id_;
        std::string action_;
        double duration_seconds_ = 0.0;
        
        std::string user_id() const { return user_id_; }
        std::string note_id() const { return note_id_; }
        std::string action() const { return action_; }
        double duration_seconds() const { return duration_seconds_; }
    };

    struct RecordEngagementResponse {
        bool success_ = false;
        std::string error_message_;
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& e) { error_message_ = e; }
    };
    
    struct GetForYouTimelineRequest {
        std::string user_id_;
        sonet::common::Pagination pagination_;
        bool include_ranking_signals_ = false;
        std::string user_id() const { return user_id_; }
        const sonet::common::Pagination& pagination() const { return pagination_; }
        bool include_ranking_signals() const { return include_ranking_signals_; }
    };

    struct GetForYouTimelineResponse {
        std::vector<TimelineItem> items_;
        TimelineMetadata metadata_;
        sonet::common::Pagination pagination_;
        bool success_ = false;
        std::string error_message_;
        TimelineItem* add_items() { items_.emplace_back(); return &items_.back(); }
        TimelineMetadata* mutable_metadata() { return &metadata_; }
        sonet::common::Pagination* mutable_pagination() { return &pagination_; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& e) { error_message_ = e; }
    };

    struct GetFollowingTimelineRequest {
        std::string user_id_;
        sonet::common::Pagination pagination_;
        bool include_ranking_signals_ = false;
        std::string user_id() const { return user_id_; }
        const sonet::common::Pagination& pagination() const { return pagination_; }
        bool include_ranking_signals() const { return include_ranking_signals_; }
    };

    struct GetFollowingTimelineResponse {
        std::vector<TimelineItem> items_;
        TimelineMetadata metadata_;
        sonet::common::Pagination pagination_;
        bool success_ = false;
        std::string error_message_;
        TimelineItem* add_items() { items_.emplace_back(); return &items_.back(); }
        TimelineMetadata* mutable_metadata() { return &metadata_; }
        sonet::common::Pagination* mutable_pagination() { return &pagination_; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& e) { error_message_ = e; }
    };

    // Timeline service base class
    struct TimelineService {
        struct Service {
            virtual ~Service() = default;
            
            virtual ::grpc::Status GetTimeline(
                ::grpc::ServerContext* context,
                const GetTimelineRequest* request,
                GetTimelineResponse* response) = 0;
                
            virtual ::grpc::Status RefreshTimeline(
                ::grpc::ServerContext* context,
                const RefreshTimelineRequest* request,
                RefreshTimelineResponse* response) = 0;
                
            virtual ::grpc::Status MarkTimelineRead(
                ::grpc::ServerContext* context,
                const MarkTimelineReadRequest* request,
                MarkTimelineReadResponse* response) = 0;
                
            virtual ::grpc::Status HealthCheck(
                ::grpc::ServerContext* context,
                const HealthCheckRequest* request,
                HealthCheckResponse* response) = 0;
                
            virtual ::grpc::Status GetUserTimeline(
                ::grpc::ServerContext* context,
                const GetUserTimelineRequest* request,
                GetUserTimelineResponse* response) = 0;
                
            virtual ::grpc::Status UpdateTimelinePreferences(
                ::grpc::ServerContext* context,
                const UpdateTimelinePreferencesRequest* request,
                UpdateTimelinePreferencesResponse* response) = 0;
                
            virtual ::grpc::Status GetTimelinePreferences(
                ::grpc::ServerContext* context,
                const GetTimelinePreferencesRequest* request,
                GetTimelinePreferencesResponse* response) = 0;
                
            virtual ::grpc::Status SubscribeTimelineUpdates(
                ::grpc::ServerContext* context,
                const SubscribeTimelineUpdatesRequest* request,
                ::grpc::ServerWriter<TimelineUpdate>* writer) = 0;

            virtual ::grpc::Status RecordEngagement(
                ::grpc::ServerContext* context,
                const RecordEngagementRequest* request,
                RecordEngagementResponse* response) = 0;

            virtual ::grpc::Status GetForYouTimeline(
                ::grpc::ServerContext* context,
                const GetForYouTimelineRequest* request,
                GetForYouTimelineResponse* response) = 0;

            virtual ::grpc::Status GetFollowingTimeline(
                ::grpc::ServerContext* context,
                const GetFollowingTimelineRequest* request,
                GetFollowingTimelineResponse* response) = 0;
        };
    };
}

namespace follow {
    struct GetFollowingRequest { std::string user_id_; std::string user_id() const { return user_id_; } };
    struct GetFollowingResponse { std::vector<std::string> user_ids_; const std::vector<std::string>& user_ids() const { return user_ids_; } };
    struct GetFollowersRequest { std::string user_id_; std::string user_id() const { return user_id_; } };
    struct GetFollowersResponse { std::vector<std::string> user_ids_; const std::vector<std::string>& user_ids() const { return user_ids_; } };
    
    struct FollowService {
        struct Stub {
            GetFollowingResponse GetFollowing(const GetFollowingRequest& req) {
                // Deterministic sample following list per user id hash
                GetFollowingResponse resp;
                std::hash<std::string> h;
                size_t base = h(req.user_id()) % 5;
                std::vector<std::string> candidates = {"alice_dev","bob_designer","charlie_pm","diana_data","eve_security","frank_frontend"};
                for (size_t i = 0; i < candidates.size(); ++i) {
                    if ((i + base) % 2 == 0) resp.user_ids_.push_back(candidates[i]);
                }
                return resp;
            }
            GetFollowersResponse GetFollowers(const GetFollowersRequest& req) {
                // Deterministic sample followers list per user id
                GetFollowersResponse resp;
                std::hash<std::string> h;
                size_t base = h(req.user_id()) % 7;
                std::vector<std::string> crowd = {"user123","user456","user789","userABC","userDEF","userGHI","userJKL"};
                for (size_t i = 0; i < crowd.size(); ++i) {
                    if ((i + base) % 3 != 0) resp.user_ids_.push_back(crowd[i]);
                }
                return resp;
            }
        };
    };
}
} // namespace sonet
