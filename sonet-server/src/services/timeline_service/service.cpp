//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "service.h"
#include <algorithm>
#include <cmath>
#include <random>
#include <sstream>
#include <thread>
#include <iostream>
#include <unordered_set>
#include <cstring>

namespace sonet::timeline {

namespace {
    // Helper to convert system_clock::time_point to protobuf timestamp
    ::sonet::common::Timestamp ToProtoTimestamp(std::chrono::system_clock::time_point tp) {
        ::sonet::common::Timestamp result;
        auto duration = tp.time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);
        
        result.set_seconds(seconds.count());
        result.set_nanos(static_cast<int32_t>(nanos.count()));
        return result;
    }

    // Helper to convert protobuf timestamp to system_clock::time_point
    std::chrono::system_clock::time_point FromProtoTimestamp(const ::sonet::common::Timestamp& ts) {
        auto duration = std::chrono::seconds(ts.seconds()) + std::chrono::nanoseconds(ts.nanos());
        return std::chrono::system_clock::time_point(duration);
    }

    // Calculate time decay factor for recency scoring
    double CalculateTimeDecay(std::chrono::system_clock::time_point created_at, double half_life_hours = 6.0) {
        auto now = std::chrono::system_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::hours>(now - created_at);
        double age_hours = static_cast<double>(age.count());
        return std::exp(-age_hours * std::log(2.0) / half_life_hours);
    }

    // Generate unique ID for timeline items
    std::string GenerateItemId() {
        static std::random_device rd;
        static std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        std::ostringstream oss;
        oss << std::hex << dis(gen);
        return oss.str();
    }
}

// ============= TIMELINE SERVICE IMPLEMENTATION =============

TimelineServiceImpl::TimelineServiceImpl(
    std::shared_ptr<TimelineCache> cache,
    std::shared_ptr<RankingEngine> ranking_engine,
    std::shared_ptr<ContentFilter> content_filter,
    std::shared_ptr<RealtimeNotifier> realtime_notifier,
    std::unordered_map<::sonet::timeline::ContentSource, std::shared_ptr<ContentSourceAdapter>> content_sources,
    std::shared_ptr<::sonet::follow::FollowService::Stub> follow_service
) : cache_(std::move(cache)),
    ranking_engine_(std::move(ranking_engine)),
    content_filter_(std::move(content_filter)),
    realtime_notifier_(std::move(realtime_notifier)),
    content_sources_(std::move(content_sources)),
    follow_service_(std::move(follow_service)) {
    
    // Initialize default configuration
    default_config_.algorithm = ::sonet::timeline::TIMELINE_ALGORITHM_HYBRID;
    default_config_.max_items = 50;
    default_config_.max_age_hours = 24;
    default_config_.min_score_threshold = 0.1;
    
    // Algorithm weights
    default_config_.recency_weight = 0.3;
    default_config_.engagement_weight = 0.25;
    default_config_.author_affinity_weight = 0.2;
    default_config_.content_quality_weight = 0.15;
    default_config_.diversity_weight = 0.1;
    
    // Content mix
    default_config_.following_content_ratio = 0.7;
    default_config_.recommended_content_ratio = 0.2;
    default_config_.trending_content_ratio = 0.1;
    
    std::cout << "Timeline service initialized with " << content_sources_.size() << " content sources" << std::endl;
    // Start fanout worker
    fanout_running_.store(true);
    fanout_thread_ = std::thread([this]() { FanoutLoop(); });
}

TimelineServiceImpl::~TimelineServiceImpl() {
    fanout_running_.store(false);
    fanout_cv_.notify_all();
    if (fanout_thread_.joinable()) fanout_thread_.join();
}

grpc::Status TimelineServiceImpl::GetTimeline(
    grpc::ServerContext* context,
    const ::sonet::timeline::GetTimelineRequest* request,
    ::sonet::timeline::GetTimelineResponse* response
) {
    try {
        // Per-user rate limit
        int rpm_override = -1; try { auto v = GetMetadataValue(context, "x-rate-rpm"); if (!v.empty()) rpm_override = std::stoi(v); } catch (...) {}
        if (!RateAllow("tl:" + request->user_id(), rpm_override)) {
            return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "rate limit");
        }
        if (!IsAuthorized(context, request->user_id())) {
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Unauthorized access");
        }

        std::cout << "Generating timeline for user: " << request->user_id() << std::endl;

        // Get user configuration
        TimelineConfig config = GetUserTimelineConfig(request->user_id());
        if (request->algorithm() != ::sonet::timeline::TIMELINE_ALGORITHM_UNKNOWN) {
            config.algorithm = request->algorithm();
        }

        // Allow A/B overrides via metadata
        ApplyABOverridesFromMetadata(context, config);

        // Try cache first
        std::vector<RankedTimelineItem> timeline_items;
        bool cache_hit = cache_->GetTimeline(request->user_id(), timeline_items);

        if (!cache_hit || timeline_items.empty()) {
            std::cout << "Cache miss - generating new timeline" << std::endl;
            
            // Generate timeline
            auto since = std::chrono::system_clock::now() - std::chrono::hours(config.max_age_hours);
            timeline_items = GenerateTimeline(request->user_id(), config, since, config.max_items);
            
            // Cache the result (default TTL)
            cache_->SetTimeline(request->user_id(), timeline_items);
        } else {
            std::cout << "Cache hit - using cached timeline with " << timeline_items.size() << " items" << std::endl;
        }

        // Apply pagination (clamp offset and bounds-check)
        int32_t raw_offset = request->pagination().offset;
        int32_t offset = std::max(0, raw_offset);
        int32_t limit = request->pagination().limit > 0 ? request->pagination().limit : 20;
        
        size_t start_index = static_cast<size_t>(std::min<int32_t>(offset, static_cast<int32_t>(timeline_items.size())));
        size_t end_index = std::min(start_index + static_cast<size_t>(limit), timeline_items.size());
        
        // Build response
        for (size_t i = start_index; i < end_index; ++i) {
            const auto& it = timeline_items[i];
            auto* item = response->add_items();
            *item->mutable_note() = it.note;
            item->set_source(it.source);
            item->set_final_score(it.final_score);
            *item->mutable_injected_at() = ToProtoTimestamp(it.injected_at);
            item->set_injection_reason(it.injection_reason);
            
            if (request->include_ranking_signals()) {
                *item->mutable_ranking_signals() = it.signals;
            }
        }

        // Set metadata
            auto* metadata = response->mutable_metadata();
    *metadata = BuildTimelineMetadata(timeline_items, request->user_id(), config);
    auto* params = metadata->mutable_algorithm_params();
    (*params)["recency_weight"] = config.recency_weight;
    (*params)["engagement_weight"] = config.engagement_weight;
    (*params)["author_affinity_weight"] = config.author_affinity_weight;
    (*params)["content_quality_weight"] = config.content_quality_weight;
    (*params)["diversity_weight"] = config.diversity_weight;

        // Set pagination info
        auto* page_info = response->mutable_pagination();
        page_info->set_offset(offset);
        page_info->set_limit(limit);
        page_info->set_total_count(static_cast<int32_t>(timeline_items.size()));
        page_info->set_has_next(static_cast<int32_t>(end_index) < static_cast<int32_t>(timeline_items.size()));

        response->set_success(true);
        
        // Update metrics
        {
            std::lock_guard<std::mutex> lock(metrics_mutex_);
            metrics_["timeline_requests"]++;
            if (cache_hit) metrics_["cache_hits"]++;
            else metrics_["cache_misses"]++;
        }

        std::cout << "Timeline generated successfully: " << (end_index - start_index) << " items returned" << std::endl;
        return grpc::Status::OK;

    } catch (const std::exception& e) {
        std::cerr << "Error generating timeline: " << e.what() << std::endl;
        response->set_success(false);
        response->set_error_message(e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status TimelineServiceImpl::RefreshTimeline(
    grpc::ServerContext* context,
    const ::sonet::timeline::RefreshTimelineRequest* request,
    ::sonet::timeline::RefreshTimelineResponse* response
) {
    try {
        if (!IsAuthorized(context, request->user_id())) {
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Unauthorized access");
        }

        std::cout << "Refreshing timeline for user: " << request->user_id() << std::endl;

        // Invalidate cache to force regeneration
        cache_->InvalidateTimeline(request->user_id());

        // Get fresh content since specified time
        auto since = FromProtoTimestamp(request->since());
        auto config = GetUserTimelineConfig(request->user_id());
        int32_t max_items = request->max_items() > 0 ? request->max_items() : 20;

        auto new_items = GenerateTimeline(request->user_id(), config, since, max_items);

        // Build response
        for (const auto& item : new_items) {
            auto* timeline_item = response->add_new_items();
            *timeline_item->mutable_note() = item.note;
            timeline_item->set_source(item.source);
            timeline_item->set_final_score(item.final_score);
            *timeline_item->mutable_injected_at() = ToProtoTimestamp(item.injected_at);
            timeline_item->set_injection_reason(item.injection_reason);
        }

        response->set_total_new_items(static_cast<int32_t>(new_items.size()));
        response->set_has_more(new_items.size() >= static_cast<size_t>(max_items));
        response->set_success(true);

        // Notify real-time subscribers
        if (!new_items.empty()) {
            realtime_notifier_->NotifyNewItems(request->user_id(), new_items);
        }

        std::cout << "Timeline refreshed: " << new_items.size() << " new items" << std::endl;
        return grpc::Status::OK;

    } catch (const std::exception& e) {
        std::cerr << "Error refreshing timeline: " << e.what() << std::endl;
        response->set_success(false);
        response->set_error_message(e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status TimelineServiceImpl::MarkTimelineRead(
    grpc::ServerContext* context,
    const ::sonet::timeline::MarkTimelineReadRequest* request,
    ::sonet::timeline::MarkTimelineReadResponse* response
) {
    try {
        if (!IsAuthorized(context, request->user_id())) {
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Unauthorized access");
        }

        auto read_until = FromProtoTimestamp(request->read_until());
        cache_->SetLastRead(request->user_id(), read_until);

        response->set_success(true);
        std::cout << "Timeline marked as read for user: " << request->user_id() << std::endl;
        return grpc::Status::OK;

    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status TimelineServiceImpl::HealthCheck(
    grpc::ServerContext* /*context*/,
    const ::sonet::timeline::HealthCheckRequest* /*request*/,
    ::sonet::timeline::HealthCheckResponse* response
) {
    response->set_status("healthy");
    
    // Add component health details
    auto& details = *response->mutable_details();
    details["cache"] = cache_ ? "healthy" : "unavailable";
    details["ranking_engine"] = ranking_engine_ ? "healthy" : "unavailable";
    details["content_filter"] = content_filter_ ? "healthy" : "unavailable";
    details["realtime_notifier"] = realtime_notifier_ ? "healthy" : "unavailable";
    details["content_sources"] = std::to_string(content_sources_.size());
    
    {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        details["total_requests"] = std::to_string(metrics_["timeline_requests"].load());
        details["cache_hit_ratio"] = metrics_["timeline_requests"] > 0 
            ? std::to_string(static_cast<double>(metrics_["cache_hits"]) / metrics_["timeline_requests"]) 
            : "0.0";
    }
    
    return grpc::Status::OK;
}

grpc::Status TimelineServiceImpl::RecordEngagement(
    grpc::ServerContext* /*context*/,
    const ::sonet::timeline::RecordEngagementRequest* request,
    ::sonet::timeline::RecordEngagementResponse* response
) {
    try {
        if (!request) {
            response->set_success(false);
            response->set_error_message("invalid request");
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid request");
        }
        if (ranking_engine_) {
            ranking_engine_->UpdateUserEngagement(
                request->user_id(), request->note_id(), request->action(), request->duration_seconds());
        }
        response->set_success(true);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status TimelineServiceImpl::GetForYouTimeline(
    grpc::ServerContext* context,
    const ::sonet::timeline::GetForYouTimelineRequest* request,
    ::sonet::timeline::GetForYouTimelineResponse* response
) {
    if (!request) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid request");
    }
    TimelineConfig config = GetUserTimelineConfig(request->user_id());
    if (config.algorithm == ::sonet::timeline::TIMELINE_ALGORITHM_UNKNOWN ||
        config.algorithm == ::sonet::timeline::TIMELINE_ALGORITHM_CHRONOLOGICAL) {
        config.algorithm = ::sonet::timeline::TIMELINE_ALGORITHM_HYBRID;
    }
    ApplyABOverridesFromMetadata(context, config);
    // Discovery toggles from headers (For You only)
    auto disc = GetMetadataValue(context, "x-discovery-share");
    if (!disc.empty()) {
        try {
            double share = std::stod(disc); // 0..1
            share = std::max(0.0, std::min(1.0, share));
            // Scale non-following ratios proportionally
            double non_following = config.recommended_content_ratio + config.trending_content_ratio + config.lists_content_ratio;
            if (non_following > 0.0) {
                double scale = (share) / non_following;
                config.recommended_content_ratio *= scale;
                config.trending_content_ratio *= scale;
                config.lists_content_ratio *= scale;
                config.following_content_ratio = 1.0 - share;
            }
        } catch (...) {}
    }
    auto cap_rec = GetMetadataValue(context, "x-cap-recommended-for-you");
    if (!cap_rec.empty()) { try { config.cap_recommended = std::stoi(cap_rec); } catch (...) {} }
    auto cap_tr = GetMetadataValue(context, "x-cap-trending-for-you");
    if (!cap_tr.empty()) { try { config.cap_trending = std::stoi(cap_tr); } catch (...) {} }
    auto cap_ls = GetMetadataValue(context, "x-cap-lists-for-you");
    if (!cap_ls.empty()) { try { config.cap_lists = std::stoi(cap_ls); } catch (...) {} }
    auto since = std::chrono::system_clock::now() - std::chrono::hours(config.max_age_hours);
    // Overdrive integration toggle via metadata header (safe default: off)
    bool use_overdrive = false;
    try {
        auto v = GetMetadataValue(context, "x-use-overdrive");
        use_overdrive = (v == "1" || v == "true");
    } catch (...) {}

    auto items = GenerateTimeline(request->user_id(), config, since, config.max_items);

    if (use_overdrive && overdrive_client_ && !items.empty()) {
        // Collect candidate ids
        std::vector<std::string> candidate_ids;
        candidate_ids.reserve(items.size());
        for (const auto& it : items) {
            candidate_ids.push_back(it.note.id());
        }
        // Call Overdrive for final ranking
        auto ranked = overdrive_client_->RankForYou(request->user_id(), candidate_ids, config.max_items);
        // Reorder items based on Overdrive scores
        std::unordered_map<std::string, double> score_map;
        score_map.reserve(ranked.size());
        for (const auto& r : ranked) score_map[r.note_id] = r.score;
        std::stable_sort(items.begin(), items.end(), [&](const RankedTimelineItem& a, const RankedTimelineItem& b){
            double sa = a.final_score; auto ita = score_map.find(a.note.id()); if (ita != score_map.end()) sa = ita->second;
            double sb = b.final_score; auto itb = score_map.find(b.note.id()); if (itb != score_map.end()) sb = itb->second;
            return sa > sb;
        });
        // Inject Overdrive signal into final_score
        for (auto& it : items) {
            auto sit = score_map.find(it.note.id());
            if (sit != score_map.end()) it.final_score = sit->second;
        }
    }
 
    int32_t offset = std::max(0, request->pagination().offset);
    int32_t limit = request->pagination().limit > 0 ? request->pagination().limit : 20;
    size_t start_index = static_cast<size_t>(std::min<int32_t>(offset, static_cast<int32_t>(items.size())));
    size_t end_index = std::min(start_index + static_cast<size_t>(limit), items.size());
 
    for (size_t i = start_index; i < end_index; ++i) {
        const auto& it = items[i];
        auto* item = response->add_items();
        *item->mutable_note() = it.note;
        item->set_source(it.source);
        item->set_final_score(it.final_score);
        *item->mutable_injected_at() = ToProtoTimestamp(it.injected_at);
        item->set_injection_reason("for_you");
        if (request->include_ranking_signals()) {
            *item->mutable_ranking_signals() = it.signals;
        }
    }
 
    auto* metadata = response->mutable_metadata();
    *metadata = BuildTimelineMetadata(items, request->user_id(), config);
    auto* params = metadata->mutable_algorithm_params();
    (*params)["recency_weight"] = config.recency_weight;
    (*params)["engagement_weight"] = config.engagement_weight;
    (*params)["author_affinity_weight"] = config.author_affinity_weight;
    (*params)["content_quality_weight"] = config.content_quality_weight;
    (*params)["diversity_weight"] = config.diversity_weight;
 
    auto* page = response->mutable_pagination();
    page->set_offset(offset);
    page->set_limit(limit);
    page->set_total_count(static_cast<int32_t>(items.size()));
    page->set_has_next(static_cast<int32_t>(end_index) < static_cast<int32_t>(items.size()));
 
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status TimelineServiceImpl::GetFollowingTimeline(
    grpc::ServerContext* context,
    const ::sonet::timeline::GetFollowingTimelineRequest* request,
    ::sonet::timeline::GetFollowingTimelineResponse* response
) {
    if (!request) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid request");
    }
    TimelineConfig config = GetUserTimelineConfig(request->user_id());
    config.algorithm = ::sonet::timeline::TIMELINE_ALGORITHM_CHRONOLOGICAL;
    ApplyABOverridesFromMetadata(context, config);
    config.following_content_ratio = 1.0;
    config.recommended_content_ratio = 0.0;
    config.trending_content_ratio = 0.0;
    config.lists_content_ratio = 0.0;
 
    auto since = std::chrono::system_clock::now() - std::chrono::hours(config.max_age_hours);
    auto items = GenerateTimeline(request->user_id(), config, since, config.max_items);
 
    int32_t offset = std::max(0, request->pagination().offset);
    int32_t limit = request->pagination().limit > 0 ? request->pagination().limit : 20;
    size_t start_index = static_cast<size_t>(std::min<int32_t>(offset, static_cast<int32_t>(items.size())));
    size_t end_index = std::min(start_index + static_cast<size_t>(limit), items.size());
 
    for (size_t i = start_index; i < end_index; ++i) {
        const auto& it = items[i];
        auto* item = response->add_items();
        *item->mutable_note() = it.note;
        item->set_source(::sonet::timeline::CONTENT_SOURCE_FOLLOWING);
        item->set_final_score(it.final_score);
        *item->mutable_injected_at() = ToProtoTimestamp(it.injected_at);
        item->set_injection_reason("following");
        if (request->include_ranking_signals()) {
            *item->mutable_ranking_signals() = it.signals;
        }
    }
 
    auto* metadata = response->mutable_metadata();
    *metadata = BuildTimelineMetadata(items, request->user_id(), config);
    auto* params = metadata->mutable_algorithm_params();
    (*params)["mode"] = 0.0; // chronological
 
    auto* page = response->mutable_pagination();
    page->set_offset(offset);
    page->set_limit(limit);
    page->set_total_count(static_cast<int32_t>(items.size()));
    page->set_has_next(static_cast<int32_t>(end_index) < static_cast<int32_t>(items.size()));
 
    response->set_success(true);
    return grpc::Status::OK;
}

// ============= PRIVATE METHODS =============

std::vector<RankedTimelineItem> TimelineServiceImpl::GenerateTimeline(
    const std::string& user_id,
    const TimelineConfig& config,
    std::chrono::system_clock::time_point since,
    int32_t limit
) {
    std::cout << "Generating timeline with algorithm: " << static_cast<int>(config.algorithm) << std::endl;

    // Get user engagement profile
    auto profile = GetOrCreateUserProfile(user_id);
    
    // Collect content from various sources
    std::vector<::sonet::note::Note> all_notes;
    all_notes.reserve(static_cast<size_t>(limit) * 2);
    
    // Following content (70% of timeline)
    int32_t following_limit = static_cast<int32_t>(limit * config.following_content_ratio * config.ab_following_weight);
    following_limit = std::min(following_limit, config.cap_following);
    if (following_limit > 0) {
        auto following_notes = FetchFollowingContent(user_id, config, since, following_limit);
        all_notes.insert(all_notes.end(), following_notes.begin(), following_notes.end());
        std::cout << "Fetched " << following_notes.size() << " notes from following" << std::endl;
    }
    
    // Recommended content (20% of timeline)
    int32_t recommended_limit = static_cast<int32_t>(limit * config.recommended_content_ratio * config.ab_recommended_weight);
    recommended_limit = std::min(recommended_limit, config.cap_recommended);
    if (recommended_limit > 0) {
        auto recommended_notes = FetchRecommendedContent(user_id, profile, config, recommended_limit);
        all_notes.insert(all_notes.end(), recommended_notes.begin(), recommended_notes.end());
        std::cout << "Fetched " << recommended_notes.size() << " recommended notes" << std::endl;
    }
    
    // Trending content
    int32_t trending_limit = static_cast<int32_t>(limit * config.trending_content_ratio * config.ab_trending_weight);
    trending_limit = std::min(trending_limit, config.cap_trending);
    if (trending_limit > 0) {
        auto trending_notes = FetchTrendingContent(user_id, config, trending_limit);
        all_notes.insert(all_notes.end(), trending_notes.begin(), trending_notes.end());
        std::cout << "Fetched " << trending_notes.size() << " trending notes" << std::endl;
    }

    // Lists content
    int32_t lists_limit = static_cast<int32_t>(limit * config.lists_content_ratio * config.ab_lists_weight);
    lists_limit = std::min(lists_limit, config.cap_lists);
    if (lists_limit > 0) {
        auto it = content_sources_.find(::sonet::timeline::CONTENT_SOURCE_LISTS);
        if (it != content_sources_.end()) {
            auto lists_notes = it->second->GetContent(user_id, config, since, lists_limit);
            all_notes.insert(all_notes.end(), lists_notes.begin(), lists_notes.end());
            std::cout << "Fetched " << lists_notes.size() << " lists notes" << std::endl;
        }
    }

    // Deduplicate by note id
    if (!all_notes.empty()) {
        std::unordered_set<std::string> seen_ids;
        seen_ids.reserve(all_notes.size());
        std::vector<::sonet::note::Note> unique_notes;
        unique_notes.reserve(all_notes.size());
        for (const auto& note : all_notes) {
            auto id = note.id();
            if (seen_ids.insert(id).second) {
                unique_notes.push_back(note);
            }
        }
        all_notes.swap(unique_notes);
        std::cout << "After deduplication: " << all_notes.size() << " notes remain" << std::endl;
    }

    // Filter content based on user preferences and safety
    if (content_filter_) {
        all_notes = content_filter_->FilterNotes(all_notes, user_id, profile);
        std::cout << "After filtering: " << all_notes.size() << " notes remain" << std::endl;
    }

    // Score and rank content
    std::vector<RankedTimelineItem> ranked_items;
    if (config.algorithm == ::sonet::timeline::TIMELINE_ALGORITHM_CHRONOLOGICAL || !ranking_engine_) {
        // Pure chronological ordering
        for (const auto& note : all_notes) {
            RankedTimelineItem item;
            item.note = note;
            item.source = ::sonet::timeline::CONTENT_SOURCE_FOLLOWING;
            item.final_score = static_cast<double>(FromProtoTimestamp(note.created_at()).time_since_epoch().count());
            item.injected_at = std::chrono::system_clock::now();
            item.injection_reason = "chronological";
            ranked_items.push_back(item);
        }
    } else {
        ranked_items = ranking_engine_->ScoreNotes(all_notes, user_id, profile, config);
    }

    // Sort by score (descending)
    std::sort(ranked_items.begin(), ranked_items.end(), 
        [](const RankedTimelineItem& a, const RankedTimelineItem& b) {
            return a.final_score > b.final_score;
        });

    // Apply score threshold and limit; enforce per-source caps on final output as a last safety net
    std::vector<RankedTimelineItem> final_items;
    final_items.reserve(static_cast<size_t>(limit));
    std::unordered_map<::sonet::timeline::ContentSource, int> final_counts;
    for (const auto& item : ranked_items) {
        if (item.final_score < config.min_score_threshold) continue;
        if (static_cast<int32_t>(final_items.size()) >= limit) break;
        // Enforce caps by source
        int cap = 0;
        switch (item.source) {
            case ::sonet::timeline::CONTENT_SOURCE_FOLLOWING: cap = config.cap_following; break;
            case ::sonet::timeline::CONTENT_SOURCE_RECOMMENDED: cap = config.cap_recommended; break;
            case ::sonet::timeline::CONTENT_SOURCE_TRENDING: cap = config.cap_trending; break;
            case ::sonet::timeline::CONTENT_SOURCE_LISTS: cap = config.cap_lists; break;
            default: cap = limit; break;
        }
        if (final_counts[item.source] >= cap) continue;
        final_items.push_back(item);
        final_counts[item.source]++;
    }

    std::cout << "Timeline generation complete: " << final_items.size() << " items ranked and filtered" << std::endl;
    return final_items;
}

std::vector<::sonet::note::Note> TimelineServiceImpl::FetchFollowingContent(
    const std::string& user_id,
    const TimelineConfig& config,
    std::chrono::system_clock::time_point since,
    int32_t limit
) {
    auto it = content_sources_.find(::sonet::timeline::CONTENT_SOURCE_FOLLOWING);
    if (it != content_sources_.end()) {
        return it->second->GetContent(user_id, config, since, limit);
    }
    return {};
}

std::vector<::sonet::note::Note> TimelineServiceImpl::FetchRecommendedContent(
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

std::vector<::sonet::note::Note> TimelineServiceImpl::FetchTrendingContent(
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

UserEngagementProfile TimelineServiceImpl::GetOrCreateUserProfile(const std::string& user_id) {
    UserEngagementProfile profile;
    
    if (!cache_->GetUserProfile(user_id, profile)) {
        // Create new profile with defaults
        profile.user_id = user_id;
        profile.last_updated = std::chrono::system_clock::now();
        profile.avg_session_length_minutes = 15.0;  // Default 15 minutes
        profile.daily_engagement_score = 0.5;       // Neutral engagement
        
        cache_->SetUserProfile(user_id, profile);
        std::cout << "Created new user profile for: " << user_id << std::endl;
    }
    
    return profile;
}

TimelineConfig TimelineServiceImpl::GetUserTimelineConfig(const std::string& user_id) {
    // For now, return default config
    // In production, this would fetch user preferences from database
    ::sonet::timeline::TimelinePreferences prefs;
    {
        std::lock_guard<std::mutex> lock(preferences_mutex_);
        auto it = user_preferences_.find(user_id);
        if (it != user_preferences_.end()) {
            prefs = it->second;
        }
    }
    // Defaults if not set
    TimelineConfig config;
    config.algorithm = prefs.algorithm() != ::sonet::timeline::TIMELINE_ALGORITHM_UNKNOWN ? prefs.algorithm() : default_config_.algorithm;
    config.max_items = prefs.max_items() > 0 ? prefs.max_items() : default_config_.max_items;
    config.max_age_hours = prefs.max_age_hours() > 0 ? prefs.max_age_hours() : default_config_.max_age_hours;
    config.min_score_threshold = prefs.min_score_threshold() > 0.0 ? prefs.min_score_threshold() : default_config_.min_score_threshold;
    
    // Algorithm weights
    config.recency_weight = prefs.recency_weight() > 0.0 ? prefs.recency_weight() : default_config_.recency_weight;
    config.engagement_weight = prefs.engagement_weight() > 0.0 ? prefs.engagement_weight() : default_config_.engagement_weight;
    config.author_affinity_weight = prefs.author_affinity_weight() > 0.0 ? prefs.author_affinity_weight() : default_config_.author_affinity_weight;
    config.content_quality_weight = prefs.content_quality_weight() > 0.0 ? prefs.content_quality_weight() : default_config_.content_quality_weight;
    config.diversity_weight = prefs.diversity_weight() > 0.0 ? prefs.diversity_weight() : default_config_.diversity_weight;
    
    // Content mix
    config.following_content_ratio = prefs.following_content_ratio() > 0.0 ? prefs.following_content_ratio() : default_config_.following_content_ratio;
    config.recommended_content_ratio = prefs.recommended_content_ratio() > 0.0 ? prefs.recommended_content_ratio() : default_config_.recommended_content_ratio;
    config.trending_content_ratio = prefs.trending_content_ratio() > 0.0 ? prefs.trending_content_ratio() : default_config_.trending_content_ratio;
    
    return config;
}

::sonet::timeline::TimelineMetadata TimelineServiceImpl::BuildTimelineMetadata(
    const std::vector<RankedTimelineItem>& items,
    const std::string& user_id,
    const TimelineConfig& config
) {
    ::sonet::timeline::TimelineMetadata metadata;
    
    metadata.set_total_items(static_cast<int32_t>(items.size()));
    metadata.set_algorithm_used(config.algorithm);
    metadata.set_timeline_version("v1.0");
    *metadata.mutable_last_updated() = ToProtoTimestamp(std::chrono::system_clock::now());
    
    auto last_read = cache_->GetLastRead(user_id);
    *metadata.mutable_last_user_read() = ToProtoTimestamp(last_read);
    
    // Count new items since last read
    int32_t new_items = 0;
    for (const auto& item : items) {
        if (item.injected_at > last_read) {
            new_items++;
        }
    }
    metadata.set_new_items_since_last_fetch(new_items);
    
    return metadata;
}

std::string TimelineServiceImpl::GetMetadataValue(grpc::ServerContext* context, const std::string& key) {
    auto& client_metadata = context->client_metadata();
    auto it = client_metadata.find(grpc::string_ref(key.c_str(), key.size()));
    if (it != client_metadata.end()) {
        return std::string(it->second.data(), it->second.length());
    }
    return {};
}

bool TimelineServiceImpl::IsAuthorized(grpc::ServerContext* context, const std::string& user_id) {

    // Check x-user-id metadata matches requested user
    std::string caller_id = GetMetadataValue(context, "x-user-id");
    if (!caller_id.empty() && caller_id != user_id) {
        // Check if admin
        std::string admin = GetMetadataValue(context, "x-admin");
        if (admin != "true" && admin != "1") {
            return false;
        }
    }
    
    // Check auth token if required
    const char* required_token = std::getenv("SONET_TIMELINE_TOKEN");
    if (required_token && std::strlen(required_token) > 0) {
        std::string provided_token = GetMetadataValue(context, "x-auth-token");
        if (provided_token != required_token) {
            return false;
        }
    }
    
    return true;
}

void TimelineServiceImpl::ApplyABOverridesFromMetadata(grpc::ServerContext* context, TimelineConfig& config) {
    if (!context) return;
    auto parse_double = [&](const std::string& key, double& target) {
        auto v = GetMetadataValue(context, key);
        if (!v.empty()) {
            try { target = std::stod(v); } catch (...) {}
        }
    };
    auto parse_int = [&](const std::string& key, int32_t& target) {
        auto v = GetMetadataValue(context, key);
        if (!v.empty()) {
            try { target = std::stoi(v); } catch (...) {}
        }
    };
    parse_double("x-ab-following-weight", config.ab_following_weight);
    parse_double("x-ab-recommended-weight", config.ab_recommended_weight);
    parse_double("x-ab-trending-weight", config.ab_trending_weight);
    parse_double("x-ab-lists-weight", config.ab_lists_weight);
    parse_int("x-cap-following", config.cap_following);
    parse_int("x-cap-recommended", config.cap_recommended);
    parse_int("x-cap-trending", config.cap_trending);
    parse_int("x-cap-lists", config.cap_lists);
}

// ============= EVENT HANDLERS =============

bool TimelineServiceImpl::RateAllow(const std::string& key, int override_rpm) {
    std::lock_guard<std::mutex> lock(rate_mutex_);
    auto& b = rate_buckets_[key];
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - b.last_refill).count();
    if (elapsed > 0) {
        int rpm = override_rpm > 0 ? override_rpm : rate_rpm_;
        double tokens_to_add = (static_cast<double>(rpm) / 60000.0) * elapsed;
        b.tokens = std::min(1.0 * rpm, b.tokens + tokens_to_add);
        b.last_refill = now;
    }
    if (b.tokens >= 1.0) { b.tokens -= 1.0; return true; }
    return false;
}

void TimelineServiceImpl::OnNewNote(const ::sonet::note::Note& note) {
    std::cout << "Processing new note event: " << note.id() << " by " << note.author_id() << std::endl;
    
    // Invalidate author's followers' timelines
    cache_->InvalidateAuthorTimelines(note.author_id());
    
    // TODO: Trigger fanout process for this note
    // fanout_service_->InitiateFanout(note);

    // Notify streaming subscribers for the author followers (best-effort)
    ::sonet::timeline::TimelineUpdate upd;
    PushUpdateToSubscribers(note.author_id(), upd);

    // Enqueue to fanout worker
    {
        std::lock_guard<std::mutex> ql(fanout_mutex_);
        fanout_queue_.push(note);
    }
    fanout_cv_.notify_one();
}

void TimelineServiceImpl::OnNoteDeleted(const std::string& note_id, const std::string& author_id) {
    std::cout << "Processing note deletion: " << note_id << std::endl;
    
    // Invalidate affected timelines
    cache_->InvalidateAuthorTimelines(author_id);
    
    // TODO: Notify real-time subscribers about deletion
    // realtime_notifier_->NotifyItemDeleted("*", note_id);

    ::sonet::timeline::TimelineUpdate upd;
    PushUpdateToSubscribers(author_id, upd);
}

void TimelineServiceImpl::OnFollowEvent(const std::string& follower_id, const std::string& following_id, bool is_follow) {
    std::cout << "Processing follow event: " << follower_id << " -> " << following_id 
              << " (follow: " << is_follow << ")" << std::endl;
    
    // Invalidate follower's timeline since their following list changed
    cache_->InvalidateTimeline(follower_id);
    
    // Update user engagement profile to reflect new relationships
    // This would update affinity scores and interests
}

void TimelineServiceImpl::OnNoteUpdated(const ::sonet::note::Note& note) {
    std::cout << "Processing note update: " << note.id() << " by " << note.author_id() << std::endl;
    // Invalidate timelines that may include this author's content
    cache_->InvalidateAuthorTimelines(note.author_id());
    // Optionally notify live connections (best-effort)
    ::sonet::timeline::TimelineUpdate update;
    if (realtime_notifier_) {
        // Also push to streaming sessions
        ::sonet::timeline::TimelineUpdate upd;
        PushUpdateToSubscribers(note.author_id(), upd);
    }
}

// ============= STUB IMPLEMENTATIONS FOR MISSING METHODS =============

grpc::Status TimelineServiceImpl::GetUserTimeline(
    grpc::ServerContext* context,
    const ::sonet::timeline::GetUserTimelineRequest* request,
    ::sonet::timeline::GetUserTimelineResponse* response
) {
    try {
        if (!request) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid request");
        }

        const std::string target_user_id = request->target_user_id();

        // Authorization: allow if requesting self or admin
        (void)context; // context may be null in tests

        // Build simple config derived from preferences
        TimelineConfig config = GetUserTimelineConfig(target_user_id);
        auto since = std::chrono::system_clock::now() - std::chrono::hours(std::max(1, config.max_age_hours));

        // Generate sample notes authored by target user
        std::vector<::sonet::note::Note> authored_notes;
        authored_notes.reserve(50);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> hours_dis(1, std::max(2, config.max_age_hours));

        int32_t to_generate = std::max(10, std::min(50, config.max_items));
        for (int i = 0; i < to_generate; ++i) {
            auto created_time = std::chrono::system_clock::now() - std::chrono::hours(hours_dis(gen));
            if (created_time < since) created_time = since;

            ::sonet::note::Note note;
            note.set_id("user_note_" + std::to_string(i + 1));
            note.set_author_id(target_user_id);
            note.set_content("Note #" + std::to_string(i + 1) + " by " + target_user_id);
            note.set_visibility(::sonet::note::VISIBILITY_PUBLIC);
            *note.mutable_created_at() = ToProtoTimestamp(created_time);
            *note.mutable_updated_at() = ToProtoTimestamp(created_time);
            auto* metrics = note.mutable_metrics();
            metrics->set_views(100 + i * 7);
            metrics->set_likes(10 + (i % 13));
            metrics->set_renotes(2 + (i % 5));
            metrics->set_replies(3 + (i % 7));
            metrics->set_quotes(1 + (i % 3));

            authored_notes.push_back(note);
        }

        // Score and rank
        auto profile = GetOrCreateUserProfile(request->requesting_user_id());
        std::vector<RankedTimelineItem> ranked_items;
        if (ranking_engine_) {
            ranked_items = ranking_engine_->ScoreNotes(authored_notes, request->requesting_user_id(), profile, config);
        }

        // Sort by score desc
        std::sort(ranked_items.begin(), ranked_items.end(), [](const RankedTimelineItem& a, const RankedTimelineItem& b){ return a.final_score > b.final_score; });

        // Pagination
        int32_t offset = std::max(0, request->pagination().offset);
        int32_t limit = request->pagination().limit > 0 ? request->pagination().limit : 20;
        size_t start_index = static_cast<size_t>(std::min<int32_t>(offset, static_cast<int32_t>(ranked_items.size())));
        size_t end_index = std::min(start_index + static_cast<size_t>(limit), ranked_items.size());

        for (size_t i = start_index; i < end_index; ++i) {
            const auto& it = ranked_items[i];
            auto* item = response->add_items();
            *item->mutable_note() = it.note;
            item->set_source(::sonet::timeline::CONTENT_SOURCE_FOLLOWING);
            item->set_final_score(it.final_score);
            *item->mutable_injected_at() = ToProtoTimestamp(std::chrono::system_clock::now());
            item->set_injection_reason("user_profile");
        }

        auto* page = response->mutable_pagination();
        page->set_offset(offset);
        page->set_limit(limit);
        page->set_total_count(static_cast<int32_t>(ranked_items.size()));
        page->set_has_next(static_cast<int32_t>(end_index) < static_cast<int32_t>(ranked_items.size()));

        response->set_success(true);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status TimelineServiceImpl::UpdateTimelinePreferences(
    grpc::ServerContext* /*context*/,
    const ::sonet::timeline::UpdateTimelinePreferencesRequest* request,
    ::sonet::timeline::UpdateTimelinePreferencesResponse* response
) {
    try {
        if (!request) {
            response->set_success(false);
            response->set_error_message("invalid request");
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid request");
        }
        {
            std::lock_guard<std::mutex> lock(preferences_mutex_);
            // Prefer accessor if available; fall back to field for stub compatibility
            user_preferences_[request->user_id()] = request->preferences_;
        }
        response->set_success(true);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status TimelineServiceImpl::GetTimelinePreferences(
    grpc::ServerContext* /*context*/,
    const ::sonet::timeline::GetTimelinePreferencesRequest* request,
    ::sonet::timeline::GetTimelinePreferencesResponse* response
) {
    try {
        if (!request) {
            response->set_success(false);
            response->set_error_message("invalid request");
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid request");
        }
        ::sonet::timeline::TimelinePreferences prefs;
        {
            std::lock_guard<std::mutex> lock(preferences_mutex_);
            auto it = user_preferences_.find(request->user_id());
            if (it != user_preferences_.end()) {
                prefs = it->second;
            }
        }
        // Defaults if not set
        response->preferences_ = prefs;
        response->set_success(true);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

void TimelineServiceImpl::PushUpdateToSubscribers(const std::string& user_id, const ::sonet::timeline::TimelineUpdate& update) {
    std::lock_guard<std::mutex> lock(stream_mutex_);
    auto it = stream_sessions_.find(user_id);
    if (it == stream_sessions_.end()) return;
    auto& vec = it->second;
    for (auto iter = vec.begin(); iter != vec.end();) {
        if (auto sp = iter->lock()) {
            {
                std::lock_guard<std::mutex> ql(sp->mutex);
                sp->pending_updates.push_back(update);
            }
            sp->cv.notify_one();
            ++iter;
        } else {
            iter = vec.erase(iter);
        }
    }
}

grpc::Status TimelineServiceImpl::SubscribeTimelineUpdates(
    grpc::ServerContext* /*context*/,
    const ::sonet::timeline::SubscribeTimelineUpdatesRequest* request,
    grpc::ServerWriter<::sonet::timeline::TimelineUpdate>* writer
) {
    if (!writer || !request) return grpc::Status::OK;

    auto session = std::make_shared<StreamSession>();
    {
        std::lock_guard<std::mutex> lock(stream_mutex_);
        stream_sessions_[request->user_id()].push_back(session);
    }

    // Lightweight token-bucket rate limiter per stream
    const int max_msgs_per_sec = 5;
    int tokens = max_msgs_per_sec;
    auto last_refill = std::chrono::steady_clock::now();

    while (session->open.load()) {
        // Refill tokens
        auto now = std::chrono::steady_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_refill).count();
        if (elapsed_ms >= 1000) {
            tokens = max_msgs_per_sec;
            last_refill = now;
        }

        ::sonet::timeline::TimelineUpdate update;
        {
            std::unique_lock<std::mutex> uq(session->mutex);
            if (session->pending_updates.empty()) {
                session->cv.wait_for(uq, std::chrono::milliseconds(500));
            }
            if (!session->pending_updates.empty()) {
                update = session->pending_updates.front();
                session->pending_updates.pop_front();
            } else {
                // heartbeat
                // update defaults
            }
        }

        if (tokens > 0) {
            writer->Write(update);
            tokens--;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    return grpc::Status::OK;
}

void TimelineServiceImpl::FanoutLoop() {
    while (fanout_running_.load()) {
        ::sonet::note::Note note;
        {
            std::unique_lock<std::mutex> ul(fanout_mutex_);
            if (fanout_queue_.empty()) {
                fanout_cv_.wait_for(ul, std::chrono::milliseconds(500));
            }
            if (!fanout_queue_.empty()) { note = fanout_queue_.front(); fanout_queue_.pop(); }
            else { continue; }
        }
        // Compute affected users via follow service
        if (follow_service_) {
            ::sonet::follow::GetFollowersRequest req; req.user_id_ = note.author_id();
            auto followers = follow_service_->GetFollowers(req).user_ids();
            for (const auto& uid : followers) {
                cache_->InvalidateTimeline(uid);
                ::sonet::timeline::TimelineUpdate upd; PushUpdateToSubscribers(uid, upd);
            }
        }
    }
}

} // namespace sonet::timeline
