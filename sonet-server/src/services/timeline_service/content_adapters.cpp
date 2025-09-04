//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "implementations.h"
#include <algorithm>
#include <random>
#include <iostream>

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

    // Create sample note for testing
    ::sonet::note::Note CreateSampleNote(
        const std::string& note_id,
        const std::string& author_id,
        const std::string& content,
        std::chrono::system_clock::time_point created_at = std::chrono::system_clock::now()
    ) {
        ::sonet::note::Note note;
        note.set_id(note_id);
        note.set_author_id(author_id);
        note.set_content(content);
        note.set_visibility(::sonet::note::VISIBILITY_PUBLIC);
        *note.mutable_created_at() = ToProtoTimestamp(created_at);
        *note.mutable_updated_at() = ToProtoTimestamp(created_at);
        
        // Add some sample metrics
        auto* metrics = note.mutable_metrics();
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 100);
        
        metrics->set_views(dis(gen) + 10);
        metrics->set_likes(dis(gen) / 5);
    metrics->set_renotes(dis(gen) / 10);
        metrics->set_replies(dis(gen) / 8);
        metrics->set_quotes(dis(gen) / 15);
        
        return note;
    }
}

// ============= FOLLOWING CONTENT ADAPTER IMPLEMENTATION =============

FollowingContentAdapter::FollowingContentAdapter(std::shared_ptr<::sonet::note::NoteService::Stub> note_service)
    : note_service_(note_service) {
    std::cout << "Following Content Adapter initialized" << std::endl;
}

std::vector<::sonet::note::Note> FollowingContentAdapter::GetContent(
    const std::string& user_id,
    const TimelineConfig& config,
    std::chrono::system_clock::time_point since,
    int32_t limit
) {
    std::cout << "Fetching following content for user " << user_id 
              << " (limit: " << limit << ")" << std::endl;
    
    std::vector<::sonet::note::Note> notes;
    
    // Get the user's following list
    auto following_list = GetFollowingList(user_id);
    
    if (following_list.empty()) {
        std::cout << "User " << user_id << " is not following anyone" << std::endl;
        return notes;
    }
    
    std::cout << "User " << user_id << " follows " << following_list.size() << " accounts" << std::endl;
    
    // For now, create sample content from followed users
    // In production, this would query the note service
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> content_dis(0, following_list.size() - 1);
    
    std::vector<std::string> sample_contents = {
        "Just had an amazing coffee! ☕ #coffee #morning",
        "Working on some exciting new features today! 💻 #coding #development",
        "Beautiful sunset from my window 🌅 #photography #nature",
        "Reading a great book about machine learning 📚 #ai #learning",
        "Weekend plans: hiking and relaxation 🏔️ #weekend #hiking",
        "New recipe turned out perfectly! 👨‍🍳 #cooking #food",
        "Concert was absolutely incredible! 🎵 #music #livemusic",
        "Travel planning for next month ✈️ #travel #adventure",
        "Great workout session this morning 💪 #fitness #health",
        "Team lunch at our favorite restaurant 🍕 #team #food"
    };
    
    // Generate sample notes from followed users
    int notes_per_user = std::max(1, limit / static_cast<int>(following_list.size()));
    int generated = 0;
    
    for (const auto& followed_user : following_list) {
        if (generated >= limit) break;
        
        for (int i = 0; i < notes_per_user && generated < limit; ++i) {
            std::uniform_int_distribution<> time_dis(1, 24 * 7); // Last week
            auto created_time = std::chrono::system_clock::now() - std::chrono::hours(time_dis(gen));
            
            if (created_time < since) continue; // Respect since filter
            
            std::string note_id = "note_" + std::to_string(generated + 1);
            std::string content = sample_contents[content_dis(gen) % sample_contents.size()];
            
            notes.push_back(CreateSampleNote(note_id, followed_user, content, created_time));
            generated++;
        }
    }
    
    // Sort by creation time (newest first)
    std::sort(notes.begin(), notes.end(), 
        [](const ::sonet::note::Note& a, const ::sonet::note::Note& b) {
            return a.created_at().seconds() > b.created_at().seconds();
        });
    
    std::cout << "Generated " << notes.size() << " notes from following content" << std::endl;
    return notes;
}

std::vector<std::string> FollowingContentAdapter::GetFollowingList(const std::string& user_id) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Check cache first
    auto cache_it = following_cache_.find(user_id);
    auto timestamp_it = cache_timestamps_.find(user_id);
    
    auto now = std::chrono::system_clock::now();
    bool cache_valid = (cache_it != following_cache_.end() && 
                       timestamp_it != cache_timestamps_.end() &&
                       (now - timestamp_it->second) < std::chrono::minutes(10)); // 10 min cache
    
    if (cache_valid) {
        return cache_it->second;
    }
    
    // In production, this would query the follow service
    // For now, create sample following lists
    std::vector<std::string> following_list;
    
    // Create some sample followed users
    std::vector<std::string> sample_users = {
        "alice_dev", "bob_designer", "charlie_pm", "diana_data", "eve_security",
        "frank_frontend", "grace_backend", "henry_devops", "iris_mobile", "jack_ml"
    };
    
    // Random sample of 3-7 followed users
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> count_dis(3, 7);
    std::uniform_int_distribution<> user_dis(0, sample_users.size() - 1);
    
    int follow_count = count_dis(gen);
    std::unordered_set<std::string> selected;
    
    while (static_cast<int>(selected.size()) < follow_count) {
        selected.insert(sample_users[user_dis(gen)]);
    }
    
    following_list.assign(selected.begin(), selected.end());
    
    // Update cache
    following_cache_[user_id] = following_list;
    cache_timestamps_[user_id] = now;
    
    return following_list;
}

// ============= RECOMMENDED CONTENT ADAPTER IMPLEMENTATION =============

RecommendedContentAdapter::RecommendedContentAdapter(
    std::shared_ptr<::sonet::note::NoteService::Stub> note_service,
    std::shared_ptr<MLRankingEngine> ranking_engine
) : note_service_(note_service), ranking_engine_(ranking_engine) {
    std::cout << "Recommended Content Adapter initialized" << std::endl;
}

std::vector<::sonet::note::Note> RecommendedContentAdapter::GetContent(
    const std::string& user_id,
    const TimelineConfig& config,
    std::chrono::system_clock::time_point since,
    int32_t limit
) {
    std::cout << "Fetching recommended content for user " << user_id 
              << " (limit: " << limit << ")" << std::endl;
    
    // In production, this would use ML models to find similar content
    // For now, create sample recommended content
    
    std::vector<::sonet::note::Note> notes;
    
    std::vector<std::string> recommended_topics = {
        "Exciting developments in #AI and machine learning! 🤖 #technology #innovation",
        "Great tips for #productivity and time management ⏰ #lifehacks #efficiency",
        "Amazing #photography from around the world 📸 #art #travel",
        "Delicious #recipes for busy weekdays 🍽️ #cooking #quickmeals",
        "Latest trends in #webdevelopment and #programming 💻 #coding #tech",
        "Inspiring #entrepreneurship stories from startups 🚀 #business #success",
        "Fascinating #science discoveries and breakthroughs 🔬 #research #knowledge",
        "Creative #design patterns and user experience tips 🎨 #ux #ui",
        "Sustainable living and #environment friendly tips 🌱 #sustainability #green",
        "Mental health awareness and #wellness strategies 🧘‍♀️ #mindfulness #health"
    };
    
    std::vector<std::string> recommended_authors = {
        "ai_researcher", "productivity_guru", "photo_artist", "chef_alex", "code_ninja",
        "startup_mentor", "science_explorer", "design_wizard", "eco_warrior", "wellness_coach"
    };
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> topic_dis(0, recommended_topics.size() - 1);
    std::uniform_int_distribution<> author_dis(0, recommended_authors.size() - 1);
    std::uniform_int_distribution<> time_dis(1, 48); // Last 2 days
    
    for (int i = 0; i < limit; ++i) {
        auto created_time = std::chrono::system_clock::now() - std::chrono::hours(time_dis(gen));
        
        if (created_time < since) {
            created_time = since + std::chrono::minutes(i * 10); // Ensure it's after since
        }
        
        std::string note_id = "rec_note_" + std::to_string(i + 1);
        std::string author = recommended_authors[author_dis(gen)];
        std::string content = recommended_topics[topic_dis(gen)];
        
        notes.push_back(CreateSampleNote(note_id, author, content, created_time));
    }
    
    // Sort by creation time (newest first)
    std::sort(notes.begin(), notes.end(), 
        [](const ::sonet::note::Note& a, const ::sonet::note::Note& b) {
            return a.created_at().seconds() > b.created_at().seconds();
        });
    
    std::cout << "Generated " << notes.size() << " recommended notes" << std::endl;
    return notes;
}

std::vector<::sonet::note::Note> RecommendedContentAdapter::FindSimilarContent(
    const std::string& user_id,
    const UserEngagementProfile& profile,
    int32_t limit
) {
    // TODO: Implement content similarity using ML
    // This would analyze user's engagement history and find similar content
    
    std::vector<::sonet::note::Note> similar_notes;
    // Placeholder implementation
    return similar_notes;
}

// ============= TRENDING PROVIDERS IMPLEMENTATION =============

TrendingHashtagsProvider::TrendingHashtagsProvider() {
    MaybeRefresh();
}

void TrendingHashtagsProvider::MaybeRefresh() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::system_clock::now();
    if (last_update_.time_since_epoch().count() == 0 ||
        (now - last_update_) > std::chrono::hours(1)) {
        UpdateTrendingHashtags();
        last_update_ = now;
    }
}

void TrendingHashtagsProvider::UpdateTrendingHashtags() {
    trending_hashtags_ = {
        "ai", "technology", "coding", "startup", "innovation",
        "productivity", "design", "photography", "travel", "food",
        "fitness", "music", "books", "gaming", "science"
    };
}

std::vector<::sonet::note::Note> TrendingHashtagsProvider::Get(int32_t limit,
    std::chrono::system_clock::time_point since) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<::sonet::note::Note> notes;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> hashtag_dis(0, std::max(0, (int)trending_hashtags_.size() - 1));
    std::uniform_int_distribution<> time_dis(1, 6); // last 6 hours

    std::vector<std::string> templates = {
        "Breaking: Major developments in #{} technology! This could change everything 🚀",
        "Amazing {} tips that everyone should know! Thread 🧵",
        "The future of {} is looking incredibly bright ✨",
        "Just discovered this incredible {} resource - sharing with everyone!",
        "Hot take: {} is about to revolutionize the industry 💡",
        "Weekly {} roundup: Here are the highlights you missed 📅",
        "Deep dive into {} - what you need to know right now 🔍",
        "Game-changing {} announcement just dropped! 🎮",
        "The {} community is absolutely crushing it today! 💪",
        "Mind-blowing {} facts that will surprise you 🤯"
    };

    for (int i = 0; i < limit && !trending_hashtags_.empty(); ++i) {
        std::string hashtag = trending_hashtags_[hashtag_dis(gen)];
        std::string tmpl = templates[i % templates.size()];
        size_t pos = tmpl.find("{}");
        if (pos != std::string::npos) {
            tmpl.replace(pos, 2, "#" + hashtag);
        }
        auto created_time = std::chrono::system_clock::now() - std::chrono::hours(time_dis(gen));
        if (created_time < since) created_time = since;
        std::string note_id = "trend_hash_" + std::to_string(i + 1);
        std::string author = "trending_user_" + std::to_string((i % 5) + 1);
        auto note = CreateSampleNote(note_id, author, tmpl, created_time);
        auto* m = note.mutable_metrics();
        m->set_views(m->views() * 5);
        m->set_likes(m->likes() * 3);
    m->set_renotes(m->renotes() * 4);
        m->set_replies(m->replies() * 2);
        notes.push_back(note);
    }
    return notes;
}

TrendingTopicsProvider::TrendingTopicsProvider() {
    MaybeRefresh();
}

void TrendingTopicsProvider::MaybeRefresh() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::system_clock::now();
    if (last_update_.time_since_epoch().count() == 0 ||
        (now - last_update_) > std::chrono::hours(1)) {
        UpdateTrendingTopics();
        last_update_ = now;
    }
}

void TrendingTopicsProvider::UpdateTrendingTopics() {
    trending_topics_ = {
        "world_news", "sports_final", "tech_launch", "movie_release", "music_awards",
        "space_mission", "election_debate", "stock_rally", "game_update", "weather_alert"
    };
}

std::vector<::sonet::note::Note> TrendingTopicsProvider::Get(int32_t limit,
    std::chrono::system_clock::time_point since) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<::sonet::note::Note> notes;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> topic_dis(0, std::max(0, (int)trending_topics_.size() - 1));
    std::uniform_int_distribution<> time_dis(1, 6);

    for (int i = 0; i < limit && !trending_topics_.empty(); ++i) {
        std::string topic = trending_topics_[topic_dis(gen)];
        std::string content = "Trending now: " + topic + " — live updates and best takes.";
        auto created_time = std::chrono::system_clock::now() - std::chrono::hours(time_dis(gen));
        if (created_time < since) created_time = since;
        std::string note_id = "trend_topic_" + std::to_string(i + 1);
        std::string author = "topic_curator_" + std::to_string((i % 5) + 1);
        auto note = CreateSampleNote(note_id, author, content, created_time);
        auto* m = note.mutable_metrics();
        m->set_views(m->views() * 4);
        m->set_likes(m->likes() * 2);
    m->set_renotes(m->renotes() * 3);
        m->set_replies(m->replies() * 2);
        notes.push_back(note);
    }
    return notes;
}

TrendingVideosProvider::TrendingVideosProvider(std::shared_ptr<::sonet::note::NoteService::Stub> note_service)
    : note_service_(note_service) {
    MaybeRefresh();
}

void TrendingVideosProvider::MaybeRefresh() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::system_clock::now();
    if (last_update_.time_since_epoch().count() == 0 ||
        (now - last_update_) > std::chrono::hours(1)) {
        UpdateTrendingVideos();
        last_update_ = now;
    }
}

void TrendingVideosProvider::UpdateTrendingVideos() {
    trending_video_urls_ = {
        "https://cdn.example.com/video/abc123.m3u8",
        "https://cdn.example.com/video/def456.m3u8",
        "https://cdn.example.com/video/ghi789.m3u8",
        "https://cdn.example.com/video/jkl012.m3u8",
        "https://cdn.example.com/video/mno345.m3u8"
    };
}

std::vector<::sonet::note::Note> TrendingVideosProvider::Get(int32_t limit,
    std::chrono::system_clock::time_point since) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<::sonet::note::Note> notes;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> url_dis(0, std::max(0, (int)trending_video_urls_.size() - 1));
    std::uniform_int_distribution<> time_dis(1, 6);

    for (int i = 0; i < limit && !trending_video_urls_.empty(); ++i) {
        std::string url = trending_video_urls_[url_dis(gen)];
        std::string content = std::string("Watch this trending video ▶️ ") + url;
        auto created_time = std::chrono::system_clock::now() - std::chrono::hours(time_dis(gen));
        if (created_time < since) created_time = since;
        std::string note_id = "trend_video_" + std::to_string(i + 1);
        std::string author = "video_creator_" + std::to_string((i % 5) + 1);
        auto note = CreateSampleNote(note_id, author, content, created_time);
        auto* m = note.mutable_metrics();
        m->set_views(m->views() * 12);
        m->set_likes(m->likes() * 6);
    m->set_renotes(m->renotes() * 5);
        m->set_replies(m->replies() * 3);
        notes.push_back(note);
    }
    return notes;
}

// ============= TRENDING CONTENT ADAPTER IMPLEMENTATION =============

TrendingContentAdapter::TrendingContentAdapter(std::shared_ptr<::sonet::note::NoteService::Stub> note_service)
    : note_service_(note_service) {
    hashtags_provider_ = std::make_unique<TrendingHashtagsProvider>();
    topics_provider_ = std::make_unique<TrendingTopicsProvider>();
    videos_provider_ = std::make_unique<TrendingVideosProvider>(note_service_);
    std::cout << "Trending Content Adapter initialized (hashtags/topics/videos)" << std::endl;
}

std::vector<::sonet::note::Note> TrendingContentAdapter::GetContent(
    const std::string& user_id,
    const TimelineConfig& config,
    std::chrono::system_clock::time_point since,
    int32_t limit
) {
    (void)user_id; // not used in this stub
    std::cout << "Fetching trending content (composite) for user " << user_id << " (limit: " << limit << ")" << std::endl;

    // Refresh providers if needed
    hashtags_provider_->MaybeRefresh();
    topics_provider_->MaybeRefresh();
    videos_provider_->MaybeRefresh();

    // Ratios inside trending bucket
    int hashtags_limit = std::max(0, (int)std::floor(limit * 0.5));
    int topics_limit   = std::max(0, (int)std::floor(limit * 0.3));
    int videos_limit   = std::max(0, limit - hashtags_limit - topics_limit);

    auto hashtag_notes = hashtags_provider_->Get(hashtags_limit, since);
    auto topic_notes   = topics_provider_->Get(topics_limit, since);
    auto video_notes   = videos_provider_->Get(videos_limit, since);

    std::vector<::sonet::note::Note> notes;
    notes.reserve(hashtag_notes.size() + topic_notes.size() + video_notes.size());
    notes.insert(notes.end(), hashtag_notes.begin(), hashtag_notes.end());
    notes.insert(notes.end(), topic_notes.begin(), topic_notes.end());
    notes.insert(notes.end(), video_notes.begin(), video_notes.end());

    // Sort by a simple engagement score (likes + renotes + replies)
    std::sort(notes.begin(), notes.end(),
        [](const ::sonet::note::Note& a, const ::sonet::note::Note& b) {
            auto a_score = a.metrics().likes() + a.metrics().renotes() + a.metrics().replies();
            auto b_score = b.metrics().likes() + b.metrics().renotes() + b.metrics().replies();
            return a_score > b_score;
        });

    if ((int)notes.size() > limit) notes.resize(limit);
    std::cout << "Generated " << notes.size() << " trending notes (composite)" << std::endl;
    return notes;
}

// ============= LISTS CONTENT ADAPTER IMPLEMENTATION =============

class ListsContentAdapter : public ContentSourceAdapter {
public:
    explicit ListsContentAdapter(std::shared_ptr<::sonet::note::NoteService::Stub> note_service)
        : note_service_(std::move(note_service)) {
        std::cout << "Lists Content Adapter initialized" << std::endl;
    }

    std::vector<::sonet::note::Note> GetContent(
        const std::string& user_id,
        const TimelineConfig& /*config*/,
        std::chrono::system_clock::time_point since,
        int32_t limit
    ) override {
        // Stub implementation: generate sample notes from a few list authors
        std::vector<::sonet::note::Note> notes;
        std::vector<std::string> list_authors = {"list_author_a", "list_author_b", "list_author_c"};
        std::vector<std::string> contents = {
            "Curated pick: Top engineering reads #tech",
            "Curated pick: Product insights #product",
            "Curated pick: Design inspirations #design"
        };
        std::random_device rd; std::mt19937 gen(rd());
        std::uniform_int_distribution<> author_dis(0, static_cast<int>(list_authors.size()) - 1);
        std::uniform_int_distribution<> content_dis(0, static_cast<int>(contents.size()) - 1);
        std::uniform_int_distribution<> time_dis(1, 72);
        for (int i = 0; i < limit; ++i) {
            auto created_time = std::chrono::system_clock::now() - std::chrono::hours(time_dis(gen));
            if (created_time < since) created_time = since;
            ::sonet::note::Note note;
            note.set_id("list_note_" + std::to_string(i + 1));
            note.set_author_id(list_authors[author_dis(gen)]);
            note.set_content(contents[content_dis(gen)] + " (for " + user_id + ")");
            note.set_visibility(::sonet::note::VISIBILITY_PUBLIC);
            *note.mutable_created_at() = ToProtoTimestamp(created_time);
            *note.mutable_updated_at() = ToProtoTimestamp(created_time);
            auto* metrics = note.mutable_metrics();
            metrics->set_views(50 + i * 3);
            metrics->set_likes(5 + (i % 7));
            metrics->set_renotes(1 + (i % 4));
            notes.push_back(note);
        }
        return notes;
    }

private:
    std::shared_ptr<::sonet::note::NoteService::Stub> note_service_;
};

// ============= FACTORY FUNCTION =============

std::shared_ptr<TimelineServiceImpl> CreateTimelineService(
    const std::string& redis_host,
    int redis_port,
    int websocket_port,
    std::shared_ptr<::sonet::note::NoteService::Stub> note_service
) {
    // Create components
    auto cache = std::make_shared<RedisTimelineCache>(redis_host, redis_port);
    auto ranking_engine = std::make_shared<MLRankingEngine>();
    auto content_filter = std::make_shared<AdvancedContentFilter>();
    auto realtime_notifier = std::make_shared<WebSocketRealtimeNotifier>(websocket_port);
    
    // Create content source adapters
    std::unordered_map<::sonet::timeline::ContentSource, std::shared_ptr<ContentSourceAdapter>> content_sources;
    
    // Prefer real adapters when available services are provided
    if (note_service) {
        auto note_client = std::make_shared<clients::StubBackedNoteClient>(note_service);
        auto follow_client = std::make_shared<clients::StubBackedFollowClient>();
        content_sources[::sonet::timeline::CONTENT_SOURCE_FOLLOWING] = 
            std::make_shared<RealFollowingContentAdapter>(note_client, follow_client);
        content_sources[::sonet::timeline::CONTENT_SOURCE_LISTS] =
            std::make_shared<RealListsContentAdapter>(note_client);
    } else {
        content_sources[::sonet::timeline::CONTENT_SOURCE_FOLLOWING] = 
            std::make_shared<FollowingContentAdapter>(note_service);
        content_sources[::sonet::timeline::CONTENT_SOURCE_LISTS] =
            std::make_shared<ListsContentAdapter>(note_service);
    }
    
    content_sources[::sonet::timeline::CONTENT_SOURCE_RECOMMENDED] = 
        std::make_shared<RecommendedContentAdapter>(note_service, ranking_engine);
    
    content_sources[::sonet::timeline::CONTENT_SOURCE_TRENDING] = 
        std::make_shared<TrendingContentAdapter>(note_service);
    
    // Start real-time notifier
    realtime_notifier->Start();
    
    auto timeline_service = std::make_shared<TimelineServiceImpl>(
        cache, ranking_engine, content_filter, realtime_notifier, content_sources,
        note_service ? std::make_shared<::sonet::follow::FollowService::Stub>() : nullptr);
    
    std::cout << "Timeline service created with all components initialized" << std::endl;
    
    return timeline_service;
}

} // namespace sonet::timeline
