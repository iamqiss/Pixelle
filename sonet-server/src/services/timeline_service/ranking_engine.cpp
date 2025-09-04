//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "implementations.h"
#include <algorithm>
#include <cmath>
#include <sstream>
#include <regex>
#include <iostream>
#include <numeric>

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

    // Extract hashtags from text content
    std::vector<std::string> ExtractHashtags(const std::string& text) {
        std::vector<std::string> hashtags;
        std::regex hashtag_regex(R"(#(\w+))");
        std::sregex_iterator iter(text.begin(), text.end(), hashtag_regex);
        std::sregex_iterator end;
        
        for (; iter != end; ++iter) {
            hashtags.push_back((*iter)[1].str());
        }
        return hashtags;
    }

    // Extract mentions from text content
    std::vector<std::string> ExtractMentions(const std::string& text) {
        std::vector<std::string> mentions;
        std::regex mention_regex(R"(@(\w+))");
        std::sregex_iterator iter(text.begin(), text.end(), mention_regex);
        std::sregex_iterator end;
        
        for (; iter != end; ++iter) {
            mentions.push_back((*iter)[1].str());
        }
        return mentions;
    }

    // Simple URL detection
    bool ContainsURL(const std::string& text) {
        std::regex url_regex(R"(https?://[^\s]+)");
        return std::regex_search(text, url_regex);
    }

    // Calculate engagement rate
    double CalculateEngagementRate(const ::sonet::note::Note& note) {
        if (!note.has_metrics()) return 0.0;
        
        const auto& metrics = note.metrics();
    double total_engagements = metrics.likes() + metrics.renotes() + metrics.replies() + metrics.quotes();
        double views = std::max(1.0, static_cast<double>(metrics.views()));
        
        return total_engagements / views;
    }
}

// ============= ML RANKING ENGINE IMPLEMENTATION =============

MLRankingEngine::MLRankingEngine() {
    std::cout << "ML Ranking Engine initialized" << std::endl;
    
    // TODO: Load ML model weights from file or training data
    // For now, using heuristic-based scoring
}

std::vector<RankedTimelineItem> MLRankingEngine::ScoreNotes(
    const std::vector<::sonet::note::Note>& notes,
    const std::string& user_id,
    const UserEngagementProfile& profile,
    const TimelineConfig& config
) {
    std::vector<RankedTimelineItem> ranked_items;
    ranked_items.reserve(notes.size());
    
    std::cout << "Scoring " << notes.size() << " notes for user " << user_id << std::endl;

    for (const auto& note : notes) {
        RankedTimelineItem item;
        item.note = note;
        item.injected_at = std::chrono::system_clock::now();
        
        // Determine content source
        if (profile.following_ids.count(note.author_id())) {
            item.source = ::sonet::timeline::CONTENT_SOURCE_FOLLOWING;
            item.injection_reason = "following";
        } else {
            item.source = ::sonet::timeline::CONTENT_SOURCE_RECOMMENDED;
            item.injection_reason = "recommended";
        }

        // Calculate individual signal scores
        ::sonet::timeline::RankingSignals signals;
        
        double author_affinity = CalculateAuthorAffinity(user_id, note.author_id(), profile);
        signals.set_author_affinity_score(author_affinity);
        
        double content_quality = CalculateContentQuality(note, profile);
        signals.set_content_quality_score(content_quality);
        
        double engagement_velocity = CalculateEngagementVelocity(note);
        signals.set_engagement_velocity_score(engagement_velocity);
        
        double recency = CalculateRecencyScore(note);
        signals.set_recency_score(recency);
        
        double personalization = CalculatePersonalizationScore(note, profile);
        signals.set_personalization_score(personalization);

        // Calculate final weighted score
        double final_score = 
            (author_affinity * config.author_affinity_weight) +
            (content_quality * config.content_quality_weight) +
            (engagement_velocity * config.engagement_weight) +
            (recency * config.recency_weight) +
            (personalization * 0.1); // Additional personalization boost

        item.final_score = final_score;
        item.signals = signals;
        
        ranked_items.push_back(item);
    }

    // Apply diversity boosts to prevent too much similar content
    ApplyDiversityBoosts(ranked_items, config.diversity_weight);

    // TikTok-style repetition control and novelty boosts
    {
        // Penalize author repetition beyond a window and boost novel creators/topics
        std::unordered_map<std::string, int> authored_counts_in_slate;
        std::unordered_map<std::string, int> hashtag_frequency_over_slate;

        // Precompute hashtag frequencies across slate
        for (const auto& item : ranked_items) {
            auto hashtags = ExtractHashtags(item.note.content());
            for (const auto& h : hashtags) {
                hashtag_frequency_over_slate[h]++;
            }
        }

        const int author_soft_cap = 2; // allow at most 2 per slate without penalty
        const double author_penalty_step = 0.06; // increasing penalty per extra
        const double back_to_back_penalty = 0.05; // avoid immediate repeats
        const double novelty_boost = 0.04; // encourage unseen authors/rare topics

        std::string last_author;
        for (size_t i = 0; i < ranked_items.size(); ++i) {
            auto& item = ranked_items[i];
            const std::string author = item.note.author_id();
            int& count = authored_counts_in_slate[author];
            count++;

            // Author repetition penalty beyond soft cap
            if (count > author_soft_cap) {
                item.final_score -= (count - author_soft_cap) * author_penalty_step;
            }

            // Back-to-back same-author penalty
            if (!last_author.empty() && last_author == author) {
                item.final_score -= back_to_back_penalty;
            }
            last_author = author;

            // Novelty boost for authors with low presence in slate
            if (count == 1) {
                item.final_score += novelty_boost;
            }

            // Topic novelty: boost rare hashtags; penalize over-frequent ones
            auto hashtags = ExtractHashtags(item.note.content());
            for (const auto& h : hashtags) {
                int freq = hashtag_frequency_over_slate[h];
                if (freq == 1) {
                    item.final_score += 0.02; // unique topic
                } else if (freq > 4) {
                    item.final_score -= 0.01; // topic saturation
                }
            }

            // Ensure score stays non-negative
            if (item.final_score < 0.0) item.final_score = 0.0;
        }
    }

    // Hybrid-specific tweaks: freshness micro-boost and source diversity
    if (config.algorithm == ::sonet::timeline::TIMELINE_ALGORITHM_HYBRID) {
        auto now = std::chrono::system_clock::now();
        for (auto& item : ranked_items) {
            // Micro-boost for very recent items (< 30 minutes)
            auto created_time = FromProtoTimestamp(item.note.created_at());
            auto age_minutes = std::chrono::duration_cast<std::chrono::minutes>(now - created_time).count();
            if (age_minutes >= 0 && age_minutes <= 30) {
                item.final_score += 0.02;
            }
            // Prefer some proportion of non-following content to improve discovery
            if (item.source == ::sonet::timeline::CONTENT_SOURCE_RECOMMENDED ||
                item.source == ::sonet::timeline::CONTENT_SOURCE_TRENDING ||
                item.source == ::sonet::timeline::CONTENT_SOURCE_LISTS) {
                item.final_score += 0.01;
            }
        }
    }

    std::cout << "Scoring complete. Average score: " 
              << (ranked_items.empty() ? 0.0 : 
                  std::accumulate(ranked_items.begin(), ranked_items.end(), 0.0,
                      [](double sum, const RankedTimelineItem& item) { return sum + item.final_score; }) / ranked_items.size())
              << std::endl;

    return ranked_items;
}

double MLRankingEngine::CalculateAuthorAffinity(
    const std::string& user_id,
    const std::string& author_id,
    const UserEngagementProfile& profile
) {
    std::lock_guard<std::mutex> lock(affinity_mutex_);
    
    // Base affinity for following relationship
    double base_affinity = profile.following_ids.count(author_id) ? 0.8 : 0.1;
    
    // Check historical engagement with this author
    auto user_it = user_author_affinity_.find(user_id);
    if (user_it != user_author_affinity_.end()) {
        auto author_it = user_it->second.find(author_id);
        if (author_it != user_it->second.end()) {
            base_affinity = std::max(base_affinity, author_it->second);
        }
    }
    
    // Global author reputation score
    auto global_it = global_author_scores_.find(author_id);
    if (global_it != global_author_scores_.end()) {
        base_affinity += global_it->second * 0.2; // 20% weight on global reputation
    }
    
    return std::min(1.0, base_affinity);
}

double MLRankingEngine::CalculateContentQuality(
    const ::sonet::note::Note& note,
    const UserEngagementProfile& profile
) {
    double quality_score = 0.5; // Base quality
    
    // Text length scoring (optimal range: 50-280 characters)
    int text_length = note.content().length();
    if (text_length >= 50 && text_length <= 280) {
        quality_score += quality_text_length_weight_;
    } else if (text_length < 10) {
        quality_score -= 0.2; // Penalty for very short content
    }
    
    // Media content boost
    if (note.has_media() && note.media().items_size() > 0) {
        quality_score += quality_media_boost_;
    }
    
    // Link penalty (might be promotional)
    if (ContainsURL(note.content())) {
        quality_score += quality_link_penalty_;
    }
    
    // Hashtag analysis
    auto hashtags = ExtractHashtags(note.content());
    if (!hashtags.empty() && hashtags.size() <= 5) { // Reasonable number of hashtags
        quality_score += quality_hashtag_boost_;
        
        // Boost for hashtags user has engaged with
        std::lock_guard<std::mutex> lock(affinity_mutex_);
        auto user_hashtags = user_engaged_hashtags_.find(profile.user_id);
        if (user_hashtags != user_engaged_hashtags_.end()) {
            for (const auto& hashtag : hashtags) {
                if (user_hashtags->second.count(hashtag)) {
                    quality_score += 0.05; // Small boost per relevant hashtag
                }
            }
        }
    } else if (hashtags.size() > 10) {
        quality_score -= 0.1; // Penalty for hashtag spam
    }
    
    // Mention analysis
    auto mentions = ExtractMentions(note.content());
    if (!mentions.empty() && mentions.size() <= 3) {
        quality_score += quality_mention_boost_;
    }
    
    // Engagement quality (likes vs controversial ratio)
    if (note.has_metrics()) {
        double engagement_rate = CalculateEngagementRate(note);
        quality_score += std::min(0.3, engagement_rate * 2.0); // Cap at 0.3 boost
    }
    
    return std::max(0.0, std::min(1.0, quality_score));
}

double MLRankingEngine::CalculateEngagementVelocity(const ::sonet::note::Note& note) {
    if (!note.has_metrics()) return 0.0;
    
    auto created_time = FromProtoTimestamp(note.created_at());
    auto now = std::chrono::system_clock::now();
    auto age_hours = std::chrono::duration_cast<std::chrono::hours>(now - created_time).count();
    
    if (age_hours <= 0) return 0.0;
    
    const auto& metrics = note.metrics();
    double total_engagements = metrics.likes() + metrics.renotes() + metrics.replies() + metrics.quotes();
    
    // Engagements per hour
    double velocity = total_engagements / age_hours;
    
    // Normalize to 0-1 range (assuming 10 engagements/hour is very high)
    return std::min(1.0, velocity / 10.0);
}

double MLRankingEngine::CalculateRecencyScore(const ::sonet::note::Note& note, double half_life_hours) {
    auto created_time = FromProtoTimestamp(note.created_at());
    auto now = std::chrono::system_clock::now();
    auto age_hours = std::chrono::duration_cast<std::chrono::hours>(now - created_time).count();
    
    // Exponential decay with specified half-life
    return std::exp(-age_hours * std::log(2.0) / half_life_hours);
}

double MLRankingEngine::CalculatePersonalizationScore(
    const ::sonet::note::Note& note,
    const UserEngagementProfile& profile
) {
    double personalization = 0.0;
    
    // Time-based personalization (user's active hours)
    auto created_time = FromProtoTimestamp(note.created_at());
    auto created_hour = std::chrono::duration_cast<std::chrono::hours>(
        created_time.time_since_epoch()).count() % 24;
    
    // Boost content noteed during user's typical active hours
    // For simplicity, assume users are most active 9 AM - 11 PM
    if (created_hour >= 9 && created_hour <= 23) {
        personalization += 0.1;
    }
    
    // Interest-based scoring from hashtags
    auto hashtags = ExtractHashtags(note.content());
    std::lock_guard<std::mutex> lock(affinity_mutex_);
    auto user_hashtags = user_engaged_hashtags_.find(profile.user_id);
    if (user_hashtags != user_engaged_hashtags_.end()) {
        for (const auto& hashtag : hashtags) {
            if (user_hashtags->second.count(hashtag)) {
                personalization += 0.05; // Boost per matching interest
            }
        }
    }
    
    // Language and content type preferences could be added here
    
    return std::min(1.0, personalization);
}

void MLRankingEngine::ApplyDiversityBoosts(
    std::vector<RankedTimelineItem>& items,
    double diversity_factor
) {
    if (items.size() <= 1) return;
    
    std::unordered_map<std::string, int> author_count;
    std::unordered_set<std::string> seen_hashtags;
    
    // First pass: count authors and collect hashtags
    for (const auto& item : items) {
        author_count[item.note.author_id()]++;
        
        auto hashtags = ExtractHashtags(item.note.content());
        for (const auto& hashtag : hashtags) {
            seen_hashtags.insert(hashtag);
        }
    }
    
    // Second pass: apply diversity penalties/boosts
    for (auto& item : items) {
        double diversity_adjustment = 0.0;
        
        // Penalize overrepresented authors
        int author_notes = author_count[item.note.author_id()];
        if (author_notes > 3) {
            diversity_adjustment -= (author_notes - 3) * 0.05; // Penalty for author spam
        }
        
        // Boost unique content (novel hashtags)
        auto hashtags = ExtractHashtags(item.note.content());
        for (const auto& hashtag : hashtags) {
            // Check if this hashtag is rare in the current batch
            int hashtag_frequency = 0;
            for (const auto& other_item : items) {
                auto other_hashtags = ExtractHashtags(other_item.note.content());
                if (std::find(other_hashtags.begin(), other_hashtags.end(), hashtag) != other_hashtags.end()) {
                    hashtag_frequency++;
                }
            }
            
            if (hashtag_frequency == 1) { // Unique hashtag
                diversity_adjustment += 0.02;
            }
        }
        
        item.final_score += diversity_adjustment * diversity_factor;
        item.final_score = std::max(0.0, item.final_score); // Ensure non-negative
    }
}

void MLRankingEngine::UpdateUserEngagement(
    const std::string& user_id,
    const std::string& note_id,
    const std::string& action,
    double duration_seconds
) {
    std::lock_guard<std::mutex> lock(affinity_mutex_);
    
    // TODO: In production, this would update ML model features
    // For now, track simple engagement patterns
    
    last_engagement_time_[user_id] = std::chrono::system_clock::now();
    
    std::cout << "Updated engagement: User " << user_id << " " << action << " note " << note_id 
              << " (duration: " << duration_seconds << "s)" << std::endl;
}

void MLRankingEngine::TrainOnEngagementData(const std::vector<EngagementEvent>& events) {
    std::lock_guard<std::mutex> lock(affinity_mutex_);
    
    std::cout << "Training on " << events.size() << " engagement events" << std::endl;
    
    // TODO: Implement actual ML training
    // For now, update affinity scores based on engagement patterns
    
    for (const auto& event : events) {
        // Update author affinity
        double& affinity = user_author_affinity_[event.user_id][event.author_id];
        
        if (event.action == "like") {
            affinity += 0.05;
        } else if (event.action == "renote") {
            affinity += 0.1;
        } else if (event.action == "reply") {
            affinity += 0.15;
        } else if (event.action == "follow") {
            affinity += 0.3;
        }
        
        affinity = std::min(1.0, affinity);
        
        // Update global author scores
        global_author_scores_[event.author_id] += 0.01;
        global_author_scores_[event.author_id] = std::min(1.0, global_author_scores_[event.author_id]);
    }
}

} // namespace sonet::timeline
