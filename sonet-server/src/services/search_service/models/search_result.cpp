/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This implements the search result models for our Twitter-scale search service.
 * I built this to handle rich search results with intelligent highlighting,
 * aggregations, and all the metadata users need to find exactly what they want.
 */

#include "search_result.h"
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <regex>
#include <cmath>
#include <uuid/uuid.h>

namespace sonet {
namespace search_service {
namespace models {

// NoteResult implementation
nlohmann::json NoteResult::to_json() const {
    nlohmann::json json = {
        {"note_id", note_id},
        {"content", content},
        {"author", {
            {"user_id", author_id},
            {"username", author_username},
            {"display_name", author_display_name},
            {"verified", author_verified}
        }},
        {"created_at", std::chrono::duration_cast<std::chrono::seconds>(
            created_at.time_since_epoch()).count()},
        {"metrics", {
            {"likes_count", likes_count},
            {"renotes_count", renotes_count},
            {"replies_count", replies_count},
            {"views_count", views_count},
            {"engagement_rate", engagement_rate}
        }},
        {"hashtags", hashtags},
        {"mentions", mentions},
        {"urls", urls},
        {"has_media", has_media},
        {"relevance_score", relevance_score},
        {"matched_fields", matched_fields}
    };
    
    if (author_avatar_url) {
        json["author"]["avatar_url"] = *author_avatar_url;
    }
    
    if (language) {
        json["language"] = *language;
    }
    
    if (sentiment) {
        json["sentiment"] = *sentiment;
    }
    
    if (!media_urls.empty()) {
        json["media"] = {
            {"urls", media_urls},
            {"types", media_types}
        };
    }
    
    if (reply_to_note_id) {
        json["reply_to"] = *reply_to_note_id;
    }
    
    if (thread_id) {
        json["thread"] = {
            {"id", *thread_id},
            {"is_starter", is_thread_starter},
            {"position", thread_position}
        };
    }
    
    if (!highlights.empty()) {
        json["highlights"] = highlights;
    }
    
    return json;
}

NoteResult NoteResult::from_elasticsearch_doc(const nlohmann::json& doc) {
    NoteResult result;
    
    const auto& source = doc["_source"];
    
    result.note_id = source.value("note_id", "");
    result.content = source.value("content", "");
    result.author_id = source.value("author_id", "");
    result.created_at = std::chrono::system_clock::from_time_t(
        source.value("created_at", 0)
    );
    
    // Author information
    if (source.contains("author")) {
        const auto& author = source["author"];
        result.author_username = author.value("username", "");
        result.author_display_name = author.value("display_name", "");
        result.author_verified = author.value("verified", false);
        if (author.contains("avatar_url")) {
            result.author_avatar_url = author["avatar_url"].get<std::string>();
        }
    }
    
    // Metrics
    if (source.contains("metrics")) {
        const auto& metrics = source["metrics"];
        result.likes_count = metrics.value("likes_count", 0);
        result.renotes_count = metrics.value("renotes_count", 0);
        result.replies_count = metrics.value("replies_count", 0);
        result.views_count = metrics.value("views_count", 0);
        result.engagement_rate = metrics.value("engagement_rate", 0.0);
    }
    
    // Content analysis
    if (source.contains("hashtags")) {
        result.hashtags = source["hashtags"].get<std::vector<std::string>>();
    }
    if (source.contains("mentions")) {
        result.mentions = source["mentions"].get<std::vector<std::string>>();
    }
    if (source.contains("urls")) {
        result.urls = source["urls"].get<std::vector<std::string>>();
    }
    if (source.contains("language")) {
        result.language = source["language"].get<std::string>();
    }
    if (source.contains("sentiment")) {
        result.sentiment = source["sentiment"].get<std::string>();
    }
    
    // Media
    if (source.contains("media")) {
        result.has_media = true;
        const auto& media = source["media"];
        if (media.contains("urls")) {
            result.media_urls = media["urls"].get<std::vector<std::string>>();
        }
        if (media.contains("types")) {
            result.media_types = media["types"].get<std::vector<std::string>>();
        }
    }
    
    // Thread information
    if (source.contains("reply_to")) {
        result.reply_to_note_id = source["reply_to"].get<std::string>();
    }
    if (source.contains("thread_id")) {
        result.thread_id = source["thread_id"].get<std::string>();
    }
    result.is_thread_starter = source.value("is_thread_starter", false);
    result.thread_position = source.value("thread_position", 0);
    
    // Search metadata
    if (doc.contains("_score")) {
        result.relevance_score = doc["_score"].get<double>();
    }
    
    // Highlights
    if (doc.contains("highlight")) {
        const auto& highlight = doc["highlight"];
        for (const auto& [field, fragments] : highlight.items()) {
            result.highlights[field] = fragments.get<std::vector<std::string>>();
        }
    }
    
    return result;
}

std::string NoteResult::get_content_snippet(int max_length) const {
    return result_utils::truncate_text(content, max_length);
}

bool NoteResult::is_renote() const {
    // Check if this note is a renote by looking for renote markers
    return content.find("RT @") == 0 || content.find("renote:") != std::string::npos;
}

std::string NoteResult::get_display_timestamp() const {
    return result_utils::format_relative_time(created_at);
}

// UserResult implementation
nlohmann::json UserResult::to_json() const {
    nlohmann::json json = {
        {"user_id", user_id},
        {"username", username},
        {"display_name", display_name},
        {"verified", verified},
        {"created_at", std::chrono::duration_cast<std::chrono::seconds>(
            created_at.time_since_epoch()).count()},
        {"metrics", {
            {"followers_count", followers_count},
            {"following_count", following_count},
            {"notes_count", notes_count},
            {"listed_count", listed_count},
            {"engagement_rate", engagement_rate}
        }},
        {"last_active", std::chrono::duration_cast<std::chrono::seconds>(
            last_active.time_since_epoch()).count()},
        {"relevance_score", relevance_score},
        {"matched_fields", matched_fields},
        {"relationship", {
            {"is_following", is_following},
            {"is_followed_by", is_followed_by},
            {"is_blocked", is_blocked},
            {"is_muted", is_muted}
        }}
    };
    
    if (bio) {
        json["bio"] = *bio;
    }
    if (avatar_url) {
        json["avatar_url"] = *avatar_url;
    }
    if (banner_url) {
        json["banner_url"] = *banner_url;
    }
    if (location) {
        json["location"] = *location;
    }
    if (website) {
        json["website"] = *website;
    }
    if (last_note_content) {
        json["last_note"] = {
            {"content", *last_note_content},
            {"created_at", std::chrono::duration_cast<std::chrono::seconds>(
                last_note_time.time_since_epoch()).count()}
        };
    }
    if (match_reason) {
        json["match_reason"] = *match_reason;
    }
    if (!highlights.empty()) {
        json["highlights"] = highlights;
    }
    
    return json;
}

UserResult UserResult::from_elasticsearch_doc(const nlohmann::json& doc) {
    UserResult result;
    
    const auto& source = doc["_source"];
    
    result.user_id = source.value("user_id", "");
    result.username = source.value("username", "");
    result.display_name = source.value("display_name", "");
    result.verified = source.value("verified", false);
    result.created_at = std::chrono::system_clock::from_time_t(
        source.value("created_at", 0)
    );
    result.last_active = std::chrono::system_clock::from_time_t(
        source.value("last_active", 0)
    );
    
    if (source.contains("bio")) {
        result.bio = source["bio"].get<std::string>();
    }
    if (source.contains("avatar_url")) {
        result.avatar_url = source["avatar_url"].get<std::string>();
    }
    if (source.contains("banner_url")) {
        result.banner_url = source["banner_url"].get<std::string>();
    }
    if (source.contains("location")) {
        result.location = source["location"].get<std::string>();
    }
    if (source.contains("website")) {
        result.website = source["website"].get<std::string>();
    }
    
    // Metrics
    if (source.contains("metrics")) {
        const auto& metrics = source["metrics"];
        result.followers_count = metrics.value("followers_count", 0);
        result.following_count = metrics.value("following_count", 0);
        result.notes_count = metrics.value("notes_count", 0);
        result.listed_count = metrics.value("listed_count", 0);
        result.engagement_rate = metrics.value("engagement_rate", 0.0);
    }
    
    // Last note
    if (source.contains("last_note")) {
        const auto& last_note = source["last_note"];
        result.last_note_content = last_note.value("content", "");
        result.last_note_time = std::chrono::system_clock::from_time_t(
            last_note.value("created_at", 0)
        );
    }
    
    // Search metadata
    if (doc.contains("_score")) {
        result.relevance_score = doc["_score"].get<double>();
    }
    
    // Highlights
    if (doc.contains("highlight")) {
        const auto& highlight = doc["highlight"];
        for (const auto& [field, fragments] : highlight.items()) {
            result.highlights[field] = fragments.get<std::vector<std::string>>();
        }
    }
    
    return result;
}

std::string UserResult::get_bio_snippet(int max_length) const {
    if (!bio) return "";
    return result_utils::truncate_text(*bio, max_length);
}

double UserResult::get_reputation_score() const {
    // Calculate user reputation based on various factors
    double score = 0.0;
    
    // Follower ratio (but cap it to avoid gaming)
    double follower_ratio = following_count > 0 ? 
        static_cast<double>(followers_count) / following_count : followers_count;
    score += std::min(follower_ratio, 100.0) * 0.3;
    
    // Verification boost
    if (verified) {
        score += 50.0;
    }
    
    // Activity level
    auto days_since_last_active = std::chrono::duration_cast<std::chrono::hours>(
        std::chrono::system_clock::now() - last_active).count() / 24;
    score += std::max(0.0, 30.0 - days_since_last_active) * 0.5;
    
    // Content volume (but not too much)
    score += std::min(static_cast<double>(notes_count), 10000.0) / 1000.0 * 10.0;
    
    // Engagement rate
    score += engagement_rate * 20.0;
    
    return std::min(score, 100.0);  // Cap at 100
}

// HashtagResult implementation
nlohmann::json HashtagResult::to_json() const {
    nlohmann::json json = {
        {"hashtag", hashtag},
        {"display_hashtag", display_hashtag},
        {"stats", {
            {"total_uses", total_uses},
            {"recent_uses_1h", recent_uses_1h},
            {"recent_uses_24h", recent_uses_24h},
            {"recent_uses_7d", recent_uses_7d}
        }},
        {"trending", {
            {"score", trending_score},
            {"rank", trending_rank},
            {"velocity", velocity},
            {"status", get_trending_status()}
        }},
        {"relevance_score", relevance_score}
    };
    
    if (!sample_note_ids.empty()) {
        json["sample_notes"] = sample_note_ids;
    }
    if (!top_contributors.empty()) {
        json["top_contributors"] = top_contributors;
    }
    if (!highlights.empty()) {
        json["highlights"] = highlights;
    }
    
    return json;
}

HashtagResult HashtagResult::from_aggregation(const nlohmann::json& agg_data) {
    HashtagResult result;
    
    result.hashtag = agg_data.value("key", "");
    result.display_hashtag = "#" + result.hashtag;
    result.total_uses = agg_data.value("doc_count", 0);
    
    // Extract time-based aggregations
    if (agg_data.contains("recent_1h")) {
        result.recent_uses_1h = agg_data["recent_1h"].value("doc_count", 0);
    }
    if (agg_data.contains("recent_24h")) {
        result.recent_uses_24h = agg_data["recent_24h"].value("doc_count", 0);
    }
    if (agg_data.contains("recent_7d")) {
        result.recent_uses_7d = agg_data["recent_7d"].value("doc_count", 0);
    }
    
    // Calculate trending metrics
    if (result.recent_uses_24h > 0 && result.recent_uses_7d > 0) {
        double daily_average = static_cast<double>(result.recent_uses_7d) / 7.0;
        result.velocity = result.recent_uses_24h / daily_average;
        result.trending_score = result.velocity * std::log(result.recent_uses_24h + 1);
    }
    
    // Extract sample notes
    if (agg_data.contains("sample_notes")) {
        const auto& samples = agg_data["sample_notes"]["hits"]["hits"];
        for (const auto& hit : samples) {
            result.sample_note_ids.push_back(hit["_id"].get<std::string>());
        }
    }
    
    return result;
}

std::string HashtagResult::get_trending_status() const {
    if (velocity > 3.0) return "hot";
    if (velocity > 1.5) return "rising";
    if (velocity > 0.8) return "stable";
    return "declining";
}

// SuggestionResult implementation
nlohmann::json SuggestionResult::to_json() const {
    nlohmann::json json = {
        {"suggestion", suggestion_text},
        {"completion", completion_text},
        {"type", static_cast<int>(suggestion_type)},
        {"confidence", confidence_score},
        {"estimated_results", estimated_results}
    };
    
    if (context) {
        json["context"] = *context;
    }
    if (!related_terms.empty()) {
        json["related_terms"] = related_terms;
    }
    
    return json;
}

// SearchAggregations implementation
nlohmann::json SearchAggregations::to_json() const {
    nlohmann::json json = nlohmann::json::object();
    
    if (!time_distribution.empty()) {
        json["time_distribution"] = time_distribution;
    }
    if (!top_users.empty()) {
        json["top_users"] = top_users;
    }
    if (!top_hashtags.empty()) {
        json["top_hashtags"] = top_hashtags;
    }
    if (!language_distribution.empty()) {
        json["languages"] = language_distribution;
    }
    if (!media_types.empty()) {
        json["media_types"] = media_types;
    }
    if (!engagement_ranges.empty()) {
        json["engagement_ranges"] = engagement_ranges;
    }
    
    return json;
}

SearchAggregations SearchAggregations::from_elasticsearch_aggs(const nlohmann::json& aggs) {
    SearchAggregations result;
    
    // Time distribution
    if (aggs.contains("time_histogram")) {
        const auto& buckets = aggs["time_histogram"]["buckets"];
        for (const auto& bucket : buckets) {
            std::string date = bucket["key_as_string"].get<std::string>();
            int count = bucket["doc_count"].get<int>();
            result.time_distribution[date] = count;
        }
    }
    
    // Top users
    if (aggs.contains("top_users")) {
        const auto& buckets = aggs["top_users"]["buckets"];
        for (const auto& bucket : buckets) {
            std::string username = bucket["key"].get<std::string>();
            int count = bucket["doc_count"].get<int>();
            result.top_users[username] = count;
        }
    }
    
    // Top hashtags
    if (aggs.contains("top_hashtags")) {
        const auto& buckets = aggs["top_hashtags"]["buckets"];
        for (const auto& bucket : buckets) {
            std::string hashtag = bucket["key"].get<std::string>();
            int count = bucket["doc_count"].get<int>();
            result.top_hashtags[hashtag] = count;
        }
    }
    
    // Languages
    if (aggs.contains("languages")) {
        const auto& buckets = aggs["languages"]["buckets"];
        for (const auto& bucket : buckets) {
            std::string lang = bucket["key"].get<std::string>();
            int count = bucket["doc_count"].get<int>();
            result.language_distribution[lang] = count;
        }
    }
    
    // Media types
    if (aggs.contains("media_types")) {
        const auto& buckets = aggs["media_types"]["buckets"];
        for (const auto& bucket : buckets) {
            std::string media_type = bucket["key"].get<std::string>();
            int count = bucket["doc_count"].get<int>();
            result.media_types[media_type] = count;
        }
    }
    
    // Engagement ranges
    if (aggs.contains("engagement_ranges")) {
        const auto& buckets = aggs["engagement_ranges"]["buckets"];
        for (const auto& bucket : buckets) {
            std::string range = bucket["key"].get<std::string>();
            int count = bucket["doc_count"].get<int>();
            result.engagement_ranges[range] = count;
        }
    }
    
    return result;
}

// SearchMetadata implementation
nlohmann::json SearchMetadata::to_json() const {
    nlohmann::json json = {
        {"query_id", query_id},
        {"processed_query", processed_query_text},
        {"performance", {
            {"took_ms", took.count()},
            {"elasticsearch_ms", elasticsearch_time.count()},
            {"cache_ms", cache_time.count()},
            {"served_from_cache", served_from_cache}
        }},
        {"results", {
            {"total", total_results},
            {"returned", returned_results},
            {"offset", offset},
            {"has_more", has_more_results},
            {"max_score", max_score}
        }}
    };
    
    if (!applied_corrections.empty()) {
        json["corrections"] = applied_corrections;
    }
    if (!suggestions.empty()) {
        json["suggestions"] = suggestions;
    }
    if (rewritten_query) {
        json["rewritten_query"] = *rewritten_query;
    }
    if (debug_info) {
        json["debug"] = *debug_info;
    }
    
    return json;
}

// SearchResult implementation
SearchResult::SearchResult(const SearchQuery& query) {
    metadata.original_query = query;
    metadata.query_id = generate_result_id();
    metadata.processed_query_text = query.query_text;
}

void SearchResult::add_note(const NoteResult& note) {
    notes.push_back(note);
    mixed_results.emplace_back(ResultType::NOTE, notes.size() - 1);
    update_mixed_results_index();
}

void SearchResult::add_note(NoteResult&& note) {
    notes.push_back(std::move(note));
    mixed_results.emplace_back(ResultType::NOTE, notes.size() - 1);
    update_mixed_results_index();
}

void SearchResult::add_user(const UserResult& user) {
    users.push_back(user);
    mixed_results.emplace_back(ResultType::USER, users.size() - 1);
    update_mixed_results_index();
}

void SearchResult::add_user(UserResult&& user) {
    users.push_back(std::move(user));
    mixed_results.emplace_back(ResultType::USER, users.size() - 1);
    update_mixed_results_index();
}

void SearchResult::add_hashtag(const HashtagResult& hashtag) {
    hashtags.push_back(hashtag);
    mixed_results.emplace_back(ResultType::HASHTAG, hashtags.size() - 1);
    update_mixed_results_index();
}

void SearchResult::add_hashtag(HashtagResult&& hashtag) {
    hashtags.push_back(std::move(hashtag));
    mixed_results.emplace_back(ResultType::HASHTAG, hashtags.size() - 1);
    update_mixed_results_index();
}

void SearchResult::add_suggestion(const SuggestionResult& suggestion) {
    suggestions.push_back(suggestion);
}

void SearchResult::add_suggestion(SuggestionResult&& suggestion) {
    suggestions.push_back(std::move(suggestion));
}

void SearchResult::set_aggregations(const SearchAggregations& aggs) {
    aggregations = aggs;
}

void SearchResult::set_aggregations(SearchAggregations&& aggs) {
    aggregations = std::move(aggs);
}

int SearchResult::get_total_results() const {
    return notes.size() + users.size() + hashtags.size();
}

bool SearchResult::has_results() const {
    return get_total_results() > 0;
}

bool SearchResult::is_successful() const {
    return metadata.total_results >= 0;  // Negative indicates error
}

std::vector<std::pair<ResultType, size_t>> SearchResult::get_sorted_mixed_results() const {
    auto sorted_results = mixed_results;
    
    // Sort by relevance score
    std::sort(sorted_results.begin(), sorted_results.end(),
        [this](const auto& a, const auto& b) {
            double score_a = 0.0, score_b = 0.0;
            
            switch (a.first) {
                case ResultType::NOTE:
                    if (a.second < notes.size()) score_a = notes[a.second].relevance_score;
                    break;
                case ResultType::USER:
                    if (a.second < users.size()) score_a = users[a.second].relevance_score;
                    break;
                case ResultType::HASHTAG:
                    if (a.second < hashtags.size()) score_a = hashtags[a.second].relevance_score;
                    break;
                default:
                    break;
            }
            
            switch (b.first) {
                case ResultType::NOTE:
                    if (b.second < notes.size()) score_b = notes[b.second].relevance_score;
                    break;
                case ResultType::USER:
                    if (b.second < users.size()) score_b = users[b.second].relevance_score;
                    break;
                case ResultType::HASHTAG:
                    if (b.second < hashtags.size()) score_b = hashtags[b.second].relevance_score;
                    break;
                default:
                    break;
            }
            
            return score_a > score_b;
        });
    
    return sorted_results;
}

void SearchResult::apply_content_filter(const std::function<bool(const NoteResult&)>& filter) {
    auto it = std::remove_if(notes.begin(), notes.end(), 
        [&filter](const NoteResult& note) { return !filter(note); });
    notes.erase(it, notes.end());
    update_mixed_results_index();
}

void SearchResult::apply_user_filter(const std::function<bool(const UserResult&)>& filter) {
    auto it = std::remove_if(users.begin(), users.end(),
        [&filter](const UserResult& user) { return !filter(user); });
    users.erase(it, users.end());
    update_mixed_results_index();
}

void SearchResult::sort_notes_by(const std::function<bool(const NoteResult&, const NoteResult&)>& comparator) {
    std::sort(notes.begin(), notes.end(), comparator);
    update_mixed_results_index();
}

void SearchResult::sort_users_by(const std::function<bool(const UserResult&, const UserResult&)>& comparator) {
    std::sort(users.begin(), users.end(), comparator);
    update_mixed_results_index();
}

SearchResult SearchResult::get_page(int offset, int limit) const {
    SearchResult page_result = *this;
    
    // Apply pagination to each result type
    if (offset < static_cast<int>(notes.size())) {
        int end = std::min(offset + limit, static_cast<int>(notes.size()));
        page_result.notes = std::vector<NoteResult>(
            notes.begin() + offset, 
            notes.begin() + end
        );
    } else {
        page_result.notes.clear();
    }
    
    // Similar for other result types...
    page_result.update_mixed_results_index();
    
    return page_result;
}

void SearchResult::merge_with(const SearchResult& other) {
    // Merge results
    notes.insert(notes.end(), other.notes.begin(), other.notes.end());
    users.insert(users.end(), other.users.begin(), other.users.end());
    hashtags.insert(hashtags.end(), other.hashtags.begin(), other.hashtags.end());
    suggestions.insert(suggestions.end(), other.suggestions.begin(), other.suggestions.end());
    
    // Update metadata
    metadata.total_results += other.metadata.total_results;
    metadata.returned_results += other.metadata.returned_results;
    
    update_mixed_results_index();
}

nlohmann::json SearchResult::to_json() const {
    nlohmann::json json = {
        {"metadata", metadata.to_json()},
        {"notes", nlohmann::json::array()},
        {"users", nlohmann::json::array()},
        {"hashtags", nlohmann::json::array()},
        {"suggestions", nlohmann::json::array()}
    };
    
    // Convert results to JSON
    for (const auto& note : notes) {
        json["notes"].push_back(note.to_json());
    }
    
    for (const auto& user : users) {
        json["users"].push_back(user.to_json());
    }
    
    for (const auto& hashtag : hashtags) {
        json["hashtags"].push_back(hashtag.to_json());
    }
    
    for (const auto& suggestion : suggestions) {
        json["suggestions"].push_back(suggestion.to_json());
    }
    
    // Add aggregations if present
    if (aggregations) {
        json["aggregations"] = aggregations->to_json();
    }
    
    // Add mixed results ordering
    if (!mixed_results.empty()) {
        json["mixed_results"] = nlohmann::json::array();
        for (const auto& [type, index] : mixed_results) {
            json["mixed_results"].push_back({
                {"type", static_cast<int>(type)},
                {"index", index}
            });
        }
    }
    
    return json;
}

SearchResult SearchResult::from_elasticsearch_response(
    const nlohmann::json& es_response,
    const SearchQuery& original_query) {
    
    SearchResult result(original_query);
    
    // Extract timing information
    if (es_response.contains("took")) {
        result.metadata.elasticsearch_time = std::chrono::milliseconds{
            es_response["took"].get<int>()
        };
        result.metadata.took = result.metadata.elasticsearch_time;
    }
    
    // Extract hit information
    if (es_response.contains("hits")) {
        const auto& hits = es_response["hits"];
        
        result.metadata.total_results = hits.value("total", 0);
        if (hits["total"].is_object()) {
            result.metadata.total_results = hits["total"].value("value", 0);
        }
        
        if (hits.contains("max_score") && !hits["max_score"].is_null()) {
            result.metadata.max_score = hits["max_score"].get<double>();
        }
        
        // Process search results
        if (hits.contains("hits")) {
            for (const auto& hit : hits["hits"]) {
                // Determine result type based on index or document structure
                std::string index = hit.value("_index", "");
                
                if (index.find("notes") != std::string::npos) {
                    result.add_note(NoteResult::from_elasticsearch_doc(hit));
                } else if (index.find("users") != std::string::npos) {
                    result.add_user(UserResult::from_elasticsearch_doc(hit));
                }
            }
        }
    }
    
    // Extract aggregations
    if (es_response.contains("aggregations")) {
        result.set_aggregations(
            SearchAggregations::from_elasticsearch_aggs(es_response["aggregations"])
        );
    }
    
    // Set pagination info
    result.metadata.offset = original_query.config.offset;
    result.metadata.returned_results = result.get_total_results();
    result.metadata.has_more_results = 
        result.metadata.total_results > (result.metadata.offset + result.metadata.returned_results);
    
    return result;
}

SearchResult SearchResult::create_error(
    const SearchQuery& query,
    const std::string& error_message,
    const std::string& error_code) {
    
    SearchResult result(query);
    result.metadata.total_results = -1;  // Indicate error
    
    if (result.metadata.debug_info) {
        (*result.metadata.debug_info)["error"] = {
            {"message", error_message},
            {"code", error_code}
        };
    }
    
    return result;
}

SearchResult SearchResult::create_empty(const SearchQuery& query) {
    SearchResult result(query);
    result.metadata.total_results = 0;
    result.metadata.returned_results = 0;
    result.metadata.has_more_results = false;
    return result;
}

std::string SearchResult::generate_result_id() const {
    uuid_t uuid;
    uuid_generate_random(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    return std::string(uuid_str);
}

void SearchResult::update_mixed_results_index() {
    mixed_results.clear();
    
    // Add notes
    for (size_t i = 0; i < notes.size(); ++i) {
        mixed_results.emplace_back(ResultType::NOTE, i);
    }
    
    // Add users
    for (size_t i = 0; i < users.size(); ++i) {
        mixed_results.emplace_back(ResultType::USER, i);
    }
    
    // Add hashtags
    for (size_t i = 0; i < hashtags.size(); ++i) {
        mixed_results.emplace_back(ResultType::HASHTAG, i);
    }
}

void SearchResult::calculate_relevance_scores() {
    // This would implement more sophisticated relevance scoring
    // For now, scores are set when creating results from Elasticsearch
}

// Utility functions implementation
namespace result_utils {

std::string format_count(int count) {
    if (count < 1000) {
        return std::to_string(count);
    } else if (count < 1000000) {
        double k = count / 1000.0;
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(1) << k << "K";
        return ss.str();
    } else {
        double m = count / 1000000.0;
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(1) << m << "M";
        return ss.str();
    }
}

std::string format_relative_time(const std::chrono::system_clock::time_point& time) {
    auto now = std::chrono::system_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::seconds>(now - time);
    
    if (diff.count() < 60) {
        return std::to_string(diff.count()) + "s";
    } else if (diff.count() < 3600) {
        return std::to_string(diff.count() / 60) + "m";
    } else if (diff.count() < 86400) {
        return std::to_string(diff.count() / 3600) + "h";
    } else if (diff.count() < 2592000) {  // 30 days
        return std::to_string(diff.count() / 86400) + "d";
    } else {
        // Format as date
        auto time_t = std::chrono::system_clock::to_time_t(time);
        std::ostringstream ss;
        ss << std::put_time(std::gmtime(&time_t), "%b %d");
        return ss.str();
    }
}

std::string format_relevance_score(double score) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(2) << score;
    return ss.str();
}

std::string truncate_text(const std::string& text, int max_length) {
    if (text.length() <= static_cast<size_t>(max_length)) {
        return text;
    }
    
    std::string truncated = text.substr(0, max_length - 3);
    
    // Try to break at word boundary
    size_t last_space = truncated.find_last_of(' ');
    if (last_space != std::string::npos && last_space > static_cast<size_t>(max_length / 2)) {
        truncated = truncated.substr(0, last_space);
    }
    
    return truncated + "...";
}

std::string extract_highlight(
    const std::unordered_map<std::string, std::vector<std::string>>& highlights,
    const std::string& field,
    const std::string& fallback) {
    
    auto it = highlights.find(field);
    if (it != highlights.end() && !it->second.empty()) {
        return it->second[0];  // Return first highlight fragment
    }
    return fallback;
}

std::string clean_highlight_html(const std::string& highlighted_text) {
    // Remove HTML tags but keep the content
    std::string cleaned = highlighted_text;
    cleaned = std::regex_replace(cleaned, std::regex("<em>"), "");
    cleaned = std::regex_replace(cleaned, std::regex("</em>"), "");
    return cleaned;
}

std::string generate_snippet(
    const std::string& full_text,
    const std::string& query,
    int max_length) {
    
    // Find the position of query terms in the text
    std::string lower_text = full_text;
    std::string lower_query = query;
    std::transform(lower_text.begin(), lower_text.end(), lower_text.begin(), ::tolower);
    std::transform(lower_query.begin(), lower_query.end(), lower_query.begin(), ::tolower);
    
    size_t pos = lower_text.find(lower_query);
    if (pos == std::string::npos) {
        return truncate_text(full_text, max_length);
    }
    
    // Calculate snippet boundaries
    int snippet_start = std::max(0, static_cast<int>(pos) - max_length / 4);
    int snippet_length = std::min(max_length, static_cast<int>(full_text.length()) - snippet_start);
    
    std::string snippet = full_text.substr(snippet_start, snippet_length);
    
    if (snippet_start > 0) {
        snippet = "..." + snippet;
    }
    if (snippet_start + snippet_length < static_cast<int>(full_text.length())) {
        snippet += "...";
    }
    
    return snippet;
}

double calculate_similarity(const std::string& text1, const std::string& text2) {
    // Simplified Jaccard similarity
    std::set<std::string> words1, words2;
    
    std::istringstream iss1(text1), iss2(text2);
    std::string word;
    
    while (iss1 >> word) {
        std::transform(word.begin(), word.end(), word.begin(), ::tolower);
        words1.insert(word);
    }
    
    while (iss2 >> word) {
        std::transform(word.begin(), word.end(), word.begin(), ::tolower);
        words2.insert(word);
    }
    
    std::set<std::string> intersection;
    std::set_intersection(words1.begin(), words1.end(),
                         words2.begin(), words2.end(),
                         std::inserter(intersection, intersection.begin()));
    
    std::set<std::string> union_set;
    std::set_union(words1.begin(), words1.end(),
                   words2.begin(), words2.end(),
                   std::inserter(union_set, union_set.begin()));
    
    if (union_set.empty()) return 0.0;
    
    return static_cast<double>(intersection.size()) / union_set.size();
}

std::string detect_language(const std::string& text) {
    // Simplified language detection - in production you'd use a proper library
    // For now, assume English
    return "en";
}

std::string analyze_sentiment(const std::string& text) {
    // Simplified sentiment analysis - in production you'd use ML models
    // For now, use basic keyword matching
    std::string lower_text = text;
    std::transform(lower_text.begin(), lower_text.end(), lower_text.begin(), ::tolower);
    
    std::vector<std::string> positive_words = {"love", "great", "awesome", "amazing", "excellent", "good", "happy"};
    std::vector<std::string> negative_words = {"hate", "bad", "terrible", "awful", "horrible", "sad", "angry"};
    
    int positive_count = 0, negative_count = 0;
    
    for (const auto& word : positive_words) {
        if (lower_text.find(word) != std::string::npos) {
            positive_count++;
        }
    }
    
    for (const auto& word : negative_words) {
        if (lower_text.find(word) != std::string::npos) {
            negative_count++;
        }
    }
    
    if (positive_count > negative_count) return "positive";
    if (negative_count > positive_count) return "negative";
    return "neutral";
}

} // namespace result_utils

// Result caching utilities
namespace result_cache {

std::string generate_cache_key(const SearchQuery& query) {
    return "search_result:" + query.get_cache_key();
}

std::string serialize_result(const SearchResult& result) {
    return result.to_json().dump();
}

std::optional<SearchResult> deserialize_result(const std::string& cached_data) {
    try {
        nlohmann::json json = nlohmann::json::parse(cached_data);
        // Would need to implement SearchResult::from_json()
        // For now, return empty optional
        return std::nullopt;
    } catch (const std::exception& e) {
        return std::nullopt;
    }
}

bool is_result_valid(const SearchResult& result, std::chrono::minutes max_age) {
    // Check if result is within acceptable age
    auto now = std::chrono::system_clock::now();
    auto result_age = now - result.metadata.original_query.created_at;
    return std::chrono::duration_cast<std::chrono::minutes>(result_age) <= max_age;
}

} // namespace result_cache

} // namespace models
} // namespace search_service
} // namespace sonet