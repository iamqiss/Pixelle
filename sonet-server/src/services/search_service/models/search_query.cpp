/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * This implements the search query model for our Twitter-scale search service.
 * I built this to handle complex search queries with intelligent parsing,
 * filtering capabilities, and optimized Elasticsearch query generation.
 */

#include "search_query.h"
#include <regex>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <random>
#include <uuid/uuid.h>

namespace sonet {
namespace search_service {
namespace models {

// SearchFilters implementation
nlohmann::json SearchFilters::to_elasticsearch_query() const {
    nlohmann::json query = nlohmann::json::object();
    nlohmann::json bool_query = nlohmann::json::object();
    nlohmann::json must = nlohmann::json::array();
    nlohmann::json filter = nlohmann::json::array();
    nlohmann::json must_not = nlohmann::json::array();
    
    // Time range filters
    if (from_date || to_date) {
        nlohmann::json range_query = nlohmann::json::object();
        nlohmann::json time_range = nlohmann::json::object();
        
        if (from_date) {
            auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                from_date->time_since_epoch()).count();
            time_range["gte"] = timestamp;
        }
        
        if (to_date) {
            auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                to_date->time_since_epoch()).count();
            time_range["lte"] = timestamp;
        }
        
        range_query["range"] = {{"created_at", time_range}};
        filter.push_back(range_query);
    }
    
    // Last hours filter
    if (last_hours) {
        auto cutoff_time = std::chrono::system_clock::now() - *last_hours;
        auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
            cutoff_time.time_since_epoch()).count();
        
        nlohmann::json range_query = {
            {"range", {{"created_at", {{"gte", timestamp}}}}}
        };
        filter.push_back(range_query);
    }
    
    // User filters
    if (from_user) {
        nlohmann::json term_query = {
            {"term", {{"author.username.keyword", *from_user}}}
        };
        filter.push_back(term_query);
    }
    
    // Mentioned users
    for (const auto& user : mentioned_users) {
        nlohmann::json term_query = {
            {"term", {{"mentions.username.keyword", user}}}
        };
        filter.push_back(term_query);
    }
    
    // Exclude users
    for (const auto& user : exclude_users) {
        nlohmann::json term_query = {
            {"term", {{"author.username.keyword", user}}}
        };
        must_not.push_back(term_query);
    }
    
    // Hashtag filters
    for (const auto& hashtag : hashtags) {
        nlohmann::json term_query = {
            {"term", {{"hashtags.keyword", hashtag}}}
        };
        filter.push_back(term_query);
    }
    
    // Exclude hashtags
    for (const auto& hashtag : exclude_hashtags) {
        nlohmann::json term_query = {
            {"term", {{"hashtags.keyword", hashtag}}}
        };
        must_not.push_back(term_query);
    }
    
    // Media filter
    if (has_media) {
        if (*has_media) {
            nlohmann::json exists_query = {
                {"exists", {{"field", "media"}}}
            };
            filter.push_back(exists_query);
        } else {
            nlohmann::json exists_query = {
                {"exists", {{"field", "media"}}}
            };
            must_not.push_back(exists_query);
        }
    }
    
    // Links filter
    if (has_links) {
        if (*has_links) {
            nlohmann::json exists_query = {
                {"exists", {{"field", "links"}}}
            };
            filter.push_back(exists_query);
        } else {
            nlohmann::json exists_query = {
                {"exists", {{"field", "links"}}}
            };
            must_not.push_back(exists_query);
        }
    }
    
    // Verified user filter
    if (is_verified_user && *is_verified_user) {
        nlohmann::json term_query = {
            {"term", {{"author.verified", true}}}
        };
        filter.push_back(term_query);
    }
    
    // Engagement filters
    if (min_likes) {
        nlohmann::json range_query = {
            {"range", {{"metrics.likes_count", {{"gte", *min_likes}}}}}
        };
        filter.push_back(range_query);
    }
    
    if (min_renotes) {
        nlohmann::json range_query = {
            {"range", {{"metrics.renotes_count", {{"gte", *min_renotes}}}}}
        };
        filter.push_back(range_query);
    }
    
    if (min_replies) {
        nlohmann::json range_query = {
            {"range", {{"metrics.replies_count", {{"gte", *min_replies}}}}}
        };
        filter.push_back(range_query);
    }
    
    // Geographic filters
    if (latitude && longitude && radius_km) {
        nlohmann::json geo_query = {
            {"geo_distance", {
                {"distance", std::to_string(*radius_km) + "km"},
                {"location", {
                    {"lat", *latitude},
                    {"lon", *longitude}
                }}
            }}
        };
        filter.push_back(geo_query);
    }
    
    // Language filter
    if (language) {
        nlohmann::json term_query = {
            {"term", {{"language", *language}}}
        };
        filter.push_back(term_query);
    }
    
    // Content type filters
    for (const auto& content_type : content_types) {
        nlohmann::json term_query = {
            {"term", {{"content_type", content_type}}}
        };
        filter.push_back(term_query);
    }
    
    // Build final query
    if (!must.empty()) {
        bool_query["must"] = must;
    }
    if (!filter.empty()) {
        bool_query["filter"] = filter;
    }
    if (!must_not.empty()) {
        bool_query["must_not"] = must_not;
    }
    
    if (!bool_query.empty()) {
        query["bool"] = bool_query;
    }
    
    return query;
}

bool SearchFilters::has_filters() const {
    return from_date.has_value() || to_date.has_value() || last_hours.has_value() ||
           from_user.has_value() || !mentioned_users.empty() || !exclude_users.empty() ||
           !hashtags.empty() || !exclude_hashtags.empty() || has_media.has_value() ||
           has_links.has_value() || is_verified_user.has_value() || min_likes.has_value() ||
           min_renotes.has_value() || min_replies.has_value() || location.has_value() ||
           (latitude.has_value() && longitude.has_value()) || language.has_value() ||
           !content_types.empty();
}

nlohmann::json SearchFilters::to_json() const {
    nlohmann::json json = nlohmann::json::object();
    
    if (from_date) {
        json["from_date"] = std::chrono::duration_cast<std::chrono::seconds>(
            from_date->time_since_epoch()).count();
    }
    if (to_date) {
        json["to_date"] = std::chrono::duration_cast<std::chrono::seconds>(
            to_date->time_since_epoch()).count();
    }
    if (last_hours) {
        json["last_hours"] = last_hours->count();
    }
    if (from_user) {
        json["from_user"] = *from_user;
    }
    if (!mentioned_users.empty()) {
        json["mentioned_users"] = mentioned_users;
    }
    if (!exclude_users.empty()) {
        json["exclude_users"] = exclude_users;
    }
    if (!hashtags.empty()) {
        json["hashtags"] = hashtags;
    }
    if (!exclude_hashtags.empty()) {
        json["exclude_hashtags"] = exclude_hashtags;
    }
    if (has_media) {
        json["has_media"] = *has_media;
    }
    if (has_links) {
        json["has_links"] = *has_links;
    }
    if (is_verified_user) {
        json["is_verified_user"] = *is_verified_user;
    }
    if (min_likes) {
        json["min_likes"] = *min_likes;
    }
    if (min_renotes) {
        json["min_renotes"] = *min_renotes;
    }
    if (min_replies) {
        json["min_replies"] = *min_replies;
    }
    if (location) {
        json["location"] = *location;
    }
    if (latitude) {
        json["latitude"] = *latitude;
    }
    if (longitude) {
        json["longitude"] = *longitude;
    }
    if (radius_km) {
        json["radius_km"] = *radius_km;
    }
    if (language) {
        json["language"] = *language;
    }
    if (!content_types.empty()) {
        json["content_types"] = content_types;
    }
    
    return json;
}

SearchFilters SearchFilters::from_json(const nlohmann::json& json) {
    SearchFilters filters;
    
    if (json.contains("from_date")) {
        auto timestamp = json["from_date"].get<int64_t>();
        filters.from_date = std::chrono::system_clock::from_time_t(timestamp);
    }
    if (json.contains("to_date")) {
        auto timestamp = json["to_date"].get<int64_t>();
        filters.to_date = std::chrono::system_clock::from_time_t(timestamp);
    }
    if (json.contains("last_hours")) {
        filters.last_hours = std::chrono::hours{json["last_hours"].get<int>()};
    }
    if (json.contains("from_user")) {
        filters.from_user = json["from_user"].get<std::string>();
    }
    if (json.contains("mentioned_users")) {
        filters.mentioned_users = json["mentioned_users"].get<std::vector<std::string>>();
    }
    if (json.contains("exclude_users")) {
        filters.exclude_users = json["exclude_users"].get<std::vector<std::string>>();
    }
    if (json.contains("hashtags")) {
        filters.hashtags = json["hashtags"].get<std::vector<std::string>>();
    }
    if (json.contains("exclude_hashtags")) {
        filters.exclude_hashtags = json["exclude_hashtags"].get<std::vector<std::string>>();
    }
    if (json.contains("has_media")) {
        filters.has_media = json["has_media"].get<bool>();
    }
    if (json.contains("has_links")) {
        filters.has_links = json["has_links"].get<bool>();
    }
    if (json.contains("is_verified_user")) {
        filters.is_verified_user = json["is_verified_user"].get<bool>();
    }
    if (json.contains("min_likes")) {
        filters.min_likes = json["min_likes"].get<int>();
    }
    if (json.contains("min_renotes")) {
        filters.min_renotes = json["min_renotes"].get<int>();
    }
    if (json.contains("min_replies")) {
        filters.min_replies = json["min_replies"].get<int>();
    }
    if (json.contains("location")) {
        filters.location = json["location"].get<std::string>();
    }
    if (json.contains("latitude")) {
        filters.latitude = json["latitude"].get<double>();
    }
    if (json.contains("longitude")) {
        filters.longitude = json["longitude"].get<double>();
    }
    if (json.contains("radius_km")) {
        filters.radius_km = json["radius_km"].get<double>();
    }
    if (json.contains("language")) {
        filters.language = json["language"].get<std::string>();
    }
    if (json.contains("content_types")) {
        filters.content_types = json["content_types"].get<std::vector<std::string>>();
    }
    
    return filters;
}

// SearchConfig implementation
bool SearchConfig::is_valid() const {
    return offset >= 0 && 
           limit > 0 && 
           limit <= max_limit &&
           timeout.count() > 0 &&
           cache_ttl.count() > 0 &&
           relevance_weight >= 0.0 &&
           recency_weight >= 0.0 &&
           popularity_weight >= 0.0 &&
           user_reputation_weight >= 0.0;
}

SearchConfig SearchConfig::default_config() {
    SearchConfig config;
    config.offset = 0;
    config.limit = 20;
    config.max_limit = 100;
    config.enable_autocomplete = true;
    config.enable_spell_correction = true;
    config.enable_fuzzy_matching = true;
    config.enable_stemming = true;
    config.timeout = std::chrono::milliseconds{5000};
    config.use_cache = true;
    config.cache_ttl = std::chrono::minutes{5};
    config.relevance_weight = 1.0;
    config.recency_weight = 0.3;
    config.popularity_weight = 0.5;
    config.user_reputation_weight = 0.2;
    config.include_deleted = false;
    config.include_private = false;
    return config;
}

SearchConfig SearchConfig::realtime_config() {
    SearchConfig config = default_config();
    config.recency_weight = 1.0;  // Prioritize recent content
    config.popularity_weight = 0.1;
    config.use_cache = false;  // Don't cache real-time searches
    config.timeout = std::chrono::milliseconds{2000};  // Faster timeout
    return config;
}

SearchConfig SearchConfig::trending_config() {
    SearchConfig config = default_config();
    config.popularity_weight = 1.0;  // Prioritize popular content
    config.recency_weight = 0.8;     // Still care about recency
    config.relevance_weight = 0.3;
    config.cache_ttl = std::chrono::minutes{15};  // Cache trending longer
    return config;
}

// SearchQuery implementation
SearchQuery::SearchQuery(const std::string& query_text) 
    : query_text(query_text), created_at(std::chrono::system_clock::now()) {
}

SearchQuery::SearchQuery(const std::string& query_text, SearchType type)
    : query_text(query_text), search_type(type), created_at(std::chrono::system_clock::now()) {
}

SearchQuery& SearchQuery::set_query(const std::string& text) {
    query_text = text;
    return *this;
}

SearchQuery& SearchQuery::set_type(SearchType type) {
    search_type = type;
    return *this;
}

SearchQuery& SearchQuery::set_sort(SortOrder order) {
    sort_order = order;
    return *this;
}

SearchQuery& SearchQuery::set_pagination(int offset, int limit) {
    config.offset = offset;
    config.limit = std::min(limit, config.max_limit);
    return *this;
}

SearchQuery& SearchQuery::set_time_range(
    const std::optional<std::chrono::system_clock::time_point>& from,
    const std::optional<std::chrono::system_clock::time_point>& to) {
    filters.from_date = from;
    filters.to_date = to;
    return *this;
}

SearchQuery& SearchQuery::set_from_user(const std::string& username) {
    filters.from_user = username;
    return *this;
}

SearchQuery& SearchQuery::add_hashtag(const std::string& hashtag) {
    filters.hashtags.push_back(hashtag);
    return *this;
}

SearchQuery& SearchQuery::add_mention(const std::string& username) {
    filters.mentioned_users.push_back(username);
    return *this;
}

SearchQuery& SearchQuery::set_min_engagement(int min_likes, int min_renotes, int min_replies) {
    filters.min_likes = min_likes;
    if (min_renotes > 0) filters.min_renotes = min_renotes;
    if (min_replies > 0) filters.min_replies = min_replies;
    return *this;
}

SearchQuery& SearchQuery::set_location(double lat, double lon, double radius_km) {
    filters.latitude = lat;
    filters.longitude = lon;
    filters.radius_km = radius_km;
    return *this;
}

SearchQuery& SearchQuery::set_user_context(
    const std::string& user_id,
    const std::vector<std::string>& interests,
    const std::vector<std::string>& following) {
    this->user_id = user_id;
    this->user_interests = interests;
    this->following_users = following;
    return *this;
}

SearchQuery SearchQuery::parse_natural_language(const std::string& query) {
    SearchQuery search_query;
    std::string cleaned_text = query;
    
    // Regular expressions for parsing operators
    std::regex from_regex(R"(from:@?(\w+))");
    std::regex hashtag_regex(R"(#(\w+))");
    std::regex mention_regex(R"(@(\w+))");
    std::regex since_regex(R"(since:(\S+))");
    std::regex until_regex(R"(until:(\S+))");
    std::regex min_likes_regex(R"(min_likes:(\d+))");
    std::regex min_renotes_regex(R"(min_renotes:(\d+))");
    std::regex near_regex(R"(near:"([^"]+)"\s+within:(\d+)km)");
    std::regex lang_regex(R"(lang:(\w+))");
    
    std::smatch match;
    
    // Parse from: operator
    if (std::regex_search(query, match, from_regex)) {
        search_query.filters.from_user = match[1].str();
        cleaned_text = std::regex_replace(cleaned_text, from_regex, "");
    }
    
    // Parse hashtags
    auto hashtag_begin = std::sregex_iterator(query.begin(), query.end(), hashtag_regex);
    auto hashtag_end = std::sregex_iterator();
    for (auto it = hashtag_begin; it != hashtag_end; ++it) {
        search_query.filters.hashtags.push_back(it->str(1));
    }
    cleaned_text = std::regex_replace(cleaned_text, hashtag_regex, "");
    
    // Parse mentions
    auto mention_begin = std::sregex_iterator(query.begin(), query.end(), mention_regex);
    auto mention_end = std::sregex_iterator();
    for (auto it = mention_begin; it != mention_end; ++it) {
        search_query.filters.mentioned_users.push_back(it->str(1));
    }
    cleaned_text = std::regex_replace(cleaned_text, mention_regex, "");
    
    // Parse since: operator
    if (std::regex_search(query, match, since_regex)) {
        auto time_opt = query_utils::parse_absolute_time(match[1].str());
        if (time_opt) {
            search_query.filters.from_date = *time_opt;
        }
        cleaned_text = std::regex_replace(cleaned_text, since_regex, "");
    }
    
    // Parse until: operator
    if (std::regex_search(query, match, until_regex)) {
        auto time_opt = query_utils::parse_absolute_time(match[1].str());
        if (time_opt) {
            search_query.filters.to_date = *time_opt;
        }
        cleaned_text = std::regex_replace(cleaned_text, until_regex, "");
    }
    
    // Parse min_likes: operator
    if (std::regex_search(query, match, min_likes_regex)) {
        search_query.filters.min_likes = std::stoi(match[1].str());
        cleaned_text = std::regex_replace(cleaned_text, min_likes_regex, "");
    }
    
    // Parse min_renotes: operator
    if (std::regex_search(query, match, min_renotes_regex)) {
        search_query.filters.min_renotes = std::stoi(match[1].str());
        cleaned_text = std::regex_replace(cleaned_text, min_renotes_regex, "");
    }
    
    // Parse language
    if (std::regex_search(query, match, lang_regex)) {
        search_query.filters.language = match[1].str();
        cleaned_text = std::regex_replace(cleaned_text, lang_regex, "");
    }
    
    // Clean up extra whitespace
    cleaned_text = std::regex_replace(cleaned_text, std::regex(R"(\s+)"), " ");
    cleaned_text = std::regex_replace(cleaned_text, std::regex(R"(^\s+|\s+$)"), "");
    
    search_query.query_text = cleaned_text;
    search_query.created_at = std::chrono::system_clock::now();
    
    return search_query;
}

nlohmann::json SearchQuery::to_elasticsearch_query() const {
    nlohmann::json es_query = nlohmann::json::object();
    
    // Main query structure
    nlohmann::json query = nlohmann::json::object();
    nlohmann::json bool_query = nlohmann::json::object();
    
    // Text search
    if (!query_text.empty()) {
        nlohmann::json multi_match = {
            {"multi_match", {
                {"query", query_text},
                {"fields", nlohmann::json::array({
                    "content^3",           // Boost content matches
                    "author.username^2",   // Boost username matches
                    "author.display_name^2",
                    "hashtags^1.5",
                    "mentions"
                })},
                {"type", "best_fields"},
                {"fuzziness", config.enable_fuzzy_matching ? "AUTO" : "0"},
                {"operator", "and"}
            }}
        };
        
        if (config.enable_stemming) {
            multi_match["multi_match"]["analyzer"] = "standard";
        }
        
        bool_query["must"] = nlohmann::json::array({multi_match});
    }
    
    // Apply filters
    nlohmann::json filter_query = filters.to_elasticsearch_query();
    if (!filter_query.empty() && filter_query.contains("bool")) {
        if (filter_query["bool"].contains("filter")) {
            bool_query["filter"] = filter_query["bool"]["filter"];
        }
        if (filter_query["bool"].contains("must_not")) {
            bool_query["must_not"] = filter_query["bool"]["must_not"];
        }
    }
    
    // Personalization boost
    if (!user_id.empty()) {
        nlohmann::json personalization_boost = build_personalization_boost();
        if (!personalization_boost.empty()) {
            bool_query["should"] = personalization_boost;
        }
    }
    
    query["bool"] = bool_query;
    es_query["query"] = query;
    
    // Sorting
    nlohmann::json sort = nlohmann::json::array();
    
    switch (sort_order) {
        case SortOrder::RELEVANCE:
            sort.push_back({{"_score", {{"order", "desc"}}}});
            break;
            
        case SortOrder::RECENCY:
            sort.push_back({{"created_at", {{"order", "desc"}}}});
            break;
            
        case SortOrder::POPULARITY:
            sort.push_back({{"metrics.engagement_score", {{"order", "desc"}}}});
            sort.push_back({{"_score", {{"order", "desc"}}}});
            break;
            
        case SortOrder::TRENDING:
            sort.push_back({{"trending_score", {{"order", "desc"}}}});
            sort.push_back({{"created_at", {{"order", "desc"}}}});
            break;
            
        case SortOrder::MIXED_SIGNALS: {
            // Use function_score for complex ranking
            nlohmann::json function_score = {
                {"function_score", {
                    {"query", query},
                    {"functions", nlohmann::json::array({
                        {
                            {"field_value_factor", {
                                {"field", "metrics.likes_count"},
                                {"factor", popularity_weight},
                                {"modifier", "log1p"}
                            }}
                        },
                        {
                            {"gauss", {
                                {"created_at", {
                                    {"scale", "7d"},
                                    {"decay", 0.5}
                                }}
                            }},
                            {"weight", config.recency_weight}
                        }
                    })},
                    {"score_mode", "sum"},
                    {"boost_mode", "multiply"}
                }}
            };
            es_query["query"] = function_score;
            sort.push_back({{"_score", {{"order", "desc"}}}});
            break;
        }
    }
    
    es_query["sort"] = sort;
    
    // Pagination
    es_query["from"] = config.offset;
    es_query["size"] = config.limit;
    
    // Source fields
    if (search_type == SearchType::USERS) {
        es_query["_source"] = nlohmann::json::array({
            "user_id", "username", "display_name", "bio", "verified",
            "followers_count", "following_count", "notes_count", "avatar_url"
        });
    } else {
        es_query["_source"] = nlohmann::json::array({
            "note_id", "content", "author", "created_at", "metrics",
            "hashtags", "mentions", "media", "reply_to"
        });
    }
    
    // Highlighting
    es_query["highlight"] = {
        {"fields", {
            {"content", nlohmann::json::object()},
            {"author.display_name", nlohmann::json::object()}
        }},
        {"pre_tags", nlohmann::json::array({"<em>"})},
        {"note_tags", nlohmann::json::array({"</em>"})}
    };
    
    return es_query;
}

std::string SearchQuery::get_cache_key() const {
    // Create a deterministic cache key based on query parameters
    std::stringstream ss;
    ss << "search:" << std::hash<std::string>{}(query_text)
       << ":" << static_cast<int>(search_type)
       << ":" << static_cast<int>(sort_order)
       << ":" << config.offset
       << ":" << config.limit;
    
    if (filters.has_filters()) {
        ss << ":" << std::hash<std::string>{}(filters.to_json().dump());
    }
    
    if (!user_id.empty()) {
        ss << ":user:" << user_id;
    }
    
    return ss.str();
}

bool SearchQuery::is_valid() const {
    return !query_text.empty() && config.is_valid();
}

std::optional<int> SearchQuery::estimate_result_count() const {
    // This would typically query Elasticsearch with count API
    // For now, return empty optional indicating unknown
    return std::nullopt;
}

bool SearchQuery::is_trending_query() const {
    return sort_order == SortOrder::TRENDING ||
           sort_order == SortOrder::POPULARITY ||
           (filters.last_hours && filters.last_hours->count() <= 24);
}

bool SearchQuery::is_realtime_query() const {
    return sort_order == SortOrder::RECENCY ||
           (filters.last_hours && filters.last_hours->count() <= 1);
}

double SearchQuery::get_complexity_score() const {
    double score = 1.0;
    
    // Base text complexity
    score += query_text.length() / 100.0;
    
    // Filter complexity
    if (filters.has_filters()) {
        score += 2.0;
    }
    
    // Personalization complexity
    if (!user_id.empty()) {
        score += 1.5;
    }
    
    // Result size complexity
    score += config.limit / 50.0;
    
    return score;
}

nlohmann::json SearchQuery::to_json() const {
    nlohmann::json json = nlohmann::json::object();
    
    json["query_text"] = query_text;
    json["search_type"] = static_cast<int>(search_type);
    json["sort_order"] = static_cast<int>(sort_order);
    json["filters"] = filters.to_json();
    json["config"] = {
        {"offset", config.offset},
        {"limit", config.limit},
        {"enable_autocomplete", config.enable_autocomplete},
        {"enable_spell_correction", config.enable_spell_correction},
        {"enable_fuzzy_matching", config.enable_fuzzy_matching},
        {"enable_stemming", config.enable_stemming},
        {"timeout", config.timeout.count()},
        {"use_cache", config.use_cache},
        {"cache_ttl", config.cache_ttl.count()},
        {"relevance_weight", config.relevance_weight},
        {"recency_weight", config.recency_weight},
        {"popularity_weight", config.popularity_weight},
        {"user_reputation_weight", config.user_reputation_weight}
    };
    
    if (!user_id.empty()) {
        json["user_id"] = user_id;
    }
    if (!session_id.empty()) {
        json["session_id"] = session_id;
    }
    if (!user_interests.empty()) {
        json["user_interests"] = user_interests;
    }
    if (!following_users.empty()) {
        json["following_users"] = following_users;
    }
    
    json["created_at"] = std::chrono::duration_cast<std::chrono::seconds>(
        created_at.time_since_epoch()).count();
    
    return json;
}

SearchQuery SearchQuery::from_json(const nlohmann::json& json) {
    SearchQuery query;
    
    query.query_text = json.value("query_text", "");
    query.search_type = static_cast<SearchType>(json.value("search_type", 0));
    query.sort_order = static_cast<SortOrder>(json.value("sort_order", 0));
    
    if (json.contains("filters")) {
        query.filters = SearchFilters::from_json(json["filters"]);
    }
    
    if (json.contains("config")) {
        const auto& config_json = json["config"];
        query.config.offset = config_json.value("offset", 0);
        query.config.limit = config_json.value("limit", 20);
        query.config.enable_autocomplete = config_json.value("enable_autocomplete", true);
        query.config.enable_spell_correction = config_json.value("enable_spell_correction", true);
        query.config.enable_fuzzy_matching = config_json.value("enable_fuzzy_matching", true);
        query.config.enable_stemming = config_json.value("enable_stemming", true);
        query.config.timeout = std::chrono::milliseconds{config_json.value("timeout", 5000)};
        query.config.use_cache = config_json.value("use_cache", true);
        query.config.cache_ttl = std::chrono::minutes{config_json.value("cache_ttl", 5)};
        query.config.relevance_weight = config_json.value("relevance_weight", 1.0);
        query.config.recency_weight = config_json.value("recency_weight", 0.3);
        query.config.popularity_weight = config_json.value("popularity_weight", 0.5);
        query.config.user_reputation_weight = config_json.value("user_reputation_weight", 0.2);
    }
    
    query.user_id = json.value("user_id", "");
    query.session_id = json.value("session_id", "");
    
    if (json.contains("user_interests")) {
        query.user_interests = json["user_interests"].get<std::vector<std::string>>();
    }
    if (json.contains("following_users")) {
        query.following_users = json["following_users"].get<std::vector<std::string>>();
    }
    
    if (json.contains("created_at")) {
        auto timestamp = json["created_at"].get<int64_t>();
        query.created_at = std::chrono::system_clock::from_time_t(timestamp);
    }
    
    return query;
}

SearchQuery SearchQuery::create_autocomplete_query(const std::string& partial_text) {
    SearchQuery query(partial_text);
    query.search_type = SearchType::MIXED;
    query.sort_order = SortOrder::POPULARITY;
    query.config.limit = 10;
    query.config.enable_autocomplete = true;
    query.config.enable_fuzzy_matching = true;
    query.config.use_cache = true;
    query.config.cache_ttl = std::chrono::minutes{30};
    return query;
}

SearchQuery SearchQuery::create_trending_query(const std::chrono::hours& time_window) {
    SearchQuery query;
    query.search_type = SearchType::HASHTAGS;
    query.sort_order = SortOrder::TRENDING;
    query.filters.last_hours = time_window;
    query.config = SearchConfig::trending_config();
    return query;
}

SearchQuery SearchQuery::create_user_recommendation_query(
    const std::string& user_id,
    const std::vector<std::string>& interests) {
    SearchQuery query;
    query.search_type = SearchType::USERS;
    query.sort_order = SortOrder::POPULARITY;
    query.user_id = user_id;
    query.user_interests = interests;
    query.config.limit = 50;
    return query;
}

nlohmann::json SearchQuery::build_personalization_boost() const {
    nlohmann::json should_clauses = nlohmann::json::array();
    
    // Boost content from followed users
    if (!following_users.empty()) {
        nlohmann::json following_boost = {
            {"terms", {
                {"author.user_id", following_users},
                {"boost", 2.0}
            }}
        };
        should_clauses.push_back(following_boost);
    }
    
    // Boost content matching user interests
    if (!user_interests.empty()) {
        for (const auto& interest : user_interests) {
            nlohmann::json interest_boost = {
                {"match", {
                    {"content", {
                        {"query", interest},
                        {"boost", 1.5}
                    }}
                }}
            };
            should_clauses.push_back(interest_boost);
        }
    }
    
    return should_clauses;
}

std::string SearchQuery::generate_query_id() const {
    uuid_t uuid;
    uuid_generate_random(uuid);
    char uuid_str[37];
    uuid_unparse_lower(uuid, uuid_str);
    return std::string(uuid_str);
}

// SearchQueryBuilder implementation
SearchQueryBuilder& SearchQueryBuilder::query(const std::string& text) {
    query_.query_text = text;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::type(SearchType search_type) {
    query_.search_type = search_type;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::sort(SortOrder order) {
    query_.sort_order = order;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::limit(int limit) {
    query_.config.limit = limit;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::offset(int offset) {
    query_.config.offset = offset;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::from_user(const std::string& username) {
    query_.filters.from_user = username;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::hashtag(const std::string& hashtag) {
    query_.filters.hashtags.push_back(hashtag);
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::mention(const std::string& username) {
    query_.filters.mentioned_users.push_back(username);
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::since(const std::chrono::system_clock::time_point& time) {
    query_.filters.from_date = time;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::until(const std::chrono::system_clock::time_point& time) {
    query_.filters.to_date = time;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::min_likes(int likes) {
    query_.filters.min_likes = likes;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::near(double lat, double lon, double radius_km) {
    query_.filters.latitude = lat;
    query_.filters.longitude = lon;
    query_.filters.radius_km = radius_km;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::language(const std::string& lang) {
    query_.filters.language = lang;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::with_media() {
    query_.filters.has_media = true;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::verified_only() {
    query_.filters.is_verified_user = true;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::user_context(const std::string& user_id) {
    query_.user_id = user_id;
    return *this;
}

SearchQuery SearchQueryBuilder::build() {
    query_.created_at = std::chrono::system_clock::now();
    return query_;
}

// Utility functions implementation
namespace query_utils {

std::optional<std::chrono::system_clock::time_point> parse_relative_time(const std::string& expr) {
    std::regex time_regex(R"((\d+)([hdw]))");
    std::smatch match;
    
    if (std::regex_match(expr, match, time_regex)) {
        int value = std::stoi(match[1].str());
        char unit = match[2].str()[0];
        
        auto now = std::chrono::system_clock::now();
        
        switch (unit) {
            case 'h':
                return now - std::chrono::hours{value};
            case 'd':
                return now - std::chrono::hours{value * 24};
            case 'w':
                return now - std::chrono::hours{value * 24 * 7};
        }
    }
    
    return std::nullopt;
}

std::optional<std::chrono::system_clock::time_point> parse_absolute_time(const std::string& expr) {
    // Try parsing ISO format first: 2024-01-01 or 2024-01-01T10:30:00
    std::tm tm = {};
    std::istringstream ss(expr);
    
    // Try different formats
    ss >> std::get_time(&tm, "%Y-%m-%d");
    if (!ss.fail()) {
        auto time_t = std::mktime(&tm);
        return std::chrono::system_clock::from_time_t(time_t);
    }
    
    // Try relative time format
    return parse_relative_time(expr);
}

std::unordered_map<std::string, std::string> extract_operators(const std::string& query) {
    std::unordered_map<std::string, std::string> operators;
    
    std::regex operator_regex(R"((\w+):(\S+))");
    auto begin = std::sregex_iterator(query.begin(), query.end(), operator_regex);
    auto end = std::sregex_iterator();
    
    for (auto it = begin; it != end; ++it) {
        std::smatch match = *it;
        operators[match[1].str()] = match[2].str();
    }
    
    return operators;
}

std::string clean_query_text(const std::string& query) {
    // Remove all operators and extra whitespace
    std::string cleaned = std::regex_replace(query, std::regex(R"(\w+:\S+)"), "");
    cleaned = std::regex_replace(cleaned, std::regex(R"(\s+)"), " ");
    cleaned = std::regex_replace(cleaned, std::regex(R"(^\s+|\s+$)"), "");
    return cleaned;
}

bool is_valid_username(const std::string& username) {
    std::regex username_regex(R"(^[a-zA-Z0-9_]{1,15}$)");
    return std::regex_match(username, username_regex);
}

bool is_valid_hashtag(const std::string& hashtag) {
    std::regex hashtag_regex(R"(^[a-zA-Z0-9_]{1,100}$)");
    return std::regex_match(hashtag, hashtag_regex);
}

std::vector<std::string> generate_suggestions(const std::string& partial_query) {
    // This would typically query a suggestions database or API
    // For now, return some basic suggestions
    std::vector<std::string> suggestions;
    
    if (partial_query.empty()) {
        return suggestions;
    }
    
    // Add some common completion patterns
    suggestions.push_back(partial_query + " from:verified");
    suggestions.push_back(partial_query + " filter:media");
    suggestions.push_back(partial_query + " since:24h");
    suggestions.push_back(partial_query + " min_likes:10");
    
    return suggestions;
}

} // namespace query_utils

} // namespace models
} // namespace search_service
} // namespace sonet