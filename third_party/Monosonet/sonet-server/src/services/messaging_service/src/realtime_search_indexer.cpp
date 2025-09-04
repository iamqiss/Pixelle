/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "realtime_search_indexer.hpp"
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <cctype>
#include <locale>
#include <regex>

namespace sonet::messaging::search {

// SearchFilters implementation
Json::Value SearchFilters::to_json() const {
    Json::Value json;
    json["query"] = query;
    json["scope"] = static_cast<int>(scope);
    
    // User filters
    Json::Value from_users_json(Json::arrayValue);
    for (const auto& user : from_users) {
        from_users_json.append(user);
    }
    json["from_users"] = from_users_json;
    
    Json::Value exclude_users_json(Json::arrayValue);
    for (const auto& user : exclude_users) {
        exclude_users_json.append(user);
    }
    json["exclude_users"] = exclude_users_json;
    
    // Time filters
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["start_time"] = static_cast<int64_t>(to_timestamp(start_time));
    json["end_time"] = static_cast<int64_t>(to_timestamp(end_time));
    
    // Content filters
    Json::Value include_types_json(Json::arrayValue);
    for (const auto& type : include_types) {
        include_types_json.append(static_cast<int>(type));
    }
    json["include_types"] = include_types_json;
    
    Json::Value exclude_types_json(Json::arrayValue);
    for (const auto& type : exclude_types) {
        exclude_types_json.append(static_cast<int>(type));
    }
    json["exclude_types"] = exclude_types_json;
    
    json["include_deleted"] = include_deleted;
    json["include_edited"] = include_edited;
    json["only_starred"] = only_starred;
    json["only_pinned"] = only_pinned;
    
    // Context filters
    Json::Value in_chats_json(Json::arrayValue);
    for (const auto& chat : in_chats) {
        in_chats_json.append(chat);
    }
    json["in_chats"] = in_chats_json;
    
    json["min_message_length"] = min_message_length;
    json["max_message_length"] = max_message_length;
    json["semantic_search_enabled"] = semantic_search_enabled;
    json["fuzzy_matching_enabled"] = fuzzy_matching_enabled;
    json["min_relevance_score"] = min_relevance_score;
    
    // Ranking weights
    Json::Value weights_json;
    for (const auto& [factor, weight] : ranking_weights) {
        weights_json[std::to_string(static_cast<int>(factor))] = weight;
    }
    json["ranking_weights"] = weights_json;
    
    return json;
}

SearchFilters SearchFilters::from_json(const Json::Value& json) {
    SearchFilters filters;
    filters.query = json["query"].asString();
    filters.scope = static_cast<SearchScope>(json["scope"].asInt());
    
    // User filters
    for (const auto& user_json : json["from_users"]) {
        filters.from_users.push_back(user_json.asString());
    }
    for (const auto& user_json : json["exclude_users"]) {
        filters.exclude_users.push_back(user_json.asString());
    }
    
    // Time filters
    auto from_timestamp = [](int64_t ms) {
        return std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
    };
    
    filters.start_time = from_timestamp(json["start_time"].asInt64());
    filters.end_time = from_timestamp(json["end_time"].asInt64());
    
    // Content filters
    for (const auto& type_json : json["include_types"]) {
        filters.include_types.push_back(static_cast<SearchResultType>(type_json.asInt()));
    }
    for (const auto& type_json : json["exclude_types"]) {
        filters.exclude_types.push_back(static_cast<SearchResultType>(type_json.asInt()));
    }
    
    filters.include_deleted = json["include_deleted"].asBool();
    filters.include_edited = json["include_edited"].asBool();
    filters.only_starred = json["only_starred"].asBool();
    filters.only_pinned = json["only_pinned"].asBool();
    
    // Context filters
    for (const auto& chat_json : json["in_chats"]) {
        filters.in_chats.push_back(chat_json.asString());
    }
    
    filters.min_message_length = json["min_message_length"].asUInt();
    filters.max_message_length = json["max_message_length"].asUInt();
    filters.semantic_search_enabled = json["semantic_search_enabled"].asBool();
    filters.fuzzy_matching_enabled = json["fuzzy_matching_enabled"].asBool();
    filters.min_relevance_score = json["min_relevance_score"].asDouble();
    
    // Ranking weights
    for (const auto& member : json["ranking_weights"].getMemberNames()) {
        auto factor = static_cast<SearchRankingFactor>(std::stoi(member));
        filters.ranking_weights[factor] = json["ranking_weights"][member].asDouble();
    }
    
    return filters;
}

SearchFilters SearchFilters::default_filters() {
    SearchFilters filters;
    filters.scope = SearchScope::ALL_CONTENT;
    filters.include_deleted = false;
    filters.include_edited = true;
    filters.only_starred = false;
    filters.only_pinned = false;
    filters.min_message_length = 0;
    filters.max_message_length = UINT32_MAX;
    filters.semantic_search_enabled = false;
    filters.fuzzy_matching_enabled = true;
    filters.min_relevance_score = 0.1;
    
    // Default ranking weights
    filters.ranking_weights[SearchRankingFactor::EXACT_MATCH] = 1.0;
    filters.ranking_weights[SearchRankingFactor::PARTIAL_MATCH] = 0.7;
    filters.ranking_weights[SearchRankingFactor::RELEVANCE_SCORE] = 0.8;
    filters.ranking_weights[SearchRankingFactor::RECENCY] = 0.3;
    filters.ranking_weights[SearchRankingFactor::USER_INTERACTION] = 0.5;
    filters.ranking_weights[SearchRankingFactor::MESSAGE_IMPORTANCE] = 0.9;
    
    return filters;
}

bool SearchFilters::matches_result_type(SearchResultType type) const {
    if (!include_types.empty()) {
        return std::find(include_types.begin(), include_types.end(), type) != include_types.end();
    }
    if (!exclude_types.empty()) {
        return std::find(exclude_types.begin(), exclude_types.end(), type) == exclude_types.end();
    }
    return true;
}

bool SearchFilters::matches_time_range(const std::chrono::system_clock::time_point& timestamp) const {
    if (start_time != std::chrono::system_clock::time_point{} && timestamp < start_time) {
        return false;
    }
    if (end_time != std::chrono::system_clock::time_point{} && timestamp > end_time) {
        return false;
    }
    return true;
}

// SearchResult implementation
Json::Value SearchResult::to_json() const {
    Json::Value json;
    json["result_id"] = result_id;
    json["message_id"] = message_id;
    json["chat_id"] = chat_id;
    json["thread_id"] = thread_id;
    json["user_id"] = user_id;
    json["type"] = static_cast<int>(type);
    
    json["content"] = content;
    json["original_content"] = original_content;
    json["highlighted_content"] = highlighted_content;
    
    Json::Value matched_terms_json(Json::arrayValue);
    for (const auto& term : matched_terms) {
        matched_terms_json.append(term);
    }
    json["matched_terms"] = matched_terms_json;
    
    Json::Value positions_json(Json::arrayValue);
    for (const auto& [start, length] : match_positions) {
        Json::Value pos;
        pos["start"] = static_cast<uint64_t>(start);
        pos["length"] = static_cast<uint64_t>(length);
        positions_json.append(pos);
    }
    json["match_positions"] = positions_json;
    
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["timestamp"] = static_cast<int64_t>(to_timestamp(timestamp));
    json["edited_at"] = static_cast<int64_t>(to_timestamp(edited_at));
    
    json["is_deleted"] = is_deleted;
    json["is_edited"] = is_edited;
    json["is_starred"] = is_starred;
    json["is_pinned"] = is_pinned;
    
    json["reply_to_message_id"] = reply_to_message_id;
    json["forwarded_from_chat_id"] = forwarded_from_chat_id;
    
    json["reaction_count"] = reaction_count;
    json["reply_count"] = reply_count;
    json["view_count"] = view_count;
    
    json["relevance_score"] = relevance_score;
    json["exact_match_score"] = exact_match_score;
    json["recency_score"] = recency_score;
    json["engagement_score"] = engagement_score;
    json["final_score"] = final_score;
    
    json["before_context"] = before_context;
    json["after_context"] = after_context;
    
    return json;
}

SearchResult SearchResult::from_json(const Json::Value& json) {
    SearchResult result;
    result.result_id = json["result_id"].asString();
    result.message_id = json["message_id"].asString();
    result.chat_id = json["chat_id"].asString();
    result.thread_id = json["thread_id"].asString();
    result.user_id = json["user_id"].asString();
    result.type = static_cast<SearchResultType>(json["type"].asInt());
    
    result.content = json["content"].asString();
    result.original_content = json["original_content"].asString();
    result.highlighted_content = json["highlighted_content"].asString();
    
    for (const auto& term_json : json["matched_terms"]) {
        result.matched_terms.push_back(term_json.asString());
    }
    
    for (const auto& pos_json : json["match_positions"]) {
        size_t start = pos_json["start"].asUInt64();
        size_t length = pos_json["length"].asUInt64();
        result.match_positions.emplace_back(start, length);
    }
    
    auto from_timestamp = [](int64_t ms) {
        return std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
    };
    
    result.timestamp = from_timestamp(json["timestamp"].asInt64());
    result.edited_at = from_timestamp(json["edited_at"].asInt64());
    
    result.is_deleted = json["is_deleted"].asBool();
    result.is_edited = json["is_edited"].asBool();
    result.is_starred = json["is_starred"].asBool();
    result.is_pinned = json["is_pinned"].asBool();
    
    result.reply_to_message_id = json["reply_to_message_id"].asString();
    result.forwarded_from_chat_id = json["forwarded_from_chat_id"].asString();
    
    result.reaction_count = json["reaction_count"].asUInt();
    result.reply_count = json["reply_count"].asUInt();
    result.view_count = json["view_count"].asUInt();
    
    result.relevance_score = json["relevance_score"].asDouble();
    result.exact_match_score = json["exact_match_score"].asDouble();
    result.recency_score = json["recency_score"].asDouble();
    result.engagement_score = json["engagement_score"].asDouble();
    result.final_score = json["final_score"].asDouble();
    
    result.before_context = json["before_context"].asString();
    result.after_context = json["after_context"].asString();
    
    return result;
}

bool SearchResult::is_relevant(double min_score) const {
    return final_score >= min_score;
}

std::string SearchResult::get_display_content(size_t max_length) const {
    std::string display = highlighted_content.empty() ? content : highlighted_content;
    if (display.length() <= max_length) {
        return display;
    }
    
    // Try to find a good truncation point
    size_t truncate_pos = max_length - 3; // Reserve space for "..."
    
    // Look for the first match position to center around
    if (!match_positions.empty()) {
        size_t match_start = match_positions[0].first;
        if (match_start > max_length / 2) {
            size_t context_start = std::max(0, static_cast<int>(match_start - max_length / 2));
            truncate_pos = std::min(context_start + max_length - 3, display.length());
            return "..." + display.substr(context_start, truncate_pos - context_start) + "...";
        }
    }
    
    return display.substr(0, truncate_pos) + "...";
}

// SearchIndexEntry implementation
Json::Value SearchIndexEntry::to_json() const {
    Json::Value json;
    json["message_id"] = message_id;
    json["chat_id"] = chat_id;
    json["user_id"] = user_id;
    json["thread_id"] = thread_id;
    json["type"] = static_cast<int>(type);
    
    Json::Value words_json(Json::arrayValue);
    for (const auto& word : words) {
        words_json.append(word);
    }
    json["words"] = words_json;
    
    Json::Value stemmed_words_json(Json::arrayValue);
    for (const auto& word : stemmed_words) {
        stemmed_words_json.append(word);
    }
    json["stemmed_words"] = stemmed_words_json;
    
    Json::Value frequencies_json;
    for (const auto& [word, freq] : word_frequencies) {
        frequencies_json[word] = freq;
    }
    json["word_frequencies"] = frequencies_json;
    
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["timestamp"] = static_cast<int64_t>(to_timestamp(timestamp));
    json["message_length"] = message_length;
    json["engagement_score"] = engagement_score;
    json["is_important"] = is_important;
    
    Json::Value semantic_vector_json(Json::arrayValue);
    for (const auto& value : semantic_vector) {
        semantic_vector_json.append(value);
    }
    json["semantic_vector"] = semantic_vector_json;
    
    json["semantic_summary"] = semantic_summary;
    
    return json;
}

SearchIndexEntry SearchIndexEntry::from_json(const Json::Value& json) {
    SearchIndexEntry entry;
    entry.message_id = json["message_id"].asString();
    entry.chat_id = json["chat_id"].asString();
    entry.user_id = json["user_id"].asString();
    entry.thread_id = json["thread_id"].asString();
    entry.type = static_cast<SearchResultType>(json["type"].asInt());
    
    for (const auto& word_json : json["words"]) {
        entry.words.push_back(word_json.asString());
        entry.unique_words.insert(word_json.asString());
    }
    
    for (const auto& word_json : json["stemmed_words"]) {
        entry.stemmed_words.push_back(word_json.asString());
    }
    
    for (const auto& member : json["word_frequencies"].getMemberNames()) {
        entry.word_frequencies[member] = json["word_frequencies"][member].asUInt();
    }
    
    auto from_timestamp = [](int64_t ms) {
        return std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
    };
    
    entry.timestamp = from_timestamp(json["timestamp"].asInt64());
    entry.message_length = json["message_length"].asUInt();
    entry.engagement_score = json["engagement_score"].asUInt();
    entry.is_important = json["is_important"].asBool();
    
    for (const auto& value_json : json["semantic_vector"]) {
        entry.semantic_vector.push_back(value_json.asDouble());
    }
    
    entry.semantic_summary = json["semantic_summary"].asString();
    
    return entry;
}

double SearchIndexEntry::calculate_tf_idf_score(const std::string& term,
                                               const std::unordered_map<std::string, uint32_t>& document_frequencies,
                                               uint32_t total_documents) const {
    auto tf_it = word_frequencies.find(term);
    if (tf_it == word_frequencies.end()) return 0.0;
    
    auto df_it = document_frequencies.find(term);
    if (df_it == document_frequencies.end()) return 0.0;
    
    double tf = static_cast<double>(tf_it->second) / words.size();
    double idf = std::log(static_cast<double>(total_documents) / df_it->second);
    
    return tf * idf;
}

bool SearchIndexEntry::matches_term(const std::string& term, bool exact_match) const {
    if (exact_match) {
        return unique_words.find(term) != unique_words.end();
    }
    
    // Fuzzy matching - check if any word contains the term
    for (const auto& word : unique_words) {
        if (word.find(term) != std::string::npos) {
            return true;
        }
    }
    return false;
}

// SearchStatistics implementation
Json::Value SearchStatistics::to_json() const {
    Json::Value json;
    
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["collection_start"] = static_cast<int64_t>(to_timestamp(collection_start));
    json["last_update"] = static_cast<int64_t>(to_timestamp(last_update));
    
    json["total_indexed_messages"] = static_cast<uint64_t>(total_indexed_messages);
    json["total_indexed_words"] = static_cast<uint64_t>(total_indexed_words);
    json["unique_words_count"] = static_cast<uint64_t>(unique_words_count);
    json["total_index_size_bytes"] = static_cast<uint64_t>(total_index_size_bytes);
    
    json["total_queries_processed"] = static_cast<uint64_t>(total_queries_processed);
    json["successful_queries"] = static_cast<uint64_t>(successful_queries);
    json["failed_queries"] = static_cast<uint64_t>(failed_queries);
    json["average_query_time"] = static_cast<int64_t>(average_query_time.count());
    json["fastest_query_time"] = static_cast<int64_t>(fastest_query_time.count());
    json["slowest_query_time"] = static_cast<int64_t>(slowest_query_time.count());
    
    Json::Value popular_terms_json;
    for (const auto& [term, count] : popular_terms) {
        popular_terms_json[term] = count;
    }
    json["popular_terms"] = popular_terms_json;
    
    json["index_update_rate"] = index_update_rate;
    json["query_success_rate"] = query_success_rate;
    json["average_results_per_query"] = average_results_per_query;
    json["cache_hit_rate"] = cache_hit_rate;
    
    json["current_concurrent_queries"] = current_concurrent_queries;
    json["pending_index_updates"] = pending_index_updates;
    json["current_index_lag"] = static_cast<int64_t>(current_index_lag.count());
    
    return json;
}

void SearchStatistics::reset() {
    total_indexed_messages = 0;
    total_indexed_words = 0;
    unique_words_count = 0;
    total_index_size_bytes = 0;
    total_queries_processed = 0;
    successful_queries = 0;
    failed_queries = 0;
    average_query_time = std::chrono::milliseconds::zero();
    fastest_query_time = std::chrono::milliseconds::max();
    slowest_query_time = std::chrono::milliseconds::zero();
    popular_terms.clear();
    scope_usage.clear();
    result_type_distribution.clear();
    index_update_rate = 0.0;
    query_success_rate = 0.0;
    average_results_per_query = 0.0;
    cache_hit_rate = 0;
    current_concurrent_queries = 0;
    pending_index_updates = 0;
    current_index_lag = std::chrono::milliseconds::zero();
}

void SearchStatistics::update_query_time(std::chrono::milliseconds query_time) {
    if (query_time < fastest_query_time) {
        fastest_query_time = query_time;
    }
    if (query_time > slowest_query_time) {
        slowest_query_time = query_time;
    }
    
    // Update rolling average
    if (total_queries_processed == 0) {
        average_query_time = query_time;
    } else {
        auto total_time = average_query_time * total_queries_processed + query_time;
        average_query_time = total_time / (total_queries_processed + 1);
    }
}

void SearchStatistics::record_query(const std::string& query, SearchScope scope, bool successful) {
    total_queries_processed++;
    if (successful) {
        successful_queries++;
    } else {
        failed_queries++;
    }
    
    // Update success rate
    query_success_rate = static_cast<double>(successful_queries) / total_queries_processed;
    
    // Record popular terms
    std::istringstream iss(query);
    std::string term;
    while (iss >> term) {
        // Simple tokenization
        std::transform(term.begin(), term.end(), term.begin(), ::tolower);
        popular_terms[term]++;
    }
    
    // Record scope usage
    scope_usage[scope]++;
    
    last_update = std::chrono::system_clock::now();
}

// SearchIndexConfig implementation
Json::Value SearchIndexConfig::to_json() const {
    Json::Value json;
    json["real_time_indexing"] = real_time_indexing;
    json["index_batch_interval"] = static_cast<int64_t>(index_batch_interval.count());
    json["max_batch_size"] = max_batch_size;
    json["enable_stemming"] = enable_stemming;
    json["enable_stop_words_removal"] = enable_stop_words_removal;
    json["enable_semantic_indexing"] = enable_semantic_indexing;
    
    json["index_storage_path"] = index_storage_path;
    json["persist_to_disk"] = persist_to_disk;
    json["memory_cache_size_mb"] = memory_cache_size_mb;
    json["max_cache_age"] = static_cast<int64_t>(max_cache_age.count());
    
    json["max_results_per_query"] = max_results_per_query;
    json["query_timeout"] = static_cast<int64_t>(query_timeout.count());
    json["enable_query_caching"] = enable_query_caching;
    json["enable_fuzzy_search"] = enable_fuzzy_search;
    json["fuzzy_threshold"] = fuzzy_threshold;
    
    Json::Value ignored_types_json(Json::arrayValue);
    for (const auto& type : ignored_file_types) {
        ignored_types_json.append(type);
    }
    json["ignored_file_types"] = ignored_types_json;
    
    Json::Value stop_words_json(Json::arrayValue);
    for (const auto& word : stop_words) {
        stop_words_json.append(word);
    }
    json["stop_words"] = stop_words_json;
    
    json["max_word_length"] = max_word_length;
    json["min_word_length"] = min_word_length;
    json["primary_language"] = primary_language;
    json["auto_detect_language"] = auto_detect_language;
    
    return json;
}

SearchIndexConfig SearchIndexConfig::from_json(const Json::Value& json) {
    SearchIndexConfig config;
    config.real_time_indexing = json["real_time_indexing"].asBool();
    config.index_batch_interval = std::chrono::milliseconds(json["index_batch_interval"].asInt64());
    config.max_batch_size = json["max_batch_size"].asUInt();
    config.enable_stemming = json["enable_stemming"].asBool();
    config.enable_stop_words_removal = json["enable_stop_words_removal"].asBool();
    config.enable_semantic_indexing = json["enable_semantic_indexing"].asBool();
    
    config.index_storage_path = json["index_storage_path"].asString();
    config.persist_to_disk = json["persist_to_disk"].asBool();
    config.memory_cache_size_mb = json["memory_cache_size_mb"].asUInt();
    config.max_cache_age = std::chrono::hours(json["max_cache_age"].asInt64());
    
    config.max_results_per_query = json["max_results_per_query"].asUInt();
    config.query_timeout = std::chrono::milliseconds(json["query_timeout"].asInt64());
    config.enable_query_caching = json["enable_query_caching"].asBool();
    config.enable_fuzzy_search = json["enable_fuzzy_search"].asBool();
    config.fuzzy_threshold = json["fuzzy_threshold"].asDouble();
    
    for (const auto& type_json : json["ignored_file_types"]) {
        config.ignored_file_types.push_back(type_json.asString());
    }
    
    for (const auto& word_json : json["stop_words"]) {
        config.stop_words.push_back(word_json.asString());
    }
    
    config.max_word_length = json["max_word_length"].asUInt();
    config.min_word_length = json["min_word_length"].asUInt();
    config.primary_language = json["primary_language"].asString();
    config.auto_detect_language = json["auto_detect_language"].asBool();
    
    return config;
}

SearchIndexConfig SearchIndexConfig::default_config() {
    SearchIndexConfig config;
    config.real_time_indexing = true;
    config.index_batch_interval = std::chrono::seconds(5);
    config.max_batch_size = 100;
    config.enable_stemming = true;
    config.enable_stop_words_removal = true;
    config.enable_semantic_indexing = false;
    
    config.index_storage_path = "/tmp/search_index";
    config.persist_to_disk = true;
    config.memory_cache_size_mb = 256;
    config.max_cache_age = std::chrono::hours(24);
    
    config.max_results_per_query = 100;
    config.query_timeout = std::chrono::seconds(30);
    config.enable_query_caching = true;
    config.enable_fuzzy_search = true;
    config.fuzzy_threshold = 0.7;
    
    config.ignored_file_types = {".exe", ".bin", ".dll", ".so"};
    config.stop_words = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by"};
    config.max_word_length = 50;
    config.min_word_length = 2;
    config.primary_language = "en";
    config.auto_detect_language = false;
    
    return config;
}

// RealTimeSearchIndexer implementation
RealTimeSearchIndexer::RealTimeSearchIndexer(const SearchIndexConfig& config)
    : config_(config)
    , encrypted_search_enabled_(false)
    , background_running_(true) {
    
    statistics_.collection_start = std::chrono::system_clock::now();
    statistics_.last_update = std::chrono::system_clock::now();
    
    // Start background threads
    indexing_thread_ = std::thread(&RealTimeSearchIndexer::run_indexing_loop, this);
    optimization_thread_ = std::thread(&RealTimeSearchIndexer::run_optimization_loop, this);
    
    log_info("Real-time search indexer initialized");
}

RealTimeSearchIndexer::~RealTimeSearchIndexer() {
    background_running_ = false;
    
    if (indexing_thread_.joinable()) {
        indexing_thread_.join();
    }
    if (optimization_thread_.joinable()) {
        optimization_thread_.join();
    }
    
    log_info("Real-time search indexer destroyed");
}

std::future<bool> RealTimeSearchIndexer::initialize_index() {
    return std::async(std::launch::async, [this]() -> bool {
        try {
            std::unique_lock<std::shared_mutex> lock(index_mutex_);
            
            // Initialize data structures
            message_index_.clear();
            word_to_messages_.clear();
            chat_to_messages_.clear();
            user_to_messages_.clear();
            document_frequencies_.clear();
            
            // Load from disk if enabled
            if (config_.persist_to_disk) {
                // Implementation would load persisted index
                log_info("Loading persisted index from disk");
            }
            
            log_info("Search index initialized successfully");
            return true;
            
        } catch (const std::exception& e) {
            log_error("Failed to initialize index: " + std::string(e.what()));
            return false;
        }
    });
}

std::future<bool> RealTimeSearchIndexer::index_message(const std::string& message_id,
                                                      const std::string& chat_id,
                                                      const std::string& user_id,
                                                      const std::string& content,
                                                      SearchResultType type,
                                                      const std::string& thread_id) {
    return std::async(std::launch::async, [=]() -> bool {
        try {
            if (config_.real_time_indexing) {
                // Process immediately
                return index_message_internal(message_id, chat_id, user_id, content, type, thread_id);
            } else {
                // Add to batch queue
                Json::Value message_data;
                message_data["message_id"] = message_id;
                message_data["chat_id"] = chat_id;
                message_data["user_id"] = user_id;
                message_data["content"] = content;
                message_data["type"] = static_cast<int>(type);
                message_data["thread_id"] = thread_id;
                
                std::lock_guard<std::mutex> lock(pending_updates_mutex_);
                pending_updates_.push(message_data);
                
                return true;
            }
            
        } catch (const std::exception& e) {
            log_error("Failed to index message: " + std::string(e.what()));
            return false;
        }
    });
}

std::future<std::vector<SearchResult>> RealTimeSearchIndexer::search(const std::string& query,
                                                                     const SearchFilters& filters,
                                                                     uint32_t max_results) {
    return std::async(std::launch::async, [=]() -> std::vector<SearchResult> {
        auto start_time = std::chrono::steady_clock::now();
        
        try {
            // Check cache first
            std::string cache_key = generate_cache_key(query, filters);
            if (config_.enable_query_caching && is_query_cached(cache_key)) {
                auto cached_results = get_cached_result(cache_key);
                
                auto end_time = std::chrono::steady_clock::now();
                auto query_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                update_statistics(query, filters.scope, true, query_time);
                
                return cached_results;
            }
            
            std::shared_lock<std::shared_mutex> lock(index_mutex_);
            
            // Process query
            auto query_terms = process_query(query);
            if (query_terms.empty()) {
                return {};
            }
            
            std::vector<SearchResult> results;
            std::unordered_set<std::string> candidate_messages;
            
            // Find candidate messages
            for (const auto& term : query_terms) {
                auto word_it = word_to_messages_.find(term);
                if (word_it != word_to_messages_.end()) {
                    if (candidate_messages.empty()) {
                        candidate_messages = word_it->second;
                    } else {
                        // Intersection for AND logic
                        std::unordered_set<std::string> intersection;
                        for (const auto& msg_id : candidate_messages) {
                            if (word_it->second.find(msg_id) != word_it->second.end()) {
                                intersection.insert(msg_id);
                            }
                        }
                        candidate_messages = std::move(intersection);
                    }
                }
            }
            
            // Process candidates
            for (const auto& message_id : candidate_messages) {
                auto entry_it = message_index_.find(message_id);
                if (entry_it == message_index_.end()) continue;
                
                const auto& entry = entry_it->second;
                
                // Apply filters
                if (!matches_filters(entry, filters)) continue;
                
                // Create search result
                SearchResult result;
                result.result_id = generate_result_id();
                result.message_id = message_id;
                result.chat_id = entry.chat_id;
                result.user_id = entry.user_id;
                result.thread_id = entry.thread_id;
                result.type = entry.type;
                result.timestamp = entry.timestamp;
                
                // Calculate relevance
                result.relevance_score = calculate_relevance_score(entry, query_terms, filters);
                result.recency_score = calculate_recency_score(entry.timestamp);
                result.engagement_score = static_cast<double>(entry.engagement_score) / 100.0;
                
                // Calculate final score
                result.final_score = result.relevance_score * 
                    filters.ranking_weights.at(SearchRankingFactor::RELEVANCE_SCORE) +
                    result.recency_score * 
                    filters.ranking_weights.at(SearchRankingFactor::RECENCY);
                
                if (result.is_relevant(filters.min_relevance_score)) {
                    results.push_back(result);
                }
            }
            
            lock.unlock();
            
            // Rank and limit results
            results = rank_search_results(results, filters);
            if (results.size() > max_results) {
                results.resize(max_results);
            }
            
            // Cache results
            if (config_.enable_query_caching) {
                cache_query_result(cache_key, results);
            }
            
            auto end_time = std::chrono::steady_clock::now();
            auto query_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            update_statistics(query, filters.scope, true, query_time);
            
            log_info("Search completed: " + std::to_string(results.size()) + " results for '" + query + "'");
            return results;
            
        } catch (const std::exception& e) {
            auto end_time = std::chrono::steady_clock::now();
            auto query_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            update_statistics(query, filters.scope, false, query_time);
            
            log_error("Search failed: " + std::string(e.what()));
            return {};
        }
    });
}

// Private helper methods
bool RealTimeSearchIndexer::index_message_internal(const std::string& message_id,
                                                   const std::string& chat_id,
                                                   const std::string& user_id,
                                                   const std::string& content,
                                                   SearchResultType type,
                                                   const std::string& thread_id) {
    std::unique_lock<std::shared_mutex> lock(index_mutex_);
    
    // Create index entry
    SearchIndexEntry entry;
    entry.message_id = message_id;
    entry.chat_id = chat_id;
    entry.user_id = user_id;
    entry.thread_id = thread_id;
    entry.type = type;
    entry.timestamp = std::chrono::system_clock::now();
    
    // Process content
    std::string normalized_content = normalize_text(content);
    entry.words = tokenize_text(normalized_content);
    
    if (config_.enable_stop_words_removal) {
        entry.words = remove_stop_words(entry.words);
    }
    
    if (config_.enable_stemming) {
        entry.stemmed_words = stem_words(entry.words);
    }
    
    // Calculate word frequencies
    for (const auto& word : entry.words) {
        entry.word_frequencies[word]++;
        entry.unique_words.insert(word);
        
        // Update global word-to-messages mapping
        word_to_messages_[word].insert(message_id);
        
        // Update document frequency
        if (entry.word_frequencies[word] == 1) { // First occurrence in this document
            document_frequencies_[word]++;
        }
    }
    
    entry.message_length = static_cast<uint32_t>(content.length());
    entry.engagement_score = 0; // Will be updated with reactions, etc.
    entry.is_important = false;
    
    // Generate semantic vector if enabled
    if (config_.enable_semantic_indexing) {
        entry.semantic_vector = generate_semantic_vector(content);
        entry.semantic_summary = content.substr(0, 100); // Simple summary
    }
    
    // Store entry
    message_index_[message_id] = entry;
    chat_to_messages_[chat_id].insert(message_id);
    user_to_messages_[user_id].insert(message_id);
    
    // Update statistics
    {
        std::lock_guard<std::shared_mutex> stats_lock(statistics_mutex_);
        statistics_.total_indexed_messages++;
        statistics_.total_indexed_words += entry.words.size();
        statistics_.unique_words_count = document_frequencies_.size();
        statistics_.last_update = std::chrono::system_clock::now();
    }
    
    return true;
}

std::vector<std::string> RealTimeSearchIndexer::tokenize_text(const std::string& text) {
    std::vector<std::string> tokens;
    std::string normalized = normalize_text(text);
    
    std::regex word_regex(R"(\b[a-zA-Z]+\b)");
    std::sregex_iterator iter(normalized.begin(), normalized.end(), word_regex);
    std::sregex_iterator end;
    
    for (; iter != end; ++iter) {
        std::string word = iter->str();
        if (word.length() >= config_.min_word_length && 
            word.length() <= config_.max_word_length) {
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);
            tokens.push_back(word);
        }
    }
    
    return tokens;
}

std::string RealTimeSearchIndexer::normalize_text(const std::string& text) {
    // Simple normalization - remove special characters, convert to lowercase
    std::string normalized;
    for (char c : text) {
        if (std::isalnum(c) || std::isspace(c)) {
            normalized += std::tolower(c);
        } else {
            normalized += ' ';
        }
    }
    return normalized;
}

std::vector<std::string> RealTimeSearchIndexer::stem_words(const std::vector<std::string>& words) {
    // Simple stemming - remove common suffixes
    std::vector<std::string> stemmed;
    std::vector<std::string> suffixes = {"ing", "ed", "er", "est", "ly", "tion", "ness"};
    
    for (const auto& word : words) {
        std::string stemmed_word = word;
        for (const auto& suffix : suffixes) {
            if (word.length() > suffix.length() + 2 && 
                word.substr(word.length() - suffix.length()) == suffix) {
                stemmed_word = word.substr(0, word.length() - suffix.length());
                break;
            }
        }
        stemmed.push_back(stemmed_word);
    }
    
    return stemmed;
}

std::vector<std::string> RealTimeSearchIndexer::remove_stop_words(const std::vector<std::string>& words) {
    std::vector<std::string> filtered;
    std::unordered_set<std::string> stop_word_set(config_.stop_words.begin(), config_.stop_words.end());
    
    for (const auto& word : words) {
        if (stop_word_set.find(word) == stop_word_set.end()) {
            filtered.push_back(word);
        }
    }
    
    return filtered;
}

std::vector<std::string> RealTimeSearchIndexer::process_query(const std::string& query) {
    return tokenize_text(query);
}

double RealTimeSearchIndexer::calculate_relevance_score(const SearchIndexEntry& entry,
                                                       const std::vector<std::string>& query_terms,
                                                       const SearchFilters& filters) {
    double score = 0.0;
    uint32_t total_docs = static_cast<uint32_t>(message_index_.size());
    
    for (const auto& term : query_terms) {
        double tf_idf = entry.calculate_tf_idf_score(term, document_frequencies_, total_docs);
        score += tf_idf;
    }
    
    return score / query_terms.size();
}

double RealTimeSearchIndexer::calculate_recency_score(const std::chrono::system_clock::time_point& timestamp) {
    auto now = std::chrono::system_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::hours>(now - timestamp);
    
    // Exponential decay: newer messages get higher scores
    double hours = static_cast<double>(age.count());
    return std::exp(-hours / (24.0 * 7.0)); // Decay over a week
}

std::vector<SearchResult> RealTimeSearchIndexer::rank_search_results(std::vector<SearchResult>& results,
                                                                    const SearchFilters& filters) {
    std::sort(results.begin(), results.end(), 
              [](const SearchResult& a, const SearchResult& b) {
                  return a.final_score > b.final_score;
              });
    return results;
}

bool RealTimeSearchIndexer::matches_filters(const SearchIndexEntry& entry, const SearchFilters& filters) {
    // Check type filters
    if (!filters.matches_result_type(entry.type)) {
        return false;
    }
    
    // Check time filters
    if (!filters.matches_time_range(entry.timestamp)) {
        return false;
    }
    
    // Check user filters
    if (!filters.from_users.empty()) {
        if (std::find(filters.from_users.begin(), filters.from_users.end(), 
                     entry.user_id) == filters.from_users.end()) {
            return false;
        }
    }
    
    if (!filters.exclude_users.empty()) {
        if (std::find(filters.exclude_users.begin(), filters.exclude_users.end(), 
                     entry.user_id) != filters.exclude_users.end()) {
            return false;
        }
    }
    
    // Check chat filters
    if (!filters.in_chats.empty()) {
        if (std::find(filters.in_chats.begin(), filters.in_chats.end(), 
                     entry.chat_id) == filters.in_chats.end()) {
            return false;
        }
    }
    
    // Check message length filters
    if (entry.message_length < filters.min_message_length || 
        entry.message_length > filters.max_message_length) {
        return false;
    }
    
    return true;
}

std::string RealTimeSearchIndexer::generate_cache_key(const std::string& query, const SearchFilters& filters) {
    // Create a hash of query + filters
    std::hash<std::string> hasher;
    std::string combined = query + filters.to_json().toStyledString();
    return "cache_" + std::to_string(hasher(combined));
}

std::string RealTimeSearchIndexer::generate_result_id() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    
    std::stringstream ss;
    ss << "res_";
    for (int i = 0; i < 32; ++i) {
        ss << std::hex << dis(gen);
    }
    return ss.str();
}

void RealTimeSearchIndexer::run_indexing_loop() {
    while (background_running_) {
        try {
            process_pending_updates();
            std::this_thread::sleep_for(config_.index_batch_interval);
        } catch (const std::exception& e) {
            log_error("Error in indexing loop: " + std::string(e.what()));
        }
    }
}

void RealTimeSearchIndexer::run_optimization_loop() {
    while (background_running_) {
        try {
            // Periodic index optimization
            std::this_thread::sleep_for(std::chrono::minutes(30));
            
            if (background_running_) {
                // Cleanup old cache entries
                clear_search_cache();
                
                // Update statistics
                {
                    std::lock_guard<std::shared_mutex> lock(statistics_mutex_);
                    statistics_.last_update = std::chrono::system_clock::now();
                }
            }
        } catch (const std::exception& e) {
            log_error("Error in optimization loop: " + std::string(e.what()));
        }
    }
}

void RealTimeSearchIndexer::process_pending_updates() {
    std::lock_guard<std::mutex> lock(pending_updates_mutex_);
    
    uint32_t processed = 0;
    while (!pending_updates_.empty() && processed < config_.max_batch_size) {
        auto update = pending_updates_.front();
        pending_updates_.pop();
        
        try {
            index_message_internal(
                update["message_id"].asString(),
                update["chat_id"].asString(),
                update["user_id"].asString(),
                update["content"].asString(),
                static_cast<SearchResultType>(update["type"].asInt()),
                update["thread_id"].asString()
            );
            processed++;
        } catch (const std::exception& e) {
            log_error("Failed to process pending update: " + std::string(e.what()));
        }
    }
    
    if (processed > 0) {
        log_info("Processed " + std::to_string(processed) + " pending index updates");
    }
}

void RealTimeSearchIndexer::update_statistics(const std::string& query, 
                                             SearchScope scope, 
                                             bool successful, 
                                             std::chrono::milliseconds query_time) {
    std::lock_guard<std::shared_mutex> lock(statistics_mutex_);
    statistics_.record_query(query, scope, successful);
    statistics_.update_query_time(query_time);
}

void RealTimeSearchIndexer::log_info(const std::string& message) {
    std::cout << "[INFO] RealTimeSearchIndexer: " << message << std::endl;
}

void RealTimeSearchIndexer::log_warning(const std::string& message) {
    std::cout << "[WARN] RealTimeSearchIndexer: " << message << std::endl;
}

void RealTimeSearchIndexer::log_error(const std::string& message) {
    std::cerr << "[ERROR] RealTimeSearchIndexer: " << message << std::endl;
}

// SearchQueryBuilder implementation
SearchQueryBuilder::SearchQueryBuilder() {
    filters_ = SearchFilters::default_filters();
}

SearchQueryBuilder& SearchQueryBuilder::with_text(const std::string& text) {
    filters_.query = text;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::from_user(const std::string& user_id) {
    filters_.from_users.push_back(user_id);
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::in_chat(const std::string& chat_id) {
    filters_.in_chats.push_back(chat_id);
    filters_.scope = SearchScope::CURRENT_CHAT;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::of_type(SearchResultType type) {
    filters_.include_types.push_back(type);
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::after(const std::chrono::system_clock::time_point& time) {
    filters_.start_time = time;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::before(const std::chrono::system_clock::time_point& time) {
    filters_.end_time = time;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::in_last_days(uint32_t days) {
    auto now = std::chrono::system_clock::now();
    filters_.start_time = now - std::chrono::hours(24 * days);
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::enable_semantic_search() {
    filters_.semantic_search_enabled = true;
    return *this;
}

SearchQueryBuilder& SearchQueryBuilder::enable_fuzzy_matching(double threshold) {
    filters_.fuzzy_matching_enabled = true;
    return *this;
}

SearchFilters SearchQueryBuilder::build() const {
    return filters_;
}

// SearchUtils implementation
std::string SearchUtils::highlight_matches(const std::string& content, 
                                          const std::vector<std::string>& terms) {
    std::string highlighted = content;
    for (const auto& term : terms) {
        std::regex term_regex("\\b" + term + "\\b", std::regex_constants::icase);
        highlighted = std::regex_replace(highlighted, term_regex, 
                                       "<mark>$&</mark>");
    }
    return highlighted;
}

std::vector<std::string> SearchUtils::extract_keywords(const std::string& text, uint32_t max_keywords) {
    // Simple keyword extraction - find most frequent non-stop words
    std::unordered_map<std::string, uint32_t> word_freq;
    std::regex word_regex(R"(\b[a-zA-Z]+\b)");
    std::sregex_iterator iter(text.begin(), text.end(), word_regex);
    std::sregex_iterator end;
    
    std::unordered_set<std::string> stop_words = {"the", "a", "an", "and", "or", "but", "in", "on", "at"};
    
    for (; iter != end; ++iter) {
        std::string word = iter->str();
        std::transform(word.begin(), word.end(), word.begin(), ::tolower);
        if (stop_words.find(word) == stop_words.end() && word.length() > 2) {
            word_freq[word]++;
        }
    }
    
    std::vector<std::pair<std::string, uint32_t>> sorted_words(word_freq.begin(), word_freq.end());
    std::sort(sorted_words.begin(), sorted_words.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    
    std::vector<std::string> keywords;
    for (size_t i = 0; i < std::min(static_cast<size_t>(max_keywords), sorted_words.size()); ++i) {
        keywords.push_back(sorted_words[i].first);
    }
    
    return keywords;
}

std::string SearchUtils::clean_search_query(const std::string& query) {
    std::string cleaned;
    for (char c : query) {
        if (std::isalnum(c) || std::isspace(c) || c == '"' || c == '\'' || c == '-') {
            cleaned += c;
        }
    }
    return cleaned;
}

bool SearchUtils::is_semantic_query(const std::string& query) {
    // Heuristic: semantic queries tend to be longer and more descriptive
    std::vector<std::string> semantic_indicators = {
        "how", "what", "why", "when", "where", "who", "explain", "describe", "tell me about"
    };
    
    std::string lower_query = query;
    std::transform(lower_query.begin(), lower_query.end(), lower_query.begin(), ::tolower);
    
    for (const auto& indicator : semantic_indicators) {
        if (lower_query.find(indicator) != std::string::npos) {
            return true;
        }
    }
    
    // Also consider length and structure
    return query.length() > 20 && std::count(query.begin(), query.end(), ' ') > 3;
}

std::string SearchUtils::format_search_summary(const std::vector<SearchResult>& results, 
                                              const std::string& query) {
    if (results.empty()) {
        return "No results found for '" + query + "'";
    }
    
    std::string summary = "Found " + std::to_string(results.size()) + " result";
    if (results.size() != 1) summary += "s";
    summary += " for '" + query + "'";
    
    if (results.size() > 1) {
        auto total_score = std::accumulate(results.begin(), results.end(), 0.0,
                                         [](double sum, const SearchResult& r) { 
                                             return sum + r.final_score; 
                                         });
        double avg_score = total_score / results.size();
        summary += " (avg relevance: " + std::to_string(static_cast<int>(avg_score * 100)) + "%)";
    }
    
    return summary;
}

} // namespace sonet::messaging::search
