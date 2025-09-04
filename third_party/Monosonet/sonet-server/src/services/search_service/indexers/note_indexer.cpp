/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Implementation of the real-time note indexer for Twitter-scale search operations.
 * This processes millions of notes per second with intelligent content analysis,
 * trending detection, and engagement tracking.
 */

#include "note_indexer.h"
#include <regex>
#include <algorithm>
#include <sstream>
#include <random>
#include <chrono>
#include <future>
#include <unordered_set>
#include <queue>
#include <thread>
#include <condition_variable>
#include <iomanip>

namespace sonet {
namespace search_service {
namespace indexers {

// Content analysis patterns
const std::vector<std::string> ContentAnalyzer::SPAM_PATTERNS = {
    R"(\b(?:click here|buy now|limited time|act fast|guaranteed|free money|earn \$\d+)\b)",
    R"(\b(?:viagra|cialis|casino|lottery|winner|congratulations)\b)",
    R"((?:https?://)?(?:bit\.ly|tinyurl|t\.co)/[a-zA-Z0-9]{6,})",
    R"(\b\d{3}-\d{3}-\d{4}\b)",  // Phone numbers
    R"(\$\d+(?:\.\d{2})?(?:\s*(?:per|/)\s*(?:hour|day|week|month))?)",  // Money amounts
};

const std::vector<std::string> ContentAnalyzer::NSFW_PATTERNS = {
    R"(\b(?:porn|xxx|nude|naked|sex|adult|18\+)\b)",
    R"(\b(?:fuck|shit|damn|hell|bitch|asshole)\b)",
    R"(\b(?:onlyfans|pornhub|xhamster|redtube)\b)",
};

const std::vector<std::string> ContentAnalyzer::SENSITIVE_PATTERNS = {
    R"(\b(?:suicide|depression|self-harm|cutting|overdose)\b)",
    R"(\b(?:terrorism|bomb|weapon|gun|violence)\b)",
    R"(\b(?:hate|racist|nazi|fascist|supremacist)\b)",
};

const std::unordered_map<std::string, std::vector<std::string>> ContentAnalyzer::TOPIC_KEYWORDS = {
    {"technology", {"AI", "machine learning", "blockchain", "cryptocurrency", "programming", "software", "tech", "innovation"}},
    {"sports", {"football", "basketball", "soccer", "baseball", "tennis", "olympics", "championship", "game", "match"}},
    {"politics", {"election", "government", "policy", "democracy", "vote", "politician", "congress", "senate"}},
    {"entertainment", {"movie", "music", "celebrity", "Hollywood", "Netflix", "streaming", "concert", "album"}},
    {"science", {"research", "study", "discovery", "experiment", "physics", "chemistry", "biology", "space"}},
    {"health", {"fitness", "workout", "diet", "nutrition", "medical", "doctor", "hospital", "medicine"}},
    {"business", {"startup", "entrepreneur", "investment", "stock", "market", "economy", "finance", "company"}},
    {"travel", {"vacation", "trip", "tourism", "hotel", "flight", "destination", "adventure", "explore"}},
    {"food", {"recipe", "cooking", "restaurant", "chef", "cuisine", "meal", "dinner", "lunch"}},
    {"education", {"university", "college", "student", "teacher", "learning", "course", "degree", "scholarship"}}
};

/**
 * NoteDocument implementation
 */
nlohmann::json NoteDocument::to_elasticsearch_document() const {
    nlohmann::json doc = {
        {"id", id},
        {"user_id", user_id},
        {"username", username},
        {"display_name", display_name},
        {"content", content},
        {"hashtags", hashtags},
        {"mentions", mentions},
        {"media_urls", media_urls},
        {"language", language},
        {"created_at", std::chrono::duration_cast<std::chrono::milliseconds>(created_at.time_since_epoch()).count()},
        {"updated_at", std::chrono::duration_cast<std::chrono::milliseconds>(updated_at.time_since_epoch()).count()},
        {"is_reply", is_reply},
        {"reply_to_id", reply_to_id},
        {"is_renote", is_renote},
        {"renote_of_id", renote_of_id},
        {"thread_id", thread_id},
        {"visibility", visibility},
        {"nsfw", nsfw},
        {"sensitive", sensitive}
    };
    
    // Add location if present
    if (location.has_value()) {
        doc["location"] = {
            {"lat", location->first},
            {"lon", location->second}
        };
    }
    
    if (!place_name.empty()) {
        doc["place_name"] = place_name;
    }
    
    // Add metrics
    doc["metrics"] = {
        {"likes_count", metrics.likes_count},
        {"renotes_count", metrics.renotes_count},
        {"replies_count", metrics.replies_count},
        {"views_count", metrics.views_count},
        {"engagement_score", metrics.engagement_score},
        {"virality_score", metrics.virality_score},
        {"trending_score", metrics.trending_score}
    };
    
    // Add user metrics
    doc["user_metrics"] = {
        {"followers_count", user_metrics.followers_count},
        {"following_count", user_metrics.following_count},
        {"reputation_score", user_metrics.reputation_score},
        {"verification_level", user_metrics.verification_level}
    };
    
    // Add boost factors
    doc["boost_factors"] = {
        {"recency_boost", boost_factors.recency_boost},
        {"engagement_boost", boost_factors.engagement_boost},
        {"author_boost", boost_factors.author_boost},
        {"content_quality_boost", boost_factors.content_quality_boost}
    };
    
    // Add indexing metadata
    doc["indexing_metadata"] = {
        {"indexed_at", std::chrono::duration_cast<std::chrono::milliseconds>(indexing_metadata.indexed_at.time_since_epoch()).count()},
        {"version", indexing_metadata.version},
        {"source", indexing_metadata.source}
    };
    
    return doc;
}

NoteDocument NoteDocument::from_json(const nlohmann::json& json) {
    NoteDocument note;
    
    note.id = json.value("id", "");
    note.user_id = json.value("user_id", "");
    note.username = json.value("username", "");
    note.display_name = json.value("display_name", "");
    note.content = json.value("content", "");
    note.language = json.value("language", "en");
    note.visibility = json.value("visibility", "public");
    note.nsfw = json.value("nsfw", false);
    note.sensitive = json.value("sensitive", false);
    note.is_reply = json.value("is_reply", false);
    note.reply_to_id = json.value("reply_to_id", "");
    note.is_renote = json.value("is_renote", false);
    note.renote_of_id = json.value("renote_of_id", "");
    note.thread_id = json.value("thread_id", "");
    note.place_name = json.value("place_name", "");
    
    // Parse timestamps
    if (json.contains("created_at")) {
        auto timestamp = json["created_at"].get<long long>();
        note.created_at = std::chrono::system_clock::time_point{std::chrono::milliseconds{timestamp}};
    }
    
    if (json.contains("updated_at")) {
        auto timestamp = json["updated_at"].get<long long>();
        note.updated_at = std::chrono::system_clock::time_point{std::chrono::milliseconds{timestamp}};
    }
    
    // Parse arrays
    if (json.contains("hashtags") && json["hashtags"].is_array()) {
        note.hashtags = json["hashtags"].get<std::vector<std::string>>();
    }
    
    if (json.contains("mentions") && json["mentions"].is_array()) {
        note.mentions = json["mentions"].get<std::vector<std::string>>();
    }
    
    if (json.contains("media_urls") && json["media_urls"].is_array()) {
        note.media_urls = json["media_urls"].get<std::vector<std::string>>();
    }
    
    // Parse location
    if (json.contains("location") && json["location"].contains("lat") && json["location"].contains("lon")) {
        note.location = std::make_pair(
            json["location"]["lat"].get<double>(),
            json["location"]["lon"].get<double>()
        );
    }
    
    // Parse metrics
    if (json.contains("metrics")) {
        const auto& metrics = json["metrics"];
        note.metrics.likes_count = metrics.value("likes_count", 0);
        note.metrics.renotes_count = metrics.value("renotes_count", 0);
        note.metrics.replies_count = metrics.value("replies_count", 0);
        note.metrics.views_count = metrics.value("views_count", 0L);
        note.metrics.engagement_score = metrics.value("engagement_score", 0.0f);
        note.metrics.virality_score = metrics.value("virality_score", 0.0f);
        note.metrics.trending_score = metrics.value("trending_score", 0.0f);
    }
    
    // Parse user metrics
    if (json.contains("user_metrics")) {
        const auto& user_metrics = json["user_metrics"];
        note.user_metrics.followers_count = user_metrics.value("followers_count", 0);
        note.user_metrics.following_count = user_metrics.value("following_count", 0);
        note.user_metrics.reputation_score = user_metrics.value("reputation_score", 0.0f);
        note.user_metrics.verification_level = user_metrics.value("verification_level", "none");
    }
    
    // Auto-extract content features if not provided
    if (note.hashtags.empty()) {
        note.hashtags = extract_hashtags(note.content);
    }
    
    if (note.mentions.empty()) {
        note.mentions = extract_mentions(note.content);
    }
    
    if (note.language.empty() || note.language == "unknown") {
        note.language = detect_language(note.content);
    }
    
    // Calculate scores
    note.metrics.engagement_score = note.calculate_engagement_score();
    note.metrics.virality_score = note.calculate_virality_score();
    note.metrics.trending_score = note.calculate_trending_score();
    
    // Set indexing metadata
    note.indexing_metadata.indexed_at = std::chrono::system_clock::now();
    note.indexing_metadata.version = 1;
    note.indexing_metadata.source = "api";
    
    return note;
}

float NoteDocument::calculate_engagement_score() const {
    if (metrics.views_count == 0) return 0.0f;
    
    // Calculate engagement rate
    float total_engagements = static_cast<float>(metrics.likes_count + metrics.renotes_count + metrics.replies_count);
    float engagement_rate = total_engagements / static_cast<float>(metrics.views_count);
    
    // Apply logarithmic scaling to prevent outliers
    float scaled_rate = std::log1p(engagement_rate * 1000.0f) / std::log(1001.0f);
    
    // Factor in absolute engagement numbers
    float absolute_factor = std::log1p(total_engagements) / std::log(10001.0f);
    
    // Combine with user reputation
    float user_factor = std::min(1.0f, user_metrics.reputation_score / 100.0f);
    
    return std::min(1.0f, scaled_rate * 0.6f + absolute_factor * 0.3f + user_factor * 0.1f);
}

float NoteDocument::calculate_virality_score() const {
    auto now = std::chrono::system_clock::now();
    auto age_hours = std::chrono::duration_cast<std::chrono::hours>(now - created_at).count();
    
    if (age_hours == 0) age_hours = 1;  // Prevent division by zero
    
    // Calculate engagement velocity (engagements per hour)
    float total_engagements = static_cast<float>(metrics.likes_count + metrics.renotes_count + metrics.replies_count);
    float velocity = total_engagements / static_cast<float>(age_hours);
    
    // Factor in renote ratio (viral content gets shared more)
    float renote_ratio = metrics.renotes_count > 0 ? 
        static_cast<float>(metrics.renotes_count) / total_engagements : 0.0f;
    
    // Factor in user reach potential
    float reach_factor = std::log1p(static_cast<float>(user_metrics.followers_count)) / std::log(1000001.0f);
    
    // Combine factors
    float velocity_score = std::log1p(velocity) / std::log(1001.0f);
    float viral_score = velocity_score * 0.5f + renote_ratio * 0.3f + reach_factor * 0.2f;
    
    return std::min(1.0f, viral_score);
}

float NoteDocument::calculate_trending_score() const {
    // Trending combines recency, virality, and engagement
    auto now = std::chrono::system_clock::now();
    auto age_hours = std::chrono::duration_cast<std::chrono::hours>(now - created_at).count();
    
    // Recency factor (exponential decay)
    float recency_factor = std::exp(-static_cast<float>(age_hours) / 24.0f);  // Half-life of 24 hours
    
    // Get engagement and virality scores
    float engagement_score = calculate_engagement_score();
    float virality_score = calculate_virality_score();
    
    // Factor in hashtag popularity (simplified)
    float hashtag_factor = hashtags.empty() ? 0.5f : 0.8f;
    
    // Combine all factors
    return recency_factor * 0.4f + engagement_score * 0.3f + virality_score * 0.2f + hashtag_factor * 0.1f;
}

std::vector<std::string> NoteDocument::extract_hashtags(const std::string& content) {
    std::vector<std::string> hashtags;
    std::regex hashtag_regex(R"(#([a-zA-Z0-9_\u00C0-\u017F\u0400-\u04FF\u4e00-\u9fff]+))");
    std::sregex_iterator iter(content.begin(), content.end(), hashtag_regex);
    std::sregex_iterator end;
    
    for (; iter != end; ++iter) {
        std::string hashtag = iter->str(1);  // Get the capture group (without #)
        
        // Convert to lowercase for consistency
        std::transform(hashtag.begin(), hashtag.end(), hashtag.begin(), ::tolower);
        
        // Avoid duplicates
        if (std::find(hashtags.begin(), hashtags.end(), hashtag) == hashtags.end()) {
            hashtags.push_back(hashtag);
        }
    }
    
    return hashtags;
}

std::vector<std::string> NoteDocument::extract_mentions(const std::string& content) {
    std::vector<std::string> mentions;
    std::regex mention_regex(R"(@([a-zA-Z0-9_]+))");
    std::sregex_iterator iter(content.begin(), content.end(), mention_regex);
    std::sregex_iterator end;
    
    for (; iter != end; ++iter) {
        std::string mention = iter->str(1);  // Get the capture group (without @)
        
        // Convert to lowercase for consistency
        std::transform(mention.begin(), mention.end(), mention.begin(), ::tolower);
        
        // Avoid duplicates
        if (std::find(mentions.begin(), mentions.end(), mention) == mentions.end()) {
            mentions.push_back(mention);
        }
    }
    
    return mentions;
}

std::string NoteDocument::detect_language(const std::string& content) {
    // Simplified language detection based on character sets and common words
    
    // Check for non-Latin scripts
    bool has_cyrillic = false;
    bool has_cjk = false;
    bool has_arabic = false;
    
    for (char32_t c : content) {
        if (c >= 0x0400 && c <= 0x04FF) has_cyrillic = true;
        if (c >= 0x4E00 && c <= 0x9FFF) has_cjk = true;
        if (c >= 0x0600 && c <= 0x06FF) has_arabic = true;
    }
    
    if (has_cyrillic) return "ru";
    if (has_cjk) return "zh";
    if (has_arabic) return "ar";
    
    // Check for common words in major languages
    std::string lower_content = content;
    std::transform(lower_content.begin(), lower_content.end(), lower_content.begin(), ::tolower);
    
    // English indicators
    std::vector<std::string> english_words = {"the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by"};
    int english_count = 0;
    for (const auto& word : english_words) {
        if (lower_content.find(" " + word + " ") != std::string::npos) english_count++;
    }
    
    // Spanish indicators
    std::vector<std::string> spanish_words = {"el", "la", "de", "que", "y", "en", "un", "es", "se", "no", "te", "lo"};
    int spanish_count = 0;
    for (const auto& word : spanish_words) {
        if (lower_content.find(" " + word + " ") != std::string::npos) spanish_count++;
    }
    
    // French indicators
    std::vector<std::string> french_words = {"le", "de", "et", "à", "un", "il", "être", "et", "en", "avoir", "que", "pour"};
    int french_count = 0;
    for (const auto& word : french_words) {
        if (lower_content.find(" " + word + " ") != std::string::npos) french_count++;
    }
    
    if (spanish_count > english_count && spanish_count > french_count) return "es";
    if (french_count > english_count && french_count > spanish_count) return "fr";
    
    return "en";  // Default to English
}

float NoteDocument::calculate_content_quality_score() const {
    float score = 0.5f;  // Base score
    
    // Length factor
    size_t content_length = content.length();
    if (content_length < 10) {
        score -= 0.3f;  // Very short content
    } else if (content_length > 280 && content_length < 1000) {
        score += 0.2f;  // Good length content
    } else if (content_length > 2000) {
        score -= 0.1f;  // Very long content might be spammy
    }
    
    // Check for proper capitalization
    bool has_capital = std::any_of(content.begin(), content.end(), ::isupper);
    if (has_capital) score += 0.1f;
    
    // Check for excessive caps
    int caps_count = std::count_if(content.begin(), content.end(), ::isupper);
    float caps_ratio = static_cast<float>(caps_count) / content_length;
    if (caps_ratio > 0.5f) score -= 0.3f;  // Too many caps
    
    // Check for excessive punctuation
    int punct_count = std::count_if(content.begin(), content.end(), ::ispunct);
    float punct_ratio = static_cast<float>(punct_count) / content_length;
    if (punct_ratio > 0.3f) score -= 0.2f;
    
    // Check for URLs (can be positive or negative)
    size_t url_count = 0;
    std::regex url_regex(R"(https?://[^\s]+)");
    std::sregex_iterator iter(content.begin(), content.end(), url_regex);
    std::sregex_iterator end;
    url_count = std::distance(iter, end);
    
    if (url_count == 1) score += 0.1f;  // Single URL is good
    if (url_count > 3) score -= 0.3f;   // Too many URLs is spammy
    
    // Check for hashtag spam
    if (hashtags.size() > 5) score -= 0.2f;
    if (hashtags.size() > 10) score -= 0.3f;
    
    return std::max(0.0f, std::min(1.0f, score));
}

bool NoteDocument::should_be_indexed() const {
    // Don't index private or deleted content
    if (visibility == "private" || visibility == "deleted") return false;
    
    // Don't index very short content (likely spam or low quality)
    if (content.length() < 3) return false;
    
    // Don't index content that's too spammy
    float quality_score = calculate_content_quality_score();
    if (quality_score < 0.2f) return false;
    
    return true;
}

std::string NoteDocument::get_routing_key() const {
    // Use user_id for routing to ensure user's notes are co-located
    return user_id;
}

/**
 * IndexingConfig implementation
 */
IndexingConfig IndexingConfig::production_config() {
    IndexingConfig config;
    config.batch_size = 5000;
    config.batch_timeout = std::chrono::milliseconds{2000};
    config.max_concurrent_batches = 10;
    config.max_retry_attempts = 3;
    config.retry_delay = std::chrono::milliseconds{1000};
    config.enable_real_time_indexing = true;
    config.real_time_delay = std::chrono::milliseconds{50};
    config.max_queue_size = 500000;
    config.memory_warning_threshold_mb = 1000;
    config.memory_limit_threshold_mb = 2000;
    return config;
}

IndexingConfig IndexingConfig::development_config() {
    IndexingConfig config;
    config.batch_size = 100;
    config.batch_timeout = std::chrono::milliseconds{5000};
    config.max_concurrent_batches = 2;
    config.enable_real_time_indexing = true;
    config.real_time_delay = std::chrono::milliseconds{1000};
    config.max_queue_size = 10000;
    config.memory_warning_threshold_mb = 100;
    config.memory_limit_threshold_mb = 200;
    return config;
}

bool IndexingConfig::is_valid() const {
    return batch_size > 0 &&
           batch_timeout.count() > 0 &&
           max_concurrent_batches > 0 &&
           max_retry_attempts >= 0 &&
           max_queue_size > 0;
}

/**
 * IndexingMetrics implementation
 */
nlohmann::json IndexingMetrics::to_json() const {
    auto now = std::chrono::system_clock::now();
    auto uptime_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - last_reset).count();
    
    return nlohmann::json{
        {"notes_processed", notes_processed.load()},
        {"notes_indexed", notes_indexed.load()},
        {"notes_updated", notes_updated.load()},
        {"notes_deleted", notes_deleted.load()},
        {"notes_skipped", notes_skipped.load()},
        {"notes_failed", notes_failed.load()},
        {"batches_processed", batches_processed.load()},
        {"batches_failed", batches_failed.load()},
        {"retries_attempted", retries_attempted.load()},
        {"total_processing_time_ms", total_processing_time_ms.load()},
        {"total_indexing_time_ms", total_indexing_time_ms.load()},
        {"total_queue_time_ms", total_queue_time_ms.load()},
        {"content_analysis_time_ms", content_analysis_time_ms.load()},
        {"language_detection_time_ms", language_detection_time_ms.load()},
        {"scoring_time_ms", scoring_time_ms.load()},
        {"current_queue_size", current_queue_size.load()},
        {"current_memory_usage_mb", current_memory_usage_mb.load()},
        {"active_worker_threads", active_worker_threads.load()},
        {"processing_rate_per_second", get_processing_rate()},
        {"success_rate", get_success_rate()},
        {"average_processing_time_ms", get_average_processing_time_ms()},
        {"uptime_seconds", uptime_seconds}
    };
}

void IndexingMetrics::reset() {
    notes_processed = 0;
    notes_indexed = 0;
    notes_updated = 0;
    notes_deleted = 0;
    notes_skipped = 0;
    notes_failed = 0;
    batches_processed = 0;
    batches_failed = 0;
    retries_attempted = 0;
    total_processing_time_ms = 0;
    total_indexing_time_ms = 0;
    total_queue_time_ms = 0;
    content_analysis_time_ms = 0;
    language_detection_time_ms = 0;
    scoring_time_ms = 0;
    current_queue_size = 0;
    current_memory_usage_mb = 0;
    active_worker_threads = 0;
    last_reset = std::chrono::system_clock::now();
}

double IndexingMetrics::get_processing_rate() const {
    auto now = std::chrono::system_clock::now();
    auto duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - last_reset).count();
    
    if (duration_seconds == 0) return 0.0;
    
    return static_cast<double>(notes_processed.load()) / duration_seconds;
}

double IndexingMetrics::get_success_rate() const {
    long total = notes_processed.load();
    if (total == 0) return 0.0;
    
    long successful = notes_indexed.load() + notes_updated.load() + notes_deleted.load();
    return static_cast<double>(successful) / total;
}

double IndexingMetrics::get_average_processing_time_ms() const {
    long total = notes_processed.load();
    if (total == 0) return 0.0;
    
    return static_cast<double>(total_processing_time_ms.load()) / total;
}

bool IndexingMetrics::is_memory_critical(const IndexingConfig& config) const {
    return current_memory_usage_mb.load() >= config.memory_limit_threshold_mb;
}

/**
 * IndexingTask implementation
 */
bool IndexingTask::should_retry(const IndexingConfig& config) const {
    return retry_count < config.max_retry_attempts;
}

int IndexingTask::calculate_priority(const NoteDocument& note) {
    int priority = 0;
    
    // Higher priority for verified users
    if (note.user_metrics.verification_level != "none") {
        priority += 10;
    }
    
    // Higher priority for high-engagement content
    if (note.metrics.engagement_score > 0.7f) {
        priority += 5;
    }
    
    // Higher priority for viral content
    if (note.metrics.virality_score > 0.8f) {
        priority += 8;
    }
    
    // Higher priority for recent content
    auto now = std::chrono::system_clock::now();
    auto age_minutes = std::chrono::duration_cast<std::chrono::minutes>(now - note.created_at).count();
    if (age_minutes < 10) {
        priority += 3;
    }
    
    // Higher priority for trending hashtags (simplified)
    if (!note.hashtags.empty()) {
        priority += 2;
    }
    
    return priority;
}

std::chrono::milliseconds IndexingTask::get_retry_delay(const IndexingConfig& config) const {
    // Exponential backoff with jitter
    auto base_delay = config.retry_delay;
    auto backoff_factor = std::pow(2, retry_count);
    auto delay_ms = static_cast<long>(base_delay.count() * backoff_factor);
    
    // Add jitter (±25%)
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> jitter(0.75, 1.25);
    delay_ms = static_cast<long>(delay_ms * jitter(gen));
    
    return std::chrono::milliseconds{delay_ms};
}

/**
 * ContentAnalyzer implementation
 */
ContentAnalyzer::AnalysisResult ContentAnalyzer::analyze_content(const std::string& content) {
    AnalysisResult result;
    
    // Extract hashtags and mentions
    result.hashtags = NoteDocument::extract_hashtags(content);
    result.mentions = NoteDocument::extract_mentions(content);
    
    // Extract media URLs
    result.media_urls = extract_media_urls(content);
    
    // Detect language
    result.language = detect_language_advanced(content);
    
    // Calculate quality and spam scores
    result.content_quality_score = calculate_content_quality(content);
    result.spam_score = calculate_spam_score(content, "");
    
    // Detect NSFW and sensitive content
    result.is_nsfw = is_nsfw_content(content);
    result.is_sensitive = is_sensitive_content(content);
    
    // Extract topics
    result.topics = extract_topics(content);
    
    // Analyze sentiment
    result.sentiment = analyze_sentiment(content);
    
    return result;
}

std::vector<std::string> ContentAnalyzer::extract_media_urls(const std::string& content) {
    std::vector<std::string> media_urls;
    
    // Image extensions
    std::regex image_regex(R"(https?://[^\s]+\.(?:jpg|jpeg|png|gif|webp|svg)(?:\?[^\s]*)?)", std::regex_constants::icase);
    std::sregex_iterator iter(content.begin(), content.end(), image_regex);
    std::sregex_iterator end;
    
    for (; iter != end; ++iter) {
        media_urls.push_back(iter->str());
    }
    
    // Video extensions
    std::regex video_regex(R"(https?://[^\s]+\.(?:mp4|webm|mov|avi|mkv)(?:\?[^\s]*)?)", std::regex_constants::icase);
    iter = std::sregex_iterator(content.begin(), content.end(), video_regex);
    
    for (; iter != end; ++iter) {
        media_urls.push_back(iter->str());
    }
    
    // Known media platforms
    std::regex platform_regex(R"(https?://(?:www\.)?(?:youtube\.com/watch|youtu\.be|twitter\.com/[^/]+/status|instagram\.com/p/|tiktok\.com/@[^/]+/video)[^\s]*)", std::regex_constants::icase);
    iter = std::sregex_iterator(content.begin(), content.end(), platform_regex);
    
    for (; iter != end; ++iter) {
        media_urls.push_back(iter->str());
    }
    
    return media_urls;
}

std::string ContentAnalyzer::detect_language_advanced(const std::string& content) {
    // This is a simplified implementation
    // In production, you'd use a proper language detection library
    return NoteDocument::detect_language(content);
}

float ContentAnalyzer::calculate_content_quality(const std::string& content) {
    // This uses the same logic as NoteDocument::calculate_content_quality_score
    // In a real implementation, you might extract this to a shared utility
    
    float score = 0.5f;  // Base score
    
    size_t content_length = content.length();
    if (content_length < 10) {
        score -= 0.3f;
    } else if (content_length > 50 && content_length < 500) {
        score += 0.2f;
    } else if (content_length > 2000) {
        score -= 0.1f;
    }
    
    // Check for proper grammar indicators
    bool has_periods = content.find('.') != std::string::npos;
    bool has_commas = content.find(',') != std::string::npos;
    if (has_periods || has_commas) score += 0.1f;
    
    // Check for excessive repetition
    std::unordered_map<std::string, int> word_count;
    std::istringstream iss(content);
    std::string word;
    while (iss >> word) {
        word_count[word]++;
    }
    
    bool has_repetition = false;
    for (const auto& pair : word_count) {
        if (pair.second > 5) {  // Same word repeated more than 5 times
            has_repetition = true;
            break;
        }
    }
    
    if (has_repetition) score -= 0.3f;
    
    return std::max(0.0f, std::min(1.0f, score));
}

float ContentAnalyzer::calculate_spam_score(const std::string& content, const std::string& user_id) {
    float spam_score = 0.0f;
    
    // Check against spam patterns
    for (const auto& pattern : SPAM_PATTERNS) {
        std::regex spam_regex(pattern, std::regex_constants::icase);
        if (std::regex_search(content, spam_regex)) {
            spam_score += 0.3f;
        }
    }
    
    // Check for excessive URLs
    std::regex url_regex(R"(https?://[^\s]+)");
    auto url_count = std::distance(
        std::sregex_iterator(content.begin(), content.end(), url_regex),
        std::sregex_iterator()
    );
    
    if (url_count > 3) spam_score += 0.4f;
    
    // Check for excessive capitalization
    int caps_count = std::count_if(content.begin(), content.end(), ::isupper);
    float caps_ratio = static_cast<float>(caps_count) / content.length();
    if (caps_ratio > 0.7f) spam_score += 0.2f;
    
    // Check for excessive exclamation marks
    int exclamation_count = std::count(content.begin(), content.end(), '!');
    if (exclamation_count > 5) spam_score += 0.1f;
    
    return std::min(1.0f, spam_score);
}

bool ContentAnalyzer::is_nsfw_content(const std::string& content) {
    for (const auto& pattern : NSFW_PATTERNS) {
        std::regex nsfw_regex(pattern, std::regex_constants::icase);
        if (std::regex_search(content, nsfw_regex)) {
            return true;
        }
    }
    return false;
}

bool ContentAnalyzer::is_sensitive_content(const std::string& content) {
    for (const auto& pattern : SENSITIVE_PATTERNS) {
        std::regex sensitive_regex(pattern, std::regex_constants::icase);
        if (std::regex_search(content, sensitive_regex)) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> ContentAnalyzer::extract_topics(const std::string& content) {
    std::vector<std::string> topics;
    std::string lower_content = content;
    std::transform(lower_content.begin(), lower_content.end(), lower_content.begin(), ::tolower);
    
    for (const auto& topic_pair : TOPIC_KEYWORDS) {
        const std::string& topic = topic_pair.first;
        const std::vector<std::string>& keywords = topic_pair.second;
        
        int keyword_matches = 0;
        for (const auto& keyword : keywords) {
            std::string lower_keyword = keyword;
            std::transform(lower_keyword.begin(), lower_keyword.end(), lower_keyword.begin(), ::tolower);
            
            if (lower_content.find(lower_keyword) != std::string::npos) {
                keyword_matches++;
            }
        }
        
        // If we found multiple keywords for this topic, include it
        if (keyword_matches >= 2) {
            topics.push_back(topic);
        }
    }
    
    return topics;
}

std::string ContentAnalyzer::analyze_sentiment(const std::string& content) {
    // Simplified sentiment analysis
    std::vector<std::string> positive_words = {
        "good", "great", "awesome", "amazing", "wonderful", "excellent", "fantastic", "love", "happy", "excited"
    };
    
    std::vector<std::string> negative_words = {
        "bad", "terrible", "awful", "horrible", "hate", "angry", "sad", "disappointed", "frustrated", "annoying"
    };
    
    std::string lower_content = content;
    std::transform(lower_content.begin(), lower_content.end(), lower_content.begin(), ::tolower);
    
    int positive_count = 0;
    int negative_count = 0;
    
    for (const auto& word : positive_words) {
        if (lower_content.find(word) != std::string::npos) {
            positive_count++;
        }
    }
    
    for (const auto& word : negative_words) {
        if (lower_content.find(word) != std::string::npos) {
            negative_count++;
        }
    }
    
    if (positive_count > negative_count) return "positive";
    if (negative_count > positive_count) return "negative";
    return "neutral";
}

std::string ContentAnalyzer::normalize_content(const std::string& content) {
    std::string normalized = content;
    
    // Remove excessive whitespace
    std::regex whitespace_regex(R"(\s+)");
    normalized = std::regex_replace(normalized, whitespace_regex, " ");
    
    // Trim leading/trailing whitespace
    size_t start = normalized.find_first_not_of(" \t\n\r");
    if (start != std::string::npos) {
        normalized = normalized.substr(start);
    }
    
    size_t end = normalized.find_last_not_of(" \t\n\r");
    if (end != std::string::npos) {
        normalized = normalized.substr(0, end + 1);
    }
    
    return normalized;
}

/**
 * NoteIndexer::Impl - Private implementation
 */
struct NoteIndexer::Impl {
    std::shared_ptr<engines::ElasticsearchEngine> engine;
    IndexingConfig config;
    
    // State management
    std::atomic<bool> running{false};
    std::atomic<bool> paused{false};
    std::atomic<bool> debug_mode{false};
    
    // Task queue
    std::priority_queue<IndexingTask, std::vector<IndexingTask>, TaskPriorityComparator> task_queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    
    // Worker threads
    std::vector<std::thread> worker_threads;
    
    // Metrics
    mutable std::mutex metrics_mutex;
    IndexingMetrics metrics;
    
    // Failed operations log
    std::vector<nlohmann::json> failed_operations;
    std::mutex failed_operations_mutex;
    static constexpr size_t MAX_FAILED_OPERATIONS = 1000;
    
    Impl(std::shared_ptr<engines::ElasticsearchEngine> eng, const IndexingConfig& cfg)
        : engine(std::move(eng)), config(cfg) {
        metrics.last_reset = std::chrono::system_clock::now();
    }
    
    ~Impl() {
        stop();
    }
    
    void stop() {
        running = false;
        queue_cv.notify_all();
        
        for (auto& thread : worker_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        
        worker_threads.clear();
    }
};

/**
 * NoteIndexer implementation
 */
NoteIndexer::NoteIndexer(std::shared_ptr<engines::ElasticsearchEngine> engine, const IndexingConfig& config)
    : pimpl_(std::make_unique<Impl>(std::move(engine), config)) {
}

NoteIndexer::~NoteIndexer() = default;

std::future<bool> NoteIndexer::start() {
    return std::async(std::launch::async, [this]() {
        if (pimpl_->running.exchange(true)) {
            return false;  // Already running
        }
        
        // Start worker threads
        int thread_count = std::max(1, static_cast<int>(std::thread::hardware_concurrency()) / 2);
        pimpl_->worker_threads.reserve(thread_count);
        
        for (int i = 0; i < thread_count; ++i) {
            pimpl_->worker_threads.emplace_back([this] { indexing_worker_loop(); });
        }
        
        return true;
    });
}

void NoteIndexer::stop() {
    pimpl_->stop();
}

bool NoteIndexer::is_running() const {
    return pimpl_->running.load();
}

bool NoteIndexer::queue_note_for_indexing(const NoteDocument& note, int priority) {
    if (!is_running()) return false;
    
    // Check memory usage
    {
        std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
        if (pimpl_->metrics.is_memory_critical(pimpl_->config)) {
            return false;  // Drop request due to memory pressure
        }
    }
    
    // Create indexing task
    IndexingTask task;
    task.operation = IndexingOperation::CREATE;
    task.note = note;
    task.queued_at = std::chrono::system_clock::now();
    task.scheduled_at = std::chrono::system_clock::now();
    task.priority = priority > 0 ? priority : IndexingTask::calculate_priority(note);
    task.correlation_id = "note_" + note.id + "_" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    
    // Add to queue
    {
        std::lock_guard<std::mutex> lock(pimpl_->queue_mutex);
        
        if (pimpl_->task_queue.size() >= static_cast<size_t>(pimpl_->config.max_queue_size)) {
            return false;  // Queue is full
        }
        
        pimpl_->task_queue.push(task);
        pimpl_->metrics.current_queue_size = static_cast<int>(pimpl_->task_queue.size());
    }
    
    pimpl_->queue_cv.notify_one();
    return true;
}

std::future<bool> NoteIndexer::index_note_immediately(const NoteDocument& note) {
    return std::async(std::launch::async, [this, note]() {
        if (!note.should_be_indexed()) {
            return false;
        }
        
        try {
            auto es_doc = note.to_elasticsearch_document();
            auto future = pimpl_->engine->index_note(note.id, es_doc);
            bool success = future.get();
            
            // Update metrics
            {
                std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
                pimpl_->metrics.notes_processed++;
                if (success) {
                    pimpl_->metrics.notes_indexed++;
                } else {
                    pimpl_->metrics.notes_failed++;
                }
            }
            
            return success;
        } catch (const std::exception& e) {
            // Log error
            {
                std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
                pimpl_->metrics.notes_failed++;
            }
            return false;
        }
    });
}

IndexingMetrics NoteIndexer::get_metrics() const {
    std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
    return pimpl_->metrics;
}

int NoteIndexer::get_queue_size() const {
    std::lock_guard<std::mutex> lock(pimpl_->queue_mutex);
    return static_cast<int>(pimpl_->task_queue.size());
}

void NoteIndexer::indexing_worker_loop() {
    pimpl_->metrics.active_worker_threads++;
    
    while (pimpl_->running) {
        std::vector<IndexingTask> batch;
        
        // Collect a batch of tasks
        {
            std::unique_lock<std::mutex> lock(pimpl_->queue_mutex);
            
            pimpl_->queue_cv.wait_for(lock, pimpl_->config.batch_timeout, [this] {
                return !pimpl_->task_queue.empty() || !pimpl_->running;
            });
            
            if (!pimpl_->running) break;
            if (pimpl_->paused) continue;
            
            // Collect tasks for batch processing
            while (!pimpl_->task_queue.empty() && batch.size() < static_cast<size_t>(pimpl_->config.batch_size)) {
                batch.push_back(pimpl_->task_queue.top());
                pimpl_->task_queue.pop();
            }
            
            pimpl_->metrics.current_queue_size = static_cast<int>(pimpl_->task_queue.size());
        }
        
        // Process the batch
        if (!batch.empty()) {
            process_task_batch(batch);
        }
    }
    
    pimpl_->metrics.active_worker_threads--;
}

bool NoteIndexer::process_task_batch(const std::vector<IndexingTask>& tasks) {
    auto start_time = std::chrono::steady_clock::now();
    
    bool batch_success = true;
    
    for (const auto& task : tasks) {
        bool task_success = process_task(task);
        if (!task_success) {
            batch_success = false;
            
            // Handle failed task
            if (task.should_retry(pimpl_->config)) {
                // Reschedule with delay
                IndexingTask retry_task = task;
                retry_task.retry_count++;
                retry_task.scheduled_at = std::chrono::system_clock::now() + task.get_retry_delay(pimpl_->config);
                
                {
                    std::lock_guard<std::mutex> lock(pimpl_->queue_mutex);
                    pimpl_->task_queue.push(retry_task);
                }
                
                pimpl_->metrics.retries_attempted++;
            } else {
                handle_failed_task(task, "Max retries exceeded");
            }
        }
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    // Update metrics
    {
        std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
        pimpl_->metrics.batches_processed++;
        if (!batch_success) {
            pimpl_->metrics.batches_failed++;
        }
    }
    
    return batch_success;
}

bool NoteIndexer::process_task(const IndexingTask& task) {
    auto start_time = std::chrono::steady_clock::now();
    
    try {
        bool success = false;
        
        switch (task.operation) {
            case IndexingOperation::CREATE:
                {
                    auto es_doc = task.note.to_elasticsearch_document();
                    auto future = pimpl_->engine->index_note(task.note.id, es_doc);
                    success = future.get();
                    
                    if (success) {
                        update_metrics(task.operation, true, std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time), std::chrono::milliseconds{0});
                    }
                }
                break;
                
            case IndexingOperation::UPDATE:
                {
                    auto es_doc = task.note.to_elasticsearch_document();
                    auto future = pimpl_->engine->index_note(task.note.id, es_doc);  // Same as create for now
                    success = future.get();
                }
                break;
                
            case IndexingOperation::DELETE:
                {
                    auto future = pimpl_->engine->delete_note(task.note.id);
                    success = future.get();
                }
                break;
                
            case IndexingOperation::UPDATE_METRICS:
                {
                    nlohmann::json metrics_update = {
                        {"metrics", {
                            {"likes_count", task.note.metrics.likes_count},
                            {"renotes_count", task.note.metrics.renotes_count},
                            {"replies_count", task.note.metrics.replies_count},
                            {"views_count", task.note.metrics.views_count}
                        }}
                    };
                    auto future = pimpl_->engine->update_note_metrics(task.note.id, metrics_update);
                    success = future.get();
                }
                break;
        }
        
        auto end_time = std::chrono::steady_clock::now();
        auto processing_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        update_metrics(task.operation, success, processing_time, std::chrono::milliseconds{0});
        
        return success;
        
    } catch (const std::exception& e) {
        auto end_time = std::chrono::steady_clock::now();
        auto processing_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        update_metrics(task.operation, false, processing_time, std::chrono::milliseconds{0});
        handle_failed_task(task, std::string("Exception: ") + e.what());
        
        return false;
    }
}

void NoteIndexer::handle_failed_task(const IndexingTask& task, const std::string& error) {
    // Log failed operation
    nlohmann::json failed_op = {
        {"task_id", task.correlation_id},
        {"operation", static_cast<int>(task.operation)},
        {"note_id", task.note.id},
        {"error", error},
        {"retry_count", task.retry_count},
        {"timestamp", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()}
    };
    
    {
        std::lock_guard<std::mutex> lock(pimpl_->failed_operations_mutex);
        pimpl_->failed_operations.push_back(failed_op);
        
        // Keep only recent failures
        if (pimpl_->failed_operations.size() > Impl::MAX_FAILED_OPERATIONS) {
            pimpl_->failed_operations.erase(pimpl_->failed_operations.begin());
        }
    }
}

void NoteIndexer::update_metrics(IndexingOperation operation, bool success, std::chrono::milliseconds processing_time, std::chrono::milliseconds indexing_time) {
    std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
    
    pimpl_->metrics.notes_processed++;
    pimpl_->metrics.total_processing_time_ms += processing_time.count();
    pimpl_->metrics.total_indexing_time_ms += indexing_time.count();
    
    if (success) {
        switch (operation) {
            case IndexingOperation::CREATE:
                pimpl_->metrics.notes_indexed++;
                break;
            case IndexingOperation::UPDATE:
            case IndexingOperation::UPDATE_METRICS:
                pimpl_->metrics.notes_updated++;
                break;
            case IndexingOperation::DELETE:
                pimpl_->metrics.notes_deleted++;
                break;
        }
    } else {
        pimpl_->metrics.notes_failed++;
    }
}

/**
 * Utility functions
 */
namespace indexing_utils {

std::string generate_note_id(const std::string& user_id, const std::string& content_hash) {
    return user_id + "_" + content_hash;
}

std::string calculate_content_hash(const std::string& content) {
    // Simple hash implementation - in production use proper cryptographic hash
    std::hash<std::string> hasher;
    auto hash_value = hasher(content);
    
    std::ostringstream oss;
    oss << std::hex << hash_value;
    return oss.str();
}

bool validate_note_document(const NoteDocument& note, std::string& error_message) {
    if (note.id.empty()) {
        error_message = "Note ID is required";
        return false;
    }
    
    if (note.user_id.empty()) {
        error_message = "User ID is required";
        return false;
    }
    
    if (note.content.empty()) {
        error_message = "Content is required";
        return false;
    }
    
    if (note.content.length() > 10000) {
        error_message = "Content is too long";
        return false;
    }
    
    return true;
}

size_t estimate_document_size(const NoteDocument& note) {
    // Rough estimation of document size in bytes
    size_t size = 0;
    
    size += note.id.length();
    size += note.user_id.length();
    size += note.username.length();
    size += note.display_name.length();
    size += note.content.length();
    size += note.place_name.length();
    
    for (const auto& hashtag : note.hashtags) {
        size += hashtag.length();
    }
    
    for (const auto& mention : note.mentions) {
        size += mention.length();
    }
    
    for (const auto& url : note.media_urls) {
        size += url.length();
    }
    
    // Add overhead for JSON structure
    size += 500;
    
    return size;
}

bool is_indexable(const NoteDocument& note, const IndexingConfig& config) {
    if (!note.should_be_indexed()) {
        return false;
    }
    
    // Check if content filtering is enabled
    if (!config.index_spam_content) {
        float spam_score = ContentAnalyzer::calculate_spam_score(note.content, note.user_id);
        if (spam_score > 0.7f) {
            return false;
        }
    }
    
    // Check NSFW filtering
    if (!config.index_nsfw_content && note.nsfw) {
        return false;
    }
    
    return true;
}

} // namespace indexing_utils

} // namespace indexers
} // namespace search_service
} // namespace sonet