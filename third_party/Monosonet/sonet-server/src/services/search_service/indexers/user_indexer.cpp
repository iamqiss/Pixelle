/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Implementation of the real-time user indexer for Twitter-scale search operations.
 * This processes user profiles with intelligent analysis, reputation scoring,
 * and suggestion generation.
 */

#include "user_indexer.h"
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

// Profile analysis patterns
const std::vector<std::string> ProfileAnalyzer::BIO_PROFESSION_PATTERNS = {
    R"(\b(?:CEO|CTO|CFO|VP|Director|Manager|Engineer|Developer|Designer|Analyst|Consultant)\b)",
    R"(\b(?:Doctor|Lawyer|Teacher|Professor|Nurse|Artist|Writer|Journalist|Photographer)\b)",
    R"(\b(?:Student|Researcher|Scientist|Entrepreneur|Founder|Co-founder|Freelancer)\b)",
    R"(\b(?:at\s+(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)|@[A-Za-z0-9_]+)\b)",
    R"(\b(?:working|works|employed|job|career|profession|position)\b)",
};

const std::vector<std::string> ProfileAnalyzer::BIO_EDUCATION_PATTERNS = {
    R"(\b(?:University|College|Institute|School|Academy|Harvard|MIT|Stanford|Oxford|Cambridge)\b)",
    R"(\b(?:PhD|Masters|Bachelor|MBA|Degree|Graduate|Alumni|Class of \d{4})\b)",
    R"(\b(?:studying|studied|student|education|major|minor|thesis|research)\b)",
};

const std::vector<std::string> ProfileAnalyzer::BIO_LOCATION_PATTERNS = {
    R"(\b(?:San Francisco|New York|London|Tokyo|Paris|Berlin|Sydney|Toronto|Singapore)\b)",
    R"(\b(?:📍|🌍|🌎|🌏|Located in|Based in|From|Living in|Currently in)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b)",
    R"(\b(?:USA|US|UK|Canada|Australia|Germany|France|Japan|China|India|Brazil)\b)",
};

const std::unordered_map<std::string, std::vector<std::string>> ProfileAnalyzer::INTEREST_KEYWORDS = {
    {"technology", {"AI", "ML", "tech", "programming", "coding", "software", "development", "innovation", "startup", "crypto", "blockchain"}},
    {"sports", {"football", "basketball", "soccer", "tennis", "running", "fitness", "workout", "gym", "marathon", "cycling"}},
    {"music", {"music", "musician", "guitar", "piano", "singing", "concert", "album", "band", "artist", "DJ"}},
    {"travel", {"travel", "traveling", "wanderlust", "adventure", "explore", "vacation", "journey", "nomad", "backpacking"}},
    {"food", {"foodie", "cooking", "chef", "restaurant", "cuisine", "recipe", "baking", "coffee", "wine", "culinary"}},
    {"photography", {"photography", "photographer", "photo", "camera", "lens", "portrait", "landscape", "street photography"}},
    {"art", {"art", "artist", "painting", "drawing", "sculpture", "gallery", "creative", "design", "illustration"}},
    {"books", {"reading", "books", "author", "writer", "literature", "novel", "poetry", "bookworm", "library"}},
    {"science", {"science", "research", "physics", "chemistry", "biology", "astronomy", "discovery", "experiment"}},
    {"business", {"business", "entrepreneur", "startup", "marketing", "sales", "finance", "investing", "leadership"}}
};

const std::vector<std::string> ProfileAnalyzer::BOT_INDICATORS = {
    R"(\b(?:bot|automated|auto|generated|script|api|service|system)\b)",
    R"(\b(?:follow\s*back|#followback|f4f|follow4follow|teamfollowback)\b)",
    R"(\b(?:retweet|rt|spam|promotion|advertisement|sale|discount)\b)",
    R"(\$\d+|\d+%\s*(?:off|discount)|free\s+(?:shipping|trial|sample))",
};

/**
 * UserDocument implementation
 */
nlohmann::json UserDocument::to_elasticsearch_document() const {
    nlohmann::json doc = {
        {"id", id},
        {"username", username},
        {"display_name", display_name},
        {"bio", bio},
        {"location", location},
        {"website", website},
        {"created_at", std::chrono::duration_cast<std::chrono::milliseconds>(created_at.time_since_epoch()).count()},
        {"updated_at", std::chrono::duration_cast<std::chrono::milliseconds>(updated_at.time_since_epoch()).count()},
        {"is_private", is_private},
        {"is_verified", is_verified},
        {"verification_level", verification_level},
        {"is_suspended", is_suspended},
        {"is_deleted", is_deleted},
        {"avatar_url", avatar_url},
        {"banner_url", banner_url}
    };
    
    // Add metrics
    doc["metrics"] = {
        {"followers_count", metrics.followers_count},
        {"following_count", metrics.following_count},
        {"notes_count", metrics.notes_count},
        {"likes_received_count", metrics.likes_received_count},
        {"engagement_rate", metrics.engagement_rate},
        {"average_note_quality", metrics.average_note_quality},
        {"activity_score", metrics.activity_score},
        {"influence_score", metrics.influence_score}
    };
    
    // Add profile data
    doc["profile_data"] = {
        {"interests", profile_data.interests},
        {"topics", profile_data.topics},
        {"profession", profile_data.profession},
        {"education", profile_data.education},
        {"languages", profile_data.languages}
    };
    
    // Add reputation data
    doc["reputation"] = {
        {"overall_score", reputation.overall_score},
        {"content_quality_score", reputation.content_quality_score},
        {"engagement_quality_score", reputation.engagement_quality_score},
        {"network_quality_score", reputation.network_quality_score},
        {"trust_score", reputation.trust_score},
        {"influence_score", reputation.influence_score},
        {"expertise_score", reputation.expertise_score},
        {"activity_consistency_score", reputation.activity_consistency_score}
    };
    
    // Add analysis data
    doc["analysis"] = {
        {"is_bot_likely", analysis.is_bot_likely},
        {"bot_confidence", analysis.bot_confidence},
        {"account_age_days", analysis.account_age_days},
        {"noteing_frequency", analysis.noteing_frequency},
        {"interaction_patterns", analysis.interaction_patterns},
        {"content_diversity", analysis.content_diversity},
        {"network_diversity", analysis.network_diversity},
        {"spam_likelihood", analysis.spam_likelihood}
    };
    
    // Add boost factors
    doc["boost_factors"] = {
        {"verification_boost", boost_factors.verification_boost},
        {"follower_boost", boost_factors.follower_boost},
        {"activity_boost", boost_factors.activity_boost},
        {"quality_boost", boost_factors.quality_boost},
        {"recency_boost", boost_factors.recency_boost}
    };
    
    // Add indexing metadata
    doc["indexing_metadata"] = {
        {"indexed_at", std::chrono::duration_cast<std::chrono::milliseconds>(indexing_metadata.indexed_at.time_since_epoch()).count()},
        {"version", indexing_metadata.version},
        {"source", indexing_metadata.source}
    };
    
    return doc;
}

UserDocument UserDocument::from_json(const nlohmann::json& json) {
    UserDocument user;
    
    user.id = json.value("id", "");
    user.username = json.value("username", "");
    user.display_name = json.value("display_name", "");
    user.bio = json.value("bio", "");
    user.location = json.value("location", "");
    user.website = json.value("website", "");
    user.verification_level = json.value("verification_level", "none");
    user.is_private = json.value("is_private", false);
    user.is_verified = json.value("is_verified", false);
    user.is_suspended = json.value("is_suspended", false);
    user.is_deleted = json.value("is_deleted", false);
    user.avatar_url = json.value("avatar_url", "");
    user.banner_url = json.value("banner_url", "");
    
    // Parse timestamps
    if (json.contains("created_at")) {
        auto timestamp = json["created_at"].get<long long>();
        user.created_at = std::chrono::system_clock::time_point{std::chrono::milliseconds{timestamp}};
    }
    
    if (json.contains("updated_at")) {
        auto timestamp = json["updated_at"].get<long long>();
        user.updated_at = std::chrono::system_clock::time_point{std::chrono::milliseconds{timestamp}};
    }
    
    // Parse metrics
    if (json.contains("metrics")) {
        const auto& metrics = json["metrics"];
        user.metrics.followers_count = metrics.value("followers_count", 0);
        user.metrics.following_count = metrics.value("following_count", 0);
        user.metrics.notes_count = metrics.value("notes_count", 0);
        user.metrics.likes_received_count = metrics.value("likes_received_count", 0L);
        user.metrics.engagement_rate = metrics.value("engagement_rate", 0.0f);
        user.metrics.average_note_quality = metrics.value("average_note_quality", 0.0f);
        user.metrics.activity_score = metrics.value("activity_score", 0.0f);
        user.metrics.influence_score = metrics.value("influence_score", 0.0f);
    }
    
    // Parse profile data
    if (json.contains("profile_data")) {
        const auto& profile = json["profile_data"];
        if (profile.contains("interests") && profile["interests"].is_array()) {
            user.profile_data.interests = profile["interests"].get<std::vector<std::string>>();
        }
        if (profile.contains("topics") && profile["topics"].is_array()) {
            user.profile_data.topics = profile["topics"].get<std::vector<std::string>>();
        }
        user.profile_data.profession = profile.value("profession", "");
        user.profile_data.education = profile.value("education", "");
        if (profile.contains("languages") && profile["languages"].is_array()) {
            user.profile_data.languages = profile["languages"].get<std::vector<std::string>>();
        }
    }
    
    // Calculate derived fields if not provided
    if (user.profile_data.interests.empty() || user.profile_data.topics.empty()) {
        auto analysis = ProfileAnalyzer::analyze_profile(user.bio, user.username, user.display_name);
        if (user.profile_data.interests.empty()) {
            user.profile_data.interests = analysis.interests;
        }
        if (user.profile_data.topics.empty()) {
            user.profile_data.topics = analysis.topics;
        }
        if (user.profile_data.profession.empty()) {
            user.profile_data.profession = analysis.profession;
        }
        if (user.profile_data.education.empty()) {
            user.profile_data.education = analysis.education;
        }
    }
    
    // Calculate reputation
    user.reputation = ReputationCalculator::calculate_reputation(user);
    
    // Calculate analysis
    user.analysis = ProfileAnalyzer::analyze_user_behavior(user);
    
    // Calculate boost factors
    user.boost_factors = user.calculate_boost_factors();
    
    // Set indexing metadata
    user.indexing_metadata.indexed_at = std::chrono::system_clock::now();
    user.indexing_metadata.version = 1;
    user.indexing_metadata.source = "api";
    
    return user;
}

bool UserDocument::should_be_indexed() const {
    // Don't index deleted or suspended accounts
    if (is_deleted || is_suspended) return false;
    
    // Don't index empty profiles
    if (username.empty() || display_name.empty()) return false;
    
    // Don't index likely bots (with high confidence)
    if (analysis.is_bot_likely && analysis.bot_confidence > 0.8f) return false;
    
    // Don't index accounts with very low reputation
    if (reputation.overall_score < 0.1f) return false;
    
    return true;
}

std::string UserDocument::get_routing_key() const {
    // Use user_id for routing
    return id;
}

UserBoostFactors UserDocument::calculate_boost_factors() const {
    UserBoostFactors factors;
    
    // Verification boost
    if (is_verified) {
        if (verification_level == "official") {
            factors.verification_boost = 2.0f;
        } else if (verification_level == "organization") {
            factors.verification_boost = 1.5f;
        } else {
            factors.verification_boost = 1.2f;
        }
    }
    
    // Follower boost (logarithmic scaling)
    if (metrics.followers_count > 0) {
        factors.follower_boost = std::log1p(static_cast<float>(metrics.followers_count)) / std::log(1000000.0f);
        factors.follower_boost = std::min(2.0f, std::max(1.0f, factors.follower_boost + 1.0f));
    }
    
    // Activity boost
    factors.activity_boost = std::min(1.5f, std::max(0.5f, metrics.activity_score + 0.5f));
    
    // Quality boost
    factors.quality_boost = std::min(1.8f, std::max(0.8f, metrics.average_note_quality + 0.8f));
    
    // Recency boost (based on last activity)
    auto now = std::chrono::system_clock::now();
    auto age_days = std::chrono::duration_cast<std::chrono::hours>(now - updated_at).count() / 24;
    
    if (age_days < 1) {
        factors.recency_boost = 1.3f;
    } else if (age_days < 7) {
        factors.recency_boost = 1.1f;
    } else if (age_days < 30) {
        factors.recency_boost = 1.0f;
    } else {
        factors.recency_boost = 0.8f;
    }
    
    return factors;
}

/**
 * ProfileAnalyzer implementation
 */
ProfileAnalyzer::ProfileAnalysis ProfileAnalyzer::analyze_profile(const std::string& bio, const std::string& username, const std::string& display_name) {
    ProfileAnalysis analysis;
    
    std::string combined_text = bio + " " + username + " " + display_name;
    std::transform(combined_text.begin(), combined_text.end(), combined_text.begin(), ::tolower);
    
    // Extract interests
    analysis.interests = extract_interests(combined_text);
    
    // Extract topics (similar to interests but more specific)
    analysis.topics = extract_topics(combined_text);
    
    // Extract profession
    analysis.profession = extract_profession(bio);
    
    // Extract education
    analysis.education = extract_education(bio);
    
    // Extract location
    analysis.location = extract_location(bio);
    
    // Detect languages
    analysis.languages = detect_languages(combined_text);
    
    return analysis;
}

UserBehaviorAnalysis ProfileAnalyzer::analyze_user_behavior(const UserDocument& user) {
    UserBehaviorAnalysis behavior;
    
    // Calculate account age
    auto now = std::chrono::system_clock::now();
    behavior.account_age_days = std::chrono::duration_cast<std::chrono::hours>(now - user.created_at).count() / 24;
    
    // Calculate noteing frequency
    if (behavior.account_age_days > 0) {
        behavior.noteing_frequency = static_cast<float>(user.metrics.notes_count) / behavior.account_age_days;
    }
    
    // Analyze follower/following ratio
    if (user.metrics.following_count > 0) {
        float ratio = static_cast<float>(user.metrics.followers_count) / user.metrics.following_count;
        behavior.follower_following_ratio = ratio;
    }
    
    // Bot detection
    float bot_score = calculate_bot_likelihood(user);
    behavior.is_bot_likely = bot_score > 0.6f;
    behavior.bot_confidence = bot_score;
    
    // Spam likelihood
    behavior.spam_likelihood = calculate_spam_likelihood(user);
    
    // Content diversity (simplified)
    behavior.content_diversity = user.profile_data.interests.size() > 2 ? 0.8f : 0.4f;
    
    // Network diversity (simplified based on follower/following patterns)
    if (user.metrics.followers_count > 100 && user.metrics.following_count > 50) {
        behavior.network_diversity = 0.8f;
    } else {
        behavior.network_diversity = 0.5f;
    }
    
    // Interaction patterns (simplified)
    if (user.metrics.engagement_rate > 0.05f) {
        behavior.interaction_patterns.push_back("high_engagement");
    }
    if (user.metrics.followers_count > user.metrics.following_count * 2) {
        behavior.interaction_patterns.push_back("influencer_pattern");
    }
    
    return behavior;
}

std::vector<std::string> ProfileAnalyzer::extract_interests(const std::string& text) {
    std::vector<std::string> interests;
    
    for (const auto& category_pair : INTEREST_KEYWORDS) {
        const std::string& category = category_pair.first;
        const std::vector<std::string>& keywords = category_pair.second;
        
        int keyword_matches = 0;
        for (const auto& keyword : keywords) {
            std::string lower_keyword = keyword;
            std::transform(lower_keyword.begin(), lower_keyword.end(), lower_keyword.begin(), ::tolower);
            
            if (text.find(lower_keyword) != std::string::npos) {
                keyword_matches++;
            }
        }
        
        // If we found keywords for this interest, include it
        if (keyword_matches > 0) {
            interests.push_back(category);
        }
    }
    
    return interests;
}

std::vector<std::string> ProfileAnalyzer::extract_topics(const std::string& text) {
    // Topics are more specific than interests
    std::vector<std::string> topics;
    
    // Technology topics
    if (text.find("ai") != std::string::npos || text.find("machine learning") != std::string::npos) {
        topics.push_back("artificial_intelligence");
    }
    if (text.find("blockchain") != std::string::npos || text.find("crypto") != std::string::npos) {
        topics.push_back("cryptocurrency");
    }
    if (text.find("startup") != std::string::npos || text.find("entrepreneur") != std::string::npos) {
        topics.push_back("entrepreneurship");
    }
    
    // Add more topic extraction logic here
    
    return topics;
}

std::string ProfileAnalyzer::extract_profession(const std::string& bio) {
    for (const auto& pattern : BIO_PROFESSION_PATTERNS) {
        std::regex profession_regex(pattern, std::regex_constants::icase);
        std::smatch match;
        
        if (std::regex_search(bio, match, profession_regex)) {
            std::string profession = match.str();
            std::transform(profession.begin(), profession.end(), profession.begin(), ::tolower);
            return profession;
        }
    }
    
    return "";
}

std::string ProfileAnalyzer::extract_education(const std::string& bio) {
    for (const auto& pattern : BIO_EDUCATION_PATTERNS) {
        std::regex education_regex(pattern, std::regex_constants::icase);
        std::smatch match;
        
        if (std::regex_search(bio, match, education_regex)) {
            std::string education = match.str();
            std::transform(education.begin(), education.end(), education.begin(), ::tolower);
            return education;
        }
    }
    
    return "";
}

std::string ProfileAnalyzer::extract_location(const std::string& bio) {
    for (const auto& pattern : BIO_LOCATION_PATTERNS) {
        std::regex location_regex(pattern, std::regex_constants::icase);
        std::smatch match;
        
        if (std::regex_search(bio, match, location_regex)) {
            if (match.size() > 1) {
                return match.str(1);  // Return captured group
            } else {
                return match.str();
            }
        }
    }
    
    return "";
}

std::vector<std::string> ProfileAnalyzer::detect_languages(const std::string& text) {
    std::vector<std::string> languages;
    
    // Simple language detection based on character sets
    bool has_latin = false;
    bool has_cyrillic = false;
    bool has_cjk = false;
    bool has_arabic = false;
    
    for (char c : text) {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
            has_latin = true;
        }
        // Add more character set detection here
    }
    
    if (has_latin) languages.push_back("en");
    if (has_cyrillic) languages.push_back("ru");
    if (has_cjk) languages.push_back("zh");
    if (has_arabic) languages.push_back("ar");
    
    if (languages.empty()) {
        languages.push_back("en");  // Default
    }
    
    return languages;
}

float ProfileAnalyzer::calculate_bot_likelihood(const UserDocument& user) {
    float bot_score = 0.0f;
    
    // Check bio for bot indicators
    std::string bio_lower = user.bio;
    std::transform(bio_lower.begin(), bio_lower.end(), bio_lower.begin(), ::tolower);
    
    for (const auto& pattern : BOT_INDICATORS) {
        std::regex bot_regex(pattern, std::regex_constants::icase);
        if (std::regex_search(bio_lower, bot_regex)) {
            bot_score += 0.3f;
        }
    }
    
    // Check username patterns
    std::regex username_bot_pattern(R"([a-zA-Z]+\d{6,}|user\d+|bot_?\w+)");
    if (std::regex_match(user.username, username_bot_pattern)) {
        bot_score += 0.2f;
    }
    
    // Check follower/following patterns
    if (user.metrics.following_count > user.metrics.followers_count * 10) {
        bot_score += 0.2f;  // Following way more than followers
    }
    
    // Check noteing frequency
    auto now = std::chrono::system_clock::now();
    auto account_age_days = std::chrono::duration_cast<std::chrono::hours>(now - user.created_at).count() / 24;
    
    if (account_age_days > 0) {
        float notes_per_day = static_cast<float>(user.metrics.notes_count) / account_age_days;
        if (notes_per_day > 50) {  // More than 50 notes per day
            bot_score += 0.3f;
        }
    }
    
    // Check profile completeness (bots often have incomplete profiles)
    if (user.bio.empty() || user.display_name.empty()) {
        bot_score += 0.1f;
    }
    
    return std::min(1.0f, bot_score);
}

float ProfileAnalyzer::calculate_spam_likelihood(const UserDocument& user) {
    float spam_score = 0.0f;
    
    // Check bio for promotional content
    std::string bio_lower = user.bio;
    std::transform(bio_lower.begin(), bio_lower.end(), bio_lower.begin(), ::tolower);
    
    std::vector<std::string> spam_patterns = {
        R"(\b(?:buy|sale|discount|promotion|deal|offer|click|link)\b)",
        R"(\$\d+|\d+%|free|guaranteed|limited time)",
        R"(\b(?:DM|message|contact|email|phone)\b.*\b(?:business|sell|offer)\b)"
    };
    
    for (const auto& pattern : spam_patterns) {
        std::regex spam_regex(pattern, std::regex_constants::icase);
        if (std::regex_search(bio_lower, spam_regex)) {
            spam_score += 0.2f;
        }
    }
    
    // Check for excessive external links
    std::regex url_regex(R"(https?://[^\s]+)");
    auto url_count = std::distance(
        std::sregex_iterator(user.bio.begin(), user.bio.end(), url_regex),
        std::sregex_iterator()
    );
    
    if (url_count > 2) {
        spam_score += 0.3f;
    }
    
    return std::min(1.0f, spam_score);
}

/**
 * ReputationCalculator implementation
 */
UserReputation ReputationCalculator::calculate_reputation(const UserDocument& user) {
    UserReputation reputation;
    
    // Content quality score
    reputation.content_quality_score = calculate_content_quality_score(user);
    
    // Engagement quality score
    reputation.engagement_quality_score = calculate_engagement_quality_score(user);
    
    // Network quality score
    reputation.network_quality_score = calculate_network_quality_score(user);
    
    // Trust score
    reputation.trust_score = calculate_trust_score(user);
    
    // Influence score
    reputation.influence_score = calculate_influence_score(user);
    
    // Expertise score
    reputation.expertise_score = calculate_expertise_score(user);
    
    // Activity consistency score
    reputation.activity_consistency_score = calculate_activity_consistency_score(user);
    
    // Overall score (weighted average)
    reputation.overall_score = 
        reputation.content_quality_score * 0.25f +
        reputation.engagement_quality_score * 0.20f +
        reputation.network_quality_score * 0.15f +
        reputation.trust_score * 0.15f +
        reputation.influence_score * 0.10f +
        reputation.expertise_score * 0.10f +
        reputation.activity_consistency_score * 0.05f;
    
    return reputation;
}

float ReputationCalculator::calculate_content_quality_score(const UserDocument& user) {
    if (user.metrics.notes_count == 0) return 0.5f;
    
    // Base on average note quality
    float base_score = user.metrics.average_note_quality;
    
    // Factor in engagement rate
    float engagement_factor = std::min(1.0f, user.metrics.engagement_rate * 10.0f);
    
    // Factor in consistency (number of notes)
    float consistency_factor = std::log1p(static_cast<float>(user.metrics.notes_count)) / std::log(1001.0f);
    
    return base_score * 0.6f + engagement_factor * 0.25f + consistency_factor * 0.15f;
}

float ReputationCalculator::calculate_engagement_quality_score(const UserDocument& user) {
    if (user.metrics.notes_count == 0) return 0.5f;
    
    // Calculate likes per note
    float avg_likes = static_cast<float>(user.metrics.likes_received_count) / user.metrics.notes_count;
    
    // Normalize to 0-1 scale
    float normalized_likes = std::log1p(avg_likes) / std::log(101.0f);
    
    // Factor in engagement rate
    float engagement_rate = user.metrics.engagement_rate;
    
    return normalized_likes * 0.7f + engagement_rate * 0.3f;
}

float ReputationCalculator::calculate_network_quality_score(const UserDocument& user) {
    if (user.metrics.followers_count == 0) return 0.3f;
    
    // Calculate follower/following ratio
    float ratio = user.metrics.following_count > 0 ? 
        static_cast<float>(user.metrics.followers_count) / user.metrics.following_count : 
        static_cast<float>(user.metrics.followers_count);
    
    // Normalize ratio (healthy range is 0.5 to 5.0)
    float normalized_ratio = std::min(1.0f, ratio / 5.0f);
    
    // Factor in absolute follower count
    float follower_factor = std::log1p(static_cast<float>(user.metrics.followers_count)) / std::log(1000001.0f);
    
    return normalized_ratio * 0.6f + follower_factor * 0.4f;
}

float ReputationCalculator::calculate_trust_score(const UserDocument& user) {
    float trust_score = 0.5f;  // Base trust
    
    // Verification boosts trust
    if (user.is_verified) {
        if (user.verification_level == "official") {
            trust_score += 0.4f;
        } else if (user.verification_level == "organization") {
            trust_score += 0.3f;
        } else {
            trust_score += 0.2f;
        }
    }
    
    // Account age boosts trust
    auto now = std::chrono::system_clock::now();
    auto age_days = std::chrono::duration_cast<std::chrono::hours>(now - user.created_at).count() / 24;
    
    if (age_days > 365) {
        trust_score += 0.2f;
    } else if (age_days > 90) {
        trust_score += 0.1f;
    }
    
    // Complete profile boosts trust
    if (!user.bio.empty() && !user.location.empty()) {
        trust_score += 0.1f;
    }
    
    // Bot likelihood reduces trust
    if (user.analysis.is_bot_likely) {
        trust_score -= user.analysis.bot_confidence * 0.5f;
    }
    
    return std::max(0.0f, std::min(1.0f, trust_score));
}

float ReputationCalculator::calculate_influence_score(const UserDocument& user) {
    // Based on follower count and engagement
    float follower_score = std::log1p(static_cast<float>(user.metrics.followers_count)) / std::log(1000001.0f);
    float engagement_score = user.metrics.engagement_rate;
    
    return follower_score * 0.7f + engagement_score * 0.3f;
}

float ReputationCalculator::calculate_expertise_score(const UserDocument& user) {
    float expertise = 0.3f;  // Base score
    
    // Verification indicates expertise
    if (user.is_verified) {
        expertise += 0.3f;
    }
    
    // Professional bio indicates expertise
    if (!user.profile_data.profession.empty()) {
        expertise += 0.2f;
    }
    
    // Education indicates expertise
    if (!user.profile_data.education.empty()) {
        expertise += 0.1f;
    }
    
    // Many interests indicate broad expertise
    if (user.profile_data.interests.size() > 3) {
        expertise += 0.1f;
    }
    
    return std::min(1.0f, expertise);
}

float ReputationCalculator::calculate_activity_consistency_score(const UserDocument& user) {
    auto now = std::chrono::system_clock::now();
    auto age_days = std::chrono::duration_cast<std::chrono::hours>(now - user.created_at).count() / 24;
    
    if (age_days == 0 || user.metrics.notes_count == 0) return 0.5f;
    
    float notes_per_day = static_cast<float>(user.metrics.notes_count) / age_days;
    
    // Ideal noteing frequency is 1-5 notes per day
    if (notes_per_day >= 1.0f && notes_per_day <= 5.0f) {
        return 1.0f;
    } else if (notes_per_day < 1.0f) {
        return notes_per_day;  // Linear decrease for less frequent noteing
    } else {
        // Exponential decrease for excessive noteing
        return std::max(0.1f, 1.0f / (notes_per_day / 5.0f));
    }
}

/**
 * UserIndexer::Impl - Private implementation
 */
struct UserIndexer::Impl {
    std::shared_ptr<engines::ElasticsearchEngine> engine;
    IndexingConfig config;
    
    // State management
    std::atomic<bool> running{false};
    std::atomic<bool> paused{false};
    std::atomic<bool> debug_mode{false};
    
    // Task queue
    std::priority_queue<UserIndexingTask, std::vector<UserIndexingTask>, UserTaskPriorityComparator> task_queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    
    // Worker threads
    std::vector<std::thread> worker_threads;
    
    // Metrics
    mutable std::mutex metrics_mutex;
    UserIndexingMetrics metrics;
    
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
 * UserIndexingTask implementation
 */
bool UserIndexingTask::should_retry(const IndexingConfig& config) const {
    return retry_count < config.max_retry_attempts;
}

int UserIndexingTask::calculate_priority(const UserDocument& user) {
    int priority = 0;
    
    // Higher priority for verified users
    if (user.is_verified) {
        if (user.verification_level == "official") {
            priority += 15;
        } else if (user.verification_level == "organization") {
            priority += 10;
        } else {
            priority += 5;
        }
    }
    
    // Higher priority for high-reputation users
    if (user.reputation.overall_score > 0.8f) {
        priority += 8;
    }
    
    // Higher priority for influential users
    if (user.metrics.followers_count > 10000) {
        priority += 5;
    }
    
    // Higher priority for recent profile updates
    auto now = std::chrono::system_clock::now();
    auto age_hours = std::chrono::duration_cast<std::chrono::hours>(now - user.updated_at).count();
    if (age_hours < 1) {
        priority += 3;
    }
    
    return priority;
}

std::chrono::milliseconds UserIndexingTask::get_retry_delay(const IndexingConfig& config) const {
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
 * UserIndexingMetrics implementation
 */
nlohmann::json UserIndexingMetrics::to_json() const {
    auto now = std::chrono::system_clock::now();
    auto uptime_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - last_reset).count();
    
    return nlohmann::json{
        {"users_processed", users_processed.load()},
        {"users_indexed", users_indexed.load()},
        {"users_updated", users_updated.load()},
        {"users_deleted", users_deleted.load()},
        {"users_skipped", users_skipped.load()},
        {"users_failed", users_failed.load()},
        {"profile_analyses_completed", profile_analyses_completed.load()},
        {"reputation_calculations_completed", reputation_calculations_completed.load()},
        {"bot_detections_performed", bot_detections_performed.load()},
        {"batches_processed", batches_processed.load()},
        {"batches_failed", batches_failed.load()},
        {"retries_attempted", retries_attempted.load()},
        {"total_processing_time_ms", total_processing_time_ms.load()},
        {"total_indexing_time_ms", total_indexing_time_ms.load()},
        {"total_analysis_time_ms", total_analysis_time_ms.load()},
        {"current_queue_size", current_queue_size.load()},
        {"current_memory_usage_mb", current_memory_usage_mb.load()},
        {"active_worker_threads", active_worker_threads.load()},
        {"processing_rate_per_second", get_processing_rate()},
        {"success_rate", get_success_rate()},
        {"average_processing_time_ms", get_average_processing_time_ms()},
        {"uptime_seconds", uptime_seconds}
    };
}

void UserIndexingMetrics::reset() {
    users_processed = 0;
    users_indexed = 0;
    users_updated = 0;
    users_deleted = 0;
    users_skipped = 0;
    users_failed = 0;
    profile_analyses_completed = 0;
    reputation_calculations_completed = 0;
    bot_detections_performed = 0;
    batches_processed = 0;
    batches_failed = 0;
    retries_attempted = 0;
    total_processing_time_ms = 0;
    total_indexing_time_ms = 0;
    total_analysis_time_ms = 0;
    current_queue_size = 0;
    current_memory_usage_mb = 0;
    active_worker_threads = 0;
    last_reset = std::chrono::system_clock::now();
}

double UserIndexingMetrics::get_processing_rate() const {
    auto now = std::chrono::system_clock::now();
    auto duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - last_reset).count();
    
    if (duration_seconds == 0) return 0.0;
    
    return static_cast<double>(users_processed.load()) / duration_seconds;
}

double UserIndexingMetrics::get_success_rate() const {
    long total = users_processed.load();
    if (total == 0) return 0.0;
    
    long successful = users_indexed.load() + users_updated.load() + users_deleted.load();
    return static_cast<double>(successful) / total;
}

double UserIndexingMetrics::get_average_processing_time_ms() const {
    long total = users_processed.load();
    if (total == 0) return 0.0;
    
    return static_cast<double>(total_processing_time_ms.load()) / total;
}

/**
 * UserIndexer implementation
 */
UserIndexer::UserIndexer(std::shared_ptr<engines::ElasticsearchEngine> engine, const IndexingConfig& config)
    : pimpl_(std::make_unique<Impl>(std::move(engine), config)) {
}

UserIndexer::~UserIndexer() = default;

std::future<bool> UserIndexer::start() {
    return std::async(std::launch::async, [this]() {
        if (pimpl_->running.exchange(true)) {
            return false;  // Already running
        }
        
        // Start worker threads
        int thread_count = std::max(1, static_cast<int>(std::thread::hardware_concurrency()) / 4);
        pimpl_->worker_threads.reserve(thread_count);
        
        for (int i = 0; i < thread_count; ++i) {
            pimpl_->worker_threads.emplace_back([this] { indexing_worker_loop(); });
        }
        
        return true;
    });
}

void UserIndexer::stop() {
    pimpl_->stop();
}

bool UserIndexer::is_running() const {
    return pimpl_->running.load();
}

bool UserIndexer::queue_user_for_indexing(const UserDocument& user, int priority) {
    if (!is_running()) return false;
    
    // Check memory usage
    {
        std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
        if (pimpl_->metrics.current_memory_usage_mb.load() >= pimpl_->config.memory_limit_threshold_mb) {
            return false;  // Drop request due to memory pressure
        }
    }
    
    // Create indexing task
    UserIndexingTask task;
    task.operation = UserIndexingOperation::CREATE;
    task.user = user;
    task.queued_at = std::chrono::system_clock::now();
    task.scheduled_at = std::chrono::system_clock::now();
    task.priority = priority > 0 ? priority : UserIndexingTask::calculate_priority(user);
    task.correlation_id = "user_" + user.id + "_" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    
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

std::future<bool> UserIndexer::index_user_immediately(const UserDocument& user) {
    return std::async(std::launch::async, [this, user]() {
        if (!user.should_be_indexed()) {
            return false;
        }
        
        try {
            auto es_doc = user.to_elasticsearch_document();
            auto future = pimpl_->engine->index_user(user.id, es_doc);
            bool success = future.get();
            
            // Update metrics
            {
                std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
                pimpl_->metrics.users_processed++;
                if (success) {
                    pimpl_->metrics.users_indexed++;
                } else {
                    pimpl_->metrics.users_failed++;
                }
            }
            
            return success;
        } catch (const std::exception& e) {
            // Log error
            {
                std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
                pimpl_->metrics.users_failed++;
            }
            return false;
        }
    });
}

UserIndexingMetrics UserIndexer::get_metrics() const {
    std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
    return pimpl_->metrics;
}

int UserIndexer::get_queue_size() const {
    std::lock_guard<std::mutex> lock(pimpl_->queue_mutex);
    return static_cast<int>(pimpl_->task_queue.size());
}

void UserIndexer::indexing_worker_loop() {
    pimpl_->metrics.active_worker_threads++;
    
    while (pimpl_->running) {
        std::vector<UserIndexingTask> batch;
        
        // Collect a batch of tasks
        {
            std::unique_lock<std::mutex> lock(pimpl_->queue_mutex);
            
            pimpl_->queue_cv.wait_for(lock, pimpl_->config.batch_timeout, [this] {
                return !pimpl_->task_queue.empty() || !pimpl_->running;
            });
            
            if (!pimpl_->running) break;
            if (pimpl_->paused) continue;
            
            // Collect tasks for batch processing (smaller batches for users)
            size_t batch_size = static_cast<size_t>(pimpl_->config.batch_size) / 5;  // Users are more complex
            while (!pimpl_->task_queue.empty() && batch.size() < batch_size) {
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

bool UserIndexer::process_task_batch(const std::vector<UserIndexingTask>& tasks) {
    auto start_time = std::chrono::steady_clock::now();
    
    bool batch_success = true;
    
    for (const auto& task : tasks) {
        bool task_success = process_task(task);
        if (!task_success) {
            batch_success = false;
            
            // Handle failed task
            if (task.should_retry(pimpl_->config)) {
                // Reschedule with delay
                UserIndexingTask retry_task = task;
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

bool UserIndexer::process_task(const UserIndexingTask& task) {
    auto start_time = std::chrono::steady_clock::now();
    
    try {
        bool success = false;
        
        switch (task.operation) {
            case UserIndexingOperation::CREATE:
                {
                    auto es_doc = task.user.to_elasticsearch_document();
                    auto future = pimpl_->engine->index_user(task.user.id, es_doc);
                    success = future.get();
                    
                    if (success) {
                        update_metrics(task.operation, true, std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time), std::chrono::milliseconds{0});
                    }
                }
                break;
                
            case UserIndexingOperation::UPDATE:
                {
                    auto es_doc = task.user.to_elasticsearch_document();
                    auto future = pimpl_->engine->index_user(task.user.id, es_doc);  // Same as create for now
                    success = future.get();
                }
                break;
                
            case UserIndexingOperation::DELETE:
                {
                    auto future = pimpl_->engine->delete_user(task.user.id);
                    success = future.get();
                }
                break;
                
            case UserIndexingOperation::UPDATE_METRICS:
                {
                    nlohmann::json metrics_update = {
                        {"metrics", {
                            {"followers_count", task.user.metrics.followers_count},
                            {"following_count", task.user.metrics.following_count},
                            {"notes_count", task.user.metrics.notes_count}
                        }}
                    };
                    auto future = pimpl_->engine->update_user_metrics(task.user.id, metrics_update);
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

void UserIndexer::handle_failed_task(const UserIndexingTask& task, const std::string& error) {
    // Log failed operation
    nlohmann::json failed_op = {
        {"task_id", task.correlation_id},
        {"operation", static_cast<int>(task.operation)},
        {"user_id", task.user.id},
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

void UserIndexer::update_metrics(UserIndexingOperation operation, bool success, std::chrono::milliseconds processing_time, std::chrono::milliseconds indexing_time) {
    std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
    
    pimpl_->metrics.users_processed++;
    pimpl_->metrics.total_processing_time_ms += processing_time.count();
    pimpl_->metrics.total_indexing_time_ms += indexing_time.count();
    
    if (success) {
        switch (operation) {
            case UserIndexingOperation::CREATE:
                pimpl_->metrics.users_indexed++;
                break;
            case UserIndexingOperation::UPDATE:
            case UserIndexingOperation::UPDATE_METRICS:
                pimpl_->metrics.users_updated++;
                break;
            case UserIndexingOperation::DELETE:
                pimpl_->metrics.users_deleted++;
                break;
        }
    } else {
        pimpl_->metrics.users_failed++;
    }
}

/**
 * Utility functions
 */
namespace user_indexing_utils {

std::string generate_user_search_id(const std::string& username) {
    return "user_" + username;
}

bool validate_user_document(const UserDocument& user, std::string& error_message) {
    if (user.id.empty()) {
        error_message = "User ID is required";
        return false;
    }
    
    if (user.username.empty()) {
        error_message = "Username is required";
        return false;
    }
    
    if (user.display_name.empty()) {
        error_message = "Display name is required";
        return false;
    }
    
    // Validate username format
    std::regex username_regex(R"(^[a-zA-Z0-9_]{1,50}$)");
    if (!std::regex_match(user.username, username_regex)) {
        error_message = "Invalid username format";
        return false;
    }
    
    return true;
}

size_t estimate_user_document_size(const UserDocument& user) {
    // Rough estimation of document size in bytes
    size_t size = 0;
    
    size += user.id.length();
    size += user.username.length();
    size += user.display_name.length();
    size += user.bio.length();
    size += user.location.length();
    size += user.website.length();
    size += user.avatar_url.length();
    size += user.banner_url.length();
    
    for (const auto& interest : user.profile_data.interests) {
        size += interest.length();
    }
    
    for (const auto& topic : user.profile_data.topics) {
        size += topic.length();
    }
    
    for (const auto& lang : user.profile_data.languages) {
        size += lang.length();
    }
    
    // Add overhead for JSON structure
    size += 800;  // Users have more complex structure than notes
    
    return size;
}

bool is_user_indexable(const UserDocument& user, const IndexingConfig& config) {
    if (!user.should_be_indexed()) {
        return false;
    }
    
    // Check if bot filtering is enabled
    if (!config.index_bot_accounts && user.analysis.is_bot_likely) {
        return false;
    }
    
    // Check reputation threshold
    if (user.reputation.overall_score < 0.1f) {
        return false;
    }
    
    return true;
}

std::vector<std::string> extract_searchable_terms(const UserDocument& user) {
    std::vector<std::string> terms;
    
    // Add username and display name
    terms.push_back(user.username);
    terms.push_back(user.display_name);
    
    // Add interests and topics
    terms.insert(terms.end(), user.profile_data.interests.begin(), user.profile_data.interests.end());
    terms.insert(terms.end(), user.profile_data.topics.begin(), user.profile_data.topics.end());
    
    // Add profession and education if available
    if (!user.profile_data.profession.empty()) {
        terms.push_back(user.profile_data.profession);
    }
    
    if (!user.profile_data.education.empty()) {
        terms.push_back(user.profile_data.education);
    }
    
    // Add location if available
    if (!user.location.empty()) {
        terms.push_back(user.location);
    }
    
    return terms;
}

} // namespace user_indexing_utils

} // namespace indexers
} // namespace search_service
} // namespace sonet