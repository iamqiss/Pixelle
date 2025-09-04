//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "implementations.h"
#include <algorithm>
#include <regex>
#include <iostream>
#include <cctype>

namespace sonet::timeline {

namespace {
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

    // Simple spam detection patterns
    bool MatchesSpamPattern(const std::string& text) {
        // Convert to lowercase for case-insensitive matching
        std::string lower_text = text;
        std::transform(lower_text.begin(), lower_text.end(), lower_text.begin(), ::tolower);
        
        // Common spam patterns
        std::vector<std::regex> spam_patterns = {
            std::regex(R"(click\s+here)"),
            std::regex(R"(buy\s+now)"),
            std::regex(R"(limited\s+time)"),
            std::regex(R"(act\s+fast)"),
            std::regex(R"(free\s+money)"),
            std::regex(R"(\$\$\$+)"),
            std::regex(R"(!!!!!+)"),
        };
        
        for (const auto& pattern : spam_patterns) {
            if (std::regex_search(lower_text, pattern)) {
                return true;
            }
        }
        
        return false;
    }

    // Check for excessive capitalization
    bool HasExcessiveCaps(const std::string& text) {
        if (text.length() < 10) return false;
        
        int caps_count = 0;
        for (char c : text) {
            if (std::isupper(static_cast<unsigned char>(c))) caps_count++;
        }
        
        double caps_ratio = static_cast<double>(caps_count) / text.length();
        return caps_ratio > 0.7; // More than 70% caps
    }

    // Check for content warnings
    bool RequiresContentWarning(const ::sonet::note::Note& note) {
        // For stub implementation, assume no content warnings
        return false;
    }
}

// ============= CONTENT FILTER IMPLEMENTATION =============

AdvancedContentFilter::AdvancedContentFilter() {
    std::cout << "Advanced Content Filter initialized" << std::endl;
    
    // Initialize banned keywords
    banned_keywords_.insert({
        "hate", "harassment", "bullying", "doxxing", "spam", "scam", 
        "phishing", "malware", "virus", "illegal", "drugs", "weapons"
    });
    
    // Initialize spam patterns
    spam_patterns_.insert({
        "click here", "buy now", "limited time", "act fast", 
        "free money", "get rich", "work from home", "lose weight fast"
    });
}

std::vector<::sonet::note::Note> AdvancedContentFilter::FilterNotes(
    const std::vector<::sonet::note::Note>& notes,
    const std::string& user_id,
    const UserEngagementProfile& profile
) {
    std::vector<::sonet::note::Note> filtered_notes;
    filtered_notes.reserve(notes.size());
    
    int blocked_muted = 0, blocked_keywords = 0, blocked_policy = 0, blocked_spam = 0;
    
    for (const auto& note : notes) {
        bool should_include = true;
        std::string filter_reason;
        
        // Check if author is muted
        if (IsUserMuted(user_id, note.author_id())) {
            should_include = false;
            filter_reason = "muted_user";
            blocked_muted++;
        }
        
        // Check for muted keywords
        else if (ContainsMutedKeywords(user_id, note)) {
            should_include = false;
            filter_reason = "muted_keywords";
            blocked_keywords++;
        }
        
        // Check content policy violations
        else if (ViolatesContentPolicy(note)) {
            should_include = false;
            filter_reason = "policy_violation";
            blocked_policy++;
        }
        
        // Check spam detection
        else if (PassesSpamDetection(note) == false) {
            should_include = false;
            filter_reason = "spam_detected";
            blocked_spam++;
        }
        
        // Check engagement threshold
        else if (!MeetsEngagementThreshold(note, profile)) {
            should_include = false;
            filter_reason = "low_engagement";
        }
        
        // Check age-appropriate content
        else if (!IsAppropriateForUserAge(note, profile)) {
            should_include = false;
            filter_reason = "age_inappropriate";
        }
        
        if (should_include) {
            filtered_notes.push_back(note);
        }
    }
    
    std::cout << "Content filtering complete: " << notes.size() << " -> " << filtered_notes.size() 
              << " (blocked: muted=" << blocked_muted << ", keywords=" << blocked_keywords
              << ", policy=" << blocked_policy << ", spam=" << blocked_spam << ")" << std::endl;
    
    return filtered_notes;
}

bool AdvancedContentFilter::IsUserMuted(const std::string& user_id, const std::string& author_id) {
    std::lock_guard<std::mutex> lock(filter_mutex_);
    
    auto it = muted_users_.find(user_id);
    if (it != muted_users_.end()) {
        return it->second.count(author_id) > 0;
    }
    
    return false;
}

bool AdvancedContentFilter::ContainsMutedKeywords(const std::string& user_id, const ::sonet::note::Note& note) {
    std::lock_guard<std::mutex> lock(filter_mutex_);
    
    auto it = muted_keywords_.find(user_id);
    if (it == muted_keywords_.end()) {
        return false;
    }
    
    std::string lower_content = note.content();
    std::transform(lower_content.begin(), lower_content.end(), lower_content.begin(), ::tolower);
    
    for (const auto& keyword : it->second) {
        std::string lower_keyword = keyword;
        std::transform(lower_keyword.begin(), lower_keyword.end(), lower_keyword.begin(), ::tolower);
        
        if (lower_content.find(lower_keyword) != std::string::npos) {
            return true;
        }
    }
    
    // Check hashtags
    auto hashtags = ExtractHashtags(note.content());
    for (const auto& hashtag : hashtags) {
        std::string lower_hashtag = hashtag;
        std::transform(lower_hashtag.begin(), lower_hashtag.end(), lower_hashtag.begin(), ::tolower);
        
        if (it->second.count(lower_hashtag) > 0) {
            return true;
        }
    }
    
    return false;
}

bool AdvancedContentFilter::ViolatesContentPolicy(const ::sonet::note::Note& note) {
    std::string lower_content = note.content();
    std::transform(lower_content.begin(), lower_content.end(), lower_content.begin(), ::tolower);
    
    // Check for banned keywords
    for (const auto& banned : banned_keywords_) {
        if (lower_content.find(banned) != std::string::npos) {
            return true;
        }
    }
    
    // Check visibility settings vs content
    // For stub implementation, assume all content is acceptable
    return false;
}

bool AdvancedContentFilter::PassesSpamDetection(const ::sonet::note::Note& note) {
    // Check for spam patterns in content
    if (MatchesSpamPattern(note.content())) {
        return false;
    }
    
    // Check for excessive capitalization
    if (HasExcessiveCaps(note.content())) {
        return false;
    }
    
    // Check metrics for spam indicators (very high engagement ratios might indicate fake engagement)
    auto likes = note.metrics().likes();
    auto views = note.metrics().views();
    
    // For stub implementation, assume no spam
    return true;
}

bool AdvancedContentFilter::MeetsEngagementThreshold(
    const ::sonet::note::Note& note, 
    const UserEngagementProfile& profile
) {
    // For new users or high-engagement users, show more content
    if (profile.engagement_score < 0.3) {
        return true; // Show everything to new users
    }
    
    // For active users, filter out very low-engagement content
    const auto& metrics = note.metrics();
    double total_engagements = metrics.likes() + metrics.renotes() + metrics.comments();
    
    // Very basic threshold - in production this would be more sophisticated
    if (total_engagements == 0 && metrics.views() > 100) {
        return false; // High views but no engagement - potential spam
    }
    
    return true;
}

bool AdvancedContentFilter::IsAppropriateForUserAge(
    const ::sonet::note::Note& note, 
    const UserEngagementProfile& profile
) {
    // For this implementation, assume all content is appropriate
    // In production, this would check:
    // - User's age/birthdate
    // - Content ratings
    // - Explicit content markers
    // - Regional content restrictions
    
    if (RequiresContentWarning(note)) {
        // Could hide content warning content for younger users
        // For now, allow all content
    }
    
    return true;
}

void AdvancedContentFilter::UpdateUserPreferences(
    const std::string& user_id,
    const ContentFilterPreferences& preferences
) {
    std::lock_guard<std::mutex> lock(filter_mutex_);
    
    user_preferences_[user_id] = preferences;
    
    std::cout << "Updated content filter preferences for user " << user_id << std::endl;
}

void AdvancedContentFilter::AddMutedUser(const std::string& user_id, const std::string& muted_user_id) {
    std::lock_guard<std::mutex> lock(filter_mutex_);
    
    muted_users_[user_id].insert(muted_user_id);
    
    std::cout << "User " << user_id << " muted user " << muted_user_id << std::endl;
}

void AdvancedContentFilter::RemoveMutedUser(const std::string& user_id, const std::string& muted_user_id) {
    std::lock_guard<std::mutex> lock(filter_mutex_);
    
    auto it = muted_users_.find(user_id);
    if (it != muted_users_.end()) {
        it->second.erase(muted_user_id);
        
        if (it->second.empty()) {
            muted_users_.erase(it);
        }
        
        std::cout << "User " << user_id << " unmuted user " << muted_user_id << std::endl;
    }
}

void AdvancedContentFilter::AddMutedKeyword(const std::string& user_id, const std::string& keyword) {
    std::lock_guard<std::mutex> lock(filter_mutex_);
    
    muted_keywords_[user_id].insert(keyword);
    
    std::cout << "User " << user_id << " muted keyword \"" << keyword << "\"" << std::endl;
}

void AdvancedContentFilter::RemoveMutedKeyword(const std::string& user_id, const std::string& keyword) {
    std::lock_guard<std::mutex> lock(filter_mutex_);
    
    auto it = muted_keywords_.find(user_id);
    if (it != muted_keywords_.end()) {
        it->second.erase(keyword);
        
        if (it->second.empty()) {
            muted_keywords_.erase(it);
        }
        
        std::cout << "User " << user_id << " unmuted keyword \"" << keyword << "\"" << std::endl;
    }
}

} // namespace sonet::timeline
