/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "profile.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <regex>
#include <algorithm>
#include <chrono>

namespace sonet::user::models {

// CustomProfileField implementation
CustomProfileField::CustomProfileField(const std::string& label, const std::string& value, 
                                      ProfileFieldType type, ProfileVisibility visibility)
    : label(label), value(value), type(type), visibility(visibility), 
      display_order(0), is_verified(false) {
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    created_at = now;
    updated_at = now;
}

bool CustomProfileField::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> CustomProfileField::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (label.empty() || label.length() > 50) {
        errors.push_back("Field label must be between 1 and 50 characters");
    }
    
    if (value.length() > 500) {
        errors.push_back("Field value cannot exceed 500 characters");
    }
    
    if (type == ProfileFieldType::URL && !value.empty()) {
        std::regex url_regex(R"(^https?://[^\s/$.?#].[^\s]*$)");
        if (!std::regex_match(value, url_regex)) {
            errors.push_back("Invalid URL format");
        }
    }
    
    if (type == ProfileFieldType::EMAIL && !value.empty()) {
        std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
        if (!std::regex_match(value, email_regex)) {
            errors.push_back("Invalid email format");
        }
    }
    
    return errors;
}

std::string CustomProfileField::to_json() const {
    nlohmann::json j;
    j["field_id"] = field_id;
    j["label"] = label;
    j["value"] = value;
    j["type"] = static_cast<int>(type);
    j["visibility"] = static_cast<int>(visibility);
    j["display_order"] = display_order;
    j["is_verified"] = is_verified;
    j["created_at"] = created_at;
    j["updated_at"] = updated_at;
    return j.dump();
}

void CustomProfileField::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        field_id = j.value("field_id", "");
        label = j.value("label", "");
        value = j.value("value", "");
        type = static_cast<ProfileFieldType>(j.value("type", 0));
        visibility = static_cast<ProfileVisibility>(j.value("visibility", 0));
        display_order = j.value("display_order", 0);
        is_verified = j.value("is_verified", false);
        created_at = j.value("created_at", 0);
        updated_at = j.value("updated_at", 0);
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse custom field JSON: {}", e.what());
    }
}

// SocialLink implementation
SocialLink::SocialLink(const std::string& platform, const std::string& username, const std::string& url)
    : platform(platform), username(username), url(url), is_verified(false), 
      visibility(ProfileVisibility::PUBLIC) {
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    created_at = now;
    updated_at = now;
}

bool SocialLink::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> SocialLink::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (platform.empty()) {
        errors.push_back("Platform is required");
    }
    
    if (username.empty()) {
        errors.push_back("Username is required");
    }
    
    if (!url.empty()) {
        std::regex url_regex(R"(^https?://[^\s/$.?#].[^\s]*$)");
        if (!std::regex_match(url, url_regex)) {
            errors.push_back("Invalid URL format");
        }
    }
    
    return errors;
}

std::string SocialLink::to_json() const {
    nlohmann::json j;
    j["platform"] = platform;
    j["username"] = username;
    j["url"] = url;
    j["is_verified"] = is_verified;
    j["visibility"] = static_cast<int>(visibility);
    j["created_at"] = created_at;
    j["updated_at"] = updated_at;
    return j.dump();
}

void SocialLink::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        platform = j.value("platform", "");
        username = j.value("username", "");
        url = j.value("url", "");
        is_verified = j.value("is_verified", false);
        visibility = static_cast<ProfileVisibility>(j.value("visibility", 0));
        created_at = j.value("created_at", 0);
        updated_at = j.value("updated_at", 0);
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse social link JSON: {}", e.what());
    }
}

// ProfileAnalytics implementation
ProfileAnalytics::ProfileAnalytics(const std::string& user_id)
    : user_id(user_id), profile_views_today(0), profile_views_week(0), 
      profile_views_month(0), profile_views_total(0), unique_visitors_today(0),
      unique_visitors_week(0), unique_visitors_month(0), unique_visitors_total(0) {
    last_updated = std::time(nullptr);
}

void ProfileAnalytics::increment_view(const std::string& visitor_id) {
    profile_views_today++;
    profile_views_week++;
    profile_views_month++;
    profile_views_total++;
    
    // Check if this is a unique visitor
    auto it = std::find(recent_visitors.begin(), recent_visitors.end(), visitor_id);
    if (it == recent_visitors.end()) {
        unique_visitors_today++;
        unique_visitors_week++;
        unique_visitors_month++;
        unique_visitors_total++;
        
        // Add to recent visitors (keep only last 10)
        recent_visitors.insert(recent_visitors.begin(), visitor_id);
        if (recent_visitors.size() > 10) {
            recent_visitors.pop_back();
        }
    }
    
    last_updated = std::time(nullptr);
}

void ProfileAnalytics::reset_daily_stats() {
    profile_views_today = 0;
    unique_visitors_today = 0;
    last_updated = std::time(nullptr);
}

void ProfileAnalytics::reset_weekly_stats() {
    profile_views_week = 0;
    unique_visitors_week = 0;
    last_updated = std::time(nullptr);
}

void ProfileAnalytics::reset_monthly_stats() {
    profile_views_month = 0;
    unique_visitors_month = 0;
    last_updated = std::time(nullptr);
}

std::string ProfileAnalytics::to_json() const {
    nlohmann::json j;
    j["user_id"] = user_id;
    j["profile_views_today"] = profile_views_today;
    j["profile_views_week"] = profile_views_week;
    j["profile_views_month"] = profile_views_month;
    j["profile_views_total"] = profile_views_total;
    j["unique_visitors_today"] = unique_visitors_today;
    j["unique_visitors_week"] = unique_visitors_week;
    j["unique_visitors_month"] = unique_visitors_month;
    j["unique_visitors_total"] = unique_visitors_total;
    j["recent_visitors"] = recent_visitors;
    j["last_updated"] = last_updated;
    return j.dump();
}

void ProfileAnalytics::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        user_id = j.value("user_id", "");
        profile_views_today = j.value("profile_views_today", 0);
        profile_views_week = j.value("profile_views_week", 0);
        profile_views_month = j.value("profile_views_month", 0);
        profile_views_total = j.value("profile_views_total", 0);
        unique_visitors_today = j.value("unique_visitors_today", 0);
        unique_visitors_week = j.value("unique_visitors_week", 0);
        unique_visitors_month = j.value("unique_visitors_month", 0);
        unique_visitors_total = j.value("unique_visitors_total", 0);
        
        if (j.contains("recent_visitors")) {
            recent_visitors = j["recent_visitors"].get<std::vector<std::string>>();
        }
        
        last_updated = j.value("last_updated", 0);
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse profile analytics JSON: {}", e.what());
    }
}

// Profile implementation
Profile::Profile() {
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    created_at = now;
    updated_at = now;
    last_profile_update = now;
    visibility = ProfileVisibility::PUBLIC;
    is_featured = false;
    is_searchable = true;
    allow_indexing = true;
    is_identity_verified = false;
    is_address_verified = false;
    is_phone_verified = false;
    is_email_verified = false;
    completeness_score = 0.0;
    show_birth_year = false;
    show_join_date = true;
    show_last_seen = true;
    theme_color = "#1DA1F2";  // Twitter blue
    accent_color = "#657786";
}

Profile::Profile(const std::string& user_id) : Profile() {
    this->user_id = user_id;
    analytics = ProfileAnalytics(user_id);
}

Profile::Profile(const std::string& user_id, const std::string& display_name) : Profile(user_id) {
    this->display_name = display_name;
    calculate_completeness_score();
}

void Profile::update_display_name(const std::string& new_name) {
    display_name = new_name;
    updated_at = std::time(nullptr);
    last_profile_update = updated_at;
    calculate_completeness_score();
}

void Profile::update_bio(const std::string& new_bio) {
    bio = new_bio;
    updated_at = std::time(nullptr);
    last_profile_update = updated_at;
    calculate_completeness_score();
}

void Profile::update_location(const std::string& new_location) {
    location = new_location;
    updated_at = std::time(nullptr);
    last_profile_update = updated_at;
    calculate_completeness_score();
}

void Profile::update_website(const std::string& new_website) {
    website = new_website;
    updated_at = std::time(nullptr);
    last_profile_update = updated_at;
    calculate_completeness_score();
}

void Profile::update_avatar(const std::string& new_avatar_url) {
    avatar_url = new_avatar_url;
    updated_at = std::time(nullptr);
    last_profile_update = updated_at;
    calculate_completeness_score();
}

void Profile::update_banner(const std::string& new_banner_url) {
    banner_url = new_banner_url;
    updated_at = std::time(nullptr);
    last_profile_update = updated_at;
    calculate_completeness_score();
}

void Profile::update_theme_colors(const std::string& theme_color, const std::string& accent_color) {
    this->theme_color = theme_color;
    this->accent_color = accent_color;
    updated_at = std::time(nullptr);
}

void Profile::add_social_link(const SocialLink& link) {
    // Remove existing link for same platform
    remove_social_link(link.platform);
    
    social_links.push_back(link);
    updated_at = std::time(nullptr);
    calculate_completeness_score();
}

void Profile::remove_social_link(const std::string& platform) {
    auto it = std::remove_if(social_links.begin(), social_links.end(),
        [&platform](const SocialLink& link) { return link.platform == platform; });
    
    if (it != social_links.end()) {
        social_links.erase(it, social_links.end());
        updated_at = std::time(nullptr);
        calculate_completeness_score();
    }
}

void Profile::update_social_link(const std::string& platform, const SocialLink& updated_link) {
    auto it = std::find_if(social_links.begin(), social_links.end(),
        [&platform](const SocialLink& link) { return link.platform == platform; });
    
    if (it != social_links.end()) {
        *it = updated_link;
        it->updated_at = std::time(nullptr);
        updated_at = std::time(nullptr);
    }
}

std::optional<SocialLink> Profile::get_social_link(const std::string& platform) const {
    auto it = std::find_if(social_links.begin(), social_links.end(),
        [&platform](const SocialLink& link) { return link.platform == platform; });
    
    if (it != social_links.end()) {
        return *it;
    }
    return std::nullopt;
}

std::vector<SocialLink> Profile::get_visible_social_links(ProfileVisibility viewer_level) const {
    std::vector<SocialLink> visible_links;
    
    for (const auto& link : social_links) {
        if (static_cast<int>(link.visibility) <= static_cast<int>(viewer_level)) {
            visible_links.push_back(link);
        }
    }
    
    return visible_links;
}

void Profile::add_custom_field(const CustomProfileField& field) {
    custom_fields.push_back(field);
    updated_at = std::time(nullptr);
    calculate_completeness_score();
}

void Profile::remove_custom_field(const std::string& field_id) {
    auto it = std::remove_if(custom_fields.begin(), custom_fields.end(),
        [&field_id](const CustomProfileField& field) { return field.field_id == field_id; });
    
    if (it != custom_fields.end()) {
        custom_fields.erase(it, custom_fields.end());
        updated_at = std::time(nullptr);
        calculate_completeness_score();
    }
}

void Profile::update_custom_field(const std::string& field_id, const CustomProfileField& updated_field) {
    auto it = std::find_if(custom_fields.begin(), custom_fields.end(),
        [&field_id](const CustomProfileField& field) { return field.field_id == field_id; });
    
    if (it != custom_fields.end()) {
        *it = updated_field;
        it->updated_at = std::time(nullptr);
        updated_at = std::time(nullptr);
    }
}

void Profile::reorder_custom_fields(const std::vector<std::string>& field_order) {
    for (size_t i = 0; i < field_order.size(); ++i) {
        auto it = std::find_if(custom_fields.begin(), custom_fields.end(),
            [&field_order, i](const CustomProfileField& field) { 
                return field.field_id == field_order[i]; 
            });
        
        if (it != custom_fields.end()) {
            it->display_order = static_cast<int>(i);
        }
    }
    
    // Sort by display order
    std::sort(custom_fields.begin(), custom_fields.end(),
        [](const CustomProfileField& a, const CustomProfileField& b) {
            return a.display_order < b.display_order;
        });
    
    updated_at = std::time(nullptr);
}

std::vector<CustomProfileField> Profile::get_visible_custom_fields(ProfileVisibility viewer_level) const {
    std::vector<CustomProfileField> visible_fields;
    
    for (const auto& field : custom_fields) {
        if (static_cast<int>(field.visibility) <= static_cast<int>(viewer_level)) {
            visible_fields.push_back(field);
        }
    }
    
    return visible_fields;
}

void Profile::record_profile_view(const std::string& visitor_id) {
    analytics.increment_view(visitor_id);
}

ProfileAnalytics Profile::get_analytics() const {
    return analytics;
}

void Profile::update_analytics(const ProfileAnalytics& new_analytics) {
    analytics = new_analytics;
}

void Profile::calculate_completeness_score() {
    int completed_fields = 0;
    int total_fields = 15;  // Base fields
    
    if (!display_name.empty()) completed_fields++;
    if (!bio.empty()) completed_fields++;
    if (!location.empty()) completed_fields++;
    if (!website.empty()) completed_fields++;
    if (!avatar_url.empty()) completed_fields++;
    if (!banner_url.empty()) completed_fields++;
    if (!tagline.empty()) completed_fields++;
    if (!profession.empty()) completed_fields++;
    if (!company.empty()) completed_fields++;
    if (!education.empty()) completed_fields++;
    if (!pronouns.empty()) completed_fields++;
    if (birth_date.has_value()) completed_fields++;
    if (!contact_email.empty()) completed_fields++;
    if (!social_links.empty()) completed_fields++;
    if (!custom_fields.empty()) completed_fields++;
    
    completeness_score = (static_cast<double>(completed_fields) / total_fields) * 100.0;
    
    // Update missing fields list
    missing_fields.clear();
    if (display_name.empty()) missing_fields.push_back("display_name");
    if (bio.empty()) missing_fields.push_back("bio");
    if (location.empty()) missing_fields.push_back("location");
    if (website.empty()) missing_fields.push_back("website");
    if (avatar_url.empty()) missing_fields.push_back("avatar_url");
    if (banner_url.empty()) missing_fields.push_back("banner_url");
    if (tagline.empty()) missing_fields.push_back("tagline");
    if (profession.empty()) missing_fields.push_back("profession");
    if (company.empty()) missing_fields.push_back("company");
    if (education.empty()) missing_fields.push_back("education");
    if (pronouns.empty()) missing_fields.push_back("pronouns");
    if (!birth_date.has_value()) missing_fields.push_back("birth_date");
    if (contact_email.empty()) missing_fields.push_back("contact_email");
    if (social_links.empty()) missing_fields.push_back("social_links");
    if (custom_fields.empty()) missing_fields.push_back("custom_fields");
}

double Profile::get_completeness_percentage() const {
    return completeness_score;
}

std::vector<std::string> Profile::get_missing_profile_fields() const {
    return missing_fields;
}

bool Profile::is_profile_complete() const {
    return completeness_score >= 85.0;  // 85% considered complete
}

void Profile::set_verification_status(const std::string& badge_type, bool verified) {
    verification_badge_type = badge_type;
    
    if (badge_type == "identity") {
        is_identity_verified = verified;
    } else if (badge_type == "address") {
        is_address_verified = verified;
    } else if (badge_type == "phone") {
        is_phone_verified = verified;
    } else if (badge_type == "email") {
        is_email_verified = verified;
    }
    
    updated_at = std::time(nullptr);
}

bool Profile::is_verified() const {
    return is_identity_verified || is_address_verified || is_phone_verified || is_email_verified;
}

std::string Profile::get_verification_badge() const {
    return verification_badge_type;
}

void Profile::set_visibility(ProfileVisibility new_visibility) {
    visibility = new_visibility;
    updated_at = std::time(nullptr);
}

ProfileVisibility Profile::get_visibility() const {
    return visibility;
}

bool Profile::is_visible_to(ProfileVisibility viewer_level) const {
    return static_cast<int>(visibility) <= static_cast<int>(viewer_level);
}

bool Profile::is_field_visible_to(const std::string& field_name, ProfileVisibility viewer_level) const {
    // Basic fields are visible based on profile visibility
    if (field_name == "display_name" || field_name == "avatar_url" || field_name == "tagline") {
        return true;  // Always visible
    }
    
    // Other fields follow profile visibility
    return is_visible_to(viewer_level);
}

Profile Profile::get_public_view() const {
    Profile public_profile = *this;
    
    // Clear sensitive information
    public_profile.contact_email = "";
    public_profile.contact_phone = "";
    public_profile.birth_date = std::nullopt;
    public_profile.birth_location = "";
    
    // Filter social links and custom fields
    public_profile.social_links = get_visible_social_links(ProfileVisibility::PUBLIC);
    public_profile.custom_fields = get_visible_custom_fields(ProfileVisibility::PUBLIC);
    
    // Hide analytics for public view
    public_profile.analytics = ProfileAnalytics();
    
    return public_profile;
}

Profile Profile::get_follower_view() const {
    Profile follower_profile = *this;
    
    // Show more info to followers
    follower_profile.social_links = get_visible_social_links(ProfileVisibility::FOLLOWERS);
    follower_profile.custom_fields = get_visible_custom_fields(ProfileVisibility::FOLLOWERS);
    
    // Still hide sensitive info
    follower_profile.contact_phone = "";
    follower_profile.analytics = ProfileAnalytics();
    
    return follower_profile;
}

Profile Profile::get_friend_view() const {
    Profile friend_profile = *this;
    
    // Show most info to close friends
    friend_profile.social_links = get_visible_social_links(ProfileVisibility::FRIENDS);
    friend_profile.custom_fields = get_visible_custom_fields(ProfileVisibility::FRIENDS);
    
    // Hide analytics
    friend_profile.analytics = ProfileAnalytics();
    
    return friend_profile;
}

Profile Profile::get_self_view() const {
    return *this;  // Show everything to self
}

Profile Profile::get_view_for_relationship(ProfileVisibility viewer_level) const {
    switch (viewer_level) {
        case ProfileVisibility::PUBLIC:
            return get_public_view();
        case ProfileVisibility::FOLLOWERS:
            return get_follower_view();
        case ProfileVisibility::FRIENDS:
            return get_friend_view();
        case ProfileVisibility::PRIVATE:
            return get_self_view();
        default:
            return get_public_view();
    }
}

bool Profile::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> Profile::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (user_id.empty()) {
        errors.push_back("User ID is required");
    }
    
    if (display_name.length() > 100) {
        errors.push_back("Display name cannot exceed 100 characters");
    }
    
    if (bio.length() > 500) {
        errors.push_back("Bio cannot exceed 500 characters");
    }
    
    if (location.length() > 100) {
        errors.push_back("Location cannot exceed 100 characters");
    }
    
    if (!website.empty()) {
        std::regex url_regex(R"(^https?://[^\s/$.?#].[^\s]*$)");
        if (!std::regex_match(website, url_regex)) {
            errors.push_back("Invalid website URL format");
        }
    }
    
    if (tagline.length() > 200) {
        errors.push_back("Tagline cannot exceed 200 characters");
    }
    
    if (!contact_email.empty()) {
        std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
        if (!std::regex_match(contact_email, email_regex)) {
            errors.push_back("Invalid contact email format");
        }
    }
    
    // Validate social links
    for (const auto& link : social_links) {
        auto link_errors = link.get_validation_errors();
        errors.insert(errors.end(), link_errors.begin(), link_errors.end());
    }
    
    // Validate custom fields
    for (const auto& field : custom_fields) {
        auto field_errors = field.get_validation_errors();
        errors.insert(errors.end(), field_errors.begin(), field_errors.end());
    }
    
    return errors;
}

std::string Profile::to_json() const {
    nlohmann::json j;
    
    j["profile_id"] = profile_id;
    j["user_id"] = user_id;
    j["display_name"] = display_name;
    j["bio"] = bio;
    j["location"] = location;
    j["website"] = website;
    j["avatar_url"] = avatar_url;
    j["banner_url"] = banner_url;
    j["tagline"] = tagline;
    j["profession"] = profession;
    j["company"] = company;
    j["education"] = education;
    j["pronouns"] = pronouns;
    
    if (birth_date.has_value()) {
        j["birth_date"] = *birth_date;
    }
    
    j["birth_location"] = birth_location;
    j["contact_email"] = contact_email;
    j["contact_phone"] = contact_phone;
    j["theme_color"] = theme_color;
    j["accent_color"] = accent_color;
    j["background_image"] = background_image;
    j["show_birth_year"] = show_birth_year;
    j["show_join_date"] = show_join_date;
    j["show_last_seen"] = show_last_seen;
    j["visibility"] = static_cast<int>(visibility);
    j["is_featured"] = is_featured;
    j["is_searchable"] = is_searchable;
    j["allow_indexing"] = allow_indexing;
    j["is_identity_verified"] = is_identity_verified;
    j["is_address_verified"] = is_address_verified;
    j["is_phone_verified"] = is_phone_verified;
    j["is_email_verified"] = is_email_verified;
    j["verification_badge_type"] = verification_badge_type;
    j["completeness_score"] = completeness_score;
    j["missing_fields"] = missing_fields;
    j["created_at"] = created_at;
    j["updated_at"] = updated_at;
    j["last_profile_update"] = last_profile_update;
    
    // Serialize social links
    nlohmann::json social_links_json = nlohmann::json::array();
    for (const auto& link : social_links) {
        social_links_json.push_back(nlohmann::json::parse(link.to_json()));
    }
    j["social_links"] = social_links_json;
    
    // Serialize custom fields
    nlohmann::json custom_fields_json = nlohmann::json::array();
    for (const auto& field : custom_fields) {
        custom_fields_json.push_back(nlohmann::json::parse(field.to_json()));
    }
    j["custom_fields"] = custom_fields_json;
    
    // Serialize analytics
    j["analytics"] = nlohmann::json::parse(analytics.to_json());
    
    return j.dump();
}

void Profile::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        
        profile_id = j.value("profile_id", "");
        user_id = j.value("user_id", "");
        display_name = j.value("display_name", "");
        bio = j.value("bio", "");
        location = j.value("location", "");
        website = j.value("website", "");
        avatar_url = j.value("avatar_url", "");
        banner_url = j.value("banner_url", "");
        tagline = j.value("tagline", "");
        profession = j.value("profession", "");
        company = j.value("company", "");
        education = j.value("education", "");
        pronouns = j.value("pronouns", "");
        
        if (j.contains("birth_date")) {
            birth_date = j["birth_date"];
        }
        
        birth_location = j.value("birth_location", "");
        contact_email = j.value("contact_email", "");
        contact_phone = j.value("contact_phone", "");
        theme_color = j.value("theme_color", "#1DA1F2");
        accent_color = j.value("accent_color", "#657786");
        background_image = j.value("background_image", "");
        show_birth_year = j.value("show_birth_year", false);
        show_join_date = j.value("show_join_date", true);
        show_last_seen = j.value("show_last_seen", true);
        visibility = static_cast<ProfileVisibility>(j.value("visibility", 0));
        is_featured = j.value("is_featured", false);
        is_searchable = j.value("is_searchable", true);
        allow_indexing = j.value("allow_indexing", true);
        is_identity_verified = j.value("is_identity_verified", false);
        is_address_verified = j.value("is_address_verified", false);
        is_phone_verified = j.value("is_phone_verified", false);
        is_email_verified = j.value("is_email_verified", false);
        verification_badge_type = j.value("verification_badge_type", "");
        completeness_score = j.value("completeness_score", 0.0);
        
        if (j.contains("missing_fields")) {
            missing_fields = j["missing_fields"].get<std::vector<std::string>>();
        }
        
        created_at = j.value("created_at", 0);
        updated_at = j.value("updated_at", 0);
        last_profile_update = j.value("last_profile_update", 0);
        
        // Deserialize social links
        if (j.contains("social_links")) {
            social_links.clear();
            for (const auto& link_json : j["social_links"]) {
                SocialLink link;
                link.from_json(link_json.dump());
                social_links.push_back(link);
            }
        }
        
        // Deserialize custom fields
        if (j.contains("custom_fields")) {
            custom_fields.clear();
            for (const auto& field_json : j["custom_fields"]) {
                CustomProfileField field;
                field.from_json(field_json.dump());
                custom_fields.push_back(field);
            }
        }
        
        // Deserialize analytics
        if (j.contains("analytics")) {
            analytics.from_json(j["analytics"].dump());
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse profile JSON: {}", e.what());
    }
}

bool Profile::operator==(const Profile& other) const {
    return profile_id == other.profile_id && user_id == other.user_id;
}

bool Profile::operator!=(const Profile& other) const {
    return !(*this == other);
}

// ProfileUpdateRequest implementation
bool ProfileUpdateRequest::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> ProfileUpdateRequest::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (user_id.empty()) {
        errors.push_back("User ID is required");
    }
    
    if (display_name.has_value() && display_name->length() > 100) {
        errors.push_back("Display name cannot exceed 100 characters");
    }
    
    if (bio.has_value() && bio->length() > 500) {
        errors.push_back("Bio cannot exceed 500 characters");
    }
    
    if (location.has_value() && location->length() > 100) {
        errors.push_back("Location cannot exceed 100 characters");
    }
    
    if (website.has_value() && !website->empty()) {
        std::regex url_regex(R"(^https?://[^\s/$.?#].[^\s]*$)");
        if (!std::regex_match(*website, url_regex)) {
            errors.push_back("Invalid website URL format");
        }
    }
    
    if (tagline.has_value() && tagline->length() > 200) {
        errors.push_back("Tagline cannot exceed 200 characters");
    }
    
    if (contact_email.has_value() && !contact_email->empty()) {
        std::regex email_regex(R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})");
        if (!std::regex_match(*contact_email, email_regex)) {
            errors.push_back("Invalid contact email format");
        }
    }
    
    return errors;
}

std::vector<std::string> ProfileUpdateRequest::get_updated_fields() const {
    std::vector<std::string> fields;
    
    if (display_name.has_value()) fields.push_back("display_name");
    if (bio.has_value()) fields.push_back("bio");
    if (location.has_value()) fields.push_back("location");
    if (website.has_value()) fields.push_back("website");
    if (tagline.has_value()) fields.push_back("tagline");
    if (profession.has_value()) fields.push_back("profession");
    if (company.has_value()) fields.push_back("company");
    if (education.has_value()) fields.push_back("education");
    if (pronouns.has_value()) fields.push_back("pronouns");
    if (birth_date.has_value()) fields.push_back("birth_date");
    if (birth_location.has_value()) fields.push_back("birth_location");
    if (contact_email.has_value()) fields.push_back("contact_email");
    if (contact_phone.has_value()) fields.push_back("contact_phone");
    if (theme_color.has_value()) fields.push_back("theme_color");
    if (accent_color.has_value()) fields.push_back("accent_color");
    if (show_birth_year.has_value()) fields.push_back("show_birth_year");
    if (show_join_date.has_value()) fields.push_back("show_join_date");
    if (show_last_seen.has_value()) fields.push_back("show_last_seen");
    if (visibility.has_value()) fields.push_back("visibility");
    if (is_searchable.has_value()) fields.push_back("is_searchable");
    if (allow_indexing.has_value()) fields.push_back("allow_indexing");
    
    return fields;
}

// ProfileMediaUploadRequest implementation
bool ProfileMediaUploadRequest::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> ProfileMediaUploadRequest::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (user_id.empty()) {
        errors.push_back("User ID is required");
    }
    
    if (media_type != "avatar" && media_type != "banner") {
        errors.push_back("Media type must be 'avatar' or 'banner'");
    }
    
    if (file_path.empty()) {
        errors.push_back("File path is required");
    }
    
    if (mime_type.empty()) {
        errors.push_back("MIME type is required");
    }
    
    if (mime_type.find("image/") != 0) {
        errors.push_back("File must be an image");
    }
    
    // Size limits: 2MB for avatar, 8MB for banner
    size_t max_size = (media_type == "avatar") ? 2 * 1024 * 1024 : 8 * 1024 * 1024;
    if (file_size > max_size) {
        errors.push_back("File size exceeds maximum allowed size");
    }
    
    return errors;
}

// Utility functions
std::string profile_visibility_to_string(ProfileVisibility visibility) {
    switch (visibility) {
        case ProfileVisibility::PUBLIC: return "public";
        case ProfileVisibility::FOLLOWERS: return "followers";
        case ProfileVisibility::FRIENDS: return "friends";
        case ProfileVisibility::PRIVATE: return "private";
        default: return "public";
    }
}

ProfileVisibility string_to_profile_visibility(const std::string& visibility) {
    if (visibility == "public") return ProfileVisibility::PUBLIC;
    if (visibility == "followers") return ProfileVisibility::FOLLOWERS;
    if (visibility == "friends") return ProfileVisibility::FRIENDS;
    if (visibility == "private") return ProfileVisibility::PRIVATE;
    return ProfileVisibility::PUBLIC;
}

std::string profile_field_type_to_string(ProfileFieldType type) {
    switch (type) {
        case ProfileFieldType::TEXT: return "text";
        case ProfileFieldType::URL: return "url";
        case ProfileFieldType::EMAIL: return "email";
        case ProfileFieldType::PHONE: return "phone";
        case ProfileFieldType::DATE: return "date";
        case ProfileFieldType::LOCATION: return "location";
        case ProfileFieldType::SOCIAL_LINK: return "social_link";
        default: return "text";
    }
}

ProfileFieldType string_to_profile_field_type(const std::string& type) {
    if (type == "text") return ProfileFieldType::TEXT;
    if (type == "url") return ProfileFieldType::URL;
    if (type == "email") return ProfileFieldType::EMAIL;
    if (type == "phone") return ProfileFieldType::PHONE;
    if (type == "date") return ProfileFieldType::DATE;
    if (type == "location") return ProfileFieldType::LOCATION;
    if (type == "social_link") return ProfileFieldType::SOCIAL_LINK;
    return ProfileFieldType::TEXT;
}

} // namespace sonet::user::models