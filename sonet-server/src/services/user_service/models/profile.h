/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <ctime>
#include <memory>

namespace sonet::user::models {

enum class ProfileVisibility {
    PUBLIC,      // Visible to everyone
    FOLLOWERS,   // Visible to followers only
    FRIENDS,     // Visible to close friends only
    PRIVATE      // Visible to user only
};

enum class ProfileFieldType {
    TEXT,
    URL,
    EMAIL,
    PHONE,
    DATE,
    LOCATION,
    SOCIAL_LINK
};

struct CustomProfileField {
    std::string field_id;
    std::string label;
    std::string value;
    ProfileFieldType type;
    ProfileVisibility visibility;
    int display_order;
    bool is_verified;
    std::time_t created_at;
    std::time_t updated_at;
    
    CustomProfileField() = default;
    CustomProfileField(const std::string& label, const std::string& value, 
                      ProfileFieldType type = ProfileFieldType::TEXT,
                      ProfileVisibility visibility = ProfileVisibility::PUBLIC);
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    std::string to_json() const;
    void from_json(const std::string& json);
};

struct SocialLink {
    std::string platform;      // twitter, instagram, linkedin, etc.
    std::string username;      // username on that platform
    std::string url;          // full URL to profile
    bool is_verified;         // platform verification status
    ProfileVisibility visibility;
    std::time_t created_at;
    std::time_t updated_at;
    
    SocialLink() = default;
    SocialLink(const std::string& platform, const std::string& username, const std::string& url);
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    std::string to_json() const;
    void from_json(const std::string& json);
};

struct ProfileAnalytics {
    std::string user_id;
    int profile_views_today;
    int profile_views_week;
    int profile_views_month;
    int profile_views_total;
    int unique_visitors_today;
    int unique_visitors_week;
    int unique_visitors_month;
    int unique_visitors_total;
    std::vector<std::string> recent_visitors;  // Last 10 visitors
    std::time_t last_updated;
    
    ProfileAnalytics() = default;
    ProfileAnalytics(const std::string& user_id);
    
    void increment_view(const std::string& visitor_id);
    void reset_daily_stats();
    void reset_weekly_stats();
    void reset_monthly_stats();
    std::string to_json() const;
    void from_json(const std::string& json);
};

class Profile {
public:
    // Core identifiers
    std::string profile_id;
    std::string user_id;
    
    // Basic profile information
    std::string display_name;
    std::string bio;
    std::string location;
    std::string website;
    std::string avatar_url;
    std::string banner_url;
    
    // Extended profile information
    std::string tagline;           // Short one-liner
    std::string profession;        // Job title/occupation
    std::string company;          // Company name
    std::string education;        // Educational background
    std::string pronouns;         // He/Him, She/Her, They/Them, etc.
    std::optional<std::time_t> birth_date;  // Optional birthday
    std::string birth_location;   // Birth city/country
    
    // Contact information
    std::string contact_email;    // Public contact email (different from login email)
    std::string contact_phone;    // Public contact phone
    
    // Profile customization
    std::string theme_color;      // Hex color for profile theme
    std::string accent_color;     // Secondary color
    std::string background_image; // Custom background image URL
    bool show_birth_year;         // Whether to show birth year publicly
    bool show_join_date;          // Whether to show account creation date
    bool show_last_seen;          // Whether to show last active time
    
    // Social links
    std::vector<SocialLink> social_links;
    
    // Custom fields
    std::vector<CustomProfileField> custom_fields;
    
    // Profile status
    ProfileVisibility visibility;
    bool is_featured;             // Featured profile (for discovery)
    bool is_searchable;           // Appears in search results
    bool allow_indexing;          // Allow search engine indexing
    
    // Analytics and metrics
    ProfileAnalytics analytics;
    
    // Verification status
    bool is_identity_verified;    // Government ID verified
    bool is_address_verified;     // Address verified
    bool is_phone_verified;       // Phone number verified
    bool is_email_verified;       // Email verified
    std::string verification_badge_type; // blue, gold, business, etc.
    
    // Profile completion
    double completeness_score;    // 0-100% profile completion
    std::vector<std::string> missing_fields; // Fields needed for 100% completion
    
    // Timestamps
    std::time_t created_at;
    std::time_t updated_at;
    std::time_t last_profile_update;
    
    // Constructors
    Profile();
    Profile(const std::string& user_id);
    Profile(const std::string& user_id, const std::string& display_name);
    
    // Profile management
    void update_display_name(const std::string& new_name);
    void update_bio(const std::string& new_bio);
    void update_location(const std::string& new_location);
    void update_website(const std::string& new_website);
    void update_avatar(const std::string& new_avatar_url);
    void update_banner(const std::string& new_banner_url);
    void update_theme_colors(const std::string& theme_color, const std::string& accent_color);
    
    // Social links management
    void add_social_link(const SocialLink& link);
    void remove_social_link(const std::string& platform);
    void update_social_link(const std::string& platform, const SocialLink& updated_link);
    std::optional<SocialLink> get_social_link(const std::string& platform) const;
    std::vector<SocialLink> get_visible_social_links(ProfileVisibility viewer_level) const;
    
    // Custom fields management
    void add_custom_field(const CustomProfileField& field);
    void remove_custom_field(const std::string& field_id);
    void update_custom_field(const std::string& field_id, const CustomProfileField& updated_field);
    void reorder_custom_fields(const std::vector<std::string>& field_order);
    std::vector<CustomProfileField> get_visible_custom_fields(ProfileVisibility viewer_level) const;
    
    // Analytics
    void record_profile_view(const std::string& visitor_id);
    ProfileAnalytics get_analytics() const;
    void update_analytics(const ProfileAnalytics& new_analytics);
    
    // Profile completion
    void calculate_completeness_score();
    double get_completeness_percentage() const;
    std::vector<std::string> get_missing_profile_fields() const;
    bool is_profile_complete() const;
    
    // Verification
    void set_verification_status(const std::string& badge_type, bool verified);
    bool is_verified() const;
    std::string get_verification_badge() const;
    
    // Privacy and visibility
    void set_visibility(ProfileVisibility new_visibility);
    ProfileVisibility get_visibility() const;
    bool is_visible_to(ProfileVisibility viewer_level) const;
    bool is_field_visible_to(const std::string& field_name, ProfileVisibility viewer_level) const;
    
    // Profile views based on viewer relationship
    Profile get_public_view() const;
    Profile get_follower_view() const;
    Profile get_friend_view() const;
    Profile get_self_view() const;
    Profile get_view_for_relationship(ProfileVisibility viewer_level) const;
    
    // Validation
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    
    // Serialization
    std::string to_json() const;
    void from_json(const std::string& json);
    
    // Comparison operators
    bool operator==(const Profile& other) const;
    bool operator!=(const Profile& other) const;
};

// Profile management requests
struct ProfileUpdateRequest {
    std::string user_id;
    std::optional<std::string> display_name;
    std::optional<std::string> bio;
    std::optional<std::string> location;
    std::optional<std::string> website;
    std::optional<std::string> tagline;
    std::optional<std::string> profession;
    std::optional<std::string> company;
    std::optional<std::string> education;
    std::optional<std::string> pronouns;
    std::optional<std::time_t> birth_date;
    std::optional<std::string> birth_location;
    std::optional<std::string> contact_email;
    std::optional<std::string> contact_phone;
    std::optional<std::string> theme_color;
    std::optional<std::string> accent_color;
    std::optional<bool> show_birth_year;
    std::optional<bool> show_join_date;
    std::optional<bool> show_last_seen;
    std::optional<ProfileVisibility> visibility;
    std::optional<bool> is_searchable;
    std::optional<bool> allow_indexing;
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    std::vector<std::string> get_updated_fields() const;
};

struct ProfileMediaUploadRequest {
    std::string user_id;
    std::string media_type;  // "avatar" or "banner"
    std::string file_path;   // Path to uploaded file
    std::string mime_type;
    size_t file_size;
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
};

// Utility functions
std::string profile_visibility_to_string(ProfileVisibility visibility);
ProfileVisibility string_to_profile_visibility(const std::string& visibility);
std::string profile_field_type_to_string(ProfileFieldType type);
ProfileFieldType string_to_profile_field_type(const std::string& type);

} // namespace sonet::user::models