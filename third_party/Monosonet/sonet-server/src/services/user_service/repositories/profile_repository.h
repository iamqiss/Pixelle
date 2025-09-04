/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../include/repository.h"
#include "../models/profile.h"
#include <pqxx/pqxx>
#include <memory>
#include <optional>
#include <vector>
#include <string>

namespace sonet::user::repositories {

using namespace sonet::user::models;

// postgresql implementation of ProfileRepository
class PostgreSQLProfileRepository : public IProfileRepository {
private:
    std::shared_ptr<pqxx::connection> db_connection_;
    std::string profiles_table_;
    std::string social_links_table_;
    std::string custom_fields_table_;
    std::string profile_analytics_table_;
    std::string profile_views_table_;
    
    // Database connection management
    void ensure_connection();
    void reconnect_if_needed();
    bool test_connection();
    
    // Query builders
    std::string build_select_query(const std::vector<std::string>& fields = {}) const;
    std::string build_insert_query(const Profile& profile) const;
    std::string build_update_query(const Profile& profile) const;
    std::string build_search_query(const ProfileSearchCriteria& criteria) const;
    
    // Result mapping
    Profile map_row_to_profile(const pqxx::row& row) const;
    std::vector<Profile> map_result_to_profiles(const pqxx::result& result) const;
    SocialLink map_row_to_social_link(const pqxx::row& row) const;
    CustomProfileField map_row_to_custom_field(const pqxx::row& row) const;
    ProfileAnalytics map_row_to_analytics(const pqxx::row& row) const;
    
    // Helper methods for complex operations
    void update_social_links(pqxx::work& txn, const Profile& profile);
    void update_custom_fields(pqxx::work& txn, const Profile& profile);
    void update_profile_analytics(pqxx::work& txn, const Profile& profile);
    void load_profile_relations(Profile& profile) const;
    
    // Validation and security
    bool validate_profile_data(const Profile& profile) const;
    void log_profile_operation(const std::string& operation, const std::string& profile_id) const;

public:
    explicit PostgreSQLProfileRepository(std::shared_ptr<pqxx::connection> connection);
    virtual ~PostgreSQLProfileRepository() = default;
    
    // Basic CRUD operations
    bool create(const Profile& profile) override;
    std::optional<Profile> get_by_id(const std::string& profile_id) override;
    std::optional<Profile> get_by_user_id(const std::string& user_id) override;
    bool update(const Profile& profile) override;
    bool delete_profile(const std::string& profile_id) override;
    
    // Batch operations
    std::vector<Profile> get_by_user_ids(const std::vector<std::string>& user_ids) override;
    bool update_multiple(const std::vector<Profile>& profiles) override;
    
    // Profile search and discovery
    SearchResult<Profile> search(const ProfileSearchCriteria& criteria) override;
    std::vector<Profile> get_featured_profiles(int limit, int offset) override;
    std::vector<Profile> get_verified_profiles(int limit, int offset) override;
    std::vector<Profile> get_profiles_by_location(const std::string& location, int limit) override;
    std::vector<Profile> get_profiles_by_profession(const std::string& profession, int limit) override;
    std::vector<Profile> get_recently_updated(int limit, int hours_back) override;
    
    // Profile media management
    bool update_avatar(const std::string& user_id, const std::string& avatar_url) override;
    bool update_banner(const std::string& user_id, const std::string& banner_url) override;
    bool remove_avatar(const std::string& user_id) override;
    bool remove_banner(const std::string& user_id) override;
    
    // Social links management
    bool add_social_link(const std::string& user_id, const SocialLink& link) override;
    bool update_social_link(const std::string& user_id, const SocialLink& link) override;
    bool remove_social_link(const std::string& user_id, const std::string& platform) override;
    std::vector<SocialLink> get_social_links(const std::string& user_id) override;
    
    // Custom fields management
    bool add_custom_field(const std::string& user_id, const CustomProfileField& field) override;
    bool update_custom_field(const std::string& user_id, const CustomProfileField& field) override;
    bool remove_custom_field(const std::string& user_id, const std::string& field_id) override;
    bool reorder_custom_fields(const std::string& user_id, const std::vector<std::string>& field_order) override;
    std::vector<CustomProfileField> get_custom_fields(const std::string& user_id) override;
    
    // Profile analytics
    bool record_profile_view(const std::string& user_id, const std::string& viewer_id,
                            const std::string& source = "") override;
    ProfileAnalytics get_profile_analytics(const std::string& user_id) override;
    bool update_profile_analytics(const std::string& user_id, const ProfileAnalytics& analytics) override;
    std::vector<std::string> get_recent_profile_visitors(const std::string& user_id, int limit) override;
    
    // Profile verification
    bool set_verification_status(const std::string& user_id, const std::string& badge_type, bool verified) override;
    bool is_profile_verified(const std::string& user_id) override;
    std::string get_verification_badge(const std::string& user_id) override;
    
    // Profile visibility and privacy
    bool update_visibility(const std::string& user_id, ProfileVisibility visibility) override;
    ProfileVisibility get_visibility(const std::string& user_id) override;
    bool is_profile_visible_to(const std::string& user_id, const std::string& viewer_id) override;
    
    // Profile completion and recommendations
    double calculate_profile_completeness(const std::string& user_id) override;
    std::vector<std::string> get_missing_profile_fields(const std::string& user_id) override;
    std::vector<Profile> get_incomplete_profiles(double threshold = 50.0, int limit = 100) override;
    
    // Profile statistics and metrics
    int count_total_profiles() override;
    int count_public_profiles() override;
    int count_verified_profiles() override;
    std::map<std::string, int> get_profile_completion_stats() override;
    std::map<std::string, int> get_verification_stats() override;
    
    // Profile maintenance and cleanup
    bool cleanup_inactive_profiles(int months_inactive = 12) override;
    bool optimize_profile_search() override;
    ProfileMaintenanceResult perform_maintenance() override;
    
    // Advanced search and filtering
    std::vector<Profile> search_by_keywords(const std::vector<std::string>& keywords, int limit) override;
    std::vector<Profile> find_similar_profiles(const std::string& user_id, int limit) override;
    std::vector<Profile> get_trending_profiles(int hours_back = 24, int limit = 50) override;
    
    // Profile recommendations
    std::vector<Profile> get_recommended_profiles(const std::string& user_id, int limit) override;
    std::vector<Profile> get_profiles_to_follow(const std::string& user_id, int limit) override;
    
    // Additional postgresql-specific methods
    bool create_profile_indexes();
    bool optimize_profile_performance();
    std::string get_connection_info() const;
    bool backup_profile_data(const std::string& backup_path);
    bool restore_profile_data(const std::string& backup_path);
    
private:
    // Internal helper methods
    void setup_prepared_statements();
    void create_database_schema();
    void migrate_database_schema();
    bool validate_database_schema() const;
    void handle_database_error(const std::exception& e, const std::string& operation) const;
    
    // Profile relationship helpers
    void load_social_links(Profile& profile) const;
    void load_custom_fields(Profile& profile) const;
    void load_analytics(Profile& profile) const;
    
    // Search and indexing helpers
    std::string build_full_text_search_query(const std::vector<std::string>& keywords) const;
    void update_search_index(const Profile& profile);
    void remove_from_search_index(const std::string& profile_id);
};

// Profile view tracking for analytics
class ProfileViewTracker {
private:
    std::shared_ptr<pqxx::connection> db_connection_;
    std::string views_table_;
    
public:
    explicit ProfileViewTracker(std::shared_ptr<pqxx::connection> connection);
    
    bool track_view(const std::string& profile_user_id, const std::string& viewer_id,
                   const std::string& ip_address, const std::string& user_agent,
                   const std::string& referrer = "");
    
    bool is_duplicate_view(const std::string& profile_user_id, const std::string& viewer_id,
                          int minutes_window = 30);
    
    int get_view_count(const std::string& profile_user_id, int hours_back = 24);
    
    std::vector<std::string> get_recent_viewers(const std::string& profile_user_id, int limit = 10);
    
    std::map<std::string, int> get_view_statistics(const std::string& profile_user_id, int days_back = 30);
    
    bool cleanup_old_views(int days_old = 90);
};

// Profile recommendation engine
class ProfileRecommendationEngine {
private:
    std::shared_ptr<pqxx::connection> db_connection_;
    
    struct RecommendationScore {
        std::string user_id;
        double score;
        std::vector<std::string> reasons;
    };
    
public:
    explicit ProfileRecommendationEngine(std::shared_ptr<pqxx::connection> connection);
    
    std::vector<Profile> get_recommendations(const std::string& user_id, int limit = 20);
    
    std::vector<Profile> get_location_based_recommendations(const std::string& user_id, int limit = 10);
    
    std::vector<Profile> get_interest_based_recommendations(const std::string& user_id, int limit = 10);
    
    std::vector<Profile> get_mutual_connection_recommendations(const std::string& user_id, int limit = 10);
    
    std::vector<Profile> get_trending_recommendations(int hours_back = 24, int limit = 10);
    
private:
    double calculate_location_similarity(const Profile& profile1, const Profile& profile2);
    double calculate_interest_similarity(const Profile& profile1, const Profile& profile2);
    double calculate_social_similarity(const std::string& user_id1, const std::string& user_id2);
    std::vector<RecommendationScore> score_candidates(const std::string& user_id, 
                                                     const std::vector<std::string>& candidate_ids);
};

// Factory for creating profile repository instances
class ProfileRepositoryFactory {
public:
    static std::unique_ptr<PostgreSQLProfileRepository> create_profile_repository(
        const std::string& connection_string);
    
    static std::unique_ptr<ProfileViewTracker> create_view_tracker(
        const std::string& connection_string);
    
    static std::unique_ptr<ProfileRecommendationEngine> create_recommendation_engine(
        const std::string& connection_string);
    
    static std::shared_ptr<pqxx::connection> create_database_connection(
        const std::string& connection_string);
};

} // namespace sonet::user::repositories