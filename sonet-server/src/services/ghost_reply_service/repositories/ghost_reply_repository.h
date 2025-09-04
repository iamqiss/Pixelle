/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../ghost_reply_service.h"
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <functional>

namespace sonet::ghost_reply::repositories {

using json = nlohmann::json;

/**
 * Ghost Reply Repository Interface
 * Handles all database operations for ghost replies
 */
class GhostReplyRepository {
public:
    virtual ~GhostReplyRepository() = default;

    // Core CRUD operations
    virtual std::optional<GhostReply> create_ghost_reply(const CreateGhostReplyRequest& request) = 0;
    virtual std::optional<GhostReply> get_ghost_reply_by_id(const std::string& ghost_reply_id) = 0;
    virtual std::optional<GhostReply> get_ghost_reply_by_ghost_id(const std::string& ghost_id) = 0;
    virtual bool update_ghost_reply(const std::string& ghost_reply_id, const json& update_data) = 0;
    virtual bool delete_ghost_reply(const std::string& ghost_reply_id) = 0;
    virtual bool soft_delete_ghost_reply(const std::string& ghost_reply_id) = 0;

    // Query operations
    virtual std::vector<GhostReply> get_ghost_replies_by_thread_id(const std::string& thread_id, int limit = 20, int offset = 0) = 0;
    virtual std::vector<GhostReply> get_ghost_replies_by_parent_note_id(const std::string& note_id, int limit = 20, int offset = 0) = 0;
    virtual std::vector<GhostReply> get_ghost_replies_by_moderation_status(const std::string& status, int limit = 50, int offset = 0) = 0;
    virtual std::vector<GhostReply> get_ghost_replies_by_avatar(const std::string& avatar_id, int limit = 20, int offset = 0) = 0;
    
    // Search operations
    virtual std::vector<GhostReply> search_ghost_replies_by_content(const std::string& query, int limit = 20) = 0;
    virtual std::vector<GhostReply> search_ghost_replies_by_tags(const std::vector<std::string>& tags, int limit = 20) = 0;
    virtual std::vector<GhostReply> search_ghost_replies_full_text(const std::string& query, int limit = 20) = 0;
    
    // Pagination and cursor-based queries
    virtual std::vector<GhostReply> get_ghost_replies_with_cursor(
        const std::string& thread_id, 
        const std::string& cursor, 
        int limit = 20
    ) = 0;
    
    // Statistics and analytics
    virtual int get_ghost_reply_count_by_thread_id(const std::string& thread_id) = 0;
    virtual int get_ghost_reply_count_by_parent_note_id(const std::string& note_id) = 0;
    virtual int get_ghost_reply_count_by_moderation_status(const std::string& status) = 0;
    virtual json get_ghost_reply_engagement_stats(const std::string& ghost_reply_id) = 0;
    
    // Thread tracking
    virtual bool create_or_update_thread_tracking(const std::string& thread_id, const std::string& note_id) = 0;
    virtual std::optional<json> get_thread_tracking(const std::string& thread_id) = 0;
    virtual bool increment_thread_ghost_reply_count(const std::string& thread_id) = 0;
    virtual bool decrement_thread_ghost_reply_count(const std::string& thread_id) = 0;
    
    // Media operations
    virtual bool add_media_to_ghost_reply(const std::string& ghost_reply_id, const json& media_data) = 0;
    virtual bool remove_media_from_ghost_reply(const std::string& ghost_reply_id, const std::string& media_id) = 0;
    virtual std::vector<json> get_ghost_reply_media(const std::string& ghost_reply_id) = 0;
    virtual bool update_ghost_reply_media_order(const std::string& ghost_reply_id, const std::vector<std::string>& media_order) = 0;
    
    // Engagement operations
    virtual bool add_ghost_reply_like(const std::string& ghost_reply_id, const std::string& anonymous_user_hash) = 0;
    virtual bool remove_ghost_reply_like(const std::string& ghost_reply_id, const std::string& anonymous_user_hash) = 0;
    virtual bool has_user_liked_ghost_reply(const std::string& ghost_reply_id, const std::string& anonymous_user_hash) = 0;
    virtual int get_ghost_reply_like_count(const std::string& ghost_reply_id) = 0;
    virtual void increment_ghost_reply_view_count(const std::string& ghost_reply_id) = 0;
    
    // Moderation operations
    virtual bool log_moderation_action(const GhostReplyModerationAction& action) = 0;
    virtual std::vector<json> get_moderation_log_for_ghost_reply(const std::string& ghost_reply_id, int limit = 50) = 0;
    virtual std::vector<json> get_moderation_log_by_moderator(const std::string& moderator_id, int limit = 50) = 0;
    
    // Analytics operations
    virtual bool create_ghost_reply_analytics_entry(const std::string& ghost_reply_id, const json& analytics_data) = 0;
    virtual bool update_ghost_reply_analytics(const std::string& ghost_reply_id, const json& analytics_data) = 0;
    virtual json get_ghost_reply_analytics(const std::string& ghost_reply_id, const std::string& date, int hour = -1) = 0;
    virtual std::vector<json> get_ghost_reply_analytics_by_date_range(
        const std::string& ghost_reply_id, 
        const std::string& start_date, 
        const std::string& end_date
    ) = 0;
    
    // Ghost avatar operations
    virtual std::vector<json> get_all_ghost_avatars() = 0;
    virtual std::optional<json> get_ghost_avatar_by_id(const std::string& avatar_id) = 0;
    virtual bool update_ghost_avatar_usage_count(const std::string& avatar_id) = 0;
    virtual bool is_ghost_avatar_active(const std::string& avatar_id) = 0;
    
    // Ghost ID operations
    virtual bool is_ghost_id_unique(const std::string& ghost_id) = 0;
    virtual std::string generate_unique_ghost_id() = 0;
    
    // Content analysis storage
    virtual bool store_content_analysis_results(
        const std::string& ghost_reply_id, 
        double spam_score, 
        double toxicity_score, 
        const std::vector<std::string>& detected_languages
    ) = 0;
    
    // Batch operations
    virtual bool batch_create_ghost_replies(const std::vector<CreateGhostReplyRequest>& requests) = 0;
    virtual bool batch_update_ghost_replies(const std::vector<std::pair<std::string, json>>& updates) = 0;
    virtual bool batch_delete_ghost_replies(const std::vector<std::string>& ghost_reply_ids) = 0;
    
    // Cleanup operations
    virtual bool cleanup_deleted_ghost_replies(int days_old = 30) = 0;
    virtual bool cleanup_old_analytics(int days_old = 90) = 0;
    virtual bool cleanup_old_moderation_logs(int days_old = 365) = 0;
    
    // Database maintenance
    virtual bool vacuum_ghost_reply_tables() = 0;
    virtual bool reindex_ghost_reply_tables() = 0;
    virtual json get_database_stats() = 0;
    
    // Transaction support
    virtual bool begin_transaction() = 0;
    virtual bool commit_transaction() = 0;
    virtual bool rollback_transaction() = 0;
    
    // Connection management
    virtual bool is_connected() const = 0;
    virtual bool ping() = 0;
    virtual void close_connection() = 0;
    
    // Error handling
    virtual std::string get_last_error() const = 0;
    virtual void clear_last_error() = 0;
    virtual bool has_error() const = 0;
};

} // namespace sonet::ghost_reply::repositories