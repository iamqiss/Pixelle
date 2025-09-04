/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/note.h"
#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <pqxx/pqxx>

namespace sonet::note::repositories {

using namespace sonet::note::models;

/**
 * Abstract repository interface for Note operations
 * Defines the contract for note data persistence
 */
class NoteRepository {
public:
    virtual ~NoteRepository() = default;
    
    // Core CRUD operations
    virtual bool create(const Note& note) = 0;
    virtual std::optional<Note> get_by_id(const std::string& note_id) = 0;
    virtual bool update(const Note& note) = 0;
    virtual bool delete_note(const std::string& note_id) = 0;
    
    // Batch operations
    virtual std::vector<Note> get_by_ids(const std::vector<std::string>& note_ids) = 0;
    virtual bool create_batch(const std::vector<Note>& notes) = 0;
    virtual bool update_batch(const std::vector<Note>& notes) = 0;
    virtual bool delete_batch(const std::vector<std::string>& note_ids) = 0;
    
    // User-based queries
    virtual std::vector<Note> get_by_user_id(const std::string& user_id, int limit = 20, int offset = 0) = 0;
    virtual int count_by_user_id(const std::string& user_id) = 0;
    virtual std::vector<Note> get_user_timeline(const std::string& user_id, int limit = 20, int offset = 0) = 0;
    
    // Timeline operations
    virtual std::vector<Note> get_timeline_for_users(const std::vector<std::string>& user_ids, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_public_notes(int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_trending_notes(int hours_back = 24, int limit = 20) = 0;
    virtual std::vector<Note> get_recent_notes(int hours_back = 24, int limit = 20) = 0;
    
    // Engagement operations
    virtual std::vector<Note> get_liked_by_user(const std::string& user_id, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_renoted_by_user(const std::string& user_id, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_bookmarked_by_user(const std::string& user_id, int limit = 20, int offset = 0) = 0;
    
    // Relationship operations
    virtual std::vector<Note> get_replies(const std::string& note_id, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_quotes(const std::string& note_id, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_renotes(const std::string& note_id, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_thread(const std::string& thread_id) = 0;
    
    // Search operations
    virtual std::vector<Note> search_notes(const std::string& query, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> search_by_content(const std::string& content, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_by_hashtag(const std::string& hashtag, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_by_mention(const std::string& user_id, int limit = 20, int offset = 0) = 0;
    
    // Status-based queries
    virtual std::vector<Note> get_drafts(const std::string& user_id, int limit = 20) = 0;
    virtual std::vector<Note> get_scheduled_notes(const std::string& user_id, int limit = 20) = 0;
    virtual std::vector<Note> get_flagged_notes(int limit = 20, int offset = 0) = 0;
    virtual std::vector<Note> get_deleted_notes(const std::string& user_id, int limit = 20) = 0;
    
    // Analytics operations
    virtual int get_total_notes_count() = 0;
    virtual int get_notes_count_by_timeframe(int hours_back) = 0;
    virtual std::vector<std::pair<std::string, int>> get_top_hashtags(int limit = 10, int hours_back = 24) = 0;
    virtual std::vector<std::pair<std::string, int>> get_trending_topics(int limit = 10, int hours_back = 24) = 0;
    
    // Maintenance operations
    virtual bool cleanup_deleted_notes(int days_old) = 0;
    virtual bool cleanup_old_drafts(int days_old) = 0;
    virtual bool optimize_database() = 0;
    virtual bool rebuild_indexes() = 0;
};

/**
 * postgresql implementation of NoteRepository
 * Provides full-featured note persistence with Cassandra-like performance optimizations
 */
class NotegreSQLNoteRepository : public NoteRepository {
private:
    std::shared_ptr<pqxx::connection> db_connection_;
    std::string notes_table_;
    std::string note_metrics_table_;
    std::string note_hashtags_table_;
    std::string note_mentions_table_;
    std::string note_urls_table_;
    std::string user_interactions_table_;
    
public:
    explicit NotegreSQLNoteRepository(std::shared_ptr<pqxx::connection> connection);
    ~NotegreSQLNoteRepository() override = default;
    
    // Core CRUD operations
    bool create(const Note& note) override;
    std::optional<Note> get_by_id(const std::string& note_id) override;
    bool update(const Note& note) override;
    bool delete_note(const std::string& note_id) override;
    
    // Batch operations
    std::vector<Note> get_by_ids(const std::vector<std::string>& note_ids) override;
    bool create_batch(const std::vector<Note>& notes) override;
    bool update_batch(const std::vector<Note>& notes) override;
    bool delete_batch(const std::vector<std::string>& note_ids) override;
    
    // User-based queries
    std::vector<Note> get_by_user_id(const std::string& user_id, int limit = 20, int offset = 0) override;
    int count_by_user_id(const std::string& user_id) override;
    std::vector<Note> get_user_timeline(const std::string& user_id, int limit = 20, int offset = 0) override;
    
    // Timeline operations
    std::vector<Note> get_timeline_for_users(const std::vector<std::string>& user_ids, int limit = 20, int offset = 0) override;
    std::vector<Note> get_public_notes(int limit = 20, int offset = 0) override;
    std::vector<Note> get_trending_notes(int hours_back = 24, int limit = 20) override;
    std::vector<Note> get_recent_notes(int hours_back = 24, int limit = 20) override;
    
    // Engagement operations
    std::vector<Note> get_liked_by_user(const std::string& user_id, int limit = 20, int offset = 0) override;
    std::vector<Note> get_renoted_by_user(const std::string& user_id, int limit = 20, int offset = 0) override;
    std::vector<Note> get_bookmarked_by_user(const std::string& user_id, int limit = 20, int offset = 0) override;
    
    // Relationship operations
    std::vector<Note> get_replies(const std::string& note_id, int limit = 20, int offset = 0) override;
    std::vector<Note> get_quotes(const std::string& note_id, int limit = 20, int offset = 0) override;
    std::vector<Note> get_renotes(const std::string& note_id, int limit = 20, int offset = 0) override;
    std::vector<Note> get_thread(const std::string& thread_id) override;
    
    // Search operations
    std::vector<Note> search_notes(const std::string& query, int limit = 20, int offset = 0) override;
    std::vector<Note> search_by_content(const std::string& content, int limit = 20, int offset = 0) override;
    std::vector<Note> get_by_hashtag(const std::string& hashtag, int limit = 20, int offset = 0) override;
    std::vector<Note> get_by_mention(const std::string& user_id, int limit = 20, int offset = 0) override;
    
    // Status-based queries
    std::vector<Note> get_drafts(const std::string& user_id, int limit = 20) override;
    std::vector<Note> get_scheduled_notes(const std::string& user_id, int limit = 20) override;
    std::vector<Note> get_flagged_notes(int limit = 20, int offset = 0) override;
    std::vector<Note> get_deleted_notes(const std::string& user_id, int limit = 20) override;
    
    // Analytics operations
    int get_total_notes_count() override;
    int get_notes_count_by_timeframe(int hours_back) override;
    std::vector<std::pair<std::string, int>> get_top_hashtags(int limit = 10, int hours_back = 24) override;
    std::vector<std::pair<std::string, int>> get_trending_topics(int limit = 10, int hours_back = 24) override;
    
    // Maintenance operations
    bool cleanup_deleted_notes(int days_old) override;
    bool cleanup_old_drafts(int days_old) override;
    bool optimize_database() override;
    bool rebuild_indexes() override;
    
private:
    // Database schema management
    void create_database_schema();
    void create_indexes();
    void setup_prepared_statements();
    
    // Connection management
    void ensure_connection();
    void reconnect_if_needed();
    bool test_connection();
    
    // Query building helpers
    std::string build_select_query(const std::vector<std::string>& fields = {}) const;
    std::string build_insert_query(const Note& note) const;
    std::string build_update_query(const Note& note) const;
    std::string build_timeline_query(const std::vector<std::string>& user_ids, int limit, int offset) const;
    std::string build_search_query(const std::string& query, int limit, int offset) const;
    
    // Data mapping
    Note map_row_to_note(const pqxx::row& row) const;
    std::vector<Note> map_result_to_notes(const pqxx::result& result) const;
    void populate_note_relations(Note& note) const;
    void populate_note_metrics(Note& note) const;
    
    // Related data operations
    void save_note_hashtags(pqxx::work& txn, const Note& note);
    void save_note_mentions(pqxx::work& txn, const Note& note);
    void save_note_urls(pqxx::work& txn, const Note& note);
    void save_note_metrics(pqxx::work& txn, const Note& note);
    void load_note_hashtags(Note& note) const;
    void load_note_mentions(Note& note) const;
    void load_note_urls(Note& note) const;
    void load_note_metrics(Note& note) const;
    
    // Cache management
    void invalidate_cache(const std::string& cache_key) const;
    void update_trending_cache() const;
    
    // Performance optimization
    void update_materialized_views() const;
    void refresh_statistics() const;
    void analyze_query_performance() const;
    
    // Error handling
    void handle_database_error(const std::exception& e, const std::string& operation) const;
    void log_repository_operation(const std::string& operation, const std::string& note_id = "") const;
    
    // Validation
    bool validate_note_data(const Note& note) const;
    bool check_note_exists(const std::string& note_id) const;
    
    // Utility methods
    std::time_t get_timestamp_from_timeframe(int hours_back) const;
    std::string format_user_ids_for_query(const std::vector<std::string>& user_ids) const;
    std::vector<std::string> extract_hashtags_from_query(const std::string& query) const;
    std::vector<std::string> extract_mentions_from_query(const std::string& query) const;
};

/**
 * Factory class for creating note repository instances
 */
class NoteRepositoryFactory {
public:
    static std::unique_ptr<NotegreSQLNoteRepository> create_notegresql_repository(const std::string& connection_string);
    static std::shared_ptr<pqxx::connection> create_database_connection(const std::string& connection_string);
    
private:
    static void validate_connection_string(const std::string& connection_string);
};

} // namespace sonet::note::repositories