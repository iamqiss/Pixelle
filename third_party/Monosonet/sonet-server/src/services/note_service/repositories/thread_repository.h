/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/thread.h"
#include "../models/note.h"
#include <string>
#include <vector>
#include <optional>
#include <memory>

namespace sonet::note::repositories {

using namespace sonet::note::models;

/**
 * Thread repository interface for managing note threads
 * 
 * This handles all the thread-related database operations. I'm keeping it 
 * separate from the note repository because threads have their own specific 
 * patterns and queries that are different from individual notes.
 */
class ThreadRepository {
public:
    virtual ~ThreadRepository() = default;
    
    // Core CRUD operations
    virtual bool create_thread(const Thread& thread) = 0;
    virtual std::optional<Thread> get_thread_by_id(const std::string& thread_id) = 0;
    virtual bool update_thread(const Thread& thread) = 0;
    virtual bool delete_thread(const std::string& thread_id) = 0;
    
    // Thread structure operations
    virtual bool add_note_to_thread(const std::string& thread_id, const std::string& note_id, int position = -1) = 0;
    virtual bool remove_note_from_thread(const std::string& thread_id, const std::string& note_id) = 0;
    virtual bool reorder_thread_note(const std::string& thread_id, const std::string& note_id, int new_position) = 0;
    virtual std::vector<Note> get_thread_notes(const std::string& thread_id, bool include_hidden = false) = 0;
    
    // Thread discovery
    virtual std::vector<Thread> get_threads_by_author(const std::string& author_id, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Thread> get_trending_threads(int hours_back = 24, int limit = 20) = 0;
    virtual std::vector<Thread> get_recent_threads(int limit = 20, int offset = 0) = 0;
    virtual std::vector<Thread> get_pinned_threads(const std::string& author_id) = 0;
    
    // Thread search
    virtual std::vector<Thread> search_threads(const std::string& query, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Thread> search_threads_by_tag(const std::string& tag, int limit = 20, int offset = 0) = 0;
    virtual std::vector<Thread> get_threads_by_hashtag(const std::string& hashtag, int limit = 20, int offset = 0) = 0;
    
    // Thread statistics
    virtual ThreadStatistics get_thread_statistics(const std::string& thread_id) = 0;
    virtual std::vector<ThreadParticipant> get_thread_participants(const std::string& thread_id) = 0;
    virtual int get_thread_note_count(const std::string& thread_id) = 0;
    virtual int get_thread_view_count(const std::string& thread_id) = 0;
    
    // Thread moderation
    virtual bool lock_thread(const std::string& thread_id) = 0;
    virtual bool unlock_thread(const std::string& thread_id) = 0;
    virtual bool pin_thread(const std::string& thread_id) = 0;
    virtual bool unpin_thread(const std::string& thread_id) = 0;
    virtual bool add_thread_moderator(const std::string& thread_id, const std::string& user_id) = 0;
    virtual bool remove_thread_moderator(const std::string& thread_id, const std::string& user_id) = 0;
    virtual bool block_user_from_thread(const std::string& thread_id, const std::string& user_id) = 0;
    virtual bool unblock_user_from_thread(const std::string& thread_id, const std::string& user_id) = 0;
    
    // Engagement tracking
    virtual bool record_thread_view(const std::string& thread_id, const std::string& user_id) = 0;
    virtual bool update_thread_engagement(const std::string& thread_id) = 0;
    virtual std::vector<std::string> get_thread_viewers(const std::string& thread_id, int limit = 50) = 0;
    
    // Analytics operations
    virtual int get_total_threads_count() = 0;
    virtual int get_threads_count_by_timeframe(int hours_back) = 0;
    virtual std::vector<std::pair<std::string, int>> get_top_thread_tags(int limit = 10, int hours_back = 24) = 0;
    virtual std::vector<std::pair<std::string, int>> get_most_active_thread_authors(int limit = 10, int hours_back = 24) = 0;
    
    // Cleanup operations
    virtual bool cleanup_empty_threads() = 0;
    virtual bool cleanup_old_thread_statistics(int days_old) = 0;
    virtual bool rebuild_thread_indexes() = 0;
};

/**
 * Cassandra implementation of ThreadRepository
 * 
 * Using Cassandra because thread operations are perfect for its data model.
 * Threads are naturally ordered sequences, which maps perfectly to Cassandra's
 * clustering columns. Plus the denormalization fits our read patterns.
 */
class CassandraThreadRepository : public ThreadRepository {
private:
    CassCluster* cluster_;
    CassSession* session_;
    std::string keyspace_;
    
    // Prepared statements for performance
    CassPrepared* insert_thread_stmt_;
    CassPrepared* select_thread_stmt_;
    CassPrepared* update_thread_stmt_;
    CassPrepared* delete_thread_stmt_;
    CassPrepared* insert_thread_note_stmt_;
    CassPrepared* select_thread_notes_stmt_;
    CassPrepared* delete_thread_note_stmt_;
    CassPrepared* insert_thread_view_stmt_;
    CassPrepared* select_author_threads_stmt_;
    
    bool is_connected_;
    
public:
    explicit CassandraThreadRepository(CassCluster* cluster, CassSession* session, const std::string& keyspace);
    ~CassandraThreadRepository() override = default;
    
    // Core CRUD operations
    bool create_thread(const Thread& thread) override;
    std::optional<Thread> get_thread_by_id(const std::string& thread_id) override;
    bool update_thread(const Thread& thread) override;
    bool delete_thread(const std::string& thread_id) override;
    
    // Thread structure operations
    bool add_note_to_thread(const std::string& thread_id, const std::string& note_id, int position = -1) override;
    bool remove_note_from_thread(const std::string& thread_id, const std::string& note_id) override;
    bool reorder_thread_note(const std::string& thread_id, const std::string& note_id, int new_position) override;
    std::vector<Note> get_thread_notes(const std::string& thread_id, bool include_hidden = false) override;
    
    // Thread discovery
    std::vector<Thread> get_threads_by_author(const std::string& author_id, int limit = 20, int offset = 0) override;
    std::vector<Thread> get_trending_threads(int hours_back = 24, int limit = 20) override;
    std::vector<Thread> get_recent_threads(int limit = 20, int offset = 0) override;
    std::vector<Thread> get_pinned_threads(const std::string& author_id) override;
    
    // Thread search
    std::vector<Thread> search_threads(const std::string& query, int limit = 20, int offset = 0) override;
    std::vector<Thread> search_threads_by_tag(const std::string& tag, int limit = 20, int offset = 0) override;
    std::vector<Thread> get_threads_by_hashtag(const std::string& hashtag, int limit = 20, int offset = 0) override;
    
    // Thread statistics
    ThreadStatistics get_thread_statistics(const std::string& thread_id) override;
    std::vector<ThreadParticipant> get_thread_participants(const std::string& thread_id) override;
    int get_thread_note_count(const std::string& thread_id) override;
    int get_thread_view_count(const std::string& thread_id) override;
    
    // Thread moderation
    bool lock_thread(const std::string& thread_id) override;
    bool unlock_thread(const std::string& thread_id) override;
    bool pin_thread(const std::string& thread_id) override;
    bool unpin_thread(const std::string& thread_id) override;
    bool add_thread_moderator(const std::string& thread_id, const std::string& user_id) override;
    bool remove_thread_moderator(const std::string& thread_id, const std::string& user_id) override;
    bool block_user_from_thread(const std::string& thread_id, const std::string& user_id) override;
    bool unblock_user_from_thread(const std::string& thread_id, const std::string& user_id) override;
    
    // Engagement tracking
    bool record_thread_view(const std::string& thread_id, const std::string& user_id) override;
    bool update_thread_engagement(const std::string& thread_id) override;
    std::vector<std::string> get_thread_viewers(const std::string& thread_id, int limit = 50) override;
    
    // Analytics operations
    int get_total_threads_count() override;
    int get_threads_count_by_timeframe(int hours_back) override;
    std::vector<std::pair<std::string, int>> get_top_thread_tags(int limit = 10, int hours_back = 24) override;
    std::vector<std::pair<std::string, int>> get_most_active_thread_authors(int limit = 10, int hours_back = 24) override;
    
    // Cleanup operations
    bool cleanup_empty_threads() override;
    bool cleanup_old_thread_statistics(int days_old) override;
    bool rebuild_thread_indexes() override;
    
    // Cassandra-specific operations
    bool create_thread_tables();
    void setup_prepared_statements();
    
private:
    // Connection management
    bool ensure_connected();
    void handle_cassandra_error(CassError error, const std::string& operation);
    
    // Data mapping
    Thread map_row_to_thread(const CassRow* row);
    std::vector<Thread> map_result_to_threads(const CassResult* result);
    void bind_thread_to_statement(CassStatement* statement, const Thread& thread);
    
    // Schema management
    void create_threads_table();
    void create_thread_notes_table();
    void create_thread_tags_table();
    void create_thread_views_table();
    void create_thread_participants_table();
    void create_author_threads_table();
    
    // Query helpers
    CassStatement* create_statement(const char* query, int param_count = 0);
    CassError execute_statement(CassStatement* statement);
    CassFuture* execute_async(CassStatement* statement);
    
    // Utility methods
    std::string generate_time_uuid();
    void log_operation(const std::string& operation, const std::string& thread_id = "");
    bool validate_thread_for_cassandra(const Thread& thread);
};

/**
 * Factory for creating thread repository instances
 */
class ThreadRepositoryFactory {
public:
    static std::unique_ptr<CassandraThreadRepository> create_cassandra_repository(
        CassCluster* cluster, 
        CassSession* session, 
        const std::string& keyspace
    );
    
    static bool test_thread_schema(CassSession* session, const std::string& keyspace);
};

} // namespace sonet::note::repositories