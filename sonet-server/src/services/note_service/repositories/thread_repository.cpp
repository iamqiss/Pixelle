/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "thread_repository.h"
#include "note_repository.h"
#include <spdlog/spdlog.h>
#include <cassandra.h>
#include <sstream>
#include <algorithm>

namespace sonet::note::repositories {

// Constructor
CassandraThreadRepository::CassandraThreadRepository(CassCluster* cluster, CassSession* session, const std::string& keyspace)
    : cluster_(cluster), session_(session), keyspace_(keyspace), is_connected_(true) {
    
    spdlog::info("Initializing Cassandra thread repository for keyspace: {}", keyspace_);
    
    // Create thread-specific tables
    if (!create_thread_tables()) {
        throw std::runtime_error("Failed to create thread tables");
    }
    
    // Setup prepared statements for better performance
    setup_prepared_statements();
    
    spdlog::info("Cassandra thread repository initialized successfully");
}

// Core CRUD operations

bool CassandraThreadRepository::create_thread(const Thread& thread) {
    if (!ensure_connected()) {
        spdlog::error("Not connected to Cassandra");
        return false;
    }
    
    if (!validate_thread_for_cassandra(thread)) {
        spdlog::error("Thread validation failed for thread_id: {}", thread.thread_id);
        return false;
    }
    
    try {
        // Insert main thread record
        CassStatement* statement = cass_prepared_bind(insert_thread_stmt_);
        bind_thread_to_statement(statement, thread);
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            handle_cassandra_error(error, "create_thread");
            return false;
        }
        
        // Insert into author_threads table for fast author lookups
        std::string author_query = 
            "INSERT INTO " + keyspace_ + ".author_threads "
            "(author_id, created_at, thread_id, title, total_notes, is_pinned) "
            "VALUES (?, ?, ?, ?, ?, ?)";
        
        CassStatement* author_stmt = cass_statement_new(author_query.c_str(), 6);
        cass_statement_bind_string(author_stmt, 0, thread.author_id.c_str());
        cass_statement_bind_int64(author_stmt, 1, thread.created_at);
        cass_statement_bind_string(author_stmt, 2, thread.thread_id.c_str());
        cass_statement_bind_string(author_stmt, 3, thread.title.c_str());
        cass_statement_bind_int32(author_stmt, 4, thread.total_notes);
        cass_statement_bind_bool(author_stmt, 5, thread.is_pinned);
        
        error = execute_statement(author_stmt);
        cass_statement_free(author_stmt);
        
        if (error != CASS_OK) {
            handle_cassandra_error(error, "create_thread_author_index");
            return false;
        }
        
        // Insert thread notes
        for (size_t i = 0; i < thread.note_ids.size(); ++i) {
            if (!add_note_to_thread(thread.thread_id, thread.note_ids[i], static_cast<int>(i))) {
                spdlog::warn("Failed to add note {} to thread {}", thread.note_ids[i], thread.thread_id);
            }
        }
        
        log_operation("create_thread", thread.thread_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception creating thread {}: {}", thread.thread_id, e.what());
        return false;
    }
}

std::optional<Thread> CassandraThreadRepository::get_thread_by_id(const std::string& thread_id) {
    if (!ensure_connected()) {
        return std::nullopt;
    }
    
    try {
        CassStatement* statement = cass_prepared_bind(select_thread_stmt_);
        cass_statement_bind_string(statement, 0, thread_id.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            handle_cassandra_error(error, "get_thread_by_id");
            cass_future_free(future);
            return std::nullopt;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        if (cass_result_row_count(result) == 0) {
            cass_result_free(result);
            return std::nullopt;
        }
        
        const CassRow* row = cass_result_first_row(result);
        Thread thread = map_row_to_thread(row);
        cass_result_free(result);
        
        // Load thread notes
        std::string notes_query = 
            "SELECT note_id FROM " + keyspace_ + ".thread_notes "
            "WHERE thread_id = ? ORDER BY position ASC";
        
        CassStatement* notes_stmt = cass_statement_new(notes_query.c_str(), 1);
        cass_statement_bind_string(notes_stmt, 0, thread_id.c_str());
        
        CassFuture* notes_future = execute_async(notes_stmt);
        cass_statement_free(notes_stmt);
        
        error = cass_future_error_code(notes_future);
        if (error == CASS_OK) {
            const CassResult* notes_result = cass_future_get_result(notes_future);
            
            thread.note_ids.clear();
            CassIterator* iterator = cass_iterator_from_result(notes_result);
            
            while (cass_iterator_next(iterator)) {
                const CassRow* note_row = cass_iterator_get_row(iterator);
                const CassValue* note_id_value = cass_row_get_column_by_name(note_row, "note_id");
                
                const char* note_id_str;
                size_t note_id_len;
                cass_value_get_string(note_id_value, &note_id_str, &note_id_len);
                thread.note_ids.emplace_back(note_id_str, note_id_len);
            }
            
            cass_iterator_free(iterator);
            cass_result_free(notes_result);
        }
        cass_future_free(notes_future);
        
        return thread;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting thread {}: {}", thread_id, e.what());
        return std::nullopt;
    }
}

bool CassandraThreadRepository::update_thread(const Thread& thread) {
    if (!ensure_connected()) {
        return false;
    }
    
    try {
        CassStatement* statement = cass_prepared_bind(update_thread_stmt_);
        bind_thread_to_statement(statement, thread);
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            handle_cassandra_error(error, "update_thread");
            return false;
        }
        
        log_operation("update_thread", thread.thread_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception updating thread {}: {}", thread.thread_id, e.what());
        return false;
    }
}

bool CassandraThreadRepository::delete_thread(const std::string& thread_id) {
    if (!ensure_connected()) {
        return false;
    }
    
    try {
        // First get the thread to know what to clean up
        auto thread_opt = get_thread_by_id(thread_id);
        if (!thread_opt) {
            spdlog::warn("Trying to delete non-existent thread: {}", thread_id);
            return false;
        }
        
        Thread thread = thread_opt.value();
        
        // Delete main thread record
        CassStatement* statement = cass_prepared_bind(delete_thread_stmt_);
        cass_statement_bind_string(statement, 0, thread_id.c_str());
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            handle_cassandra_error(error, "delete_thread");
            return false;
        }
        
        // Delete from author_threads index
        std::string author_query = 
            "DELETE FROM " + keyspace_ + ".author_threads "
            "WHERE author_id = ? AND created_at = ? AND thread_id = ?";
        
        CassStatement* author_stmt = cass_statement_new(author_query.c_str(), 3);
        cass_statement_bind_string(author_stmt, 0, thread.author_id.c_str());
        cass_statement_bind_int64(author_stmt, 1, thread.created_at);
        cass_statement_bind_string(author_stmt, 2, thread_id.c_str());
        
        execute_statement(author_stmt);
        cass_statement_free(author_stmt);
        
        // Delete thread notes
        std::string notes_query = 
            "DELETE FROM " + keyspace_ + ".thread_notes WHERE thread_id = ?";
        
        CassStatement* notes_stmt = cass_statement_new(notes_query.c_str(), 1);
        cass_statement_bind_string(notes_stmt, 0, thread_id.c_str());
        
        execute_statement(notes_stmt);
        cass_statement_free(notes_stmt);
        
        log_operation("delete_thread", thread_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception deleting thread {}: {}", thread_id, e.what());
        return false;
    }
}

// Thread structure operations

bool CassandraThreadRepository::add_note_to_thread(const std::string& thread_id, const std::string& note_id, int position) {
    if (!ensure_connected()) {
        return false;
    }
    
    try {
        CassStatement* statement = cass_prepared_bind(insert_thread_note_stmt_);
        cass_statement_bind_string(statement, 0, thread_id.c_str());
        cass_statement_bind_int32(statement, 1, position);
        cass_statement_bind_string(statement, 2, note_id.c_str());
        cass_statement_bind_int64(statement, 3, std::time(nullptr));
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            handle_cassandra_error(error, "add_note_to_thread");
            return false;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception adding note {} to thread {}: {}", note_id, thread_id, e.what());
        return false;
    }
}

bool CassandraThreadRepository::remove_note_from_thread(const std::string& thread_id, const std::string& note_id) {
    if (!ensure_connected()) {
        return false;
    }
    
    try {
        CassStatement* statement = cass_prepared_bind(delete_thread_note_stmt_);
        cass_statement_bind_string(statement, 0, thread_id.c_str());
        cass_statement_bind_string(statement, 1, note_id.c_str());
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            handle_cassandra_error(error, "remove_note_from_thread");
            return false;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception removing note {} from thread {}: {}", note_id, thread_id, e.what());
        return false;
    }
}

std::vector<Note> CassandraThreadRepository::get_thread_notes(const std::string& thread_id, bool include_hidden) {
    std::vector<Note> notes;
    
    if (!ensure_connected()) {
        return notes;
    }
    
    try {
        CassStatement* statement = cass_prepared_bind(select_thread_notes_stmt_);
        cass_statement_bind_string(statement, 0, thread_id.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            handle_cassandra_error(error, "get_thread_notes");
            cass_future_free(future);
            return notes;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        // Extract note IDs in order
        std::vector<std::string> note_ids;
        CassIterator* iterator = cass_iterator_from_result(result);
        
        while (cass_iterator_next(iterator)) {
            const CassRow* row = cass_iterator_get_row(iterator);
            const CassValue* note_id_value = cass_row_get_column_by_name(row, "note_id");
            
            const char* note_id_str;
            size_t note_id_len;
            cass_value_get_string(note_id_value, &note_id_str, &note_id_len);
            note_ids.emplace_back(note_id_str, note_id_len);
        }
        
        cass_iterator_free(iterator);
        cass_result_free(result);
        
        // Now fetch the actual notes - this would use the note repository
        // For now, return empty vector as we'd need to inject the note repository
        spdlog::debug("Found {} notes for thread {}", note_ids.size(), thread_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting notes for thread {}: {}", thread_id, e.what());
    }
    
    return notes;
}

// Thread discovery

std::vector<Thread> CassandraThreadRepository::get_threads_by_author(const std::string& author_id, int limit, int offset) {
    std::vector<Thread> threads;
    
    if (!ensure_connected()) {
        return threads;
    }
    
    try {
        CassStatement* statement = cass_prepared_bind(select_author_threads_stmt_);
        cass_statement_bind_string(statement, 0, author_id.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            handle_cassandra_error(error, "get_threads_by_author");
            cass_future_free(future);
            return threads;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        threads = map_result_to_threads(result);
        cass_result_free(result);
        
        spdlog::debug("Retrieved {} threads for author {}", threads.size(), author_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting threads for author {}: {}", author_id, e.what());
    }
    
    return threads;
}

// Thread statistics

ThreadStatistics CassandraThreadRepository::get_thread_statistics(const std::string& thread_id) {
    ThreadStatistics stats;
    stats.thread_id = thread_id;
    stats.calculated_at = std::time(nullptr);
    
    if (!ensure_connected()) {
        return stats;
    }
    
    try {
        // Get basic thread info
        auto thread_opt = get_thread_by_id(thread_id);
        if (!thread_opt) {
            return stats;
        }
        
        Thread thread = thread_opt.value();
        stats.total_notes = thread.total_notes;
        stats.total_views = thread.total_views;
        stats.total_engagement = thread.total_likes + thread.total_renotes + thread.total_replies;
        
        // Calculate other metrics
        if (thread.created_at > 0) {
            auto age_hours = (std::time(nullptr) - thread.created_at) / 3600.0;
            stats.total_thread_duration = age_hours;
            
            if (thread.total_notes > 1) {
                stats.average_time_between_notes = age_hours / (thread.total_notes - 1) * 60.0; // in minutes
            }
        }
        
        // Calculate engagement rate
        if (stats.total_views > 0) {
            stats.engagement_rate = static_cast<double>(stats.total_engagement) / stats.total_views;
        }
        
        spdlog::debug("Calculated statistics for thread {}", thread_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception calculating statistics for thread {}: {}", thread_id, e.what());
    }
    
    return stats;
}

// Engagement tracking

bool CassandraThreadRepository::record_thread_view(const std::string& thread_id, const std::string& user_id) {
    if (!ensure_connected()) {
        return false;
    }
    
    try {
        std::string query = 
            "INSERT INTO " + keyspace_ + ".thread_views "
            "(thread_id, user_id, viewed_at) VALUES (?, ?, ?)";
        
        CassStatement* statement = cass_statement_new(query.c_str(), 3);
        cass_statement_bind_string(statement, 0, thread_id.c_str());
        cass_statement_bind_string(statement, 1, user_id.c_str());
        cass_statement_bind_int64(statement, 2, std::time(nullptr));
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            handle_cassandra_error(error, "record_thread_view");
            return false;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception recording view for thread {}: {}", thread_id, e.what());
        return false;
    }
}

// Cassandra-specific operations

bool CassandraThreadRepository::create_thread_tables() {
    try {
        create_threads_table();
        create_thread_notes_table();
        create_thread_tags_table();
        create_thread_views_table();
        create_thread_participants_table();
        create_author_threads_table();
        
        spdlog::info("Created all thread tables successfully");
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to create thread tables: {}", e.what());
        return false;
    }
}

void CassandraThreadRepository::create_threads_table() {
    // Main threads table
    std::string query = 
        "CREATE TABLE IF NOT EXISTS " + keyspace_ + ".threads ("
        "thread_id TEXT PRIMARY KEY, "
        "starter_note_id TEXT, "
        "author_id TEXT, "
        "author_username TEXT, "
        "title TEXT, "
        "description TEXT, "
        "tags LIST<TEXT>, "
        "total_notes INT, "
        "max_depth INT, "
        "is_locked BOOLEAN, "
        "is_pinned BOOLEAN, "
        "is_published BOOLEAN, "
        "allow_replies BOOLEAN, "
        "allow_renotes BOOLEAN, "
        "total_likes INT, "
        "total_renotes INT, "
        "total_replies INT, "
        "total_views INT, "
        "total_bookmarks INT, "
        "unique_participants INT, "
        "visibility INT, "
        "moderator_ids LIST<TEXT>, "
        "blocked_user_ids LIST<TEXT>, "
        "engagement_rate DOUBLE, "
        "completion_rate DOUBLE, "
        "created_at TIMESTAMP, "
        "updated_at TIMESTAMP, "
        "last_activity_at TIMESTAMP, "
        "completed_at TIMESTAMP"
        ")";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create threads table");
    }
}

void CassandraThreadRepository::create_thread_notes_table() {
    // Thread notes table - ordered list of notes in a thread
    std::string query = 
        "CREATE TABLE IF NOT EXISTS " + keyspace_ + ".thread_notes ("
        "thread_id TEXT, "
        "position INT, "
        "note_id TEXT, "
        "added_at TIMESTAMP, "
        "PRIMARY KEY (thread_id, position)"
        ") WITH CLUSTERING ORDER BY (position ASC)";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create thread_notes table");
    }
}

void CassandraThreadRepository::create_author_threads_table() {
    // Author threads index - for getting threads by author
    std::string query = 
        "CREATE TABLE IF NOT EXISTS " + keyspace_ + ".author_threads ("
        "author_id TEXT, "
        "created_at TIMESTAMP, "
        "thread_id TEXT, "
        "title TEXT, "
        "total_notes INT, "
        "is_pinned BOOLEAN, "
        "PRIMARY KEY (author_id, created_at, thread_id)"
        ") WITH CLUSTERING ORDER BY (created_at DESC, thread_id DESC)";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create author_threads table");
    }
}

void CassandraThreadRepository::create_thread_tags_table() {
    // Thread tags table - for tag-based search
    std::string query = 
        "CREATE TABLE IF NOT EXISTS " + keyspace_ + ".thread_tags ("
        "tag TEXT, "
        "thread_id TEXT, "
        "created_at TIMESTAMP, "
        "author_id TEXT, "
        "title TEXT, "
        "PRIMARY KEY (tag, created_at, thread_id)"
        ") WITH CLUSTERING ORDER BY (created_at DESC, thread_id DESC)";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create thread_tags table");
    }
}

void CassandraThreadRepository::create_thread_views_table() {
    // Thread views table - for tracking who viewed what
    std::string query = 
        "CREATE TABLE IF NOT EXISTS " + keyspace_ + ".thread_views ("
        "thread_id TEXT, "
        "user_id TEXT, "
        "viewed_at TIMESTAMP, "
        "PRIMARY KEY (thread_id, user_id)"
        ")";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create thread_views table");
    }
}

void CassandraThreadRepository::create_thread_participants_table() {
    // Thread participants table - for tracking engagement
    std::string query = 
        "CREATE TABLE IF NOT EXISTS " + keyspace_ + ".thread_participants ("
        "thread_id TEXT, "
        "user_id TEXT, "
        "username TEXT, "
        "first_participated_at TIMESTAMP, "
        "last_participated_at TIMESTAMP, "
        "total_notes INT, "
        "engagement_score DOUBLE, "
        "PRIMARY KEY (thread_id, user_id)"
        ")";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create thread_participants table");
    }
}

void CassandraThreadRepository::setup_prepared_statements() {
    try {
        // Insert thread statement
        std::string insert_query = 
            "INSERT INTO " + keyspace_ + ".threads "
            "(thread_id, starter_note_id, author_id, author_username, title, description, "
            "tags, total_notes, max_depth, is_locked, is_pinned, is_published, "
            "allow_replies, allow_renotes, total_likes, total_renotes, total_replies, "
            "total_views, total_bookmarks, unique_participants, visibility, "
            "moderator_ids, blocked_user_ids, engagement_rate, completion_rate, "
            "created_at, updated_at, last_activity_at, completed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        CassFuture* prepare_future = cass_session_prepare(session_, insert_query.c_str());
        CassError error = cass_future_error_code(prepare_future);
        if (error == CASS_OK) {
            insert_thread_stmt_ = cass_future_get_prepared(prepare_future);
        }
        cass_future_free(prepare_future);
        
        // Select thread statement
        std::string select_query = 
            "SELECT * FROM " + keyspace_ + ".threads WHERE thread_id = ?";
        
        prepare_future = cass_session_prepare(session_, select_query.c_str());
        error = cass_future_error_code(prepare_future);
        if (error == CASS_OK) {
            select_thread_stmt_ = cass_future_get_prepared(prepare_future);
        }
        cass_future_free(prepare_future);
        
        // Update thread statement
        std::string update_query = 
            "UPDATE " + keyspace_ + ".threads SET "
            "title = ?, description = ?, tags = ?, total_notes = ?, max_depth = ?, "
            "is_locked = ?, is_pinned = ?, is_published = ?, allow_replies = ?, "
            "allow_renotes = ?, total_likes = ?, total_renotes = ?, total_replies = ?, "
            "total_views = ?, total_bookmarks = ?, unique_participants = ?, "
            "visibility = ?, moderator_ids = ?, blocked_user_ids = ?, "
            "engagement_rate = ?, completion_rate = ?, updated_at = ?, "
            "last_activity_at = ?, completed_at = ? "
            "WHERE thread_id = ?";
        
        prepare_future = cass_session_prepare(session_, update_query.c_str());
        error = cass_future_error_code(prepare_future);
        if (error == CASS_OK) {
            update_thread_stmt_ = cass_future_get_prepared(prepare_future);
        }
        cass_future_free(prepare_future);
        
        // Delete thread statement
        std::string delete_query = 
            "DELETE FROM " + keyspace_ + ".threads WHERE thread_id = ?";
        
        prepare_future = cass_session_prepare(session_, delete_query.c_str());
        error = cass_future_error_code(prepare_future);
        if (error == CASS_OK) {
            delete_thread_stmt_ = cass_future_get_prepared(prepare_future);
        }
        cass_future_free(prepare_future);
        
        // Insert thread note statement
        std::string insert_note_query = 
            "INSERT INTO " + keyspace_ + ".thread_notes "
            "(thread_id, position, note_id, added_at) VALUES (?, ?, ?, ?)";
        
        prepare_future = cass_session_prepare(session_, insert_note_query.c_str());
        error = cass_future_error_code(prepare_future);
        if (error == CASS_OK) {
            insert_thread_note_stmt_ = cass_future_get_prepared(prepare_future);
        }
        cass_future_free(prepare_future);
        
        // Delete thread note statement
        std::string delete_note_query = 
            "DELETE FROM " + keyspace_ + ".thread_notes WHERE thread_id = ? AND note_id = ?";
        
        prepare_future = cass_session_prepare(session_, delete_note_query.c_str());
        error = cass_future_error_code(prepare_future);
        if (error == CASS_OK) {
            delete_thread_note_stmt_ = cass_future_get_prepared(prepare_future);
        }
        cass_future_free(prepare_future);
        
        // Select thread notes statement
        std::string select_notes_query = 
            "SELECT note_id FROM " + keyspace_ + ".thread_notes "
            "WHERE thread_id = ? ORDER BY position ASC";
        
        prepare_future = cass_session_prepare(session_, select_notes_query.c_str());
        error = cass_future_error_code(prepare_future);
        if (error == CASS_OK) {
            select_thread_notes_stmt_ = cass_future_get_prepared(prepare_future);
        }
        cass_future_free(prepare_future);
        
        // Select author threads statement
        std::string select_author_query = 
            "SELECT thread_id, title, total_notes, is_pinned, created_at FROM " + keyspace_ + ".author_threads "
            "WHERE author_id = ?";
        
        prepare_future = cass_session_prepare(session_, select_author_query.c_str());
        error = cass_future_error_code(prepare_future);
        if (error == CASS_OK) {
            select_author_threads_stmt_ = cass_future_get_prepared(prepare_future);
        }
        cass_future_free(prepare_future);
        
        spdlog::info("Prepared statements setup completed");
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to setup prepared statements: {}", e.what());
        throw;
    }
}

void CassandraThreadRepository::bind_thread_to_statement(CassStatement* statement, const Thread& thread) {
    int index = 0;
    
    cass_statement_bind_string(statement, index++, thread.thread_id.c_str());
    cass_statement_bind_string(statement, index++, thread.starter_note_id.c_str());
    cass_statement_bind_string(statement, index++, thread.author_id.c_str());
    cass_statement_bind_string(statement, index++, thread.author_username.c_str());
    cass_statement_bind_string(statement, index++, thread.title.c_str());
    cass_statement_bind_string(statement, index++, thread.description.c_str());
    
    // Bind tags list
    CassCollection* tags_collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, thread.tags.size());
    for (const auto& tag : thread.tags) {
        cass_collection_append_string(tags_collection, tag.c_str());
    }
    cass_statement_bind_collection(statement, index++, tags_collection);
    cass_collection_free(tags_collection);
    
    cass_statement_bind_int32(statement, index++, thread.total_notes);
    cass_statement_bind_int32(statement, index++, thread.max_depth);
    cass_statement_bind_bool(statement, index++, thread.is_locked);
    cass_statement_bind_bool(statement, index++, thread.is_pinned);
    cass_statement_bind_bool(statement, index++, thread.is_published);
    cass_statement_bind_bool(statement, index++, thread.allow_replies);
    cass_statement_bind_bool(statement, index++, thread.allow_renotes);
    cass_statement_bind_int32(statement, index++, thread.total_likes);
    cass_statement_bind_int32(statement, index++, thread.total_renotes);
    cass_statement_bind_int32(statement, index++, thread.total_replies);
    cass_statement_bind_int32(statement, index++, thread.total_views);
    cass_statement_bind_int32(statement, index++, thread.total_bookmarks);
    cass_statement_bind_int32(statement, index++, thread.unique_participants);
    cass_statement_bind_int32(statement, index++, static_cast<int>(thread.visibility));
    
    // Bind moderator IDs list
    CassCollection* moderators_collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, thread.moderator_ids.size());
    for (const auto& mod_id : thread.moderator_ids) {
        cass_collection_append_string(moderators_collection, mod_id.c_str());
    }
    cass_statement_bind_collection(statement, index++, moderators_collection);
    cass_collection_free(moderators_collection);
    
    // Bind blocked user IDs list
    CassCollection* blocked_collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, thread.blocked_user_ids.size());
    for (const auto& blocked_id : thread.blocked_user_ids) {
        cass_collection_append_string(blocked_collection, blocked_id.c_str());
    }
    cass_statement_bind_collection(statement, index++, blocked_collection);
    cass_collection_free(blocked_collection);
    
    cass_statement_bind_double(statement, index++, thread.engagement_rate);
    cass_statement_bind_double(statement, index++, thread.completion_rate);
    cass_statement_bind_int64(statement, index++, thread.created_at);
    cass_statement_bind_int64(statement, index++, thread.updated_at);
    cass_statement_bind_int64(statement, index++, thread.last_activity_at);
    cass_statement_bind_int64(statement, index++, thread.completed_at);
}

Thread CassandraThreadRepository::map_row_to_thread(const CassRow* row) {
    Thread thread;
    
    // Get basic fields
    const CassValue* value = cass_row_get_column_by_name(row, "thread_id");
    const char* str_val;
    size_t str_len;
    cass_value_get_string(value, &str_val, &str_len);
    thread.thread_id.assign(str_val, str_len);
    
    value = cass_row_get_column_by_name(row, "starter_note_id");
    cass_value_get_string(value, &str_val, &str_len);
    thread.starter_note_id.assign(str_val, str_len);
    
    value = cass_row_get_column_by_name(row, "author_id");
    cass_value_get_string(value, &str_val, &str_len);
    thread.author_id.assign(str_val, str_len);
    
    value = cass_row_get_column_by_name(row, "author_username");
    cass_value_get_string(value, &str_val, &str_len);
    thread.author_username.assign(str_val, str_len);
    
    value = cass_row_get_column_by_name(row, "title");
    cass_value_get_string(value, &str_val, &str_len);
    thread.title.assign(str_val, str_len);
    
    value = cass_row_get_column_by_name(row, "description");
    cass_value_get_string(value, &str_val, &str_len);
    thread.description.assign(str_val, str_len);
    
    // Get tags list
    value = cass_row_get_column_by_name(row, "tags");
    if (value && !cass_value_is_null(value)) {
        CassIterator* tags_iter = cass_iterator_from_collection(value);
        while (cass_iterator_next(tags_iter)) {
            const CassValue* tag_value = cass_iterator_get_value(tags_iter);
            cass_value_get_string(tag_value, &str_val, &str_len);
            thread.tags.emplace_back(str_val, str_len);
        }
        cass_iterator_free(tags_iter);
    }
    
    // Get numeric fields
    value = cass_row_get_column_by_name(row, "total_notes");
    cass_value_get_int32(value, &thread.total_notes);
    
    value = cass_row_get_column_by_name(row, "max_depth");
    cass_value_get_int32(value, &thread.max_depth);
    
    // Get boolean fields
    value = cass_row_get_column_by_name(row, "is_locked");
    cass_value_get_bool(value, &thread.is_locked);
    
    value = cass_row_get_column_by_name(row, "is_pinned");
    cass_value_get_bool(value, &thread.is_pinned);
    
    value = cass_row_get_column_by_name(row, "is_published");
    cass_value_get_bool(value, &thread.is_published);
    
    value = cass_row_get_column_by_name(row, "allow_replies");
    cass_value_get_bool(value, &thread.allow_replies);
    
    value = cass_row_get_column_by_name(row, "allow_renotes");
    cass_value_get_bool(value, &thread.allow_renotes);
    
    // Get engagement metrics
    value = cass_row_get_column_by_name(row, "total_likes");
    cass_value_get_int32(value, &thread.total_likes);
    
    value = cass_row_get_column_by_name(row, "total_renotes");
    cass_value_get_int32(value, &thread.total_renotes);
    
    value = cass_row_get_column_by_name(row, "total_replies");
    cass_value_get_int32(value, &thread.total_replies);
    
    value = cass_row_get_column_by_name(row, "total_views");
    cass_value_get_int32(value, &thread.total_views);
    
    value = cass_row_get_column_by_name(row, "total_bookmarks");
    cass_value_get_int32(value, &thread.total_bookmarks);
    
    value = cass_row_get_column_by_name(row, "unique_participants");
    cass_value_get_int32(value, &thread.unique_participants);
    
    // Get visibility
    value = cass_row_get_column_by_name(row, "visibility");
    int vis_val;
    cass_value_get_int32(value, &vis_val);
    thread.visibility = static_cast<models::Visibility>(vis_val);
    
    // Get moderator IDs list
    value = cass_row_get_column_by_name(row, "moderator_ids");
    if (value && !cass_value_is_null(value)) {
        CassIterator* mods_iter = cass_iterator_from_collection(value);
        while (cass_iterator_next(mods_iter)) {
            const CassValue* mod_value = cass_iterator_get_value(mods_iter);
            cass_value_get_string(mod_value, &str_val, &str_len);
            thread.moderator_ids.emplace_back(str_val, str_len);
        }
        cass_iterator_free(mods_iter);
    }
    
    // Get blocked user IDs list
    value = cass_row_get_column_by_name(row, "blocked_user_ids");
    if (value && !cass_value_is_null(value)) {
        CassIterator* blocked_iter = cass_iterator_from_collection(value);
        while (cass_iterator_next(blocked_iter)) {
            const CassValue* blocked_value = cass_iterator_get_value(blocked_iter);
            cass_value_get_string(blocked_value, &str_val, &str_len);
            thread.blocked_user_ids.emplace_back(str_val, str_len);
        }
        cass_iterator_free(blocked_iter);
    }
    
    // Get rates
    value = cass_row_get_column_by_name(row, "engagement_rate");
    cass_value_get_double(value, &thread.engagement_rate);
    
    value = cass_row_get_column_by_name(row, "completion_rate");
    cass_value_get_double(value, &thread.completion_rate);
    
    // Get timestamps
    value = cass_row_get_column_by_name(row, "created_at");
    cass_value_get_int64(value, &thread.created_at);
    
    value = cass_row_get_column_by_name(row, "updated_at");
    cass_value_get_int64(value, &thread.updated_at);
    
    value = cass_row_get_column_by_name(row, "last_activity_at");
    cass_value_get_int64(value, &thread.last_activity_at);
    
    value = cass_row_get_column_by_name(row, "completed_at");
    cass_value_get_int64(value, &thread.completed_at);
    
    return thread;
}

std::vector<Thread> CassandraThreadRepository::map_result_to_threads(const CassResult* result) {
    std::vector<Thread> threads;
    
    CassIterator* iterator = cass_iterator_from_result(result);
    while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        threads.push_back(map_row_to_thread(row));
    }
    cass_iterator_free(iterator);
    
    return threads;
}

bool CassandraThreadRepository::validate_thread_for_cassandra(const Thread& thread) const {
    if (thread.thread_id.empty()) {
        spdlog::error("Thread ID cannot be empty");
        return false;
    }
    
    if (thread.author_id.empty()) {
        spdlog::error("Author ID cannot be empty");
        return false;
    }
    
    if (thread.title.length() > 500) {
        spdlog::error("Thread title too long: {} characters", thread.title.length());
        return false;
    }
    
    if (thread.description.length() > 10000) {
        spdlog::error("Thread description too long: {} characters", thread.description.length());
        return false;
    }
    
    if (thread.tags.size() > 50) {
        spdlog::error("Too many tags: {}", thread.tags.size());
        return false;
    }
    
    for (const auto& tag : thread.tags) {
        if (tag.length() > 100) {
            spdlog::error("Tag too long: {}", tag);
            return false;
        }
    }
    
    return true;
}

bool CassandraThreadRepository::ensure_connected() {
    return is_connected_;
}

CassError CassandraThreadRepository::execute_statement(CassStatement* statement) {
    CassFuture* future = cass_session_execute(session_, statement);
    CassError error = cass_future_error_code(future);
    cass_future_free(future);
    return error;
}

CassFuture* CassandraThreadRepository::execute_async(CassStatement* statement) {
    return cass_session_execute(session_, statement);
}

void CassandraThreadRepository::handle_cassandra_error(CassError error, const std::string& operation) {
    const char* error_msg = cass_error_desc(error);
    spdlog::error("Cassandra error in {}: {} ({})", operation, error_msg, static_cast<int>(error));
}

void CassandraThreadRepository::log_operation(const std::string& operation, const std::string& thread_id) {
    spdlog::debug("Thread operation completed: {} for thread {}", operation, thread_id);
}

// Factory methods

std::unique_ptr<CassandraThreadRepository> ThreadRepositoryFactory::create_cassandra_repository(
    CassCluster* cluster, 
    CassSession* session, 
    const std::string& keyspace) {
    
    if (!cluster || !session) {
        throw std::invalid_argument("Cluster and session cannot be null");
    }
    
    if (keyspace.empty()) {
        throw std::invalid_argument("Keyspace cannot be empty");
    }
    
    return std::make_unique<CassandraThreadRepository>(cluster, session, keyspace);
}

bool ThreadRepositoryFactory::test_thread_schema(CassSession* session, const std::string& keyspace) {
    try {
        std::string query = "SELECT thread_id FROM " + keyspace + ".threads LIMIT 1";
        CassStatement* statement = cass_statement_new(query.c_str(), 0);
        
        CassFuture* future = cass_session_execute(session, statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        cass_future_free(future);
        
        return error == CASS_OK;
        
    } catch (const std::exception& e) {
        spdlog::error("Thread schema test failed: {}", e.what());
        return false;
    }
}

} // namespace sonet::note::repositories