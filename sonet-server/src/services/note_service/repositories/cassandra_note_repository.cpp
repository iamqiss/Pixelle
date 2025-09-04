/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "cassandra_note_repository.h"
#include <spdlog/spdlog.h>
#include <sstream>
#include <algorithm>
#include <random>
#include <regex>

namespace sonet::note::repositories {

// Constructor - setting up our Cassandra connection
CassandraNoteRepository::CassandraNoteRepository(
    const std::vector<std::string>& contact_points,
    const std::string& keyspace,
    const std::string& username,
    const std::string& password,
    int port
) : contact_points_(contact_points), 
    keyspace_(keyspace), 
    username_(username), 
    password_(password), 
    port_(port),
    is_connected_(false),
    cluster_(nullptr),
    session_(nullptr) {
    
    spdlog::info("Initializing Cassandra repository for keyspace: {}", keyspace_);
    
    // Create cluster configuration
    cluster_ = cass_cluster_new();
    
    // Set contact points - these are our Cassandra nodes
    std::string contacts = "";
    for (size_t i = 0; i < contact_points_.size(); ++i) {
        if (i > 0) contacts += ",";
        contacts += contact_points_[i];
    }
    cass_cluster_set_contact_points(cluster_, contacts.c_str());
    cass_cluster_set_port(cluster_, port_);
    
    // Authentication if provided
    if (!username_.empty() && !password_.empty()) {
        cass_cluster_set_credentials(cluster_, username_.c_str(), password_.c_str());
    }
    
    // Performance tuning - because I want this thing to fly
    cass_cluster_set_core_connections_per_host(cluster_, 4);
    cass_cluster_set_max_connections_per_host(cluster_, 8);
    cass_cluster_set_max_concurrent_creation(cluster_, 5);
    cass_cluster_set_max_concurrent_requests_threshold(cluster_, 100);
    cass_cluster_set_request_timeout(cluster_, 12000); // 12 seconds
    cass_cluster_set_connect_timeout(cluster_, 5000);   // 5 seconds
    
    // Connect to Cassandra
    if (!connect()) {
        throw std::runtime_error("Failed to connect to Cassandra cluster");
    }
    
    // Create keyspace and tables if they don't exist
    if (!create_keyspace_if_not_exists()) {
        throw std::runtime_error("Failed to create keyspace");
    }
    
    if (!create_tables()) {
        throw std::runtime_error("Failed to create tables");
    }
    
    // Prepare our statements for better performance
    setup_prepared_statements();
    
    spdlog::info("Cassandra repository initialized successfully");
}

// Destructor - cleaning up our connections
CassandraNoteRepository::~CassandraNoteRepository() {
    disconnect();
}

// Core CRUD operations

bool CassandraNoteRepository::create(const Note& note) {
    if (!ensure_connected()) {
        spdlog::error("Not connected to Cassandra");
        return false;
    }
    
    if (!validate_note_for_cassandra(note)) {
        spdlog::error("Note validation failed for note_id: {}", note.note_id);
        return false;
    }
    
    try {
        // Create the main note entry
        CassStatement* statement = cass_prepared_bind(insert_note_stmt_);
        bind_note_to_statement(statement, note);
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            log_cassandra_error(error, "create_note");
            return false;
        }
        
        // Write to denormalized tables for fast reads
        write_to_user_timeline(note);
        
        // Only fanout to followers if it's a public note
        if (note.visibility == NoteVisibility::PUBLIC) {
            fanout_to_followers(note);
        }
        
        // Index hashtags and mentions
        write_hashtag_entries(note);
        write_mention_entries(note);
        
        // Update counters
        update_counters(note);
        
        spdlog::debug("Created note: {}", note.note_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception creating note {}: {}", note.note_id, e.what());
        return false;
    }
}

std::optional<Note> CassandraNoteRepository::get_by_id(const std::string& note_id) {
    if (!ensure_connected()) {
        return std::nullopt;
    }
    
    try {
        CassStatement* statement = cass_prepared_bind(select_note_stmt_);
        cass_statement_bind_string(statement, 0, note_id.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            log_cassandra_error(error, "get_note_by_id");
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
        Note note = map_row_to_note(row);
        cass_result_free(result);
        
        // Load related data
        populate_note_collections(note);
        
        return note;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting note {}: {}", note_id, e.what());
        return std::nullopt;
    }
}

bool CassandraNoteRepository::update(const Note& note) {
    if (!ensure_connected()) {
        return false;
    }
    
    try {
        CassStatement* statement = cass_prepared_bind(update_note_stmt_);
        bind_note_to_statement(statement, note);
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            log_cassandra_error(error, "update_note");
            return false;
        }
        
        // Invalidate timeline caches since the note changed
        invalidate_timeline_cache(note.author_id);
        
        spdlog::debug("Updated note: {}", note.note_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception updating note {}: {}", note.note_id, e.what());
        return false;
    }
}

bool CassandraNoteRepository::delete_note(const std::string& note_id) {
    if (!ensure_connected()) {
        return false;
    }
    
    try {
        // First get the note to know what to clean up
        auto note_opt = get_by_id(note_id);
        if (!note_opt) {
            spdlog::warn("Trying to delete non-existent note: {}", note_id);
            return false;
        }
        
        Note note = note_opt.value();
        
        // Soft delete the note
        CassStatement* statement = cass_prepared_bind(delete_note_stmt_);
        cass_statement_bind_string(statement, 0, note_id.c_str());
        
        CassError error = execute_statement(statement);
        cass_statement_free(statement);
        
        if (error != CASS_OK) {
            log_cassandra_error(error, "delete_note");
            return false;
        }
        
        // Clean up from indexes and timelines
        remove_note_from_indexes(note_id);
        
        // Remove from follower timelines
        auto followers = get_follower_list(note.author_id);
        for (const auto& follower_id : followers) {
            remove_timeline_entry(follower_id, note_id);
        }
        
        spdlog::debug("Deleted note: {}", note_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception deleting note {}: {}", note_id, e.what());
        return false;
    }
}

// Batch operations - for when you need to do lots of stuff at once

std::vector<Note> CassandraNotereRepository::get_by_ids(const std::vector<std::string>& note_ids) {
    std::vector<Note> notes;
    
    if (!ensure_connected() || note_ids.empty()) {
        return notes;
    }
    
    try {
        // Build query with IN clause for multiple IDs
        std::string query = "SELECT * FROM " + keyspace_ + ".notes WHERE note_id IN (";
        for (size_t i = 0; i < note_ids.size(); ++i) {
            if (i > 0) query += ",";
            query += "?";
        }
        query += ")";
        
        CassStatement* statement = cass_statement_new(query.c_str(), note_ids.size());
        
        for (size_t i = 0; i < note_ids.size(); ++i) {
            cass_statement_bind_string(statement, i, note_ids[i].c_str());
        }
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            log_cassandra_error(error, "get_notes_by_ids");
            cass_future_free(future);
            return notes;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        notes = map_result_to_notes(result);
        cass_result_free(result);
        
        // Populate collections for all notes
        for (auto& note : notes) {
            populate_note_collections(note);
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting notes by IDs: {}", e.what());
    }
    
    return notes;
}

bool CassandraNotereRepository::create_batch(const std::vector<Note>& notes) {
    if (!ensure_connected() || notes.empty()) {
        return false;
    }
    
    try {
        // Use Cassandra batch for atomicity
        CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
        
        for (const auto& note : notes) {
            if (!validate_note_for_cassandra(note)) {
                spdlog::warn("Skipping invalid note in batch: {}", note.note_id);
                continue;
            }
            
            CassStatement* statement = cass_prepared_bind(insert_note_stmt_);
            bind_note_to_statement(statement, note);
            cass_batch_add_statement(batch, statement);
            cass_statement_free(statement);
        }
        
        CassFuture* future = cass_session_execute_batch(session_, batch);
        cass_batch_free(batch);
        
        CassError error = cass_future_error_code(future);
        cass_future_free(future);
        
        if (error != CASS_OK) {
            log_cassandra_error(error, "create_batch");
            return false;
        }
        
        // Handle denormalization for each note
        for (const auto& note : notes) {
            write_to_user_timeline(note);
            if (note.visibility == NoteVisibility::PUBLIC) {
                fanout_to_followers(note);
            }
            write_hashtag_entries(note);
            write_mention_entries(note);
        }
        
        spdlog::debug("Created batch of {} notes", notes.size());
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception creating batch: {}", e.what());
        return false;
    }
}

// User timeline operations - this is where Cassandra really shines

std::vector<Note> CassandraNotereRepository::get_by_user_id(const std::string& user_id, int limit, int offset) {
    std::vector<Note> notes;
    
    if (!ensure_connected() || !validate_pagination_params(limit, offset)) {
        return notes;
    }
    
    try {
        // Query the user_notes table which is ordered by creation time
        std::string query = "SELECT * FROM " + keyspace_ + ".user_notes WHERE user_id = ? ORDER BY created_at DESC";
        if (limit > 0) query += " LIMIT " + std::to_string(limit);
        
        CassStatement* statement = cass_statement_new(query.c_str(), 1);
        cass_statement_bind_string(statement, 0, user_id.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            log_cassandra_error(error, "get_user_notes");
            cass_future_free(future);
            return notes;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        notes = map_result_to_notes(result);
        cass_result_free(result);
        
        spdlog::debug("Retrieved {} notes for user {}", notes.size(), user_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting user notes for {}: {}", user_id, e.what());
    }
    
    return notes;
}

std::vector<Note> CassandraNotereRepository::get_user_timeline(const std::string& user_id, int limit, int offset) {
    std::vector<Note> notes;
    
    if (!ensure_connected()) {
        return notes;
    }
    
    try {
        // Query the pre-computed timeline table
        std::string query = "SELECT * FROM " + keyspace_ + ".user_timeline WHERE user_id = ? ORDER BY timeline_id DESC";
        if (limit > 0) query += " LIMIT " + std::to_string(limit);
        
        CassStatement* statement = cass_statement_new(query.c_str(), 1);
        cass_statement_bind_string(statement, 0, user_id.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            log_cassandra_error(error, "get_user_timeline");
            cass_future_free(future);
            return notes;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        // Extract note IDs from timeline entries
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
        
        // Fetch the actual notes
        notes = get_by_ids(note_ids);
        
        spdlog::debug("Retrieved timeline with {} notes for user {}", notes.size(), user_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting timeline for {}: {}", user_id, e.what());
    }
    
    return notes;
}

// Hashtag operations - finding notes by hashtags

std::vector<Note> CassandraNotereRepository::get_by_hashtag(const std::string& hashtag, int limit, int offset) {
    std::vector<Note> notes;
    
    if (!ensure_connected()) {
        return notes;
    }
    
    try {
        std::string query = "SELECT note_id FROM " + keyspace_ + ".hashtag_notes WHERE hashtag = ? ORDER BY created_at DESC";
        if (limit > 0) query += " LIMIT " + std::to_string(limit);
        
        CassStatement* statement = cass_statement_new(query.c_str(), 1);
        cass_statement_bind_string(statement, 0, hashtag.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            log_cassandra_error(error, "get_hashtag_notes");
            cass_future_free(future);
            return notes;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        // Extract note IDs
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
        
        // Get the actual notes
        notes = get_by_ids(note_ids);
        
        spdlog::debug("Found {} notes for hashtag #{}", notes.size(), hashtag);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting hashtag notes for #{}: {}", hashtag, e.what());
    }
    
    return notes;
}

// Connection management

bool CassandraNotereRepository::connect() {
    if (is_connected_) {
        return true;
    }
    
    spdlog::info("Connecting to Cassandra cluster...");
    
    session_ = cass_session_new();
    CassFuture* connect_future = cass_session_connect(session_, cluster_);
    
    CassError error = cass_future_error_code(connect_future);
    cass_future_free(connect_future);
    
    if (error != CASS_OK) {
        handle_connection_error(error);
        cass_session_free(session_);
        session_ = nullptr;
        return false;
    }
    
    is_connected_ = true;
    spdlog::info("Connected to Cassandra successfully");
    return true;
}

void CassandraNotereRepository::disconnect() {
    if (session_) {
        CassFuture* close_future = cass_session_close(session_);
        cass_future_wait(close_future);
        cass_future_free(close_future);
        cass_session_free(session_);
        session_ = nullptr;
    }
    
    if (cluster_) {
        cass_cluster_free(cluster_);
        cluster_ = nullptr;
    }
    
    is_connected_ = false;
    spdlog::info("Disconnected from Cassandra");
}

bool CassandraNotereRepository::ensure_connected() {
    if (!is_connected_ && !connect()) {
        return false;
    }
    return test_connection();
}

bool CassandraNotereRepository::test_connection() {
    if (!session_) {
        return false;
    }
    
    // Simple query to test connection
    CassStatement* statement = cass_statement_new("SELECT now() FROM system.local", 0);
    CassFuture* future = execute_async(statement);
    cass_statement_free(statement);
    
    CassError error = cass_future_error_code(future);
    cass_future_free(future);
    
    return error == CASS_OK;
}

// Schema management

bool CassandraNotereRepository::create_keyspace_if_not_exists() {
    if (!session_) {
        return false;
    }
    
    std::string query = 
        "CREATE KEYSPACE IF NOT EXISTS " + keyspace_ + " "
        "WITH REPLICATION = { "
        "'class': 'SimpleStrategy', "
        "'replication_factor': 3 "
        "}";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        log_cassandra_error(error, "create_keyspace");
        return false;
    }
    
    // Use the keyspace
    std::string use_query = "USE " + keyspace_;
    statement = cass_statement_new(use_query.c_str(), 0);
    error = execute_statement(statement);
    cass_statement_free(statement);
    
    return error == CASS_OK;
}

bool CassandraNotereRepository::create_tables() {
    try {
        create_notes_table();
        create_user_notes_table();
        create_timeline_table();
        create_hashtag_notes_table();
        create_mention_notes_table();
        create_note_counters_table();
        create_trending_table();
        create_user_interactions_table();
        
        spdlog::info("Created all Cassandra tables successfully");
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to create tables: {}", e.what());
        return false;
    }
}

void CassandraNotereRepository::create_notes_table() {
    // Main notes table - the source of truth
    std::string query = 
        "CREATE TABLE IF NOT EXISTS notes ("
        "note_id TEXT PRIMARY KEY, "
        "author_id TEXT, "
        "author_username TEXT, "
        "content TEXT, "
        "raw_content TEXT, "
        "processed_content TEXT, "
        "note_type INT, "
        "visibility INT, "
        "status INT, "
        "content_warning INT, "
        "reply_to_id TEXT, "
        "reply_to_user_id TEXT, "
        "renote_of_id TEXT, "
        "quote_of_id TEXT, "
        "thread_id TEXT, "
        "thread_position INT, "
        "like_count INT, "
        "renote_count INT, "
        "reply_count INT, "
        "quote_count INT, "
        "view_count INT, "
        "bookmark_count INT, "
        "is_sensitive BOOLEAN, "
        "is_nsfw BOOLEAN, "
        "contains_spoilers BOOLEAN, "
        "spam_score DOUBLE, "
        "toxicity_score DOUBLE, "
        "latitude DOUBLE, "
        "longitude DOUBLE, "
        "location_name TEXT, "
        "created_at TIMESTAMP, "
        "updated_at TIMESTAMP, "
        "scheduled_at TIMESTAMP, "
        "deleted_at TIMESTAMP, "
        "client_name TEXT, "
        "client_version TEXT, "
        "user_agent TEXT, "
        "ip_address TEXT, "
        "is_promoted BOOLEAN, "
        "is_verified_author BOOLEAN, "
        "allow_replies BOOLEAN, "
        "allow_renotes BOOLEAN, "
        "allow_quotes BOOLEAN"
        ")";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create notes table");
    }
}

void CassandraNotereRepository::create_user_notes_table() {
    // User timeline table - for getting a user's notes quickly
    std::string query = 
        "CREATE TABLE IF NOT EXISTS user_notes ("
        "user_id TEXT, "
        "created_at TIMESTAMP, "
        "note_id TEXT, "
        "PRIMARY KEY (user_id, created_at, note_id)"
        ") WITH CLUSTERING ORDER BY (created_at DESC, note_id DESC)";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create user_notes table");
    }
}

void CassandraNotereRepository::create_timeline_table() {
    // Timeline table - pre-computed timelines for users
    std::string query = 
        "CREATE TABLE IF NOT EXISTS user_timeline ("
        "user_id TEXT, "
        "timeline_id TIMEUUID, "
        "note_id TEXT, "
        "author_id TEXT, "
        "note_type INT, "
        "created_at TIMESTAMP, "
        "PRIMARY KEY (user_id, timeline_id)"
        ") WITH CLUSTERING ORDER BY (timeline_id DESC)";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create user_timeline table");
    }
}

void CassandraNotereRepository::create_hashtag_notes_table() {
    // Hashtag index - for finding notes by hashtag
    std::string query = 
        "CREATE TABLE IF NOT EXISTS hashtag_notes ("
        "hashtag TEXT, "
        "created_at TIMESTAMP, "
        "note_id TEXT, "
        "author_id TEXT, "
        "PRIMARY KEY (hashtag, created_at, note_id)"
        ") WITH CLUSTERING ORDER BY (created_at DESC, note_id DESC)";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create hashtag_notes table");
    }
}

// More table creation methods would continue here...

void CassandraNoteRepository::create_mention_notes_table() {
    // Mention index - for finding notes by mentions
    std::string query = 
        "CREATE TABLE IF NOT EXISTS mention_notes ("
        "mentioned_user_id TEXT, "
        "created_at TIMESTAMP, "
        "note_id TEXT, "
        "author_id TEXT, "
        "PRIMARY KEY (mentioned_user_id, created_at, note_id)"
        ") WITH CLUSTERING ORDER BY (created_at DESC, note_id DESC)";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create mention_notes table");
    }
}

void CassandraNoteRepository::create_note_counters_table() {
    // Counters table - for tracking engagement metrics
    std::string query = 
        "CREATE TABLE IF NOT EXISTS note_counters ("
        "note_id TEXT PRIMARY KEY, "
        "like_count COUNTER, "
        "renote_count COUNTER, "
        "reply_count COUNTER, "
        "quote_count COUNTER, "
        "view_count COUNTER, "
        "bookmark_count COUNTER"
        ")";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create note_counters table");
    }
}

void CassandraNoteRepository::create_trending_table() {
    // Trending hashtags table
    std::string query = 
        "CREATE TABLE IF NOT EXISTS trending_hashtags ("
        "time_bucket TEXT, "  // hourly/daily bucket
        "hashtag TEXT, "
        "note_count COUNTER, "
        "PRIMARY KEY (time_bucket, hashtag)"
        ")";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create trending_hashtags table");
    }
}

void CassandraNoteRepository::create_user_interactions_table() {
    // User interactions table - for tracking who liked/renoted what
    std::string query = 
        "CREATE TABLE IF NOT EXISTS user_interactions ("
        "user_id TEXT, "
        "interaction_type TEXT, "
        "created_at TIMESTAMP, "
        "note_id TEXT, "
        "author_id TEXT, "
        "PRIMARY KEY ((user_id, interaction_type), created_at, note_id)"
        ") WITH CLUSTERING ORDER BY (created_at DESC, note_id DESC)";
    
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassError error = execute_statement(statement);
    cass_statement_free(statement);
    
    if (error != CASS_OK) {
        throw std::runtime_error("Failed to create user_interactions table");
    }
}

// Update batch operations - fixing those methods I missed earlier

bool CassandraNoteRepository::update_batch(const std::vector<Note>& notes) {
    if (!ensure_connected() || notes.empty()) {
        return false;
    }
    
    try {
        CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
        
        for (const auto& note : notes) {
            CassStatement* statement = cass_prepared_bind(update_note_stmt_);
            bind_note_to_statement(statement, note);
            cass_batch_add_statement(batch, statement);
            cass_statement_free(statement);
        }
        
        CassFuture* future = cass_session_execute_batch(session_, batch);
        cass_batch_free(batch);
        
        CassError error = cass_future_error_code(future);
        cass_future_free(future);
        
        if (error != CASS_OK) {
            log_cassandra_error(error, "update_batch");
            return false;
        }
        
        spdlog::debug("Updated batch of {} notes", notes.size());
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception updating batch: {}", e.what());
        return false;
    }
}

bool CassandraNoteRepository::delete_batch(const std::vector<std::string>& note_ids) {
    if (!ensure_connected() || note_ids.empty()) {
        return false;
    }
    
    try {
        CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
        
        for (const auto& note_id : note_ids) {
            CassStatement* statement = cass_prepared_bind(delete_note_stmt_);
            cass_statement_bind_string(statement, 0, note_id.c_str());
            cass_batch_add_statement(batch, statement);
            cass_statement_free(statement);
        }
        
        CassFuture* future = cass_session_execute_batch(session_, batch);
        cass_batch_free(batch);
        
        CassError error = cass_future_error_code(future);
        cass_future_free(future);
        
        if (error != CASS_OK) {
            log_cassandra_error(error, "delete_batch");
            return false;
        }
        
        spdlog::debug("Deleted batch of {} notes", note_ids.size());
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception deleting batch: {}", e.what());
        return false;
    }
}

// Count operations

int CassandraNoteRepository::count_by_user_id(const std::string& user_id) {
    if (!ensure_connected()) {
        return 0;
    }
    
    try {
        std::string query = "SELECT COUNT(*) FROM " + keyspace_ + ".user_notes WHERE user_id = ?";
        
        CassStatement* statement = cass_statement_new(query.c_str(), 1);
        cass_statement_bind_string(statement, 0, user_id.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            log_cassandra_error(error, "count_user_notes");
            cass_future_free(future);
            return 0;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        const CassRow* row = cass_result_first_row(result);
        if (!row) {
            cass_result_free(result);
            return 0;
        }
        
        const CassValue* count_value = cass_row_get_column(row, 0);
        cass_int64_t count;
        cass_value_get_int64(count_value, &count);
        cass_result_free(result);
        
        return static_cast<int>(count);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception counting notes for user {}: {}", user_id, e.what());
        return 0;
    }
}

// Engagement operations - the fun stuff

std::vector<Note> CassandraNoteRepository::get_liked_by_user(const std::string& user_id, int limit, int offset) {
    return get_user_interactions(user_id, "like", limit, offset);
}

std::vector<Note> CassandraNoteRepository::get_renoted_by_user(const std::string& user_id, int limit, int offset) {
    return get_user_interactions(user_id, "renote", limit, offset);
}

std::vector<Note> CassandraNoteRepository::get_bookmarked_by_user(const std::string& user_id, int limit, int offset) {
    return get_user_interactions(user_id, "bookmark", limit, offset);
}

// Helper method for user interactions
std::vector<Note> CassandraNoteRepository::get_user_interactions(const std::string& user_id, 
                                                               const std::string& interaction_type,
                                                               int limit, int offset) {
    std::vector<Note> notes;
    
    if (!ensure_connected()) {
        return notes;
    }
    
    try {
        std::string query = "SELECT note_id FROM " + keyspace_ + ".user_interactions "
                           "WHERE user_id = ? AND interaction_type = ? "
                           "ORDER BY created_at DESC";
        if (limit > 0) query += " LIMIT " + std::to_string(limit);
        
        CassStatement* statement = cass_statement_new(query.c_str(), 2);
        cass_statement_bind_string(statement, 0, user_id.c_str());
        cass_statement_bind_string(statement, 1, interaction_type.c_str());
        
        CassFuture* future = execute_async(statement);
        cass_statement_free(statement);
        
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            log_cassandra_error(error, "get_user_interactions");
            cass_future_free(future);
            return notes;
        }
        
        const CassResult* result = cass_future_get_result(future);
        cass_future_free(future);
        
        // Extract note IDs
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
        
        // Get the actual notes
        notes = get_by_ids(note_ids);
        
        spdlog::debug("Found {} {} notes for user {}", notes.size(), interaction_type, user_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Exception getting {} interactions for {}: {}", interaction_type, user_id, e.what());
    }
    
    return notes;
}

// More implementation methods would continue here...
// [Additional implementation methods follow the same pattern]

} // namespace sonet::note::repositories
