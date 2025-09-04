/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "note_repository.h"
#include "cassandra_note_repository.h"
#include <spdlog/spdlog.h>
#include <sstream>
#include <algorithm>

namespace sonet::note::repositories {

// postgresql implementation - keeping this simple since Cassandra is the star

NotegreSQLNoteRepository::NotegreSQLNoteRepository(std::shared_ptr<pqxx::connection> connection)
    : db_connection_(connection) {
    
    // I'm keeping the postgresql implementation for compatibility, but honestly 
    // Cassandra is where the magic happens for this kind of workload
    notes_table_ = "notes";
    note_metrics_table_ = "note_metrics";
    note_hashtags_table_ = "note_hashtags";
    note_mentions_table_ = "note_mentions";
    note_urls_table_ = "note_urls";
    user_interactions_table_ = "user_interactions";
    
    spdlog::info("postgresql Note Repository initialized");
    
    // Create schema if it doesn't exist
    create_database_schema();
    create_indexes();
    setup_prepared_statements();
}

bool NotegreSQLNoteRepository::create(const Note& note) {
    if (!validate_note_data(note)) {
        spdlog::error("Note validation failed for: {}", note.note_id);
        return false;
    }
    
    try {
        pqxx::work txn(*db_connection_);
        
        // Insert main note
        std::string insert_query = build_insert_query(note);
        txn.exec(insert_query);
        
        // Insert related data
        save_note_hashtags(txn, note);
        save_note_mentions(txn, note);
        save_note_urls(txn, note);
        save_note_metrics(txn, note);
        
        txn.commit();
        
        spdlog::debug("Created note in postgresql: {}", note.note_id);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("postgresql error creating note {}: {}", note.note_id, e.what());
        return false;
    }
}

std::optional<Note> NotegreSQLNoteRepository::get_by_id(const std::string& note_id) {
    try {
        pqxx::work txn(*db_connection_);
        
        std::string query = build_select_query() + " WHERE note_id = " + txn.quote(note_id);
        pqxx::result result = txn.exec(query);
        
        if (result.empty()) {
            return std::nullopt;
        }
        
        Note note = map_row_to_note(result[0]);
        populate_note_relations(note);
        populate_note_metrics(note);
        
        return note;
        
    } catch (const std::exception& e) {
        spdlog::error("postgresql error getting note {}: {}", note_id, e.what());
        return std::nullopt;
    }
}

bool NotegreSQLNoteRepository::update(const Note& note) {
    try {
        pqxx::work txn(*db_connection_);
        
        std::string update_query = build_update_query(note);
        txn.exec(update_query);
        
        txn.commit();
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("postgresql error updating note {}: {}", note.note_id, e.what());
        return false;
    }
}

bool NotegreSQLNoteRepository::delete_note(const std::string& note_id) {
    try {
        pqxx::work txn(*db_connection_);
        
        std::string query = "UPDATE " + notes_table_ + " SET status = " + 
                           std::to_string(static_cast<int>(NoteStatus::DELETED)) + 
                           ", deleted_at = NOW() WHERE note_id = " + txn.quote(note_id);
        
        pqxx::result result = txn.exec(query);
        txn.commit();
        
        return result.affected_rows() > 0;
        
    } catch (const std::exception& e) {
        spdlog::error("postgresql error deleting note {}: {}", note_id, e.what());
        return false;
    }
}

// User timeline operations
std::vector<Note> NotegreSQLNoteRepository::get_by_user_id(const std::string& user_id, int limit, int offset) {
    std::vector<Note> notes;
    
    try {
        pqxx::work txn(*db_connection_);
        
        std::string query = build_select_query() + " WHERE author_id = " + txn.quote(user_id) + 
                           " ORDER BY created_at DESC";
        if (limit > 0) query += " LIMIT " + std::to_string(limit);
        if (offset > 0) query += " OFFSET " + std::to_string(offset);
        
        pqxx::result result = txn.exec(query);
        notes = map_result_to_notes(result);
        
    } catch (const std::exception& e) {
        spdlog::error("postgresql error getting user notes for {}: {}", user_id, e.what());
    }
    
    return notes;
}

std::vector<Note> NotegreSQLNoteRepository::get_user_timeline(const std::string& user_id, int limit, int offset) {
    // For postgresql, this is basically the same as get_by_user_id
    // In production, you'd have materialized views or timeline tables
    return get_by_user_id(user_id, limit, offset);
}

// Engagement operations
std::vector<Note> NotegreSQLNoteRepository::get_liked_by_user(const std::string& user_id, int limit, int offset) {
    return get_user_interaction_notes(user_id, "like", limit, offset);
}

std::vector<Note> NotegreSQLNoteRepository::get_renoted_by_user(const std::string& user_id, int limit, int offset) {
    return get_user_interaction_notes(user_id, "renote", limit, offset);
}

std::vector<Note> NotegreSQLNoteRepository::get_bookmarked_by_user(const std::string& user_id, int limit, int offset) {
    return get_user_interaction_notes(user_id, "bookmark", limit, offset);
}

// Helper methods - the behind-the-scenes stuff

std::vector<Note> NotegreSQLNoteRepository::get_user_interaction_notes(const std::string& user_id, 
                                                                      const std::string& interaction_type,
                                                                      int limit, int offset) {
    std::vector<Note> notes;
    
    try {
        pqxx::work txn(*db_connection_);
        
        std::string query = 
            "SELECT n.* FROM " + notes_table_ + " n "
            "JOIN " + user_interactions_table_ + " ui ON n.note_id = ui.note_id "
            "WHERE ui.user_id = " + txn.quote(user_id) + 
            " AND ui.interaction_type = " + txn.quote(interaction_type) + 
            " ORDER BY ui.created_at DESC";
            
        if (limit > 0) query += " LIMIT " + std::to_string(limit);
        if (offset > 0) query += " OFFSET " + std::to_string(offset);
        
        pqxx::result result = txn.exec(query);
        notes = map_result_to_notes(result);
        
    } catch (const std::exception& e) {
        spdlog::error("postgresql error getting {} interactions for {}: {}", 
                     interaction_type, user_id, e.what());
    }
    
    return notes;
}

void NotegreSQLNoteRepository::create_database_schema() {
    try {
        pqxx::work txn(*db_connection_);
        
        // Create main notes table
        std::string create_notes = R"(
            CREATE TABLE IF NOT EXISTS notes (
                note_id VARCHAR(255) PRIMARY KEY,
                author_id VARCHAR(255) NOT NULL,
                author_username VARCHAR(255),
                content TEXT,
                raw_content TEXT,
                processed_content TEXT,
                note_type INTEGER DEFAULT 0,
                visibility INTEGER DEFAULT 0,
                status INTEGER DEFAULT 0,
                content_warning INTEGER DEFAULT 0,
                reply_to_id VARCHAR(255),
                reply_to_user_id VARCHAR(255),
                renote_of_id VARCHAR(255),
                quote_of_id VARCHAR(255),
                thread_id VARCHAR(255),
                thread_position INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                renote_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                quote_count INTEGER DEFAULT 0,
                view_count INTEGER DEFAULT 0,
                bookmark_count INTEGER DEFAULT 0,
                is_sensitive BOOLEAN DEFAULT FALSE,
                is_nsfw BOOLEAN DEFAULT FALSE,
                contains_spoilers BOOLEAN DEFAULT FALSE,
                spam_score REAL DEFAULT 0.0,
                toxicity_score REAL DEFAULT 0.0,
                latitude REAL,
                longitude REAL,
                location_name TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                scheduled_at TIMESTAMP,
                deleted_at TIMESTAMP,
                client_name VARCHAR(255),
                client_version VARCHAR(255),
                user_agent TEXT,
                ip_address INET,
                is_promoted BOOLEAN DEFAULT FALSE,
                is_verified_author BOOLEAN DEFAULT FALSE,
                allow_replies BOOLEAN DEFAULT TRUE,
                allow_renotes BOOLEAN DEFAULT TRUE,
                allow_quotes BOOLEAN DEFAULT TRUE
            )
        )";
        
        txn.exec(create_notes);
        
        // Create hashtags table
        std::string create_hashtags = R"(
            CREATE TABLE IF NOT EXISTS note_hashtags (
                note_id VARCHAR(255) REFERENCES notes(note_id),
                hashtag VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (note_id, hashtag)
            )
        )";
        
        txn.exec(create_hashtags);
        
        // Create mentions table
        std::string create_mentions = R"(
            CREATE TABLE IF NOT EXISTS note_mentions (
                note_id VARCHAR(255) REFERENCES notes(note_id),
                mentioned_user_id VARCHAR(255),
                username VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (note_id, mentioned_user_id)
            )
        )";
        
        txn.exec(create_mentions);
        
        // Create user interactions table
        std::string create_interactions = R"(
            CREATE TABLE IF NOT EXISTS user_interactions (
                user_id VARCHAR(255),
                note_id VARCHAR(255) REFERENCES notes(note_id),
                interaction_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, note_id, interaction_type)
            )
        )";
        
        txn.exec(create_interactions);
        
        txn.commit();
        spdlog::info("postgresql schema created successfully");
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to create postgresql schema: {}", e.what());
    }
}

void NotegreSQLNoteRepository::create_indexes() {
    try {
        pqxx::work txn(*db_connection_);
        
        // Performance indexes - because slow queries are the enemy
        std::vector<std::string> index_queries = {
            "CREATE INDEX IF NOT EXISTS idx_notes_author_created ON notes(author_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_notes_created_at ON notes(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_notes_hashtags ON note_hashtags(hashtag, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_notes_mentions ON note_mentions(mentioned_user_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_user_interactions ON user_interactions(user_id, interaction_type, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_notes_visibility_status ON notes(visibility, status) WHERE status = 0"
        };
        
        for (const auto& query : index_queries) {
            txn.exec(query);
        }
        
        txn.commit();
        spdlog::info("postgresql indexes created successfully");
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to create postgresql indexes: {}", e.what());
    }
}

// Factory methods

std::unique_ptr<NotegreSQLNoteRepository> NoteRepositoryFactory::create_notegresql_repository(const std::string& connection_string) {
    validate_connection_string(connection_string);
    auto connection = create_database_connection(connection_string);
    return std::make_unique<NotegreSQLNoteRepository>(connection);
}

std::shared_ptr<pqxx::connection> NoteRepositoryFactory::create_database_connection(const std::string& connection_string) {
    try {
        auto connection = std::make_shared<pqxx::connection>(connection_string);
        
        // Test the connection
        pqxx::work test_txn(*connection);
        test_txn.exec("SELECT 1");
        
        spdlog::info("postgresql connection established successfully");
        return connection;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to connect to postgresql: {}", e.what());
        throw;
    }
}

void NoteRepositoryFactory::validate_connection_string(const std::string& connection_string) {
    if (connection_string.empty()) {
        throw std::invalid_argument("Connection string cannot be empty");
    }
    
    // Basic validation - should contain host and dbname
    if (connection_string.find("host=") == std::string::npos || 
        connection_string.find("dbname=") == std::string::npos) {
        throw std::invalid_argument("Invalid postgresql connection string format");
    }
}

// Cassandra factory methods

std::unique_ptr<CassandraNoteRepository> CassandraRepositoryFactory::create_repository(
    const std::vector<std::string>& contact_points,
    const std::string& keyspace,
    const std::string& username,
    const std::string& password,
    int port) {
    
    if (contact_points.empty()) {
        throw std::invalid_argument("Contact points cannot be empty");
    }
    
    if (keyspace.empty()) {
        throw std::invalid_argument("Keyspace cannot be empty");
    }
    
    return std::make_unique<CassandraNoteRepository>(contact_points, keyspace, username, password, port);
}

bool CassandraRepositoryFactory::test_connection(
    const std::vector<std::string>& contact_points,
    const std::string& username,
    const std::string& password,
    int port) {
    
    try {
        CassCluster* test_cluster = cass_cluster_new();
        
        std::string contacts = "";
        for (size_t i = 0; i < contact_points.size(); ++i) {
            if (i > 0) contacts += ",";
            contacts += contact_points[i];
        }
        
        cass_cluster_set_contact_points(test_cluster, contacts.c_str());
        cass_cluster_set_port(test_cluster, port);
        
        if (!username.empty() && !password.empty()) {
            cass_cluster_set_credentials(test_cluster, username.c_str(), password.c_str());
        }
        
        CassSession* test_session = cass_session_new();
        CassFuture* connect_future = cass_session_connect(test_session, test_cluster);
        
        CassError error = cass_future_error_code(connect_future);
        cass_future_free(connect_future);
        
        cass_session_free(test_session);
        cass_cluster_free(test_cluster);
        
        return error == CASS_OK;
        
    } catch (const std::exception& e) {
        spdlog::error("Cassandra connection test failed: {}", e.what());
        return false;
    }
}

} // namespace sonet::note::repositories