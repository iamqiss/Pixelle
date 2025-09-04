/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "../models/note.h"
#include "../validators/note_validator.h"
#include "../repositories/note_repository.h"
#include <nlohmann/json.hpp>
#include <string>
#include <memory>

namespace sonet::note::handlers {

using json = nlohmann::json;
using namespace sonet::note::models;
using namespace sonet::note::validators;
using namespace sonet::note::repositories;

/**
 * @brief Handler for creating new notes with comprehensive validation
 * 
 * Handles the complete note creation pipeline:
 * - Request validation and sanitization
 * - Content processing and feature extraction
 * - Attachment handling and validation
 * - Database persistence
 * - Real-time broadcasting
 * - Analytics tracking
 */
class CreateNoteHandler {
public:
    CreateNoteHandler(
        std::shared_ptr<NoteRepository> note_repo,
        std::shared_ptr<NoteValidator> validator
    );
    
    /**
     * @brief Create a new note from HTTP request
     */
    json handle_create_note(const json& request_data, const std::string& user_id);
    
    /**
     * @brief Create a reply note
     */
    json handle_create_reply(const json& request_data, const std::string& user_id, const std::string& reply_to_id);
    
    /**
     * @brief Create a renote (renote)
     */
    json handle_create_renote(const json& request_data, const std::string& user_id, const std::string& renote_of_id);
    
    /**
     * @brief Create a quote note
     */
    json handle_create_quote(const json& request_data, const std::string& user_id, const std::string& quote_of_id);
    
    /**
     * @brief Create a thread note
     */
    json handle_create_thread_note(const json& request_data, const std::string& user_id, const std::string& thread_id, int position);
    
    /**
     * @brief Schedule a note for future noteing
     */
    json handle_schedule_note(const json& request_data, const std::string& user_id, std::time_t scheduled_at);

private:
    std::shared_ptr<NoteRepository> note_repository_;
    std::shared_ptr<NoteValidator> validator_;
    
    // Helper methods
    Note create_note_from_request(const json& request_data, const std::string& user_id) const;
    void process_attachments(Note& note, const json& attachments) const;
    void broadcast_note_created(const Note& note) const;
    void track_creation_analytics(const Note& note) const;
    json create_success_response(const Note& note) const;
    json create_error_response(const std::string& error_code, const std::string& message) const;
};

} // namespace sonet::note::handlers
