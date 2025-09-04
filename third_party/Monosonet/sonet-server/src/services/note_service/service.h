/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "models/note.h"
#include "validators/note_validator.h"
#include "repositories/note_repository.h"
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <vector>
#include <optional>

namespace sonet::note {

using json = nlohmann::json;
using namespace sonet::note::models;
using namespace sonet::note::validators;
using namespace sonet::note::repositories;

/**
 * Core Note Service Implementation
 */
class NoteService {
public:
    NoteService(
        std::shared_ptr<NoteRepository> note_repository,
        std::shared_ptr<NoteValidator> validator
    );

    // Core note operations
    std::optional<Note> create_note(const std::string& user_id, const json& note_data);
    std::optional<Note> get_note(const std::string& note_id, const std::string& requesting_user_id = "");
    std::optional<Note> update_note(const std::string& note_id, const json& update_data, const std::string& user_id);
    bool delete_note(const std::string& note_id, const std::string& user_id);

    // Engagement
    json like_note(const std::string& user_id, const std::string& note_id);
    json unlike_note(const std::string& user_id, const std::string& note_id);
    std::optional<Note> create_renote(const std::string& note_id, const std::string& user_id);
    bool remove_renote(const std::string& note_id, const std::string& user_id);
    bool has_user_renoted(const std::string& note_id, const std::string& user_id);

    // Threads
    std::vector<Note> get_thread(const std::string& root_note_id);
    std::vector<Note> get_thread_context(const std::string& note_id);

    // Timelines
    std::vector<Note> get_timeline(const std::string& user_id, int limit = 20, const std::string& cursor = "");
    json get_timeline_updates(const std::string& user_id, const std::string& since_cursor = "");
    std::vector<Note> get_user_timeline(const std::string& target_user_id, const std::string& requesting_user_id = "", int limit = 20);

    // Search
    std::vector<Note> search_notes(const std::string& query, const std::string& user_id = "", int limit = 20);
    std::vector<std::string> search_hashtags(const std::string& query, int limit = 20);

    // Analytics
    json get_note_analytics(const std::string& note_id, const std::string& user_id);
    json get_user_analytics(const std::string& user_id);

    // Attachments
    void process_attachment(const std::string& note_id, const json& attachment_data);

private:
    std::shared_ptr<NoteRepository> note_repository_;
    std::shared_ptr<NoteValidator> validator_;

    // Helpers
    void track_note_view(const std::string& note_id, const std::string& user_id);
    void invalidate_timeline_cache(const std::string& user_id);
};

} // namespace sonet::note
