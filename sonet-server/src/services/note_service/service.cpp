/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "service.h"
#include <spdlog/spdlog.h>
#include <chrono>

namespace sonet::note {

NoteService::NoteService(
    std::shared_ptr<NoteRepository> note_repository,
    std::shared_ptr<NoteValidator> validator
) : note_repository_(std::move(note_repository)), validator_(std::move(validator)) {
    spdlog::info("NoteService initialized");
}

std::optional<Note> NoteService::create_note(const std::string& user_id, const json& note_data) {
    try {
        // Validate the note data
        auto validation_result = validator_->validate_note(note_data);
        if (!validation_result.is_valid) {
            spdlog::warn("Validation failed: {} - {}", validation_result.error_code, validation_result.error_message);
            return std::nullopt;
        }

        // Create Note object from data
        Note note;
        note.author_id = user_id;
        note.content = note_data.value("content", "");
        note.visibility = NoteVisibility::PUBLIC; // TODO: map from note_data

        auto now = std::chrono::system_clock::now();
        note.created_at = std::chrono::system_clock::to_time_t(now);
        note.updated_at = note.created_at;

        // Persist
        if (!note_repository_->create(note)) {
            spdlog::error("Failed to save note for user {}", user_id);
            return std::nullopt;
        }

        // If repository sets IDs externally, fetch the created note by id if available
        // Fall back to not returning if we cannot resolve an id
        if (!note.note_id.empty()) {
            auto saved = note_repository_->get_by_id(note.note_id);
            if (saved) {
                invalidate_timeline_cache(user_id);
                return saved;
            }
        }

        invalidate_timeline_cache(user_id);
        return note; // Return the constructed note if repo didn't assign id differently

    } catch (const std::exception& e) {
        spdlog::error("Error creating note: {}", e.what());
        return std::nullopt;
    }
}

std::optional<Note> NoteService::get_note(const std::string& note_id, const std::string& requesting_user_id) {
    try {
        auto note = note_repository_->get_by_id(note_id);
        if (!note) {
            return std::nullopt;
        }

        if (!requesting_user_id.empty()) {
            track_note_view(note_id, requesting_user_id);
        }

        return note;

    } catch (const std::exception& e) {
        spdlog::error("Error getting note {}: {}", note_id, e.what());
        return std::nullopt;
    }
}

std::optional<Note> NoteService::update_note(const std::string& note_id, const json& update_data, const std::string& user_id) {
    try {
        auto existing = note_repository_->get_by_id(note_id);
        if (!existing) return std::nullopt;
        if (existing->author_id != user_id) return std::nullopt;

        auto validation_result = validator_->validate_update_request(update_data, *existing);
        if (!validation_result.is_valid) return std::nullopt;

        if (update_data.contains("content")) {
            existing->content = update_data.value("content", existing->content);
        }
        existing->updated_at = std::time(nullptr);

        if (!note_repository_->update(*existing)) return std::nullopt;
        return existing;

    } catch (const std::exception& e) {
        spdlog::error("Error updating note {}: {}", note_id, e.what());
        return std::nullopt;
    }
}

bool NoteService::delete_note(const std::string& note_id, const std::string& user_id) {
    try {
        auto existing = note_repository_->get_by_id(note_id);
        if (!existing) return false;
        if (existing->author_id != user_id) return false;

        bool ok = note_repository_->delete_note(note_id);
        if (ok) invalidate_timeline_cache(user_id);
        return ok;

    } catch (const std::exception& e) {
        spdlog::error("Error deleting note {}: {}", note_id, e.what());
        return false;
    }
}

json NoteService::like_note(const std::string& user_id, const std::string& note_id) {
    try {
        auto note = note_repository_->get_by_id(note_id);
        if (!note) {
            return nlohmann::json{{"success", false}, {"error", {{"code", "NOTE_NOT_FOUND"}, {"message", "Note not found"}}}};
        }
        // TODO: persist like via repository user_interactions
        note->like_count += 1;
        note_repository_->update(*note);
        return nlohmann::json{{"success", true}, {"new_like_count", note->like_count}};

    } catch (const std::exception& e) {
        spdlog::error("Error liking note {}: {}", note_id, e.what());
        return nlohmann::json{{"success", false}, {"error", {{"code", "INTERNAL_ERROR"}, {"message", "Internal server error"}}}};
    }
}

json NoteService::unlike_note(const std::string& user_id, const std::string& note_id) {
    try {
        auto note = note_repository_->get_by_id(note_id);
        if (!note) {
            return nlohmann::json{{"success", false}, {"error", {{"code", "NOTE_NOT_FOUND"}, {"message", "Note not found"}}}};
        }
        if (note->like_count > 0) note->like_count -= 1;
        note_repository_->update(*note);
        return nlohmann::json{{"success", true}, {"new_like_count", note->like_count}};

    } catch (const std::exception& e) {
        spdlog::error("Error unliking note {}: {}", note_id, e.what());
        return nlohmann::json{{"success", false}, {"error", {{"code", "INTERNAL_ERROR"}, {"message", "Internal server error"}}}};
    }
}

std::optional<Note> NoteService::create_renote(const std::string& note_id, const std::string& user_id) {
    // Minimal stub
    return std::nullopt;
}

bool NoteService::remove_renote(const std::string& note_id, const std::string& user_id) {
    // Minimal stub
    return false;
}

bool NoteService::has_user_renoted(const std::string& note_id, const std::string& user_id) {
    // Minimal stub
    return false;
}

std::vector<Note> NoteService::get_thread(const std::string& root_note_id) {
    // Minimal stub
    return {};
}

std::vector<Note> NoteService::get_thread_context(const std::string& note_id) {
    // Minimal stub
    return {};
}

std::vector<Note> NoteService::get_timeline(const std::string& user_id, int limit, const std::string& cursor) {
    // Minimal stub
    return {};
}

json NoteService::get_timeline_updates(const std::string& user_id, const std::string& since_cursor) {
    return nlohmann::json{{"success", false}, {"error", {{"code", "NOT_IMPLEMENTED"}, {"message", "Timeline updates not implemented"}}}};
}

std::vector<Note> NoteService::get_user_timeline(const std::string& target_user_id, const std::string& requesting_user_id, int limit) {
    return note_repository_->get_by_user_id(target_user_id, limit, 0);
}

std::vector<Note> NoteService::search_notes(const std::string& query, const std::string& user_id, int limit) {
    // Minimal stub
    return {};
}

std::vector<std::string> NoteService::search_hashtags(const std::string& query, int limit) {
    // Minimal stub
    return {};
}

json NoteService::get_note_analytics(const std::string& note_id, const std::string& user_id) {
    return nlohmann::json{{"success", false}, {"error", {{"code", "NOT_IMPLEMENTED"}, {"message", "Analytics not implemented"}}}};
}

json NoteService::get_user_analytics(const std::string& user_id) {
    return nlohmann::json{{"success", false}, {"error", {{"code", "NOT_IMPLEMENTED"}, {"message", "User analytics not implemented"}}}};
}

void NoteService::process_attachment(const std::string& note_id, const json& attachment_data) {
    // Placeholder for async attachment processing
    spdlog::debug("Process attachment for note {}", note_id);
}

void NoteService::track_note_view(const std::string& note_id, const std::string& user_id) {
    spdlog::debug("Tracking view for note {} by user {}", note_id, user_id);
}

void NoteService::invalidate_timeline_cache(const std::string& user_id) {
    spdlog::debug("Invalidating timeline cache for user {}", user_id);
}

} // namespace sonet::note
