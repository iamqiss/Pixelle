/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "create_note_handler.h"
#include "../websocket/note_websocket_handler.h"
#include "../service.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <algorithm>
#include "../clients/moderation_client.h"

namespace sonet::note::handlers {

CreateNoteHandler::CreateNoteHandler(
    std::shared_ptr<NoteRepository> note_repo,
    std::shared_ptr<NoteValidator> validator
) : note_repository_(note_repo), validator_(validator) {
    spdlog::info("CreateNoteHandler initialized");
}

json CreateNoteHandler::handle_create_note(const json& request_data, const std::string& user_id) {
    try {
        // Validate request
        auto validation_result = validator_->validate_note(request_data);
        if (!validation_result.is_valid) {
            return create_error_response(validation_result.error_code, validation_result.error_message);
        }
        
        // Check rate limits
        auto rate_limit_result = validator_->validate_rate_limits(user_id, "create_note");
        if (!rate_limit_result.is_valid) {
            return create_error_response("RATE_LIMIT_EXCEEDED", "Too many notes created recently");
        }
        
        // Create note from request
        Note note = create_note_from_request(request_data, user_id);
        
        // Process content features
        note.process_content();
        note.extract_mentions();
        note.extract_hashtags();
        note.extract_urls();
        note.detect_language();
        
        // Moderation check via gRPC
        {
            static const char* MOD_ADDR = std::getenv("MODERATION_GRPC_ADDR");
            const std::string target = MOD_ADDR ? std::string(MOD_ADDR) : std::string("127.0.0.1:9090");
            sonet::note::clients::ModerationClient mod_client(target);
            std::string label; float confidence = 0.0f;
            bool ok = mod_client.Classify(note.note_id, user_id, note.content, &label, &confidence, 150);
            if (!ok) {
                spdlog::warn("Moderation service timeout/failure for note {}", note.note_id);
            } else {
                if (label == "Spam" || label == "HateSpeech" || label == "Csam") {
                    return create_error_response("CONTENT_BLOCKED", "Content failed moderation");
                }
            }
        }
        
        // Process attachments if present
        if (request_data.contains("attachments")) {
            process_attachments(note, request_data["attachments"]);
        }
        
        // Calculate quality scores
        note.calculate_spam_score();
        note.calculate_toxicity_score();
        
        // Set timestamps
        auto now = std::chrono::system_clock::now();
        note.created_at = std::chrono::system_clock::to_time_t(now);
        note.updated_at = note.created_at;
        
        // Final validation
        if (!note.is_valid()) {
            auto errors = note.get_validation_errors();
            return create_error_response("VALIDATION_FAILED", "Note validation failed");
        }
        
        // Save to database
        auto saved_note = note_repository_->create(note);
        if (!saved_note) {
            return create_error_response("DATABASE_ERROR", "Failed to save note");
        }
        
        // Broadcast to real-time subscribers
        broadcast_note_created(*saved_note);
        
        // Track analytics
        track_creation_analytics(*saved_note);
        
        spdlog::info("Note created successfully: {}", saved_note->note_id);
        return create_success_response(*saved_note);
        
    } catch (const std::exception& e) {
        spdlog::error("Error creating note: {}", e.what());
        return create_error_response("INTERNAL_ERROR", "Internal server error");
    }
}

json CreateNoteHandler::handle_create_reply(const json& request_data, const std::string& user_id, const std::string& reply_to_id) {
    try {
        // Validate reply target exists
        auto reply_validation = validator_->validate_reply_target(reply_to_id);
        if (!reply_validation.is_valid) {
            return create_error_response("INVALID_REPLY_TARGET", "Cannot reply to this note");
        }
        
        // Get original note for context
        auto original_note = note_repository_->get_by_id(reply_to_id);
        if (!original_note) {
            return create_error_response("NOTE_NOT_FOUND", "Original note not found");
        }
        
        // Create modified request with reply information
        json reply_request = request_data;
        reply_request["reply_to_id"] = reply_to_id;
        reply_request["reply_to_user_id"] = original_note->author_id;
        reply_request["type"] = static_cast<int>(NoteType::REPLY);
        
        // Process as regular note creation
        auto result = handle_create_note(reply_request, user_id);
        
        // If successful, increment reply count on original
        if (result.contains("note")) {
            note_repository_->increment_reply_count(reply_to_id);
        }
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Error creating reply: {}", e.what());
        return create_error_response("INTERNAL_ERROR", "Internal server error");
    }
}

json CreateNoteHandler::handle_create_renote(const json& request_data, const std::string& user_id, const std::string& renote_of_id) {
    try {
        // Validate renote target
        auto renote_validation = validator_->validate_renote_target(renote_of_id);
        if (!renote_validation.is_valid) {
            return create_error_response("INVALID_RENOTE_TARGET", "Cannot renote this note");
        }
        
        // Get original note
        auto original_note = note_repository_->get_by_id(renote_of_id);
        if (!original_note) {
            return create_error_response("NOTE_NOT_FOUND", "Original note not found");
        }
        
        // Check if user already renoted
        if (note_repository_->has_user_renoted(user_id, renote_of_id)) {
            return create_error_response("ALREADY_RENOTED", "User has already renoted this note");
        }
        
        // Create renote request
        json renote_request;
        renote_request["content"] = request_data.value("content", ""); // Optional comment
        renote_request["renote_of_id"] = renote_of_id;
        renote_request["type"] = static_cast<int>(NoteType::RENOST);
        renote_request["visibility"] = request_data.value("visibility", "public");
        
        // Process as regular note creation
        auto result = handle_create_note(renote_request, user_id);
        
        // If successful, increment renote count on original
        if (result.contains("note")) {
            note_repository_->increment_renote_count(renote_of_id);
        }
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Error creating renote: {}", e.what());
        return create_error_response("INTERNAL_ERROR", "Internal server error");
    }
}

json CreateNoteHandler::handle_create_quote(const json& request_data, const std::string& user_id, const std::string& quote_of_id) {
    try {
        // Validate quote target
        auto quote_validation = validator_->validate_quote_target(quote_of_id);
        if (!quote_validation.is_valid) {
            return create_error_response("INVALID_QUOTE_TARGET", "Cannot quote this note");
        }
        
        // Ensure quote has content
        if (!request_data.contains("content") || request_data["content"].get<std::string>().empty()) {
            return create_error_response("EMPTY_QUOTE_CONTENT", "Quote must have content");
        }
        
        // Create quote request
        json quote_request = request_data;
        quote_request["quote_of_id"] = quote_of_id;
        quote_request["type"] = static_cast<int>(NoteType::QUOTE);
        
        // Process as regular note creation
        auto result = handle_create_note(quote_request, user_id);
        
        // If successful, increment quote count on original
        if (result.contains("note")) {
            note_repository_->increment_quote_count(quote_of_id);
        }
        
        return result;
        
    } catch (const std::exception& e) {
        spdlog::error("Error creating quote: {}", e.what());
        return create_error_response("INTERNAL_ERROR", "Internal server error");
    }
}

json CreateNoteHandler::handle_create_thread_note(const json& request_data, const std::string& user_id, const std::string& thread_id, int position) {
    try {
        // Validate thread context
        auto thread_validation = validator_->validate_thread_info(thread_id, position);
        if (!thread_validation.is_valid) {
            return create_error_response("INVALID_THREAD_INFO", "Invalid thread information");
        }
        
        // Create thread note request
        json thread_request = request_data;
        thread_request["thread_id"] = thread_id;
        thread_request["thread_position"] = position;
        thread_request["type"] = static_cast<int>(NoteType::THREAD);
        
        return handle_create_note(thread_request, user_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Error creating thread note: {}", e.what());
        return create_error_response("INTERNAL_ERROR", "Internal server error");
    }
}

json CreateNoteHandler::handle_schedule_note(const json& request_data, const std::string& user_id, std::time_t scheduled_at) {
    try {
        // Validate scheduled time
        auto schedule_validation = validator_->validate_scheduled_time(scheduled_at);
        if (!schedule_validation.is_valid) {
            return create_error_response("INVALID_SCHEDULE_TIME", "Invalid scheduled time");
        }
        
        // Create scheduled note request
        json scheduled_request = request_data;
        scheduled_request["scheduled_at"] = scheduled_at;
        scheduled_request["status"] = static_cast<int>(NoteStatus::SCHEDULED);
        
        return handle_create_note(scheduled_request, user_id);
        
    } catch (const std::exception& e) {
        spdlog::error("Error scheduling note: {}", e.what());
        return create_error_response("INTERNAL_ERROR", "Internal server error");
    }
}

// Private helper methods

Note CreateNoteHandler::create_note_from_request(const json& request_data, const std::string& user_id) const {
    Note note;
    
    // Generate unique ID
    note.note_id = "note_" + std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
    
    // Basic information
    note.author_id = user_id;
    note.content = request_data.value("content", "");
    note.raw_content = note.content;
    
    // Note type and visibility
    if (request_data.contains("type")) {
        note.type = static_cast<NoteType>(request_data["type"].get<int>());
    }
    
    if (request_data.contains("visibility")) {
        std::string visibility = request_data["visibility"];
        if (visibility == "public") note.visibility = NoteVisibility::PUBLIC;
        else if (visibility == "followers") note.visibility = NoteVisibility::FOLLOWERS_ONLY;
        else if (visibility == "private") note.visibility = NoteVisibility::PRIVATE;
        else if (visibility == "mentioned") note.visibility = NoteVisibility::MENTIONED_ONLY;
        else if (visibility == "circle") note.visibility = NoteVisibility::CIRCLE;
    }
    
    // Relationships
    if (request_data.contains("reply_to_id")) {
        note.reply_to_id = request_data["reply_to_id"];
    }
    if (request_data.contains("reply_to_user_id")) {
        note.reply_to_user_id = request_data["reply_to_user_id"];
    }
    if (request_data.contains("renote_of_id")) {
        note.renote_of_id = request_data["renote_of_id"];
    }
    if (request_data.contains("quote_of_id")) {
        note.quote_of_id = request_data["quote_of_id"];
    }
    if (request_data.contains("thread_id")) {
        note.thread_id = request_data["thread_id"];
        note.thread_position = request_data.value("thread_position", 0);
    }
    
    // Content warnings
    if (request_data.contains("content_warning")) {
        std::string warning = request_data["content_warning"];
        if (warning == "sensitive") note.content_warning = ContentWarning::SENSITIVE;
        else if (warning == "violence") note.content_warning = ContentWarning::VIOLENCE;
        else if (warning == "adult") note.content_warning = ContentWarning::ADULT;
        else if (warning == "spoiler") note.content_warning = ContentWarning::SPOILER;
    }
    
    // Geographic data
    if (request_data.contains("location")) {
        auto location = request_data["location"];
        if (location.contains("latitude")) note.latitude = location["latitude"];
        if (location.contains("longitude")) note.longitude = location["longitude"];
        if (location.contains("name")) note.location_name = location["name"];
    }
    
    // Scheduled time
    if (request_data.contains("scheduled_at")) {
        note.scheduled_at = request_data["scheduled_at"];
        note.status = NoteStatus::SCHEDULED;
    }
    
    // Settings
    note.allow_replies = request_data.value("allow_replies", true);
    note.allow_renotes = request_data.value("allow_renotes", true);
    note.allow_quotes = request_data.value("allow_quotes", true);
    
    return note;
}

void CreateNoteHandler::process_attachments(Note& note, const json& attachments) const {
    if (!attachments.is_array()) return;
    
    for (const auto& attachment_data : attachments) {
        if (!attachment_data.contains("type") || !attachment_data.contains("url")) {
            continue;
        }
        
        std::string attachment_id = "att_" + std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
        
        note.attachment_ids.push_back(attachment_id);
        
        // Process based on attachment type
        std::string type = attachment_data["type"];
        if (type == "image" || type == "video" || type == "gif") {
            // Handle media attachment
            note.attachments.add_media_attachment(attachment_data);
        } else if (type == "poll") {
            // Handle poll attachment
            note.attachments.add_poll_attachment(attachment_data);
        } else if (type == "location") {
            // Handle location attachment
            note.attachments.add_location_attachment(attachment_data);
        }
    }
}

void CreateNoteHandler::broadcast_note_created(const Note& note) const {
    // Broadcast to WebSocket subscribers
    try {
        json broadcast_data = {
            {"type", "note_created"},
            {"note", note.to_json()},
            {"timestamp", std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()}
        };
        
        // This would integrate with WebSocket handler
        // websocket_handler_->broadcast_to_followers(note.author_id, broadcast_data);
        // websocket_handler_->broadcast_to_timeline_subscribers(broadcast_data);
        
    } catch (const std::exception& e) {
        spdlog::warn("Failed to broadcast note creation: {}", e.what());
    }
}

void CreateNoteHandler::track_creation_analytics(const Note& note) const {
    try {
        json analytics_data = {
            {"event", "note_created"},
            {"note_id", note.note_id},
            {"author_id", note.author_id},
            {"note_type", static_cast<int>(note.type)},
            {"content_length", note.content.length()},
            {"attachment_count", note.attachment_ids.size()},
            {"has_mentions", !note.mentioned_user_ids.empty()},
            {"has_hashtags", !note.hashtags.empty()},
            {"has_urls", !note.urls.empty()},
            {"spam_score", note.spam_score},
            {"toxicity_score", note.toxicity_score},
            {"timestamp", note.created_at}
        };
        
        // This would integrate with analytics service
        // analytics_service_->track_event(analytics_data);
        
    } catch (const std::exception& e) {
        spdlog::warn("Failed to track creation analytics: {}", e.what());
    }
}

json CreateNoteHandler::create_success_response(const Note& note) const {
    return json{
        {"success", true},
        {"note", note.to_json()},
        {"message", "Note created successfully"}
    };
}

json CreateNoteHandler::create_error_response(const std::string& error_code, const std::string& message) const {
    return json{
        {"success", false},
        {"error", {
            {"code", error_code},
            {"message", message}
        }}
    };
}

} // namespace sonet::note::handlers
