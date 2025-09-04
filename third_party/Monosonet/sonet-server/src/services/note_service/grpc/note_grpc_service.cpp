/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "note_grpc_service.h"
#include <spdlog/spdlog.h>

namespace sonet::note::grpc {

NoteGrpcService::NoteGrpcService(
    std::shared_ptr<services::NoteService> note_service,
    std::shared_ptr<services::TimelineService> /*timeline_service*/,
    std::shared_ptr<services::AnalyticsService> /*analytics_service*/,
    std::shared_ptr<repositories::NoteRepository> /*note_repository*/,
    std::shared_ptr<core::cache::RedisClient> /*redis_client*/,
    std::shared_ptr<core::security::AuthService> /*auth_service*/,
    std::shared_ptr<core::logging::MetricsCollector> /*metrics_collector*/)
    : note_service_(std::move(note_service)) {
    spdlog::info("NoteGrpcService initialized");
}

::grpc::Status NoteGrpcService::CreateNote(
    ::grpc::ServerContext* context,
    const ::sonet::note::grpc::CreateNoteRequest* request,
    ::sonet::note::grpc::CreateNoteResponse* response) {
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Extract request data
        nlohmann::json note_data;
        note_data["content"] = request->content();
        note_data["visibility"] = request->visibility();
        
        // Process attachments
        if (request->attachments_size() > 0) {
            nlohmann::json attachments = nlohmann::json::array();
            for (const auto& attachment : request->attachments()) {
                nlohmann::json att_data;
                att_data["type"] = attachment.type();
                att_data["url"] = attachment.url();
                att_data["metadata"] = nlohmann::json::parse(attachment.metadata());
                attachments.push_back(att_data);
            }
            note_data["attachments"] = attachments;
        }
        
        // Create note through service
        auto created = note_service_->create_note(request->author_id(), note_data);
        
        // Measure performance
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        if (created) {
            response->set_success(true);
            response->set_note_id(created->note_id);
            response->set_created_at(created->created_at);
            response->set_processing_time_us(duration.count());
            
            if (duration.count() > 10000) { // > 10ms
                spdlog::warn("Slow note creation: {}μs for note {}", duration.count(), created->note_id);
            }
            return ::grpc::Status::OK;
        } else {
            response->set_success(false);
            response->set_error_code("VALIDATION_ERROR");
            response->set_error_message("Failed to create note");
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Failed to create note");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("gRPC CreateNote error: {}", e.what());
        response->set_success(false);
        response->set_error_code("INTERNAL_ERROR");
        response->set_error_message("Internal server error");
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Internal server error");
    }
}

::grpc::Status NoteGrpcService::GetNote(
    ::grpc::ServerContext* context,
    const ::sonet::note::grpc::GetNoteRequest* request,
    ::sonet::note::grpc::GetNoteResponse* response) {
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Get note through service
        auto found = note_service_->get_note(request->note_id(), request->requesting_user_id());
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        if (found) {
            response->set_success(true);
            response->set_note_id(found->note_id);
            response->set_author_id(found->author_id);
            response->set_content(found->content);
            response->set_created_at(found->created_at);
            response->set_like_count(found->like_count);
            response->set_renote_count(found->renote_count);
            response->set_reply_count(found->reply_count);
            response->set_processing_time_us(duration.count());
            
            if (duration.count() > 5000) {
                spdlog::warn("Slow note retrieval: {}μs for note {}", duration.count(), request->note_id());
            }
            
            return ::grpc::Status::OK;
        } else {
            response->set_success(false);
            response->set_error_code("NOTE_NOT_FOUND");
            response->set_error_message("Note not found");
            return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, "Note not found");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("gRPC GetNote error: {}", e.what());
        response->set_success(false);
        response->set_error_code("INTERNAL_ERROR");
        response->set_error_message("Internal server error");
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Internal server error");
    }
}

::grpc::Status NoteGrpcService::LikeNote(
    ::grpc::ServerContext* context,
    const ::sonet::note::grpc::LikeNoteRequest* request,
    ::sonet::note::grpc::LikeNoteResponse* response) {
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Like note through service
        auto result = note_service_->like_note(request->user_id(), request->note_id());
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        if (result.contains("success") && result["success"].get<bool>()) {
            response->set_success(true);
            response->set_new_like_count(result.value("new_like_count", 0));
            response->set_processing_time_us(duration.count());
            
            if (duration.count() > 3000) {
                spdlog::warn("Slow like operation: {}μs for note {}", duration.count(), request->note_id());
            }
            return ::grpc::Status::OK;
        } else {
            const std::string err_code = result.contains("error") && result["error"].contains("code") ? result["error"]["code"].get<std::string>() : "INVALID_ARGUMENT";
            const std::string err_msg = result.contains("error") && result["error"].contains("message") ? result["error"]["message"].get<std::string>() : "Invalid like operation";
            response->set_success(false);
            response->set_error_code(err_code);
            response->set_error_message(err_msg);
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, err_msg);
        }
        
    } catch (const std::exception& e) {
        spdlog::error("gRPC LikeNote error: {}", e.what());
        response->set_success(false);
        response->set_error_code("INTERNAL_ERROR");
        response->set_error_message("Internal server error");
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Internal server error");
    }
}

::grpc::Status NoteGrpcService::GetTimeline(
    ::grpc::ServerContext* context,
    const ::sonet::note::GetTimelineRequest* request,
    ::sonet::note::GetTimelineResponse* response) {
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Get timeline through service
        auto result = note_service_->get_timeline(request->user_id(), request->limit(), request->cursor());
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        if (result.contains("notes")) {
            response->set_success(true);
            
            // Add notes to response
            for (const auto& note : result["notes"]) {
                auto* note_proto = response->add_notes();
                note_proto->set_note_id(note["note_id"]);
                note_proto->set_author_id(note["author_id"]);
                note_proto->set_content(note["content"]);
                note_proto->set_created_at(note["created_at"]);
                note_proto->set_like_count(note["like_count"]);
                note_proto->set_renote_count(note["renote_count"]);
                note_proto->set_reply_count(note["reply_count"]);
            }
            
            if (result.contains("next_cursor")) {
                response->set_next_cursor(result["next_cursor"]);
            }
            
            response->set_processing_time_us(duration.count());
            
            // Target < 15ms for timeline
            if (duration.count() > 15000) {
                spdlog::warn("Slow timeline retrieval: {}μs for user {}", duration.count(), request->user_id());
            }
            
            return ::grpc::Status::OK;
        } else {
            response->set_success(false);
            response->set_error_code("TIMELINE_ERROR");
            response->set_error_message("Failed to retrieve timeline");
            return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Failed to retrieve timeline");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("gRPC GetTimeline error: {}", e.what());
        response->set_success(false);
        response->set_error_code("INTERNAL_ERROR");
        response->set_error_message("Internal server error");
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Internal server error");
    }
}

::grpc::Status NoteGrpcService::SearchNotes(
    ::grpc::ServerContext* context,
    const ::sonet::note::SearchNotesRequest* request,
    ::sonet::note::SearchNotesResponse* response) {
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Search notes through service
        auto result = note_service_->search_notes(request->query(), request->user_id(), request->limit());
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        if (result.contains("notes")) {
            response->set_success(true);
            response->set_total_results(result["total_results"]);
            
            // Add search results
            for (const auto& note : result["notes"]) {
                auto* note_proto = response->add_notes();
                note_proto->set_note_id(note["note_id"]);
                note_proto->set_author_id(note["author_id"]);
                note_proto->set_content(note["content"]);
                note_proto->set_created_at(note["created_at"]);
                note_proto->set_like_count(note["like_count"]);
                note_proto->set_renote_count(note["renote_count"]);
                note_proto->set_reply_count(note["reply_count"]);
                
                if (note.contains("relevance_score")) {
                    note_proto->set_relevance_score(note["relevance_score"]);
                }
            }
            
            response->set_processing_time_us(duration.count());
            
            // Target < 50ms for search
            if (duration.count() > 50000) {
                spdlog::warn("Slow search operation: {}μs for query '{}'", duration.count(), request->query());
            }
            
            return ::grpc::Status::OK;
        } else {
            response->set_success(false);
            response->set_error_code("SEARCH_ERROR");
            response->set_error_message("Search failed");
            return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Search failed");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("gRPC SearchNotes error: {}", e.what());
        response->set_success(false);
        response->set_error_code("INTERNAL_ERROR");
        response->set_error_message("Internal server error");
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Internal server error");
    }
}

// Streaming implementation for real-time timeline updates
::grpc::Status NoteGrpcService::StreamTimeline(
    ::grpc::ServerContext* context,
    const ::sonet::note::StreamTimelineRequest* request,
    ::grpc::ServerWriter<::sonet::note::TimelineUpdate>* writer) {
    
    try {
        spdlog::info("Starting timeline stream for user: {}", request->user_id());
        
        // This would integrate with the WebSocket handler for real-time updates
        // For now, implement a basic polling mechanism
        
        std::string user_id = request->user_id();
        std::string last_cursor = "";
        
        while (!context->IsCancelled()) {
            // Check for new timeline updates
            auto result = note_service_->get_timeline_updates(user_id, last_cursor);
            
            if (result.contains("updates") && !result["updates"].empty()) {
                for (const auto& update : result["updates"]) {
                    ::sonet::note::TimelineUpdate timeline_update;
                    timeline_update.set_type(update["type"]);
                    timeline_update.set_note_id(update["note_id"]);
                    timeline_update.set_timestamp(update["timestamp"]);
                    
                    if (update.contains("note")) {
                        auto note = update["note"];
                        auto* note_proto = timeline_update.mutable_note();
                        note_proto->set_note_id(note["note_id"]);
                        note_proto->set_author_id(note["author_id"]);
                        note_proto->set_content(note["content"]);
                        note_proto->set_created_at(note["created_at"]);
                        note_proto->set_like_count(note["like_count"]);
                        note_proto->set_renote_count(note["renote_count"]);
                        note_proto->set_reply_count(note["reply_count"]);
                    }
                    
                    if (!writer->Write(timeline_update)) {
                        // Client disconnected
                        break;
                    }
                }
                
                last_cursor = result["next_cursor"];
            }
            
            // Sleep for a short duration before checking again
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        spdlog::info("Timeline stream ended for user: {}", request->user_id());
        return ::grpc::Status::OK;
        
    } catch (const std::exception& e) {
        spdlog::error("gRPC StreamTimeline error: {}", e.what());
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Stream error");
    }
}

// Batch operations for high throughput
::grpc::Status NoteGrpcService::BatchCreateNotes(
    ::grpc::ServerContext* context,
    const ::sonet::note::grpc::BatchCreateNotesRequest* request,
    ::sonet::note::grpc::BatchCreateNotesResponse* response) {
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::vector<nlohmann::json> note_results;
        int successful_creates = 0;
        int failed_creates = 0;
        
        // Process each note in the batch
        for (const auto& note_request : request->notes()) {
            nlohmann::json note_data;
            note_data["content"] = note_request.content();
            note_data["visibility"] = note_request.visibility();
            
            auto created = note_service_->create_note(note_request.author_id(), note_data);
            
            auto* note_result = response->add_results();
            if (created) {
                note_result->set_success(true);
                note_result->set_note_id(created->note_id);
                successful_creates++;
            } else {
                note_result->set_success(false);
                note_result->set_error_code("VALIDATION_ERROR");
                note_result->set_error_message("Failed to create note");
                failed_creates++;
            }
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        response->set_total_processed(request->notes_size());
        response->set_successful_creates(successful_creates);
        response->set_failed_creates(failed_creates);
        response->set_processing_time_us(duration.count());
        
        spdlog::info("Batch create completed: {} successful, {} failed, {}μs", 
                    successful_creates, failed_creates, duration.count());
        
        return ::grpc::Status::OK;
        
    } catch (const std::exception& e) {
        spdlog::error("gRPC BatchCreateNotes error: {}", e.what());
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Batch operation failed");
    }
}

::grpc::Status NoteGrpcService::GetNoteAnalytics(
    ::grpc::ServerContext* context,
    const ::sonet::note::GetNoteAnalyticsRequest* request,
    ::sonet::note::GetNoteAnalyticsResponse* response) {
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Get analytics through service
        auto result = note_service_->get_note_analytics(request->note_id(), request->user_id());
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        if (result.contains("analytics")) {
            auto analytics = result["analytics"];
            
            response->set_success(true);
            response->set_note_id(request->note_id());
            response->set_view_count(analytics["view_count"]);
            response->set_like_count(analytics["like_count"]);
            response->set_renote_count(analytics["renote_count"]);
            response->set_reply_count(analytics["reply_count"]);
            response->set_engagement_rate(analytics["engagement_rate"]);
            response->set_reach(analytics["reach"]);
            response->set_processing_time_us(duration.count());
            
            return ::grpc::Status::OK;
        } else {
            response->set_success(false);
            response->set_error_code("ANALYTICS_NOT_FOUND");
            response->set_error_message("Analytics not available");
            return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, "Analytics not available");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("gRPC GetNoteAnalytics error: {}", e.what());
        response->set_success(false);
        response->set_error_code("INTERNAL_ERROR");
        response->set_error_message("Internal server error");
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "Internal server error");
    }
}

} // namespace sonet::note::grpc
