/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#ifndef SONET_NOTE_GRPC_SERVICE_H
#define SONET_NOTE_GRPC_SERVICE_H

#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>

#include "note_service.grpc.pb.h"
#include "note_service.pb.h"

#include "../models/note.h"
#include "../models/attachment.h"
#include "../service.h"
#include "../../core/cache/redis_client.h"
#include "../../core/security/auth_service.h"
#include "../../core/logging/metrics_collector.h"

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <mutex>

namespace sonet::note::grpc {

using ::grpc::Status;
using ::grpc::StatusCode;
using ::grpc::ServerContext;
using ::grpc::ServerWriter;

class NoteGrpcService final : public sonet::note::grpc::NoteService::Service {
public:
    explicit NoteGrpcService(
        std::shared_ptr<sonet::note::NoteService> note_service,
        std::shared_ptr<sonet::note::services::TimelineService> timeline_service,
        std::shared_ptr<sonet::note::services::AnalyticsService> analytics_service,
        std::shared_ptr<sonet::note::repositories::NoteRepository> note_repository,
        std::shared_ptr<sonet::core::cache::RedisClient> redis_client,
        std::shared_ptr<sonet::core::security::AuthService> auth_service,
        std::shared_ptr<sonet::core::logging::MetricsCollector> metrics_collector
    );

    virtual ~NoteGrpcService() = default;

    // Core Note Operations
    Status CreateNote(ServerContext* context, const CreateNoteRequest* request, CreateNoteResponse* response) override;
    Status GetNote(ServerContext* context, const GetNoteRequest* request, GetNoteResponse* response) override;
    Status UpdateNote(ServerContext* context, const UpdateNoteRequest* request, UpdateNoteResponse* response) override;
    Status DeleteNote(ServerContext* context, const DeleteNoteRequest* request, DeleteNoteResponse* response) override;
    Status LikeNote(ServerContext* context, const LikeNoteRequest* request, LikeNoteResponse* response) override;

private:
    std::shared_ptr<sonet::note::NoteService> note_service_;
};

} // namespace sonet::note::grpc

#endif // SONET_NOTE_GRPC_SERVICE_H
