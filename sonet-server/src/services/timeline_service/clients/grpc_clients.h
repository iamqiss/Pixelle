#pragma once

#include <memory>
#include <string>
#include <vector>
#include <chrono>

// Stubs
#include "../../../proto/services/stub_protos.h"

namespace sonet {
namespace timeline {
namespace clients {

class NoteClient {
public:
    virtual ~NoteClient() = default;
    virtual std::vector<::sonet::note::Note> ListRecentNotesByAuthors(
        const std::vector<std::string>& author_ids,
        std::chrono::system_clock::time_point since,
        int32_t limit) = 0;
};

class FollowClient {
public:
    virtual ~FollowClient() = default;
    virtual std::vector<std::string> GetFollowing(const std::string& user_id) = 0;
    virtual std::vector<std::string> GetFollowers(const std::string& user_id) = 0;
};

class StubBackedNoteClient : public NoteClient {
public:
    explicit StubBackedNoteClient(std::shared_ptr<::sonet::note::NoteService::Stub> stub)
        : stub_(std::move(stub)) {}
    std::vector<::sonet::note::Note> ListRecentNotesByAuthors(
        const std::vector<std::string>& author_ids,
        std::chrono::system_clock::time_point since,
        int32_t limit) override {
        ::sonet::note::NoteService::Stub::ListRecentNotesByAuthorsRequest req;
        req.author_ids = author_ids;
        auto secs = std::chrono::duration_cast<std::chrono::seconds>(since.time_since_epoch()).count();
        req.since.set_seconds(secs);
        req.limit = limit;
        auto resp = stub_->ListRecentNotesByAuthors(req);
        return resp.notes;
    }
private:
    std::shared_ptr<::sonet::note::NoteService::Stub> stub_;
};

class StubBackedFollowClient : public FollowClient {
public:
    StubBackedFollowClient() : stub_(std::make_shared<::sonet::follow::FollowService::Stub>()) {}
    std::vector<std::string> GetFollowing(const std::string& user_id) override {
        ::sonet::follow::GetFollowingRequest req; req.user_id_ = user_id;
        return stub_->GetFollowing(req).user_ids();
    }
    std::vector<std::string> GetFollowers(const std::string& user_id) override {
        ::sonet::follow::GetFollowersRequest req; req.user_id_ = user_id;
        return stub_->GetFollowers(req).user_ids();
    }
private:
    std::shared_ptr<::sonet::follow::FollowService::Stub> stub_;
};

#ifdef SONET_USE_GRPC_CLIENTS
// Placeholders for real gRPC clients
class GrpcNoteClient : public NoteClient {
public:
    explicit GrpcNoteClient(const std::string& endpoint) { (void)endpoint; }
    std::vector<::sonet::note::Note> ListRecentNotesByAuthors(
        const std::vector<std::string>& /*author_ids*/, std::chrono::system_clock::time_point /*since*/, int32_t /*limit*/) override {
        return {};
    }
};

class GrpcFollowClient : public FollowClient {
public:
    explicit GrpcFollowClient(const std::string& endpoint) { (void)endpoint; }
    std::vector<std::string> GetFollowing(const std::string& /*user_id*/) override { return {}; }
    std::vector<std::string> GetFollowers(const std::string& /*user_id*/) override { return {}; }
};
#endif

} // namespace clients
} // namespace timeline
} // namespace sonet