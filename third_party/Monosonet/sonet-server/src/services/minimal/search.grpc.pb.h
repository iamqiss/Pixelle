#pragma once

#include "../../../proto/grpc_stub.h"
#include "../../../proto/services/stub_protos.h"

namespace sonet {
namespace search {
    struct SearchUserRequest {
        std::string query_;
        int32_t limit_;
        std::string query() const { return query_; }
        int32_t limit() const { return limit_; }
        void set_query(const std::string& q) { query_ = q; }
        void set_limit(int32_t l) { limit_ = l; }
    };
    
    struct SearchUserResponse {
        std::vector<std::string> user_ids_;
        bool success_;
        const std::vector<std::string>& user_ids() const { return user_ids_; }
        bool success() const { return success_; }
        void add_user_ids(const std::string& id) { user_ids_.push_back(id); }
        void set_success(bool s) { success_ = s; }
    };
    
    struct SearchNoteRequest {
        std::string query_;
        int32_t limit_;
        std::string query() const { return query_; }
        int32_t limit() const { return limit_; }
        void set_query(const std::string& q) { query_ = q; }
        void set_limit(int32_t l) { limit_ = l; }
    };
    
    struct SearchNoteResponse {
        std::vector<std::string> note_ids_;
        bool success_;
        const std::vector<std::string>& note_ids() const { return note_ids_; }
        bool success() const { return success_; }
        void add_note_ids(const std::string& id) { note_ids_.push_back(id); }
        void set_success(bool s) { success_ = s; }
    };
    
    class SearchService {
    public:
        class Service {
        public:
            virtual ~Service() = default;
            virtual ::grpc::Status SearchUsers(::grpc::ServerContext* context,
                                             const SearchUserRequest* request,
                                             SearchUserResponse* response) = 0;
            virtual ::grpc::Status SearchNotes(::grpc::ServerContext* context,
                                              const SearchNoteRequest* request,
                                              SearchNoteResponse* response) = 0;
        };
    };
}
}