#include "../../../proto/grpc_stub.h"
#include "search.grpc.pb.h"

class MinimalSearchService final : public sonet::search::SearchService::Service {
public:
  grpc::Status SearchUsers(grpc::ServerContext*, const sonet::search::SearchUserRequest* req, sonet::search::SearchUserResponse* resp) override {
    (void)req; (void)resp; return grpc::Status::OK;
  }
  grpc::Status SearchNotes(grpc::ServerContext*, const sonet::search::SearchNoteRequest* req, sonet::search::SearchNoteResponse* resp) override {
    (void)req; (void)resp; return grpc::Status::OK;
  }
};

int main(int argc, char** argv) {
  (void)argc; (void)argv;
  std::string addr{"0.0.0.0:9096"};
  MinimalSearchService service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  server->Wait();
  return 0;
}