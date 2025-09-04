#include "../../../proto/grpc_stub.h"
#include "notification.grpc.pb.h"

class MinimalNotificationService final : public sonet::notification::NotificationService::Service {
public:
  grpc::Status SendNotification(grpc::ServerContext*, const sonet::notification::SendNotificationRequest* req, sonet::notification::SendNotificationResponse* resp) override {
    (void)req; resp->set_success(true); return grpc::Status::OK;
  }
  grpc::Status ListNotifications(grpc::ServerContext*, const sonet::notification::ListNotificationsRequest* req, sonet::notification::ListNotificationsResponse* resp) override {
    (void)req; (void)resp; return grpc::Status::OK;
  }
  grpc::Status MarkNotificationRead(grpc::ServerContext*, const sonet::notification::MarkNotificationReadRequest* req, sonet::notification::MarkNotificationReadResponse* resp) override {
    (void)req; resp->set_success(true); return grpc::Status::OK;
  }
};

int main(int argc, char** argv) {
  (void)argc; (void)argv;
  std::string addr{"0.0.0.0:9097"};
  MinimalNotificationService service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  server->Wait();
  return 0;
}