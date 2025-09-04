#include "messaging_service.hpp"
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <spdlog/spdlog.h>
#include <memory>
#include <stdexcept>

namespace sonet::messaging {

class MessagingGRPCServer {
public:
    MessagingGRPCServer(const std::string& address, std::shared_ptr<MessagingService> service)
        : address_(address), service_(service) {}
    
    void Start() {
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
        
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
        
        // Register the service
        builder.RegisterService(service_.get());
        
        // Build and start the server
        server_ = builder.BuildAndStart();
        if (!server_) {
            throw std::runtime_error("Failed to start gRPC server");
        }
        
        spdlog::info("Messaging gRPC server listening on: {}", address_);
    }
    
    void Stop() {
        if (server_) {
            server_->Shutdown();
            spdlog::info("Messaging gRPC server stopped");
        }
    }
    
    void Wait() {
        if (server_) {
            server_->Wait();
        }
    }

private:
    std::string address_;
    std::shared_ptr<MessagingService> service_;
    std::unique_ptr<grpc::Server> server_;
};

} // namespace sonet::messaging

// Main function for standalone gRPC server
int main(int argc, char* argv[]) {
    try {
        std::string address = "0.0.0.0:50051";
        
        if (argc > 1) {
            address = argv[1];
        }
        
        spdlog::info("Starting Sonet Messaging gRPC Server");
        spdlog::info("Address: {}", address);
        
        // Create messaging service
        auto service = std::make_shared<sonet::messaging::MessagingService>();
        
        // Create and start gRPC server
        sonet::messaging::MessagingGRPCServer server(address, service);
        server.Start();
        
        // Wait for shutdown signal
        server.Wait();
        
        return 0;
    } catch (const std::exception& e) {
        spdlog::error("Server failed: {}", e.what());
        return 1;
    }
}