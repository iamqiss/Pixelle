#include "server.h"
#include "ranker_service.h"
#include <grpcpp/grpcpp.h>
#include <iostream>

namespace overdrive {

OverdriveServer::OverdriveServer(const ServerConfig& config) : config_(config) {}
OverdriveServer::~OverdriveServer() { Stop(); }

bool OverdriveServer::Start() {
	if (running_) return true;
	std::cout << "Overdrive gRPC server starting at " << config_.address << std::endl;
	
	// Create gRPC server
	grpc::ServerBuilder builder;
	builder.AddListeningPort(config_.address, grpc::InsecureServerCredentials());
	
	// Add OverdriveRanker service
	auto ranker_service = std::make_unique<OverdriveRankerServiceImpl>();
	builder.RegisterService(ranker_service.get());
	
	// Build and start server
	server_ = builder.BuildAndStart();
	if (!server_) {
		std::cerr << "Failed to start gRPC server" << std::endl;
		return false;
	}
	
	running_ = true;
	std::cout << "Overdrive gRPC server started successfully" << std::endl;
	return true;
}

void OverdriveServer::Stop() {
	if (!running_) return;
	std::cout << "Overdrive gRPC server stopping" << std::endl;
	if (server_) {
		server_->Shutdown();
	}
	running_ = false;
}

} // namespace overdrive