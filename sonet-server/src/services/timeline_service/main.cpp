//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "implementations.h"
#include "../../../proto/grpc_stub.h"
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <csignal>
#include "../../core/logging/logger.h"

namespace {
    std::unique_ptr<grpc::Server> server;
    std::shared_ptr<sonet::timeline::TimelineServiceImpl> timeline_service;

    void SignalHandler(int signal) {
        std::cout << "\nReceived signal " << signal << ", shutting down gracefully..." << std::endl;
        if (server) {
            server->Shutdown();
        }
    }
}

int main(int argc, char** argv) {
    // Initialize JSON logger for ELK ingestion
    (void)sonet::logging::init_json_stdout_logger();
    spdlog::info(R"({"event":"startup","message":"Starting Sonet Timeline Service"})");
    // Set up signal handlers
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    // std::cout banners replaced with structured logs for ELK
    spdlog::info(R"({"event":"banner","service":"timeline","message":"Sonet Timeline Service starting"})");
    spdlog::info(R"({"event":"info","service":"timeline","message":"Starting advanced timeline service with Twitter-scale engineering"})");

    // Parse command line arguments
    std::string server_address = "0.0.0.0:50051";
    std::string redis_host = "localhost";
    int redis_port = 6379;
    int websocket_port = 8081;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--address" && i + 1 < argc) {
            server_address = argv[++i];
        } else if (arg == "--redis-host" && i + 1 < argc) {
            redis_host = argv[++i];
        } else if (arg == "--redis-port" && i + 1 < argc) {
            redis_port = std::stoi(argv[++i]);
        } else if (arg == "--websocket-port" && i + 1 < argc) {
            websocket_port = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --address HOST:PORT       gRPC server address (default: 0.0.0.0:50051)" << std::endl;
            std::cout << "  --redis-host HOST         Redis host (default: localhost)" << std::endl;
            std::cout << "  --redis-port PORT         Redis port (default: 6379)" << std::endl;
            std::cout << "  --websocket-port PORT     WebSocket port (default: 8081)" << std::endl;
            std::cout << "  --help                    Show this help message" << std::endl;
            return 0;
        }
    }

    std::cout << "Configuration:" << std::endl;
    std::cout << "  gRPC Address: " << server_address << std::endl;
    std::cout << "  Redis: " << redis_host << ":" << redis_port << std::endl;
    std::cout << "  WebSocket Port: " << websocket_port << std::endl;

    try {
        // Create timeline service with all components
        timeline_service = sonet::timeline::CreateTimelineService(
            redis_host, redis_port, websocket_port, nullptr);

        // Build gRPC server
        grpc::ServerBuilder builder;
        
        // Listen on the specified address
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        
        // Register timeline service
        builder.RegisterService(timeline_service.get());
        
        // Set server options
        builder.SetMaxReceiveMessageSize(4 * 1024 * 1024);  // 4MB
        builder.SetMaxSendMessageSize(4 * 1024 * 1024);     // 4MB
        
        // Build and start server
        server = builder.BuildAndStart();
        if (!server) {
            std::cerr << "Failed to start gRPC server" << std::endl;
            return 1;
        }

        std::cout << "Timeline service listening on " << server_address << std::endl;
        std::cout << "Features enabled:" << std::endl;
        std::cout << "  ✓ ML-based content ranking" << std::endl;
        std::cout << "  ✓ Redis-based caching (fallback mode)" << std::endl;
        std::cout << "  ✓ Advanced content filtering" << std::endl;
        std::cout << "  ✓ Real-time WebSocket notifications" << std::endl;
        std::cout << "  ✓ Multiple content sources (Following, Recommended, Trending)" << std::endl;
        std::cout << "  ✓ Hybrid timeline algorithms" << std::endl;
        std::cout << std::endl;

        // Create a test scenario
        std::cout << "=== Running Test Scenario ===" << std::endl;
        
        // Simulate some user engagement for ML training
        std::vector<sonet::timeline::EngagementEvent> sample_events = {
            {"user123", "alice_dev", "note_1", "like", 1.0, std::chrono::system_clock::now()},
            {"user123", "bob_designer", "note_2", "renote", 2.5, std::chrono::system_clock::now()},
            {"user123", "alice_dev", "note_3", "reply", 10.0, std::chrono::system_clock::now()},
            {"user456", "charlie_pm", "note_4", "like", 0.5, std::chrono::system_clock::now()},
            {"user456", "diana_data", "note_5", "follow", 0.0, std::chrono::system_clock::now()}
        };

        // Train the ranking engine
        auto ranking_engine = std::dynamic_pointer_cast<sonet::timeline::MLRankingEngine>(
            timeline_service->ranking_engine_);
        if (ranking_engine) {
            ranking_engine->TrainOnEngagementData(sample_events);
        }

        std::cout << "Test scenario complete. Service ready for requests." << std::endl;
        std::cout << "Press Ctrl+C to stop the server." << std::endl;

        // Wait for server to be shut down
        server->Wait();

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Timeline service stopped." << std::endl;
    return 0;
}
