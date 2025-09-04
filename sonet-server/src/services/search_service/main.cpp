/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Main entry point for the Sonet Search Service.
 * This launches a Twitter-scale search service capable of handling
 * millions of search requests per second with real-time indexing.
 */

#include "service.h"
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <csignal>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <future>
#include <getopt.h>
#include "../../core/logging/logger.h"
#ifdef HAVE_GRPC
#include <grpcpp/grpcpp.h>
#include "search.grpc.pb.h"
extern class SearchGrpcService; // defined in service.cpp include chain
#endif

using namespace sonet::search_service;

// Global service instance for signal handling
std::unique_ptr<SearchService> g_service;
std::atomic<bool> g_shutdown_requested{false};

/**
 * Signal handler for graceful shutdown
 */
void signal_handler(int signal) {
    std::cout << "\n🛑 Received signal " << signal << ", initiating graceful shutdown..." << std::endl;
    g_shutdown_requested = true;
    
    if (g_service) {
        g_service->stop();
    }
}

/**
 * Setup signal handlers
 */
void setup_signal_handlers() {
    std::signal(SIGINT, signal_handler);   // Ctrl+C
    std::signal(SIGTERM, signal_handler);  // Termination request
    std::signal(SIGQUIT, signal_handler);  // Quit signal
    
    // Ignore SIGPIPE (broken pipe)
    std::signal(SIGPIPE, SIG_IGN);
}

/**
 * Print banner with service information
 */
void print_banner() {
    std::cout << R"(
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║   ███████╗ ██████╗ ███╗   ██╗███████╗████████╗                ║
║   ██╔════╝██╔═══██╗████╗  ██║██╔════╝╚══██╔══╝                ║
║   ███████╗██║   ██║██╔██╗ ██║█████╗     ██║                   ║
║   ╚════██║██║   ██║██║╚██╗██║██╔══╝     ██║                   ║
║   ███████║╚██████╔╝██║ ╚████║███████╗   ██║                   ║
║   ╚══════╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝   ╚═╝                   ║
║                                                               ║
║                    SEARCH SERVICE                             ║
║                                                               ║
║   🔍 Twitter-Scale Search Engine                              ║
║   ⚡ Real-time Indexing & Trending                            ║
║   🌍 Distributed & Fault-Tolerant                            ║
║   📊 Advanced Analytics & Personalization                    ║
║                                                               ║
║   Copyright (c) 2025 Neo Qiss                                ║
║   Built for Real Connections                                  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
)" << std::endl;
}

/**
 * Print usage information
 */
void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [OPTIONS]\n\n";
    std::cout << "Sonet Search Service - Twitter-scale search engine\n\n";
    std::cout << "Options:\n";
    std::cout << "  -h, --help              Show this help message\n";
    std::cout << "  -v, --version           Show version information\n";
    std::cout << "  -c, --config FILE       Load configuration from file\n";
    std::cout << "  -e, --environment ENV   Set environment (production, staging, development, testing)\n";
    std::cout << "  -p, --port PORT         Override HTTP port\n";
    std::cout << "  -g, --grpc-port PORT    Override gRPC port\n";
    std::cout << "  -d, --debug             Enable debug mode\n";
    std::cout << "  -t, --test              Run in test mode\n";
    std::cout << "  --dry-run              Validate configuration and exit\n";
    std::cout << "  --check-health         Check service health and exit\n";
    std::cout << "  --elasticsearch-url URL Override Elasticsearch URL\n";
    std::cout << "  --log-level LEVEL      Set log level (DEBUG, INFO, WARN, ERROR, FATAL)\n";
    std::cout << "  --no-indexing          Disable real-time indexing\n";
    std::cout << "  --no-trending          Disable trending analysis\n";
    std::cout << "  --no-cache             Disable caching\n";
    std::cout << "\nExamples:\n";
    std::cout << "  " << program_name << "                           # Start with default production config\n";
    std::cout << "  " << program_name << " -e development -d         # Start in development mode with debug\n";
    std::cout << "  " << program_name << " -c config.json           # Start with custom configuration\n";
    std::cout << "  " << program_name << " --dry-run -c config.json # Validate configuration only\n";
    std::cout << "  " << program_name << " --check-health           # Check service health\n";
    std::cout << std::endl;
}

/**
 * Print version information
 */
void print_version() {
    std::cout << "Sonet Search Service v1.0.0\n";
    std::cout << "Built with:\n";
    std::cout << "  - Elasticsearch C++ Client\n";
    std::cout << "  - MongoDB C++ Driver\n";
    std::cout << "  - Redis C++ Client\n";
    std::cout << "  - gRPC C++\n";
    std::cout << "  - nlohmann/json\n";
    std::cout << "  - libcurl\n";
    std::cout << "\nCopyright (c) 2025 Neo Qiss\n";
    std::cout << "Licensed under MIT License\n";
}

/**
 * Validate configuration and print results
 */
bool validate_configuration(const SearchServiceConfig& config) {
    std::cout << "🔧 Validating configuration..." << std::endl;
    
    auto validation_errors = service_utils::validate_config(config);
    
    if (validation_errors.empty()) {
        std::cout << "✅ Configuration is valid!" << std::endl;
        
        // Print key configuration details
        std::cout << "\n📋 Configuration Summary:" << std::endl;
        std::cout << "  Service Name: " << config.service_name << std::endl;
        std::cout << "  Environment: " << config.environment << std::endl;
        std::cout << "  HTTP Port: " << config.controller_config.http_port << std::endl;
        std::cout << "  gRPC Port: " << config.controller_config.grpc_port << std::endl;
        std::cout << "  Elasticsearch: " << config.elasticsearch_config.hosts[0] << std::endl;
        std::cout << "  Real-time Indexing: " << (config.enable_real_time_indexing ? "Enabled" : "Disabled") << std::endl;
        std::cout << "  Trending Analysis: " << (config.enable_trending_analysis ? "Enabled" : "Disabled") << std::endl;
        std::cout << "  Caching: " << (config.enable_caching ? "Enabled" : "Disabled") << std::endl;
        
        return true;
    } else {
        std::cout << "❌ Configuration validation failed:" << std::endl;
        for (const auto& error : validation_errors) {
            std::cout << "  - " << error << std::endl;
        }
        return false;
    }
}

/**
 * Check service health
 */
int check_service_health(const SearchServiceConfig& config) {
    std::cout << "🏥 Checking service health..." << std::endl;
    
    try {
        auto service = SearchServiceFactory::create_with_config(config);
        
        // Initialize service
        auto init_future = service->initialize();
        auto init_result = init_future.get();
        
        if (!init_result) {
            std::cout << "❌ Service initialization failed" << std::endl;
            return 1;
        }
        
        // Perform health check
        auto health_future = service->health_check();
        auto health_result = health_future.get();
        
        std::cout << "\n📊 Health Check Results:" << std::endl;
        std::cout << health_result.to_json().dump(2) << std::endl;
        
        if (health_result.is_healthy) {
            std::cout << "✅ Service is healthy!" << std::endl;
            return 0;
        } else {
            std::cout << "❌ Service is unhealthy: " << health_result.status_message << std::endl;
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cout << "❌ Health check failed: " << e.what() << std::endl;
        return 1;
    }
}

/**
 * Run integration tests
 */
int run_integration_tests(const SearchServiceConfig& config) {
    std::cout << "🧪 Running integration tests..." << std::endl;
    
    try {
        auto service = SearchServiceFactory::create_with_config(config);
        
        // Initialize and start service
        auto init_future = service->initialize();
        if (!init_future.get()) {
            std::cout << "❌ Service initialization failed" << std::endl;
            return 1;
        }
        
        auto start_future = service->start();
        if (!start_future.get()) {
            std::cout << "❌ Service startup failed" << std::endl;
            return 1;
        }
        
        // Run tests
        auto test_future = service->run_integration_tests();
        auto test_results = test_future.get();
        
        std::cout << "\n📊 Test Results:" << std::endl;
        std::cout << test_results.dump(2) << std::endl;
        
        service->stop();
        
        bool all_passed = test_results.value("all_passed", false);
        if (all_passed) {
            std::cout << "✅ All integration tests passed!" << std::endl;
            return 0;
        } else {
            std::cout << "❌ Some integration tests failed" << std::endl;
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cout << "❌ Integration tests failed: " << e.what() << std::endl;
        return 1;
    }
}

/**
 * Wait for service to be ready
 */
bool wait_for_service_ready(SearchService* service, std::chrono::seconds timeout) {
    std::cout << "⏳ Waiting for service to be ready..." << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    
    while (std::chrono::steady_clock::now() - start_time < timeout) {
        if (service->is_ready()) {
            return true;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
    
    return false;
}

/**
 * Print startup status
 */
void print_startup_status(SearchService* service) {
    auto status = service->get_status();
    
    std::cout << "\n🚀 Service Status:" << std::endl;
    std::cout << "  Service ID: " << status.service_id << std::endl;
    std::cout << "  Version: " << status.service_version << std::endl;
    std::cout << "  Environment: " << status.environment << std::endl;
    std::cout << "  Overall Health: ";
    
    switch (status.overall_health) {
        case ServiceHealth::HEALTHY:
            std::cout << "🟢 HEALTHY" << std::endl;
            break;
        case ServiceHealth::DEGRADED:
            std::cout << "🟡 DEGRADED" << std::endl;
            break;
        case ServiceHealth::UNHEALTHY:
            std::cout << "🔴 UNHEALTHY" << std::endl;
            break;
        case ServiceHealth::CRITICAL:
            std::cout << "🆘 CRITICAL" << std::endl;
            break;
    }
    
    std::cout << "  Uptime: " << status.uptime.count() << " seconds" << std::endl;
    std::cout << "  Health Score: " << status.get_health_score() << "/100" << std::endl;
    
    std::cout << "\n📊 Component Status:" << std::endl;
    for (const auto& component : status.components) {
        std::cout << "  " << component.name << ": ";
        if (component.is_healthy) {
            std::cout << "🟢 HEALTHY";
        } else {
            std::cout << "🔴 UNHEALTHY";
        }
        std::cout << " (" << component.response_time.count() << "ms)" << std::endl;
    }
}

/**
 * Main entry point
 */
int main(int argc, char* argv[]) {
    // Setup signal handlers
    setup_signal_handlers();

    // Initialize JSON logger
    (void)sonet::logging::init_json_stdout_logger();
    spdlog::info(R"({"event":"startup","message":"Starting Sonet Search Service"})");
    spdlog::info(R"({"event":"banner","service":"search","message":"Sonet Search Service starting"})");
    
    // Parse command line arguments
    std::string config_file;
    std::string environment = "production";
    std::string log_level;
    std::string elasticsearch_url;
    int http_port = 0;
    int grpc_port = 0;
    bool debug_mode = false;
    bool test_mode = false;
    bool dry_run = false;
    bool check_health = false;
    bool no_indexing = false;
    bool no_trending = false;
    bool no_cache = false;
    
    static struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"version", no_argument, 0, 'v'},
        {"config", required_argument, 0, 'c'},
        {"environment", required_argument, 0, 'e'},
        {"port", required_argument, 0, 'p'},
        {"grpc-port", required_argument, 0, 'g'},
        {"debug", no_argument, 0, 'd'},
        {"test", no_argument, 0, 't'},
        {"dry-run", no_argument, 0, 0},
        {"check-health", no_argument, 0, 0},
        {"elasticsearch-url", required_argument, 0, 0},
        {"log-level", required_argument, 0, 0},
        {"no-indexing", no_argument, 0, 0},
        {"no-trending", no_argument, 0, 0},
        {"no-cache", no_argument, 0, 0},
        {0, 0, 0, 0}
    };
    
    int option_index = 0;
    int c;
    
    while ((c = getopt_long(argc, argv, "hvc:e:p:g:dt", long_options, &option_index)) != -1) {
        switch (c) {
            case 'h':
                print_usage(argv[0]);
                return 0;
            case 'v':
                print_version();
                return 0;
            case 'c':
                config_file = optarg;
                break;
            case 'e':
                environment = optarg;
                break;
            case 'p':
                http_port = std::atoi(optarg);
                break;
            case 'g':
                grpc_port = std::atoi(optarg);
                break;
            case 'd':
                debug_mode = true;
                break;
            case 't':
                test_mode = true;
                break;
            case 0:
                if (long_options[option_index].name == std::string("dry-run")) {
                    dry_run = true;
                } else if (long_options[option_index].name == std::string("check-health")) {
                    check_health = true;
                } else if (long_options[option_index].name == std::string("elasticsearch-url")) {
                    elasticsearch_url = optarg;
                } else if (long_options[option_index].name == std::string("log-level")) {
                    log_level = optarg;
                } else if (long_options[option_index].name == std::string("no-indexing")) {
                    no_indexing = true;
                } else if (long_options[option_index].name == std::string("no-trending")) {
                    no_trending = true;
                } else if (long_options[option_index].name == std::string("no-cache")) {
                    no_cache = true;
                }
                break;
            case '?':
                std::cerr << "Use --help for usage information" << std::endl;
                return 1;
            default:
                break;
        }
    }
    
    try {
        // Print banner
        print_banner();
        
        // Load configuration
        SearchServiceConfig config;
        
        if (!config_file.empty()) {
            std::cout << "📁 Loading configuration from: " << config_file << std::endl;
            config = SearchServiceConfig::from_file(config_file);
        } else if (environment == "production") {
            config = SearchServiceConfig::production_config();
        } else if (environment == "development") {
            config = SearchServiceConfig::development_config();
        } else if (environment == "testing") {
            config = SearchServiceConfig::testing_config();
        } else {
            config = SearchServiceConfig::from_environment();
        }
        
        // Apply command line overrides
        config.environment = environment;
        
        if (http_port > 0) {
            config.controller_config.http_port = http_port;
        }
        
        if (grpc_port > 0) {
            config.controller_config.grpc_port = grpc_port;
        }
        
        if (!log_level.empty()) {
            config.log_level = log_level;
        }
        
        if (!elasticsearch_url.empty()) {
            config.elasticsearch_config.hosts = {elasticsearch_url};
        }
        
        if (no_indexing) {
            config.enable_real_time_indexing = false;
        }
        
        if (no_trending) {
            config.enable_trending_analysis = false;
        }
        
        if (no_cache) {
            config.enable_caching = false;
        }
        
        // Set up logging
        service_utils::setup_logging(config);
        
        // Validate configuration
        if (!validate_configuration(config)) {
            return 1;
        }
        
        // Handle special modes
        if (dry_run) {
            std::cout << "✅ Configuration is valid. Exiting (dry run mode)." << std::endl;
            return 0;
        }
        
        if (check_health) {
            return check_service_health(config);
        }
        
        if (test_mode) {
            return run_integration_tests(config);
        }
        
        // Create and initialize service
        std::cout << "🏗️  Creating search service..." << std::endl;
        g_service = SearchServiceFactory::create_with_config(config);
        
        if (debug_mode) {
            g_service->set_debug_mode(true);
        }
        
        std::cout << "⚙️  Initializing service components..." << std::endl;
        auto init_future = g_service->initialize();
        if (!init_future.get()) {
            std::cerr << "❌ Failed to initialize search service" << std::endl;
            return 1;
        }
        
        std::cout << "🚀 Starting search service..." << std::endl;
        auto start_future = g_service->start();
        if (!start_future.get()) {
            std::cerr << "❌ Failed to start search service" << std::endl;
            return 1;
        }
        
        // Wait for service to be ready
        if (!wait_for_service_ready(g_service.get(), std::chrono::seconds{60})) {
            std::cerr << "❌ Service failed to become ready within timeout" << std::endl;
            return 1;
        }
        
        // Print startup status
        print_startup_status(g_service.get());
        
        std::cout << "\n✅ Search service is ready and accepting requests!" << std::endl;
        std::cout << "🌐 HTTP endpoint: http://" << config.controller_config.bind_address 
                  << ":" << config.controller_config.http_port << std::endl;
        std::cout << "⚡ gRPC endpoint: " << config.controller_config.grpc_bind_address 
                  << ":" << config.controller_config.grpc_port << std::endl;
        std::cout << "📊 Metrics endpoint: http://" << config.controller_config.bind_address 
                  << ":" << config.controller_config.http_port << config.metrics_endpoint << std::endl;
        std::cout << "🏥 Health endpoint: http://" << config.controller_config.bind_address 
                  << ":" << config.controller_config.http_port << config.health_endpoint << std::endl;
        
        std::cout << "\n💡 Press Ctrl+C to stop the service" << std::endl;
        
        // Main service loop
        while (!g_shutdown_requested && g_service->is_running()) {
            std::this_thread::sleep_for(std::chrono::seconds{1});
            
            // Optionally print periodic status updates in debug mode
            if (debug_mode) {
                static int status_counter = 0;
                if (++status_counter % 60 == 0) {  // Every minute
                    auto status = g_service->get_status();
                    std::cout << "📈 Status: Health=" << status.get_health_score() 
                              << "/100, Uptime=" << status.uptime.count() << "s" << std::endl;
                }
            }
        }
        
        std::cout << "\n🛑 Shutting down search service..." << std::endl;
        g_service->stop();
        
        std::cout << "✅ Search service shutdown complete. Goodbye!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "💥 Fatal error: " << e.what() << std::endl;
        
        if (g_service) {
            g_service->stop();
        }
        
        return 1;
    } catch (...) {
        std::cerr << "💥 Unknown fatal error occurred" << std::endl;
        
        if (g_service) {
            g_service->stop();
        }
        
        return 1;
    }
}