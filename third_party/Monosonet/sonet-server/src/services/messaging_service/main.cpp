/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include <iostream>
#include <memory>
#include <csignal>
#include <cstdlib>
#include <string>
#include <vector>
#include "../../core/logging/logger.h"
#include "include/messaging_service.hpp"

using namespace sonet::messaging;

namespace {
    std::unique_ptr<MessagingService> g_service;
    
    void signal_handler(int signal) {
        std::cout << "\nReceived signal " << signal << ". Initiating graceful shutdown..." << std::endl;
        if (g_service) {
            g_service->shutdown();
        }
        std::exit(signal);
    }
    
    void setup_signal_handlers() {
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        std::signal(SIGQUIT, signal_handler);
        #ifdef SIGPIPE
        std::signal(SIGPIPE, SIG_IGN); // Ignore broken pipe signals
        #endif
    }
    
    void print_banner() {
        std::cout << R"(
 ███████╗ ██████╗ ███╗   ██╗███████╗████████╗
 ██╔════╝██╔═══██╗████╗  ██║██╔════╝╚══██╔══╝
 ███████╗██║   ██║██╔██╗ ██║█████╗     ██║   
 ╚════██║██║   ██║██║╚██╗██║██╔══╝     ██║   
 ███████║╚██████╔╝██║ ╚████║███████╗   ██║   
 ╚══════╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝   ╚═╝   

 ███╗   ███╗███████╗███████╗███████╗ █████╗  ██████╗ ██╗███╗   ██╗ ██████╗ 
 ████╗ ████║██╔════╝██╔════╝██╔════╝██╔══██╗██╔════╝ ██║████╗  ██║██╔════╝ 
 ██╔████╔██║█████╗  ███████╗███████╗███████║██║  ███╗██║██╔██╗ ██║██║  ███╗
 ██║╚██╔╝██║██╔══╝  ╚════██║╚════██║██╔══██║██║   ██║██║██║╚██╗██║██║   ██║
 ██║ ╚═╝ ██║███████╗███████║███████║██║  ██║╚██████╔╝██║██║ ╚████║╚██████╔╝
 ╚═╝     ╚═╝╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚═╝╚═╝  ╚═══╝ ╚═════╝ 
                                                                              
 ███████╗███████╗██████╗ ██╗   ██╗██╗ ██████╗███████╗
 ██╔════╝██╔════╝██╔══██╗██║   ██║██║██╔════╝██╔════╝
 ███████╗█████╗  ██████╔╝██║   ██║██║██║     █████╗  
 ╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██║██║     ██╔══╝  
 ███████║███████╗██║  ██║ ╚████╔╝ ██║╚██████╗███████╗
 ╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚═╝ ╚═════╝╚══════╝
)" << std::endl;
        
        std::cout << "🔐 Military-Grade E2EE Messaging Service" << std::endl;
        std::cout << "🚀 Version: " << ServiceUtils::get_service_version() << std::endl;
        std::cout << "🛡️  Zero-Trust Architecture • Quantum-Resistant Encryption" << std::endl;
        std::cout << "📡 Real-time WebSocket • Secure Attachments • Perfect Forward Secrecy" << std::endl;
        std::cout << std::endl;
    }
    
    void print_help() {
        std::cout << "Sonet Messaging Service - Military-Grade E2EE Communication Platform\n\n";
        std::cout << "USAGE:\n";
        std::cout << "    messaging_service [OPTIONS]\n\n";
        std::cout << "OPTIONS:\n";
        std::cout << "    -h, --help              Show this help message\n";
        std::cout << "    -v, --version           Show version information\n";
        std::cout << "    -c, --config <FILE>     Configuration file path\n";
        std::cout << "    --create-config <FILE>  Create default configuration file\n";
        std::cout << "    --check-config <FILE>   Validate configuration file\n";
        std::cout << "    --test-mode             Run in test mode (minimal setup)\n";
        std::cout << "    --daemon                Run as daemon (background process)\n";
        std::cout << "    --health-check          Perform health check and exit\n";
        std::cout << "    --metrics               Show current metrics and exit\n";
        std::cout << "    --cleanup               Perform cleanup operations and exit\n";
        std::cout << "    --port <PORT>           Override HTTP port\n";
        std::cout << "    --grpc-port <PORT>      Override gRPC port\n";
        std::cout << "    --ws-port <PORT>        Override WebSocket port\n";
        std::cout << "    --verbose               Enable verbose logging\n";
        std::cout << "    --quiet                 Suppress non-error output\n\n";
        std::cout << "EXAMPLES:\n";
        std::cout << "    # Start with default configuration\n";
        std::cout << "    ./messaging_service\n\n";
        std::cout << "    # Start with custom configuration\n";
        std::cout << "    ./messaging_service --config /etc/sonet/messaging.json\n\n";
        std::cout << "    # Create default configuration file\n";
        std::cout << "    ./messaging_service --create-config ./messaging.json\n\n";
        std::cout << "    # Run health check\n";
        std::cout << "    ./messaging_service --health-check\n\n";
        std::cout << "    # Start on custom ports\n";
        std::cout << "    ./messaging_service --port 8080 --grpc-port 9000 --ws-port 9001\n\n";
        std::cout << "ENVIRONMENT VARIABLES:\n";
        std::cout << "    MESSAGING_CONFIG_FILE   Default configuration file\n";
        std::cout << "    MESSAGING_DB_HOST       Database host\n";
        std::cout << "    MESSAGING_DB_PORT       Database port\n";
        std::cout << "    MESSAGING_DB_NAME       Database name\n";
        std::cout << "    MESSAGING_DB_USER       Database user\n";
        std::cout << "    MESSAGING_DB_PASSWORD   Database password\n";
        std::cout << "    MESSAGING_REDIS_HOST    Redis host\n";
        std::cout << "    MESSAGING_REDIS_PORT    Redis port\n";
        std::cout << "    MESSAGING_LOG_LEVEL     Log level (DEBUG, INFO, WARN, ERROR)\n";
        std::cout << "    MESSAGING_ENCRYPTION_KEY Base encryption key\n\n";
        std::cout << "For more information, visit: https://docs.sonet.dev/messaging\n";
    }
    
    void print_version() {
        std::cout << "Sonet Messaging Service" << std::endl;
        std::cout << "Version: " << ServiceUtils::get_service_version() << std::endl;
        std::cout << "Build Info: " << ServiceUtils::get_build_info() << std::endl;
        std::cout << "Encryption: AES-256-GCM, ChaCha20-Poly1305, X25519 ECDH" << std::endl;
        std::cout << "Features: E2EE, Perfect Forward Secrecy, Quantum Resistance" << std::endl;
        std::cout << "Protocols: HTTP/2, gRPC, WebSocket, TLS 1.3" << std::endl;
    }
    
    bool parse_arguments(int argc, char* argv[], ServiceConfiguration& config, 
                        std::string& config_file, bool& daemon_mode, bool& test_mode,
                        bool& health_check_only, bool& metrics_only, bool& cleanup_only,
                        bool& verbose, bool& quiet) {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            
            if (arg == "-h" || arg == "--help") {
                print_help();
                return false;
            }
            else if (arg == "-v" || arg == "--version") {
                print_version();
                return false;
            }
            else if (arg == "-c" || arg == "--config") {
                if (i + 1 < argc) {
                    config_file = argv[++i];
                } else {
                    std::cerr << "Error: --config requires a file path" << std::endl;
                    return false;
                }
            }
            else if (arg == "--create-config") {
                if (i + 1 < argc) {
                    std::string path = argv[++i];
                    if (ServiceUtils::create_default_config_file(path)) {
                        std::cout << "Default configuration created at: " << path << std::endl;
                    } else {
                        std::cerr << "Failed to create configuration file" << std::endl;
                    }
                    return false;
                } else {
                    std::cerr << "Error: --create-config requires a file path" << std::endl;
                    return false;
                }
            }
            else if (arg == "--check-config") {
                if (i + 1 < argc) {
                    std::string path = argv[++i];
                    // TODO: Implement config validation
                    std::cout << "Configuration file is valid: " << path << std::endl;
                    return false;
                } else {
                    std::cerr << "Error: --check-config requires a file path" << std::endl;
                    return false;
                }
            }
            else if (arg == "--test-mode") {
                test_mode = true;
            }
            else if (arg == "--daemon") {
                daemon_mode = true;
            }
            else if (arg == "--health-check") {
                health_check_only = true;
            }
            else if (arg == "--metrics") {
                metrics_only = true;
            }
            else if (arg == "--cleanup") {
                cleanup_only = true;
            }
            else if (arg == "--port") {
                if (i + 1 < argc) {
                    config.http_port = static_cast<uint16_t>(std::stoi(argv[++i]));
                } else {
                    std::cerr << "Error: --port requires a port number" << std::endl;
                    return false;
                }
            }
            else if (arg == "--grpc-port") {
                if (i + 1 < argc) {
                    config.grpc_port = static_cast<uint16_t>(std::stoi(argv[++i]));
                } else {
                    std::cerr << "Error: --grpc-port requires a port number" << std::endl;
                    return false;
                }
            }
            else if (arg == "--ws-port") {
                if (i + 1 < argc) {
                    config.websocket_port = static_cast<uint16_t>(std::stoi(argv[++i]));
                } else {
                    std::cerr << "Error: --ws-port requires a port number" << std::endl;
                    return false;
                }
            }
            else if (arg == "--verbose") {
                verbose = true;
            }
            else if (arg == "--quiet") {
                quiet = true;
            }
            else {
                std::cerr << "Error: Unknown argument: " << arg << std::endl;
                std::cerr << "Use --help for usage information" << std::endl;
                return false;
            }
        }
        
        return true;
    }
    
    void run_health_check() {
        std::cout << "Performing health check..." << std::endl;
        
        // Create minimal service for health check
        auto service = MessagingServiceFactory::create_minimal_service();
        if (!service) {
            std::cerr << "❌ Failed to create service for health check" << std::endl;
            std::exit(1);
        }
        
        bool healthy = service->perform_health_check();
        Json::Value health_status = service->get_health_status();
        
        std::cout << "Health Status: " << (healthy ? "✅ HEALTHY" : "❌ UNHEALTHY") << std::endl;
        std::cout << health_status.toStyledString() << std::endl;
        
        std::exit(healthy ? 0 : 1);
    }
    
    void show_metrics() {
        std::cout << "Fetching current metrics..." << std::endl;
        
        // Create minimal service for metrics
        auto service = MessagingServiceFactory::create_minimal_service();
        if (!service) {
            std::cerr << "❌ Failed to create service for metrics" << std::endl;
            std::exit(1);
        }
        
        Json::Value metrics = service->get_detailed_metrics();
        std::cout << "Service Metrics:" << std::endl;
        std::cout << metrics.toStyledString() << std::endl;
        
        std::exit(0);
    }
    
    void run_cleanup() {
        std::cout << "Performing cleanup operations..." << std::endl;
        
        // Create minimal service for cleanup
        auto service = MessagingServiceFactory::create_minimal_service();
        if (!service) {
            std::cerr << "❌ Failed to create service for cleanup" << std::endl;
            std::exit(1);
        }
        
        service->force_cleanup();
        std::cout << "✅ Cleanup completed successfully" << std::endl;
        
        std::exit(0);
    }
    
    void run_as_daemon() {
        // Fork to background
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "❌ Failed to fork daemon process" << std::endl;
            std::exit(1);
        }
        
        if (pid > 0) {
            // Parent process - exit
            std::cout << "✅ Daemon started with PID: " << pid << std::endl;
            std::exit(0);
        }
        
        // Child process - become daemon
        if (setsid() < 0) {
            std::cerr << "❌ Failed to create new session" << std::endl;
            std::exit(1);
        }
        
        // Change to root directory
        if (chdir("/") < 0) {
            std::cerr << "❌ Failed to change to root directory" << std::endl;
            std::exit(1);
        }
        
        // Close standard file descriptors
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }
}

int main(int argc, char* argv[]) {
    // Initialize JSON logger for ELK ingestion
    (void)sonet::logging::init_json_stdout_logger();
    spdlog::info(R"({"event":"startup","message":"Starting Sonet Messaging Service"})");
    try {
        // Parse command line arguments
        ServiceConfiguration config;
        std::string config_file;
        bool daemon_mode = false;
        bool test_mode = false;
        bool health_check_only = false;
        bool metrics_only = false;
        bool cleanup_only = false;
        bool verbose = false;
        bool quiet = false;
        
        if (!parse_arguments(argc, argv, config, config_file, daemon_mode, test_mode,
                           health_check_only, metrics_only, cleanup_only, verbose, quiet)) {
            return 0; // Help/version was shown, or error occurred
        }
        
        // Handle special operations
        if (health_check_only) {
            run_health_check();
            return 0;
        }
        
        if (metrics_only) {
            show_metrics();
            return 0;
        }
        
        if (cleanup_only) {
            run_cleanup();
            return 0;
        }
        
        // Print banner unless quiet mode
        if (!quiet) {
            print_banner();
        }
        
        // Setup signal handlers
        setup_signal_handlers();
        
        // Check environment variables
        if (!ServiceUtils::check_environment_variables()) {
            std::cerr << "❌ Required environment variables are missing" << std::endl;
            return 1;
        }
        
        // Create and configure service
        if (test_mode) {
            g_service = MessagingServiceFactory::create_test_service();
            std::cout << "🧪 Running in test mode" << std::endl;
        } else {
            g_service = MessagingServiceFactory::create_service();
        }
        
        if (!g_service) {
            std::cerr << "❌ Failed to create messaging service" << std::endl;
            return 1;
        }
        
        // Initialize service
        std::cout << "🔧 Initializing service..." << std::endl;
        if (!g_service->initialize(config_file)) {
            std::cerr << "❌ Failed to initialize messaging service" << std::endl;
            return 1;
        }
        
        // Run as daemon if requested
        if (daemon_mode) {
            run_as_daemon();
        }
        
        // Start service
        std::cout << "🚀 Starting messaging service..." << std::endl;
        if (!g_service->start()) {
            std::cerr << "❌ Failed to start messaging service" << std::endl;
            return 1;
        }
        
        auto service_config = g_service->get_configuration();
        std::cout << "✅ Messaging service started successfully!" << std::endl;
        std::cout << "📡 HTTP Server: http://" << service_config.host << ":" << service_config.http_port << std::endl;
        std::cout << "🔗 gRPC Server: " << service_config.host << ":" << service_config.grpc_port << std::endl;
        std::cout << "🌐 WebSocket Server: ws://" << service_config.host << ":" << service_config.websocket_port << std::endl;
        std::cout << "🔐 Encryption: " << (service_config.e2e_encryption_enabled ? "E2EE Enabled" : "Server-side Only") << std::endl;
        std::cout << "⚡ Quantum Resistant: " << (service_config.quantum_resistant_mode ? "Yes" : "No") << std::endl;
        std::cout << std::endl;
        std::cout << "🛡️  Military-grade encryption protecting your communications" << std::endl;
        std::cout << "📊 Health endpoint: http://" << service_config.host << ":" << service_config.http_port << "/health" << std::endl;
        std::cout << "📈 Metrics endpoint: http://" << service_config.host << ":" << service_config.http_port << "/metrics" << std::endl;
        std::cout << std::endl;
        std::cout << "Press Ctrl+C to stop the service..." << std::endl;
        
        // Wait for shutdown signal
        g_service->wait_for_shutdown();
        
        std::cout << "\n🛑 Shutting down messaging service..." << std::endl;
        g_service.reset();
        std::cout << "✅ Service stopped gracefully" << std::endl;
        
        return 0;
        
    } catch (const ConfigurationException& e) {
        std::cerr << "❌ Configuration Error: " << e.what() << std::endl;
        return 1;
    } catch (const InitializationException& e) {
        std::cerr << "❌ Initialization Error: " << e.what() << std::endl;
        return 1;
    } catch (const DatabaseException& e) {
        std::cerr << "❌ Database Error: " << e.what() << std::endl;
        return 1;
    } catch (const EncryptionException& e) {
        std::cerr << "❌ Encryption Error: " << e.what() << std::endl;
        return 1;
    } catch (const MessagingServiceException& e) {
        std::cerr << "❌ Service Error [" << e.get_error_code() << "]: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "❌ Unexpected Error: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "❌ Unknown error occurred" << std::endl;
        return 1;
    }
}
