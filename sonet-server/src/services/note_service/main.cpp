/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "note_service_orchestrator.h"
#include <iostream>
#include <csignal>
#include <memory>
#include "../../core/logging/logger.h"

/**
 * @brief Main entry point for the Twitter-Scale Note Service
 * 
 * This service provides:
 * 
 * 🚀 **HTTP REST API** (Port 8080):
 *    - NOTE   /api/v1/notes                    - Create note (300 chars, attachments)
 *    - GET    /api/v1/notes/:id               - Get note with thread context
 *    - PUT    /api/v1/notes/:id               - Edit note (30-min window)
 *    - DELETE /api/v1/notes/:id               - Delete with cascade
 *    - NOTE   /api/v1/notes/:id/renote        - Renote (retweet)
 *    - NOTE   /api/v1/notes/:id/like          - Like note
 *    - GET    /api/v1/timelines/home          - Personalized timeline
 *    - GET    /api/v1/timelines/trending      - Trending content
 *    - GET    /api/v1/search/notes            - Advanced search
 *    - NOTE   /api/v1/notes/batch             - Bulk operations
 * 
 * ⚡ **gRPC High-Performance API** (Port 9090):
 *    - Sub-5ms note retrieval
 *    - Sub-10ms note creation  
 *    - Batch operations (100 notes in <20ms)
 *    - Real-time streaming
 *    - Inter-service communication
 * 
 * 🔄 **WebSocket Real-Time Features** (Port 8081):
 *    - Live timeline updates
 *    - Real-time engagement (likes, renotes)
 *    - Typing indicators
 *    - Push notifications
 *    - Online presence
 * 
 * 📱 **Twitter-Scale Features**:
 *    - 300-character notes with "renote" terminology
 *    - 10 attachments per note (images, videos, GIFs, polls, location)
 *    - Tenor GIF integration
 *    - Advanced search and trending
 *    - Content moderation and safety
 *    - Analytics and insights
 *    - Horizontal scaling ready
 * 
 * Usage: ./note_service [config_file]
 */

using namespace sonet::note;

// Global service instance for signal handling
static std::unique_ptr<NoteServiceOrchestrator> g_service_instance;

void signal_handler(int signal) {
    std::cout << "\n🛑 Received signal " << signal << ", initiating graceful shutdown..." << std::endl;
    
    if (g_service_instance) {
        g_service_instance->shutdown();
        g_service_instance.reset();
    }
    
    std::cout << "✅ Note service shutdown complete. Goodbye!" << std::endl;
    std::exit(0);
}

void print_startup_banner() {
    std::cout << R"(
██████╗ ██████╗ ███╗   ██╗███████╗████████╗    ███╗   ██╗ ██████╗ ████████╗███████╗
██╔══██╗██╔═══██╗████╗  ██║██╔════╝╚══██╔══╝    ████╗  ██║██╔═══██╗╚══██╔══╝██╔════╝
██████╔╝██║   ██║██╔██╗ ██║█████╗     ██║       ██╔██╗ ██║██║   ██║   ██║   █████╗  
██╔══██╗██║   ██║██║╚██╗██║██╔══╝     ██║       ██║╚██╗██║██║   ██║   ██║   ██╔══╝  
██████╔╝╚██████╔╝██║ ╚████║███████╗   ██║       ██║ ╚████║╚██████╔╝   ██║   ███████╗
╚═════╝  ╚═════╝ ╚═╝  ╚═══╝╚══════╝   ╚═╝       ╚═╝  ╚═══╝ ╚═════╝    ╚═╝   ╚══════╝

███████╗███████╗██████╗ ██╗   ██╗██╗ ██████╗███████╗
██╔════╝██╔════╝██╔══██╗██║   ██║██║██╔════╝██╔════╝
███████╗█████╗  ██████╔╝██║   ██║██║██║     █████╗  
╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██║██║     ██╔══╝  
███████║███████╗██║  ██║ ╚████╔╝ ██║╚██████╗███████╗
╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚═╝ ╚═════╝╚══════╝

🚀 Twitter-Scale Note Service v2.0
📝 300-char notes • 🔄 Renotes • 📎 Rich attachments • ⚡ Real-time updates
)" << std::endl;
}

void print_service_info() {
    std::cout << "\n🌟 SERVICE ENDPOINTS:" << std::endl;
    std::cout << "   📡 HTTP REST API:     http://localhost:8080/api/v1/" << std::endl;
    std::cout << "   ⚡ gRPC Service:      localhost:9090" << std::endl;
    std::cout << "   🔄 WebSocket:         ws://localhost:8081/ws" << std::endl;
    
    std::cout << "\n🚀 KEY FEATURES:" << std::endl;
    std::cout << "   ✨ Twitter-like 300-character notes with 'renote' functionality" << std::endl;
    std::cout << "   📎 Rich attachments: images, videos, GIFs, polls, location data" << std::endl;
    std::cout << "   🎬 Tenor GIF integration with search and trending" << std::endl;
    std::cout << "   🔍 Advanced search with filters and real-time suggestions" << std::endl;
    std::cout << "   📊 Analytics and engagement metrics" << std::endl;
    std::cout << "   🛡️ Content moderation and safety features" << std::endl;
    std::cout << "   🌐 Real-time updates and notifications" << std::endl;
    std::cout << "   📈 Horizontal scaling and high performance" << std::endl;
    
    std::cout << "\n📊 PERFORMANCE TARGETS:" << std::endl;
    std::cout << "   🚀 Note creation: < 10ms" << std::endl;
    std::cout << "   📖 Note retrieval: < 5ms" << std::endl;
    std::cout << "   ❤️  Like operations: < 3ms" << std::endl;
    std::cout << "   📱 Timeline loading: < 15ms" << std::endl;
    std::cout << "   🔍 Search queries: < 50ms" << std::endl;
    std::cout << "   📦 Batch operations: 100 notes in < 20ms" << std::endl;
    
    std::cout << "\n🔗 EXAMPLE API CALLS:" << std::endl;
    std::cout << "   # Create a note with attachment" << std::endl;
    std::cout << "   curl -X NOTE http://localhost:8080/api/v1/notes \\" << std::endl;
    std::cout << "        -H \"Content-Type: application/json\" \\" << std::endl;
    std::cout << "        -H \"Authorization: Bearer YOUR_TOKEN\" \\" << std::endl;
    std::cout << "        -d '{\"content\":\"Hello Twitter-scale world! 🚀\",\"attachments\":[]}'" << std::endl;
    
    std::cout << "\n   # Get trending timeline" << std::endl;
    std::cout << "   curl http://localhost:8080/api/v1/timelines/trending?limit=20" << std::endl;
    
    std::cout << "\n   # WebSocket real-time updates" << std::endl;
    std::cout << "   wscat -c ws://localhost:8081/ws" << std::endl;
    std::cout << "   > {\"type\":\"subscribe\",\"timeline\":\"home\"}" << std::endl;
    
    std::cout << "\n📚 For detailed API documentation, visit: /docs/api/" << std::endl;
}

int main(int argc, char* argv[]) {
    (void)sonet::logging::init_json_stdout_logger();
    spdlog::info(R"({"event":"banner","service":"note","message":"Sonet Note Service starting"})");
    try {
        // Print startup banner
        print_startup_banner();
        
        // Setup signal handlers
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        
        // Parse command line arguments
        std::string config_file = "config/production/services.json";
        if (argc > 1) {
            config_file = argv[1];
        }
        
        std::cout << "🔧 Loading configuration from: " << config_file << std::endl;
        
        // Create and initialize service
        g_service_instance = NoteServiceBuilder::create_production_service(config_file);
        
        if (!g_service_instance) {
            std::cerr << "❌ Failed to create note service instance" << std::endl;
            return 1;
        }
        
        std::cout << "🔄 Initializing note service components..." << std::endl;
        if (!g_service_instance->initialize()) {
            std::cerr << "❌ Failed to initialize note service" << std::endl;
            return 1;
        }
        
        std::cout << "🚀 Starting note service..." << std::endl;
        if (!g_service_instance->start()) {
            std::cerr << "❌ Failed to start note service" << std::endl;
            return 1;
        }
        
        // Print service information
        print_service_info();
        
        // Service health check
        if (g_service_instance->is_healthy() && g_service_instance->is_ready()) {
            std::cout << "\n✅ Note service is healthy and ready to accept requests!" << std::endl;
            std::cout << "📊 Performance metrics: " << g_service_instance->get_performance_metrics().dump(2) << std::endl;
        } else {
            std::cerr << "⚠️  Note service started but health checks failed" << std::endl;
        }
        
        // Warm up caches for better performance
        std::cout << "\n🔥 Warming up caches for optimal performance..." << std::endl;
        g_service_instance->warm_caches();
        
        std::cout << "\n🎉 Note service fully operational! Press Ctrl+C to shutdown gracefully." << std::endl;
        
        // Keep the service running
        while (g_service_instance && g_service_instance->is_healthy()) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            
            // Print periodic status updates
            static int status_counter = 0;
            if (++status_counter % 30 == 0) { // Every 5 minutes
                auto stats = g_service_instance->get_service_statistics();
                std::cout << "📊 Service status: " 
                         << stats["total_requests"] << " requests, "
                         << stats["active_connections"] << " connections, "
                         << stats["memory_usage_mb"] << "MB memory" << std::endl;
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "💥 Fatal error: " << e.what() << std::endl;
        
        if (g_service_instance) {
            std::cerr << "🔄 Attempting emergency shutdown..." << std::endl;
            g_service_instance->shutdown();
            g_service_instance.reset();
        }
        
        return 1;
    }
    
    return 0;
}

/*
 * DEPLOYMENT NOTES:
 * 
 * 🐳 Docker Deployment:
 * docker build -t sonet-note-service .
 * docker run -d -p 8080:8080 -p 9090:9090 -p 8081:8081 \
 *   -e DATABASE_URL=postgresql://... \
 *   -e REDIS_URL=redis://... \
 *   sonet-note-service
 * 
 * ☸️ Kubernetes Deployment:
 * kubectl apply -f deployment/kubernetes/
 * 
 * 🌐 Load Balancing:
 * - HTTP: Use nginx/HAProxy for REST API
 * - gRPC: Use Envoy proxy for gRPC load balancing
 * - WebSocket: Sticky sessions or Redis pub/sub for clustering
 * 
 * 📊 Monitoring:
 * - Prometheus metrics on /metrics endpoint
 * - Grafana dashboards for visualization
 * - Jaeger for distributed tracing
 * - Health checks on /health endpoint
 * 
 * 🔧 Configuration:
 * - Environment variables for secrets
 * - ConfigMaps for application config
 * - Horizontal Pod Autoscaler for scaling
 * - Resource limits and requests
 * 
 * 🚀 Performance Optimizations:
 * - Connection pooling for database and Redis
 * - Multi-level caching (L1 memory, L2 Redis)
 * - Async processing for heavy operations
 * - CDN for static assets and media
 * - Read replicas for database scaling
 */
