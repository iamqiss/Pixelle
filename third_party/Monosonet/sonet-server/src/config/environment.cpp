// Sonet Services Environment Configuration Implementation
// Unified with monorepo environment management

#include "environment.hpp"

namespace sonet {
namespace config {

// Global configuration instance
Config globalConfig;

void initializeConfig() {
    // Configuration is automatically initialized through struct constructors
    // which read from environment variables
    
    // Validate configuration
    if (!globalConfig.validate()) {
        std::cerr << "❌ Configuration validation failed" << std::endl;
        exit(1);
    }
    
    // Print configuration summary
    globalConfig.printSummary();
}

} // namespace config
} // namespace sonet