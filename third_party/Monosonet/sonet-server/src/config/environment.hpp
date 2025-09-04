// Sonet Services Environment Configuration
// Unified with monorepo environment management

#pragma once

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <cstdlib>
#include <iostream>

namespace sonet {
namespace config {

/**
 * Environment variable helper functions
 */
class Environment {
public:
    /**
     * Get environment variable with default value
     */
    static std::string get(const std::string& key, const std::string& defaultValue = "") {
        const char* value = std::getenv(key.c_str());
        return value ? value : defaultValue;
    }

    /**
     * Get environment variable as integer with default value
     */
    static int getInt(const std::string& key, int defaultValue = 0) {
        const char* value = std::getenv(key.c_str());
        if (!value) return defaultValue;
        try {
            return std::stoi(value);
        } catch (...) {
            return defaultValue;
        }
    }

    /**
     * Get environment variable as boolean
     */
    static bool getBool(const std::string& key, bool defaultValue = false) {
        const char* value = std::getenv(key.c_str());
        if (!value) return defaultValue;
        std::string str(value);
        return str == "true" || str == "1" || str == "yes";
    }

    /**
     * Get environment variable as double with default value
     */
    static double getDouble(const std::string& key, double defaultValue = 0.0) {
        const char* value = std::getenv(key.c_str());
        if (!value) return defaultValue;
        try {
            return std::stod(value);
        } catch (...) {
            return defaultValue;
        }
    }

    /**
     * Split comma-separated environment variable into vector
     */
    static std::vector<std::string> getList(const std::string& key, const std::vector<std::string>& defaultValue = {}) {
        const char* value = std::getenv(key.c_str());
        if (!value) return defaultValue;
        
        std::vector<std::string> result;
        std::string str(value);
        size_t pos = 0;
        std::string token;
        
        while ((pos = str.find(',')) != std::string::npos) {
            token = str.substr(0, pos);
            if (!token.empty()) {
                result.push_back(token);
            }
            str.erase(0, pos + 1);
        }
        
        if (!str.empty()) {
            result.push_back(str);
        }
        
        return result.empty() ? defaultValue : result;
    }
};

/**
 * Database configuration
 */
struct DatabaseConfig {
    std::string host = Environment::get("postgres_host", "localhost");
    int port = Environment::getInt("postgres_port", 5432);
    std::string user = Environment::get("postgres_user", "sonet");
    std::string password = Environment::get("postgres_password", "sonet_dev_password");
    std::string database = Environment::get("postgres_db", "sonet_dev");
    std::string sslMode = Environment::get("postgres_ssl_mode", "disable");
    
    std::string getConnectionString() const {
        return "postgresql://" + user + ":" + password + "@" + host + ":" + std::to_string(port) + "/" + database;
    }
};

/**
 * Redis configuration
 */
struct RedisConfig {
    std::string host = Environment::get("REDIS_HOST", "localhost");
    int port = Environment::getInt("REDIS_PORT", 6379);
    std::string password = Environment::get("REDIS_PASSWORD", "");
    int database = Environment::getInt("REDIS_DB", 0);
    std::string url = Environment::get("REDIS_URL", "redis://localhost:6379");
    
    std::string getConnectionString() const {
        if (!password.empty()) {
            return "redis://:" + password + "@" + host + ":" + std::to_string(port) + "/" + std::to_string(database);
        }
        return "redis://" + host + ":" + std::to_string(port) + "/" + std::to_string(database);
    }
};

/**
 * Service configuration
 */
struct ServiceConfig {
    std::string name = Environment::get("SERVICE_NAME", "sonet-service");
    int port = Environment::getInt("SERVICE_PORT", 8080);
    int grpcPort = Environment::getInt("SERVICE_GRPC_PORT", 9090);
    std::string logLevel = Environment::get("LOG_LEVEL", "debug");
    std::string environment = Environment::get("NODE_ENV", "development");
};

/**
 * JWT configuration
 */
struct JWTConfig {
    std::string secret = Environment::get("JWT_SECRET", "dev_jwt_secret_key_change_in_production");
    std::string expiresIn = Environment::get("JWT_EXPIRES_IN", "7d");
    std::string refreshExpiresIn = Environment::get("JWT_REFRESH_EXPIRES_IN", "30d");
};

/**
 * Rate limiting configuration
 */
struct RateLimitConfig {
    bool enabled = Environment::getBool("RATE_LIMIT_ENABLED", true);
    int windowMs = Environment::getInt("RATE_LIMIT_WINDOW_MS", 900000);
    int maxRequests = Environment::getInt("RATE_LIMIT_MAX_REQUESTS", 100);
};

/**
 * File upload configuration
 */
struct FileUploadConfig {
    int maxFileSize = Environment::getInt("MAX_FILE_SIZE", 10485760); // 10MB
    std::vector<std::string> allowedFileTypes = Environment::getList("ALLOWED_FILE_TYPES", {
        "image/jpeg", "image/png", "image/gif", "video/mp4", "video/webm"
    });
};

/**
 * CDN configuration
 */
struct CDNConfig {
    std::string provider = Environment::get("CDN_PROVIDER", "local");
    std::string baseUrl = Environment::get("CDN_BASE_URL", "http://localhost:8080/cdn");
    std::string region = Environment::get("CDN_REGION", "us-east-1");
    std::string accessKeyId = Environment::get("CDN_ACCESS_KEY_ID", "");
    std::string secretAccessKey = Environment::get("CDN_SECRET_ACCESS_KEY", "");
    std::string bucketName = Environment::get("CDN_BUCKET_NAME", "sonet-media");
};

/**
 * Monitoring configuration
 */
struct MonitoringConfig {
    std::string sentryDsn = Environment::get("SENTRY_DSN", "");
    std::string sentryOrg = Environment::get("SENTRY_ORG", "sonet");
    std::string sentryProject = Environment::get("SENTRY_PROJECT", "sonet-app");
    std::string sentryAuthToken = Environment::get("SENTRY_AUTH_TOKEN", "");
    std::string logLevel = Environment::get("LOG_LEVEL", "debug");
    std::string logFormat = Environment::get("LOG_FORMAT", "json");
    std::string logDestination = Environment::get("LOG_DESTINATION", "console");
};

/**
 * Security configuration
 */
struct SecurityConfig {
    std::string encryptionAlgorithm = Environment::get("ENCRYPTION_ALGORITHM", "AES-256-GCM");
    int encryptionKeySize = Environment::getInt("ENCRYPTION_KEY_SIZE", 32);
    int encryptionIvSize = Environment::getInt("ENCRYPTION_IV_SIZE", 12);
    std::string moderationApiKey = Environment::get("MODERATION_API_KEY", "");
    std::string moderationEndpoint = Environment::get("MODERATION_ENDPOINT", "");
    double moderationThreshold = Environment::getDouble("MODERATION_THRESHOLD", 0.8);
};

/**
 * Main configuration class
 */
class Config {
public:
    DatabaseConfig database;
    RedisConfig redis;
    ServiceConfig service;
    JWTConfig jwt;
    RateLimitConfig rateLimit;
    FileUploadConfig fileUpload;
    CDNConfig cdn;
    MonitoringConfig monitoring;
    SecurityConfig security;
    
    Config() = default;
    
    /**
     * Validate configuration
     */
    bool validate() const {
        bool valid = true;
        
        // Check required fields
        if (database.password.empty()) {
            std::cerr << "❌ postgres_password is required" << std::endl;
            valid = false;
        }
        
        if (jwt.secret == "dev_jwt_secret_key_change_in_production" && service.environment == "production") {
            std::cerr << "❌ JWT_SECRET must be changed in production" << std::endl;
            valid = false;
        }
        
        if (valid) {
            std::cout << "✅ Configuration validation passed" << std::endl;
        }
        
        return valid;
    }
    
    /**
     * Print configuration summary
     */
    void printSummary() const {
        std::cout << "\n🔧 Sonet Service Configuration Summary" << std::endl;
        std::cout << "=====================================" << std::endl;
        std::cout << "Service: " << service.name << std::endl;
        std::cout << "Environment: " << service.environment << std::endl;
        std::cout << "Port: " << service.port << std::endl;
        std::cout << "gRPC Port: " << service.grpcPort << std::endl;
        std::cout << "Database: " << database.host << ":" << database.port << "/" << database.database << std::endl;
        std::cout << "Redis: " << redis.host << ":" << redis.port << std::endl;
        std::cout << "Log Level: " << service.logLevel << std::endl;
        std::cout << "=====================================\n" << std::endl;
    }
};

/**
 * Global configuration instance
 */
extern Config globalConfig;

/**
 * Initialize global configuration
 */
void initializeConfig();

} // namespace config
} // namespace sonet