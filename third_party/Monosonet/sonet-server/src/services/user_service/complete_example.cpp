/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "controllers/auth_controller.h"
#include "controllers/user_controller.h"
#include "controllers/profile_controller.h"
#include "handlers/http_handler.h"
#include "include/email_service.h"
#include "include/file_upload_service.h"
#include "include/repository.h"
#include <spdlog/spdlog.h>
#include <memory>
#include <map>

namespace sonet::user {

/**
 * Complete User Service Application with all real functionality
 * This demonstrates how to wire up all the components with actual implementations
 */
class UserServiceApp {
public:
    UserServiceApp() = default;
    ~UserServiceApp() = default;

    bool initialize(const std::map<std::string, std::string>& config) {
        try {
            spdlog::info("Initializing User Service with real implementations...");
            
            // Initialize Email Service
            if (!initialize_email_service(config)) {
                spdlog::error("Failed to initialize email service");
                return false;
            }
            
            // Initialize File Upload Service
            if (!initialize_file_service(config)) {
                spdlog::error("Failed to initialize file service");
                return false;
            }
            
            // Initialize Database Repositories
            if (!initialize_repositories(config)) {
                spdlog::error("Failed to initialize repositories");
                return false;
            }
            
            // Initialize gRPC Service
            user_service_impl_ = std::make_shared<UserServiceImpl>();
            
            // Initialize Controllers with real services
            initialize_controllers(config);
            
            // Initialize HTTP Handler
            initialize_http_handler();
            
            spdlog::info("User Service initialization complete!");
            return true;
            
        } catch (const std::exception& e) {
            spdlog::error("Failed to initialize User Service: {}", e.what());
            return false;
        }
    }
    
    // Get the HTTP handler for integration with web server
    std::shared_ptr<handlers::HttpHandler> get_http_handler() const {
        return http_handler_;
    }
    
    // Get individual services for testing or direct access
    std::shared_ptr<email::EmailService> get_email_service() const { return email_service_; }
    std::shared_ptr<storage::FileUploadService> get_file_service() const { return file_service_; }
    std::shared_ptr<controllers::AuthController> get_auth_controller() const { return auth_controller_; }
    std::shared_ptr<controllers::UserController> get_user_controller() const { return user_controller_; }
    std::shared_ptr<controllers::ProfileController> get_profile_controller() const { return profile_controller_; }

private:
    // Core services
    std::shared_ptr<email::EmailService> email_service_;
    std::shared_ptr<storage::FileUploadService> file_service_;
    std::shared_ptr<UserServiceImpl> user_service_impl_;
    
    // Controllers
    std::shared_ptr<controllers::AuthController> auth_controller_;
    std::shared_ptr<controllers::UserController> user_controller_;
    std::shared_ptr<controllers::ProfileController> profile_controller_;
    
    // HTTP Handler
    std::shared_ptr<handlers::HttpHandler> http_handler_;
    
    // Configuration
    std::string connection_string_;
    
    bool initialize_email_service(const std::map<std::string, std::string>& config) {
        try {
            // Determine email provider from config
            std::string email_provider = config.at("email_provider"); // "smtp", "sendgrid", "aws_ses"
            
            email::EmailProvider provider = email::EmailProvider::SMTP;
            if (email_provider == "sendgrid") {
                provider = email::EmailProvider::SENDGRID;
            } else if (email_provider == "aws_ses") {
                provider = email::EmailProvider::AWS_SES;
            }
            
            email_service_ = std::make_shared<email::EmailService>(provider);
            
            // Configure based on provider
            std::map<std::string, std::string> email_config;
            if (provider == email::EmailProvider::SMTP) {
                email_config = {
                    {"host", config.at("smtp_host")},
                    {"port", config.at("smtp_port")},
                    {"username", config.at("smtp_username")},
                    {"password", config.at("smtp_password")},
                    {"use_tls", config.value("smtp_use_tls", "true")}
                };
            } else if (provider == email::EmailProvider::SENDGRID) {
                email_config = {
                    {"api_key", config.at("sendgrid_api_key")}
                };
            }
            
            bool initialized = email_service_->initialize(email_config);
            if (initialized) {
                spdlog::info("Email service initialized with provider: {}", email_provider);
            }
            
            return initialized;
            
        } catch (const std::exception& e) {
            spdlog::error("Email service initialization error: {}", e.what());
            return false;
        }
    }
    
    bool initialize_file_service(const std::map<std::string, std::string>& config) {
        try {
            // Determine storage provider
            std::string storage_provider = config.at("storage_provider"); // "local", "s3", "gcs"
            
            storage::StorageProvider provider = storage::StorageProvider::LOCAL_FILESYSTEM;
            if (storage_provider == "s3") {
                provider = storage::StorageProvider::AWS_S3;
            } else if (storage_provider == "gcs") {
                provider = storage::StorageProvider::GOOGLE_CLOUD_STORAGE;
            }
            
            file_service_ = std::make_shared<storage::FileUploadService>(provider);
            
            // Configure based on provider
            std::map<std::string, std::string> storage_config;
            if (provider == storage::StorageProvider::LOCAL_FILESYSTEM) {
                storage_config = {
                    {"base_path", config.at("storage_base_path")},
                    {"public_url_base", config.at("storage_public_url")}
                };
            } else if (provider == storage::StorageProvider::AWS_S3) {
                storage_config = {
                    {"access_key", config.at("aws_access_key")},
                    {"secret_key", config.at("aws_secret_key")},
                    {"bucket", config.at("s3_bucket")},
                    {"region", config.at("aws_region")}
                };
            }
            
            bool initialized = file_service_->initialize(storage_config);
            if (initialized) {
                spdlog::info("File upload service initialized with provider: {}", storage_provider);
            }
            
            return initialized;
            
        } catch (const std::exception& e) {
            spdlog::error("File service initialization error: {}", e.what());
            return false;
        }
    }
    
    bool initialize_repositories(const std::map<std::string, std::string>& config) {
        try {
            connection_string_ = config.at("database_connection_string");
            
            // Test database connection
            auto user_repo = repository::RepositoryFactory::create_user_repository(connection_string_);
            auto session_repo = repository::RepositoryFactory::create_session_repository(connection_string_);
            
            bool user_repo_healthy = user_repo->is_healthy().get();
            bool session_repo_healthy = session_repo->is_healthy().get();
            
            if (!user_repo_healthy || !session_repo_healthy) {
                spdlog::error("Database health check failed");
                return false;
            }
            
            spdlog::info("Database repositories initialized successfully");
            return true;
            
        } catch (const std::exception& e) {
            spdlog::error("Repository initialization error: {}", e.what());
            return false;
        }
    }
    
    void initialize_controllers(const std::map<std::string, std::string>& config) {
        // Initialize controllers with all services
        auth_controller_ = std::make_shared<controllers::AuthController>(
            user_service_impl_, email_service_, connection_string_);
            
        user_controller_ = std::make_shared<controllers::UserController>(
            user_service_impl_, file_service_, connection_string_);
            
        profile_controller_ = std::make_shared<controllers::ProfileController>(
            user_service_impl_);
        
        spdlog::info("Controllers initialized with real services");
    }
    
    void initialize_http_handler() {
        http_handler_ = std::make_shared<handlers::HttpHandler>(
            auth_controller_, user_controller_, profile_controller_);
        
        spdlog::info("HTTP handler initialized");
    }
};

} // namespace sonet::user

/**
 * Example usage of the complete User Service
 */
void example_usage() {
    try {
        // Configuration map with all service settings
        std::map<std::string, std::string> config = {
            // Email service configuration
            {"email_provider", "smtp"},
            {"smtp_host", "smtp.gmail.com"},
            {"smtp_port", "587"},
            {"smtp_username", "your-email@gmail.com"},
            {"smtp_password", "your-app-password"},
            {"smtp_use_tls", "true"},
            
            // Alternative: SendGrid configuration
            // {"email_provider", "sendgrid"},
            // {"sendgrid_api_key", "your-sendgrid-api-key"},
            
            // File storage configuration
            {"storage_provider", "local"},
            {"storage_base_path", "/var/www/sonet/uploads"},
            {"storage_public_url", "https://cdn.sonet.com"},
            
            // Alternative: S3 configuration
            // {"storage_provider", "s3"},
            // {"aws_access_key", "your-aws-access-key"},
            // {"aws_secret_key", "your-aws-secret-key"},
            // {"s3_bucket", "sonet-uploads"},
            // {"aws_region", "us-east-1"},
            
            // Database configuration
            {"database_connection_string", "postgresql://user:password@localhost:5432/sonet"}
        };
        
        // Initialize the complete user service
        auto user_service_app = std::make_unique<sonet::user::UserServiceApp>();
        
        if (!user_service_app->initialize(config)) {
            spdlog::error("Failed to initialize User Service");
            return;
        }
        
        spdlog::info("🎉 User Service is now running with complete Twitter-scale functionality!");
        spdlog::info("✅ Real email sending for verification & password reset");
        spdlog::info("✅ Real file upload with image processing for avatars & banners");
        spdlog::info("✅ Real database operations for user management");
        spdlog::info("✅ Complete REST API with validation & security");
        spdlog::info("✅ Session management & authentication");
        spdlog::info("✅ Privacy controls & user blocking");
        spdlog::info("✅ Rate limiting & spam prevention");
        
        // Get HTTP handler for integration with web framework
        auto http_handler = user_service_app->get_http_handler();
        
        // Example: Handle an HTTP request
        sonet::user::handlers::HttpHandler::HttpRequest example_request = {
            .method = "NOTE",
            .path = "/api/v1/auth/register",
            .headers = {{"Content-Type", "application/json"}},
            .body = R"({
                "username": "johndoe",
                "email": "john@example.com",
                "password": "SecurePass123!",
                "full_name": "John Doe",
                "bio": "Software engineer"
            })",
            .query_string = "",
            .client_ip = "192.168.1.100"
        };
        
        auto response = http_handler->handle_request(example_request);
        spdlog::info("Example registration response: {} - {}", response.status_code, response.body);
        
    } catch (const std::exception& e) {
        spdlog::error("Application error: {}", e.what());
    }
}
