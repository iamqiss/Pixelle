/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Implementation of the main search service orchestrator for Twitter-scale operations.
 * This coordinates all search components with lifecycle management, health monitoring,
 * and comprehensive service orchestration.
 */

#include "service.h"
#include <algorithm>
#include <sstream>
#include <random>
#include <chrono>
#include <future>
#include <fstream>
#include <iomanip>
#include <thread>
#ifdef HAVE_GRPC
#include <grpcpp/grpcpp.h>
#include "search.grpc.pb.h"

class SearchGrpcService final : public sonet::search::SearchService::Service {
public:
    explicit SearchGrpcService(std::shared_ptr<sonet::search_service::controllers::SearchController> controller)
        : controller_(std::move(controller)) {}

    grpc::Status SearchUsers(grpc::ServerContext* context,
                             const sonet::search::SearchUserRequest* request,
                             sonet::search::SearchUserResponse* response) override {
        sonet::search_service::controllers::SearchRequestContext ctx;
        ctx.user_id = ""; // TODO: extract from metadata
        auto q = sonet::search_service::models::SearchQuery{request->query()};
        auto result = controller_->search_users(q, ctx);
        if (!result.success) {
            return grpc::Status(grpc::StatusCode::INTERNAL, result.message);
        }
        // Map to proto
        for (const auto& item : result.search_result->users) {
            auto* u = response->add_users();
            u->set_user_id(item.user_id);
            u->set_username(item.username);
            u->set_display_name(item.display_name);
            u->set_avatar_url(item.avatar_url);
            u->set_is_verified(item.is_verified);
        }
        return grpc::Status::OK;
    }

    grpc::Status SearchNotes(grpc::ServerContext* context,
                             const sonet::search::SearchNoteRequest* request,
                             sonet::search::SearchNoteResponse* response) override {
        sonet::search_service::controllers::SearchRequestContext ctx;
        ctx.user_id = ""; // TODO: extract from metadata
        auto q = sonet::search_service::models::SearchQuery{request->query()};
        auto result = controller_->search_notes(q, ctx);
        if (!result.success) {
            return grpc::Status(grpc::StatusCode::INTERNAL, result.message);
        }
        for (const auto& note : result.search_result->notes) {
            auto* n = response->add_notes();
            n->set_note_id(note.id);
            n->set_author_id(note.author_id);
            n->set_content(note.content);
        }
        return grpc::Status::OK;
    }

private:
    std::shared_ptr<sonet::search_service::controllers::SearchController> controller_;
};
#endif

namespace sonet {
namespace search_service {

/**
 * SearchServiceConfig implementation
 */
SearchServiceConfig SearchServiceConfig::from_file(const std::string& config_path) {
    SearchServiceConfig config;
    
    std::ifstream file(config_path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open config file: " + config_path);
    }
    
    nlohmann::json json_config;
    file >> json_config;
    
    return from_json(json_config);
}

SearchServiceConfig SearchServiceConfig::from_json(const nlohmann::json& json) {
    SearchServiceConfig config;
    
    // Service configuration
    config.service_name = json.value("service_name", "sonet-search-service");
    config.service_version = json.value("service_version", "1.0.0");
    config.environment = json.value("environment", "development");
    config.log_level = json.value("log_level", "INFO");
    config.debug_mode = json.value("debug_mode", false);
    
    // Network configuration
    if (json.contains("network")) {
        const auto& network = json["network"];
        config.http_port = network.value("http_port", 8080);
        config.grpc_port = network.value("grpc_port", 9090);
        config.metrics_port = network.value("metrics_port", 8081);
        config.health_port = network.value("health_port", 8082);
        config.bind_address = network.value("bind_address", "0.0.0.0");
        config.max_connections = network.value("max_connections", 1000);
        config.request_timeout_seconds = network.value("request_timeout_seconds", 30);
    }
    
    // Elasticsearch configuration
    if (json.contains("elasticsearch")) {
        const auto& es = json["elasticsearch"];
        if (es.contains("hosts") && es["hosts"].is_array()) {
            config.elasticsearch_hosts = es["hosts"].get<std::vector<std::string>>();
        }
        config.elasticsearch_username = es.value("username", "");
        config.elasticsearch_password = es.value("password", "");
        config.elasticsearch_use_ssl = es.value("use_ssl", false);
        config.elasticsearch_verify_certs = es.value("verify_certs", true);
        config.elasticsearch_connection_timeout_ms = es.value("connection_timeout_ms", 5000);
        config.elasticsearch_request_timeout_ms = es.value("request_timeout_ms", 30000);
        config.elasticsearch_max_retries = es.value("max_retries", 3);
    }
    
    // Redis configuration
    if (json.contains("redis")) {
        const auto& redis = json["redis"];
        config.redis_host = redis.value("host", "localhost");
        config.redis_port = redis.value("port", 6379);
        config.redis_password = redis.value("password", "");
        config.redis_database = redis.value("database", 0);
        config.redis_connection_timeout_ms = redis.value("connection_timeout_ms", 5000);
        config.redis_socket_timeout_ms = redis.value("socket_timeout_ms", 5000);
        config.redis_max_connections = redis.value("max_connections", 100);
    }
    
    // Message queue configuration
    if (json.contains("message_queue")) {
        const auto& mq = json["message_queue"];
        config.message_queue_type = mq.value("type", "redis");
        config.message_queue_hosts = mq.value("hosts", std::vector<std::string>{"localhost:9092"});
        config.message_queue_username = mq.value("username", "");
        config.message_queue_password = mq.value("password", "");
        config.enable_real_time_indexing = mq.value("enable_real_time_indexing", true);
        config.indexing_batch_size = mq.value("indexing_batch_size", 1000);
        config.indexing_batch_timeout_ms = mq.value("indexing_batch_timeout_ms", 5000);
    }
    
    // Rate limiting configuration
    if (json.contains("rate_limiting")) {
        const auto& rl = json["rate_limiting"];
        config.enable_rate_limiting = rl.value("enabled", true);
        config.default_rate_limit_rpm = rl.value("default_rpm", 100);
        config.authenticated_rate_limit_rpm = rl.value("authenticated_rpm", 1000);
        config.premium_rate_limit_rpm = rl.value("premium_rpm", 10000);
        config.rate_limit_burst_capacity = rl.value("burst_capacity", 50);
    }
    
    // Caching configuration
    if (json.contains("caching")) {
        const auto& cache = json["caching"];
        config.enable_caching = cache.value("enabled", true);
        config.cache_ttl_seconds = cache.value("ttl_seconds", 300);
        config.cache_max_size = cache.value("max_size", 10000);
        config.cache_compression = cache.value("compression", true);
    }
    
    // Monitoring configuration
    if (json.contains("monitoring")) {
        const auto& mon = json["monitoring"];
        config.enable_metrics = mon.value("enable_metrics", true);
        config.enable_tracing = mon.value("enable_tracing", true);
        config.metrics_collection_interval_seconds = mon.value("metrics_interval_seconds", 60);
        config.health_check_interval_seconds = mon.value("health_check_interval_seconds", 30);
        config.prometheus_endpoint = mon.value("prometheus_endpoint", "/metrics");
        config.jaeger_endpoint = mon.value("jaeger_endpoint", "http://localhost:14268/api/traces");
    }
    
    // Feature flags
    if (json.contains("features")) {
        const auto& features = json["features"];
        config.enable_real_time_search = features.value("real_time_search", false);
        config.enable_ai_ranking = features.value("ai_ranking", false);
        config.enable_personalization = features.value("personalization", true);
        config.enable_trending_analysis = features.value("trending_analysis", true);
        config.enable_spam_detection = features.value("spam_detection", true);
        config.enable_content_analysis = features.value("content_analysis", true);
    }
    
    return config;
}

SearchServiceConfig SearchServiceConfig::production_config() {
    SearchServiceConfig config;
    
    // Production defaults
    config.service_name = "sonet-search-service";
    config.service_version = "1.0.0";
    config.environment = "production";
    config.log_level = "INFO";
    config.debug_mode = false;
    
    // Network
    config.http_port = 8080;
    config.grpc_port = 9090;
    config.metrics_port = 8081;
    config.health_port = 8082;
    config.bind_address = "0.0.0.0";
    config.max_connections = 10000;
    config.request_timeout_seconds = 30;
    
    // Elasticsearch
    config.elasticsearch_hosts = {"elasticsearch-cluster:9200"};
    config.elasticsearch_use_ssl = true;
    config.elasticsearch_verify_certs = true;
    config.elasticsearch_connection_timeout_ms = 5000;
    config.elasticsearch_request_timeout_ms = 30000;
    config.elasticsearch_max_retries = 3;
    
    // Redis
    config.redis_host = "redis-cluster";
    config.redis_port = 6379;
    config.redis_database = 0;
    config.redis_connection_timeout_ms = 5000;
    config.redis_socket_timeout_ms = 5000;
    config.redis_max_connections = 200;
    
    // Message queue
    config.message_queue_type = "kafka";
    config.message_queue_hosts = {"kafka-cluster:9092"};
    config.enable_real_time_indexing = true;
    config.indexing_batch_size = 5000;
    config.indexing_batch_timeout_ms = 2000;
    
    // Rate limiting
    config.enable_rate_limiting = true;
    config.default_rate_limit_rpm = 100;
    config.authenticated_rate_limit_rpm = 1000;
    config.premium_rate_limit_rpm = 10000;
    config.rate_limit_burst_capacity = 100;
    
    // Caching
    config.enable_caching = true;
    config.cache_ttl_seconds = 300;
    config.cache_max_size = 100000;
    config.cache_compression = true;
    
    // Monitoring
    config.enable_metrics = true;
    config.enable_tracing = true;
    config.metrics_collection_interval_seconds = 60;
    config.health_check_interval_seconds = 30;
    config.prometheus_endpoint = "/metrics";
    config.jaeger_endpoint = "http://jaeger-collector:14268/api/traces";
    
    // Features
    config.enable_real_time_search = true;
    config.enable_ai_ranking = true;
    config.enable_personalization = true;
    config.enable_trending_analysis = true;
    config.enable_spam_detection = true;
    config.enable_content_analysis = true;
    
    return config;
}

SearchServiceConfig SearchServiceConfig::development_config() {
    SearchServiceConfig config;
    
    // Development defaults
    config.service_name = "sonet-search-service-dev";
    config.service_version = "1.0.0-dev";
    config.environment = "development";
    config.log_level = "DEBUG";
    config.debug_mode = true;
    
    // Network
    config.http_port = 8080;
    config.grpc_port = 9090;
    config.metrics_port = 8081;
    config.health_port = 8082;
    config.bind_address = "127.0.0.1";
    config.max_connections = 100;
    config.request_timeout_seconds = 60;
    
    // Elasticsearch
    config.elasticsearch_hosts = {"localhost:9200"};
    config.elasticsearch_use_ssl = false;
    config.elasticsearch_verify_certs = false;
    config.elasticsearch_connection_timeout_ms = 10000;
    config.elasticsearch_request_timeout_ms = 60000;
    config.elasticsearch_max_retries = 1;
    
    // Redis
    config.redis_host = "localhost";
    config.redis_port = 6379;
    config.redis_database = 1;  // Use different database for dev
    config.redis_connection_timeout_ms = 10000;
    config.redis_socket_timeout_ms = 10000;
    config.redis_max_connections = 10;
    
    // Message queue
    config.message_queue_type = "redis";
    config.message_queue_hosts = {"localhost:6379"};
    config.enable_real_time_indexing = true;
    config.indexing_batch_size = 100;
    config.indexing_batch_timeout_ms = 10000;
    
    // Rate limiting (more lenient for development)
    config.enable_rate_limiting = false;
    config.default_rate_limit_rpm = 1000;
    config.authenticated_rate_limit_rpm = 10000;
    config.premium_rate_limit_rpm = 100000;
    config.rate_limit_burst_capacity = 500;
    
    // Caching
    config.enable_caching = true;
    config.cache_ttl_seconds = 60;  // Shorter TTL for development
    config.cache_max_size = 1000;
    config.cache_compression = false;
    
    // Monitoring
    config.enable_metrics = true;
    config.enable_tracing = false;  // Disable tracing for development
    config.metrics_collection_interval_seconds = 30;
    config.health_check_interval_seconds = 15;
    config.prometheus_endpoint = "/metrics";
    config.jaeger_endpoint = "http://localhost:14268/api/traces";
    
    // Features (enable all for testing)
    config.enable_real_time_search = true;
    config.enable_ai_ranking = true;
    config.enable_personalization = true;
    config.enable_trending_analysis = true;
    config.enable_spam_detection = true;
    config.enable_content_analysis = true;
    
    return config;
}

bool SearchServiceConfig::is_valid() const {
    // Validate required fields
    if (service_name.empty() || service_version.empty()) {
        return false;
    }
    
    // Validate ports
    if (http_port <= 0 || http_port > 65535 ||
        grpc_port <= 0 || grpc_port > 65535 ||
        metrics_port <= 0 || metrics_port > 65535 ||
        health_port <= 0 || health_port > 65535) {
        return false;
    }
    
    // Validate Elasticsearch configuration
    if (elasticsearch_hosts.empty()) {
        return false;
    }
    
    // Validate timeouts
    if (request_timeout_seconds <= 0 ||
        elasticsearch_connection_timeout_ms <= 0 ||
        elasticsearch_request_timeout_ms <= 0) {
        return false;
    }
    
    return true;
}

nlohmann::json SearchServiceConfig::to_json() const {
    return nlohmann::json{
        {"service_name", service_name},
        {"service_version", service_version},
        {"environment", environment},
        {"log_level", log_level},
        {"debug_mode", debug_mode},
        {"network", {
            {"http_port", http_port},
            {"grpc_port", grpc_port},
            {"metrics_port", metrics_port},
            {"health_port", health_port},
            {"bind_address", bind_address},
            {"max_connections", max_connections},
            {"request_timeout_seconds", request_timeout_seconds}
        }},
        {"elasticsearch", {
            {"hosts", elasticsearch_hosts},
            {"username", elasticsearch_username},
            {"password", "***"},  // Don't expose password
            {"use_ssl", elasticsearch_use_ssl},
            {"verify_certs", elasticsearch_verify_certs},
            {"connection_timeout_ms", elasticsearch_connection_timeout_ms},
            {"request_timeout_ms", elasticsearch_request_timeout_ms},
            {"max_retries", elasticsearch_max_retries}
        }},
        {"redis", {
            {"host", redis_host},
            {"port", redis_port},
            {"password", "***"},  // Don't expose password
            {"database", redis_database},
            {"connection_timeout_ms", redis_connection_timeout_ms},
            {"socket_timeout_ms", redis_socket_timeout_ms},
            {"max_connections", redis_max_connections}
        }},
        {"message_queue", {
            {"type", message_queue_type},
            {"hosts", message_queue_hosts},
            {"username", message_queue_username},
            {"password", "***"},  // Don't expose password
            {"enable_real_time_indexing", enable_real_time_indexing},
            {"indexing_batch_size", indexing_batch_size},
            {"indexing_batch_timeout_ms", indexing_batch_timeout_ms}
        }},
        {"rate_limiting", {
            {"enabled", enable_rate_limiting},
            {"default_rpm", default_rate_limit_rpm},
            {"authenticated_rpm", authenticated_rate_limit_rpm},
            {"premium_rpm", premium_rate_limit_rpm},
            {"burst_capacity", rate_limit_burst_capacity}
        }},
        {"caching", {
            {"enabled", enable_caching},
            {"ttl_seconds", cache_ttl_seconds},
            {"max_size", cache_max_size},
            {"compression", cache_compression}
        }},
        {"monitoring", {
            {"enable_metrics", enable_metrics},
            {"enable_tracing", enable_tracing},
            {"metrics_interval_seconds", metrics_collection_interval_seconds},
            {"health_check_interval_seconds", health_check_interval_seconds},
            {"prometheus_endpoint", prometheus_endpoint},
            {"jaeger_endpoint", jaeger_endpoint}
        }},
        {"features", {
            {"real_time_search", enable_real_time_search},
            {"ai_ranking", enable_ai_ranking},
            {"personalization", enable_personalization},
            {"trending_analysis", enable_trending_analysis},
            {"spam_detection", enable_spam_detection},
            {"content_analysis", enable_content_analysis}
        }}
    };
}

/**
 * ComponentStatus implementation
 */
nlohmann::json ComponentStatus::to_json() const {
    return nlohmann::json{
        {"name", name},
        {"status", status_to_string(status)},
        {"health", health_to_string(health)},
        {"message", message},
        {"last_check", std::chrono::duration_cast<std::chrono::milliseconds>(last_check.time_since_epoch()).count()},
        {"uptime_seconds", std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start_time).count()},
        {"details", details}
    };
}

std::string ComponentStatus::status_to_string(ServiceStatus status) {
    switch (status) {
        case ServiceStatus::STOPPED: return "STOPPED";
        case ServiceStatus::STARTING: return "STARTING";
        case ServiceStatus::RUNNING: return "RUNNING";
        case ServiceStatus::STOPPING: return "STOPPING";
        case ServiceStatus::ERROR: return "ERROR";
        default: return "UNKNOWN";
    }
}

std::string ComponentStatus::health_to_string(ServiceHealth health) {
    switch (health) {
        case ServiceHealth::HEALTHY: return "HEALTHY";
        case ServiceHealth::DEGRADED: return "DEGRADED";
        case ServiceHealth::UNHEALTHY: return "UNHEALTHY";
        case ServiceHealth::UNKNOWN: return "UNKNOWN";
        default: return "UNKNOWN";
    }
}

/**
 * ServiceMetrics implementation
 */
nlohmann::json ServiceMetrics::to_json() const {
    auto now = std::chrono::system_clock::now();
    auto uptime_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
    
    return nlohmann::json{
        {"start_time", std::chrono::duration_cast<std::chrono::milliseconds>(start_time.time_since_epoch()).count()},
        {"uptime_seconds", uptime_seconds},
        {"total_requests", total_requests.load()},
        {"successful_requests", successful_requests.load()},
        {"failed_requests", failed_requests.load()},
        {"note_searches", note_searches.load()},
        {"user_searches", user_searches.load()},
        {"trending_requests", trending_requests.load()},
        {"suggestion_requests", suggestion_requests.load()},
        {"notes_indexed", notes_indexed.load()},
        {"users_indexed", users_indexed.load()},
        {"cache_hits", cache_hits.load()},
        {"cache_misses", cache_misses.load()},
        {"elasticsearch_requests", elasticsearch_requests.load()},
        {"elasticsearch_errors", elasticsearch_errors.load()},
        {"redis_operations", redis_operations.load()},
        {"redis_errors", redis_errors.load()},
        {"message_queue_messages_sent", message_queue_messages_sent.load()},
        {"message_queue_messages_received", message_queue_messages_received.load()},
        {"message_queue_errors", message_queue_errors.load()},
        {"active_connections", active_connections.load()},
        {"current_memory_usage_mb", current_memory_usage_mb.load()},
        {"current_cpu_usage_percent", current_cpu_usage_percent.load()},
        {"average_response_time_ms", get_average_response_time_ms()},
        {"success_rate", get_success_rate()},
        {"cache_hit_rate", get_cache_hit_rate()},
        {"elasticsearch_success_rate", get_elasticsearch_success_rate()}
    };
}

void ServiceMetrics::reset() {
    total_requests = 0;
    successful_requests = 0;
    failed_requests = 0;
    note_searches = 0;
    user_searches = 0;
    trending_requests = 0;
    suggestion_requests = 0;
    notes_indexed = 0;
    users_indexed = 0;
    cache_hits = 0;
    cache_misses = 0;
    elasticsearch_requests = 0;
    elasticsearch_errors = 0;
    redis_operations = 0;
    redis_errors = 0;
    message_queue_messages_sent = 0;
    message_queue_messages_received = 0;
    message_queue_errors = 0;
    active_connections = 0;
    current_memory_usage_mb = 0;
    current_cpu_usage_percent = 0.0f;
    total_response_time_ms = 0;
}

double ServiceMetrics::get_success_rate() const {
    long total = total_requests.load();
    if (total == 0) return 0.0;
    
    return static_cast<double>(successful_requests.load()) / total;
}

double ServiceMetrics::get_cache_hit_rate() const {
    long total_cache_requests = cache_hits.load() + cache_misses.load();
    if (total_cache_requests == 0) return 0.0;
    
    return static_cast<double>(cache_hits.load()) / total_cache_requests;
}

double ServiceMetrics::get_elasticsearch_success_rate() const {
    long total = elasticsearch_requests.load();
    if (total == 0) return 0.0;
    
    long successful = total - elasticsearch_errors.load();
    return static_cast<double>(successful) / total;
}

double ServiceMetrics::get_average_response_time_ms() const {
    long total = total_requests.load();
    if (total == 0) return 0.0;
    
    return static_cast<double>(total_response_time_ms.load()) / total;
}

/**
 * HealthMonitor implementation
 */
HealthMonitor::HealthMonitor(const SearchServiceConfig& config) : config_(config) {
    overall_status_.name = "SearchService";
    overall_status_.status = ServiceStatus::STOPPED;
    overall_status_.health = ServiceHealth::UNKNOWN;
    overall_status_.start_time = std::chrono::system_clock::now();
}

HealthMonitor::~HealthMonitor() {
    stop_monitoring();
}

void HealthMonitor::start_monitoring() {
    if (monitoring_active_.exchange(true)) {
        return;  // Already monitoring
    }
    
    monitoring_thread_ = std::thread([this]() {
        while (monitoring_active_) {
            perform_health_checks();
            
            auto interval = std::chrono::seconds{config_.health_check_interval_seconds};
            std::this_thread::sleep_for(interval);
        }
    });
}

void HealthMonitor::stop_monitoring() {
    monitoring_active_ = false;
    
    if (monitoring_thread_.joinable()) {
        monitoring_thread_.join();
    }
}

void HealthMonitor::register_component(const std::string& name, HealthChecker checker) {
    std::lock_guard<std::mutex> lock(mutex_);
    health_checkers_[name] = std::move(checker);
    
    // Initialize component status
    ComponentStatus status;
    status.name = name;
    status.status = ServiceStatus::STOPPED;
    status.health = ServiceHealth::UNKNOWN;
    status.start_time = std::chrono::system_clock::now();
    component_statuses_[name] = status;
}

ComponentStatus HealthMonitor::get_component_status(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = component_statuses_.find(name);
    if (it != component_statuses_.end()) {
        return it->second;
    }
    
    ComponentStatus status;
    status.name = name;
    status.status = ServiceStatus::ERROR;
    status.health = ServiceHealth::UNKNOWN;
    status.message = "Component not found";
    return status;
}

ComponentStatus HealthMonitor::get_overall_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return overall_status_;
}

nlohmann::json HealthMonitor::get_health_report() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    nlohmann::json report;
    report["overall"] = overall_status_.to_json();
    
    nlohmann::json components = nlohmann::json::array();
    for (const auto& pair : component_statuses_) {
        components.push_back(pair.second.to_json());
    }
    report["components"] = components;
    
    return report;
}

void HealthMonitor::update_component_status(const std::string& name, ServiceStatus status, ServiceHealth health, const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = component_statuses_.find(name);
    if (it != component_statuses_.end()) {
        it->second.status = status;
        it->second.health = health;
        it->second.message = message;
        it->second.last_check = std::chrono::system_clock::now();
    }
    
    // Update overall status
    update_overall_status();
}

void HealthMonitor::perform_health_checks() {
    std::vector<std::future<void>> futures;
    
    {
        std::lock_guard<std::mutex> lock(mutex_);
        
        for (const auto& pair : health_checkers_) {
            const std::string& name = pair.first;
            const HealthChecker& checker = pair.second;
            
            auto future = std::async(std::launch::async, [this, name, checker]() {
                try {
                    auto [health, message] = checker();
                    update_component_status(name, ServiceStatus::RUNNING, health, message);
                } catch (const std::exception& e) {
                    update_component_status(name, ServiceStatus::ERROR, ServiceHealth::UNHEALTHY, e.what());
                }
            });
            
            futures.push_back(std::move(future));
        }
    }
    
    // Wait for all health checks to complete
    for (auto& future : futures) {
        future.wait();
    }
}

void HealthMonitor::update_overall_status() {
    ServiceHealth overall_health = ServiceHealth::HEALTHY;
    ServiceStatus overall_status = ServiceStatus::RUNNING;
    std::vector<std::string> issues;
    
    for (const auto& pair : component_statuses_) {
        const ComponentStatus& status = pair.second;
        
        if (status.status == ServiceStatus::ERROR || status.status == ServiceStatus::STOPPED) {
            overall_status = ServiceStatus::ERROR;
        }
        
        if (status.health == ServiceHealth::UNHEALTHY) {
            overall_health = ServiceHealth::UNHEALTHY;
            issues.push_back(status.name + ": " + status.message);
        } else if (status.health == ServiceHealth::DEGRADED && overall_health == ServiceHealth::HEALTHY) {
            overall_health = ServiceHealth::DEGRADED;
            issues.push_back(status.name + ": " + status.message);
        }
    }
    
    overall_status_.status = overall_status;
    overall_status_.health = overall_health;
    overall_status_.last_check = std::chrono::system_clock::now();
    
    if (!issues.empty()) {
        std::ostringstream oss;
        for (size_t i = 0; i < issues.size(); ++i) {
            if (i > 0) oss << "; ";
            oss << issues[i];
        }
        overall_status_.message = oss.str();
    } else {
        overall_status_.message = "All components healthy";
    }
}

/**
 * MessageQueueSubscriber implementation
 */
MessageQueueSubscriber::MessageQueueSubscriber(const SearchServiceConfig& config) : config_(config) {
}

MessageQueueSubscriber::~MessageQueueSubscriber() {
    stop_consuming();
}

bool MessageQueueSubscriber::start_consuming() {
    if (consuming_active_.exchange(true)) {
        return false;  // Already consuming
    }
    
    // Start consumer threads
    for (const auto& topic : topics_) {
        consumer_threads_.emplace_back([this, topic]() {
            consume_messages(topic);
        });
    }
    
    return true;
}

void MessageQueueSubscriber::stop_consuming() {
    consuming_active_ = false;
    
    for (auto& thread : consumer_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    
    consumer_threads_.clear();
}

void MessageQueueSubscriber::subscribe(const std::string& topic, MessageHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    message_handlers_[topic] = std::move(handler);
    topics_.push_back(topic);
}

void MessageQueueSubscriber::consume_messages(const std::string& topic) {
    while (consuming_active_) {
        try {
            // Simulate message consumption
            // In real implementation, this would connect to Kafka/Redis/etc.
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            
            // Mock message processing
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = message_handlers_.find(topic);
            if (it != message_handlers_.end()) {
                // Create mock message
                nlohmann::json message = {
                    {"type", "note_created"},
                    {"id", "test_note_" + std::to_string(std::time(nullptr))},
                    {"user_id", "test_user"},
                    {"content", "This is a test note"}
                };
                
                // Process message
                it->second(message);
            }
            
        } catch (const std::exception& e) {
            // Log error and continue
            std::this_thread::sleep_for(std::chrono::seconds{1});
        }
    }
}

nlohmann::json MessageQueueSubscriber::get_statistics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return nlohmann::json{
        {"topics", topics_},
        {"handlers", message_handlers_.size()},
        {"active", consuming_active_.load()},
        {"threads", consumer_threads_.size()}
    };
}

/**
 * ServiceDiscoveryClient implementation
 */
ServiceDiscoveryClient::ServiceDiscoveryClient(const SearchServiceConfig& config) : config_(config) {
    service_info_.service_name = config.service_name;
    service_info_.service_version = config.service_version;
    service_info_.host = config.bind_address;
    service_info_.http_port = config.http_port;
    service_info_.grpc_port = config.grpc_port;
    service_info_.health_endpoint = "http://" + config.bind_address + ":" + std::to_string(config.health_port) + "/health";
    service_info_.metrics_endpoint = "http://" + config.bind_address + ":" + std::to_string(config.metrics_port) + "/metrics";
    service_info_.registration_time = std::chrono::system_clock::now();
}

ServiceDiscoveryClient::~ServiceDiscoveryClient() {
    unregister_service();
}

bool ServiceDiscoveryClient::register_service() {
    // In real implementation, this would register with Consul, etcd, etc.
    service_info_.last_heartbeat = std::chrono::system_clock::now();
    registered_ = true;
    return true;
}

bool ServiceDiscoveryClient::unregister_service() {
    // In real implementation, this would unregister from service discovery
    registered_ = false;
    return true;
}

bool ServiceDiscoveryClient::send_heartbeat() {
    if (!registered_) return false;
    
    service_info_.last_heartbeat = std::chrono::system_clock::now();
    return true;
}

std::vector<ServiceInfo> ServiceDiscoveryClient::discover_services(const std::string& service_name) {
    // In real implementation, this would query service discovery
    std::vector<ServiceInfo> services;
    
    // Mock response
    if (service_name == "elasticsearch") {
        ServiceInfo es_service;
        es_service.service_name = "elasticsearch";
        es_service.host = "localhost";
        es_service.http_port = 9200;
        services.push_back(es_service);
    }
    
    return services;
}

ServiceInfo ServiceDiscoveryClient::get_service_info() const {
    return service_info_;
}

void ServiceDiscoveryClient::start_heartbeat() {
    // No-op placeholder for future implementation
}

void ServiceDiscoveryClient::stop_heartbeat() {
    // No-op placeholder for future implementation
}

/**
 * SearchService::Impl - Private implementation
 */
struct SearchService::Impl {
    SearchServiceConfig config;
    
    // Core components
    std::shared_ptr<engines::ElasticsearchEngine> elasticsearch_engine;
    std::shared_ptr<indexers::NoteIndexer> note_indexer;
    std::shared_ptr<indexers::UserIndexer> user_indexer;
    std::shared_ptr<controllers::SearchController> search_controller;
    
    // Service components
    std::unique_ptr<HealthMonitor> health_monitor;
    std::unique_ptr<MessageQueueSubscriber> message_queue_subscriber;
    std::unique_ptr<ServiceDiscoveryClient> service_discovery_client;
    
    // State
    std::atomic<ServiceStatus> service_status{ServiceStatus::STOPPED};
    std::atomic<bool> shutdown_requested{false};
    
    // Metrics
    mutable std::mutex metrics_mutex;
    ServiceMetrics metrics;
    
    // Threads
    std::thread metrics_collection_thread;
    std::thread heartbeat_thread;
    
    // Callbacks
    std::vector<std::function<void(ServiceStatus, ServiceStatus)>> status_change_callbacks;
    std::vector<std::function<void(const ServiceMetrics&)>> metrics_callbacks;
    std::vector<std::function<void(const nlohmann::json&)>> health_check_callbacks;
    
    Impl(const SearchServiceConfig& cfg) : config(cfg) {
        metrics.start_time = std::chrono::system_clock::now();
    }
    
    ~Impl() {
        if (service_status != ServiceStatus::STOPPED) {
            stop_service();
        }
    }
    
    void stop_service() {
        shutdown_requested = true;
        
        // Stop background threads
        if (metrics_collection_thread.joinable()) {
            metrics_collection_thread.join();
        }
        
        if (heartbeat_thread.joinable()) {
            heartbeat_thread.join();
        }
        
        // Stop components
        if (health_monitor) {
            health_monitor->stop_monitoring();
        }
        
        if (message_queue_subscriber) {
            message_queue_subscriber->stop_consuming();
        }
        
        if (service_discovery_client) {
            service_discovery_client->unregister_service();
        }
        
        if (note_indexer && note_indexer->is_running()) {
            note_indexer->stop();
        }
        
        if (user_indexer && user_indexer->is_running()) {
            user_indexer->stop();
        }
    }
};

/**
 * SearchService implementation
 */
SearchService::SearchService(const SearchServiceConfig& config)
    : pimpl_(std::make_unique<Impl>(config)) {
}

SearchService::~SearchService() = default;

std::future<bool> SearchService::initialize() {
    return std::async(std::launch::async, [this]() {
        try {
            auto old_status = pimpl_->service_status.exchange(ServiceStatus::STARTING);
            notify_status_change(old_status, ServiceStatus::STARTING);
            
            // Initialize Elasticsearch engine
            engines::ElasticsearchConfig es_config;
            es_config.hosts = pimpl_->config.elasticsearch_hosts;
            es_config.username = pimpl_->config.elasticsearch_username;
            es_config.password = pimpl_->config.elasticsearch_password;
            es_config.use_ssl = pimpl_->config.elasticsearch_use_ssl;
            es_config.verify_ssl = pimpl_->config.elasticsearch_verify_certs;
            es_config.connection_timeout_ms = pimpl_->config.elasticsearch_connection_timeout_ms;
            es_config.request_timeout_ms = pimpl_->config.elasticsearch_request_timeout_ms;
            es_config.max_retries = pimpl_->config.elasticsearch_max_retries;
            
            pimpl_->elasticsearch_engine = std::make_shared<engines::ElasticsearchEngine>(es_config);
            auto es_init_future = pimpl_->elasticsearch_engine->initialize();
            if (!es_init_future.get()) {
                throw std::runtime_error("Failed to initialize Elasticsearch engine");
            }
            
            // Initialize indexers
            indexers::IndexingConfig indexing_config;
            indexing_config.batch_size = pimpl_->config.indexing_batch_size;
            indexing_config.batch_timeout = std::chrono::milliseconds{pimpl_->config.indexing_batch_timeout_ms};
            indexing_config.enable_real_time_indexing = pimpl_->config.enable_real_time_indexing;
            
            pimpl_->note_indexer = std::make_shared<indexers::NoteIndexer>(pimpl_->elasticsearch_engine, indexing_config);
            pimpl_->user_indexer = std::make_shared<indexers::UserIndexer>(pimpl_->elasticsearch_engine, indexing_config);
            
            // Initialize search controller
            controllers::SearchControllerConfig controller_config;
            controller_config.enable_rate_limiting = pimpl_->config.enable_rate_limiting;
            controller_config.authenticated_rate_limit_rpm = pimpl_->config.authenticated_rate_limit_rpm;
            controller_config.authenticated_burst_capacity = pimpl_->config.rate_limit_burst_capacity;
            controller_config.enable_caching = pimpl_->config.enable_caching;
            controller_config.cache_ttl_minutes = pimpl_->config.cache_ttl_seconds / 60;
            controller_config.cache_max_size = pimpl_->config.cache_max_size;
            
            pimpl_->search_controller = std::make_shared<controllers::SearchController>(pimpl_->elasticsearch_engine, controller_config);
            
            // Initialize health monitor
            pimpl_->health_monitor = std::make_unique<HealthMonitor>(pimpl_->config);
            
            // Register health checkers
            pimpl_->health_monitor->register_component("elasticsearch", [this]() -> std::pair<ServiceHealth, std::string> {
                try {
                    auto health_future = pimpl_->elasticsearch_engine->check_health();
                    bool healthy = health_future.get();
                    return {healthy ? ServiceHealth::HEALTHY : ServiceHealth::UNHEALTHY, 
                            healthy ? "Elasticsearch cluster is healthy" : "Elasticsearch cluster is unhealthy"};
                } catch (const std::exception& e) {
                    return {ServiceHealth::UNHEALTHY, std::string("Elasticsearch error: ") + e.what()};
                }
            });
            
            pimpl_->health_monitor->register_component("note_indexer", [this]() -> std::pair<ServiceHealth, std::string> {
                if (!pimpl_->note_indexer || !pimpl_->note_indexer->is_running()) {
                    return {ServiceHealth::UNHEALTHY, "Note indexer is not running"};
                }
                
                auto metrics = pimpl_->note_indexer->get_metrics();
                int queue_size = pimpl_->note_indexer->get_queue_size();
                
                if (queue_size > 100000) {
                    return {ServiceHealth::DEGRADED, "Note indexer queue is growing: " + std::to_string(queue_size) + " items"};
                }
                
                double success_rate = metrics.get_success_rate();
                if (success_rate < 0.95) {
                    return {ServiceHealth::DEGRADED, "Note indexer success rate is low: " + std::to_string(success_rate * 100) + "%"};
                }
                
                return {ServiceHealth::HEALTHY, "Note indexer is healthy"};
            });
            
            pimpl_->health_monitor->register_component("user_indexer", [this]() -> std::pair<ServiceHealth, std::string> {
                if (!pimpl_->user_indexer || !pimpl_->user_indexer->is_running()) {
                    return {ServiceHealth::UNHEALTHY, "User indexer is not running"};
                }
                
                auto metrics = pimpl_->user_indexer->get_metrics();
                int queue_size = pimpl_->user_indexer->get_queue_size();
                
                if (queue_size > 10000) {
                    return {ServiceHealth::DEGRADED, "User indexer queue is growing: " + std::to_string(queue_size) + " items"};
                }
                
                double success_rate = metrics.get_success_rate();
                if (success_rate < 0.95) {
                    return {ServiceHealth::DEGRADED, "User indexer success rate is low: " + std::to_string(success_rate * 100) + "%"};
                }
                
                return {ServiceHealth::HEALTHY, "User indexer is healthy"};
            });
            
            // Initialize message queue subscriber
            if (pimpl_->config.enable_real_time_indexing) {
                pimpl_->message_queue_subscriber = std::make_unique<MessageQueueSubscriber>(pimpl_->config);
                
                // Subscribe to note events
                pimpl_->message_queue_subscriber->subscribe("note_events", [this](const nlohmann::json& message) {
                    try {
                        std::string event_type = message.value("type", "");
                        
                        if (event_type == "note_created" || event_type == "note_updated") {
                            // Convert message to NoteDocument and queue for indexing
                            auto note = indexers::NoteDocument::from_json(message);
                            pimpl_->note_indexer->queue_note_for_indexing(note);
                        } else if (event_type == "note_deleted") {
                            // Handle note deletion
                            std::string note_id = message.value("id", "");
                            if (!note_id.empty()) {
                                // Queue for deletion
                                indexers::NoteDocument note;
                                note.id = note_id;
                                pimpl_->note_indexer->queue_note_for_indexing(note);
                            }
                        }
                        
                        pimpl_->metrics.message_queue_messages_received++;
                    } catch (const std::exception& e) {
                        pimpl_->metrics.message_queue_errors++;
                    }
                });
                
                // Subscribe to user events
                pimpl_->message_queue_subscriber->subscribe("user_events", [this](const nlohmann::json& message) {
                    try {
                        std::string event_type = message.value("type", "");
                        
                        if (event_type == "user_created" || event_type == "user_updated") {
                            // Convert message to UserDocument and queue for indexing
                            auto user = indexers::UserDocument::from_json(message);
                            pimpl_->user_indexer->queue_user_for_indexing(user);
                        } else if (event_type == "user_deleted") {
                            // Handle user deletion
                            std::string user_id = message.value("id", "");
                            if (!user_id.empty()) {
                                // Queue for deletion
                                indexers::UserDocument user;
                                user.id = user_id;
                                pimpl_->user_indexer->queue_user_for_indexing(user);
                            }
                        }
                        
                        pimpl_->metrics.message_queue_messages_received++;
                    } catch (const std::exception& e) {
                        pimpl_->metrics.message_queue_errors++;
                    }
                });
            }
            
            // Initialize service discovery
            pimpl_->service_discovery_client = std::make_unique<ServiceDiscoveryClient>(pimpl_->config);
            
            return true;
            
        } catch (const std::exception& e) {
            pimpl_->service_status = ServiceStatus::ERROR;
            notify_status_change(ServiceStatus::STARTING, ServiceStatus::ERROR);
            return false;
        }
    });
}

std::future<bool> SearchService::start() {
    return std::async(std::launch::async, [this]() {
        try {
            if (pimpl_->service_status != ServiceStatus::STARTING) {
                return false;
            }
            
            // Start indexers
            auto note_indexer_future = pimpl_->note_indexer->start();
            auto user_indexer_future = pimpl_->user_indexer->start();
            
            if (!note_indexer_future.get() || !user_indexer_future.get()) {
                throw std::runtime_error("Failed to start indexers");
            }
            
            // Start health monitoring
            pimpl_->health_monitor->start_monitoring();
            
            // Start message queue subscriber
            if (pimpl_->message_queue_subscriber) {
                pimpl_->message_queue_subscriber->start_consuming();
            }
            
            // Register with service discovery
            if (pimpl_->service_discovery_client) {
                pimpl_->service_discovery_client->register_service();
            }
            
            // Start background threads
            start_background_threads();
            
            auto old_status = pimpl_->service_status.exchange(ServiceStatus::RUNNING);
            notify_status_change(old_status, ServiceStatus::RUNNING);
            
            return true;
            
        } catch (const std::exception& e) {
            pimpl_->service_status = ServiceStatus::ERROR;
            return false;
        }
    });
}

void SearchService::stop() {
    auto old_status = pimpl_->service_status.exchange(ServiceStatus::STOPPING);
    notify_status_change(old_status, ServiceStatus::STOPPING);
    
    pimpl_->stop_service();
    
    pimpl_->service_status = ServiceStatus::STOPPED;
    notify_status_change(ServiceStatus::STOPPING, ServiceStatus::STOPPED);
}

ServiceStatus SearchService::get_status() const {
    return pimpl_->service_status.load();
}

ComponentStatus SearchService::get_health() const {
    if (pimpl_->health_monitor) {
        return pimpl_->health_monitor->get_overall_status();
    }
    
    ComponentStatus status;
    status.name = "SearchService";
    status.status = get_status();
    status.health = ServiceHealth::UNKNOWN;
    status.message = "Health monitor not available";
    return status;
}

ServiceMetrics SearchService::get_metrics() const {
    std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
    return pimpl_->metrics;
}

nlohmann::json SearchService::get_detailed_health_report() const {
    if (pimpl_->health_monitor) {
        return pimpl_->health_monitor->get_health_report();
    }
    
    return nlohmann::json{
        {"error", "Health monitor not available"}
    };
}

SearchServiceConfig SearchService::get_config() const {
    return pimpl_->config;
}

void SearchService::update_config(const SearchServiceConfig& new_config) {
    pimpl_->config = new_config;
    // Note: Some config changes may require service restart
}

bool SearchService::is_feature_enabled(const std::string& feature_name) const {
    if (feature_name == "real_time_search") return pimpl_->config.enable_real_time_search;
    if (feature_name == "ai_ranking") return pimpl_->config.enable_ai_ranking;
    if (feature_name == "personalization") return pimpl_->config.enable_personalization;
    if (feature_name == "trending_analysis") return pimpl_->config.enable_trending_analysis;
    if (feature_name == "spam_detection") return pimpl_->config.enable_spam_detection;
    if (feature_name == "content_analysis") return pimpl_->config.enable_content_analysis;
    return false;
}

void SearchService::enable_feature(const std::string& feature_name, bool enabled) {
    if (feature_name == "real_time_search") pimpl_->config.enable_real_time_search = enabled;
    else if (feature_name == "ai_ranking") pimpl_->config.enable_ai_ranking = enabled;
    else if (feature_name == "personalization") pimpl_->config.enable_personalization = enabled;
    else if (feature_name == "trending_analysis") pimpl_->config.enable_trending_analysis = enabled;
    else if (feature_name == "spam_detection") pimpl_->config.enable_spam_detection = enabled;
    else if (feature_name == "content_analysis") pimpl_->config.enable_content_analysis = enabled;
}

void SearchService::register_status_change_callback(std::function<void(ServiceStatus, ServiceStatus)> callback) {
    pimpl_->status_change_callbacks.push_back(std::move(callback));
}

void SearchService::register_metrics_callback(std::function<void(const ServiceMetrics&)> callback) {
    pimpl_->metrics_callbacks.push_back(std::move(callback));
}

void SearchService::register_health_check_callback(std::function<void(const nlohmann::json&)> callback) {
    pimpl_->health_check_callbacks.push_back(std::move(callback));
}

void SearchService::perform_maintenance() {
    // Clear caches
    if (pimpl_->search_controller) {
        pimpl_->search_controller->clear_cache();
    }
    
    // Trigger garbage collection in Elasticsearch
    if (pimpl_->elasticsearch_engine) {
        // Implementation would trigger ES garbage collection
    }
    
    // Update trending data
    // Implementation would refresh trending caches
}

std::shared_ptr<engines::ElasticsearchEngine> SearchService::get_elasticsearch_engine() const {
    return pimpl_->elasticsearch_engine;
}

std::shared_ptr<indexers::NoteIndexer> SearchService::get_note_indexer() const {
    return pimpl_->note_indexer;
}

std::shared_ptr<indexers::UserIndexer> SearchService::get_user_indexer() const {
    return pimpl_->user_indexer;
}

std::shared_ptr<controllers::SearchController> SearchService::get_search_controller() const {
    return pimpl_->search_controller;
}

void SearchService::start_background_threads() {
    // Start metrics collection thread
    pimpl_->metrics_collection_thread = std::thread([this]() {
        while (!pimpl_->shutdown_requested && pimpl_->service_status == ServiceStatus::RUNNING) {
            collect_metrics();
            
            auto interval = std::chrono::seconds{pimpl_->config.metrics_collection_interval_seconds};
            std::this_thread::sleep_for(interval);
        }
    });
    
    // Start heartbeat thread
    pimpl_->heartbeat_thread = std::thread([this]() {
        while (!pimpl_->shutdown_requested && pimpl_->service_status == ServiceStatus::RUNNING) {
            if (pimpl_->service_discovery_client) {
                pimpl_->service_discovery_client->send_heartbeat();
            }
            
            std::this_thread::sleep_for(std::chrono::seconds{30});  // Heartbeat every 30 seconds
        }
    });
}

void SearchService::collect_metrics() {
    std::lock_guard<std::mutex> lock(pimpl_->metrics_mutex);
    
    // Collect metrics from indexers
    if (pimpl_->note_indexer) {
        auto note_metrics = pimpl_->note_indexer->get_metrics();
        pimpl_->metrics.notes_indexed = note_metrics.notes_indexed.load();
    }
    
    if (pimpl_->user_indexer) {
        auto user_metrics = pimpl_->user_indexer->get_metrics();
        pimpl_->metrics.users_indexed = user_metrics.users_indexed.load();
    }
    
    // Collect metrics from search controller
    if (pimpl_->search_controller) {
        auto controller_metrics = pimpl_->search_controller->get_metrics();
        pimpl_->metrics.total_requests = controller_metrics.total_requests.load();
        pimpl_->metrics.successful_requests = controller_metrics.successful_requests.load();
        pimpl_->metrics.failed_requests = controller_metrics.failed_requests.load();
        pimpl_->metrics.note_searches = controller_metrics.note_searches.load();
        pimpl_->metrics.user_searches = controller_metrics.user_searches.load();
        pimpl_->metrics.cache_hits = controller_metrics.cache_hits.load();
        pimpl_->metrics.cache_misses = controller_metrics.cache_misses.load();
    }
    
    // Notify metrics callbacks
    for (const auto& callback : pimpl_->metrics_callbacks) {
        try {
            callback(pimpl_->metrics);
        } catch (const std::exception& e) {
            // Log error but continue
        }
    }
}

void SearchService::notify_status_change(ServiceStatus old_status, ServiceStatus new_status) {
    for (const auto& callback : pimpl_->status_change_callbacks) {
        try {
            callback(old_status, new_status);
        } catch (const std::exception& e) {
            // Log error but continue
        }
    }
}

} // namespace search_service
} // namespace sonet