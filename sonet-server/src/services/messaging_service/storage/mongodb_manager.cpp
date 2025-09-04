/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "include/mongodb_manager.hpp"
#include <cryptopp/sha.h>
#include <cryptopp/hex.h>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <filesystem>
#include <ctime>
#include <sodium.h>

namespace sonet::messaging::storage {

// EncryptionMetadata implementation
Json::Value EncryptionMetadata::to_json() const {
    Json::Value json;
    json["encryption_key_id"] = encryption_key_id;
    json["algorithm"] = algorithm;
    json["nonce"] = nonce;
    json["tag"] = tag;
    json["is_encrypted"] = is_encrypted;
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        expires_at.time_since_epoch()).count();
    return json;
}

EncryptionMetadata EncryptionMetadata::from_json(const Json::Value& json) {
    EncryptionMetadata meta;
    meta.encryption_key_id = json["encryption_key_id"].asString();
    meta.algorithm = json["algorithm"].asString();
    meta.nonce = json["nonce"].asString();
    meta.tag = json["tag"].asString();
    meta.is_encrypted = json["is_encrypted"].asBool();
    
    auto created_ms = json["created_at"].asInt64();
    meta.created_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(created_ms));
    
    auto expires_ms = json["expires_at"].asInt64();
    meta.expires_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(expires_ms));
    
    return meta;
}

// EncryptedBlob implementation
Json::Value EncryptedBlob::to_json() const {
    Json::Value json;
    json["blob_id"] = blob_id;
    json["collection_name"] = collection_name;
    json["document_id"] = document_id;
    json["field_name"] = field_name;
    json["encrypted_data"] = encrypted_data;
    json["encryption_meta"] = encryption_meta.to_json();
    json["checksum"] = checksum;
    json["size"] = static_cast<Json::UInt64>(size);
    json["content_type"] = content_type;
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["last_accessed"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_accessed.time_since_epoch()).count();
    json["access_count"] = access_count;
    
    Json::Value tags_json(Json::objectValue);
    for (const auto& [key, value] : tags) {
        tags_json[key] = value;
    }
    json["tags"] = tags_json;
    
    return json;
}

EncryptedBlob EncryptedBlob::from_json(const Json::Value& json) {
    EncryptedBlob blob;
    blob.blob_id = json["blob_id"].asString();
    blob.collection_name = json["collection_name"].asString();
    blob.document_id = json["document_id"].asString();
    blob.field_name = json["field_name"].asString();
    blob.encrypted_data = json["encrypted_data"].asString();
    blob.encryption_meta = EncryptionMetadata::from_json(json["encryption_meta"]);
    blob.checksum = json["checksum"].asString();
    blob.size = json["size"].asUInt64();
    blob.content_type = json["content_type"].asString();
    
    auto created_ms = json["created_at"].asInt64();
    blob.created_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(created_ms));
    
    auto accessed_ms = json["last_accessed"].asInt64();
    blob.last_accessed = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(accessed_ms));
    
    blob.access_count = json["access_count"].asUInt();
    
    const auto& tags_json = json["tags"];
    for (const auto& key : tags_json.getMemberNames()) {
        blob.tags[key] = tags_json[key].asString();
    }
    
    return blob;
}

bsoncxx::document::value EncryptedBlob::to_bson() const {
    using bsoncxx::builder::stream::document;
    using bsoncxx::builder::stream::open_document;
    using bsoncxx::builder::stream::close_document;
    using bsoncxx::builder::stream::finalize;
    
    auto builder = document{};
    
    builder << "blob_id" << blob_id
            << "collection_name" << collection_name
            << "document_id" << document_id
            << "field_name" << field_name
            << "encrypted_data" << encrypted_data
            << "checksum" << checksum
            << "size" << static_cast<int64_t>(size)
            << "content_type" << content_type
            << "access_count" << static_cast<int32_t>(access_count)
            << "created_at" << bsoncxx::types::b_date{created_at}
            << "last_accessed" << bsoncxx::types::b_date{last_accessed};
    
    // Add encryption metadata
    builder << "encryption_meta" << open_document
            << "encryption_key_id" << encryption_meta.encryption_key_id
            << "algorithm" << encryption_meta.algorithm
            << "nonce" << encryption_meta.nonce
            << "tag" << encryption_meta.tag
            << "is_encrypted" << encryption_meta.is_encrypted
            << "created_at" << bsoncxx::types::b_date{encryption_meta.created_at}
            << "expires_at" << bsoncxx::types::b_date{encryption_meta.expires_at}
            << close_document;
    
    // Add tags
    auto tags_doc = document{};
    for (const auto& [key, value] : tags) {
        tags_doc << key << value;
    }
    builder << "tags" << tags_doc;
    
    return builder << finalize;
}

EncryptedBlob EncryptedBlob::from_bson(const bsoncxx::document::view& doc) {
    EncryptedBlob blob;
    
    blob.blob_id = doc["blob_id"].get_utf8().value.to_string();
    blob.collection_name = doc["collection_name"].get_utf8().value.to_string();
    blob.document_id = doc["document_id"].get_utf8().value.to_string();
    blob.field_name = doc["field_name"].get_utf8().value.to_string();
    blob.encrypted_data = doc["encrypted_data"].get_utf8().value.to_string();
    blob.checksum = doc["checksum"].get_utf8().value.to_string();
    blob.size = static_cast<uint64_t>(doc["size"].get_int64().value);
    blob.content_type = doc["content_type"].get_utf8().value.to_string();
    blob.access_count = static_cast<uint32_t>(doc["access_count"].get_int32().value);
    
    blob.created_at = doc["created_at"].get_date().value;
    blob.last_accessed = doc["last_accessed"].get_date().value;
    
    // Parse encryption metadata
    auto meta_doc = doc["encryption_meta"].get_document().view();
    blob.encryption_meta.encryption_key_id = meta_doc["encryption_key_id"].get_utf8().value.to_string();
    blob.encryption_meta.algorithm = meta_doc["algorithm"].get_utf8().value.to_string();
    blob.encryption_meta.nonce = meta_doc["nonce"].get_utf8().value.to_string();
    blob.encryption_meta.tag = meta_doc["tag"].get_utf8().value.to_string();
    blob.encryption_meta.is_encrypted = meta_doc["is_encrypted"].get_bool().value;
    blob.encryption_meta.created_at = meta_doc["created_at"].get_date().value;
    blob.encryption_meta.expires_at = meta_doc["expires_at"].get_date().value;
    
    // Parse tags
    auto tags_doc = doc["tags"].get_document().view();
    for (const auto& element : tags_doc) {
        blob.tags[element.key().to_string()] = element.get_utf8().value.to_string();
    }
    
    return blob;
}

// MongoQuery implementation
MongoQuery& MongoQuery::where(const std::string& field, const std::string& value) {
    query_builder_ << field << value;
    return *this;
}

MongoQuery& MongoQuery::where_in(const std::string& field, const std::vector<std::string>& values) {
    using bsoncxx::builder::stream::open_array;
    using bsoncxx::builder::stream::close_array;
    
    auto array_builder = bsoncxx::builder::stream::array{};
    for (const auto& value : values) {
        array_builder << value;
    }
    
    query_builder_ << field << open_array << "$in" << array_builder << close_array;
    return *this;
}

MongoQuery& MongoQuery::where_range(const std::string& field, const std::string& min, const std::string& max) {
    using bsoncxx::builder::stream::open_document;
    using bsoncxx::builder::stream::close_document;
    
    query_builder_ << field << open_document
                   << "$gte" << min
                   << "$lte" << max
                   << close_document;
    return *this;
}

MongoQuery& MongoQuery::where_exists(const std::string& field, bool exists) {
    using bsoncxx::builder::stream::open_document;
    using bsoncxx::builder::stream::close_document;
    
    query_builder_ << field << open_document << "$exists" << exists << close_document;
    return *this;
}

MongoQuery& MongoQuery::where_regex(const std::string& field, const std::string& pattern) {
    using bsoncxx::builder::stream::open_document;
    using bsoncxx::builder::stream::close_document;
    
    query_builder_ << field << open_document
                   << "$regex" << pattern
                   << "$options" << "i"  // Case insensitive
                   << close_document;
    return *this;
}

MongoQuery& MongoQuery::sort(const std::string& field, bool ascending) {
    sort_builder_ << field << (ascending ? 1 : -1);
    return *this;
}

MongoQuery& MongoQuery::limit(uint32_t limit) {
    limit_ = limit;
    return *this;
}

MongoQuery& MongoQuery::skip(uint32_t skip) {
    skip_ = skip;
    return *this;
}

bsoncxx::document::value MongoQuery::build() const {
    using bsoncxx::builder::stream::finalize;
    return query_builder_ << finalize;
}

bsoncxx::document::value MongoQuery::build_sort() const {
    using bsoncxx::builder::stream::finalize;
    return sort_builder_ << finalize;
}

// ConnectionPoolStats implementation
Json::Value ConnectionPoolStats::to_json() const {
    Json::Value json;
    json["total_connections"] = total_connections;
    json["available_connections"] = available_connections;
    json["active_connections"] = active_connections;
    json["failed_connections"] = failed_connections;
    json["total_operations"] = static_cast<Json::UInt64>(total_operations);
    json["successful_operations"] = static_cast<Json::UInt64>(successful_operations);
    json["failed_operations"] = static_cast<Json::UInt64>(failed_operations);
    json["average_operation_time_ms"] = average_operation_time_ms;
    json["last_reset"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_reset.time_since_epoch()).count();
    return json;
}

void ConnectionPoolStats::reset() {
    total_connections = 0;
    available_connections = 0;
    active_connections = 0;
    failed_connections = 0;
    total_operations = 0;
    successful_operations = 0;
    failed_operations = 0;
    average_operation_time_ms = 0.0;
    last_reset = std::chrono::system_clock::now();
}

// MongoConfig implementation
Json::Value MongoConfig::to_json() const {
    Json::Value json;
    json["connection_string"] = connection_string;
    json["database_name"] = database_name;
    json["min_pool_size"] = min_pool_size;
    json["max_pool_size"] = max_pool_size;
    json["connection_timeout_ms"] = static_cast<int64_t>(connection_timeout.count());
    json["socket_timeout_ms"] = static_cast<int64_t>(socket_timeout.count());
    json["server_selection_timeout_ms"] = static_cast<int64_t>(server_selection_timeout.count());
    json["enable_ssl"] = enable_ssl;
    json["enable_compression"] = enable_compression;
    json["replica_set"] = replica_set;
    json["read_preference"] = read_preference;
    json["write_concern"] = write_concern;
    json["enable_retries"] = enable_retries;
    json["max_retries"] = max_retries;
    return json;
}

MongoConfig MongoConfig::from_json(const Json::Value& json) {
    MongoConfig config;
    config.connection_string = json.get("connection_string", "mongodb://localhost:27017").asString();
    config.database_name = json.get("database_name", "sonet_messaging").asString();
    config.min_pool_size = json.get("min_pool_size", 5).asUInt();
    config.max_pool_size = json.get("max_pool_size", 100).asUInt();
    config.connection_timeout = std::chrono::milliseconds(json.get("connection_timeout_ms", 30000).asInt64());
    config.socket_timeout = std::chrono::milliseconds(json.get("socket_timeout_ms", 60000).asInt64());
    config.server_selection_timeout = std::chrono::milliseconds(json.get("server_selection_timeout_ms", 30000).asInt64());
    config.enable_ssl = json.get("enable_ssl", false).asBool();
    config.enable_compression = json.get("enable_compression", true).asBool();
    config.replica_set = json.get("replica_set", "").asString();
    config.read_preference = json.get("read_preference", "primary").asString();
    config.write_concern = json.get("write_concern", "majority").asString();
    config.enable_retries = json.get("enable_retries", true).asBool();
    config.max_retries = json.get("max_retries", 3).asUInt();
    return config;
}

MongoConfig MongoConfig::default_config() {
    MongoConfig config;
    config.connection_string = "mongodb://localhost:27017";
    config.database_name = "sonet_messaging";
    config.min_pool_size = 5;
    config.max_pool_size = 100;
    config.connection_timeout = std::chrono::milliseconds(30000);
    config.socket_timeout = std::chrono::milliseconds(60000);
    config.server_selection_timeout = std::chrono::milliseconds(30000);
    config.enable_ssl = false;
    config.enable_compression = true;
    config.replica_set = "";
    config.read_preference = "primary";
    config.write_concern = "majority";
    config.enable_retries = true;
    config.max_retries = 3;
    return config;
}

// MongoDBManager implementation
MongoDBManager::MongoDBManager(const MongoConfig& config)
    : config_(config), initialized_(false), shutdown_requested_(false), background_running_(false) {
    stats_.reset();
}

MongoDBManager::~MongoDBManager() {
    shutdown();
}

bool MongoDBManager::initialize() {
    if (initialized_.load()) {
        return true;
    }
    
    try {
        log_info("Initializing MongoDB connection...");
        
        // Initialize MongoDB instance
        instance_ = std::make_unique<mongocxx::instance>();
        
        // Create connection pool
        mongocxx::uri uri{config_.connection_string};
        auto pool_options = mongocxx::options::pool{};
        
        pool_options.min_size(config_.min_pool_size);
        pool_options.max_size(config_.max_pool_size);
        
        pool_ = std::make_unique<mongocxx::pool>(uri, pool_options);
        
        // Test connection
        auto client = pool_->acquire();
        auto db = (*client)[config_.database_name];
        
        // Run a simple ping command
        auto ping_cmd = bsoncxx::builder::stream::document{} << "ping" << 1 << bsoncxx::builder::stream::finalize;
        db.run_command(ping_cmd.view());
        
        // Setup collections and indexes
        setup_indexes();
        
        // Start background threads
        background_running_ = true;
        maintenance_thread_ = std::thread([this]() { run_maintenance_loop(); });
        metrics_thread_ = std::thread([this]() { run_metrics_loop(); });
        
        initialized_ = true;
        log_info("MongoDB connection initialized successfully");
        
        return true;
        
    } catch (const std::exception& e) {
        log_error("Failed to initialize MongoDB: " + std::string(e.what()));
        return false;
    }
}

void MongoDBManager::shutdown() {
    if (!initialized_.load()) {
        return;
    }
    
    log_info("Shutting down MongoDB connection...");
    
    shutdown_requested_ = true;
    background_running_ = false;
    
    // Wait for background threads
    if (maintenance_thread_.joinable()) {
        maintenance_thread_.join();
    }
    
    if (metrics_thread_.joinable()) {
        metrics_thread_.join();
    }
    
    // Reset pool and instance
    pool_.reset();
    instance_.reset();
    
    initialized_ = false;
    log_info("MongoDB connection shut down");
}

bool MongoDBManager::is_connected() const {
    if (!initialized_.load()) {
        return false;
    }
    
    try {
        auto client = pool_->acquire();
        auto db = (*client)[config_.database_name];
        
        auto ping_cmd = bsoncxx::builder::stream::document{} << "ping" << 1 << bsoncxx::builder::stream::finalize;
        db.run_command(ping_cmd.view());
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

std::future<bool> MongoDBManager::store_encrypted_blob(const EncryptedBlob& blob) {
    return std::async(std::launch::async, [this, blob]() -> bool {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        try {
            auto client = pool_->acquire();
            auto collection = (*client)[config_.database_name]["encrypted_blobs"];
            
            auto doc = blob.to_bson();
            auto result = collection.insert_one(doc.view());
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            update_stats(result.has_value(), duration.count());
            
            return result.has_value();
            
        } catch (const std::exception& e) {
            handle_error("store_encrypted_blob", e);
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            update_stats(false, duration.count());
            
            return false;
        }
    });
}

std::future<std::optional<EncryptedBlob>> MongoDBManager::retrieve_blob(const std::string& blob_id) {
    return std::async(std::launch::async, [this, blob_id]() -> std::optional<EncryptedBlob> {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        try {
            auto client = pool_->acquire();
            auto collection = (*client)[config_.database_name]["encrypted_blobs"];
            
            auto filter = bsoncxx::builder::stream::document{} << "blob_id" << blob_id << bsoncxx::builder::stream::finalize;
            auto result = collection.find_one(filter.view());
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            if (result) {
                auto blob = EncryptedBlob::from_bson(result->view());
                
                // Update access count and time
                auto update_doc = bsoncxx::builder::stream::document{} 
                    << "$inc" << bsoncxx::builder::stream::open_document << "access_count" << 1 << bsoncxx::builder::stream::close_document
                    << "$set" << bsoncxx::builder::stream::open_document << "last_accessed" << bsoncxx::types::b_date{std::chrono::system_clock::now()} << bsoncxx::builder::stream::close_document
                    << bsoncxx::builder::stream::finalize;
                
                collection.update_one(filter.view(), update_doc.view());
                
                update_stats(true, duration.count());
                return blob;
            }
            
            update_stats(false, duration.count());
            return std::nullopt;
            
        } catch (const std::exception& e) {
            handle_error("retrieve_blob", e);
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            update_stats(false, duration.count());
            
            return std::nullopt;
        }
    });
}

mongocxx::collection MongoDBManager::get_collection(const std::string& collection_name) {
    auto client = pool_->acquire();
    return (*client)[config_.database_name][collection_name];
}

void MongoDBManager::setup_indexes() {
    try {
        auto collection = get_collection("encrypted_blobs");
        
        // Create unique index on blob_id
        auto blob_id_index = bsoncxx::builder::stream::document{} << "blob_id" << 1 << bsoncxx::builder::stream::finalize;
        collection.create_index(blob_id_index.view(), 
            mongocxx::options::index{}.unique(true));
        
        // Create compound index for queries
        auto compound_index = bsoncxx::builder::stream::document{} 
            << "collection_name" << 1 
            << "document_id" << 1 
            << "field_name" << 1 
            << bsoncxx::builder::stream::finalize;
        collection.create_index(compound_index.view());
        
        // Create TTL index for expiration
        auto ttl_index = bsoncxx::builder::stream::document{} << "encryption_meta.expires_at" << 1 << bsoncxx::builder::stream::finalize;
        collection.create_index(ttl_index.view(), 
            mongocxx::options::index{}.expire_after(std::chrono::seconds(0)));
        
        // Create text index for search
        auto text_index = bsoncxx::builder::stream::document{} 
            << "content_type" << "text"
            << "tags" << "text"
            << bsoncxx::builder::stream::finalize;
        collection.create_index(text_index.view());
        
        log_info("MongoDB indexes created successfully");
        
    } catch (const std::exception& e) {
        log_error("Failed to create indexes: " + std::string(e.what()));
    }
}

void MongoDBManager::run_maintenance_loop() {
    while (background_running_.load()) {
        try {
            cleanup_expired_blobs();
            optimize_indexes();
            
            std::this_thread::sleep_for(std::chrono::minutes(30));
            
        } catch (const std::exception& e) {
            log_error("Maintenance loop error: " + std::string(e.what()));
            std::this_thread::sleep_for(std::chrono::minutes(5));
        }
    }
}

void MongoDBManager::run_metrics_loop() {
    while (background_running_.load()) {
        try {
            if (metrics_callback_) {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                metrics_callback_(stats_);
            }
            
            std::this_thread::sleep_for(std::chrono::seconds(30));
            
        } catch (const std::exception& e) {
            log_error("Metrics loop error: " + std::string(e.what()));
            std::this_thread::sleep_for(std::chrono::minutes(1));
        }
    }
}

void MongoDBManager::update_stats(bool success, double operation_time_ms) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    stats_.total_operations++;
    if (success) {
        stats_.successful_operations++;
    } else {
        stats_.failed_operations++;
    }
    
    // Update average operation time (exponential moving average)
    double alpha = 0.1;
    stats_.average_operation_time_ms = 
        alpha * operation_time_ms + (1.0 - alpha) * stats_.average_operation_time_ms;
}

std::string MongoDBManager::generate_blob_id() {
    if (sodium_init() < 0) {
        std::stringstream ss; ss << "blob_" << std::hex << std::time(nullptr);
        return ss.str();
    }
    unsigned char buf[16];
    randombytes_buf(buf, sizeof(buf));
    std::stringstream ss;
    ss << "blob_";
    for (unsigned char b : buf) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(b);
    }
    return ss.str();
}

void MongoDBManager::handle_error(const std::string& operation, const std::exception& e) {
    std::string error_msg = "MongoDB operation '" + operation + "' failed: " + e.what();
    log_error(error_msg);
    
    if (error_callback_) {
        error_callback_(error_msg);
    }
}

void MongoDBManager::log_info(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cout << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [MongoDB] [INFO] " << message << std::endl;
}

void MongoDBManager::log_warning(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cout << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [MongoDB] [WARN] " << message << std::endl;
}

void MongoDBManager::log_error(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cerr << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [MongoDB] [ERROR] " << message << std::endl;
}

void MongoDBManager::cleanup_expired_blobs() {
    try {
        auto collection = get_collection("encrypted_blobs");
        
        auto filter = bsoncxx::builder::stream::document{} 
            << "encryption_meta.expires_at" << bsoncxx::builder::stream::open_document
            << "$lt" << bsoncxx::types::b_date{std::chrono::system_clock::now()}
            << bsoncxx::builder::stream::close_document
            << bsoncxx::builder::stream::finalize;
        
        auto result = collection.delete_many(filter.view());
        
        if (result && result->deleted_count() > 0) {
            log_info("Cleaned up " + std::to_string(result->deleted_count()) + " expired blobs");
        }
        
    } catch (const std::exception& e) {
        log_error("Failed to cleanup expired blobs: " + std::string(e.what()));
    }
}

ConnectionPoolStats MongoDBManager::get_pool_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

// MongoUtils implementation
std::string MongoUtils::calculate_blob_checksum(const std::string& data) {
    CryptoPP::SHA256 hash;
    std::string digest;
    
    CryptoPP::StringSource ss(data, true,
        new CryptoPP::HashFilter(hash,
            new CryptoPP::HexEncoder(new CryptoPP::StringSink(digest))));
    
    return digest;
}

bool MongoUtils::verify_blob_integrity(const EncryptedBlob& blob) {
    std::string calculated_checksum = calculate_blob_checksum(blob.encrypted_data);
    return calculated_checksum == blob.checksum;
}

bool MongoUtils::validate_blob_id(const std::string& blob_id) {
    return !blob_id.empty() && blob_id.length() >= 10 && blob_id.length() <= 100;
}

bool MongoUtils::validate_collection_name(const std::string& collection_name) {
    if (collection_name.empty() || collection_name.length() > 127) {
        return false;
    }
    
    // Check for invalid characters
    for (char c : collection_name) {
        if (!std::isalnum(c) && c != '_' && c != '-') {
            return false;
        }
    }
    
    return true;
}

std::string MongoUtils::sanitize_field_name(const std::string& field_name) {
    std::string sanitized = field_name;
    
    // Replace invalid characters with underscores
    std::replace_if(sanitized.begin(), sanitized.end(), 
        [](char c) { return !std::isalnum(c) && c != '_'; }, '_');
    
    // Ensure it doesn't start with a number
    if (!sanitized.empty() && std::isdigit(sanitized[0])) {
        sanitized = "_" + sanitized;
    }
    
    return sanitized;
}

} // namespace sonet::messaging::storage