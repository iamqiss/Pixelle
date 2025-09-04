#include "database_connection.h"
#include <libpq-fe.h>
#include <thread>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <unordered_map>

namespace sonet {
namespace database {

// DatabaseConnection implementation
DatabaseConnection::DatabaseConnection(pg_conn* conn)
    : conn_(conn), busy_(false), in_transaction_(false) {
    if (conn_) {
        // Set query timeout
        std::string timeout_cmd = "SET statement_timeout = '60s'";
        PGresult* result = PQexec(conn_, timeout_cmd.c_str());
        if (result) {
            PQclear(result);
        }
    }
}

DatabaseConnection::~DatabaseConnection() {
    if (conn_) {
        // Rollback any pending transaction
        if (in_transaction_) {
            rollback_transaction();
        }
        PQfinish(conn_);
    }
}

DatabaseConnection::DatabaseConnection(DatabaseConnection&& other) noexcept
    : conn_(other.conn_), busy_(other.busy_), 
      in_transaction_(other.in_transaction_), last_error_(std::move(other.last_error_)) {
    other.conn_ = nullptr;
    other.busy_ = false;
    other.in_transaction_ = false;
}

DatabaseConnection& DatabaseConnection::operator=(DatabaseConnection&& other) noexcept {
    if (this != &other) {
        if (conn_) {
            if (in_transaction_) {
                rollback_transaction();
            }
            PQfinish(conn_);
        }
        
        conn_ = other.conn_;
        busy_ = other.busy_;
        in_transaction_ = other.in_transaction_;
        last_error_ = std::move(other.last_error_);
        
        other.conn_ = nullptr;
        other.busy_ = false;
        other.in_transaction_ = false;
    }
    return *this;
}

bool DatabaseConnection::is_valid() const {
    return conn_ && PQstatus(conn_) == CONNECTION_OK;
}

bool DatabaseConnection::is_busy() const {
    return busy_;
}

void DatabaseConnection::mark_busy(bool busy) {
    busy_ = busy;
}

std::unique_ptr<pg_result> DatabaseConnection::execute_query(const std::string& query) {
    if (!is_valid()) {
        last_error_ = "Connection is not valid";
        return nullptr;
    }
    
    PGresult* result = PQexec(conn_, query.c_str());
    if (!result) {
        last_error_ = "Failed to execute query";
        return nullptr;
    }
    
    ExecStatusType status = PQresultStatus(result);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        last_error_ = PQresultErrorMessage(result);
        PQclear(result);
        return nullptr;
    }
    
    return std::unique_ptr<pg_result>(result);
}

std::unique_ptr<pg_result> DatabaseConnection::execute_prepared(
    const std::string& stmt_name, 
    const std::vector<std::string>& params) {
    
    if (!is_valid()) {
        last_error_ = "Connection is not valid";
        return nullptr;
    }
    
    // Convert params to const char* array
    std::vector<const char*> param_ptrs;
    for (const auto& param : params) {
        param_ptrs.push_back(param.c_str());
    }
    
    PGresult* result = PQexecPrepared(conn_, stmt_name.c_str(), 
                                     static_cast<int>(params.size()),
                                     param_ptrs.data(), nullptr, nullptr, 0);
    
    if (!result) {
        last_error_ = "Failed to execute prepared statement";
        return nullptr;
    }
    
    ExecStatusType status = PQresultStatus(result);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        last_error_ = PQresultErrorMessage(result);
        PQclear(result);
        return nullptr;
    }
    
    return std::unique_ptr<pg_result>(result);
}

bool DatabaseConnection::begin_transaction() {
    if (in_transaction_) {
        last_error_ = "Already in a transaction";
        return false;
    }
    
    auto result = execute_query("BEGIN");
    if (result) {
        in_transaction_ = true;
        return true;
    }
    return false;
}

bool DatabaseConnection::commit_transaction() {
    if (!in_transaction_) {
        last_error_ = "Not in a transaction";
        return false;
    }
    
    auto result = execute_query("COMMIT");
    if (result) {
        in_transaction_ = false;
        return true;
    }
    return false;
}

bool DatabaseConnection::rollback_transaction() {
    if (!in_transaction_) {
        return true; // Nothing to rollback
    }
    
    auto result = execute_query("ROLLBACK");
    if (result) {
        in_transaction_ = false;
        return true;
    }
    return false;
}

bool DatabaseConnection::is_in_transaction() const {
    return in_transaction_;
}

std::string DatabaseConnection::get_database_name() const {
    if (conn_) {
        const char* db_name = PQdb(conn_);
        return db_name ? db_name : "";
    }
    return "";
}

std::string DatabaseConnection::get_user_name() const {
    if (conn_) {
        const char* user_name = PQuser(conn_);
        return user_name ? user_name : "";
    }
    return "";
}

std::string DatabaseConnection::get_server_version() const {
    if (conn_) {
        int version = PQserverVersion(conn_);
        return std::to_string(version);
    }
    return "";
}

std::string DatabaseConnection::get_last_error() const {
    return last_error_;
}

void DatabaseConnection::clear_errors() {
    last_error_.clear();
}

// ConnectionPool implementation
ConnectionPool::ConnectionPool(const std::string& connection_string, 
                             const ConnectionPoolConfig& config)
    : connection_string_(connection_string), config_(config) {
    
    // Create initial connections
    for (size_t i = 0; i < config_.min_connections; ++i) {
        auto conn = create_connection();
        if (conn) {
            connections_.push_back(std::make_unique<PooledConnection>());
            connections_.back()->conn = std::move(conn);
            connections_.back()->last_used = std::chrono::steady_clock::now();
            connections_.back()->in_use = false;
            total_connections_++;
        }
    }
    
    // Start background threads
    cleanup_thread_ = std::thread(&ConnectionPool::cleanup_idle_connections, this);
    health_monitor_thread_ = std::thread(&ConnectionPool::monitor_pool_health, this);
}

ConnectionPool::~ConnectionPool() {
    shutdown();
}

std::unique_ptr<DatabaseConnection> ConnectionPool::get_connection() {
    std::unique_lock<std::mutex> lock(pool_mutex_);
    
    // Wait for available connection
    connection_available_.wait(lock, [this] {
        return shutdown_requested_ || 
               std::any_of(connections_.begin(), connections_.end(),
                          [](const auto& conn) { return !conn->in_use; });
    });
    
    if (shutdown_requested_) {
        return nullptr;
    }
    
    // Find available connection
    for (auto& pooled_conn : connections_) {
        if (!pooled_conn->in_use && pooled_conn->conn->is_valid()) {
            pooled_conn->in_use = true;
            pooled_conn->last_used = std::chrono::steady_clock::now();
            active_connections_++;
            return std::move(pooled_conn->conn);
        }
    }
    
    // Create new connection if under max limit
    if (total_connections_ < config_.max_connections) {
        auto new_conn = create_connection();
        if (new_conn) {
            total_connections_++;
            active_connections_++;
            return new_conn;
        }
    }
    
    return nullptr;
}

void ConnectionPool::return_connection(std::unique_ptr<DatabaseConnection> conn) {
    if (!conn) return;
    
    std::lock_guard<std::mutex> lock(pool_mutex_);
    
    // Find the connection in our pool
    for (auto& pooled_conn : connections_) {
        if (pooled_conn->conn.get() == conn.get()) {
            pooled_conn->conn = std::move(conn);
            pooled_conn->in_use = false;
            pooled_conn->last_used = std::chrono::steady_clock::now();
            active_connections_--;
            connection_available_.notify_one();
            return;
        }
    }
    
    // If not found in pool, just close it
    conn.reset();
    active_connections_--;
}

size_t ConnectionPool::get_active_connections() const {
    return active_connections_;
}

size_t ConnectionPool::get_idle_connections() const {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    size_t idle = 0;
    for (const auto& conn : connections_) {
        if (!conn->in_use) idle++;
    }
    return idle;
}

size_t ConnectionPool::get_total_connections() const {
    return total_connections_;
}

bool ConnectionPool::is_healthy() const {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    
    // Check if we have at least min_connections valid connections
    size_t valid_connections = 0;
    for (const auto& conn : connections_) {
        if (conn->conn && conn->conn->is_valid()) {
            valid_connections++;
        }
    }
    
    return valid_connections >= config_.min_connections;
}

void ConnectionPool::shutdown() {
    shutdown_requested_ = true;
    connection_available_.notify_all();
    
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    if (health_monitor_thread_.joinable()) {
        health_monitor_thread_.join();
    }
    
    // Close all connections
    std::lock_guard<std::mutex> lock(pool_mutex_);
    connections_.clear();
    total_connections_ = 0;
    active_connections_ = 0;
}

std::unique_ptr<DatabaseConnection> ConnectionPool::create_connection() {
    pg_conn* conn = PQconnectdb(connection_string_.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return nullptr;
    }
    
    return std::make_unique<DatabaseConnection>(conn);
}

void ConnectionPool::cleanup_idle_connections() {
    while (!shutdown_requested_) {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        auto now = std::chrono::steady_clock::now();
        auto it = connections_.begin();
        
        while (it != connections_.end()) {
            if (!(*it)->in_use && 
                (*it)->conn &&
                std::chrono::duration_cast<std::chrono::seconds>(
                    now - (*it)->last_used).count() > config_.max_idle_time) {
                
                it = connections_.erase(it);
                total_connections_--;
            } else {
                ++it;
            }
        }
    }
}

void ConnectionPool::monitor_pool_health() {
    while (!shutdown_requested_) {
        std::this_thread::sleep_for(std::chrono::seconds(60));
        
        // Check pool health and log if needed
        if (!is_healthy()) {
            std::cerr << "Warning: Connection pool health check failed" << std::endl;
        }
    }
}

// DatabaseManager implementation
DatabaseManager& DatabaseManager::get_instance() {
    static DatabaseManager instance;
    return instance;
}

bool DatabaseManager::initialize_service_pool(const std::string& service_name,
                                            const std::string& connection_string,
                                            const ConnectionPoolConfig& config) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    
    if (service_pools_.find(service_name) != service_pools_.end()) {
        return false; // Pool already exists
    }
    
    auto pool = std::make_unique<ConnectionPool>(connection_string, config);
    if (pool->is_healthy()) {
        service_pools_[service_name] = std::move(pool);
        return true;
    }
    
    return false;
}

ConnectionPool* DatabaseManager::get_service_pool(const std::string& service_name) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    
    auto it = service_pools_.find(service_name);
    if (it != service_pools_.end()) {
        return it->second.get();
    }
    
    return nullptr;
}

void DatabaseManager::shutdown_all() {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    
    for (auto& pool : service_pools_) {
        pool.second->shutdown();
    }
    service_pools_.clear();
}

// Utility functions implementation
namespace utils {

ConnectionParams parse_connection_string(const std::string& conn_str) {
    ConnectionParams params;
    
    // Simple parsing for postgresql://user:pass@host:port/db?sslmode=...
    size_t protocol_end = conn_str.find("://");
    if (protocol_end == std::string::npos) return params;
    
    size_t auth_start = protocol_end + 3;
    size_t at_pos = conn_str.find('@', auth_start);
    size_t slash_pos = conn_str.find('/', at_pos);
    size_t question_pos = conn_str.find('?', slash_pos);
    
    if (at_pos != std::string::npos && slash_pos != std::string::npos) {
        // Parse username:password
        std::string auth = conn_str.substr(auth_start, at_pos - auth_start);
        size_t colon_pos = auth.find(':');
        if (colon_pos != std::string::npos) {
            params.username = auth.substr(0, colon_pos);
            params.password = auth.substr(colon_pos + 1);
        } else {
            params.username = auth;
        }
        
        // Parse host:port
        std::string host_port = conn_str.substr(at_pos + 1, slash_pos - at_pos - 1);
        size_t port_colon = host_port.find(':');
        if (port_colon != std::string::npos) {
            params.host = host_port.substr(0, port_colon);
            params.port = std::stoi(host_port.substr(port_colon + 1));
        } else {
            params.host = host_port;
            params.port = 5432; // Default postgresql port
        }
        
        // Parse database name
        size_t db_end = (question_pos != std::string::npos) ? question_pos : conn_str.length();
        params.database = conn_str.substr(slash_pos + 1, db_end - slash_pos - 1);
        
        // Parse SSL mode
        if (question_pos != std::string::npos) {
            std::string query = conn_str.substr(question_pos + 1);
            size_t ssl_pos = query.find("sslmode=");
            if (ssl_pos != std::string::npos) {
                size_t ssl_start = ssl_pos + 8;
                size_t ssl_end = query.find('&', ssl_start);
                if (ssl_end == std::string::npos) {
                    ssl_end = query.length();
                }
                params.ssl_mode = query.substr(ssl_start, ssl_end - ssl_start);
            }
        }
    }
    
    return params;
}

std::string escape_string(const std::string& input) {
    // Simple escaping - in production, use PQescapeString
    std::string result;
    result.reserve(input.length() * 2);
    
    for (char c : input) {
        if (c == '\'') {
            result += "''";
        } else {
            result += c;
        }
    }
    
    return result;
}

std::string escape_identifier(const std::string& input) {
    // Escape postgresql identifiers
    std::string result = "\"";
    for (char c : input) {
        if (c == '"') {
            result += "\"\"";
        } else {
            result += c;
        }
    }
    result += "\"";
    return result;
}

std::string get_result_value(pg_result* result, int row, int col) {
    if (!result || row < 0 || col < 0 || 
        row >= PQntuples(result) || col >= PQnfields(result)) {
        return "";
    }
    
    const char* value = PQgetvalue(result, row, col);
    return value ? value : "";
}

int get_result_int(pg_result* result, int row, int col) {
    std::string value = get_result_value(result, row, col);
    return value.empty() ? 0 : std::stoi(value);
}

bool get_result_bool(pg_result* result, int row, int col) {
    std::string value = get_result_value(result, row, col);
    return value == "t" || value == "true" || value == "1";
}

std::vector<std::string> get_result_array(pg_result* result, int row, int col) {
    std::vector<std::string> array;
    std::string value = get_result_value(result, row, col);
    
    if (value.empty() || value == "{}") {
        return array;
    }
    
    // Simple array parsing - remove {} and split by comma
    if (value.length() > 2 && value[0] == '{' && value[value.length()-1] == '}') {
        value = value.substr(1, value.length() - 2);
        std::istringstream iss(value);
        std::string item;
        
        while (std::getline(iss, item, ',')) {
            // Remove quotes if present
            if (item.length() > 1 && item[0] == '"' && item[item.length()-1] == '"') {
                item = item.substr(1, item.length() - 2);
            }
            array.push_back(item);
        }
    }
    
    return array;
}

std::string uuid_to_string(const std::string& uuid) {
    // Remove hyphens if present
    std::string result = uuid;
    result.erase(std::remove(result.begin(), result.end(), '-'), result.end());
    return result;
}

std::string string_to_uuid(const std::string& str) {
    if (str.length() != 32) return str;
    
    // Add hyphens in UUID format: 8-4-4-4-12
    return str.substr(0, 8) + "-" + str.substr(8, 4) + "-" + 
           str.substr(12, 4) + "-" + str.substr(16, 4) + "-" + str.substr(20, 12);
}

std::string timestamp_to_string(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::tm* tm = std::gmtime(&time_t);
    
    char buffer[64];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm);
    return std::string(buffer);
}

std::chrono::system_clock::time_point string_to_timestamp(const std::string& str) {
    std::tm tm = {};
    std::istringstream ss(str);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    
    auto time_t = std::mktime(&tm);
    return std::chrono::system_clock::from_time_t(time_t);
}

} // namespace utils

} // namespace database
} // namespace sonet