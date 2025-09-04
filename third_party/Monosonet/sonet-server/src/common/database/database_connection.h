#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <unordered_map>

// Forward declarations
struct pg_conn;
struct pg_result;

namespace sonet {
namespace database {

// Connection pool configuration
struct ConnectionPoolConfig {
    size_t min_connections = 5;
    size_t max_connections = 20;
    size_t max_idle_time = 300; // 5 minutes
    size_t connection_timeout = 30; // 30 seconds
    size_t query_timeout = 60; // 60 seconds
    bool enable_ssl = false;
    std::string ssl_mode = "prefer";
};

// Database connection wrapper
class DatabaseConnection {
public:
    explicit DatabaseConnection(pg_conn* conn);
    ~DatabaseConnection();

    // Prevent copying
    DatabaseConnection(const DatabaseConnection&) = delete;
    DatabaseConnection& operator=(const DatabaseConnection&) = delete;

    // Allow moving
    DatabaseConnection(DatabaseConnection&& other) noexcept;
    DatabaseConnection& operator=(DatabaseConnection&& other) noexcept;

    // Connection status
    bool is_valid() const;
    bool is_busy() const;
    void mark_busy(bool busy);

    // Query execution
    std::unique_ptr<pg_result> execute_query(const std::string& query);
    std::unique_ptr<pg_result> execute_prepared(const std::string& stmt_name, 
                                               const std::vector<std::string>& params);

    // Transaction management
    bool begin_transaction();
    bool commit_transaction();
    bool rollback_transaction();
    bool is_in_transaction() const;

    // Connection info
    std::string get_database_name() const;
    std::string get_user_name() const;
    std::string get_server_version() const;

    // Error handling
    std::string get_last_error() const;
    void clear_errors();

private:
    pg_conn* conn_;
    bool busy_;
    bool in_transaction_;
    std::string last_error_;
};

// Connection pool
class ConnectionPool {
public:
    explicit ConnectionPool(const std::string& connection_string, 
                          const ConnectionPoolConfig& config = {});
    ~ConnectionPool();

    // Get connection from pool
    std::unique_ptr<DatabaseConnection> get_connection();
    
    // Return connection to pool
    void return_connection(std::unique_ptr<DatabaseConnection> conn);

    // Pool management
    size_t get_active_connections() const;
    size_t get_idle_connections() const;
    size_t get_total_connections() const;
    
    // Health check
    bool is_healthy() const;
    
    // Shutdown pool
    void shutdown();

private:
    struct PooledConnection {
        std::unique_ptr<DatabaseConnection> conn;
        std::chrono::steady_clock::time_point last_used;
        bool in_use;
    };

    // Connection management
    std::unique_ptr<DatabaseConnection> create_connection();
    void cleanup_idle_connections();
    void monitor_pool_health();

    // Pool state
    std::string connection_string_;
    ConnectionPoolConfig config_;
    std::vector<std::unique_ptr<PooledConnection>> connections_;
    
    // Synchronization
    mutable std::mutex pool_mutex_;
    std::condition_variable connection_available_;
    std::atomic<bool> shutdown_requested_{false};
    
    // Statistics
    std::atomic<size_t> active_connections_{0};
    std::atomic<size_t> total_connections_{0};
    
    // Background tasks
    std::thread cleanup_thread_;
    std::thread health_monitor_thread_;
};

// Database manager - main interface for services
class DatabaseManager {
public:
    static DatabaseManager& get_instance();
    
    // Initialize connection pools for different services
    bool initialize_service_pool(const std::string& service_name, 
                                const std::string& connection_string,
                                const ConnectionPoolConfig& config = {});
    
    // Get connection pool for a service
    ConnectionPool* get_service_pool(const std::string& service_name);
    
    // Shutdown all pools
    void shutdown_all();

private:
    DatabaseManager() = default;
    ~DatabaseManager() = default;
    
    // Prevent copying
    DatabaseManager(const DatabaseManager&) = delete;
    DatabaseManager& operator=(const DatabaseManager&) = delete;

    std::unordered_map<std::string, std::unique_ptr<ConnectionPool>> service_pools_;
    mutable std::mutex manager_mutex_;
};

// Utility functions
namespace utils {
    
    // Parse connection string
    struct ConnectionParams {
        std::string host;
        int port;
        std::string database;
        std::string username;
        std::string password;
        std::string ssl_mode;
    };
    
    ConnectionParams parse_connection_string(const std::string& conn_str);
    
    // SQL escaping and sanitization
    std::string escape_string(const std::string& input);
    std::string escape_identifier(const std::string& input);
    
    // Result set utilities
    std::string get_result_value(pg_result* result, int row, int col);
    int get_result_int(pg_result* result, int row, int col);
    bool get_result_bool(pg_result* result, int row, int col);
    std::vector<std::string> get_result_array(pg_result* result, int row, int col);
    
    // UUID utilities
    std::string uuid_to_string(const std::string& uuid);
    std::string string_to_uuid(const std::string& str);
    
    // Timestamp utilities
    std::string timestamp_to_string(const std::chrono::system_clock::time_point& tp);
    std::chrono::system_clock::time_point string_to_timestamp(const std::string& str);
}

} // namespace database
} // namespace sonet