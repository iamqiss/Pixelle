#pragma once

#include "database_connection.h"
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <functional>

namespace sonet {
namespace database {

// Base repository class that all service repositories inherit from
class BaseRepository {
public:
    explicit BaseRepository(ConnectionPool* pool);
    virtual ~BaseRepository() = default;

    // Prevent copying
    BaseRepository(const BaseRepository&) = delete;
    BaseRepository& operator=(const BaseRepository&) = delete;

    // Allow moving
    BaseRepository(BaseRepository&&) = default;
    BaseRepository& operator=(BaseRepository&&) = default;

    // Transaction management
    bool execute_in_transaction(std::function<bool(DatabaseConnection*)> operation);
    bool execute_in_transaction_with_rollback(std::function<bool(DatabaseConnection*)> operation);

    // Connection management
    std::unique_ptr<DatabaseConnection> get_connection();
    void return_connection(std::unique_ptr<DatabaseConnection> conn);

    // Health check
    bool is_healthy() const;

protected:
    // Common query execution methods
    std::unique_ptr<pg_result> execute_query(const std::string& query);
    std::unique_ptr<pg_result> execute_prepared(const std::string& stmt_name, 
                                               const std::vector<std::string>& params);
    
    // Result set utilities
    std::string get_result_value(pg_result* result, int row, int col);
    int get_result_int(pg_result* result, int row, int col);
    bool get_result_bool(pg_result* result, int row, int col);
    std::vector<std::string> get_result_array(pg_result* result, int row, int col);
    
    // Utility methods
    std::string escape_string(const std::string& input);
    std::string escape_identifier(const std::string& input);
    
    // UUID utilities
    std::string uuid_to_string(const std::string& uuid);
    std::string string_to_uuid(const std::string& str);
    
    // Timestamp utilities
    std::string timestamp_to_string(const std::chrono::system_clock::time_point& tp);
    std::chrono::system_clock::time_point string_to_timestamp(const std::string& str);

private:
    ConnectionPool* pool_;
};

// Transaction scope class for RAII-style transaction management
class TransactionScope {
public:
    explicit TransactionScope(DatabaseConnection* conn);
    ~TransactionScope();

    // Prevent copying
    TransactionScope(const TransactionScope&) = delete;
    TransactionScope& operator=(const TransactionScope&) = delete;

    // Allow moving
    TransactionScope(TransactionScope&&) = default;
    TransactionScope& operator=(TransactionScope&&) = default;

    // Transaction control
    bool commit();
    void rollback();

    // Status
    bool is_active() const { return active_; }

private:
    DatabaseConnection* conn_;
    bool active_;
    bool committed_;
};

// Query builder for dynamic SQL construction
class QueryBuilder {
public:
    QueryBuilder& select(const std::vector<std::string>& columns);
    QueryBuilder& from(const std::string& table);
    QueryBuilder& where(const std::string& condition);
    QueryBuilder& and_where(const std::string& condition);
    QueryBuilder& or_where(const std::string& condition);
    QueryBuilder& order_by(const std::string& column, bool ascending = true);
    QueryBuilder& limit(size_t limit);
    QueryBuilder& offset(size_t offset);
    QueryBuilder& group_by(const std::vector<std::string>& columns);
    QueryBuilder& having(const std::string& condition);

    // Build the final query
    std::string build() const;

    // Clear the builder
    void clear();

private:
    std::vector<std::string> select_columns_;
    std::string from_table_;
    std::vector<std::string> where_conditions_;
    std::vector<std::string> order_by_clauses_;
    std::vector<std::string> group_by_columns_;
    std::string having_condition_;
    size_t limit_value_ = 0;
    size_t offset_value_ = 0;
};

// Result wrapper for easier data access
class ResultRow {
public:
    explicit ResultRow(pg_result* result, int row);

    // Column access by name
    std::string get_string(const std::string& column_name) const;
    int get_int(const std::string& column_name) const;
    bool get_bool(const std::string& column_name) const;
    std::vector<std::string> get_array(const std::string& column_name) const;
    
    // Column access by index
    std::string get_string(int column_index) const;
    int get_int(int column_index) const;
    bool get_bool(int column_index) const;
    std::vector<std::string> get_array(int column_index) const;

    // Check if column is null
    bool is_null(const std::string& column_name) const;
    bool is_null(int column_index) const;

    // Get column count
    int get_column_count() const;

private:
    pg_result* result_;
    int row_;
    std::unordered_map<std::string, int> column_map_;
};

// Result set wrapper
class ResultSet {
public:
    explicit ResultSet(std::unique_ptr<pg_result> result);

    // Iteration
    class Iterator {
    public:
        Iterator(pg_result* result, int row);
        Iterator& operator++();
        ResultRow operator*();
        bool operator!=(const Iterator& other) const;

    private:
        pg_result* result_;
        int row_;
    };

    Iterator begin();
    Iterator end();

    // Access by index
    ResultRow operator[](int index) const;

    // Size and status
    size_t size() const;
    bool empty() const;
    bool has_error() const;
    std::string get_error() const;

private:
    std::unique_ptr<pg_result> result_;
    std::string error_message_;
};

} // namespace database
} // namespace sonet