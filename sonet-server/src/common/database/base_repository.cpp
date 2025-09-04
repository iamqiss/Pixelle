#include "base_repository.h"
#include <sstream>
#include <algorithm>
#include <cctype>
#include <iomanip>

namespace sonet {
namespace database {

// BaseRepository implementation
BaseRepository::BaseRepository(ConnectionPool* pool) : pool_(pool) {
    if (!pool_) {
        throw std::invalid_argument("Connection pool cannot be null");
    }
}

bool BaseRepository::execute_in_transaction(std::function<bool(DatabaseConnection*)> operation) {
    auto conn = get_connection();
    if (!conn) {
        return false;
    }

    if (!conn->begin_transaction()) {
        return_connection(std::move(conn));
        return false;
    }

    bool success = operation(conn.get());
    
    if (success) {
        success = conn->commit_transaction();
    } else {
        conn->rollback_transaction();
    }

    return_connection(std::move(conn));
    return success;
}

bool BaseRepository::execute_in_transaction_with_rollback(std::function<bool(DatabaseConnection*)> operation) {
    auto conn = get_connection();
    if (!conn) {
        return false;
    }

    if (!conn->begin_transaction()) {
        return_connection(std::move(conn));
        return false;
    }

    bool success = operation(conn.get());
    
    if (success) {
        success = conn->commit_transaction();
    } else {
        conn->rollback_transaction();
    }

    return_connection(std::move(conn));
    return success;
}

std::unique_ptr<DatabaseConnection> BaseRepository::get_connection() {
    return pool_ ? pool_->get_connection() : nullptr;
}

void BaseRepository::return_connection(std::unique_ptr<DatabaseConnection> conn) {
    if (pool_) {
        pool_->return_connection(std::move(conn));
    }
}

bool BaseRepository::is_healthy() const {
    return pool_ ? pool_->is_healthy() : false;
}

std::unique_ptr<pg_result> BaseRepository::execute_query(const std::string& query) {
    auto conn = get_connection();
    if (!conn) {
        return nullptr;
    }

    auto result = conn->execute_query(query);
    return_connection(std::move(conn));
    return result;
}

std::unique_ptr<pg_result> BaseRepository::execute_prepared(const std::string& stmt_name, 
                                                           const std::vector<std::string>& params) {
    auto conn = get_connection();
    if (!conn) {
        return nullptr;
    }

    auto result = conn->execute_prepared(stmt_name, params);
    return_connection(std::move(conn));
    return result;
}

std::string BaseRepository::get_result_value(pg_result* result, int row, int col) {
    return utils::get_result_value(result, row, col);
}

int BaseRepository::get_result_int(pg_result* result, int row, int col) {
    return utils::get_result_int(result, row, col);
}

bool BaseRepository::get_result_bool(pg_result* result, int row, int col) {
    return utils::get_result_bool(result, row, col);
}

std::vector<std::string> BaseRepository::get_result_array(pg_result* result, int row, int col) {
    return utils::get_result_array(result, row, col);
}

std::string BaseRepository::escape_string(const std::string& input) {
    return utils::escape_string(input);
}

std::string BaseRepository::escape_identifier(const std::string& input) {
    return utils::escape_identifier(input);
}

std::string BaseRepository::uuid_to_string(const std::string& uuid) {
    return utils::uuid_to_string(uuid);
}

std::string BaseRepository::string_to_uuid(const std::string& str) {
    return utils::string_to_uuid(str);
}

std::string BaseRepository::timestamp_to_string(const std::chrono::system_clock::time_point& tp) {
    return utils::timestamp_to_string(tp);
}

std::chrono::system_clock::time_point BaseRepository::string_to_timestamp(const std::string& str) {
    return utils::string_to_timestamp(str);
}

// TransactionScope implementation
TransactionScope::TransactionScope(DatabaseConnection* conn)
    : conn_(conn), active_(false), committed_(false) {
    if (conn_ && conn_->begin_transaction()) {
        active_ = true;
    }
}

TransactionScope::~TransactionScope() {
    if (active_ && !committed_) {
        conn_->rollback_transaction();
    }
}

bool TransactionScope::commit() {
    if (!active_ || committed_) {
        return false;
    }

    if (conn_->commit_transaction()) {
        committed_ = true;
        active_ = false;
        return true;
    }

    return false;
}

void TransactionScope::rollback() {
    if (active_ && !committed_) {
        conn_->rollback_transaction();
        active_ = false;
    }
}

// QueryBuilder implementation
QueryBuilder& QueryBuilder::select(const std::vector<std::string>& columns) {
    select_columns_ = columns;
    return *this;
}

QueryBuilder& QueryBuilder::from(const std::string& table) {
    from_table_ = table;
    return *this;
}

QueryBuilder& QueryBuilder::where(const std::string& condition) {
    where_conditions_.clear();
    where_conditions_.push_back(condition);
    return *this;
}

QueryBuilder& QueryBuilder::and_where(const std::string& condition) {
    where_conditions_.push_back("AND " + condition);
    return *this;
}

QueryBuilder& QueryBuilder::or_where(const std::string& condition) {
    where_conditions_.push_back("OR " + condition);
    return *this;
}

QueryBuilder& QueryBuilder::order_by(const std::string& column, bool ascending) {
    std::string order_clause = column + (ascending ? " ASC" : " DESC");
    order_by_clauses_.push_back(order_clause);
    return *this;
}

QueryBuilder& QueryBuilder::limit(size_t limit) {
    limit_value_ = limit;
    return *this;
}

QueryBuilder& QueryBuilder::offset(size_t offset) {
    offset_value_ = offset;
    return *this;
}

QueryBuilder& QueryBuilder::group_by(const std::vector<std::string>& columns) {
    group_by_columns_ = columns;
    return *this;
}

QueryBuilder& QueryBuilder::having(const std::string& condition) {
    having_condition_ = condition;
    return *this;
}

std::string QueryBuilder::build() const {
    std::ostringstream query;

    // SELECT clause
    if (select_columns_.empty()) {
        query << "SELECT *";
    } else {
        query << "SELECT " << select_columns_[0];
        for (size_t i = 1; i < select_columns_.size(); ++i) {
            query << ", " << select_columns_[i];
        }
    }

    // FROM clause
    if (!from_table_.empty()) {
        query << " FROM " << from_table_;
    }

    // WHERE clause
    if (!where_conditions_.empty()) {
        query << " WHERE " << where_conditions_[0];
        for (size_t i = 1; i < where_conditions_.size(); ++i) {
            query << " " << where_conditions_[i];
        }
    }

    // GROUP BY clause
    if (!group_by_columns_.empty()) {
        query << " GROUP BY " << group_by_columns_[0];
        for (size_t i = 1; i < group_by_columns_.size(); ++i) {
            query << ", " << group_by_columns_[i];
        }
    }

    // HAVING clause
    if (!having_condition_.empty()) {
        query << " HAVING " << having_condition_;
    }

    // ORDER BY clause
    if (!order_by_clauses_.empty()) {
        query << " ORDER BY " << order_by_clauses_[0];
        for (size_t i = 1; i < order_by_clauses_.size(); ++i) {
            query << ", " << order_by_clauses_[i];
        }
    }

    // LIMIT clause
    if (limit_value_ > 0) {
        query << " LIMIT " << limit_value_;
    }

    // OFFSET clause
    if (offset_value_ > 0) {
        query << " OFFSET " << offset_value_;
    }

    return query.str();
}

void QueryBuilder::clear() {
    select_columns_.clear();
    from_table_.clear();
    where_conditions_.clear();
    order_by_clauses_.clear();
    group_by_columns_.clear();
    having_condition_.clear();
    limit_value_ = 0;
    offset_value_ = 0;
}

// ResultRow implementation
ResultRow::ResultRow(pg_result* result, int row) : result_(result), row_(row) {
    if (result_) {
        int num_fields = PQnfields(result_);
        for (int i = 0; i < num_fields; ++i) {
            const char* field_name = PQfname(result_, i);
            if (field_name) {
                column_map_[field_name] = i;
            }
        }
    }
}

std::string ResultRow::get_string(const std::string& column_name) const {
    auto it = column_map_.find(column_name);
    if (it != column_map_.end()) {
        return get_string(it->second);
    }
    return "";
}

int ResultRow::get_int(const std::string& column_name) const {
    auto it = column_map_.find(column_name);
    if (it != column_map_.end()) {
        return get_int(it->second);
    }
    return 0;
}

bool ResultRow::get_bool(const std::string& column_name) const {
    auto it = column_map_.find(column_name);
    if (it != column_map_.end()) {
        return get_bool(it->second);
    }
    return false;
}

std::vector<std::string> ResultRow::get_array(const std::string& column_name) const {
    auto it = column_map_.find(column_name);
    if (it != column_map_.end()) {
        return get_array(it->second);
    }
    return {};
}

std::string ResultRow::get_string(int column_index) const {
    if (!result_ || row_ < 0 || column_index < 0 || 
        row_ >= PQntuples(result_) || column_index >= PQnfields(result_)) {
        return "";
    }
    
    const char* value = PQgetvalue(result_, row_, column_index);
    return value ? value : "";
}

int ResultRow::get_int(int column_index) const {
    std::string value = get_string(column_index);
    return value.empty() ? 0 : std::stoi(value);
}

bool ResultRow::get_bool(int column_index) const {
    std::string value = get_string(column_index);
    return value == "t" || value == "true" || value == "1";
}

std::vector<std::string> ResultRow::get_array(int column_index) const {
    std::string value = get_string(column_index);
    std::vector<std::string> array;
    
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

bool ResultRow::is_null(const std::string& column_name) const {
    auto it = column_map_.find(column_name);
    if (it != column_map_.end()) {
        return is_null(it->second);
    }
    return true;
}

bool ResultRow::is_null(int column_index) const {
    if (!result_ || row_ < 0 || column_index < 0 || 
        row_ >= PQntuples(result_) || column_index >= PQnfields(result_)) {
        return true;
    }
    
    return PQgetisnull(result_, row_, column_index) == 1;
}

int ResultRow::get_column_count() const {
    return result_ ? PQnfields(result_) : 0;
}

// ResultSet implementation
ResultSet::ResultSet(std::unique_ptr<pg_result> result) : result_(std::move(result)) {
    if (result_ && PQresultStatus(result_.get()) != PGRES_TUPLES_OK) {
        error_message_ = PQresultErrorMessage(result_.get());
    }
}

ResultSet::Iterator::Iterator(pg_result* result, int row) : result_(result), row_(row) {}

ResultSet::Iterator& ResultSet::Iterator::operator++() {
    if (result_ && row_ < PQntuples(result_) - 1) {
        ++row_;
    }
    return *this;
}

ResultRow ResultSet::Iterator::operator*() {
    return ResultRow(result_, row_);
}

bool ResultSet::Iterator::operator!=(const Iterator& other) const {
    return result_ != other.result_ || row_ != other.row_;
}

ResultSet::Iterator ResultSet::begin() {
    return Iterator(result_.get(), 0);
}

ResultSet::Iterator ResultSet::end() {
    return Iterator(result_.get(), PQntuples(result_.get()));
}

ResultRow ResultSet::operator[](int index) const {
    if (index >= 0 && index < static_cast<int>(size())) {
        return ResultRow(result_.get(), index);
    }
    throw std::out_of_range("Index out of range");
}

size_t ResultSet::size() const {
    return result_ ? PQntuples(result_) : 0;
}

bool ResultSet::empty() const {
    return size() == 0;
}

bool ResultSet::has_error() const {
    return !error_message_.empty();
}

std::string ResultSet::get_error() const {
    return error_message_;
}

} // namespace database
} // namespace sonet