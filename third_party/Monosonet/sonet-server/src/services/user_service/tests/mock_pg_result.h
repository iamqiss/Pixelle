/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <postgresql/libpq-fe.h>
#include <vector>
#include <string>
#include <memory>

// Mock pg_result for testing purposes
class MockPgResult {
public:
    struct Row {
        std::vector<std::string> values;
        std::vector<bool> nulls;
    };
    
    MockPgResult() = default;
    ~MockPgResult() = default;
    
    // Add a row with values
    void add_row(const std::vector<std::string>& values, const std::vector<bool>& nulls = {}) {
        Row row;
        row.values = values;
        
        // If nulls not specified, assume all values are non-null
        if (nulls.empty()) {
            row.nulls.resize(values.size(), false);
        } else {
            row.nulls = nulls;
        }
        
        rows_.push_back(row);
    }
    
    // Add a row with a single value
    void add_row(const std::string& value, bool is_null = false) {
        add_row({value}, {is_null});
    }
    
    // Get number of rows
    int get_num_rows() const {
        return static_cast<int>(rows_.size());
    }
    
    // Get number of columns (from first row)
    int get_num_cols() const {
        if (rows_.empty()) return 0;
        return static_cast<int>(rows_[0].values.size());
    }
    
    // Get value at specific position
    std::string get_value(int row, int col) const {
        if (row < 0 || row >= static_cast<int>(rows_.size()) || 
            col < 0 || col >= static_cast<int>(rows_[row].values.size())) {
            return "";
        }
        return rows_[row].values[col];
    }
    
    // Check if value is null
    bool is_null(int row, int col) const {
        if (row < 0 || row >= static_cast<int>(rows_.size()) || 
            col < 0 || col >= static_cast<int>(rows_[row].nulls.size())) {
            return true;
        }
        return rows_[row].nulls[col];
    }
    
    // Clear all data
    void clear() {
        rows_.clear();
    }
    
    // Create a mock result with user data
    static std::unique_ptr<MockPgResult> create_user_result() {
        auto result = std::make_unique<MockPgResult>();
        
        // Add a test user row
        result->add_row({
            "test-user-123",           // user_id
            "testuser",                // username
            "test@example.com",        // email
            "hashed_password",         // password_hash
            "test_salt",               // salt
            "Test User",               // display_name
            "Test",                    // first_name
            "User",                    // last_name
            "ACTIVE",                  // status
            "PERSONAL",                // account_type
            "PUBLIC",                  // privacy_level
            "true",                    // is_verified
            "false",                   // is_premium
            "false",                   // is_developer
            "2025-01-01 10:00:00",    // created_at
            "2025-01-01 10:00:00"     // updated_at
        });
        
        return result;
    }
    
    // Create a mock result with profile data
    static std::unique_ptr<MockPgResult> create_profile_result() {
        auto result = std::make_unique<MockPgResult>();
        
        // Add a test profile row
        result->add_row({
            "test-user-123",           // user_id
            "Test bio",                // bio
            "Test City",               // location
            "https://test.com",        // website
            "https://test.com/avatar.jpg", // avatar_url
            "https://test.com/banner.jpg", // banner_url
            "UTC",                     // timezone
            "en",                      // language
            "2025-01-01 10:00:00",    // created_at
            "2025-01-01 10:00:00"     // updated_at
        });
        
        return result;
    }
    
    // Create a mock result with session data
    static std::unique_ptr<MockPgResult> create_session_result() {
        auto result = std::make_unique<MockPgResult>();
        
        // Add a test session row
        result->add_row({
            "test-session-123",        // session_id
            "test-user-123",           // user_id
            "test-token-123",          // token
            "test-device",             // device_id
            "Test Device",             // device_name
            "127.0.0.1",              // ip_address
            "Test User Agent",         // user_agent
            "WEB",                     // session_type
            "2025-01-01 10:00:00",    // created_at
            "2025-01-01 10:00:00",    // last_activity
            "2025-01-02 10:00:00",    // expires_at
            "true"                     // is_active
        });
        
        return result;
    }
    
    // Create a mock result with 2FA data
    static std::unique_ptr<MockPgResult> create_2fa_result() {
        auto result = std::make_unique<MockPgResult>();
        
        // Add a test 2FA row
        result->add_row({
            "test-user-123",           // user_id
            "test_secret_key",         // secret_key
            "backup1,backup2,backup3", // backup_codes
            "true",                    // is_enabled
            "2025-01-01 10:00:00",    // created_at
            "2025-01-01 10:00:00"     // updated_at
        });
        
        return result;
    }
    
    // Create a mock result with user settings data
    static std::unique_ptr<MockPgResult> create_settings_result() {
        auto result = std::make_unique<MockPgResult>();
        
        // Add a test settings row
        result->add_row({
            "test-user-123",           // user_id
            "true",                    // email_notifications
            "false",                   // push_notifications
            "true",                    // sms_notifications
            "en",                      // language
            "UTC",                     // timezone
            "dark",                    // theme
            "true",                    // two_factor_enabled
            "false",                   // public_profile
            "2025-01-01 10:00:00",    // created_at
            "2025-01-01 10:00:00"     // updated_at
        });
        
        return result;
    }
    
    // Create a mock result with user stats data
    static std::unique_ptr<MockPgResult> create_stats_result() {
        auto result = std::make_unique<MockPgResult>();
        
        // Add a test stats row
        result->add_row({
            "test-user-123",           // user_id
            "100",                     // total_notes
            "50",                      // total_followers
            "25",                      // total_following
            "1000",                    // total_likes_received
            "500",                     // total_likes_given
            "10",                      // total_comments
            "5",                       // total_shares
            "2025-01-01 10:00:00",    // last_activity
            "2025-01-01 10:00:00"     // created_at
        });
        
        return result;
    }
    
    // Create a mock result with login history data
    static std::unique_ptr<MockPgResult> create_login_history_result() {
        auto result = std::make_unique<MockPgResult>();
        
        // Add a test login history row
        result->add_row({
            "test-user-123",           // user_id
            "test-session-123",        // session_id
            "2025-01-01 10:00:00",    // login_timestamp
            "2025-01-01 11:00:00",    // logout_timestamp
            "127.0.0.1",              // ip_address
            "Test User Agent",         // user_agent
            "test-device",             // device_id
            "Test Device",             // device_name
            "Test City",               // location
            "true",                    // success
            "",                        // failure_reason
            "2025-01-01 10:00:00"     // created_at
        });
        
        return result;
    }
    
    // Create a mock result with count data
    static std::unique_ptr<MockPgResult> create_count_result(int count) {
        auto result = std::make_unique<MockPgResult>();
        result->add_row(std::to_string(count));
        return result;
    }
    
    // Create a mock result with empty data
    static std::unique_ptr<MockPgResult> create_empty_result() {
        return std::make_unique<MockPgResult>();
    }

private:
    std::vector<Row> rows_;
};

// Helper function to convert MockPgResult to pg_result*
// Note: This is a simplified mock - in real tests you might want to use a more sophisticated approach
pg_result* mock_pg_result_to_pg_result(const MockPgResult& mock_result) {
    // This is a placeholder - in a real implementation you'd need to create
    // actual pg_result structures or use a mocking framework that can handle this
    // For now, we'll return nullptr and handle it in our tests
    return nullptr;
}