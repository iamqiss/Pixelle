/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <chrono>

namespace test_config {
    // Test database connection string (for integration tests)
    const std::string TEST_DB_CONNECTION = "postgresql://testuser:testpass@localhost:5432/sonet_test";
    
    // Test timeouts
    const int TEST_TIMEOUT_MS = 5000;
    const int LONG_TEST_TIMEOUT_MS = 30000;
    
    // Test data constants
    const std::string TEST_USER_ID_PREFIX = "test-user-";
    const std::string TEST_SESSION_ID_PREFIX = "test-session-";
    const std::string TEST_EMAIL_DOMAIN = "@test.example.com";
    
    // Performance test thresholds
    const int BULK_OPERATION_THRESHOLD_MS = 1000;
    const int SINGLE_OPERATION_THRESHOLD_MS = 100;
    
    // Test data sizes
    const int SMALL_DATASET_SIZE = 10;
    const int MEDIUM_DATASET_SIZE = 100;
    const int LARGE_DATASET_SIZE = 1000;
}