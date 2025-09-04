/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

cmake_minimum_required(VERSION 3.20)

# Test configuration for User Service
project(UserServiceTests VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(GTest REQUIRED)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../core)

# Test source files
set(TEST_SOURCES
    test_password_manager.cpp
    test_jwt_manager.cpp
    test_security_utils.cpp
    test_user_repository_libpq.cpp
    test_user_repository_integration.cpp
)

# Create test executable
add_executable(user_service_tests ${TEST_SOURCES})

# Link with dependencies
target_link_libraries(user_service_tests PRIVATE
    # Test framework
    GTest::gtest
    GTest::gtest_main
    
    # User service library dependencies
    OpenSSL::SSL
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json
    spdlog::spdlog
    fmt::fmt
    uuid
)

# Compile the user service source files for testing
target_sources(user_service_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/password_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/jwt_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/security_utils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../repository/user_repository_libpq.cpp
)

# Enable testing
enable_testing()

# Add tests
add_test(NAME PasswordManagerTests COMMAND user_service_tests --gtest_filter="PasswordManagerTest.*")
add_test(NAME JWTManagerTests COMMAND user_service_tests --gtest_filter="JWTManagerTest.*")
add_test(NAME SecurityUtilsTests COMMAND user_service_tests --gtest_filter="SecurityUtilsTest.*")
add_test(NAME UserRepositoryLibpqTests COMMAND user_service_tests --gtest_filter="UserRepositoryLibpqTest.*")
add_test(NAME UserRepositoryIntegrationTests COMMAND user_service_tests --gtest_filter="UserRepositoryIntegrationTest.*")

# Compiler options for tests
target_compile_options(user_service_tests PRIVATE 
    -Wall -Wextra -Wpedantic
    -Wno-unused-parameter
)

# Debug configuration for tests
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(user_service_tests PRIVATE DEBUG_MODE=1)
    target_compile_options(user_service_tests PRIVATE -g -O0)
endif()
