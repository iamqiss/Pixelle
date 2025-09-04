#
# Copyright (c) 2025 Neo Qiss
# All rights reserved.
# 
# This software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.
#

cmake_minimum_required(VERSION 3.20)

# Test configuration
enable_testing()

# Find test dependencies - make GTest optional
find_package(GTest QUIET)

if(NOT GTest_FOUND)
    message(WARNING "GTest not found. Tests will be disabled.")
    message(WARNING "For testing functionality, install: libgtest-dev")
    
    # Create a dummy test target that does nothing
    add_custom_target(test_main ALL
        COMMAND ${CMAKE_COMMAND} -E echo "Tests disabled - GTest not found"
        COMMENT "Tests disabled - GTest not found"
    )
    
    # Add a dummy test
    add_test(NAME disabled_tests COMMAND ${CMAKE_COMMAND} -E echo "Tests disabled")
    
else()
    message(STATUS "GTest found: ${GTest_VERSION}")
    
    # Include test directories
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../src)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR})
    
    # Test main executable
    add_executable(test_main test_main.cpp)
    target_link_libraries(test_main 
        GTest::gtest 
        GTest::gtest_main
        sonet_proto
    )
    
    # Add test subdirectories
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/unit/CMakeLists.txt)
        add_subdirectory(unit)
    endif()
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/integration/CMakeLists.txt)
        add_subdirectory(integration)
    endif()
    
    # Register tests with CTest
    add_test(NAME unit_tests COMMAND test_main)
    
    # Test coverage (optional)
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        target_compile_options(test_main PRIVATE --coverage)
        target_link_options(test_main PRIVATE --coverage)
    endif()
endif()
