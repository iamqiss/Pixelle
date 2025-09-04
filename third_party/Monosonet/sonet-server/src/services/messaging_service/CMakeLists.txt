# Sonet Messaging Service - Military-Grade E2EE Communication Platform
# Copyright (c) 2025 Neo Qiss. All rights reserved.

cmake_minimum_required(VERSION 3.16)
project(messaging_service VERSION 1.0.0)

# Set C++ standard and compiler flags
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler-specific optimizations
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -O3")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -fsanitize=address")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG -flto")
endif()

# Security hardening flags
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstack-protector-strong")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIE -pie")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wformat -Wformat-security")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FORTIFY_SOURCE=2")
endif()

# Build type configuration
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")

# Find required packages
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)

# Core dependencies
find_package(Boost REQUIRED COMPONENTS system filesystem thread random chrono)
find_package(OpenSSL REQUIRED)
# Rely on top-level Protobuf/gRPC configuration and sonet_proto target
# find_package(protobuf REQUIRED)
# find_package(gRPC REQUIRED)

# JSON library
find_package(jsoncpp REQUIRED)

# WebSocket library (websocketpp)
find_path(WEBSOCKETPP_INCLUDE_DIR websocketpp/config/asio_no_tls.hpp)
if(NOT WEBSOCKETPP_INCLUDE_DIR)
    message(FATAL_ERROR "websocketpp not found. Install with: apt-get install libwebsocketpp-dev")
endif()

# Asio for WebSocket support (header-only)
# find_package(asio REQUIRED)
include_directories(/usr/include)

# Database libraries
pkg_check_modules(LIBPQ REQUIRED libpq)
pkg_check_modules(LIBPQXX REQUIRED libpqxx)

# Redis client library
pkg_check_modules(HIREDIS REQUIRED hiredis)

# Cryptographic libraries
pkg_check_modules(LIBSODIUM REQUIRED libsodium)

# Check for additional security libraries
find_library(ARGON2_LIBRARY argon2)
if(ARGON2_LIBRARY)
    message(STATUS "Found Argon2: ${ARGON2_LIBRARY}")
    set(HAVE_ARGON2 ON)
else()
    message(WARNING "Argon2 not found. Some features may be disabled.")
    set(HAVE_ARGON2 OFF)
endif()

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../core
    ${CMAKE_CURRENT_SOURCE_DIR}/../../external
    ${WEBSOCKETPP_INCLUDE_DIR}
    ${Boost_INCLUDE_DIRS}
    ${LIBPQ_INCLUDE_DIRS}
    ${LIBPQXX_INCLUDE_DIRS}
    ${HIREDIS_INCLUDE_DIRS}
)

# Source files
set(MESSAGING_SERVICE_SOURCES
    # Core message handling
    src/message.cpp
    src/chat.cpp
    
    # Cryptography and encryption
    crypto/crypto_engine.cpp
    crypto/e2e_encryption.cpp
    crypto/e2e_encryption_advanced.cpp
    crypto/encryption_manager.cpp
    crypto/secure_random.cpp
    
    # API and controllers
    api/messaging_controller.cpp
    
    # Real-time features
    realtime/websocket_manager.cpp
    
    # Storage and database
    storage/mongodb_manager.cpp
    storage/attachment_storage.cpp
    
    # Threading and utilities
    threading/message_processor.cpp
    threading/attachment_processor.cpp
    
    # gRPC service implementation
    grpc/messaging_grpc_service.cpp
    
    # Main service
    messaging_service.cpp
    main.cpp
)

# Test sources
set(MESSAGING_SERVICE_TESTS
    tests/test_x3dh_and_ratchet.cpp
    tests/test_server_security.cpp
    tests/test_crypto_roundtrip.cpp
    tests/test_advanced_features.cpp
)

# Headers
set(MESSAGING_SERVICE_HEADERS
    include/message.hpp
    include/chat.hpp
    include/crypto_engine.hpp
    include/e2e_encryption_manager.hpp
    include/encryption_manager.hpp
    include/messaging_controller.hpp
    include/performance_optimizer.hpp
    include/websocket_manager.hpp
    include/attachment_storage.hpp
    include/mongodb_manager.hpp
    include/messaging_service.hpp
)

# Main executable
add_executable(messaging_service ${MESSAGING_SERVICE_SOURCES} ${MESSAGING_SERVICE_HEADERS})

# Test executable
add_executable(messaging_tests ${MESSAGING_SERVICE_TESTS} ${MESSAGING_SERVICE_HEADERS})

# Add MLS Protocol and PQC Algorithms
add_library(mls_protocol STATIC
    src/mls_protocol.cpp
)

add_library(pqc_algorithms STATIC
    src/pqc_algorithms.cpp
)

# Link libraries for main service
target_link_libraries(messaging_service
    ${Boost_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
    ${GRPC_LIBRARIES}
    ${JSONCPP_LIBRARIES}
    mls_protocol
    pqc_algorithms
    ${LIBPQ_LIBRARIES}
    ${LIBPQXX_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    ${LIBSODIUM_LIBRARIES}
    # ${ARGON2_LIBRARY} # optional
    Threads::Threads
    # header-only asio
    sonet_proto
)

# Optionally link Argon2 if found
if(ARGON2_LIBRARY)
    target_link_libraries(messaging_service ${ARGON2_LIBRARY})
endif()

# Link libraries for tests
target_link_libraries(messaging_tests
    gtest
    gmock
    ${Boost_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${JSONCPP_LIBRARIES}
    ${LIBSODIUM_LIBRARIES}
    Threads::Threads
)

# Include directories for tests
target_include_directories(messaging_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../core
    ${CMAKE_CURRENT_SOURCE_DIR}/../external
    ${GTEST_INCLUDE_DIRS}
)

# Compiler definitions
target_compile_definitions(messaging_service PRIVATE
    # Service information
    SERVICE_NAME="messaging_service"
    SERVICE_VERSION="1.0.0"
    BUILD_TIMESTAMP="${CMAKE_BUILD_TIMESTAMP}"
    
    # Feature flags
    ENCRYPTION_ENABLED=1
    E2E_ENCRYPTION_ENABLED=1
    WEBSOCKET_ENABLED=1
    ATTACHMENTS_ENABLED=1
    SEARCH_ENABLED=1
    
    # Security features
    PERFECT_FORWARD_SECRECY=1
    QUANTUM_RESISTANT_CRYPTO=1
    SECURE_MEMORY_MGMT=1
    
    # Performance optimizations
    USE_MEMORY_POOLS=1
    USE_CONNECTION_POOLING=1
    USE_ASYNC_IO=1
    
    # Optional features
    $<$<BOOL:${HAVE_ARGON2}>:HAVE_ARGON2=1>
    
    # Build configuration
    $<$<CONFIG:Debug>:DEBUG_BUILD=1>
    $<$<CONFIG:Release>:RELEASE_BUILD=1>
)

# Installation rules
install(TARGETS messaging_service
    RUNTIME DESTINATION bin
    COMPONENT messaging_service
)

# Install configuration files
install(FILES
    config/messaging_service.json.example
    DESTINATION etc/sonet
    COMPONENT messaging_service_config
)

# Install systemd service file
install(FILES
    deployment/systemd/messaging-service.service
    DESTINATION lib/systemd/system
    COMPONENT messaging_service_systemd
)

# Development and testing targets
if(BUILD_TESTING)
    enable_testing()
    
    # Unit tests
    add_subdirectory(tests)
endif()

# Static analysis targets
find_program(CLANG_TIDY clang-tidy)
if(CLANG_TIDY)
    add_custom_target(messaging_service_tidy
        COMMAND ${CLANG_TIDY} ${MESSAGING_SERVICE_SOURCES}
        -p ${CMAKE_BINARY_DIR}
        --config-file=${CMAKE_SOURCE_DIR}/.clang-tidy
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running clang-tidy on messaging service"
    )
endif()

find_program(CPPCHECK cppcheck)
if(CPPCHECK)
    add_custom_target(messaging_service_cppcheck
        COMMAND ${CPPCHECK}
        --enable=all
        --std=c++20
        --verbose
        --quiet
        --error-exitcode=1
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        COMMENT "Running cppcheck on messaging service"
    )
endif()

# Documentation generation
find_package(Doxygen)
if(DOXYGEN_FOUND)
    set(DOXYGEN_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
    set(DOXYGEN_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/docs)
    
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in
        ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        @ONLY
    )
    
    add_custom_target(messaging_service_docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM
    )
endif()

# Performance profiling
if(ENABLE_PROFILING)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(messaging_service PRIVATE -pg)
        target_link_options(messaging_service PRIVATE -pg)
    endif()
    
    find_program(PERF perf)
    if(PERF)
        add_custom_target(messaging_service_profile
            COMMAND ${PERF} record -g ./messaging_service --test-mode
            DEPENDS messaging_service
            COMMENT "Profiling messaging service with perf"
        )
    endif()
endif()

# Memory debugging
if(ENABLE_VALGRIND)
    find_program(VALGRIND valgrind)
    if(VALGRIND)
        add_custom_target(messaging_service_valgrind
            COMMAND ${VALGRIND}
            --tool=memcheck
            --leak-check=full
            --show-leak-kinds=all
            --track-origins=yes
            --verbose
            ./messaging_service --test-mode
            DEPENDS messaging_service
            COMMENT "Running valgrind memory check"
        )
    endif()
endif()

# Security scanning
find_program(CLANG_STATIC_ANALYZER scan-build)
if(CLANG_STATIC_ANALYZER)
    add_custom_target(messaging_service_scan
        COMMAND ${CLANG_STATIC_ANALYZER} make messaging_service
        COMMENT "Running Clang Static Analyzer"
    )
endif()

# Code coverage
if(ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(messaging_service PRIVATE --coverage)
        target_link_options(messaging_service PRIVATE --coverage)
        
        find_program(GCOV gcov)
        find_program(LCOV lcov)
        find_program(GENHTML genhtml)
        
        if(GCOV AND LCOV AND GENHTML)
            add_custom_target(messaging_service_coverage
                COMMAND ${LCOV} --capture --directory . --output-file coverage.info
                COMMAND ${LCOV} --remove coverage.info '/usr/*' --output-file coverage.info
                COMMAND ${LCOV} --list coverage.info
                COMMAND ${GENHTML} coverage.info --output-directory coverage_html
                COMMENT "Generating code coverage report"
            )
        endif()
    endif()
endif()

# Packaging
set(CPACK_PACKAGE_NAME "sonet-messaging-service")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Sonet Messaging Service - Military-Grade E2EE Communication")
set(CPACK_PACKAGE_VENDOR "Neo Qiss")
set(CPACK_PACKAGE_CONTACT "support@neoqiss.com")

set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6, libssl3, libpq5, libhiredis0.14, libjsoncpp25")
set(CPACK_DEBIAN_PACKAGE_SECTION "net")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")

set(CPACK_RPM_PACKAGE_GROUP "Applications/Internet")
set(CPACK_RPM_PACKAGE_REQUIRES "glibc, openssl-libs, postgresql-libs, hiredis, jsoncpp")

include(CPack)

# Print configuration summary
message(STATUS "")
message(STATUS "Messaging Service Configuration Summary:")
message(STATUS "  Version: ${PROJECT_VERSION}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Encryption: Enabled")
message(STATUS "  E2E Encryption: Enabled")
message(STATUS "  WebSocket: Enabled")
message(STATUS "  Attachments: Enabled")
message(STATUS "  Search: Enabled")
message(STATUS "  Argon2: ${HAVE_ARGON2}")
message(STATUS "  Testing: ${BUILD_TESTING}")
message(STATUS "  Profiling: ${ENABLE_PROFILING}")
message(STATUS "  Coverage: ${ENABLE_COVERAGE}")
message(STATUS "  Valgrind: ${ENABLE_VALGRIND}")
message(STATUS "")
message(STATUS "🔐 Military-grade encryption protecting your communications")
message(STATUS "🚀 Ready to build secure messaging at scale")
message(STATUS "")
