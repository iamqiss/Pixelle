# Copyright (c) 2025 Neo Qiss
# All rights reserved.
# 
# This software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.

cmake_minimum_required(VERSION 3.20)

# ========== PROJECT CONFIGURATION ==========
project(SonetNoteService
    VERSION 1.0.0
    DESCRIPTION "Twitter-Scale Note Service with HTTP, gRPC, and WebSocket support"
    LANGUAGES CXX
)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build Configuration
set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type")
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")

# ========== COMPILER FLAGS ==========
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Performance optimizations
    add_compile_options(
        -O3
        -march=native
        -mtune=native
        -flto
        -ffast-math
        -funroll-loops
        -finline-functions
        -fomit-frame-pointer
        -DNDEBUG
    )
    
    # Warning flags
    add_compile_options(
        -Wall
        -Wextra
        -Wpedantic
        -Wunused
        -Wconversion
        -Wsign-conversion
        -Wcast-align
        -Wcast-qual
        -Wctor-dtor-privacy
        -Wdisabled-optimization
        -Wformat=2
        -Winit-self
        -Wmissing-declarations
        -Wmissing-include-dirs
        -Wold-style-cast
        -Woverloaded-virtual
        -Wredundant-decls
        -Wshadow
        -Wsign-promo
        -Wstrict-overflow=5
        -Wswitch-default
        -Wundef
    )
    
    # Error flags
    add_compile_options(
        -Werror=return-type
        -Werror=uninitialized
        -Werror=format-security
    )
endif()

# Debug-specific flags
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g -O0 -DDEBUG)
    add_compile_definitions(SONET_DEBUG=1)
endif()

# ========== EXTERNAL DEPENDENCIES ==========

# Find required packages
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)

# nlohmann/json
find_package(nlohmann_json REQUIRED)

# gRPC and Protocol Buffers
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# HTTP/WebSocket libraries
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBHTTPSERVER REQUIRED libhttpserver)
pkg_check_modules(WEBSOCKETPP REQUIRED websocketpp)

# Database drivers
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPQXX REQUIRED libpqxx)

# Redis
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

# SSL/TLS
find_package(OpenSSL REQUIRED)

# Monitoring
find_package(PkgConfig REQUIRED)
pkg_check_modules(PROMETHEUS_CPP REQUIRED prometheus-cpp-core prometheus-cpp-pull)

# Logging
find_package(spdlog REQUIRED)

# Optional: Asynchronous I/O
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING liburing)

# ========== PROTOCOL BUFFERS GENERATION ==========

# Note service proto files
set(PROTO_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/services/note.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/services/timeline.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/services/analytics.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/services/moderation.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/common/common.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/common/pagination.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/common/timestamp.proto"
)

# Generated proto directory
set(PROTO_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_GENERATED_DIR})

# Generate C++ files from proto
set(PROTO_SRCS)
set(PROTO_HDRS)
set(GRPC_SRCS)
set(GRPC_HDRS)

foreach(proto_file ${PROTO_FILES})
    get_filename_component(proto_name ${proto_file} NAME_WE)
    
    # Protocol buffer files
    list(APPEND PROTO_SRCS "${PROTO_GENERATED_DIR}/${proto_name}.pb.cc")
    list(APPEND PROTO_HDRS "${PROTO_GENERATED_DIR}/${proto_name}.pb.h")
    
    # gRPC files
    list(APPEND GRPC_SRCS "${PROTO_GENERATED_DIR}/${proto_name}.grpc.pb.cc")
    list(APPEND GRPC_HDRS "${PROTO_GENERATED_DIR}/${proto_name}.grpc.pb.h")
    
    # Custom commands for generation
    add_custom_command(
        OUTPUT "${PROTO_GENERATED_DIR}/${proto_name}.pb.cc"
               "${PROTO_GENERATED_DIR}/${proto_name}.pb.h"
               "${PROTO_GENERATED_DIR}/${proto_name}.grpc.pb.cc"
               "${PROTO_GENERATED_DIR}/${proto_name}.grpc.pb.h"
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --cpp_out=${PROTO_GENERATED_DIR}
             --grpc_out=${PROTO_GENERATED_DIR}
             --plugin=protoc-gen-grpc=`which grpc_cpp_plugin`
             --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/../../proto
             ${proto_file}
        DEPENDS ${proto_file}
        COMMENT "Generating C++ and gRPC files for ${proto_name}"
        VERBATIM
    )
endforeach()

# ========== SOURCE FILES ==========

# Core source files
set(CORE_SOURCES
    # Models
    models/note.cpp
    models/attachment.cpp
    models/thread.cpp
    
    # Repositories
    repositories/note_repository.cpp
    repositories/attachment_repository.cpp
    repositories/timeline_repository.cpp
    
    # Services
    service.cpp
    
    # Validators
    validators/note_validator.cpp
    
    # Controllers
    controllers/note_controller.cpp
    
    # gRPC Services
    grpc/note_grpc_service.cpp
    
    # Clients
    clients/moderation_client.cpp
    
    # WebSocket Handlers
    websocket/note_websocket_handler.cpp
    
    # Handlers
    handlers/create_note_handler.cpp
    handlers/update_note_handler.cpp
    handlers/delete_note_handler.cpp
    handlers/get_note_handler.cpp
    handlers/like_note_handler.cpp
    handlers/renote_handler.cpp
    handlers/timeline_handler.cpp
    handlers/search_handler.cpp
    handlers/analytics_handler.cpp
)

# Header files for IDE
set(CORE_HEADERS
    models/note.h
    models/attachment.h
    models/thread.h
    repositories/note_repository.h
    repositories/attachment_repository.h
    repositories/timeline_repository.h
    service.h
    validators/note_validator.h
    controllers/note_controller.h
    grpc/note_grpc_service.h
    clients/moderation_client.h
    websocket/note_websocket_handler.h
    note_service_orchestrator.h
    ${PROTO_HDRS}
    ${GRPC_HDRS}
)

# ========== LIBRARY CREATION ==========

# Create note service library
add_library(note_service_lib STATIC
    ${CORE_SOURCES}
    ${CORE_HEADERS}
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

# Include directories
target_include_directories(note_service_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROTO_GENERATED_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/../../core
    PRIVATE
        ${LIBHTTPSERVER_INCLUDE_DIRS}
        ${WEBSOCKETPP_INCLUDE_DIRS}
        ${LIBPQXX_INCLUDE_DIRS}
        ${HIREDIS_INCLUDE_DIRS}
        ${PROMETHEUS_CPP_INCLUDE_DIRS}
)

# Link libraries
target_link_libraries(note_service_lib
    PUBLIC
        nlohmann_json::nlohmann_json
        gRPC::grpc++
        protobuf::libprotobuf
        spdlog::spdlog
        Threads::Threads
        OpenSSL::SSL
        OpenSSL::Crypto
    PRIVATE
        ${LIBHTTPSERVER_LIBRARIES}
        ${WEBSOCKETPP_LIBRARIES}
        ${LIBPQXX_LIBRARIES}
        ${HIREDIS_LIBRARIES}
        ${PROMETHEUS_CPP_LIBRARIES}
)

# Compiler definitions
target_compile_definitions(note_service_lib
    PUBLIC
        SONET_NOTE_SERVICE_VERSION="${PROJECT_VERSION}"
        SONET_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
    PRIVATE
        SPDLOG_FMT_EXTERNAL
        SPDLOG_COMPILED_LIB
)

# ========== EXECUTABLE CREATION ==========

# Main note service executable
add_executable(note_service
    main.cpp
)

# Link the library
target_link_libraries(note_service
    PRIVATE
        note_service_lib
)

# ========== TESTING ==========

# Enable testing
enable_testing()

# Test executable
if(BUILD_TESTING OR CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Find Google Test
    find_package(GTest REQUIRED)
    
    # Test sources
    set(TEST_SOURCES
        tests/unit/test_note_validator.cpp
        tests/unit/test_note_model.cpp
        tests/unit/test_note_repository.cpp
        tests/unit/test_note_controller.cpp
        tests/unit/test_note_grpc_service.cpp
        tests/integration/test_note_service_integration.cpp
        tests/performance/test_note_service_performance.cpp
    )
    
    # Create test executable
    add_executable(note_service_tests
        ${TEST_SOURCES}
    )
    
    # Link test dependencies
    target_link_libraries(note_service_tests
        PRIVATE
            note_service_lib
            GTest::gtest
            GTest::gtest_main
            GTest::gmock
    )
    
    # Test discovery
    include(GoogleTest)
    gtest_discover_tests(note_service_tests)
    
    # Custom test targets
    add_custom_target(run_unit_tests
        COMMAND note_service_tests --gtest_filter="Unit*"
        DEPENDS note_service_tests
        COMMENT "Running unit tests"
    )
    
    add_custom_target(run_integration_tests
        COMMAND note_service_tests --gtest_filter="Integration*"
        DEPENDS note_service_tests
        COMMENT "Running integration tests"
    )
    
    add_custom_target(run_performance_tests
        COMMAND note_service_tests --gtest_filter="Performance*"
        DEPENDS note_service_tests
        COMMENT "Running performance tests"
    )
endif()

# ========== BENCHMARKING ==========

# Benchmark executable (optional)
find_package(benchmark QUIET)
if(benchmark_FOUND)
    add_executable(note_service_benchmarks
        benchmarks/note_validation_benchmark.cpp
        benchmarks/note_creation_benchmark.cpp
        benchmarks/note_grpc_benchmark.cpp
        benchmarks/note_websocket_benchmark.cpp
    )
    
    target_link_libraries(note_service_benchmarks
        PRIVATE
            note_service_lib
            benchmark::benchmark
            benchmark::benchmark_main
    )
endif()

# ========== INSTALLATION ==========

# Install targets
install(TARGETS note_service note_service_lib
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

# Install headers
install(DIRECTORY .
    DESTINATION include/sonet/note_service
    FILES_MATCHING PATTERN "*.h"
    PATTERN "tests" EXCLUDE
    PATTERN "benchmarks" EXCLUDE
)

# Install proto generated headers
install(DIRECTORY ${PROTO_GENERATED_DIR}/
    DESTINATION include/sonet/note_service/proto
    FILES_MATCHING PATTERN "*.h"
)

# ========== DOCUMENTATION ==========

# Generate documentation with Doxygen (optional)
find_package(Doxygen QUIET)
if(DOXYGEN_FOUND)
    set(DOXYGEN_INPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set(DOXYGEN_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/docs)
    
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in
        ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        @ONLY
    )
    
    add_custom_target(note_service_docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM
    )
endif()

# ========== PACKAGING ==========

# CPack configuration
set(CPACK_PACKAGE_NAME "sonet-note-service")
set(CPACK_PACKAGE_VENDOR "Neo Qiss")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Twitter-Scale Note Service")
set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE")

# Package formats
set(CPACK_GENERATOR "TGZ;ZIP")
if(UNIX AND NOT APPLE)
    list(APPEND CPACK_GENERATOR "DEB;RPM")
endif()

include(CPack)

# ========== PERFORMANCE MONITORING ==========

# Add performance monitoring flags
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_definitions(note_service_lib
        PRIVATE
            SONET_ENABLE_METRICS=1
            SONET_ENABLE_TRACING=1
            SONET_ENABLE_PROFILING=1
    )
endif()

# ========== CUSTOM TARGETS ==========

# Build all target
add_custom_target(build_all
    DEPENDS note_service note_service_lib
    COMMENT "Building all note service components"
)

# Clean all target
add_custom_target(clean_all
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target clean
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${PROTO_GENERATED_DIR}
    COMMENT "Cleaning all build artifacts"
)

# Format code target (requires clang-format)
find_program(CLANG_FORMAT clang-format)
if(CLANG_FORMAT)
    file(GLOB_RECURSE ALL_SOURCE_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h
    )
    
    add_custom_target(format_code
        COMMAND ${CLANG_FORMAT} -i ${ALL_SOURCE_FILES}
        COMMENT "Formatting source code"
    )
endif()

# Static analysis target (requires cppcheck)
find_program(CPPCHECK cppcheck)
if(CPPCHECK)
    add_custom_target(static_analysis
        COMMAND ${CPPCHECK}
            --enable=all
            --std=c++20
            --project=${CMAKE_BINARY_DIR}/compile_commands.json
            --suppress=missingIncludeSystem
            --xml
            --xml-version=2
            --output-file=${CMAKE_BINARY_DIR}/cppcheck-report.xml
            ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running static analysis"
    )
endif()

# ========== STATUS REPORT ==========

message(STATUS "")
message(STATUS "========== Sonet Note Service Configuration ==========")
message(STATUS "Version: ${PROJECT_VERSION}")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "Install prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")
message(STATUS "Features:")
message(STATUS "  Protocol Buffers: ${Protobuf_FOUND}")
message(STATUS "  gRPC: ${gRPC_FOUND}")
message(STATUS "  WebSocket: ${WEBSOCKETPP_FOUND}")
message(STATUS "  Testing: ${BUILD_TESTING}")
message(STATUS "  Benchmarking: ${benchmark_FOUND}")
message(STATUS "  Documentation: ${DOXYGEN_FOUND}")
message(STATUS "  Code Formatting: ${CLANG_FORMAT}")
message(STATUS "  Static Analysis: ${CPPCHECK}")
message(STATUS "")
message(STATUS "Performance Optimizations: ${CMAKE_BUILD_TYPE STREQUAL \"Release\"}")
message(STATUS "Debug Features: ${CMAKE_BUILD_TYPE STREQUAL \"Debug\"}")
message(STATUS "=====================================================")
message(STATUS "")

# ========== BUILD SUMMARY ==========

# Display build configuration summary
add_custom_target(build_info
    COMMAND ${CMAKE_COMMAND} -E echo ""
    COMMAND ${CMAKE_COMMAND} -E echo "========== Build Information =========="
    COMMAND ${CMAKE_COMMAND} -E echo "Project: ${PROJECT_NAME} v${PROJECT_VERSION}"
    COMMAND ${CMAKE_COMMAND} -E echo "Build Type: ${CMAKE_BUILD_TYPE}"
    COMMAND ${CMAKE_COMMAND} -E echo "C++ Standard: C++${CMAKE_CXX_STANDARD}"
    COMMAND ${CMAKE_COMMAND} -E echo "Compiler: ${CMAKE_CXX_COMPILER}"
    COMMAND ${CMAKE_COMMAND} -E echo "Build Directory: ${CMAKE_BINARY_DIR}"
    COMMAND ${CMAKE_COMMAND} -E echo "Install Prefix: ${CMAKE_INSTALL_PREFIX}"
    COMMAND ${CMAKE_COMMAND} -E echo "======================================="
    COMMAND ${CMAKE_COMMAND} -E echo ""
    COMMENT "Displaying build information"
)
