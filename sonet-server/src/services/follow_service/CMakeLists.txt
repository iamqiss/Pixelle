# Follow Service CMake Configuration
# Twitter-Scale Social Graph Service
cmake_minimum_required(VERSION 3.20)

project(sonet_follow_service
    VERSION 1.0.0
    DESCRIPTION "Twitter-Scale Follow Service for Sonet Network"
    LANGUAGES CXX
)

# ========== COMPILER CONFIGURATION ==========

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Performance optimization flags
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG -fsanitize=address,undefined")

# Warning flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Werror")

# ========== BUILD CONFIGURATION ==========

# Default to Release build
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

message(STATUS "🚀 Building Follow Service in ${CMAKE_BUILD_TYPE} mode")

# ========== DEPENDENCIES ==========

find_package(PkgConfig REQUIRED)

# Threading support
find_package(Threads REQUIRED)

# JSON library (nlohmann/json)
find_package(nlohmann_json REQUIRED)

# Logging library (spdlog)
find_package(spdlog REQUIRED)

# Protobuf and gRPC
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# HTTP server library (optional - would use actual implementation)
# find_package(httplib REQUIRED)

# Database client (postgresql)
# find_package(postgresql REQUIRED)

# Cache client (Redis)
# find_package(hiredis REQUIRED)

# Testing framework
find_package(GTest REQUIRED)

# Benchmarking
find_package(benchmark REQUIRED)

# ========== INCLUDE DIRECTORIES ==========

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/models
    ${CMAKE_CURRENT_SOURCE_DIR}/graph
    ${CMAKE_CURRENT_SOURCE_DIR}/controllers
    ${CMAKE_CURRENT_SOURCE_DIR}/repositories
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
    ${CMAKE_CURRENT_SOURCE_DIR}/../../core
)

# ========== PROTOBUF GENERATION ==========

# Generate gRPC and protobuf files
set(PROTO_FILES
    proto/follow_service.proto
)

set(PROTO_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${PROTO_GENERATED_DIR})

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    
    set(PROTO_SRCS ${PROTO_GENERATED_DIR}/proto/${PROTO_NAME}.pb.cc)
    set(PROTO_HDRS ${PROTO_GENERATED_DIR}/proto/${PROTO_NAME}.pb.h)
    set(GRPC_SRCS ${PROTO_GENERATED_DIR}/proto/${PROTO_NAME}.grpc.pb.cc)
    set(GRPC_HDRS ${PROTO_GENERATED_DIR}/proto/${PROTO_NAME}.grpc.pb.h)
    
    add_custom_command(
        OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
        COMMAND protobuf::protoc
        ARGS 
            --cpp_out=${PROTO_GENERATED_DIR}
            --grpc_out=${PROTO_GENERATED_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            -I${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}
        COMMENT "Generating protobuf and gRPC files for ${PROTO_FILE}"
    )
    
    list(APPEND ALL_PROTO_SRCS ${PROTO_SRCS} ${GRPC_SRCS})
    list(APPEND ALL_PROTO_HDRS ${PROTO_HDRS} ${GRPC_HDRS})
endforeach()

# ========== SOURCE FILES ==========

set(FOLLOW_SERVICE_SOURCES
    # Core service implementation
    service.cpp
    
    # Models
    models/follow.cpp
    models/relationship.cpp
    
    # Social graph engine
    graph/social_graph.cpp
    
    # Data persistence
    repositories/follow_repository.cpp
    
    # Generated protobuf files
    ${ALL_PROTO_SRCS}
)

set(FOLLOW_SERVICE_HEADERS
    # Core service headers
    service.h
    
    # Models
    models/follow.h
    models/relationship.h
    
    # Social graph engine
    graph/social_graph.h
    
    # HTTP controllers (header-only for now)
    controllers/follow_controller.h
    
    # Data persistence
    repositories/follow_repository.h
    
    # Generated protobuf headers
    ${ALL_PROTO_HDRS}
)

# ========== LIBRARY TARGET ==========

# Create follow service library
add_library(sonet_follow_service_lib STATIC
    ${FOLLOW_SERVICE_SOURCES}
    ${FOLLOW_SERVICE_HEADERS}
)

# Use std::format backend for spdlog to avoid compile-time fmt issues
target_compile_definitions(sonet_follow_service_lib PUBLIC SPDLOG_USE_STD_FORMAT=1 SPDLOG_HEADER_ONLY=1)

target_link_libraries(sonet_follow_service_lib
    PUBLIC
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        Threads::Threads
        protobuf::libprotobuf
        gRPC::grpc++
        gRPC::grpc++_reflection
)

target_include_directories(sonet_follow_service_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROTO_GENERATED_DIR}
)

# ========== EXECUTABLE TARGET ==========

option(BUILD_FOLLOW_EXECUTABLE "Build the Follow Service main executable" OFF)

if(BUILD_FOLLOW_EXECUTABLE)
  add_executable(sonet_follow_service
      main.cpp
  )

  target_compile_definitions(sonet_follow_service PRIVATE SPDLOG_USE_STD_FORMAT=1 SPDLOG_HEADER_ONLY=1)

  target_link_libraries(sonet_follow_service
      PRIVATE
          sonet_follow_service_lib
  )
endif()

# ========== PERFORMANCE OPTIMIZATION ==========

# Link-time optimization for Release builds
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set_property(TARGET sonet_follow_service_lib PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    if(BUILD_FOLLOW_EXECUTABLE)
      set_property(TARGET sonet_follow_service PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    endif()
    message(STATUS "🔥 Enabled link-time optimization for performance")
endif()

# ========== TESTING ==========

enable_testing()

# Only add tests if sources exist
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  file(GLOB TEST_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
  if(TEST_SOURCES)
    add_executable(follow_service_tests ${TEST_SOURCES})
    target_link_libraries(follow_service_tests PRIVATE sonet_follow_service_lib GTest::gtest GTest::gtest_main)
    add_test(NAME FollowServiceUnitTests COMMAND follow_service_tests)
  endif()
endif()

# Integration tests
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration)
  file(GLOB INTEGRATION_TEST_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration/*.cpp)
  if(INTEGRATION_TEST_SOURCES)
    add_executable(follow_service_integration_tests ${INTEGRATION_TEST_SOURCES})
    target_link_libraries(follow_service_integration_tests PRIVATE sonet_follow_service_lib GTest::gtest GTest::gtest_main)
    add_test(NAME FollowServiceIntegrationTests COMMAND follow_service_integration_tests)
  endif()
endif()

# ========== BENCHMARKING ==========
# Performance benchmarks
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks)
  file(GLOB BENCH_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/*.cpp)
  if(BENCH_SOURCES)
    add_executable(follow_service_benchmarks ${BENCH_SOURCES})
    target_link_libraries(follow_service_benchmarks PRIVATE sonet_follow_service_lib benchmark::benchmark benchmark::benchmark_main)
  endif()
endif()

# ========== INSTALLATION ==========

# Install targets
if(BUILD_FOLLOW_EXECUTABLE)
install(TARGETS sonet_follow_service
    RUNTIME DESTINATION bin
    COMPONENT Runtime
)
endif()

install(TARGETS sonet_follow_service_lib
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    COMPONENT Development
)

install(FILES ${FOLLOW_SERVICE_HEADERS}
    DESTINATION include/sonet/follow
    COMPONENT Development
)

# ========== PACKAGING ==========

set(CPACK_PACKAGE_NAME "sonet-follow-service")
set(CPACK_PACKAGE_VERSION_MAJOR 1)
set(CPACK_PACKAGE_VERSION_MINOR 0)
set(CPACK_PACKAGE_VERSION_PATCH 0)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Twitter-Scale Follow Service")
set(CPACK_PACKAGE_VENDOR "Neo Qiss")
set(CPACK_PACKAGE_CONTACT "support@neoqiss.com")

# Docker packaging
add_custom_target(docker_build
    COMMAND docker build -t sonet-follow-service:latest .
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Building Docker image for Follow Service"
)

# ========== CUSTOM TARGETS ==========

# Code formatting
add_custom_target(format
    COMMAND find ${CMAKE_CURRENT_SOURCE_DIR} -name "*.cpp" -o -name "*.h" | xargs clang-format -i
    COMMENT "Formatting code with clang-format"
)

# Static analysis
add_custom_target(analyze
    COMMAND cppcheck --enable=all --std=c++20 ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running static analysis with cppcheck"
)

# Performance profiling
add_custom_target(profile
    COMMAND perf record -g ./sonet_follow_service
    DEPENDS sonet_follow_service
    COMMENT "Profiling Follow Service with perf"
)

# Memory leak check
add_custom_target(memcheck
    COMMAND valgrind --tool=memcheck --leak-check=full ./sonet_follow_service
    DEPENDS sonet_follow_service
    COMMENT "Checking for memory leaks with Valgrind"
)

# ========== BUILD SUMMARY ==========

message(STATUS "📋 Follow Service Build Configuration:")
message(STATUS "   • Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "   • C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "   • Compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "   • Install Prefix: ${CMAKE_INSTALL_PREFIX}")

# Performance targets summary
message(STATUS "🎯 Performance Targets:")
message(STATUS "   • Sub-1ms follow/unfollow operations")
message(STATUS "   • Sub-2ms relationship checks")
message(STATUS "   • Sub-5ms follower/following lists")
message(STATUS "   • Sub-10ms friend recommendations")
message(STATUS "   • 10K+ concurrent requests")
message(STATUS "   • 100M+ users, 10B+ relationships")

# Available targets
message(STATUS "🎯 Available Make Targets:")
message(STATUS "   • make                    - Build all targets")
message(STATUS "   • make sonet_follow_service - Build main executable")
message(STATUS "   • make test               - Run all tests")
message(STATUS "   • make follow_service_benchmarks - Build benchmarks")
message(STATUS "   • make docker_build       - Build Docker image")
message(STATUS "   • make format             - Format code")
message(STATUS "   • make analyze            - Run static analysis")
message(STATUS "   • make profile            - Profile with perf")
message(STATUS "   • make memcheck           - Check memory leaks")
message(STATUS "   • make install            - Install binaries")

include(CPack)