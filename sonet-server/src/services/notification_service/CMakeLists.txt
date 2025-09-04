#
# Copyright (c) 2025 Neo Qiss
# 
# This file is part of Sonet - a social media platform built for real connections.
# 
# CMake configuration for the notification service.
# I designed this build system to handle all the dependencies and create
# a production-ready notification service binary.
#

cmake_minimum_required(VERSION 3.20)
project(sonet_notification_service VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags for performance and debugging
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG")

# Find required packages
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)

# Make external deps optional for minimal builds
set(ENABLE_DB ON)
set(ENABLE_REDIS ON)
set(ENABLE_HTTP ON)
set(ENABLE_TLS ON)
set(ENABLE_UUID ON)
set(ENABLE_JWT ON)

# Find postgresql (optional)
find_package(postgresql QUIET)
if(NOT NotegreSQL_FOUND)
    set(ENABLE_DB OFF)
    message(STATUS "postgresql not found; building without DB support")
endif()

# Find Redis (hiredis) (optional)
pkg_check_modules(HIREDIS QUIET hiredis)
if(NOT HIREDIS_FOUND)
    set(ENABLE_REDIS OFF)
    message(STATUS "hiredis not found; building without Redis support")
endif()

# Find libcurl for HTTP clients (optional)
find_package(CURL QUIET)
if(NOT CURL_FOUND)
    set(ENABLE_HTTP OFF)
    message(STATUS "libcurl not found; building without HTTP client support")
endif()

# Find OpenSSL for cryptography (optional)
find_package(OpenSSL QUIET)
if(NOT OpenSSL_FOUND)
    set(ENABLE_TLS OFF)
    message(STATUS "OpenSSL not found; building without TLS support")
endif()

# Find UUID library (optional)
pkg_check_modules(UUID QUIET uuid)
if(NOT UUID_FOUND)
    set(ENABLE_UUID OFF)
    message(STATUS "libuuid not found; building without UUID support")
endif()

# Find gRPC and Protobuf
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# Find nlohmann/json
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
    message(STATUS "nlohmann_json not found, fetching via FetchContent")
    include(FetchContent)
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.3
    )
    FetchContent_MakeAvailable(nlohmann_json)
endif()

# Find jwt-cpp (optional)
find_package(jwt-cpp QUIET)
if(NOT jwt-cpp_FOUND)
    set(ENABLE_JWT OFF)
    message(STATUS "jwt-cpp not found; building without JWT features")
endif()

# Find httplib (if not found, we'll use a fallback)
find_path(HTTPLIB_INCLUDE_DIR httplib.h)
if(NOT HTTPLIB_INCLUDE_DIR)
    message(WARNING "httplib not found, using bundled version")
    set(HTTPLIB_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/httplib")
endif()

# Find WebSocketPP
find_path(WEBSOCKETPP_INCLUDE_DIR websocketpp/config/asio_no_tls.hpp)
if(NOT WEBSOCKETPP_INCLUDE_DIR)
    message(WARNING "WebSocketPP not found, using bundled version")
    set(WEBSOCKETPP_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/websocketpp")
endif()

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../core
    ${CMAKE_CURRENT_SOURCE_DIR}/../../external
    ${HTTPLIB_INCLUDE_DIR}
    ${WEBSOCKETPP_INCLUDE_DIR}
)
if(ENABLE_DB)
    include_directories(${PostgreSQL_INCLUDE_DIRS})
endif()
if(ENABLE_REDIS)
    include_directories(${HIREDIS_INCLUDE_DIRS})
endif()
if(ENABLE_UUID)
    include_directories(${UUID_INCLUDE_DIRS})
endif()

# Protocol buffer files
set(PROTO_FILES
    ${CMAKE_SOURCE_DIR}/proto/services/notification.proto
    ${CMAKE_SOURCE_DIR}/proto/common/common.proto
    ${CMAKE_SOURCE_DIR}/proto/common/timestamp.proto
)

# Generate protobuf and gRPC files
set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${PROTO_SRC_DIR})

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)
    
    set(PROTO_SRCS ${PROTO_SRC_DIR}/${PROTO_NAME}.pb.cc)
    set(PROTO_HDRS ${PROTO_SRC_DIR}/${PROTO_NAME}.pb.h)
    set(GRPC_SRCS ${PROTO_SRC_DIR}/${PROTO_NAME}.grpc.pb.cc)
    set(GRPC_HDRS ${PROTO_SRC_DIR}/${PROTO_NAME}.grpc.pb.h)
    
    add_custom_command(
        OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
        COMMAND protobuf::protoc
        ARGS --grpc_out ${PROTO_SRC_DIR}
             --cpp_out ${PROTO_SRC_DIR}
             --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
             -I ${CMAKE_SOURCE_DIR}/proto
             ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating protobuf and gRPC files for ${PROTO_NAME}"
    )
    
    list(APPEND ALL_PROTO_SRCS ${PROTO_SRCS} ${GRPC_SRCS})
    list(APPEND ALL_PROTO_HDRS ${PROTO_HDRS} ${GRPC_HDRS})
endforeach()

# Source files for models
set(MODEL_SOURCES
    models/notification.cpp
)

# Source files for repositories  
set(REPOSITORY_SOURCES
    repositories/notification_repository.cpp
)

# Source files for processors
set(PROCESSOR_SOURCES
    processors/notification_processor.cpp
)

# Source files for channels
set(CHANNEL_SOURCES
    channels/email_channel.cpp
    channels/push_channel.cpp
    channels/websocket_channel.cpp
)

# Ensure httplib include path if bundled
include_directories(${HTTPLIB_INCLUDE_DIR})

# Source files for controllers
set(CONTROLLER_SOURCES
    controllers/notification_controller.cpp
)

# Main service files
set(SERVICE_SOURCES
    service.cpp
    main.cpp
)

# All source files
set(ALL_SOURCES
    ${MODEL_SOURCES}
    ${REPOSITORY_SOURCES}
    ${PROCESSOR_SOURCES}
    ${CHANNEL_SOURCES}
    ${CONTROLLER_SOURCES}
    ${SERVICE_SOURCES}
    ${ALL_PROTO_SRCS}
)

# Create the notification service executable
add_executable(sonet_notification_service ${ALL_SOURCES})

# Set target properties
set_target_properties(sonet_notification_service PROPERTIES
    OUTPUT_NAME "sonet-notification-service"
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Link libraries
# Core
target_link_libraries(sonet_notification_service PRIVATE
    Threads::Threads
    gRPC::grpc++
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    nlohmann_json::nlohmann_json
)

# Optional libs
if(ENABLE_HTTP)
    target_link_libraries(sonet_notification_service PRIVATE CURL::libcurl)
endif()
if(ENABLE_TLS)
    target_link_libraries(sonet_notification_service PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()
if(ENABLE_DB)
    target_link_libraries(sonet_notification_service PRIVATE ${PostgreSQL_LIBRARIES})
endif()
if(ENABLE_REDIS)
    target_link_libraries(sonet_notification_service PRIVATE ${HIREDIS_LIBRARIES})
endif()
if(ENABLE_UUID)
    target_link_libraries(sonet_notification_service PRIVATE ${UUID_LIBRARIES})
endif()
if(ENABLE_JWT)
    target_link_libraries(sonet_notification_service PRIVATE jwt-cpp::jwt-cpp)
endif()

# Compiler definitions
target_compile_definitions(sonet_notification_service PRIVATE
    SONET_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
    SONET_VERSION_MINOR=${PROJECT_VERSION_MINOR}
    SONET_VERSION_PATCH=${PROJECT_VERSION_PATCH}
    SONET_VERSION_STRING="${PROJECT_VERSION}"
)

# Include proto headers
target_include_directories(sonet_notification_service PRIVATE ${PROTO_SRC_DIR})

# Platform-specific libraries
if(WIN32)
    target_link_libraries(sonet_notification_service PRIVATE ws2_32 wsock32)
elseif(UNIX AND NOT APPLE)
    target_link_libraries(sonet_notification_service PRIVATE dl rt)
endif()

# Installation rules
install(TARGETS sonet_notification_service
    RUNTIME DESTINATION bin
    COMPONENT notification_service
)

# Install configuration files
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../config/development/services.json
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../config/production/services.json
    DESTINATION etc/sonet
    COMPONENT notification_service
)

# Install systemd service file (Linux only)
if(UNIX AND NOT APPLE)
    install(FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/deployment/sonet-notification-service.service
        DESTINATION lib/systemd/system
        COMPONENT notification_service
        OPTIONAL
    )
endif()

# Development and testing targets
if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    # Add debug symbols
    target_compile_options(sonet_notification_service PRIVATE -g)
    
    # Enable sanitizers for debugging
    option(ENABLE_SANITIZERS "Enable AddressSanitizer and UBSan" OFF)
    if(ENABLE_SANITIZERS)
        target_compile_options(sonet_notification_service PRIVATE
            -fsanitize=address,undefined
            -fno-omit-frame-pointer
        )
        target_link_options(sonet_notification_service PRIVATE
            -fsanitize=address,undefined
        )
    endif()
endif()

# Performance profiling support
option(ENABLE_PROFILING "Enable performance profiling" OFF)
if(ENABLE_PROFILING)
    target_compile_options(sonet_notification_service PRIVATE -pg)
    target_link_options(sonet_notification_service PRIVATE -pg)
endif()

# Static analysis support
option(ENABLE_STATIC_ANALYSIS "Enable static analysis with clang-tidy" OFF)
if(ENABLE_STATIC_ANALYSIS)
    find_program(CLANG_TIDY_EXE NAMES "clang-tidy")
    if(CLANG_TIDY_EXE)
        set_target_properties(sonet_notification_service PROPERTIES
            CXX_CLANG_TIDY "${CLANG_TIDY_EXE};-checks=*,-readability-*"
        )
    endif()
endif()

# Docker targets omitted in minimal build

# Package configuration
set(CPACK_PACKAGE_NAME "sonet-notification-service")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Sonet Notification Service")
set(CPACK_PACKAGE_DESCRIPTION "Real-time notification delivery service for Sonet social media platform")
set(CPACK_PACKAGE_VENDOR "Neo Qiss")
set(CPACK_PACKAGE_CONTACT "notifications@sonet.app")

# Debian package configuration
set(CPACK_DEBIAN_PACKAGE_DEPENDS "libcurl4, libpq5, libhiredis0.14, libuuid1, libssl3")
set(CPACK_DEBIAN_PACKAGE_SECTION "web")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")

# RPM package configuration  
set(CPACK_RPM_PACKAGE_REQUIRES "libcurl, postgresql-libs, hiredis, libuuid, openssl-libs")
set(CPACK_RPM_PACKAGE_GROUP "Applications/Internet")

include(CPack)

# Print build information
message(STATUS "")
message(STATUS "Sonet Notification Service Build Configuration:")
message(STATUS "  Version: ${PROJECT_VERSION}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  Install Prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")
message(STATUS "Features:")
message(STATUS "  DB: ${ENABLE_DB}")
message(STATUS "  Redis: ${ENABLE_REDIS}")
message(STATUS "  HTTP: ${ENABLE_HTTP}")
message(STATUS "  TLS: ${ENABLE_TLS}")
message(STATUS "  UUID: ${ENABLE_UUID}")
message(STATUS "  JWT: ${ENABLE_JWT}")
message(STATUS "")