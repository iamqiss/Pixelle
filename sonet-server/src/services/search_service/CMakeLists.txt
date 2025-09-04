# CMakeLists.txt for Sonet Search Service
# Copyright (c) 2025 Neo Qiss
#
# This builds a Twitter-scale search service with real-time indexing,
# advanced analytics, and high-performance search capabilities.

cmake_minimum_required(VERSION 3.20)

project(sonet_search_service
    VERSION 1.0.0
    DESCRIPTION "Sonet Twitter-Scale Search Service"
    LANGUAGES CXX
)

# Options to reduce dependency footprint for local builds
option(ENABLE_GRPC "Enable gRPC and protobuf components" ON)
option(BUILD_TESTS "Build unit tests" OFF)
option(BUILD_BENCHMARKS "Build performance benchmarks" OFF)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG")

# Enable position independent code
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find required packages
find_package(PkgConfig REQUIRED)

# Core dependencies
find_package(Threads REQUIRED)
find_package(OpenSSL QUIET)

# JSON library
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

# cURL for HTTP requests
find_package(CURL QUIET)
if(NOT CURL_FOUND)
    message(STATUS "libcurl not found, building without HTTP support (Elasticsearch requests will throw)")
    add_compile_definitions(HAVE_CURL=0)
else()
    add_compile_definitions(HAVE_CURL=1)
    target_link_libraries(sonet_search_service PRIVATE CURL::libcurl)
endif()

# Elasticsearch C++ client (optional placeholder)
find_path(ELASTICSEARCH_INCLUDE_DIR
    NAMES elasticsearch/client.hpp
    PATHS /usr/include /usr/local/include
)

find_library(ELASTICSEARCH_LIBRARY
    NAMES elasticsearch_client
    PATHS /usr/lib /usr/local/lib
)

if(ELASTICSEARCH_INCLUDE_DIR AND ELASTICSEARCH_LIBRARY)
    set(ELASTICSEARCH_FOUND TRUE)
    message(STATUS "Found Elasticsearch C++ client")
else()
    message(STATUS "Elasticsearch C++ client not found, using HTTP fallback")
    set(ELASTICSEARCH_FOUND FALSE)
endif()

# gRPC and Protocol Buffers (optional)
if(ENABLE_GRPC)
    find_package(Protobuf REQUIRED)
    find_package(gRPC REQUIRED)
endif()

# Redis C++ client (optional)
find_path(REDIS_INCLUDE_DIR
    NAMES sw/redis++/redis++.h
    PATHS /usr/include /usr/local/include
)

find_library(REDIS_LIBRARY
    NAMES redis++
    PATHS /usr/lib /usr/local/lib
)

if(REDIS_INCLUDE_DIR AND REDIS_LIBRARY)
    set(REDIS_FOUND TRUE)
    message(STATUS "Found Redis C++ client")
else()
    message(STATUS "Redis C++ client not found, caching will be limited")
    set(REDIS_FOUND FALSE)
endif()

# MongoDB C++ driver (optional)
find_path(MONGOCXX_INCLUDE_DIR
    NAMES mongocxx/client.hpp
    PATHS /usr/include /usr/local/include
)

find_library(MONGOCXX_LIBRARY
    NAMES mongocxx
    PATHS /usr/lib /usr/local/lib
)

if(MONGOCXX_INCLUDE_DIR AND MONGOCXX_LIBRARY)
    set(MONGOCXX_FOUND TRUE)
    message(STATUS "Found MongoDB C++ driver")
else()
    message(STATUS "MongoDB C++ driver not found, some features will be limited")
    set(MONGOCXX_FOUND FALSE)
endif()

# HTTP server library (mongoose) (optional)
find_path(MONGOOSE_INCLUDE_DIR
    NAMES mongoose.h
    PATHS /usr/include /usr/local/include
)

find_library(MONGOOSE_LIBRARY
    NAMES mongoose
    PATHS /usr/lib /usr/local/lib
)

if(NOT MONGOOSE_INCLUDE_DIR OR NOT MONGOOSE_LIBRARY)
    message(STATUS "Mongoose not found, using embedded or alternative server later")
endif()

# Language processing (optional)
find_path(CPPJIEBA_INCLUDE_DIR
    NAMES cppjieba/Jieba.hpp
    PATHS /usr/include /usr/local/include
)

if(CPPJIEBA_INCLUDE_DIR)
    set(CPPJIEBA_FOUND TRUE)
    message(STATUS "Found cppjieba for text processing")
else()
    message(STATUS "cppjieba not found, using basic text processing")
    set(CPPJIEBA_FOUND FALSE)
endif()

# Core source files
set(SEARCH_SERVICE_SOURCES
    # Models
    models/search_query.cpp
    models/search_result.cpp
    
    # Engines
    engines/elasticsearch_engine.cpp
    
    # Indexers
    indexers/note_indexer.cpp
    indexers/user_indexer.cpp
    
    # Controllers
    controllers/search_controller.cpp
    
    # Service orchestration
    service.cpp
    
    # Main entry point
    main.cpp
)

# Create the main executable
add_executable(sonet_search_service ${SEARCH_SERVICE_SOURCES})

# Set target properties
set_target_properties(sonet_search_service PROPERTIES
    OUTPUT_NAME "sonet-search-service"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Include directories
set(LOCAL_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../../core)
if(ELASTICSEARCH_INCLUDE_DIR)
    list(APPEND LOCAL_INCLUDE_DIRS ${ELASTICSEARCH_INCLUDE_DIR})
endif()
if(REDIS_INCLUDE_DIR)
    list(APPEND LOCAL_INCLUDE_DIRS ${REDIS_INCLUDE_DIR})
endif()
if(MONGOCXX_INCLUDE_DIR)
    list(APPEND LOCAL_INCLUDE_DIRS ${MONGOCXX_INCLUDE_DIR})
endif()
if(MONGOOSE_INCLUDE_DIR)
    list(APPEND LOCAL_INCLUDE_DIRS ${MONGOOSE_INCLUDE_DIR})
endif()
if(CPPJIEBA_INCLUDE_DIR)
    list(APPEND LOCAL_INCLUDE_DIRS ${CPPJIEBA_INCLUDE_DIR})
endif()

target_include_directories(sonet_search_service PRIVATE ${LOCAL_INCLUDE_DIRS})

# Link libraries
target_link_libraries(sonet_search_service PRIVATE
    Threads::Threads
    nlohmann_json::nlohmann_json
)

if(OpenSSL_FOUND)
    target_link_libraries(sonet_search_service PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(sonet_search_service PRIVATE HAVE_OPENSSL)
endif()

if(ENABLE_GRPC)
    target_link_libraries(sonet_search_service PRIVATE gRPC::grpc++ protobuf::libprotobuf)
    target_compile_definitions(sonet_search_service PRIVATE HAVE_GRPC)
endif()

if(ELASTICSEARCH_FOUND)
    target_link_libraries(sonet_search_service PRIVATE ${ELASTICSEARCH_LIBRARY})
    target_compile_definitions(sonet_search_service PRIVATE HAVE_ELASTICSEARCH)
endif()

if(REDIS_FOUND)
    target_link_libraries(sonet_search_service PRIVATE ${REDIS_LIBRARY})
    target_compile_definitions(sonet_search_service PRIVATE HAVE_REDIS)
endif()

if(MONGOCXX_FOUND)
    target_link_libraries(sonet_search_service PRIVATE ${MONGOCXX_LIBRARY})
    target_compile_definitions(sonet_search_service PRIVATE HAVE_MONGOCXX)
endif()

if(MONGOOSE_LIBRARY)
    target_link_libraries(sonet_search_service PRIVATE ${MONGOOSE_LIBRARY})
    target_compile_definitions(sonet_search_service PRIVATE HAVE_MONGOOSE)
endif()

if(CPPJIEBA_FOUND)
    target_compile_definitions(sonet_search_service PRIVATE HAVE_CPPJIEBA)
endif()

# Compiler-specific options
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(sonet_search_service PRIVATE
        -Wall -Wextra -Wpedantic
        -Wno-unused-parameter
        -Wno-missing-field-initializers
    )
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        target_compile_options(sonet_search_service PRIVATE
            -O3 -march=native -mtune=native
            -flto
        )
        target_link_options(sonet_search_service PRIVATE -flto)
    endif()
endif()

# Platform-specific settings
if(UNIX AND NOT APPLE)
    target_link_libraries(sonet_search_service PRIVATE rt dl)
elseif(APPLE)
    target_link_libraries(sonet_search_service PRIVATE "-framework CoreFoundation")
elseif(WIN32)
    target_link_libraries(sonet_search_service PRIVATE ws2_32 wsock32)
endif()

# gRPC/Protobuf code generation (optional)
if(ENABLE_GRPC)
    set(PROTO_FILES
        ${CMAKE_SOURCE_DIR}/proto/services/search.proto
        ${CMAKE_SOURCE_DIR}/proto/common/common.proto
        ${CMAKE_SOURCE_DIR}/proto/common/pagination.proto
    )

    foreach(PROTO_FILE ${PROTO_FILES})
        get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
        
        set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.cc")
        set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.h")
        set(GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.cc")
        set(GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.h")
        
        add_custom_command(
            OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
            COMMAND protobuf::protoc
            ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
                 --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
                 -I "${CMAKE_SOURCE_DIR}/proto"
                 --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
                 "${PROTO_FILE}"
            DEPENDS ${PROTO_FILE}
            COMMENT "Generating protocol buffer files for ${PROTO_NAME}"
        )
        
        target_sources(sonet_search_service PRIVATE ${PROTO_SRCS} ${GRPC_SRCS})
    endforeach()

    target_include_directories(sonet_search_service PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
endif()

# Tests and Benchmarks are disabled by default via options above

# Installation rules
install(TARGETS sonet_search_service
    RUNTIME DESTINATION bin
    COMPONENT Runtime
)

# CPack configuration for packaging (summary only)
set(CPACK_PACKAGE_NAME "sonet-search-service")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION "Sonet Twitter-Scale Search Service")
set(CPACK_PACKAGE_CONTACT "Neo Qiss <neo@sonet.dev>")
set(CPACK_PACKAGE_VENDOR "Sonet")

include(CPack)

# Build configuration summary
message(STATUS "")
message(STATUS "==== Sonet Search Service Build Configuration ====")
message(STATUS "Build Type:              ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard:            ${CMAKE_CXX_STANDARD}")
message(STATUS "Install Prefix:          ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")
message(STATUS "Optional Dependencies:")
message(STATUS "  OpenSSL:               ${OpenSSL_FOUND}")
message(STATUS "  Elasticsearch:         ${ELASTICSEARCH_FOUND}")
message(STATUS "  Redis:                 ${REDIS_FOUND}")
message(STATUS "  MongoDB:               ${MONGOCXX_FOUND}")
message(STATUS "  cppjieba:              ${CPPJIEBA_FOUND}")
message(STATUS "  gRPC/Protobuf:         ${ENABLE_GRPC}")
message(STATUS "==================================================")