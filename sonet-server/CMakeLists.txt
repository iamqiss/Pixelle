#
# Copyright (c) 2025 Neo Qiss
# All rights reserved.
# 
# This software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.
#

cmake_minimum_required(VERSION 3.20)
project(Sonet VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Conan setup
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR})
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Make Boost optional in this environment
find_package(Boost COMPONENTS system filesystem thread)
# Use protobuf; we'll locate gRPC plugin via find_program
find_package(OpenSSL)

# Proper gRPC and Protobuf configuration
# These are now required for production mode
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# Check if we have the required libraries
if(NOT Protobuf_FOUND OR NOT gRPC_FOUND)
    message(FATAL_ERROR "Protobuf and gRPC are required for production mode. Install: libprotobuf-dev protobuf-compiler libgrpc++-dev libgrpc-dev")
endif()

set(USE_STUB_IMPLEMENTATIONS OFF)
message(STATUS "Found Protobuf: ${Protobuf_VERSION}")
message(STATUS "Found gRPC: ${gRPC_VERSION}")

option(BUILD_GLOBAL_PROTO "Generate all protos at top-level" OFF)
find_program(PROTOC protoc)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
endif()
if(OpenSSL_FOUND)
    include_directories(${OPENSSL_INCLUDE_DIR})
endif()

# Add gRPC and Protobuf include directories
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${gRPC_INCLUDE_DIRS})

# Remove stub mode logic
if(BUILD_GLOBAL_PROTO)
    if(NOT PROTOC)
        message(FATAL_ERROR "protoc not found but BUILD_GLOBAL_PROTO=ON")
    endif()
    # Protocol Buffers
    set(PROTO_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${PROTO_OUTPUT_DIR})

    function(compile_proto_file proto_file)
        get_filename_component(proto_name ${proto_file} NAME_WE)
        set(proto_srcs "${PROTO_OUTPUT_DIR}/${proto_name}.pb.cc")
        set(proto_hdrs "${PROTO_OUTPUT_DIR}/${proto_name}.pb.h")
        set(grpc_srcs "${PROTO_OUTPUT_DIR}/${proto_name}.grpc.pb.cc")
        set(grpc_hdrs "${PROTO_OUTPUT_DIR}/${proto_name}.grpc.pb.h")
        add_custom_command(
            OUTPUT "${proto_srcs}" "${proto_hdrs}" "${grpc_srcs}" "${grpc_hdrs}"
            COMMAND ${PROTOC}
            ARGS --grpc_out "${PROTO_OUTPUT_DIR}"
                 --cpp_out "${PROTO_OUTPUT_DIR}"
                 -I "${CMAKE_CURRENT_SOURCE_DIR}/proto"
                 --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
                 "${proto_file}"
            DEPENDS "${proto_file}"
            COMMENT "Generating protocol buffer files for ${proto_file}"
        )
        set(PROTO_SRCS ${PROTO_SRCS} ${proto_srcs} ${grpc_hdrs} PARENT_SCOPE)
        set(PROTO_HDRS ${PROTO_HDRS} ${proto_hdrs} ${grpc_hdrs} PARENT_SCOPE)
    endfunction()

    file(GLOB_RECURSE PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/proto/*.proto")
    foreach(proto_file ${PROTO_FILES})
        compile_proto_file(${proto_file})
    endforeach()

    add_library(sonet_proto ${PROTO_SRCS})
    target_link_libraries(sonet_proto 
        gRPC::grpc++ 
        gRPC::grpc++_reflection
        protobuf::libprotobuf
    )
    if(OpenSSL_FOUND)
        target_link_libraries(sonet_proto OpenSSL::Crypto OpenSSL::SSL)
    endif()
    target_include_directories(sonet_proto PUBLIC ${PROTO_OUTPUT_DIR})
else()
    # Do NOT globally add proto include dir; each target should opt-in to avoid stub collisions
    add_library(sonet_proto INTERFACE)
    target_link_libraries(sonet_proto INTERFACE 
        gRPC::grpc++ 
        gRPC::grpc++_reflection
        protobuf::libprotobuf
    )
endif()

# Add subdirectories
# Enable proto generation for real mode
if(BUILD_GLOBAL_PROTO)
    add_subdirectory(proto)
endif()
add_subdirectory(src/services)
add_subdirectory(src/gateway)
option(BUILD_EXTERNAL "Build external helper libs" OFF)
if(BUILD_EXTERNAL)
    add_subdirectory(external)
endif()

# Ensure all targets have access to real headers
# Do NOT include ${CMAKE_CURRENT_SOURCE_DIR}/proto globally to avoid stub collisions
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external)

# Tests
option(BUILD_TESTS "Build test targets" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Install configuration
install(TARGETS sonet_proto
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)
