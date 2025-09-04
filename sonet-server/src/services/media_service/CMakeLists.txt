##
# Copyright (c) 2025 Neo Qiss
# All rights reserved.
#
# This software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.
##

cmake_minimum_required(VERSION 3.20)

project(media_service LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(ENABLE_COVERAGE "Enable coverage flags" OFF)

if(ENABLE_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
	message(STATUS "Coverage enabled for media_service")
	add_compile_options(--coverage -O0 -g)
	add_link_options(--coverage)
endif()

find_package(Threads REQUIRED)

file(GLOB MEDIA_SERVICE_ALL_SOURCES
	${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/controllers/*.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/repositories/*.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/processors/*.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/storage/*.cpp
)

# Separate main for executable
set(MEDIA_SERVICE_MAIN ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
list(REMOVE_ITEM MEDIA_SERVICE_ALL_SOURCES ${MEDIA_SERVICE_MAIN})

# libpqxx (optional) - remove postgres source if not found
find_package(PkgConfig)
pkg_check_modules(PQXX libpqxx)
if(NOT PQXX_FOUND)
	list(REMOVE_ITEM MEDIA_SERVICE_ALL_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/repositories/notegres_repository.cpp)
	list(APPEND MEDIA_SERVICE_ALL_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/repositories/notegres_repository_stub.cpp)
endif()

add_library(media_service_core STATIC ${MEDIA_SERVICE_ALL_SOURCES})
add_executable(media_service ${MEDIA_SERVICE_MAIN})
# Ensure local include first
target_include_directories(media_service_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# Defer media_service to include same dirs
target_include_directories(media_service PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Generate gRPC/Protobuf sources for media service only
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
set(MEDIA_PROTO ${PROTO_DIR}/services/media.proto)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

find_program(PROTOC protoc)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)
if(PROTOC AND GRPC_CPP_PLUGIN)
	set(GEN_SRCS
		${GEN_DIR}/services/media.pb.cc
		${GEN_DIR}/services/media.grpc.pb.cc
	)
	set(GEN_HDRS
		${GEN_DIR}/services/media.pb.h
		${GEN_DIR}/services/media.grpc.pb.h
	)
	add_custom_command(
		OUTPUT ${GEN_SRCS} ${GEN_HDRS}
		COMMAND ${PROTOC}
		ARGS --grpc_out ${GEN_DIR}
			 --cpp_out ${GEN_DIR}
			 -I ${PROTO_DIR}
			 --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
			 ${MEDIA_PROTO}
		DEPENDS ${MEDIA_PROTO}
		COMMENT "Generating gRPC sources for media.proto"
	)
	target_sources(media_service_core PRIVATE ${GEN_SRCS})
	# CRITICAL: Put generated include directory before proto dir to avoid stubs
	target_include_directories(media_service_core PUBLIC ${GEN_DIR})
	# Do NOT include ${PROTO_DIR} for this target to avoid picking stub headers
else()
	message(WARNING "protoc or grpc_cpp_plugin not found; expecting pre-generated sources in include path.")
	# If protoc missing, we still need proto dir to find stubbed headers, but in real builds we avoid it
	target_include_directories(media_service PRIVATE ${PROTO_DIR})
endif()

# Link against proto aggregator and common libs
target_link_libraries(media_service_core PUBLIC sonet_proto)
# gRPC depends on absl and gpr; ensure link for executable
find_package(absl QUIET)
if(absl_FOUND)
	target_link_libraries(media_service PRIVATE absl::synchronization absl::strings)
else()
	# Fallback to system libs
	target_link_libraries(media_service PRIVATE absl_synchronization absl_strings)
endif()
# Link gRPC core libs explicitly if available
target_link_libraries(media_service PRIVATE grpc gpr grpc++)
target_link_libraries(media_service PRIVATE media_service_core sonet_proto)

# libpqxx (optional)
find_package(PkgConfig)
pkg_check_modules(PQXX libpqxx)
if(PQXX_FOUND)
	target_include_directories(media_service_core PUBLIC ${PQXX_INCLUDE_DIRS})
	target_link_libraries(media_service_core PUBLIC ${PQXX_LIBRARIES})
endif()

# Optional AWS SDK for S3 (SDK migration)
find_package(AWSSDK COMPONENTS s3 core QUIET)
if(AWSSDK_FOUND)
	message(STATUS "AWS SDK found; enabling native S3 backend")
	target_link_libraries(media_service PRIVATE AWS::aws-sdk-cpp::s3 AWS::aws-sdk-cpp::core)
	target_compile_definitions(media_service PRIVATE USE_AWS_SDK_S3=1)
else()
	message(STATUS "AWS SDK not found; falling back to CLI S3 backend implementation")
endif()

# Threads
target_link_libraries(media_service_core PUBLIC Threads::Threads)
target_link_libraries(media_service PRIVATE Threads::Threads)

if(OpenSSL_FOUND)
	target_link_libraries(media_service_core PUBLIC OpenSSL::Crypto OpenSSL::SSL)
endif()

install(TARGETS media_service RUNTIME DESTINATION bin)

if(BUILD_TESTS)
	add_executable(media_service_unit_tests
		${CMAKE_CURRENT_SOURCE_DIR}/tests/media_service_unit_tests.cpp
		${CMAKE_CURRENT_SOURCE_DIR}/tests/upload_integration_test.cpp
	)
	target_include_directories(media_service_unit_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
	target_link_libraries(media_service_unit_tests PRIVATE media_service_core)
	if(ENABLE_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
		target_compile_options(media_service_unit_tests PRIVATE --coverage -O0 -g)
		target_link_options(media_service_unit_tests PRIVATE --coverage)
	endif()
	enable_testing()
	add_test(NAME media_service_unit_tests COMMAND media_service_unit_tests)
endif()

