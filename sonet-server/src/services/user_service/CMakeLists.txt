#
# Copyright (c) 2025 Neo Qiss
# All rights r# Link with all the good stuff we need
target_link_libraries(user_service PRIVATE
    sonet_user_proto
    gRPC::grpc++
    gRPC::grpc++_reflection
    protobuf::protobuf
    libpqxx::pqxx
    OpenSSL::SSL
    OpenSSL::Crypto
    Boost::system
    Boost::filesystem
    Boost::thread
    nlohmann_json::nlohmann_json
    spdlog::spdlog
    fmt::fmt
    uuid
)is software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.
#

cmake_minimum_required(VERSION 3.20)

project(UserService VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find packages
find_package(Protobuf REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
find_package(PkgConfig)
# Optional Boost
find_package(Boost COMPONENTS system filesystem thread)
# Prefer libpqxx if available
pkg_check_modules(PQXX libpqxx)

# Include directories
include_directories(include)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
endif()

# Sources
set(USER_SERVICE_SOURCES
    src/main.cpp
    src/user_service.cpp
    src/password_manager.cpp
    src/jwt_manager.cpp
    src/session_manager.cpp
    src/security_utils.cpp
    src/user_repository.cpp
    # controllers
    controllers/auth_controller.cpp
    controllers/user_controller.cpp
    controllers/profile_controller.cpp
)

# Headers (for IDEs)
set(USER_SERVICE_HEADERS
    include/user_service.h
    include/password_manager.h
    include/jwt_manager.h
    include/session_manager.h
    include/security_utils.h
    include/user_repository.h
    include/auth_manager.h
    include/rate_limiter.h
    include/email_service.h
    include/file_upload_service.h
    include/repository.h
    include/user_types.h
)

# Executable
add_executable(user_service ${USER_SERVICE_SOURCES} ${USER_SERVICE_HEADERS})

# Link generated proto library name according to your proto build; default to sonet_proto
# If your build generates a different target (e.g., sonet_user_proto), adjust here.
target_link_libraries(user_service PRIVATE
    sonet_proto
    protobuf::protobuf
    nlohmann_json::nlohmann_json
    spdlog::spdlog
    fmt::fmt
)

# Link gRPC libraries by name
find_library(GRPCPP_LIB grpc++)
find_library(GRPCPP_REFLECTION_LIB grpc++_reflection)
if(GRPCPP_LIB)
    target_link_libraries(user_service PRIVATE ${GRPCPP_LIB})
endif()
if(GRPCPP_REFLECTION_LIB)
    target_link_libraries(user_service PRIVATE ${GRPCPP_REFLECTION_LIB})
endif()

if(Boost_FOUND)
    target_link_libraries(user_service PRIVATE ${Boost_LIBRARIES})
endif()

if(PQXX_FOUND)
    target_link_libraries(user_service PRIVATE ${PQXX_LIBRARIES})
    target_include_directories(user_service PRIVATE ${PQXX_INCLUDE_DIRS})
else()
    message(STATUS "libpqxx not found via pkg-config; proceeding without explicit link. Adjust as needed.")
endif()

# Warnings
target_compile_options(user_service PRIVATE
    -Wall -Wextra -Wpedantic
    -Wno-unused-parameter
)

# Build type specific options
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(user_service PRIVATE DEBUG_MODE=1)
    target_compile_options(user_service PRIVATE -g -O0)
else()
    target_compile_definitions(user_service PRIVATE NDEBUG=1)
    target_compile_options(user_service PRIVATE -O3 -DNDEBUG)
endif()

# Tests
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

install(TARGETS user_service RUNTIME DESTINATION bin)
