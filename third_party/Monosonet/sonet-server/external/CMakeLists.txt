#
# Copyright (c) 2025 Neo Qiss
# All rights reserved.
# 
# This software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.
#

cmake_minimum_required(VERSION 3.20)

# External dependencies CMakeLists
project(SonetExternal)

# Database connection helpers
add_subdirectory(database)

# gRPC and HTTP utilities
add_subdirectory(grpc)
add_subdirectory(http)

# JSON utilities
add_subdirectory(json)

# Redis utilities  
add_subdirectory(redis)
