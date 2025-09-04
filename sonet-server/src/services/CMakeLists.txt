#
# Copyright (c) 2025 Neo Qiss
# All rights reserved.
# 
# This software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.
#

cmake_minimum_required(VERSION 3.20)

option(BUILD_OTHER_SERVICES "Build additional services besides media" OFF)

# New fine-grained toggles to allow building a subset of services
option(BUILD_MEDIA_SERVICE "Build media service" ON)
option(BUILD_TIMELINE_SERVICE "Build timeline service" ON)
option(BUILD_USER_SERVICE "Build user service" OFF)
option(BUILD_MESSAGING_SERVICE "Build messaging service" OFF)
option(BUILD_SEARCH_SERVICE "Build search service" OFF)
option(BUILD_NOTIFICATION_SERVICE "Build notification service" OFF)
option(BUILD_MINIMAL_SERVICES "Build minimal gRPC-only services (search, notification)" ON)

# Media service
if(BUILD_MEDIA_SERVICE)
add_subdirectory(media_service)
endif()

if(BUILD_OTHER_SERVICES)
    if(BUILD_USER_SERVICE)
    add_subdirectory(user_service)
    endif()
    if(BUILD_MESSAGING_SERVICE)
    add_subdirectory(messaging_service)
    endif()
endif()

# Timeline service
if(BUILD_TIMELINE_SERVICE)
add_subdirectory(timeline_service)
endif()

# Newly implemented services
if(BUILD_SEARCH_SERVICE)
add_subdirectory(search_service)
endif()

if(BUILD_NOTIFICATION_SERVICE)
add_subdirectory(notification_service)
endif()

if(BUILD_MINIMAL_SERVICES)
add_subdirectory(minimal)
endif()

# Add other services as they're implemented
