# Video Feed Service CMakeLists.txt
cmake_minimum_required(VERSION 3.22)

# Define the video feed service library
add_library(video_feed_service STATIC
    service.cpp
    video_ml_service.cpp
    content_filtering_service.cpp
    user_engagement_service.cpp
    real_time_update_service.cpp
    video_feed_repository.cpp
    cache_impl.cpp
    database_impl.cpp
    logger_impl.cpp
)

# Set include directories
target_include_directories(video_feed_service PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/proto
    ${CMAKE_SOURCE_DIR}/proto/services
    ${CMAKE_SOURCE_DIR}/proto/common
    ${CMAKE_SOURCE_DIR}/src/common
    ${CMAKE_SOURCE_DIR}/src/utils
    ${CMAKE_SOURCE_DIR}/src/cache
    ${CMAKE_SOURCE_DIR}/src/database
)

# Link required libraries
find_package(Threads REQUIRED)
target_link_libraries(video_feed_service PUBLIC
    Threads::Threads
    sonet_proto  # gRPC and protobuf support
    ${CMAKE_SOURCE_DIR}/src/common/libsonet_common.a
    ${CMAKE_SOURCE_DIR}/src/utils/libsonet_utils.a
    ${CMAKE_SOURCE_DIR}/src/cache/libsonet_cache.a
    ${CMAKE_SOURCE_DIR}/src/database/libsonet_database.a
)

# Set C++ standard
set_target_properties(video_feed_service PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# Compiler flags for performance
target_compile_options(video_feed_service PRIVATE
    $<$<CXX_COMPILER_ID:GNU>:-O3 -march=native -mtune=native>
    $<$<CXX_COMPILER_ID:Clang>:-O3 -march=native -mtune=native>
    $<$<CXX_COMPILER_ID:MSVC>:/O2 /arch:AVX2>
)

# Add video feed service executable for standalone testing
add_executable(video_feed_service_main
    main.cpp
)

target_link_libraries(video_feed_service_main
    video_feed_service
    Threads::Threads
    sonet_proto
)

# Set properties for the executable
set_target_properties(video_feed_service_main PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    OUTPUT_NAME video_feed_service
)

# Performance optimization flags for executable
target_compile_options(video_feed_service_main PRIVATE
    $<$<CXX_COMPILER_ID:GNU>:-O3 -march=native -mtune=native>
    $<$<CXX_COMPILER_ID:Clang>:-O3 -march=native -mtune=native>
    $<$<CXX_COMPILER_ID:MSVC>:/O2 /arch:AVX2>
)

# Install targets
install(TARGETS video_feed_service
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include
)

install(TARGETS video_feed_service_main
    RUNTIME DESTINATION bin
)

# Install headers
install(FILES
    service.h
    video_ml_service.h
    content_filtering_service.h
    user_engagement_service.h
    real_time_update_service.h
    video_feed_repository.h
    cache_impl.h
    database_impl.h
    logger_impl.h
    DESTINATION include/sonet/video_feed
)

# Add to parent CMakeLists.txt
message(STATUS "Video Feed service configured successfully")

# Optional: Build tests if enabled
option(BUILD_VIDEO_FEED_TESTS "Build video feed service tests" OFF)
if(BUILD_VIDEO_FEED_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Optional: Performance profiling
option(ENABLE_PERF_PROFILING "Enable performance profiling" OFF)
if(ENABLE_PERF_PROFILING)
    target_compile_definitions(video_feed_service PRIVATE ENABLE_PERF_PROFILING=1)
    target_compile_definitions(video_feed_service_main PRIVATE ENABLE_PERF_PROFILING=1)
    
    # Link profiling libraries
    find_package(gperftools QUIET)
    if(gperftools_FOUND)
        target_link_libraries(video_feed_service PRIVATE gperftools::profiler)
        target_link_libraries(video_feed_service_main PRIVATE gperftools::profiler)
    endif()
endif()

# Optional: Memory profiling
option(ENABLE_MEMORY_PROFILING "Enable memory profiling" OFF)
if(ENABLE_MEMORY_PROFILING)
    target_compile_definitions(video_feed_service PRIVATE ENABLE_MEMORY_PROFILING=1)
    target_compile_definitions(video_feed_service_main PRIVATE ENABLE_MEMORY_PROFILING=1)
    
    # Link memory profiling libraries
    find_package(heaptrack QUIET)
    if(heaptrack_FOUND)
        target_link_libraries(video_feed_service PRIVATE heaptrack::heaptrack)
        target_link_libraries(video_feed_service_main PRIVATE heaptrack::heaptrack)
    endif()
endif()

# Optional: SIMD optimizations
option(ENABLE_SIMD_OPTIMIZATIONS "Enable SIMD optimizations" ON)
if(ENABLE_SIMD_OPTIMIZATIONS)
    target_compile_definitions(video_feed_service PRIVATE ENABLE_SIMD_OPTIMIZATIONS=1)
    target_compile_definitions(video_feed_service_main PRIVATE ENABLE_SIMD_OPTIMIZATIONS=1)
    
    # Add SIMD-specific compiler flags
    target_compile_options(video_feed_service PRIVATE
        $<$<CXX_COMPILER_ID:GNU>:-mavx2 -mfma -msse4.2>
        $<$<CXX_COMPILER_ID:Clang>:-mavx2 -mfma -msse4.2>
        $<$<CXX_COMPILER_ID:MSVC>:/arch:AVX2>
    )
    
    target_compile_options(video_feed_service_main PRIVATE
        $<$<CXX_COMPILER_ID:GNU>:-mavx2 -mfma -msse4.2>
        $<$<CXX_COMPILER_ID:Clang>:-mavx2 -mfma -msse4.2>
        $<$<CXX_COMPILER_ID:MSVC>:/arch:AVX2>
    )
endif()

# Optional: Link time optimization
option(ENABLE_LTO "Enable link time optimization" OFF)
if(ENABLE_LTO)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    target_compile_options(video_feed_service PRIVATE
        $<$<CXX_COMPILER_ID:GNU>:-flto>
        $<$<CXX_COMPILER_ID:Clang>:-flto>
        $<$<CXX_COMPILER_ID:MSVC>:/GL>
    )
    
    target_compile_options(video_feed_service_main PRIVATE
        $<$<CXX_COMPILER_ID:GNU>:-flto>
        $<$<CXX_COMPILER_ID:Clang>:-flto>
        $<$<CXX_COMPILER_ID:MSVC>:/GL>
    )
endif()