# Proto CMakeLists.txt
cmake_minimum_required(VERSION 3.22)

# Check for protobuf and grpc
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED grpc++)

# Find gRPC installation
# Looks for gRPC pkg-config module, for CMAKE to install
find_program(_PROTOBUF_PROTOC protoc)
find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin
    HINTS /usr/local/bin /usr/bin
    PATHS /usr/local/bin /usr/bin
)
if(NOT _GRPC_CPP_PLUGIN_EXECUTABLE)
    set(_GRPC_CPP_PLUGIN_EXECUTABLE "/usr/local/bin/grpc_cpp_plugin")
endif()

# Proto file directory
set(PROTO_PATH "${CMAKE_CURRENT_SOURCE_DIR}")
set(GENERATED_PROTOBUF_PATH "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

# Function to generate C++ files from proto files
function(protobuf_generate_grpc_cpp SRC_FILES HDR_FILES)
    if(NOT ARGN)
        message(SEND_ERROR "Error: protobuf_generate_grpc_cpp() called without any proto files")
        return()
    endif()

    set(${SRC_FILES})
    set(${HDR_FILES})
    
    foreach(PROTO_FILE ${ARGN})
        get_filename_component(PROTO_FILE_ABS ${PROTO_FILE} ABSOLUTE)
        get_filename_component(PROTO_FILE_DIR ${PROTO_FILE_ABS} DIRECTORY)
        get_filename_component(PROTO_FILE_NAME_WE ${PROTO_FILE_ABS} NAME_WE)
        
        set(PROTO_SRCS "${GENERATED_PROTOBUF_PATH}/${PROTO_FILE_NAME_WE}.pb.cc")
        set(PROTO_HDRS "${GENERATED_PROTOBUF_PATH}/${PROTO_FILE_NAME_WE}.pb.h")
        set(GRPC_SRCS "${GENERATED_PROTOBUF_PATH}/${PROTO_FILE_NAME_WE}.grpc.pb.cc")
        set(GRPC_HDRS "${GENERATED_PROTOBUF_PATH}/${PROTO_FILE_NAME_WE}.grpc.pb.h")
        
        add_custom_command(
            OUTPUT "${PROTO_SRCS}" "${PROTO_HDRS}" "${GRPC_SRCS}" "${GRPC_HDRS}"
            COMMAND ${_PROTOBUF_PROTOC}
            ARGS --grpc_out "${GENERATED_PROTOBUF_PATH}"
                 --cpp_out "${GENERATED_PROTOBUF_PATH}"
                 -I "${PROTO_PATH}"
                 --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
                 "${PROTO_FILE_ABS}"
            DEPENDS "${PROTO_FILE_ABS}"
            COMMENT "Running gRPC C++ protocol buffer compiler on ${PROTO_FILE}"
            VERBATIM
        )
        
        list(APPEND ${SRC_FILES} "${PROTO_SRCS}" "${GRPC_SRCS}")
        list(APPEND ${HDR_FILES} "${PROTO_HDRS}" "${GRPC_HDRS}")
    endforeach()
    
    set(${SRC_FILES} ${${SRC_FILES}} PARENT_SCOPE)
    set(${HDR_FILES} ${${HDR_FILES}} PARENT_SCOPE)
endfunction()

# Generate common protos
protobuf_generate_grpc_cpp(COMMON_PROTO_SRCS COMMON_PROTO_HDRS
    common/timestamp.proto
    common/pagination.proto
)

add_library(common_proto ${COMMON_PROTO_SRCS})
target_link_libraries(common_proto
    ${Protobuf_LIBRARIES}
    ${GRPC_LIBRARIES}
)
target_include_directories(common_proto PUBLIC ${GENERATED_PROTOBUF_PATH})

# Generate note service protos
protobuf_generate_grpc_cpp(NOTE_PROTO_SRCS NOTE_PROTO_HDRS
    services/note.proto
)

add_library(note_proto ${NOTE_PROTO_SRCS})
target_link_libraries(note_proto
    common_proto
    ${Protobuf_LIBRARIES}
    ${GRPC_LIBRARIES}
)
target_include_directories(note_proto PUBLIC ${GENERATED_PROTOBUF_PATH})

# Generate timeline service protos
protobuf_generate_grpc_cpp(TIMELINE_PROTO_SRCS TIMELINE_PROTO_HDRS
    services/timeline.proto
)

add_library(timeline_proto ${TIMELINE_PROTO_SRCS})
target_link_libraries(timeline_proto
    common_proto
    note_proto
    ${Protobuf_LIBRARIES}
    ${GRPC_LIBRARIES}
)
target_include_directories(timeline_proto PUBLIC ${GENERATED_PROTOBUF_PATH})

# Generate fanout service protos
protobuf_generate_grpc_cpp(FANOUT_PROTO_SRCS FANOUT_PROTO_HDRS
    services/fanout.proto
)

add_library(fanout_proto ${FANOUT_PROTO_SRCS})
target_link_libraries(fanout_proto
    common_proto
    note_proto
    ${Protobuf_LIBRARIES}
    ${GRPC_LIBRARIES}
)
target_include_directories(fanout_proto PUBLIC ${GENERATED_PROTOBUF_PATH})

# Set convenience variables for linking
set(_REFLECTION ${GRPC_LIBRARIES})
set(_GRPC_GRPCPP ${GRPC_LIBRARIES})
set(_PROTOBUF_LIBPROTOBUF ${Protobuf_LIBRARIES})

# Export variables to parent scope
set(_REFLECTION ${_REFLECTION} PARENT_SCOPE)
set(_GRPC_GRPCPP ${_GRPC_GRPCPP} PARENT_SCOPE)
set(_PROTOBUF_LIBPROTOBUF ${_PROTOBUF_LIBPROTOBUF} PARENT_SCOPE)

message(STATUS "Proto libraries configured successfully")
