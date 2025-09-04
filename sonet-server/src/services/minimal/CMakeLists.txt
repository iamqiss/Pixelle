cmake_minimum_required(VERSION 3.20)
project(sonet_minimal_services LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Check if we're in stub mode
if(DEFINED USE_STUB_IMPLEMENTATIONS AND USE_STUB_IMPLEMENTATIONS)
    message(STATUS "Minimal services: Building with stub implementations")
    
    # Create minimal stub library
    add_library(minimal_protos INTERFACE)
    target_include_directories(minimal_protos INTERFACE 
        ${CMAKE_SOURCE_DIR}/proto
        ${CMAKE_SOURCE_DIR}/proto/services
        ${CMAKE_SOURCE_DIR}/proto/common
    )
    
    # Create executables that use stubs
    add_executable(minimal_search_server search_server.cpp)
    target_include_directories(minimal_search_server PRIVATE 
        ${CMAKE_SOURCE_DIR}/proto/services 
        ${CMAKE_SOURCE_DIR}/proto/common
    )
    target_link_libraries(minimal_search_server PRIVATE minimal_protos sonet_proto)
    
    add_executable(minimal_notification_server notification_server.cpp)
    target_include_directories(minimal_notification_server PRIVATE 
        ${CMAKE_SOURCE_DIR}/proto/services 
        ${CMAKE_SOURCE_DIR}/proto/common
    )
    target_link_libraries(minimal_notification_server PRIVATE minimal_protos sonet_proto)
    
else()
    # Full gRPC mode
    find_package(Protobuf REQUIRED)
    find_package(gRPC REQUIRED)
    
    set(PROTO_ROOT ${CMAKE_SOURCE_DIR}/proto)
    set(PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/gen)
    file(MAKE_DIRECTORY ${PROTO_OUT})
    
    set(MINIMAL_PROTOS
      ${PROTO_ROOT}/services/search.proto
      ${PROTO_ROOT}/services/notification.proto
      ${PROTO_ROOT}/common/pagination.proto
      ${PROTO_ROOT}/common/timestamp.proto
      ${PROTO_ROOT}/common/common.proto
    )
    
    set(MINIMAL_GEN_SRCS
      ${PROTO_OUT}/common/common.pb.cc
      ${PROTO_OUT}/common/common.grpc.pb.cc
      ${PROTO_OUT}/common/timestamp.pb.cc
      ${PROTO_OUT}/common/timestamp.grpc.pb.cc
      ${PROTO_OUT}/common/pagination.pb.cc
      ${PROTO_OUT}/common/pagination.grpc.pb.cc
      ${PROTO_OUT}/services/search.pb.cc
      ${PROTO_OUT}/services/search.grpc.pb.cc
      ${PROTO_OUT}/services/notification.pb.cc
      ${PROTO_OUT}/services/notification.grpc.pb.cc
    )
    set(MINIMAL_GEN_HDRS
      ${PROTO_OUT}/common/common.pb.h
      ${PROTO_OUT}/common/common.grpc.pb.h
      ${PROTO_OUT}/common/timestamp.pb.h
      ${PROTO_OUT}/common/timestamp.grpc.pb.h
      ${PROTO_OUT}/common/pagination.pb.h
      ${PROTO_OUT}/common/pagination.grpc.pb.h
      ${PROTO_OUT}/services/search.pb.h
      ${PROTO_OUT}/services/search.grpc.pb.h
      ${PROTO_OUT}/services/notification.pb.h
      ${PROTO_OUT}/services/notification.grpc.pb.h
    )
    
    # Determine grpc c++ plugin executable. Prefer exported target file when available,
    # otherwise fall back to locating the plugin binary on the system.
    if(TARGET gRPC::grpc_cpp_plugin)
      set(GRPC_PLUGIN_EXECUTABLE "$<TARGET_FILE:gRPC::grpc_cpp_plugin>")
    else()
      find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin HINTS /usr/local/bin /usr/bin)
      if(NOT GRPC_CPP_PLUGIN_EXECUTABLE)
        set(GRPC_CPP_PLUGIN_EXECUTABLE "/usr/local/bin/grpc_cpp_plugin")
      endif()
      set(GRPC_PLUGIN_EXECUTABLE ${GRPC_CPP_PLUGIN_EXECUTABLE})
    endif()

    # Generation rules
    foreach(PF ${MINIMAL_PROTOS})
      get_filename_component(PN ${PF} NAME_WE)
      get_filename_component(PDIR ${PF} DIRECTORY)
      file(RELATIVE_PATH RELDIR ${PROTO_ROOT} ${PDIR})
      file(MAKE_DIRECTORY ${PROTO_OUT}/${RELDIR})
      add_custom_command(
        OUTPUT ${PROTO_OUT}/${RELDIR}/${PN}.pb.cc ${PROTO_OUT}/${RELDIR}/${PN}.pb.h ${PROTO_OUT}/${RELDIR}/${PN}.grpc.pb.cc ${PROTO_OUT}/${RELDIR}/${PN}.grpc.pb.h
        COMMAND protobuf::protoc
        ARGS --grpc_out ${PROTO_OUT}
             --cpp_out ${PROTO_OUT}
             -I ${PROTO_ROOT}
             --plugin=protoc-gen-grpc=${GRPC_PLUGIN_EXECUTABLE}
             ${PF}
        DEPENDS ${PF}
        COMMENT "Generating gRPC/proto for ${RELDIR}/${PN}"
      )
    endforeach()
    add_custom_target(minimal_protos_gen DEPENDS ${MINIMAL_GEN_SRCS} ${MINIMAL_GEN_HDRS})
    
    add_library(minimal_protos ${MINIMAL_GEN_SRCS})
    add_dependencies(minimal_protos minimal_protos_gen)
    target_include_directories(minimal_protos PUBLIC ${PROTO_OUT})
    target_link_libraries(minimal_protos PUBLIC gRPC::grpc++ protobuf::libprotobuf)
    
    add_executable(minimal_search_server search_server.cpp)
    add_dependencies(minimal_search_server minimal_protos)
    target_include_directories(minimal_search_server PRIVATE ${PROTO_OUT}/services ${PROTO_OUT}/common)
    target_link_libraries(minimal_search_server PRIVATE minimal_protos gRPC::grpc++ protobuf::libprotobuf)
    
    add_executable(minimal_notification_server notification_server.cpp)
    add_dependencies(minimal_notification_server minimal_protos)
    target_include_directories(minimal_notification_server PRIVATE ${PROTO_OUT}/services ${PROTO_OUT}/common)
    target_link_libraries(minimal_notification_server PRIVATE minimal_protos gRPC::grpc++ protobuf::libprotobuf)
endif()

set_target_properties(minimal_search_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set_target_properties(minimal_notification_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")