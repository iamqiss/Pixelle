# FindgRPC.cmake - Custom find module for gRPC
# This module helps locate gRPC libraries and sets up proper targets

# Try to find gRPC using pkg-config first
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(GRPC_PC grpc++)
    pkg_check_modules(GRPCPP_PC grpc++)
    pkg_check_modules(GRPCPP_REFLECTION_PC grpc++_reflection)
endif()

# Find gRPC libraries
find_library(GRPC_LIBRARY
    NAMES grpc
    PATHS ${GRPC_PC_LIBRARY_DIRS}
    PATH_SUFFIXES lib
)

find_library(GRPCPP_LIBRARY
    NAMES grpc++
    PATHS ${GRPCPP_PC_LIBRARY_DIRS}
    PATH_SUFFIXES lib
)

find_library(GRPCPP_REFLECTION_LIBRARY
    NAMES grpc++_reflection
    PATHS ${GRPCPP_REFLECTION_PC_LIBRARY_DIRS}
    PATH_SUFFIXES lib
)

# Find gRPC include directories
find_path(GRPC_INCLUDE_DIR
    NAMES grpc/grpc.h
    PATHS ${GRPC_PC_INCLUDE_DIRS}
    PATH_SUFFIXES include
)

find_path(GRPCPP_INCLUDE_DIR
    NAMES grpcpp/grpcpp.h
    PATHS ${GRPCPP_PC_INCLUDE_DIRS}
    PATH_SUFFIXES include
)

# Set variables
set(GRPC_LIBRARIES ${GRPC_LIBRARY} ${GRPCPP_LIBRARY} ${GRPCPP_REFLECTION_LIBRARY})
set(GRPC_INCLUDE_DIRS ${GRPC_INCLUDE_DIR} ${GRPCPP_INCLUDE_DIR})

# Handle REQUIRED and QUIET arguments
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gRPC
    REQUIRED_VARS GRPC_LIBRARIES GRPC_INCLUDE_DIRS
    FOUND_VAR gRPC_FOUND
)

# Create imported targets if found
if(gRPC_FOUND AND NOT TARGET gRPC::grpc++)
    add_library(gRPC::grpc++ UNKNOWN IMPORTED)
    set_target_properties(gRPC::grpc++ PROPERTIES
        IMPORTED_LOCATION "${GRPCPP_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${GRPCPP_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES "${GRPC_LIBRARY}"
    )
endif()

if(gRPC_FOUND AND NOT TARGET gRPC::grpc++_reflection)
    add_library(gRPC::grpc++_reflection UNKNOWN IMPORTED)
    set_target_properties(gRPC::grpc++_reflection PROPERTIES
        IMPORTED_LOCATION "${GRPCPP_REFLECTION_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${GRPCPP_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES "gRPC::grpc++"
    )
endif()

# Mark as advanced
mark_as_advanced(GRPC_LIBRARY GRPCPP_LIBRARY GRPCPP_REFLECTION_LIBRARY
                 GRPC_INCLUDE_DIR GRPCPP_INCLUDE_DIR)