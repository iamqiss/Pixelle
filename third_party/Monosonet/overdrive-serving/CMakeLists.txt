cmake_minimum_required(VERSION 3.20)
project(overdrive-serving LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find protobuf and gRPC
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# Generate protobuf and gRPC sources
set(PROTO_FILES proto/overdrive.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Generate gRPC sources
protobuf_generate_grpc_cpp(GRPC_SRCS GRPC_HDRS ${PROTO_FILES})

add_executable(overdrive-serving
	src/main.cpp
	src/server.cpp
	src/ranker_service.cpp
	src/faiss_index.cpp
	src/redis_client.cpp
	${PROTO_SRCS}
	${PROTO_HDRS}
	${GRPC_SRCS}
	${GRPC_HDRS}
)

target_include_directories(overdrive-serving PRIVATE 
	${CMAKE_CURRENT_SOURCE_DIR}/src 
	${CMAKE_CURRENT_SOURCE_DIR}/proto
	${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(overdrive-serving PRIVATE cxx_std_20)

# Link protobuf and gRPC
target_link_libraries(overdrive-serving 
	${Protobuf_LIBRARIES}
	gRPC::grpc++
)

# TODO: Add ONNX Runtime and FAISS when ready