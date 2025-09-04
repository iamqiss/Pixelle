# Overdrive Serving (C++)

High-performance gRPC service for Overdrive ranking and retrieval.

- Loads ONNX models via ONNX Runtime or TensorRT
- In-memory ANN indices (FAISS/HNSW)
- Low-latency online feature fetch from Redis

Build:

- Requires CMake >= 3.20, a C++20 compiler
- ONNX Runtime and FAISS to be added later

Targets:

- `overdrive-serving` binary exposing `RankForYou` gRPC (to be added)