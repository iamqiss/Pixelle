#pragma once
#include <string>
#include <memory>
#include <functional>

namespace grpc {
    enum class StatusCode {
        OK = 0,
        CANCELLED = 1,
        UNKNOWN = 2,
        INVALID_ARGUMENT = 3,
        DEADLINE_EXCEEDED = 4,
        NOT_FOUND = 5,
        ALREADY_EXISTS = 6,
        PERMISSION_DENIED = 7,
        UNAUTHENTICATED = 16,
        RESOURCE_EXHAUSTED = 8,
        FAILED_PRECONDITION = 9,
        ABORTED = 10,
        OUT_OF_RANGE = 11,
        UNIMPLEMENTED = 12,
        INTERNAL = 13,
        UNAVAILABLE = 14,
        DATA_LOSS = 15
    };

    class Status {
    public:
        Status() : code_(StatusCode::OK) {}
        Status(StatusCode code, const std::string& message = "") 
            : code_(code), message_(message) {}
        
        StatusCode error_code() const { return code_; }
        std::string error_message() const { return message_; }
        bool ok() const { return code_ == StatusCode::OK; }
        
        static Status OK() { return Status(); }
        static Status CANCELLED(const std::string& msg = "") { return Status(StatusCode::CANCELLED, msg); }
        
    private:
        StatusCode code_;
        std::string message_;
    };

    class ServerContext {
    public:
        std::string peer() const { return "127.0.0.1:12345"; }
        bool IsCancelled() const { return false; }
        void set_compression_algorithm(int algorithm) {}
    };

    class ServerBuilder {
    public:
        ServerBuilder& AddListeningPort(const std::string& address, void* creds) {
            return *this;
        }
        
        ServerBuilder& RegisterService(void* service) {
            return *this;
        }
        
        std::unique_ptr<class Server> BuildAndStart() {
            return std::make_unique<Server>();
        }
    };

    class Server {
    public:
        void Wait() {}
        void Shutdown() {}
    };

    void* InsecureServerCredentials() {
        return nullptr;
    }
}

namespace grpc_impl {
    using ServerContext = grpc::ServerContext;
    using Status = grpc::Status;
    using ServerBuilder = grpc::ServerBuilder;
    using Server = grpc::Server;
}
