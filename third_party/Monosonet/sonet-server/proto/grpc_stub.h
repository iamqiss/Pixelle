#pragma once

// Stub gRPC includes for compilation testing

#include <string>
#include <map>
#include <memory>
#include <functional>
#include <cstring>
#include <chrono>

namespace grpc {
    enum StatusCode {
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
        Status() : code_(grpc::StatusCode::OK) {}
        Status(StatusCode code, const std::string& message) : code_(code), message_(message) {}
        
        StatusCode error_code() const { return code_; }
        std::string error_message() const { return message_; }
        bool ok() const { return code_ == grpc::StatusCode::OK; }
        
        static Status OK;
        
    private:
        StatusCode code_;
        std::string message_;
    };
    
    struct string_ref {
        string_ref(const char* str, size_t len) : data_(str), length_(len) {}
        const char* data() const { return data_; }
        size_t length() const { return length_; }
        size_t size() const { return length_; }
        
    private:
        const char* data_;
        size_t length_;
    };

    // Provide ordering for string_ref for use in associative containers
    inline bool operator<(const string_ref& a, const string_ref& b) {
        size_t min_len = a.length() < b.length() ? a.length() : b.length();
        int cmp = std::memcmp(a.data(), b.data(), min_len);
        if (cmp < 0) return true;
        if (cmp > 0) return false;
        return a.length() < b.length();
    }
    
    class ServerContext {
    public:
        using metadata_type = std::multimap<string_ref, string_ref>;
        
        const metadata_type& client_metadata() const { return metadata_; }
        
    private:
        metadata_type metadata_;
    };
    
    template<typename T>
    class ServerWriter {
    public:
        bool Write(const T& item) { return true; }
    };

    template<typename T>
    class ServerReader {
    public:
        bool Read(T* item) { return false; }
    };
    
    class ServerCredentials {};
    std::shared_ptr<ServerCredentials> InsecureServerCredentials();
    
    class Server; // forward declaration

    class ServerBuilder {
    public:
        ServerBuilder& AddListeningPort(const std::string& addr, std::shared_ptr<ServerCredentials> creds) {
            return *this;
        }
        
        template<typename T>
        ServerBuilder& RegisterService(T* service) { return *this; }
        
        ServerBuilder& SetMaxReceiveMessageSize(int size) { return *this; }
        ServerBuilder& SetMaxSendMessageSize(int size) { return *this; }
        
        std::unique_ptr<Server> BuildAndStart();
    };
    
    class Server {
    public:
        void Shutdown() {}
        void Wait() {}
    };
    
    // Client-side gRPC definitions
    class ChannelCredentials {};
    std::shared_ptr<ChannelCredentials> InsecureChannelCredentials();
    
    class Channel {
    public:
        Channel() = default;
        virtual ~Channel() = default;
    };
    
    std::shared_ptr<Channel> CreateChannel(const std::string& target, std::shared_ptr<ChannelCredentials> creds);
    
    template<typename T>
    class StubInterface {
    public:
        virtual ~StubInterface() = default;
    };
    
    template<typename T>
    std::unique_ptr<T> NewStub(std::shared_ptr<Channel> channel) {
        return std::make_unique<T>();
    }
    
    class ClientContext {
    public:
        ClientContext() = default;
        ~ClientContext() = default;
        
        void AddMetadata(const std::string& key, const std::string& value) {}
        void SetDeadline(const std::chrono::system_clock::time_point& deadline) {}
    };
} // namespace grpc

// Additional gRPC stub implementations
inline std::shared_ptr<grpc::ServerCredentials> grpc::InsecureServerCredentials() {
    return std::shared_ptr<grpc::ServerCredentials>();
}

inline std::unique_ptr<grpc::Server> grpc::ServerBuilder::BuildAndStart() {
    return std::unique_ptr<grpc::Server>();
}

inline grpc::Status grpc::Status::OK;

// Client-side gRPC stub implementations
inline std::shared_ptr<grpc::ChannelCredentials> grpc::InsecureChannelCredentials() {
    return std::shared_ptr<grpc::ChannelCredentials>();
}

inline std::shared_ptr<grpc::Channel> grpc::CreateChannel(const std::string& target, std::shared_ptr<grpc::ChannelCredentials> creds) {
    return std::shared_ptr<grpc::Channel>();
}
