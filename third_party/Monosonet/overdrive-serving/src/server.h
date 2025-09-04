#pragma once

#include <string>

namespace overdrive {

class ServerConfig {
public:
	std::string address = "0.0.0.0:7070";
};

class OverdriveServer {
public:
	explicit OverdriveServer(const ServerConfig& config);
	~OverdriveServer();

	bool Start();
	void Stop();

private:
	ServerConfig config_;
	bool running_ = false;
	std::unique_ptr<grpc::Server> server_;
};

} // namespace overdrive