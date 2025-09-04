#pragma once
#include <httplib.h>
#include <chrono>
#include <iostream>

namespace sonet::gateway::middleware {

// Placeholder logging utilities for potential future integration.
struct LoggingHelper {
	static void log_request(const httplib::Request& req) {
		std::cout << req.method << " " << req.path << std::endl;
	}
	static void log_response(const httplib::Request& req, const httplib::Response& res, long ms) {
		std::cout << req.method << " " << req.path << " -> " << res.status << " (" << ms << "ms)" << std::endl;
	}
};

}
