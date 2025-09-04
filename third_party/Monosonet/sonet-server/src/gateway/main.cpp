#include "gateway.h"
#include <csignal>
#include <iostream>
#include <memory>
#include <fstream>
#include <iterator>
#include <nlohmann/json.hpp>

using sonet::gateway::RestGateway;
using sonet::gateway::GatewayRateLimitConfig;

static std::unique_ptr<RestGateway> g_gateway;

void signal_handler(int sig) {
	std::cout << "Signal " << sig << " received, shutting down REST gateway..." << std::endl;
	if (g_gateway) {
		g_gateway->stop();
		g_gateway.reset();
	}
	std::exit(0);
}

int main(int argc, char* argv[]) {
	int port = 8080;
	std::string config_path = "config/development/gateway.json";
	if (argc > 1) {
		config_path = argv[1];
	}
	// Load config if present
	GatewayRateLimitConfig rl_cfg;
			try {
			std::ifstream in(config_path);
			if (in.good()) {
				std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
				auto j = nlohmann::json::parse(content);
			port = j.value("port", port);
			if (j.contains("rate_limits")) {
				auto rl = j["rate_limits"];
				rl_cfg.global_per_minute = rl.value("global_per_minute", rl_cfg.global_per_minute);
				rl_cfg.auth_login_per_minute = rl.value("auth_login_per_minute", rl_cfg.auth_login_per_minute);
				rl_cfg.auth_register_per_minute = rl.value("auth_register_per_minute", rl_cfg.auth_register_per_minute);
				rl_cfg.timeline_home_per_minute = rl.value("timeline_home_per_minute", rl_cfg.timeline_home_per_minute);
			}
		}
	} catch(const std::exception& e) {
		std::cerr << "Gateway config load failed: " << e.what() << std::endl;
	}
	g_gateway = std::make_unique<RestGateway>(port, rl_cfg);
	std::signal(SIGINT, signal_handler);
	std::signal(SIGTERM, signal_handler);
	g_gateway->start();
	// Block forever
	while (true) { std::this_thread::sleep_for(std::chrono::seconds(60)); }
	return 0;
}
