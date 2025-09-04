#include <iostream>
#include <chrono>
#include <thread>
#include "server.h"

int main() {
	std::cout << "Overdrive Serving starting..." << std::endl;
	
	overdrive::ServerConfig config;
	overdrive::OverdriveServer server(config);
	
	if (!server.Start()) {
		std::cerr << "Failed to start server" << std::endl;
		return 1;
	}
	
	std::cout << "Overdrive Serving running. Press Ctrl+C to stop." << std::endl;
	
	// Keep server running
	while (true) {
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
	
	return 0;
}