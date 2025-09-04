#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

namespace overdrive {

class RedisClient {
public:
	virtual ~RedisClient() = default;
	
	// Connect to Redis
	virtual bool Connect(const std::string& url) = 0;
	
	// Get user features
	virtual std::unordered_map<std::string, std::string> GetUserFeatures(const std::string& user_id) = 0;
	
	// Get item features
	virtual std::unordered_map<std::string, std::string> GetItemFeatures(const std::string& item_id) = 0;
	
	// Batch get item features
	virtual std::unordered_map<std::string, std::unordered_map<std::string, std::string>> 
		MGetItemFeatures(const std::vector<std::string>& item_ids) = 0;
	
	// Check connection
	virtual bool IsConnected() const = 0;
};

// Factory function
std::unique_ptr<RedisClient> CreateRedisClient();

} // namespace overdrive