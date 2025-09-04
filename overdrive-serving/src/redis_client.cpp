#include "redis_client.h"
#include <iostream>

namespace overdrive {

class StubRedisClient : public RedisClient {
public:
	bool Connect(const std::string& url) override {
		connected_ = true;
		std::cout << "Stub Redis client connected to " << url << std::endl;
		return true;
	}
	
	std::unordered_map<std::string, std::string> GetUserFeatures(const std::string& user_id) override {
		std::cout << "Stub Redis: getting features for user " << user_id << std::endl;
		return {{"last_event_ts", "2025-01-01T00:00:00Z"}, {"session_interaction_count", "0"}};
	}
	
	std::unordered_map<std::string, std::string> GetItemFeatures(const std::string& item_id) override {
		std::cout << "Stub Redis: getting features for item " << item_id << std::endl;
		return {{"has_media", "false"}, {"text_length", "0"}};
	}
	
	std::unordered_map<std::string, std::unordered_map<std::string, std::string>> 
		MGetItemFeatures(const std::vector<std::string>& item_ids) override {
		std::cout << "Stub Redis: batch getting features for " << item_ids.size() << " items" << std::endl;
		std::unordered_map<std::string, std::unordered_map<std::string, std::string>> result;
		for (const auto& id : item_ids) {
			result[id] = GetItemFeatures(id);
		}
		return result;
	}
	
	bool IsConnected() const override { return connected_; }

private:
	bool connected_ = false;
};

std::unique_ptr<RedisClient> CreateRedisClient() {
	// TODO: Return real Redis implementation when linked
	return std::make_unique<StubRedisClient>();
}

} // namespace overdrive