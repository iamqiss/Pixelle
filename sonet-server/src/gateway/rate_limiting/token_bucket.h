#pragma once
#include <chrono>
#include <mutex>

namespace sonet::gateway::rate_limiting {

class TokenBucket {
public:
	TokenBucket(int capacity, int refill_tokens, std::chrono::seconds refill_interval)
		: capacity_(capacity), tokens_(capacity), refill_tokens_(refill_tokens), refill_interval_(refill_interval), last_refill_(std::chrono::steady_clock::now()) {}

	bool consume(int n = 1) {
		std::lock_guard<std::mutex> lock(mutex_);
		refill();
		if (tokens_ >= n) { tokens_ -= n; return true; }
		return false;
	}
	int remaining() const { return tokens_; }
private:
	void refill() {
		auto now = std::chrono::steady_clock::now();
		if (now - last_refill_ >= refill_interval_) {
			tokens_ = std::min(capacity_, tokens_ + refill_tokens_);
			last_refill_ = now;
		}
	}
	int capacity_;
	int tokens_;
	int refill_tokens_;
	std::chrono::seconds refill_interval_;
	std::chrono::steady_clock::time_point last_refill_;
	mutable std::mutex mutex_;
};

}
