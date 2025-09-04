#pragma once
#include "token_bucket.h"
#include <unordered_map>
#include <string>
#include <memory>
#include <mutex>
#include <chrono>

namespace sonet::gateway::rate_limiting {

struct LimitConfig { 
    int capacity; 
    int refill; 
    int interval_ms; 
};

class RateLimiter {
public:
    explicit RateLimiter(LimitConfig cfg): cfg_(cfg) {}

    bool allow(const std::string& key) {
        std::lock_guard<std::mutex> lock(mu_);
        auto& bucket = buckets_[key];
        if (!bucket) {
            bucket = std::make_unique<TokenBucket>(cfg_.capacity, cfg_.refill, std::chrono::seconds(cfg_.interval_ms / 1000));
        }
        return bucket->consume();
    }

private:
    LimitConfig cfg_;
    std::unordered_map<std::string, std::unique_ptr<TokenBucket>> buckets_;
    std::mutex mu_;
};

} // namespace sonet::gateway::rate_limiting
