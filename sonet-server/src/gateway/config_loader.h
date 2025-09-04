// Loads gateway configuration from JSON file
#pragma once
#include "config.h"
#include <nlohmann/json.hpp>
#include <fstream>

namespace sonet::gateway::config {

inline GatewayConfig load_config(const std::string& path) {
    GatewayConfig cfg;
    std::ifstream in(path);
    if (!in.good()) return cfg;
    nlohmann::json j; in >> j;
    cfg.port = j.value("port", cfg.port);
    if (j.contains("rate_limits") && j["rate_limits"].is_object()) {
        for (auto& [k,v] : j["rate_limits"].items()) {
            RateLimitSpec spec; spec.per_minute = v.get<int>();
            cfg.rate_limits[k] = spec;
        }
    }
    return cfg;
}

}