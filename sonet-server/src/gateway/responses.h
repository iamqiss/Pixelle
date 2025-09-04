// Unified JSON response helpers
#pragma once
#include <nlohmann/json.hpp>
#include <string>

namespace sonet::gateway::responses {

inline nlohmann::json ok(const nlohmann::json& data = {}) {
    nlohmann::json j = data;
    j["ok"] = true;
    return j;
}

inline nlohmann::json error(const std::string& code, const std::string& message, int status = 400) {
    return nlohmann::json{{"ok", false}, {"error", code}, {"message", message}, {"status", status}};
}

}
