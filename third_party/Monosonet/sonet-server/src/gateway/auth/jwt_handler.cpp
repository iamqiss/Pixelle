#include "jwt_handler.h"
#include <nlohmann/json.hpp>
#include <sstream>
#include <iostream>

namespace sonet::gateway::auth {

std::optional<JwtClaims> JwtHandler::parse(const std::string& token) const {
	// Extremely naive placeholder: treat token as JSON for now (dev only)
	try {
		auto claims_json = nlohmann::json::parse(token);
		JwtClaims c;
		c.subject = claims_json.value("sub", "");
		c.scope = claims_json.value("scope", "");
		c.session_id = claims_json.value("sid", "");
		c.expires_at = claims_json.value("exp", 0L);
		if (!c.valid()) return std::nullopt;
		return c;
	} catch (const std::exception& e) {
		std::cerr << "JWT parse error: " << e.what() << std::endl;
		return std::nullopt;
	}
}

}
