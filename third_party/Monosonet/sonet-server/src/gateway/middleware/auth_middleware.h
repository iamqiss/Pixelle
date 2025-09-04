#pragma once
#include <httplib.h>
#include "gateway/auth/jwt_handler.h"
#include <memory>

namespace sonet::gateway::middleware {

struct AuthHelper {
	static bool authenticate(const httplib::Request& req, sonet::gateway::auth::JwtHandler& jwt, std::string& user_id) {
		auto it = req.get_header_value("Authorization");
		if (it.rfind("Bearer ", 0) == 0) {
			auto token = it.substr(7);
			auto claims = jwt.parse(token);
			if (claims) { user_id = claims->subject; return true; }
		}
		return false;
	}
};

}
