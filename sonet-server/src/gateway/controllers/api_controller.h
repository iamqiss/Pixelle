#pragma once
#include <nlohmann/json.hpp>
#include <string>

namespace sonet::gateway::controllers {

class ApiController {
public:
	nlohmann::json health();
};

}
