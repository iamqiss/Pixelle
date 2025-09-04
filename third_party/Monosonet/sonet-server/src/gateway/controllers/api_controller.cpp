#include "api_controller.h"

namespace sonet::gateway::controllers {

nlohmann::json ApiController::health() {
	return nlohmann::json{{"status", "ok"}};
}

}
