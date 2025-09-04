//
// Stub for environments without libpqxx: return nullptr so the service falls back to in-memory repo.
//
#include "../service.h"

namespace sonet::media_service {

std::unique_ptr<MediaRepository> CreateNotegresRepo(const std::string& /*conn_str*/) {
	return nullptr;
}

} // namespace sonet::media_service