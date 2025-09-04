#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

namespace sonet { namespace note { class Note; } }

namespace sonet::timeline {

struct OverdriveRankedItem {
	std::string note_id;
	double score = 0.0;
	std::unordered_map<std::string, double> factors;
	std::vector<std::string> reasons;
};

class OverdriveClient {
public:
	virtual ~OverdriveClient() = default;
	virtual std::vector<OverdriveRankedItem> RankForYou(
		const std::string& user_id,
		const std::vector<std::string>& candidate_note_ids,
		int32_t limit
	) = 0;
};

} // namespace sonet::timeline