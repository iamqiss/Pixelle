#pragma once

#include "overdrive_client.h"
#include <string>
#include <vector>
#include <memory>

// Forward declarations
namespace grpc { class Channel; }
namespace overdrive { class OverdriveRanker; }

namespace sonet::timeline {

class OverdriveClientGrpc final : public OverdriveClient {
public:
	explicit OverdriveClientGrpc(const std::string& target_address);
	std::vector<OverdriveRankedItem> RankForYou(
		const std::string& user_id,
		const std::vector<std::string>& candidate_note_ids,
		int32_t limit
	) override;

private:
	std::vector<OverdriveRankedItem> FallbackRanking(
		const std::string& user_id,
		const std::vector<std::string>& candidate_note_ids,
		int32_t limit
	);
	
	std::string target_;
	std::shared_ptr<grpc::Channel> channel_;
	std::unique_ptr<overdrive::OverdriveRanker::Stub> stub_;
};

} // namespace sonet::timeline