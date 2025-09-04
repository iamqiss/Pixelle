#include "overdrive_client_grpc.h"
#include <iostream>
#include <grpcpp/grpcpp.h>
#include <grpcpp/client_context.h>

namespace sonet::timeline {

OverdriveClientGrpc::OverdriveClientGrpc(const std::string& target_address) : target_(target_address) {
	// Create gRPC channel
	channel_ = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
	if (!channel_) {
		std::cerr << "Failed to create gRPC channel to " << target_address << std::endl;
		return;
	}
	
	// Create stub
	stub_ = overdrive::OverdriveRanker::NewStub(channel_);
	if (!stub_) {
		std::cerr << "Failed to create OverdriveRanker stub" << std::endl;
		return;
	}
	
	std::cout << "Overdrive gRPC client connected to " << target_address << std::endl;
}

std::vector<OverdriveRankedItem> OverdriveClientGrpc::RankForYou(
	const std::string& user_id,
	const std::vector<std::string>& candidate_note_ids,
	int32_t limit
) {
	if (!stub_) {
		std::cerr << "Overdrive stub not available, falling back to stub ranking" << std::endl;
		return FallbackRanking(user_id, candidate_note_ids, limit);
	}
	
	try {
		// Prepare request
		overdrive::RankForYouRequest request;
		request.set_user_id(user_id);
		request.set_limit(limit);
		for (const auto& id : candidate_note_ids) {
			request.add_candidate_note_ids(id);
		}
		
		// Make gRPC call
		grpc::ClientContext context;
		overdrive::RankForYouResponse response;
		
		auto status = stub_->RankForYou(&context, request, &response);
		if (!status.ok()) {
			std::cerr << "Overdrive gRPC call failed: " << status.error_message() << std::endl;
			return FallbackRanking(user_id, candidate_note_ids, limit);
		}
		
		// Convert response
		std::vector<OverdriveRankedItem> result;
		result.reserve(response.items_size());
		
		for (const auto& resp_item : response.items()) {
			OverdriveRankedItem item;
			item.note_id = resp_item.note_id();
			item.score = resp_item.score();
			
			for (const auto& resp_factor : resp_item.factors()) {
				item.factors[resp_factor.name()] = resp_factor.value();
			}
			
			for (const auto& reason : resp_item.reasons()) {
				item.reasons.push_back(reason);
			}
			
			result.push_back(std::move(item));
		}
		
		std::cout << "Overdrive gRPC ranked " << result.size() << " items for user " << user_id << std::endl;
		return result;
		
	} catch (const std::exception& e) {
		std::cerr << "Exception in Overdrive gRPC call: " << e.what() << std::endl;
		return FallbackRanking(user_id, candidate_note_ids, limit);
	}
}

std::vector<OverdriveRankedItem> OverdriveClientGrpc::FallbackRanking(
	const std::string& user_id,
	const std::vector<std::string>& candidate_note_ids,
	int32_t limit
) {
	std::vector<OverdriveRankedItem> result;
	result.reserve(candidate_note_ids.size());
	
	for (size_t i = 0; i < candidate_note_ids.size() && static_cast<int32_t>(result.size()) < limit; ++i) {
		OverdriveRankedItem item;
		item.note_id = candidate_note_ids[i];
		item.score = 1.0 - (0.001 * i); // Simple position-based scoring
		item.factors["position"] = static_cast<double>(i);
		item.factors["overdrive_fallback"] = 1.0;
		item.reasons.push_back("overdrive_fallback_ranking");
		result.push_back(std::move(item));
	}
	
	std::cout << "Overdrive fallback ranked " << result.size() << " items for user " << user_id << std::endl;
	return result;
}

} // namespace sonet::timeline