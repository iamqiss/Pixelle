#include "ranker_service.h"
#include "faiss_index.h"
#include "redis_client.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <iostream>

namespace overdrive {

class OverdriveRankerServiceImpl final : public OverdriveRanker::Service {
public:
	OverdriveRankerServiceImpl() {
		// Initialize Redis client
		redis_client_ = CreateRedisClient();
		redis_client_->Connect("redis://localhost:6379");
		
		// Initialize FAISS index
		faiss_index_ = CreateFaissIndex("hnsw");
		std::cout << "OverdriveRanker service initialized with Redis and FAISS" << std::endl;
	}

	grpc::Status RankForYou(
		grpc::ServerContext* context,
		const RankForYouRequest* request,
		RankForYouResponse* response
	) override {
		// Get user features from Redis
		auto user_features = redis_client_->GetUserFeatures(request->user_id());
		
		// Get item features from Redis
		std::vector<std::string> item_ids(request->candidate_note_ids().begin(), request->candidate_note_ids().end());
		auto item_features = redis_client_->MGetItemFeatures(item_ids);
		
		// Call the ranker service with features
		auto ranked = ranker_.RankForYou(
			request->user_id(),
			item_ids,
			request->limit()
		);

		// Build response
		for (const auto& item : ranked) {
			auto* resp_item = response->add_items();
			resp_item->set_note_id(item.note_id);
			resp_item->set_score(item.score);
			
			for (const auto& factor : item.factors) {
				auto* resp_factor = resp_item->add_factors();
				resp_factor->set_name(factor.name);
				resp_factor->set_value(factor.value);
			}
			
			for (const auto& reason : item.reasons) {
				resp_item->add_reasons(reason);
			}
		}

		response->set_algorithm_version("0.1.0");
		response->mutable_personalization_summary()->insert({"overdrive_enabled", 1.0});
		response->mutable_personalization_summary()->insert({"user_features_count", static_cast<double>(user_features.size())});
		response->mutable_personalization_summary()->insert({"items_features_count", static_cast<double>(item_features.size())});

		return grpc::Status::OK;
	}

private:
	RankerServiceImpl ranker_;
	std::unique_ptr<RedisClient> redis_client_;
	std::unique_ptr<FaissIndex> faiss_index_;
};

} // namespace overdrive