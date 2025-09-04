#pragma once
#include <grpcpp/grpcpp.h>
#include <string>
#include <memory>

#include "moderation.grpc.pb.h"

namespace sonet::note::clients {

class ModerationClient {
public:
	explicit ModerationClient(const std::string& target)
		: channel_(grpc::CreateChannel(target, grpc::InsecureChannelCredentials())),
		  stub_(moderation::v1::ModerationService::NewStub(channel_)) {}

	bool Classify(const std::string& content_id,
	             const std::string& user_id,
	             const std::string& text,
	             std::string* out_label,
	             float* out_confidence,
	             int timeout_ms = 150) {
		moderation::v1::ClassifyRequest req;
		req.set_content_id(content_id);
		req.set_user_id(user_id);
		req.set_text(text);

		grpc::ClientContext ctx;
		AddDeadline(ctx, timeout_ms);

		moderation::v1::ClassifyResponse resp;
		auto status = stub_->Classify(&ctx, req, &resp);
		if (!status.ok()) {
			return false;
		}
		if (resp.has_result()) {
			*out_label = resp.result().label();
			*out_confidence = resp.result().confidence();
			return true;
		}
		return false;
	}

private:
	static void AddDeadline(grpc::ClientContext& ctx, int timeout_ms) {
		auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
		ctx.set_deadline(deadline);
	}

	std::shared_ptr<grpc::Channel> channel_;
	std::unique_ptr<moderation::v1::ModerationService::Stub> stub_;
};

} // namespace sonet::note::clients