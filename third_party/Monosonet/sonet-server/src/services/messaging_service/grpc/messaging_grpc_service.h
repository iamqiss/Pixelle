#pragma once

#include <mutex>
#include <condition_variable>
#include <vector>
#include <unordered_map>
#include <string>
#include <chrono>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "proto/services/messaging.grpc.pb.h"

namespace sonet::messaging::grpc_impl {

class MessagingGrpcService final : public ::sonet::messaging::MessagingService::Service {
public:
	MessagingGrpcService();
	~MessagingGrpcService() override;

	::grpc::Status SendMessage(::grpc::ServerContext* context,
		const ::sonet::messaging::SendMessageRequest* request,
		::sonet::messaging::SendMessageResponse* response) override;

	::grpc::Status GetMessages(::grpc::ServerContext* context,
		const ::sonet::messaging::GetMessagesRequest* request,
		::sonet::messaging::GetMessagesResponse* response) override;

	::grpc::Status CreateChat(::grpc::ServerContext* context,
		const ::sonet::messaging::CreateChatRequest* request,
		::sonet::messaging::CreateChatResponse* response) override;

	::grpc::Status GetChats(::grpc::ServerContext* context,
		const ::sonet::messaging::GetChatsRequest* request,
		::sonet::messaging::GetChatsResponse* response) override;

	::grpc::Status SetTyping(::grpc::ServerContext* context,
		const ::sonet::messaging::SetTypingRequest* request,
		::sonet::messaging::SetTypingResponse* response) override;

	::grpc::Status StreamMessages(::grpc::ServerContext* context,
		::grpc::ServerReaderWriter< ::sonet::messaging::WebSocketMessage, ::sonet::messaging::WebSocketMessage>* stream) override;

private:
	// Simple in-memory storage
	struct StoredMessage {
		::sonet::messaging::Message proto;
	};
	struct StoredChat {
		::sonet::messaging::Chat proto;
	};

	std::mutex storage_mutex_;
	std::unordered_map<std::string, StoredChat> chats_by_id_;
	std::unordered_map<std::string, std::vector<StoredMessage>> messages_by_chat_;

	// Event bus for streaming
	std::mutex events_mutex_;
	std::condition_variable events_cv_;
	std::vector< ::sonet::messaging::WebSocketMessage > events_;

	// Utilities
	static std::string generate_id(const std::string& prefix);
	static ::sonet::common::Timestamp now_ts();
	::sonet::messaging::Message build_message(const std::string& chat_id, const std::string& sender_id,
		const std::string& content, ::sonet::messaging::MessageType type);
	::sonet::messaging::Chat build_chat(const std::string& type,
		const std::vector<std::string>& participant_ids,
		const std::string& name);

	void append_event_new_message(const ::sonet::messaging::Message& msg);
	void append_event_typing(const std::string& chat_id, const std::string& user_id, bool is_typing);
};

} // namespace sonet::messaging::grpc_impl