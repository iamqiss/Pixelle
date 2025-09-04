#include "grpc/messaging_grpc_service.h"
#include <random>

namespace sonet::messaging::grpc_impl {

MessagingGrpcService::MessagingGrpcService() {}
MessagingGrpcService::~MessagingGrpcService() {}

::grpc::Status MessagingGrpcService::SendMessage(::grpc::ServerContext* /*context*/, const ::sonet::messaging::SendMessageRequest* request, ::sonet::messaging::SendMessageResponse* response) {
	if (!request || request->chat_id().empty() || request->content().empty()) {
		auto* st = response->mutable_status();
		st->set_code(1);
		st->set_message("missing chat_id or content");
		return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "missing fields");
	}
	// Build message
	auto msg = build_message(request->chat_id(), /*sender_id*/"", request->content(), request->type());

	{
		std::lock_guard<std::mutex> lock(storage_mutex_);
		auto& vec = messages_by_chat_[request->chat_id()];
		vec.push_back({msg});
	}

	append_event_new_message(msg);

	auto* st = response->mutable_status();
	st->set_code(0);
	st->set_message("ok");
	*response->mutable_message() = msg;
	return ::grpc::Status::OK;
}

::grpc::Status MessagingGrpcService::GetMessages(::grpc::ServerContext* /*context*/, const ::sonet::messaging::GetMessagesRequest* request, ::sonet::messaging::GetMessagesResponse* response) {
	if (!request || request->chat_id().empty()) {
		return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "missing chat_id");
	}
	std::lock_guard<std::mutex> lock(storage_mutex_);
	const auto it = messages_by_chat_.find(request->chat_id());
	if (it != messages_by_chat_.end()) {
		for (const auto& sm : it->second) {
			*response->add_messages() = sm.proto;
		}
	}
	auto* st = response->mutable_status();
	st->set_code(0);
	st->set_message("ok");
	return ::grpc::Status::OK;
}

::grpc::Status MessagingGrpcService::CreateChat(::grpc::ServerContext* /*context*/, const ::sonet::messaging::CreateChatRequest* request, ::sonet::messaging::CreateChatResponse* response) {
	if (!request || request->participant_ids_size() == 0) {
		return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "missing participants");
	}
	std::vector<std::string> participants;
	participants.reserve(request->participant_ids_size());
	for (const auto& p : request->participant_ids()) participants.push_back(p);
	auto chat = build_chat(
		request->type() == ::sonet::messaging::ChatType::CHAT_TYPE_GROUP ? "group" : "direct",
		participants,
		request->name()
	);
	{
		std::lock_guard<std::mutex> lock(storage_mutex_);
		chats_by_id_[chat.chat_id()] = {chat};
	}
	auto* st = response->mutable_status();
	st->set_code(0);
	st->set_message("ok");
	*response->mutable_chat() = chat;
	return ::grpc::Status::OK;
}

::grpc::Status MessagingGrpcService::GetChats(::grpc::ServerContext* /*context*/, const ::sonet::messaging::GetChatsRequest* request, ::sonet::messaging::GetChatsResponse* response) {
	(void)request;
	std::lock_guard<std::mutex> lock(storage_mutex_);
	for (const auto& [id, sc] : chats_by_id_) {
		*response->add_chats() = sc.proto;
	}
	auto* st = response->mutable_status();
	st->set_code(0);
	st->set_message("ok");
	return ::grpc::Status::OK;
}

::grpc::Status MessagingGrpcService::SetTyping(::grpc::ServerContext* /*context*/, const ::sonet::messaging::SetTypingRequest* request, ::sonet::messaging::SetTypingResponse* response) {
	append_event_typing(request->chat_id(), /*user_id*/"", request->is_typing());
	(void)response;
	return ::grpc::Status::OK;
}

::grpc::Status MessagingGrpcService::StreamMessages(::grpc::ServerContext* context, ::grpc::ServerReaderWriter< ::sonet::messaging::WebSocketMessage, ::sonet::messaging::WebSocketMessage>* stream) {
	std::atomic<bool> running{true};
	std::mutex write_mutex;
	std::thread writer([&]() {
		std::unique_lock<std::mutex> lk(events_mutex_);
		while (running && !context->IsCancelled()) {
			if (events_.empty()) {
				events_cv_.wait_for(lk, std::chrono::seconds(5));
				if (!running || context->IsCancelled()) break;
			}
			while (!events_.empty()) {
				auto ev = std::move(events_.front());
				events_.erase(events_.begin());
				lk.unlock();
				{
					std::lock_guard<std::mutex> wlk(write_mutex);
					if (!stream->Write(ev)) {
						running = false;
						return;
					}
				}
				lk.lock();
			}
		}
	});

	::sonet::messaging::WebSocketMessage inbound;
	while (running && stream->Read(&inbound)) {
		if (inbound.has_typing()) {
			append_event_typing(inbound.typing().chat_id(), inbound.typing().user_id(), inbound.typing().is_typing());
		}
		// read_receipt and others can be handled here similarly
	}
	running = false;
	events_cv_.notify_all();
	if (writer.joinable()) writer.join();
	return ::grpc::Status::OK;
}

std::string MessagingGrpcService::generate_id(const std::string& prefix) {
	static thread_local std::mt19937_64 rng{std::random_device{}()};
	static thread_local std::uniform_int_distribution<uint64_t> dist;
	uint64_t a = dist(rng), b = dist(rng);
	char buf[33];
	snprintf(buf, sizeof(buf), "%016llx%016llx", (unsigned long long)a, (unsigned long long)b);
	return prefix + buf;
}

::sonet::common::Timestamp MessagingGrpcService::now_ts() {
	::sonet::common::Timestamp ts;
	const auto now = std::chrono::system_clock::now().time_since_epoch();
	ts.set_seconds(std::chrono::duration_cast<std::chrono::seconds>(now).count());
	ts.set_nanos(static_cast<int32_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count() % 1000000000));
	return ts;
}

::sonet::messaging::Message MessagingGrpcService::build_message(const std::string& chat_id, const std::string& sender_id, const std::string& content, ::sonet::messaging::MessageType type) {
	::sonet::messaging::Message m;
	m.set_message_id(generate_id("msg_"));
	m.set_chat_id(chat_id);
	m.set_sender_id(sender_id);
	m.set_content(content);
	m.set_type(type == ::sonet::messaging::MESSAGE_TYPE_UNSPECIFIED ? ::sonet::messaging::MESSAGE_TYPE_TEXT : type);
	m.set_status(::sonet::messaging::MESSAGE_STATUS_SENT);
	*m.mutable_created_at() = now_ts();
	*m.mutable_updated_at() = m.created_at();
	return m;
}

::sonet::messaging::Chat MessagingGrpcService::build_chat(const std::string& type, const std::vector<std::string>& participant_ids, const std::string& name) {
	::sonet::messaging::Chat c;
	c.set_chat_id(generate_id("chat_"));
	c.set_name(name);
	c.set_type(type == "group" ? ::sonet::messaging::CHAT_TYPE_GROUP : ::sonet::messaging::CHAT_TYPE_DIRECT);
	for (const auto& p : participant_ids) c.add_participant_ids(p);
	*m.mutable_created_at() = now_ts();
	*m.mutable_updated_at() = c.created_at();
	return c;
}

void MessagingGrpcService::append_event_new_message(const ::sonet::messaging::Message& msg) {
	::sonet::messaging::WebSocketMessage ev;
	*ev.mutable_new_message() = msg;
	{
		std::lock_guard<std::mutex> lk(events_mutex_);
		events_.push_back(std::move(ev));
	}
	events_cv_.notify_all();
}

void MessagingGrpcService::append_event_typing(const std::string& chat_id, const std::string& user_id, bool is_typing) {
	::sonet::messaging::WebSocketMessage ev;
	auto* typing = ev.mutable_typing();
	typing->set_chat_id(chat_id);
	typing->set_user_id(user_id);
	typing->set_is_typing(is_typing);
	*typing->mutable_timestamp() = now_ts();
	{
		std::lock_guard<std::mutex> lk(events_mutex_);
		events_.push_back(std::move(ev));
	}
	events_cv_.notify_all();
}

} // namespace sonet::messaging::grpc_impl