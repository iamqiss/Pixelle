#include "gateway.h"
#include <iostream>
#include <nlohmann/json.hpp>
#include "middleware/logging_middleware.h" // retained for future use
#include "middleware/cors_middleware.h"
#include "middleware/auth_middleware.h"
#include "auth/jwt_handler.h"
#include "rate_limiting/rate_limiter.h"
#include "responses.h"

using nlohmann::json;

namespace sonet::gateway {

RestGateway::RestGateway(int port, GatewayRateLimitConfig rl) : port_(port), rl_cfg_(rl), server_(std::make_unique<httplib::Server>()) {
	init_limiters();
}

RestGateway::~RestGateway() {
	stop();
}

void RestGateway::register_routes() {
	// Health
	server_->Get("/health", [this](const httplib::Request&, httplib::Response& res){
		res.set_content(responses::ok({{"service", "gateway"}}).dump(), "application/json");
	});

	server_->Get("/api/v1/ping", [this](const httplib::Request&, httplib::Response& res){
		if (!rate_allow("global")) {
			auto err = responses::error("RATE_LIMITED", "Too many requests", 429);
			res.status = 429; res.set_content(err.dump(), "application/json"); return; }
		res.set_content(responses::ok({{"pong", true}}).dump(), "application/json");
	});

	// OPTIONS preflight for any path
	server_->Options(R"(/.*)", [](const httplib::Request&, httplib::Response& res){ res.status = 204; });

	// Placeholder Note endpoints
	server_->Post("/api/v1/notes", [this](const httplib::Request& req, httplib::Response& res){
		try { auto body = json::parse(req.body);
			auto resp = responses::ok({{"id", "note_123"}, {"text", body.value("text", "")}});
			res.status = 201; res.set_content(resp.dump(), "application/json"); }
		catch(const std::exception& e){ auto err = responses::error("BAD_REQUEST", e.what(), 400); res.status=400; res.set_content(err.dump(), "application/json"); }
	});

	server_->Get(R"(/api/v1/notes/(.+))", [](const httplib::Request& req, httplib::Response& res){
		auto id = req.matches[1];
		auto resp = responses::ok({{"id", id.str()}, {"text", "Sample note"}});
		res.set_content(resp.dump(), "application/json");
	});

	// Auth endpoints (stubs) ---------------------------------
	server_->Post("/api/v1/auth/login", [this](const httplib::Request& req, httplib::Response& res){
		if (!rate_allow("auth_login")) { res.status=429; res.set_content(responses::error("RATE_LIMITED","Too many login attempts",429).dump(),"application/json"); return; }
		try { auto body = json::parse(req.body); std::string username = body.value("username","user"); json token{{"sub",username},{"scope","read:profile write:note"},{"sid","sess123"},{"exp",9999999999}}; res.set_content(responses::ok({{"access_token",token.dump()},{"token_type","bearer"},{"expires_in",3600}}).dump(),"application/json"); }
		catch(const std::exception& e){ res.status=400; res.set_content(responses::error("BAD_REQUEST",e.what(),400).dump(),"application/json"); }
	});

	server_->Post("/api/v1/auth/register", [this](const httplib::Request& req, httplib::Response& res){
		if (!rate_allow("auth_register")) { res.status=429; res.set_content(responses::error("RATE_LIMITED","Too many registrations",429).dump(),"application/json"); return; }
		try { auto body = json::parse(req.body); std::string username = body.value("username","newuser"); auto resp = responses::ok({{"user", {{"username",username},{"id","user_123"}}}}); res.status=201; res.set_content(resp.dump(),"application/json"); }
		catch(const std::exception& e){ res.status=400; res.set_content(responses::error("BAD_REQUEST",e.what(),400).dump(),"application/json"); }
	});

	// Timeline endpoint (stub)
	server_->Get("/api/v1/timeline/home", [this](const httplib::Request&, httplib::Response& res){
		if (!rate_allow("timeline_home")) { res.status=429; res.set_content(responses::error("RATE_LIMITED","Too many timeline requests",429).dump(),"application/json"); return; }
		json items = json::array(); for(int i=0;i<5;i++){ items.push_back({{"id","note_"+std::to_string(i)},{"text","Home timeline sample note #"+std::to_string(i)},{"metrics",{{"likes",i*3},{"renotes",i}}}}); }
		auto resp = responses::ok({{"items", items}, {"next_cursor", nullptr}});
		res.set_content(resp.dump(), "application/json");
	});
}

bool RestGateway::start() {
	if (running_.load()) return true;
	register_routes();
	running_.store(true);
	server_thread_ = std::thread([this]{
		std::cout << "REST Gateway listening on :" << port_ << std::endl;
		server_->listen("0.0.0.0", port_);
	});
	return true;
}

void RestGateway::stop() {
	if (!running_.load()) return;
	running_.store(false);
	if (server_) server_->stop();
	if (server_thread_.joinable()) server_thread_.join();
}

void RestGateway::init_limiters() {
	using RateLimiter = sonet::gateway::rate_limiting::RateLimiter;
	using LimitConfig = sonet::gateway::rate_limiting::LimitConfig;
	auto make = [](int per_min) { return std::make_unique<RateLimiter>(LimitConfig{per_min, per_min, 60'000}); };
	limiters_["global"] = make(rl_cfg_.global_per_minute);
	limiters_["auth_login"] = make(rl_cfg_.auth_login_per_minute);
	limiters_["auth_register"] = make(rl_cfg_.auth_register_per_minute);
	limiters_["timeline_home"] = make(rl_cfg_.timeline_home_per_minute);
	limiters_["notes_create"] = make(rl_cfg_.notes_create_per_minute);
}

bool RestGateway::rate_allow(const std::string& key) {
	auto it = limiters_.find(key);
	if (it == limiters_.end()) return true; // no limiter configured
	return it->second->allow(key);
}

} // namespace sonet::gateway
