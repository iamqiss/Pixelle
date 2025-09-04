#pragma once
#include <string>
#include <optional>

namespace sonet::gateway::auth {

struct JwtClaims {
	std::string subject;
	std::string scope;
	std::string session_id;
	long expires_at = 0;
	bool valid() const { return !subject.empty() && expires_at > 0; }
};

class JwtHandler {
public:
	explicit JwtHandler(std::string secret) : secret_(std::move(secret)) {}
	std::optional<JwtClaims> parse(const std::string& token) const;
private:
	std::string secret_;
};

}
