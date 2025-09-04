//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

// Video processor using ffmpeg to extract a thumbnail and probe basic info.

#include "../service.h"
#include <cstdlib>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;
namespace sonet::media_service {

class FfmpegVideoProcessor final : public VideoProcessor {
public:
	bool Process(const std::string& path_in, std::string& path_out, std::string& thumb_out, double& duration, uint32_t& width, uint32_t& height) override {
		path_out = path_in; // keep original for now
		auto tpath = fs::path(path_in).parent_path() / (fs::path(path_in).filename().string() + ".thumb.jpg");
		auto ShellQuote = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; };
		// Grab a frame at 00:00:01 if possible
		std::string cmd = "ffmpeg -y -ss 00:00:01 -i " + ShellQuote(path_in) + " -frames:v 1 -q:v 5 " + ShellQuote(tpath.string()) + " >/dev/null 2>&1";
		int rc = std::system(cmd.c_str());
		if (rc != 0) { thumb_out = path_in; } else { thumb_out = tpath.string(); }
		// Probe duration & dimensions with ffprobe (default text output)
		duration = 0.0; width = 0; height = 0;
		std::string meta_file = tpath.string() + ".meta";
		std::string cmd_meta = "ffprobe -v error -select_streams v:0 -show_entries stream=width,height -show_entries format=duration -of default=noprint_wrappers=1:nokey=0 " + ShellQuote(path_in) + " > " + ShellQuote(meta_file) + " 2>/dev/null";
		int mrc = std::system(cmd_meta.c_str()); (void)mrc;
		std::ifstream mf(meta_file);
		if (mf) {
			std::string line;
			while (std::getline(mf, line)) {
				if (line.rfind("width=",0)==0) { width = static_cast<uint32_t>(std::stoul(line.substr(6))); }
				else if (line.rfind("height=",0)==0) { height = static_cast<uint32_t>(std::stoul(line.substr(7))); }
				else if (line.rfind("duration=",0)==0) { duration = std::stod(line.substr(9)); }
			}
		}
		fs::remove(meta_file);
		return true;
	}
};

std::unique_ptr<VideoProcessor> CreateVideoProcessor() { return std::make_unique<FfmpegVideoProcessor>(); }

} // namespace sonet::media_service

