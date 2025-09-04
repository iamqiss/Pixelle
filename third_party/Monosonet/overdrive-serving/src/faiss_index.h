#pragma once

#include <string>
#include <vector>
#include <memory>

namespace overdrive {

class FaissIndex {
public:
	virtual ~FaissIndex() = default;
	
	// Build index from vectors
	virtual bool BuildIndex(const std::vector<std::vector<float>>& vectors, const std::vector<std::string>& ids) = 0;
	
	// Query top-k similar vectors
	virtual std::vector<std::pair<std::string, float>> Search(const std::vector<float>& query_vector, int k) = 0;
	
	// Save/load index
	virtual bool SaveIndex(const std::string& path) = 0;
	virtual bool LoadIndex(const std::string& path) = 0;
	
	// Get index info
	virtual size_t GetSize() const = 0;
	virtual int GetDimension() const = 0;
};

// Factory function
std::unique_ptr<FaissIndex> CreateFaissIndex(const std::string& type = "hnsw");

} // namespace overdrive