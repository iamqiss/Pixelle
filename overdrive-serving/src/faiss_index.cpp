#include "faiss_index.h"
#include <random>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>

namespace overdrive {

class StubFaissIndex : public FaissIndex {
public:
	bool BuildIndex(const std::vector<std::vector<float>>& vectors, const std::vector<std::string>& ids) override {
		vectors_ = vectors;
		ids_ = ids;
		dimension_ = vectors.empty() ? 0 : vectors[0].size();
		std::cout << "Stub FAISS index built with " << vectors.size() << " vectors of dimension " << dimension_ << std::endl;
		return true;
	}
	
	std::vector<std::pair<std::string, float>> Search(const std::vector<float>& query_vector, int k) override {
		std::vector<std::pair<std::string, float>> results;
		if (ids_.empty()) return results;
		
		// Simple similarity search using cosine distance
		std::vector<std::pair<float, int>> similarities;
		similarities.reserve(ids_.size());
		
		for (size_t i = 0; i < vectors_.size(); ++i) {
			float similarity = ComputeCosineSimilarity(query_vector, vectors_[i]);
			similarities.emplace_back(similarity, i);
		}
		
		// Sort by similarity (descending)
		std::sort(similarities.begin(), similarities.end(), 
				 [](const auto& a, const auto& b) { return a.first > b.first; });
		
		// Return top-k results
		k = std::min(k, static_cast<int>(similarities.size()));
		for (int i = 0; i < k; ++i) {
			int idx = similarities[i].second;
			results.emplace_back(ids_[idx], similarities[i].first);
		}
		
		return results;
	}
	
	bool SaveIndex(const std::string& path) override {
		std::cout << "Stub FAISS index saved to " << path << std::endl;
		return true;
	}
	
	bool LoadIndex(const std::string& path) override {
		std::cout << "Stub FAISS index loaded from " << path << std::endl;
		return true;
	}
	
	size_t GetSize() const override { return vectors_.size(); }
	int GetDimension() const override { return dimension_; }

private:
	std::vector<std::vector<float>> vectors_;
	std::vector<std::string> ids_;
	int dimension_ = 0;
	
	float ComputeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
		if (a.size() != b.size() || a.empty()) return 0.0f;
		
		float dot_product = 0.0f;
		float norm_a = 0.0f;
		float norm_b = 0.0f;
		
		for (size_t i = 0; i < a.size(); ++i) {
			dot_product += a[i] * b[i];
			norm_a += a[i] * a[i];
			norm_b += b[i] * b[i];
		}
		
		norm_a = std::sqrt(norm_a);
		norm_b = std::sqrt(norm_b);
		
		if (norm_a == 0.0f || norm_b == 0.0f) return 0.0f;
		
		return dot_product / (norm_a * norm_b);
	}
};

std::unique_ptr<FaissIndex> CreateFaissIndex(const std::string& type) {
	// TODO: Return real FAISS implementation when linked
	// For now, return stub with improved similarity search
	return std::make_unique<StubFaissIndex>();
}

} // namespace overdrive