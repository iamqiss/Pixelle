#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <string>
#include "mls_protocol.hpp"

class GroupManagementTest : public ::testing::Test {
protected:
    void SetUp() override {
        mls_protocol_ = std::make_unique<sonet::mls::MLSProtocol>();
    }

    void TearDown() override {
        mls_protocol_.reset();
    }

    std::unique_ptr<sonet::mls::MLSProtocol> mls_protocol_;
    
    // Helper method to create a test group
    std::vector<uint8_t> createTestGroup() {
        std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
        
        return mls_protocol_->create_group(group_id, 
                                         sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                         extensions);
    }
    
    // Helper method to create a test key package
    sonet::mls::KeyPackage createTestKeyPackage() {
        sonet::mls::KeyPackage key_package;
        key_package.version = {0x00, 0x01};
        key_package.cipher_suite = {0x00, 0x01};
        key_package.init_key = std::vector<uint8_t>(32, 0x42);
        key_package.leaf_node.public_key = std::vector<uint8_t>(32, 0x43);
        key_package.leaf_node.signature_key = std::vector<uint8_t>(32, 0x44);
        key_package.leaf_node.encryption_key = std::vector<uint8_t>(32, 0x45);
        key_package.leaf_node.signature = std::vector<uint8_t>(64, 0x46);
        
        return key_package;
    }
};

// Test group creation and initial state
TEST_F(GroupManagementTest, GroupCreation) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    auto result = mls_protocol_->create_group(group_id, 
                                             sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                             extensions);
    
    EXPECT_FALSE(result.empty());
    
    // Check initial member count
    uint32_t member_count = mls_protocol_->get_group_member_count(group_id);
    EXPECT_EQ(member_count, 0); // Initial group has no members
    
    // Check if can add member
    bool can_add = mls_protocol_->can_add_member(group_id);
    EXPECT_TRUE(can_add);
    
    // Check group size status
    auto status = mls_protocol_->get_group_size_status(group_id);
    EXPECT_EQ(status, sonet::mls::GroupSizeStatus::OPTIMAL);
}

// Test adding members within limits
TEST_F(GroupManagementTest, AddMembersWithinLimits) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Add members up to optimal size
    for (uint32_t i = 0; i < 250; i++) {
        auto key_package = createTestKeyPackage();
        auto result = mls_protocol_->add_member(group_id, key_package);
        EXPECT_FALSE(result.empty());
        
        uint32_t member_count = mls_protocol_->get_group_member_count(group_id);
        EXPECT_EQ(member_count, i + 1);
        
        bool can_add = mls_protocol_->can_add_member(group_id);
        EXPECT_TRUE(can_add);
        
        auto status = mls_protocol_->get_group_size_status(group_id);
        EXPECT_EQ(status, sonet::mls::GroupSizeStatus::OPTIMAL);
    }
}

// Test adding members beyond optimal size
TEST_F(GroupManagementTest, AddMembersBeyondOptimal) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Add members up to warning threshold
    for (uint32_t i = 0; i < 400; i++) {
        auto key_package = createTestKeyPackage();
        auto result = mls_protocol_->add_member(group_id, key_package);
        EXPECT_FALSE(result.empty());
        
        uint32_t member_count = mls_protocol_->get_group_member_count(group_id);
        EXPECT_EQ(member_count, i + 1);
        
        bool can_add = mls_protocol_->can_add_member(group_id);
        EXPECT_TRUE(can_add);
        
        auto status = mls_protocol_->get_group_size_status(group_id);
        if (member_count <= 250) {
            EXPECT_EQ(status, sonet::mls::GroupSizeStatus::OPTIMAL);
        } else if (member_count <= 400) {
            EXPECT_EQ(status, sonet::mls::GroupSizeStatus::GOOD);
        }
    }
}

// Test adding members up to maximum limit
TEST_F(GroupManagementTest, AddMembersToMaximum) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Add members up to maximum limit
    for (uint32_t i = 0; i < 500; i++) {
        auto key_package = createTestKeyPackage();
        auto result = mls_protocol_->add_member(group_id, key_package);
        EXPECT_FALSE(result.empty());
        
        uint32_t member_count = mls_protocol_->get_group_member_count(group_id);
        EXPECT_EQ(member_count, i + 1);
        
        auto status = mls_protocol_->get_group_size_status(group_id);
        if (member_count <= 250) {
            EXPECT_EQ(status, sonet::mls::GroupSizeStatus::OPTIMAL);
        } else if (member_count <= 400) {
            EXPECT_EQ(status, sonet::mls::GroupSizeStatus::GOOD);
        } else if (member_count < 500) {
            EXPECT_EQ(status, sonet::mls::GroupSizeStatus::WARNING);
        } else if (member_count == 500) {
            EXPECT_EQ(status, sonet::mls::GroupSizeStatus::AT_LIMIT);
        }
    }
    
    // Check that we can't add more members
    bool can_add = mls_protocol_->can_add_member(group_id);
    EXPECT_FALSE(can_add);
}

// Test attempting to exceed maximum limit
TEST_F(GroupManagementTest, ExceedMaximumLimit) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Fill group to maximum
    for (uint32_t i = 0; i < 500; i++) {
        auto key_package = createTestKeyPackage();
        mls_protocol_->add_member(group_id, key_package);
    }
    
    // Try to add one more member - should fail
    auto key_package = createTestKeyPackage();
    EXPECT_THROW(mls_protocol_->add_member(group_id, key_package), std::runtime_error);
    
    uint32_t member_count = mls_protocol_->get_group_member_count(group_id);
    EXPECT_EQ(member_count, 500);
    
    auto status = mls_protocol_->get_group_size_status(group_id);
    EXPECT_EQ(status, sonet::mls::GroupSizeStatus::AT_LIMIT);
}

// Test performance optimization for different group sizes
TEST_F(GroupManagementTest, PerformanceOptimization) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Test optimization for optimal size
    auto optimized = mls_protocol_->optimize_group_performance(group_id);
    EXPECT_FALSE(optimized.empty());
    
    // Add members to warning threshold
    for (uint32_t i = 0; i < 450; i++) {
        auto key_package = createTestKeyPackage();
        mls_protocol_->add_member(group_id, key_package);
    }
    
    // Test optimization for warning size
    optimized = mls_protocol_->optimize_group_performance(group_id);
    EXPECT_FALSE(optimized.empty());
    
    // Fill to maximum
    for (uint32_t i = 450; i < 500; i++) {
        auto key_package = createTestKeyPackage();
        mls_protocol_->add_member(group_id, key_package);
    }
    
    // Test optimization for maximum size
    optimized = mls_protocol_->optimize_group_performance(group_id);
    EXPECT_FALSE(optimized.empty());
}

// Test member removal and status updates
TEST_F(GroupManagementTest, MemberRemoval) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Add members to warning threshold
    for (uint32_t i = 0; i < 450; i++) {
        auto key_package = createTestKeyPackage();
        mls_protocol_->add_member(group_id, key_package);
    }
    
    auto status = mls_protocol_->get_group_size_status(group_id);
    EXPECT_EQ(status, sonet::mls::GroupSizeStatus::WARNING);
    
    // Remove some members
    for (uint32_t i = 0; i < 200; i++) {
        mls_protocol_->remove_member(group_id, 0); // Remove first member each time
    }
    
    uint32_t member_count = mls_protocol_->get_group_member_count(group_id);
    EXPECT_EQ(member_count, 250);
    
    status = mls_protocol_->get_group_size_status(group_id);
    EXPECT_EQ(status, sonet::mls::GroupSizeStatus::OPTIMAL);
    
    bool can_add = mls_protocol_->can_add_member(group_id);
    EXPECT_TRUE(can_add);
}

// Test group size status transitions
TEST_F(GroupManagementTest, GroupSizeStatusTransitions) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Test status transitions as group grows
    std::vector<std::pair<uint32_t, sonet::mls::GroupSizeStatus>> test_cases = {
        {0, sonet::mls::GroupSizeStatus::OPTIMAL},
        {100, sonet::mls::GroupSizeStatus::OPTIMAL},
        {250, sonet::mls::GroupSizeStatus::OPTIMAL},
        {251, sonet::mls::GroupSizeStatus::GOOD},
        {400, sonet::mls::GroupSizeStatus::GOOD},
        {401, sonet::mls::GroupSizeStatus::WARNING},
        {499, sonet::mls::GroupSizeStatus::WARNING},
        {500, sonet::mls::GroupSizeStatus::AT_LIMIT},
    };
    
    for (const auto& test_case : test_cases) {
        uint32_t target_count = test_case.first;
        sonet::mls::GroupSizeStatus expected_status = test_case.second;
        
        uint32_t current_count = mls_protocol_->get_group_member_count(group_id);
        
        if (current_count < target_count) {
            // Add members to reach target
            for (uint32_t i = current_count; i < target_count; i++) {
                auto key_package = createTestKeyPackage();
                mls_protocol_->add_member(group_id, key_package);
            }
        } else if (current_count > target_count) {
            // Remove members to reach target
            for (uint32_t i = current_count; i > target_count; i--) {
                mls_protocol_->remove_member(group_id, 0);
            }
        }
        
        uint32_t actual_count = mls_protocol_->get_group_member_count(group_id);
        EXPECT_EQ(actual_count, target_count);
        
        auto actual_status = mls_protocol_->get_group_size_status(group_id);
        EXPECT_EQ(actual_status, expected_status);
    }
}

// Test performance optimization triggers
TEST_F(GroupManagementTest, PerformanceOptimizationTriggers) {
    std::vector<uint8_t> group_id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::vector<uint8_t> extensions = {0x01, 0x02, 0x03};
    
    mls_protocol_->create_group(group_id, 
                                sonet::mls::CipherSuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
                                extensions);
    
    // Test optimization at different thresholds
    std::vector<uint32_t> thresholds = {100, 250, 400, 500};
    
    for (uint32_t threshold : thresholds) {
        // Add members to threshold
        uint32_t current_count = mls_protocol_->get_group_member_count(group_id);
        for (uint32_t i = current_count; i < threshold; i++) {
            auto key_package = createTestKeyPackage();
            mls_protocol_->add_member(group_id, key_package);
        }
        
        // Optimize performance
        auto optimized = mls_protocol_->optimize_group_performance(group_id);
        EXPECT_FALSE(optimized.empty());
        
        // Verify optimization was applied
        uint32_t member_count = mls_protocol_->get_group_member_count(group_id);
        EXPECT_EQ(member_count, threshold);
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}