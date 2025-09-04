/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include <gtest/gtest.h>
#include <iostream>

int main(int argc, char **argv) {
    std::cout << "🧪 Running Sonet Tests..." << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    
    int result = RUN_ALL_TESTS();
    
    if (result == 0) {
        std::cout << "✅ All tests passed!" << std::endl;
    } else {
        std::cout << "❌ Some tests failed!" << std::endl;
    }
    
    return result;
}
