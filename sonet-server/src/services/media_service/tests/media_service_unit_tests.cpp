// Basic unit + integration harness
#include <cassert>
#include <iostream>

// forward declaration from upload_integration_test.cpp
int run_upload_test();

int main() {
    std::cout << "media_service_unit_tests running\n";
    // Run integration style upload test
    int r = run_upload_test();
    assert(r == 0);
    std::cout << "All tests OK\n";
    return 0;
}
