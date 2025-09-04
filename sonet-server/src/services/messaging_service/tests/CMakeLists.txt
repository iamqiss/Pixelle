cmake_minimum_required(VERSION 3.16)

if(NOT BUILD_TESTING)
  return()
endif()

find_package(GTest REQUIRED)

add_executable(messaging_crypto_roundtrip
  test_crypto_roundtrip.cpp
)

target_link_libraries(messaging_crypto_roundtrip
  GTest::gtest GTest::gtest_main
  jsoncpp_lib
  OpenSSL::SSL OpenSSL::Crypto
  libsodium::libsodium
)

add_test(NAME messaging_crypto_roundtrip COMMAND messaging_crypto_roundtrip)