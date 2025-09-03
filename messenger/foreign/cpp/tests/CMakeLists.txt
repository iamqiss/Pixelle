# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if(BUILD_TESTS)
  find_package(Catch2 CONFIG REQUIRED)
  find_package(inja CONFIG REQUIRED)
  find_package(reproc++ CONFIG REQUIRED)

  file(GLOB_RECURSE UT_SOURCES "unit/*.cc")
  add_executable(
    messenger_cpp_test

    ${UT_SOURCES}
  )
  target_include_directories(messenger_cpp_test PRIVATE ${MESSENGER_INCLUDE_DIRS})
  target_link_libraries(
    messenger_cpp_test

    messenger
    Catch2::Catch2
    Catch2::Catch2WithMain
    pantor::inja
    reproc++
  )

  file(GLOB_RECURSE E2E_SOURCES "e2e/*.cc")
  add_executable(
    messenger_e2e_test

    ${E2E_SOURCES}
  )
  target_include_directories(messenger_e2e_test PRIVATE ${MESSENGER_INCLUDE_DIRS})
  target_link_libraries(
    messenger_e2e_test

    PRIVATE

    messenger
    Catch2::Catch2
    Catch2::Catch2WithMain
    reproc++
  )


  if(ENABLE_CODE_COVERAGE)
    include(../cmake/modules/CodeCoverage.cmake)

    setup_target_for_coverage_gcovr_coveralls(
      NAME coveralls
      EXECUTABLE messenger_cpp_test
      EXCLUDE "build/vcpkg_installed/*" "tests/*"
      DEPENDENCIES messenger)

    setup_target_for_coverage_gcovr_html(
      NAME coverage
      EXECUTABLE messenger_cpp_test
      EXCLUDE "build/vcpkg_installed/*" "tests/*"
      DEPENDENCIES messenger)
  endif()

  # download correct version of Messenger, flatten out the binaries and make them executable;
  # we are going to use these directly for E2E testing rather than the Docker version
  # to allow for finer-grained control
  execute_process(
    COMMAND bash -c "curl --silent https://api.github.com/repos/apache/messenger/releases/latest | jq -r .tag_name"
    OUTPUT_VARIABLE MESSENGER_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  set(ARCH "${CMAKE_HOST_SYSTEM_PROCESSOR}")
  set(MESSENGER_DOWNLOAD_BASENAME "messenger-Linux-${ARCH}-musl.zip")
  set(URL "https://github.com/apache/messenger/releases/download/${MESSENGER_VERSION}/${MESSENGER_DOWNLOAD_BASENAME}")
  set(MESSENGER_DOWNLOAD_FILE "${CMAKE_CURRENT_BINARY_DIR}/${MESSENGER_DOWNLOAD_BASENAME}")
  message(STATUS "Downloading latest Messenger server MUSL release build from ${URL}")

  file(DOWNLOAD ${URL} ${MESSENGER_DOWNLOAD_FILE} SHOW_PROGRESS)
  execute_process(COMMAND unzip -o ${MESSENGER_DOWNLOAD_FILE} -d ${CMAKE_CURRENT_BINARY_DIR})
  execute_process(COMMAND find ${CMAKE_CURRENT_BINARY_DIR}/all_artifacts -type f -exec mv {} ${CMAKE_CURRENT_BINARY_DIR} \;)
  execute_process(COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_CURRENT_BINARY_DIR}/all_artifacts)
  file(REMOVE ${MESSENGER_DOWNLOAD_FILE})

  set(EXECUTABLE_FILES "messenger" "messenger-server")
  foreach(FILE_NAME ${EXECUTABLE_FILES})
    set(FILE_PATH "${CMAKE_CURRENT_BINARY_DIR}/${FILE_NAME}")
    file(CHMOD ${FILE_PATH} PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ)
  endforeach()

endif()
