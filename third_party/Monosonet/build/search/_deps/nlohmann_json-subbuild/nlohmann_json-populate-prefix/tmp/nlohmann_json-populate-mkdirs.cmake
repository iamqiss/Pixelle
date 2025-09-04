# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/workspace/build/search/_deps/nlohmann_json-src")
  file(MAKE_DIRECTORY "/workspace/build/search/_deps/nlohmann_json-src")
endif()
file(MAKE_DIRECTORY
  "/workspace/build/search/_deps/nlohmann_json-build"
  "/workspace/build/search/_deps/nlohmann_json-subbuild/nlohmann_json-populate-prefix"
  "/workspace/build/search/_deps/nlohmann_json-subbuild/nlohmann_json-populate-prefix/tmp"
  "/workspace/build/search/_deps/nlohmann_json-subbuild/nlohmann_json-populate-prefix/src/nlohmann_json-populate-stamp"
  "/workspace/build/search/_deps/nlohmann_json-subbuild/nlohmann_json-populate-prefix/src"
  "/workspace/build/search/_deps/nlohmann_json-subbuild/nlohmann_json-populate-prefix/src/nlohmann_json-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/workspace/build/search/_deps/nlohmann_json-subbuild/nlohmann_json-populate-prefix/src/nlohmann_json-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/workspace/build/search/_deps/nlohmann_json-subbuild/nlohmann_json-populate-prefix/src/nlohmann_json-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
