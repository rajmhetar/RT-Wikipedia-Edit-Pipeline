cmake_minimum_required(VERSION 3.20)
project(rt_wiki_pipeline LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

include(cmake/warnings.cmake)
include(FetchContent)

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        v3.6.0
)

# Header-only JSON library — used for SSE event parsing.
FetchContent_Declare(
  nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG        v3.11.3
)

FetchContent_MakeAvailable(Catch2 nlohmann_json)

# libcurl must be installed on the host (e.g. via vcpkg, apt, or brew).
# On Windows with vcpkg: vcpkg install curl
find_package(CURL REQUIRED)

enable_testing()

add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(bench)
add_subdirectory(tools)
