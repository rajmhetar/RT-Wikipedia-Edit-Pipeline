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
FetchContent_MakeAvailable(Catch2)

enable_testing()

add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(bench)
add_subdirectory(tools)
