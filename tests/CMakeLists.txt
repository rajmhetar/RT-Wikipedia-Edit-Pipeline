add_executable(wiki_tests
    test_ring_buffer.cpp
    test_sse_parser.cpp
)
target_compile_features(wiki_tests PRIVATE cxx_std_20)
target_enable_warnings(wiki_tests)
target_include_directories(wiki_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(wiki_tests PRIVATE
    Catch2::Catch2WithMain
    nlohmann_json::nlohmann_json
)

include(Catch)
catch_discover_tests(wiki_tests)
