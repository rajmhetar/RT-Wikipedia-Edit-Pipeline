add_executable(rt_wiki_pipeline main.cpp sse_client.cpp)
target_compile_features(rt_wiki_pipeline PRIVATE cxx_std_20)
target_enable_warnings(rt_wiki_pipeline)
target_include_directories(rt_wiki_pipeline PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(rt_wiki_pipeline PRIVATE
    CURL::libcurl
    nlohmann_json::nlohmann_json
)
