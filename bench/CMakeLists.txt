add_executable(bench_spsc bench_spsc.cpp)
target_compile_features(bench_spsc PRIVATE cxx_std_20)
target_enable_warnings(bench_spsc)
target_include_directories(bench_spsc PRIVATE ${CMAKE_SOURCE_DIR}/include)
