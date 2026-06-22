#include <catch2/catch_test_macros.hpp>

// Scaffold smoke test — proves the build system and Catch2 are wired up.
// Real ring-buffer tests arrive in stage 3.
TEST_CASE("scaffold sanity", "[smoke]") {
    REQUIRE(1 + 1 == 2);
}
