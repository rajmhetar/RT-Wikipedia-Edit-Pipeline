#include <catch2/catch_test_macros.hpp>
#include "spsc_ring_buffer.hpp"
#include "wiki_event.hpp"
#include <cstring>
#include <string_view>
#include <thread>
#include <vector>

// ============================================================
// Single-threaded correctness
// ============================================================

TEST_CASE("ring buffer: empty on construction", "[ring_buffer][smoke]") {
    SPSCRingBuffer<int, 4> rb;
    int x{};
    CHECK(rb.size_approx() == 0);
    CHECK_FALSE(rb.pop(x));
}

TEST_CASE("ring buffer: push and pop round-trip", "[ring_buffer]") {
    SPSCRingBuffer<int, 4> rb;
    REQUIRE(rb.push(42));
    int x{};
    REQUIRE(rb.pop(x));
    CHECK(x == 42);
    CHECK_FALSE(rb.pop(x));  // must be empty now
}

TEST_CASE("ring buffer: FIFO ordering", "[ring_buffer]") {
    SPSCRingBuffer<int, 8> rb;
    for (int i = 0; i < 5; ++i) REQUIRE(rb.push(i));
    for (int i = 0; i < 5; ++i) {
        int x{};
        REQUIRE(rb.pop(x));
        CHECK(x == i);
    }
}

TEST_CASE("ring buffer: push fails when full", "[ring_buffer]") {
    SPSCRingBuffer<int, 4> rb;
    for (int i = 0; i < 4; ++i) REQUIRE(rb.push(i));
    CHECK_FALSE(rb.push(99));          // 5th push must fail
    CHECK(rb.size_approx() == 4);
}

TEST_CASE("ring buffer: pop fails when empty after full drain", "[ring_buffer]") {
    SPSCRingBuffer<int, 4> rb;
    REQUIRE(rb.push(7));
    int x{};
    REQUIRE(rb.pop(x));
    CHECK(x == 7);
    CHECK_FALSE(rb.pop(x));
}

TEST_CASE("ring buffer: wraparound across multiple fill/drain cycles", "[ring_buffer]") {
    // Exercises the bitmask index wrap: head_ and tail_ grow past N.
    SPSCRingBuffer<int, 4> rb;
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 4; ++i) REQUIRE(rb.push(round * 10 + i));
        for (int i = 0; i < 4; ++i) {
            int x{};
            REQUIRE(rb.pop(x));
            CHECK(x == round * 10 + i);
        }
    }
}

TEST_CASE("ring buffer: interleaved push/pop stays correct", "[ring_buffer]") {
    // Simulates a producer and consumer that keep pace with each other.
    SPSCRingBuffer<int, 4> rb;
    for (int i = 0; i < 16; ++i) {
        REQUIRE(rb.push(i));
        int x{};
        REQUIRE(rb.pop(x));
        CHECK(x == i);
    }
}

TEST_CASE("ring buffer: capacity() is a compile-time constant", "[ring_buffer]") {
    static_assert(SPSCRingBuffer<int, 8>::capacity()    == 8);
    static_assert(SPSCRingBuffer<int, 1024>::capacity() == 1024);
    SUCCEED("capacity constants verified at compile time");
}

TEST_CASE("ring buffer: WikiEvent fits in a slot", "[ring_buffer]") {
    // Confirm the actual event struct can round-trip through a buffer slot.
    SPSCRingBuffer<WikiEvent, 16> rb;
    WikiEvent ev{};
    std::memcpy(ev.title, "TestPage", 9);
    ev.timestamp    = 1704067200;
    ev.length_delta = -42;
    ev.is_bot       = true;

    REQUIRE(rb.push(ev));
    WikiEvent out{};
    REQUIRE(rb.pop(out));
    CHECK(std::string_view{out.title} == "TestPage");
    CHECK(out.timestamp    == 1704067200);
    CHECK(out.length_delta == -42);
    CHECK(out.is_bot       == true);
}

// ============================================================
// Multi-threaded correctness
// ============================================================

TEST_CASE("ring buffer: producer-consumer delivers all items in order",
          "[ring_buffer][threaded]") {
    SPSCRingBuffer<int, 256> rb;
    constexpr int kCount = 100'000;
    std::vector<int> received;
    received.reserve(kCount);

    std::thread producer([&] {
        for (int i = 0; i < kCount; ++i)
            while (!rb.push(i)) { /* spin: buffer full, wait for consumer */ }
    });

    while (static_cast<int>(received.size()) < kCount) {
        int x{};
        if (rb.pop(x)) received.push_back(x);
    }
    producer.join();

    REQUIRE(static_cast<int>(received.size()) == kCount);
    for (int i = 0; i < kCount; ++i)
        CHECK(received[i] == i);
}
