#include <catch2/catch_test_macros.hpp>
#include "replay_source.hpp"
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Minimal valid JSON payloads that parse_wiki_event can handle.
// Fields not listed default to zero/empty in the parser.
static const char* kPayload[] = {
    R"({"type":"edit","title":"Page A","wiki":"enwiki","user":"Alice","comment":"c1","bot":false,"timestamp":1700000001,"revision":{"new":2,"old":1},"length":{"new":100,"old":90}})",
    R"({"type":"edit","title":"Page B","wiki":"enwiki","user":"Bob","comment":"c2","bot":false,"timestamp":1700000002,"revision":{"new":5,"old":4},"length":{"new":200,"old":180}})",
    R"({"type":"edit","title":"Page C","wiki":"enwiki","user":"Carol","comment":"c3","bot":true,"timestamp":1700000003,"revision":{"new":10,"old":9},"length":{"new":300,"old":290}})",
};
static constexpr int kPayloadCount = 3;

// Write a capture file with N events at the given microsecond timestamps.
// Returns the path as a string.
static std::string make_capture(const std::string& path,
                                 const int64_t* timestamps,
                                 const char** payloads, int n,
                                 bool add_comments = false) {
    std::ofstream f(path);
    REQUIRE(f.is_open());
    if (add_comments) {
        f << "# rt-wiki-pipeline capture v1\n";
        f << "# format: <unix_timestamp_us>\\t<json_payload>\\n\n";
        f << "\n";
    }
    for (int i = 0; i < n; ++i)
        f << timestamps[i] << '\t' << payloads[i] << '\n';
    return path;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_CASE("replay: events delivered in order at max speed", "[replay][smoke]") {
    auto path = (fs::temp_directory_path() / "wiki_replay_smoke.tsv").string();
    int64_t ts[] = {1'000'000, 2'000'000, 3'000'000};
    make_capture(path, ts, kPayload, kPayloadCount);

    std::vector<std::string> titles;
    ReplaySource src{path, ReplaySource::kNoDelay};
    const bool ok = src.run([&](const WikiEvent& ev) {
        titles.emplace_back(ev.title);
    });

    CHECK(ok);
    REQUIRE(titles.size() == 3);
    CHECK(titles[0] == "Page A");
    CHECK(titles[1] == "Page B");
    CHECK(titles[2] == "Page C");

    fs::remove(path);
}

TEST_CASE("replay: comment lines and blank lines are skipped", "[replay]") {
    auto path = (fs::temp_directory_path() / "wiki_replay_comments.tsv").string();
    int64_t ts[] = {1'000'000, 2'000'000, 3'000'000};
    make_capture(path, ts, kPayload, kPayloadCount, /*add_comments=*/true);

    std::vector<std::string> titles;
    ReplaySource src{path, ReplaySource::kNoDelay};
    src.run([&](const WikiEvent& ev) { titles.emplace_back(ev.title); });

    REQUIRE(titles.size() == 3);
    CHECK(titles[0] == "Page A");
    CHECK(titles[2] == "Page C");

    fs::remove(path);
}

TEST_CASE("replay: file not found returns false", "[replay]") {
    ReplaySource src{"/nonexistent/path/wiki_capture_no_such_file.tsv",
                     ReplaySource::kNoDelay};
    const bool ok = src.run([](const WikiEvent&) {});
    CHECK_FALSE(ok);
}

TEST_CASE("replay: malformed lines are silently skipped", "[replay]") {
    auto path = (fs::temp_directory_path() / "wiki_replay_malformed.tsv").string();
    {
        std::ofstream f(path);
        REQUIRE(f.is_open());
        f << "not-a-number\tsome data\n";                       // bad timestamp
        f << "\t\n";                                             // no timestamp, no payload
        f << "1000000\t\n";                                     // valid timestamp, empty payload
        f << "2000000\t" << kPayload[0] << "\n";               // good — should deliver
        f << "3000000\t{invalid json}\n";                       // parse_wiki_event returns false
        f << "4000000\t" << kPayload[1] << "\n";               // good — should deliver
    }

    std::vector<std::string> titles;
    ReplaySource src{path, ReplaySource::kNoDelay};
    const bool ok = src.run([&](const WikiEvent& ev) {
        titles.emplace_back(ev.title);
    });

    CHECK(ok);
    REQUIRE(titles.size() == 2);
    CHECK(titles[0] == "Page A");
    CHECK(titles[1] == "Page B");

    fs::remove(path);
}

TEST_CASE("replay: event fields are fully preserved", "[replay]") {
    auto path = (fs::temp_directory_path() / "wiki_replay_fields.tsv").string();
    int64_t ts[] = {1'000'000};
    const char* p[] = {kPayload[2]};  // Page C, bot=true
    make_capture(path, ts, p, 1);

    WikiEvent received{};
    ReplaySource src{path, ReplaySource::kNoDelay};
    src.run([&](const WikiEvent& ev) { received = ev; });

    CHECK(std::string(received.title) == "Page C");
    CHECK(std::string(received.user)  == "Carol");
    CHECK(received.is_bot            == true);
    CHECK(received.timestamp         == 1700000003);
    CHECK(received.revision_id       == 10);
    CHECK(received.parent_revision_id == 9);
    CHECK(received.length_delta      == 10);  // 300 - 290

    fs::remove(path);
}

TEST_CASE("replay: stop() terminates replay before all events are consumed", "[replay]") {
    auto path = (fs::temp_directory_path() / "wiki_replay_stop.tsv").string();

    // Write 200 events; stop() will be called after the first few arrive.
    {
        std::ofstream f(path);
        REQUIRE(f.is_open());
        for (int i = 0; i < 200; ++i) {
            // Reuse kPayload cyclically.
            f << (int64_t{1'000'000} * (i + 1)) << '\t'
              << kPayload[i % kPayloadCount] << '\n';
        }
    }

    std::atomic<std::size_t> count{0};
    ReplaySource src{path, ReplaySource::kNoDelay};
    std::thread t([&] {
        src.run([&](const WikiEvent&) {
            if (count.fetch_add(1, std::memory_order_relaxed) == 9)
                src.stop();  // stop after the 10th event
        });
    });
    t.join();

    // Should have stopped well before all 200 events were delivered.
    CHECK(count.load() < 200);

    fs::remove(path);
}
