// Stage 9 pipeline benchmark: full end-to-end throughput.
// Section 1 — parse-only baseline (no ring buffer)
// Section 2 — 4-shard SPSC pipeline with drop-on-full
//
// Build: cmake --build build --target bench_pipeline
// Run:   .\build\bin\bench_pipeline.exe

#include "replay_source.hpp"
#include "spsc_ring_buffer.hpp"
#include "wiki_event.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string_view>
#include <thread>

namespace fs = std::filesystem;

static constexpr int         kNumShards = 4;
static constexpr std::size_t kQueueCap  = 4096;
static constexpr int         kNumEvents = 200'000;

static std::string make_payload(int i) {
    char buf[512];
    // 1 000 distinct page titles to exercise realistic hash routing.
    std::snprintf(buf, sizeof(buf),
        R"({"type":"edit","title":"Bench%04d","wiki":"enwiki","user":"BenchUser","comment":"bench","bot":false,"timestamp":%d,"revision":{"new":%d,"old":%d},"length":{"new":100,"old":90}})",
        i % 1000, 1700000000 + i, i + 1, i);
    return buf;
}

static int shard_for(const char* title) {
    return static_cast<int>(
        std::hash<std::string_view>{}(std::string_view{title}) &
        static_cast<std::size_t>(kNumShards - 1));
}

// Section 2: ring buffer + consumer threads.
// Static to avoid stack overflow: 4 shards × 4096 × ~1 KB = tens of MB.
struct Shard {
    SPSCRingBuffer<WikiEvent, kQueueCap> q;
    std::uint64_t consumed{0};
    std::uint64_t dropped{0};
};
static std::array<Shard, kNumShards> g_shards;

int main() {
    // 1. Write a synthetic capture file.
    const auto path = (fs::temp_directory_path() / "bench_pipeline_cap.tsv").string();
    {
        std::ofstream f(path);
        for (int i = 0; i < kNumEvents; ++i)
            f << (std::int64_t{1'000'000} * (i + 1)) << '\t' << make_payload(i) << '\n';
    }

    // -----------------------------------------------------------------------
    // Section 1 — Parse-only baseline (JSON parse + replay overhead, no queue)
    // -----------------------------------------------------------------------
    std::printf("=== Parse-only baseline (%d events) ===\n", kNumEvents);
    {
        std::size_t n = 0;
        const auto  t0 = std::chrono::steady_clock::now();
        {
            ReplaySource src{path, ReplaySource::kNoDelay};
            src.run([&](const WikiEvent&) { ++n; });
        }
        const double secs = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - t0).count();
        std::printf("  Parsed %zu events in %.3f s  =>  %.0f K ev/s\n",
                    n, secs, static_cast<double>(n) / secs / 1'000.0);
    }
    std::puts("");

    // -----------------------------------------------------------------------
    // Section 2 — 4-shard SPSC pipeline (drop-on-full; consumers spin)
    // -----------------------------------------------------------------------
    std::printf("=== 4-shard SPSC pipeline (%d events, drop-on-full) ===\n", kNumEvents);

    // Reset per-shard counters (g_shards is static; fields are only written here).
    for (auto& s : g_shards) { s.consumed = 0; s.dropped = 0; }

    std::atomic<bool> done{false};
    std::array<std::thread, kNumShards> consumers;
    for (int i = 0; i < kNumShards; ++i) {
        consumers[i] = std::thread([i, &done] {
            WikiEvent ev{};
            // Pure spin: no yield() so the consumer doesn't cede the core.
            while (!done.load(std::memory_order_acquire)) {
                if (g_shards[i].q.pop(ev)) ++g_shards[i].consumed;
            }
            // Drain any tail items the producer left after setting done=true.
            while (g_shards[i].q.pop(ev)) ++g_shards[i].consumed;
        });
    }

    std::uint64_t pushed = 0;
    const auto    t0     = std::chrono::steady_clock::now();
    {
        ReplaySource src{path, ReplaySource::kNoDelay};
        src.run([&](const WikiEvent& ev) {
            const int s = shard_for(ev.title);
            if (g_shards[s].q.push(ev)) ++pushed;
            else                        ++g_shards[s].dropped;
        });
    }
    done.store(true, std::memory_order_release);
    for (auto& t : consumers) t.join();
    const double elapsed_s = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count();

    std::uint64_t total_consumed = 0, total_dropped = 0;
    for (const auto& s : g_shards) {
        total_consumed += s.consumed;
        total_dropped  += s.dropped;
    }

    const double keps = static_cast<double>(pushed) / elapsed_s / 1'000.0;
    std::printf("  Pushed:    %llu   Consumed: %llu   Dropped: %llu\n",
                (unsigned long long)pushed,
                (unsigned long long)total_consumed,
                (unsigned long long)total_dropped);
    std::printf("  Elapsed:   %.3f s\n", elapsed_s);
    std::printf("  Throughput: %.0f K events/s  (includes JSON parse)\n", keps);

    fs::remove(path);
}
