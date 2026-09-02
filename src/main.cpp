#include "edit_storm_detector.hpp"
#include "event_source.hpp"
#include "replay_source.hpp"
#include "sse_client.hpp"
#include "spsc_ring_buffer.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <curl/curl.h>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
static constexpr int         kNumShards     = 4;     // must be a power of two
static constexpr std::size_t kQueueCapacity = 1024;

static_assert(kNumShards > 0 && (kNumShards & (kNumShards - 1)) == 0,
              "kNumShards must be a power of two");

// ---------------------------------------------------------------------------
// Shard: one SPSC queue owned jointly by the producer (writer) and one
// consumer thread (reader).  The drop counter is written by the producer
// and read for reporting — an atomic suffices; no lock needed.
// ---------------------------------------------------------------------------
struct Shard {
    SPSCRingBuffer<WikiEvent, kQueueCapacity> queue;
    std::atomic<uint64_t>                    dropped{0};
};

// Global so the signal handler can reach them without a captured pointer.
static std::array<Shard, kNumShards>    g_shards;
static std::atomic<bool>                g_running{true};
static std::atomic<EventSource*>        g_source{nullptr};  // for Ctrl+C

// ---------------------------------------------------------------------------
// Signal handler — sets the stop flag and aborts the source so that both the
// producer and all consumers unwind cleanly.
// ---------------------------------------------------------------------------
static void on_signal(int) {
    g_running.store(false, std::memory_order_relaxed);
    EventSource* src = g_source.load(std::memory_order_relaxed);
    if (src) src->stop();
}

// ---------------------------------------------------------------------------
// Route an event to a shard by hashing the page title.
// ---------------------------------------------------------------------------
static int shard_for(const char* title) {
    std::size_t h = std::hash<std::string_view>{}(std::string_view{title});
    return static_cast<int>(h & static_cast<std::size_t>(kNumShards - 1));
}

// ---------------------------------------------------------------------------
// Stats printing — mutex prevents lines from N threads from interleaving.
// ---------------------------------------------------------------------------
static std::mutex g_print_mtx;

static void print_shard_stats(int id,
                               const std::unordered_map<std::string, uint64_t>& counts,
                               uint64_t total, uint64_t dropped) {
    std::vector<std::pair<uint64_t, std::string>> v;
    v.reserve(counts.size());
    for (const auto& [title, n] : counts) v.emplace_back(n, title);

    const std::size_t top = std::min<std::size_t>(5, v.size());
    std::partial_sort(v.begin(), v.begin() + top, v.end(), std::greater<>{});

    const double drop_pct = (total + dropped) > 0
        ? 100.0 * static_cast<double>(dropped) / static_cast<double>(total + dropped)
        : 0.0;

    std::lock_guard lock{g_print_mtx};
    std::cout << "\n[shard " << id << " | events=" << total
              << " dropped=" << dropped
              << " drop%=" << std::fixed << std::setprecision(2) << drop_pct << "]\n";
    for (std::size_t i = 0; i < top; ++i)
        std::cout << "  " << std::setw(4) << v[i].first << "  " << v[i].second << '\n';
    std::cout.flush();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    // --- Argument parsing --------------------------------------------------
    std::string record_path;
    std::string replay_path;
    double      replay_speed = 1.0;

    for (int i = 1; i < argc; ++i) {
        std::string_view a{argv[i]};
        if      (a == "--record" && i + 1 < argc) record_path  = argv[++i];
        else if (a == "--replay" && i + 1 < argc) replay_path  = argv[++i];
        else if (a == "--speed"  && i + 1 < argc) replay_speed = std::stod(argv[++i]);
        else {
            std::cerr << "Usage: " << argv[0]
                      << " [--record <file>] [--replay <file> [--speed <N>]]\n";
            return 1;
        }
    }

    std::signal(SIGINT, on_signal);
    curl_global_init(CURL_GLOBAL_DEFAULT);

    // --- Build the event source --------------------------------------------
    std::unique_ptr<EventSource> source;
    if (!replay_path.empty()) {
        source = std::make_unique<ReplaySource>(replay_path, replay_speed);
        std::cerr << "Replaying '" << replay_path << "' at "
                  << (replay_speed == ReplaySource::kNoDelay
                          ? "max speed"
                          : std::to_string(replay_speed) + "x")
                  << '\n';
    } else {
        std::optional<std::string> rec =
            record_path.empty() ? std::nullopt
                                : std::optional<std::string>{record_path};
        source = std::make_unique<SSEClient>(
            "https://stream.wikimedia.org/v2/stream/recentchange", rec);
        if (rec)
            std::cerr << "Connecting and recording to '" << record_path << "'...\n";
        else
            std::cerr << "Connecting...\n";
    }

    // --- Producer thread ---------------------------------------------------
    // Owns the event source. For each event, hashes the title to pick a
    // shard and pushes into that shard's queue.  Drop-on-full: never block
    // the network thread — it would stall the HTTP receive buffer.
    std::thread producer([&source] {
        g_source.store(source.get(), std::memory_order_relaxed);
        source->run([](const WikiEvent& ev) {
            int s = shard_for(ev.title);
            if (!g_shards[s].queue.push(ev))
                g_shards[s].dropped.fetch_add(1, std::memory_order_relaxed);
        });
        std::cerr << "Source finished.\n";
        g_source.store(nullptr, std::memory_order_relaxed);
        g_running.store(false, std::memory_order_release);
    });

    // --- N consumer threads -----------------------------------------------
    std::array<uint64_t, kNumShards> shard_totals{};

    std::array<std::thread, kNumShards> consumers;
    for (int i = 0; i < kNumShards; ++i) {
        consumers[i] = std::thread([i, &shard_totals] {
            auto& shard = g_shards[i];
            std::unordered_map<std::string, uint64_t> edit_counts;
            EditStormDetector storm;
            uint64_t total = 0;

            using Clock = std::chrono::steady_clock;
            auto next_report = Clock::now() + std::chrono::seconds(10);
            auto next_evict  = Clock::now() + std::chrono::seconds(60);

            WikiEvent ev{};

            while (g_running.load(std::memory_order_acquire)) {
                if (shard.queue.pop(ev)) {
                    edit_counts[ev.title]++;
                    ++total;

                    if (ev.timestamp > 0) {
                        if (auto alert = storm.update(ev, ev.timestamp)) {
                            std::lock_guard lock{g_print_mtx};
                            std::cout
                                << "** STORM shard=" << i
                                << " \"" << alert->page << "\""
                                << " edits=" << alert->window_count
                                << " rate="  << std::fixed << std::setprecision(1)
                                << alert->window_rate   * 60.0 << "/min"
                                << " base="
                                << alert->baseline_rate * 60.0 << "/min\n";
                            std::cout.flush();
                        }
                    }

                    auto now = Clock::now();
                    if (now >= next_report) {
                        print_shard_stats(i, edit_counts, total,
                                          shard.dropped.load(std::memory_order_relaxed));
                        next_report = now + std::chrono::seconds(10);
                    }
                    if (now >= next_evict) {
                        storm.evict_idle(ev.timestamp);
                        next_evict = now + std::chrono::seconds(60);
                    }
                } else {
                    std::this_thread::yield();
                }
            }

            // Drain any items pushed before the producer set g_running=false.
            while (shard.queue.pop(ev)) {
                edit_counts[ev.title]++;
                ++total;
            }

            shard_totals[i] = total;
            print_shard_stats(i, edit_counts, total,
                              shard.dropped.load(std::memory_order_relaxed));
        });
    }

    for (auto& c : consumers) c.join();
    producer.join();

    // Aggregate across all shards.
    uint64_t total_all = 0, dropped_all = 0;
    for (int i = 0; i < kNumShards; ++i) {
        total_all   += shard_totals[i];
        dropped_all += g_shards[i].dropped.load(std::memory_order_relaxed);
    }
    {
        std::lock_guard lock{g_print_mtx};
        const double drop_pct = (total_all + dropped_all) > 0
            ? 100.0 * static_cast<double>(dropped_all) /
              static_cast<double>(total_all + dropped_all)
            : 0.0;
        std::cout << "\n=== Total: events=" << total_all
                  << " dropped=" << dropped_all
                  << " drop%=" << std::fixed << std::setprecision(2) << drop_pct
                  << " ===\n";
    }

    curl_global_cleanup();
}
