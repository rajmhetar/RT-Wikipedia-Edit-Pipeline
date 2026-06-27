#include "sse_client.hpp"
#include "spsc_ring_buffer.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <curl/curl.h>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------------------
// Shared state between producer thread and consumer (main thread)
// ---------------------------------------------------------------------------
static constexpr std::size_t kQueueCapacity = 1024;

using Queue = SPSCRingBuffer<WikiEvent, kQueueCapacity>;

static std::atomic<bool>     g_running{true};
static std::atomic<uint64_t> g_dropped{0};

// ---------------------------------------------------------------------------
// Consumer helpers
// ---------------------------------------------------------------------------
static void print_stats(const std::unordered_map<std::string, uint64_t>& counts,
                        uint64_t total) {
    std::vector<std::pair<uint64_t, std::string>> v;
    v.reserve(counts.size());
    for (const auto& [title, n] : counts) v.emplace_back(n, title);

    const std::size_t top = std::min<std::size_t>(10, v.size());
    std::partial_sort(v.begin(), v.begin() + top, v.end(), std::greater<>{});

    const uint64_t dropped = g_dropped.load(std::memory_order_relaxed);
    const double   drop_pct = (total + dropped) > 0
                                  ? 100.0 * static_cast<double>(dropped) /
                                    static_cast<double>(total + dropped)
                                  : 0.0;

    std::cout << "\n[total=" << total
              << "  dropped=" << dropped
              << "  drop_rate=" << std::fixed << std::setprecision(2) << drop_pct << "%]\n";
    for (std::size_t i = 0; i < top; ++i)
        std::cout << "  " << std::setw(4) << v[i].first << "  " << v[i].second << '\n';
    std::cout.flush();
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
int main() {
    curl_global_init(CURL_GLOBAL_DEFAULT);

    Queue queue;

    // Producer: SSE connection on a dedicated thread, events pushed into queue.
    // If the queue is full the event is dropped and g_dropped is incremented —
    // we never block the network thread.
    std::thread producer([&] {
        SSEClient client{
            "https://stream.wikimedia.org/v2/stream/recentchange",
            [&](const WikiEvent& ev) {
                if (!queue.push(ev))
                    g_dropped.fetch_add(1, std::memory_order_relaxed);
            }
        };
        std::cerr << "Connecting to Wikipedia edit stream...\n";
        client.run();
        std::cerr << "Connection closed.\n";
        g_running.store(false, std::memory_order_release);
    });

    // Consumer: drain the queue on the main thread, count edits per page.
    // Per-page state needs no locks because only this thread touches it.
    std::unordered_map<std::string, uint64_t> edit_counts;
    uint64_t total_events = 0;

    using Clock = std::chrono::steady_clock;
    static constexpr auto kReportInterval = std::chrono::seconds(10);
    auto next_report = Clock::now() + kReportInterval;

    WikiEvent ev{};
    while (g_running.load(std::memory_order_relaxed)) {
        if (queue.pop(ev)) {
            edit_counts[ev.title]++;
            ++total_events;

            if (Clock::now() >= next_report) {
                print_stats(edit_counts, total_events);
                next_report = Clock::now() + kReportInterval;
            }
        } else {
            // Queue is empty: yield rather than spin-burning a core.
            // The live feed delivers ~30 events/s so empty stretches are common.
            std::this_thread::yield();
        }
    }

    producer.join();
    std::cout << "\n=== Final stats ===\n";
    print_stats(edit_counts, total_events);

    curl_global_cleanup();
}
