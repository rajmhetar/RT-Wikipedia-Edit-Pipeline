// Stage 9 benchmark: ring-buffer throughput and one-way latency.
// Compares NaiveSPSC (seq_cst, shared line), SPSCRingBuffer (opt), MutexQueue.
//
// Build: cmake --build build --target bench_spsc
// Run:   .\build\bin\bench_spsc.exe

#include "spsc_ring_buffer.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
static void pin_to_core(int core) {
    SetThreadAffinityMask(GetCurrentThread(), DWORD_PTR(1) << core);
}
#else
static void pin_to_core(int) {}
#endif

// ---------------------------------------------------------------------------
// Naive baseline: seq_cst atomics, head/tail share one cache line.
// ---------------------------------------------------------------------------
template <typename T, std::size_t N>
class NaiveSPSC {
    static_assert(N > 0 && (N & (N - 1)) == 0);
    static constexpr std::size_t kMask = N - 1;
public:
    bool push(const T& item) noexcept {
        const std::size_t h = head_.load();
        if (h - tail_.load() == N) return false;
        slots_[h & kMask] = item;
        head_.store(h + 1);
        return true;
    }
    bool pop(T& out) noexcept {
        const std::size_t t = tail_.load();
        if (head_.load() == t) return false;
        out = slots_[t & kMask];
        tail_.store(t + 1);
        return true;
    }
private:
    std::atomic<std::size_t> head_{0};
    std::atomic<std::size_t> tail_{0};
    std::array<T, N>         slots_{};
};

// ---------------------------------------------------------------------------
// MutexQueue: bounded std::deque protected by a single std::mutex.
// Drop-on-full so the throughput benchmark stays comparable to the SPSC types.
// ---------------------------------------------------------------------------
template <typename T, std::size_t N>
class MutexQueue {
public:
    bool push(const T& item) {
        std::lock_guard lock{mtx_};
        if (q_.size() >= N) return false;
        q_.push_back(item);
        return true;
    }
    bool pop(T& out) {
        std::lock_guard lock{mtx_};
        if (q_.empty()) return false;
        out = q_.front();
        q_.pop_front();
        return true;
    }
private:
    std::deque<T> q_;
    std::mutex    mtx_;
};

// ---------------------------------------------------------------------------
// Section 1 — Throughput
// Producer and consumer hammer the queue; producer drops on full.
// ---------------------------------------------------------------------------
template <template <typename, std::size_t> class RB>
static void run_throughput(const char* label, int seconds) {
    using Item = std::int32_t;
    constexpr std::size_t kCap = 4096;
    RB<Item, kCap> rb;

    std::atomic<bool> stop{false};
    std::uint64_t     consumed = 0;

    std::thread prod([&] {
        pin_to_core(0);
        Item i = 0;
        while (!stop.load(std::memory_order_relaxed))
            rb.push(i++);
    });

    std::thread cons([&] {
        pin_to_core(1);
        Item out{};
        while (!stop.load(std::memory_order_relaxed))
            if (rb.pop(out)) ++consumed;
    });

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop.store(true);
    prod.join();
    cons.join();

    const double mops = static_cast<double>(consumed) / 1e6 / seconds;
    std::printf("  %-44s  %6.1f Mops/s\n", label, mops);
    std::fflush(stdout);
}

// ---------------------------------------------------------------------------
// Section 2 — One-way latency
// kCap=2 keeps the queue near-empty so queuing time doesn't inflate results.
// The producer re-stamps on every failed push so push_ns is always fresh at
// the moment the item actually enters the queue.
// duration_cast<nanoseconds> ensures the result is in ns regardless of the
// system clock's internal period (QPC on Windows = 100 ns ticks by default).
// ---------------------------------------------------------------------------
struct TimedItem { std::int64_t push_ns{0}; };

static std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

template <template <typename, std::size_t> class RB>
static void run_latency(const char* label, std::size_t n_measure) {
    constexpr std::size_t kCap    = 2;      // minimal; keeps queue near-empty
    constexpr std::size_t kWarmup = 50'000;
    const     std::size_t kTotal  = n_measure + kWarmup;

    RB<TimedItem, kCap> rb;
    std::vector<std::int64_t> lats;
    lats.reserve(n_measure);

    std::thread prod([&] {
        pin_to_core(0);
        for (std::size_t i = 0; i < kTotal; ++i) {
            TimedItem item;
            // Re-stamp each retry so push_ns reflects the actual enqueue time.
            do { item.push_ns = now_ns(); } while (!rb.push(item));
        }
    });

    std::thread cons([&] {
        pin_to_core(1);
        TimedItem item;
        for (std::size_t i = 0; i < kTotal; ++i) {
            while (!rb.pop(item)) {}
            if (i >= kWarmup)
                lats.push_back(now_ns() - item.push_ns);
        }
    });

    prod.join();
    cons.join();

    std::sort(lats.begin(), lats.end());
    const auto p50  = lats[n_measure * 50  / 100];
    const auto p99  = lats[n_measure * 99  / 100];
    const auto p999 = lats[n_measure * 999 / 1000];

    std::printf("  %-44s  p50=%5lld ns   p99=%6lld ns   p99.9=%7lld ns\n",
        label,
        (long long)p50, (long long)p99, (long long)p999);
    std::fflush(stdout);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    constexpr int         kSecs      = 3;
    constexpr std::size_t kLatencyN  = 1'000'000;

    std::puts("=== Throughput  (cap=4096, item=int32, 3 s, threads pinned) ===");
    run_throughput<NaiveSPSC>     ("Naive   (seq_cst, shared cache line)",  kSecs);
    run_throughput<SPSCRingBuffer>("Opt     (acq/rel + padding + caching)", kSecs);
    run_throughput<MutexQueue>    ("Mutex   (std::mutex + std::deque)",     kSecs);
    std::puts("");

    std::puts("=== One-way latency  (cap=2, 1 M samples, threads pinned) ===");
    run_latency<SPSCRingBuffer>("Opt   (acq/rel + padding + caching)", kLatencyN);
    run_latency<MutexQueue>   ("Mutex (std::mutex + std::deque)",     kLatencyN);
    std::puts("");
}
