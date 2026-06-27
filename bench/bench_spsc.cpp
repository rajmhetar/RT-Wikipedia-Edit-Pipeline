// Stage 4 benchmark: naive seq_cst SPSC vs optimised acquire/release version.
// Build: cmake --build build --target bench_spsc
// Run:   .\build\bin\bench_spsc.exe

#include "spsc_ring_buffer.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
static void pin_to_core(int core) {
    SetThreadAffinityMask(GetCurrentThread(), DWORD_PTR(1) << core);
}
#else
// Linux: replace with pthread_setaffinity_np / sched_setaffinity.
static void pin_to_core(int) {}
#endif

// ---------------------------------------------------------------------------
// Naive baseline: seq_cst, head_ and tail_ share one cache line, no caching.
// ---------------------------------------------------------------------------
template <typename T, std::size_t N>
class NaiveSPSC {
    static_assert(N > 0 && (N & (N - 1)) == 0);
    static constexpr std::size_t kMask = N - 1;
public:
    bool push(const T& item) noexcept {
        const std::size_t h = head_.load();           // seq_cst
        if (h - tail_.load() == N) return false;      // seq_cst
        slots_[h & kMask] = item;
        head_.store(h + 1);                            // seq_cst
        return true;
    }
    bool pop(T& out) noexcept {
        const std::size_t t = tail_.load();            // seq_cst
        if (head_.load() == t) return false;           // seq_cst
        out = slots_[t & kMask];
        tail_.store(t + 1);                            // seq_cst
        return true;
    }
private:
    std::atomic<std::size_t> head_{0};   // adjacent on the same cache line as tail_
    std::atomic<std::size_t> tail_{0};
    std::array<T, N>         slots_{};
};

// ---------------------------------------------------------------------------
// Benchmark harness
// ---------------------------------------------------------------------------
template <template <typename, std::size_t> class RB>
static std::uint64_t run(const char* label, int seconds) {
    using Item = int;
    constexpr std::size_t kCap = 4096;
    RB<Item, kCap> rb;

    std::atomic<bool>  stop{false};
    std::uint64_t consumed = 0;   // written only by consumer; safe to read after join

    std::thread prod([&] {
        pin_to_core(0);
        Item i = 0;
        while (!stop.load(std::memory_order_relaxed))
            rb.push(i++);   // drop on full: we measure sustainable throughput
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
    std::printf("  %-38s  %6.1f Mops/s\n", label, mops);
    std::fflush(stdout);
    return consumed;
}

int main() {
    constexpr int kSecs = 3;
    std::puts("SPSC throughput  (capacity=4096, item=int, 3 s run, threads pinned)");
    std::puts("--------------------------------------------------------------------");
    run<NaiveSPSC>      ("Naive  (seq_cst, shared cache line)", kSecs);
    run<SPSCRingBuffer> ("Opt    (acq/rel + padding + caching)", kSecs);
    std::puts("");
}
