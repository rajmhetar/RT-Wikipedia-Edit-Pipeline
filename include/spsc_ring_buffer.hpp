#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <type_traits>

// Single-producer single-consumer (SPSC) lock-free ring buffer.
//
// Ownership rules — MUST be respected for correctness:
//   push() is called only by the producer thread.
//   pop()  is called only by the consumer thread.
//
// Index arithmetic:
//   head_ — monotonically increasing write cursor; the producer advances it.
//   tail_ — monotonically increasing read cursor;  the consumer advances it.
//   Empty : head_ == tail_
//   Full  : head_ - tail_ == N   (unsigned wrap-around arithmetic is exact for size_t)
//   Slot  : cursor & (N-1)       (equivalent to cursor % N because N is a power of two)
//
// Memory ordering: all operations use the default seq_cst in this version.
// Stage 4 will refine these to acquire/release and explain why seq_cst is
// unnecessarily strong here.

template <typename T, std::size_t N>
class SPSCRingBuffer {
    static_assert(N > 0 && (N & (N - 1)) == 0,
                  "Capacity N must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>,
                  "T must be trivially copyable to live safely in a ring-buffer slot");

    static constexpr std::size_t kMask = N - 1;

public:
    static constexpr std::size_t capacity() noexcept { return N; }

    // Returns false (drops the item) if the buffer is full.
    // Call only from the producer thread.
    bool push(const T& item) noexcept {
        const std::size_t h = head_.load();          // read our own cursor
        if (h - tail_.load() == N) return false;     // full: consumer hasn't caught up
        slots_[h & kMask] = item;                    // write the slot
        head_.store(h + 1);                          // publish: consumer may now read slot h
        return true;
    }

    // Returns false (leaves out unchanged) if the buffer is empty.
    // Call only from the consumer thread.
    bool pop(T& out) noexcept {
        const std::size_t t = tail_.load();          // read our own cursor
        if (head_.load() == t) return false;         // empty: producer hasn't written yet
        out = slots_[t & kMask];                     // read the slot
        tail_.store(t + 1);                          // release: producer may reuse slot t
        return true;
    }

    // Snapshot of head_ - tail_; may be transiently stale in a concurrent context.
    std::size_t size_approx() const noexcept {
        return head_.load() - tail_.load();
    }

private:
    std::atomic<std::size_t> head_{0};   // written only by producer
    std::atomic<std::size_t> tail_{0};   // written only by consumer
    std::array<T, N>         slots_{};
};
