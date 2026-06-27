#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <type_traits>

template <typename T, std::size_t N>
class SPSCRingBuffer {
    static_assert(N > 0 && (N & (N - 1)) == 0,
                  "Capacity N must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>,
                  "T must be trivially copyable to live safely in a ring-buffer slot");

    static constexpr std::size_t kMask = N - 1;

    // 64 bytes is the cache-line size on every mainstream x86 and ARM CPU.
    // std::hardware_destructive_interference_size is theoretically more correct
    // but GCC warns that its value can vary with -mtune flags, making it
    // unsuitable for use in a fixed struct layout.
    static constexpr std::size_t kCacheLine = 64;

    // Producer-owned hot data: the write cursor and the producer's stale copy of tail.
    // alignas places this on its own cache line so the consumer's writes to tail
    // do not invalidate this line on every push().
    struct alignas(kCacheLine) ProducerSide {
        std::atomic<std::size_t> head{0};
        std::size_t              cached_tail{0};
    };

    // Consumer-owned hot data: the read cursor and the consumer's stale copy of head.
    struct alignas(kCacheLine) ConsumerSide {
        std::atomic<std::size_t> tail{0};
        std::size_t              cached_head{0};
    };

public:
    static constexpr std::size_t capacity() noexcept { return N; }

    // Call only from the producer thread.
    bool push(const T& item) noexcept {
        // relaxed: only this thread writes p_.head; no cross-thread ordering needed here.
        const std::size_t h = p_.head.load(std::memory_order_relaxed);

        if (h - p_.cached_tail == N) {
            // Cached value says full — ask the consumer's cache line for the real tail.
            // acquire: pairs with the release in pop(), ensuring we observe all slot
            // reads the consumer completed before advancing tail.
            p_.cached_tail = c_.tail.load(std::memory_order_acquire);
            if (h - p_.cached_tail == N) return false;
        }

        slots_[h & kMask] = item;

        // release: the slot write above is sequenced-before this store, so the
        // acquire load in pop() that observes h+1 is guaranteed to see the slot write.
        p_.head.store(h + 1, std::memory_order_release);
        return true;
    }

    // Call only from the consumer thread.
    bool pop(T& out) noexcept {
        // relaxed: only this thread writes c_.tail.
        const std::size_t t = c_.tail.load(std::memory_order_relaxed);

        if (c_.cached_head == t) {
            // Cached value says empty — ask the producer's cache line for the real head.
            // acquire: pairs with the release in push(), ensuring we observe the slot
            // write the producer completed before advancing head.
            c_.cached_head = p_.head.load(std::memory_order_acquire);
            if (c_.cached_head == t) return false;
        }

        out = slots_[t & kMask];

        // release: the slot read above is sequenced-before this store, so the
        // acquire load in push() that observes t+1 is guaranteed to see the slot is free.
        c_.tail.store(t + 1, std::memory_order_release);
        return true;
    }

    // Approximate — loads two independent atomics; may be transiently stale.
    std::size_t size_approx() const noexcept {
        return p_.head.load(std::memory_order_relaxed) -
               c_.tail.load(std::memory_order_relaxed);
    }

private:
    ProducerSide     p_{};
    ConsumerSide     c_{};
    std::array<T, N> slots_{};
};
