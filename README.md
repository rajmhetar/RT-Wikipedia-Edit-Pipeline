# RT-Wikipedia-Edit-Pipeline

A real-time stream-processing pipeline in C++20 that consumes the live
Wikipedia edit feed and flags pages that suddenly start being edited far faster
than usual — the signature of breaking news, an edit war, or vandalism in
progress.

At the core is a header-only, **lock-free single-producer/single-consumer (SPSC)
ring buffer** that hands events from the network thread to the analysis threads
without ever taking a lock.

More details can be found under the **projects** section on my website:
<https://rajmhetar.github.io/>

## How it works

```
  Wikimedia SSE  ──►  producer thread  ──►  hash(title) & (N-1)
  (libcurl)           parse JSON into                │
                      fixed-size WikiEvent           │
                                        ┌────────────┼────────────┐
                                     shard 0      shard 1  ...  shard 3
                                    SPSC queue   SPSC queue    SPSC queue
                                        │            │             │
                                    consumer 0   consumer 1    consumer 3
                                    count + storm detect
```

One producer thread owns the HTTP connection to
`stream.wikimedia.org/v2/stream/recentchange`, parses each SSE payload into a
fixed-size, trivially-copyable `WikiEvent`, and routes it to one of four shards
by hashing the page title. Each shard is an independent SPSC queue drained by
its own consumer thread, so consumers never coordinate with each other. The
producer **drops on full** rather than blocking — stalling the network thread
would stall the HTTP receive buffer and get the connection dropped by the
server.

Each consumer runs its own `EditStormDetector`, which compares a page's edit
rate in a 1-minute burst window against its baseline rate over the preceding
9 minutes and alerts when the burst is ≥3× the baseline.

## Ring buffer design

`include/spsc_ring_buffer.hpp` is a fixed-capacity, power-of-two ring buffer
with three optimizations layered on the basic structure:

1. **Acquire/release ordering** instead of sequential consistency — the
   producer's slot write is release-stored behind `head`, and the consumer's
   acquire-load of `head` is what makes it visible. No full fences needed.
2. **Cache-line padding** — `head` and `tail` live in separate 64-byte-aligned
   structs, so the consumer's writes don't invalidate the producer's line
   (false sharing).
3. **Cached opposing cursor** — each side keeps a stale copy of the other's
   cursor and only touches the other cache line when its cached value says the
   queue is full/empty.

## Results

Measured on an Intel Core i9-13900H, GCC 13.2.0 (MSYS2 UCRT64), CMake
`Release` (`-O3`), threads pinned.

**Queue throughput** (capacity 4096, 3-second window):

| Implementation | Throughput | Relative |
|---|---:|---:|
| Mutex + `std::deque` | 8.1 Mops/s | 1.0× |
| Naive SPSC (seq_cst, shared cache line) | 34.2 Mops/s | 4.2× |
| **Optimized SPSC** (acq/rel, padded, cached) | **164.0 Mops/s** | **20.2×** |

**One-way handoff latency**, in nanoseconds (capacity 2, 1M samples after 50K
warmup):

| Implementation | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| **Optimized SPSC** | **100** | **200** | **400** |
| Mutex + `std::deque` | 200 | 1400 | 5500 |

The tail is the more meaningful half: the lock-free queue's p99.9 is 4× its
median, while the mutex's is 27× its own — the mutex is usually uncontended and
fast, but its rare contended cases go through the kernel scheduler and cost
microseconds.

**End-to-end pipeline** (200,000 synthetic 182-byte events, 1,000 distinct
titles, 4096-slot queues):

| Configuration | Elapsed | Throughput | Dropped |
|---|---:|---:|---:|
| Ingest + parse only (no queue) | 1.128 s | 177 K ev/s | — |
| Full 4-shard SPSC pipeline | 1.216 s | 165 K ev/s | 0 |

This is the most informative result in the project, because it undercuts the
premise of the optimization: reading and parsing an event costs ~5,650 ns while
a queue handoff costs ~6 ns, so **the stage that produces an event is roughly
900× more expensive than the queue that carries it**, and the queue
optimization is invisible at the system level. Against a live stream measured
at 43.2 events/s, the pipeline has about three orders of magnitude more
capacity than the workload needs.

## Building

Requires CMake ≥ 3.20, a C++20 compiler, and **libcurl** installed on the host
(`brew install curl`, `apt install libcurl4-openssl-dev`, or
`vcpkg install curl`). Catch2 and nlohmann/json are fetched automatically by
CMake.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

Build `Release` for any benchmarking — an unoptimized build hides the entire
effect of the optimization.

## Running

```bash
# Live: connect to the Wikimedia stream (Ctrl+C to stop)
./build/bin/rt_wiki_pipeline

# Record the live stream to a capture file while processing it
./build/bin/rt_wiki_pipeline --record data/capture.tsv

# Replay a capture at 10x speed (--speed 0 = as fast as possible)
./build/bin/rt_wiki_pipeline --replay data/capture.tsv --speed 10
```

Each shard prints its top pages, event count, and drop rate every 10 seconds,
and storm alerts print as they fire:

```
** STORM shard=2 "2026 Something Election" edits=14 rate=14.0/min base=3.1/min
```

## Tests and benchmarks

30 Catch2 test cases covering the ring buffer, SSE parser, storm detector, and
capture/replay round-trip:

```bash
ctest --test-dir build --output-on-failure
```

Benchmarks:

```bash
./build/bin/bench_spsc      # throughput + latency vs. naive SPSC and mutex queue
./build/bin/bench_pipeline  # parse-only baseline vs. full 4-shard pipeline
```

## Layout

```
include/
  spsc_ring_buffer.hpp     lock-free SPSC queue (header-only)
  wiki_event.hpp           fixed-size POD event record
  event_parser.hpp         JSON payload -> WikiEvent
  event_source.hpp         abstract source interface
  replay_source.hpp        capture-file replay with time scaling
  edit_storm_detector.hpp  per-shard burst detection
src/
  sse_client.{hpp,cpp}     libcurl SSE client, optional recording
  main.cpp                 producer + 4 sharded consumers
tests/                     Catch2 suites
bench/                     throughput, latency, end-to-end benchmarks
```

All targets build under strict warnings (`/W4` on MSVC; `-Wall -Wextra
-Wpedantic -Wconversion -Wshadow` otherwise). The ring buffer, event struct,
parser, detector, and replay source are header-only, so tests link against them
with no library dependency.

## Write-up

A full paper covering the design rationale, the memory-ordering argument, the
measurement methodology, and what the benchmarks got wrong the first time
accompanies this repository. More details are on my website under the projects
section: <https://rajmhetar.github.io/>

---

Raj Mhetar
