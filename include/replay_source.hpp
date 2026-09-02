#pragma once
#include "event_source.hpp"
#include "event_parser.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

// Replays a capture file produced by SSEClient's --record mode.
//
// File format (one event per line, lines starting with '#' are comments):
//   <unix_timestamp_us>\t<raw_json_payload>
//
// speed > 0: replay gaps are divided by speed (2.0 = twice as fast, 10.0 = 10x, etc.)
// speed == 0 (kNoDelay): no sleeps — emit events as fast as possible (useful in tests)
class ReplaySource : public EventSource {
public:
    static constexpr double kNoDelay = 0.0;

    explicit ReplaySource(std::string path, double speed = 1.0)
        : path_(std::move(path)), speed_(speed) {}

    bool run(EventCallback cb) override {
        stop_.store(false, std::memory_order_relaxed);

        std::ifstream f(path_);
        if (!f) {
            std::fprintf(stderr, "replay: cannot open '%s'\n", path_.c_str());
            return false;
        }

        using Clock  = std::chrono::steady_clock;
        using Micros = std::chrono::microseconds;

        int64_t          first_ts = -1;
        Clock::time_point replay_start{};
        std::string       line;
        std::size_t       count = 0;

        while (!stop_.load(std::memory_order_relaxed) && std::getline(f, line)) {
            if (line.empty() || line[0] == '#') continue;

            const auto tab = line.find('\t');
            if (tab == std::string::npos) continue;

            int64_t ts = 0;
            try { ts = std::stoll(line.substr(0, tab)); }
            catch (...) { continue; }

            std::string_view payload{line.data() + tab + 1, line.size() - tab - 1};
            if (payload.empty()) continue;

            if (first_ts < 0) {
                first_ts     = ts;
                replay_start = Clock::now();
            } else if (speed_ > 0.0) {
                const int64_t elapsed_us = ts - first_ts;
                const int64_t scaled_us  = static_cast<int64_t>(
                    static_cast<double>(elapsed_us) / speed_);
                const auto target = replay_start + Micros{scaled_us};
                if (Clock::now() < target)
                    std::this_thread::sleep_until(target);
            }

            WikiEvent ev{};
            if (parse_wiki_event(payload, ev))
                cb(ev);
            ++count;
        }

        std::fprintf(stderr, "replay: %zu events replayed from '%s'\n",
                     count, path_.c_str());
        return true;
    }

    void stop() noexcept override {
        stop_.store(true, std::memory_order_relaxed);
    }

private:
    std::string       path_;
    double            speed_;
    std::atomic<bool> stop_{false};
};
