#pragma once
#include "wiki_event.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <unordered_map>

// Fired when a page's edit rate in the short window is kFactor times higher
// than its baseline rate in the preceding portion of the long window.
struct StormAlert {
    std::string  page;
    int64_t      timestamp;
    std::size_t  window_count;    // edits in kShortWindow
    double       window_rate;     // edits/sec in kShortWindow
    double       baseline_rate;   // edits/sec in the baseline period
};

// Per-shard, single-threaded edit-storm detector.
// Call update() for every event; call evict_idle() periodically to bound memory.
class EditStormDetector {
public:
    std::optional<StormAlert> update(const WikiEvent& ev, int64_t now_secs);
    void evict_idle(int64_t now_secs);
    std::size_t tracked_pages() const { return states_.size(); }
    int64_t last_alert_ts(const char* title) const {
        auto it = states_.find(title);
        return it != states_.end() ? it->second.last_alert : -1;
    }

    // Tunables — public so tests can inspect them.
    static constexpr int64_t     kLongWindow       = 600;  // baseline window: 10 min
    static constexpr int64_t     kShortWindow      = 60;   // burst window: 1 min
    static constexpr std::size_t kMinCount         = 5;    // min edits in short window
    static constexpr std::size_t kBaselineMinCount = 3;    // min edits needed to establish baseline
    static constexpr double      kFactor           = 3.0;  // burst must be ≥ 3× baseline
    static constexpr int64_t     kAlertCooldown    = 30;   // suppress repeat alerts (sec)
    static constexpr int64_t     kLRUEvictSecs     = 300;  // evict page idle for 5 min

private:
    struct PageState {
        std::deque<int64_t> window;         // timestamps in the last kLongWindow seconds
        int64_t             last_edit{0};
        int64_t             last_alert{0};  // timestamp of most recent alert (for cooldown)
    };

    std::unordered_map<std::string, PageState> states_;
};

// ---------------------------------------------------------------------------
// Implementation (header-only)
// ---------------------------------------------------------------------------

inline std::optional<StormAlert>
EditStormDetector::update(const WikiEvent& ev, int64_t now) {
    auto& st = states_[ev.title];  // inserts default-constructed PageState if new

    // Maintain the long window: add this edit, evict anything older than kLongWindow.
    st.window.push_back(now);
    while (!st.window.empty() && st.window.front() < now - kLongWindow)
        st.window.pop_front();
    st.last_edit = now;

    // Partition the window into the burst window [now-kShortWindow, now]
    // and the baseline period [now-kLongWindow, now-kShortWindow).
    // The deque is sorted ascending (events arrive in time order), so a
    // single binary search gives us the split point in O(log n).
    const int64_t short_cutoff = now - kShortWindow;
    auto short_begin = std::lower_bound(st.window.begin(), st.window.end(), short_cutoff);

    const std::size_t short_count    = static_cast<std::size_t>(st.window.end()   - short_begin);
    const std::size_t baseline_count = static_cast<std::size_t>(short_begin - st.window.begin());

    // Guard 1: not enough edits in the burst window.
    if (short_count < kMinCount) return std::nullopt;

    // Guard 2: not enough baseline history — prevents new-page false positives.
    // A page whose entire history fits in the short window has no prior rate to
    // compare against, so we don't alert.
    if (baseline_count < kBaselineMinCount) return std::nullopt;

    const double short_rate    = static_cast<double>(short_count)    / kShortWindow;
    const double baseline_secs = static_cast<double>(kLongWindow - kShortWindow);
    const double baseline_rate = static_cast<double>(baseline_count) / baseline_secs;

    if (short_rate < kFactor * baseline_rate) return std::nullopt;

    // Suppress repeated alerts for a sustained storm.
    if (now - st.last_alert < kAlertCooldown) return std::nullopt;
    st.last_alert = now;

    return StormAlert{
        .page           = ev.title,
        .timestamp      = now,
        .window_count   = short_count,
        .window_rate    = short_rate,
        .baseline_rate  = baseline_rate
    };
}

inline void EditStormDetector::evict_idle(int64_t now) {
    for (auto it = states_.begin(); it != states_.end(); ) {
        if (now - it->second.last_edit > kLRUEvictSecs)
            it = states_.erase(it);
        else
            ++it;
    }
}
