#include <catch2/catch_test_macros.hpp>
#include "edit_storm_detector.hpp"
#include <cstring>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Build a minimal WikiEvent with only the title set.
static WikiEvent make_ev(const char* title) {
    WikiEvent ev{};
    std::size_t len = std::min(std::strlen(title),
                               static_cast<std::size_t>(WikiEvent::kTitleMax - 1));
    std::memcpy(ev.title, title, len);
    ev.title[len] = '\0';
    return ev;
}

// Push n edits at times [t_start, t_start + step, ...) and return the last result.
static std::optional<StormAlert> push_n(EditStormDetector& d, const WikiEvent& ev,
                                         int64_t t_start, int n, int64_t step = 1) {
    std::optional<StormAlert> last;
    for (int i = 0; i < n; ++i)
        last = d.update(ev, t_start + static_cast<int64_t>(i) * step);
    return last;
}

// At now=1000:
//   Long  window : [400, 1000]  (kLongWindow  = 600)
//   Short window : [940, 1000]  (kShortWindow = 60)
//   Baseline     : [400, 940)
static constexpr int64_t kNow      = 1000;
static constexpr int64_t kBaseStart = 400;  // start of long window
static constexpr int64_t kShortStart = kNow - EditStormDetector::kShortWindow;  // 940

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_CASE("storm detector: constructs and tracks zero pages", "[storm][smoke]") {
    EditStormDetector d;
    CHECK(d.tracked_pages() == 0);
}

TEST_CASE("storm detector: no alert below kMinCount in burst window", "[storm]") {
    EditStormDetector d;
    WikiEvent ev = make_ev("TestPage");

    // Establish a clear baseline: 10 edits spread across the baseline period.
    push_n(d, ev, kBaseStart, 10, 50);  // at 400, 450, 500, ..., 850

    // 3 edits via push_n + 1 explicit update = 4 total in the short window,
    // which is just below kMinCount (5).
    push_n(d, ev, kShortStart, 3, 1);  // at 940, 941, 942
    auto result = d.update(ev, kNow);  // 943 would still be in short window but at kNow=1000 it's t=1000
    CHECK_FALSE(result.has_value());
}

TEST_CASE("storm detector: no alert without baseline history", "[storm]") {
    EditStormDetector d;
    WikiEvent ev = make_ev("BrandNewPage");

    // 5 edits all within the short window — no baseline period edits at all.
    // baseline_count = 0 < kBaselineMinCount (3) → must not alert.
    auto result = push_n(d, ev, kShortStart, 5, 1);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("storm detector: no alert when burst matches baseline rate", "[storm]") {
    EditStormDetector d;
    WikiEvent ev = make_ev("ActiveTalkPage");

    // Baseline: 45 edits in 540 s → rate = 45/540 = 0.083 edits/sec.
    push_n(d, ev, kBaseStart, 45, 12);

    // Burst: 5 edits in 60 s → rate = 5/60 = 0.083 edits/sec.
    // 0.083 < kFactor (3.0) × 0.083 = 0.25 → NOT a storm.
    auto result = push_n(d, ev, kShortStart, 5, 12);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("storm detector: storm alert fires correctly", "[storm]") {
    EditStormDetector d;
    WikiEvent ev = make_ev("VandalTarget");

    // Baseline: 3 edits in 540 s → rate ≈ 0.0056 edits/sec.
    d.update(ev, kBaseStart + 0);
    d.update(ev, kBaseStart + 200);
    d.update(ev, kBaseStart + 400);

    // Burst: 5 edits in 5 s → rate = 5/60 = 0.083 edits/sec.
    // 0.083 >= 3.0 × 0.0056 = 0.017 → STORM.
    push_n(d, ev, kShortStart, 4, 1);
    auto result = d.update(ev, kShortStart + 4);

    REQUIRE(result.has_value());
    CHECK(result->page         == "VandalTarget");
    CHECK(result->window_count == 5);
    CHECK(result->window_rate  > result->baseline_rate * EditStormDetector::kFactor);
}

TEST_CASE("storm detector: alert cooldown suppresses repeat within kAlertCooldown", "[storm]") {
    EditStormDetector d;
    WikiEvent ev = make_ev("HotPage");

    // Baseline: 3 edits spread across the baseline period.
    push_n(d, ev, kBaseStart, 3, 180);  // at 400, 580, 760

    // Build up to 4 edits in the short window (below kMinCount — no alert yet).
    auto pre = push_n(d, ev, kShortStart, 4, 1);  // at 940, 941, 942, 943
    CHECK_FALSE(pre.has_value());

    // 5th edit crosses kMinCount — first alert fires here.
    auto first = d.update(ev, kShortStart + 4);   // at 944
    REQUIRE(first.has_value());

    // Burst immediately after — within the 30-second cooldown.
    push_n(d, ev, kShortStart + 5, 5, 1);         // at 945..949
    auto second = d.update(ev, kShortStart + 10); // at 950, only 6s after first alert
    CHECK_FALSE(second.has_value());

    // After cooldown expires, a new alert is allowed.  The window still holds
    // enough edits from the earlier bursts — one update past the boundary suffices.
    constexpr int64_t kFirstAlert = kShortStart + 4;                          // 944
    int64_t after = kFirstAlert + EditStormDetector::kAlertCooldown + 1;      // 975
    CAPTURE(after, d.last_alert_ts("HotPage"));
    auto third = d.update(ev, after);  // 31 s after first alert → cooldown cleared
    CHECK(third.has_value());
}

TEST_CASE("storm detector: LRU eviction removes idle pages", "[storm]") {
    EditStormDetector d;
    WikiEvent a = make_ev("ActivePage");
    WikiEvent b = make_ev("IdlePage");

    int64_t t = 0;
    d.update(a, t);
    d.update(b, t);
    CHECK(d.tracked_pages() == 2);

    // Advance time past the eviction threshold for b only.
    int64_t t2 = t + EditStormDetector::kLRUEvictSecs + 1;
    d.update(a, t2);  // keep a alive

    d.evict_idle(t2);
    CHECK(d.tracked_pages() == 1);  // b evicted, a remains
}

TEST_CASE("storm detector: old timestamps evicted from long window", "[storm]") {
    EditStormDetector d;
    WikiEvent ev = make_ev("Page");

    // Push 5 edits at t=0.  At t=700 they are outside kLongWindow (600 s) and
    // should be gone; the window must report only edits placed after t=100.
    push_n(d, ev, 0, 5, 1);
    push_n(d, ev, 650, 3, 1);  // 3 recent edits

    // At t=700, long window = [100, 700].  The 5 old edits (at t=0..4) are evicted.
    auto result = d.update(ev, 700);
    // Only 3 recent edits — none in the short window [640,700] except
    // those we just pushed.  Baseline check: no alert expected (below kMinCount).
    CHECK_FALSE(result.has_value());
    // Page is still tracked (last edit was recent).
    CHECK(d.tracked_pages() == 1);
}
