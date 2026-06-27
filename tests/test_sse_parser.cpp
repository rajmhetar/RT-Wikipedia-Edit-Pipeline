#include <catch2/catch_test_macros.hpp>
#include "event_parser.hpp"
#include <string>
#include <string_view>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static constexpr const char* kEditEvent = R"({
    "type": "edit",
    "title": "Albert Einstein",
    "wiki": "enwiki",
    "user": "ExampleEditor",
    "comment": "/* Early life */ fixed typo",
    "timestamp": 1704067200,
    "bot": false,
    "revision": {"old": 100, "new": 101},
    "length":   {"old": 5000, "new": 5010}
})";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_CASE("parse valid edit event", "[parser]") {
    WikiEvent ev{};
    REQUIRE(parse_wiki_event(kEditEvent, ev));

    CHECK(std::string_view{ev.type}    == "edit");
    CHECK(std::string_view{ev.title}   == "Albert Einstein");
    CHECK(std::string_view{ev.wiki}    == "enwiki");
    CHECK(std::string_view{ev.user}    == "ExampleEditor");
    CHECK(ev.timestamp          == 1704067200);
    CHECK(ev.is_bot             == false);
    CHECK(ev.revision_id        == 101);
    CHECK(ev.parent_revision_id == 100);
    CHECK(ev.length_delta       == 10);
}

TEST_CASE("bot flag is captured", "[parser]") {
    WikiEvent ev{};
    const char* json = R"({
        "type":"edit","title":"Foo","wiki":"dewiki","user":"ClueBot NG",
        "comment":"","bot":true,"timestamp":1704067201,
        "revision":{"old":5,"new":6},"length":{"old":100,"new":80}
    })";
    REQUIRE(parse_wiki_event(json, ev));
    CHECK(ev.is_bot == true);
    CHECK(std::string_view{ev.wiki} == "dewiki");
    CHECK(ev.length_delta == -20);
}

TEST_CASE("new-page event has zero parent revision", "[parser]") {
    WikiEvent ev{};
    const char* json = R"({
        "type":"new","title":"Brand New Article","wiki":"enwiki",
        "user":"Creator","comment":"initial text","bot":false,
        "timestamp":1704067300,
        "revision":{"old":0,"new":202},"length":{"old":0,"new":4000}
    })";
    REQUIRE(parse_wiki_event(json, ev));
    CHECK(std::string_view{ev.type} == "new");
    CHECK(ev.parent_revision_id == 0);
    CHECK(ev.revision_id        == 202);
    CHECK(ev.length_delta       == 4000);
}

TEST_CASE("missing revision and length default to zero", "[parser]") {
    WikiEvent ev{};
    // log/categorize events often omit these fields
    const char* json = R"({
        "type":"log","title":"SomePage","wiki":"enwiki",
        "user":"u","comment":"","bot":false,"timestamp":42
    })";
    REQUIRE(parse_wiki_event(json, ev));
    CHECK(ev.revision_id  == 0);
    CHECK(ev.length_delta == 0);
    CHECK(ev.timestamp    == 42);
}

TEST_CASE("title longer than kTitleMax is truncated and null-terminated", "[parser]") {
    std::string long_title(300, 'X');
    std::string json =
        R"({"type":"edit","wiki":"enwiki","user":"u","comment":"","bot":false,)"
        R"("timestamp":0,"title":")" + long_title +
        R"(","revision":{"old":1,"new":2},"length":{"old":0,"new":0}})";

    WikiEvent ev{};
    REQUIRE(parse_wiki_event(json, ev));
    // Must be properly null-terminated and fit in the array.
    CHECK(ev.title[WikiEvent::kTitleMax - 1] == '\0');
    // Truncated length must equal kTitleMax - 1.
    CHECK(std::string_view{ev.title}.size() == WikiEvent::kTitleMax - 1);
}

TEST_CASE("malformed JSON returns false", "[parser]") {
    WikiEvent ev{};
    CHECK_FALSE(parse_wiki_event("{not json}", ev));
    CHECK_FALSE(parse_wiki_event("",           ev));
    CHECK_FALSE(parse_wiki_event("null",       ev));
}
