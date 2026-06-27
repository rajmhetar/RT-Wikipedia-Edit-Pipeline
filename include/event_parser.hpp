#pragma once
#include "wiki_event.hpp"
#include <nlohmann/json.hpp>
#include <string_view>
#include <cstring>
#include <cstddef>

// Parses a single SSE "data:" payload (raw JSON string) into ev.
// Returns false and leaves ev unmodified on any parse error.
inline bool parse_wiki_event(std::string_view json_str, WikiEvent& ev) {
    try {
        auto j = nlohmann::json::parse(json_str);

        // Copy src into a fixed char array, truncating and null-terminating.
        auto copy = [](std::string_view src, char* dst, std::size_t n) {
            std::size_t len = src.size() < n - 1 ? src.size() : n - 1;
            std::memcpy(dst, src.data(), len);
            dst[len] = '\0';
        };

        copy(j.value("type",    std::string{}), ev.type,    WikiEvent::kTypeMax);
        copy(j.value("title",   std::string{}), ev.title,   WikiEvent::kTitleMax);
        copy(j.value("wiki",    std::string{}), ev.wiki,    WikiEvent::kWikiMax);
        copy(j.value("user",    std::string{}), ev.user,    WikiEvent::kUserMax);
        copy(j.value("comment", std::string{}), ev.comment, WikiEvent::kCommentMax);

        ev.timestamp = j.value("timestamp", int64_t{0});
        ev.is_bot    = j.value("bot",       false);

        if (j.contains("revision")) {
            ev.revision_id        = j["revision"].value("new", int64_t{0});
            ev.parent_revision_id = j["revision"].value("old", int64_t{0});
        }

        if (j.contains("length")) {
            int32_t new_len = j["length"].value("new", int32_t{0});
            int32_t old_len = j["length"].value("old", int32_t{0});
            ev.length_delta = new_len - old_len;
        }

        return true;
    } catch (const nlohmann::json::exception&) {
        return false;
    }
}
