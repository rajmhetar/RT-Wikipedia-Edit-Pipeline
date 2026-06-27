#pragma once
#include <cstdint>

// Fixed-size event struct — every field is a plain-old-data type so the struct
// can live directly in a ring-buffer slot without any heap allocation or
// pointer indirection.  Strings that exceed their array bound are truncated
// (the parser null-terminates at max-1).
struct WikiEvent {
    static constexpr int kTitleMax   = 256;
    static constexpr int kUserMax    = 256;
    static constexpr int kWikiMax    =  32;
    static constexpr int kCommentMax = 256;
    static constexpr int kTypeMax    =  16;  // "edit","new","log","categorize"

    char    title[kTitleMax]     {};
    char    wiki[kWikiMax]       {};
    char    user[kUserMax]       {};
    char    comment[kCommentMax] {};
    int64_t timestamp            {};  // Unix seconds from the SSE payload
    int64_t revision_id          {};  // new revision id
    int64_t parent_revision_id   {};  // old revision id (0 for new pages)
    int32_t length_delta         {};  // new_length - old_length (bytes)
    bool    is_bot               {};
    char    type[kTypeMax]       {};
};
