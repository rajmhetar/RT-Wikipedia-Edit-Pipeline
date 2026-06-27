#pragma once
#include "wiki_event.hpp"
#include <functional>
#include <string>

// Callback type: invoked by SSEClient for every successfully parsed event.
using EventCallback = std::function<void(const WikiEvent&)>;

// Connects to an SSE endpoint via libcurl and fires cb for each parsed event.
// run() blocks until the connection closes or an error occurs.
class SSEClient {
public:
    explicit SSEClient(std::string url, EventCallback cb);

    // Blocking.  Returns false on a libcurl error.
    bool run();

private:
    std::string   url_;
    EventCallback cb_;
    std::string   buffer_;  // accumulates raw bytes between curl callbacks

    // libcurl write callback — appended chunks arrive here.
    static std::size_t write_cb(char* ptr, std::size_t size,
                                std::size_t nmemb, void* userdata);

    // Drains complete SSE events (delimited by \n\n) from buffer_.
    void process_buffer();
};
