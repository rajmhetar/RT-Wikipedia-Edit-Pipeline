#pragma once
#include "wiki_event.hpp"
#include <atomic>
#include <functional>
#include <string>

using EventCallback = std::function<void(const WikiEvent&)>;

// Connects to an SSE endpoint via libcurl and fires cb for each parsed event.
// run() blocks until the connection closes, an error occurs, or stop() is called.
class SSEClient {
public:
    explicit SSEClient(std::string url, EventCallback cb);

    bool run();

    // Thread-safe. Causes run() to return on the next data chunk arrival.
    void stop() noexcept;

private:
    std::string       url_;
    EventCallback     cb_;
    std::string       buffer_;
    std::atomic<bool> stop_{false};

    static std::size_t write_cb(char* ptr, std::size_t size,
                                std::size_t nmemb, void* userdata);
    void process_buffer();
};
