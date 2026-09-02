#pragma once
#include "event_source.hpp"
#include <atomic>
#include <fstream>
#include <optional>
#include <string>

// Connects to an SSE endpoint via libcurl and fires cb for each parsed event.
// run(cb) blocks until the connection closes, an error occurs, or stop() is called.
// Optionally records raw JSON payloads to a timestamped capture file.
class SSEClient : public EventSource {
public:
    explicit SSEClient(std::string url,
                       std::optional<std::string> record_path = std::nullopt);

    bool run(EventCallback cb) override;
    void stop() noexcept override;

private:
    std::string                  url_;
    std::optional<std::string>   record_path_;
    EventCallback                cb_;
    std::string                  buffer_;
    std::atomic<bool>            stop_{false};
    std::optional<std::ofstream> record_file_;

    static std::size_t write_cb(char* ptr, std::size_t size,
                                std::size_t nmemb, void* userdata);
    void process_buffer();
};
