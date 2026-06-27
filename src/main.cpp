#include "sse_client.hpp"
#include <curl/curl.h>
#include <iostream>
#include <ctime>

static void print_event(const WikiEvent& ev) {
    std::time_t t = static_cast<std::time_t>(ev.timestamp);
    char tbuf[32]{};
    // gmtime is fine here — single-threaded main thread only.
    std::strftime(tbuf, sizeof(tbuf), "%H:%M:%S", std::gmtime(&t));  // NOLINT

    std::cout
        << '[' << tbuf << "] "
        << ev.wiki    << " | "
        << ev.type    << " | "
        << ev.title   << " | "
        << ev.user
        << (ev.is_bot ? " [bot]" : "")
        << " \xce\x94" << ev.length_delta   // Δ in UTF-8
        << '\n';
}

int main() {
    curl_global_init(CURL_GLOBAL_DEFAULT);

    SSEClient client{
        "https://stream.wikimedia.org/v2/stream/recentchanges",
        print_event
    };

    std::cerr << "Connecting to Wikipedia edit stream...\n";
    if (!client.run())
        std::cerr << "SSE connection failed\n";

    curl_global_cleanup();
}
