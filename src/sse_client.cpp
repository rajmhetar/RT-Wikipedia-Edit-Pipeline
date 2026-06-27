#include "sse_client.hpp"
#include "event_parser.hpp"
#include <curl/curl.h>
#include <cstdio>
#include <string_view>

SSEClient::SSEClient(std::string url, EventCallback cb)
    : url_(std::move(url)), cb_(std::move(cb)) {}

bool SSEClient::run() {
    CURL* curl = curl_easy_init();
    if (!curl) return false;

    curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: text/event-stream");
    headers = curl_slist_append(headers, "Cache-Control: no-cache");
    // Wikimedia policy requires a descriptive User-Agent (https://w.wiki/4wJS).
    headers = curl_slist_append(headers,
        "User-Agent: rt-wiki-pipeline/0.1 (portfolio project; github.com) libcurl");

    curl_easy_setopt(curl, CURLOPT_URL,            url_.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER,     headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,  write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA,      this);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE,  1L);

    // Use the Windows native certificate store when available.
    // Without this, MSYS2 curl fails SSL verification on wikimedia.org.
#ifdef CURLSSLOPT_NATIVE_CA
    curl_easy_setopt(curl, CURLOPT_SSL_OPTIONS, CURLSSLOPT_NATIVE_CA);
#endif

    CURLcode res = curl_easy_perform(curl);

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    std::fprintf(stderr, "curl finished: HTTP %ld  code=%s\n",
                 http_code, curl_easy_strerror(res));
    if (!buffer_.empty())
        std::fprintf(stderr, "unprocessed buffer (%zu bytes): %.300s\n",
                     buffer_.size(), buffer_.c_str());

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return res == CURLE_OK;
}

std::size_t SSEClient::write_cb(char* ptr, std::size_t size,
                                std::size_t nmemb, void* userdata) {
    auto* self = static_cast<SSEClient*>(userdata);
    self->buffer_.append(ptr, size * nmemb);
    self->process_buffer();
    return size * nmemb;  // any other value signals an abort to libcurl
}

void SSEClient::process_buffer() {
    std::size_t pos;
    while ((pos = buffer_.find("\n\n")) != std::string::npos) {
        // Own the event block so we can erase from buffer_ freely.
        std::string block = buffer_.substr(0, pos);
        buffer_.erase(0, pos + 2);

        // Walk lines of the block looking for the first "data:" line.
        std::size_t line_start = 0;
        while (line_start < block.size()) {
            std::size_t line_end = block.find('\n', line_start);
            if (line_end == std::string::npos) line_end = block.size();

            std::string_view line{block.data() + line_start,
                                  line_end - line_start};
            // SSE allows \r\n line endings.
            if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

            if (line.starts_with("data:")) {
                std::string_view payload = line.substr(5);
                if (!payload.empty() && payload.front() == ' ')
                    payload.remove_prefix(1);

                WikiEvent ev{};
                if (!payload.empty() && parse_wiki_event(payload, ev))
                    cb_(ev);

                break;  // one data: line per event is all we need
            }

            line_start = line_end + 1;
        }
    }
}
