#include "sse_client.hpp"
#include "event_parser.hpp"
#include <curl/curl.h>
#include <chrono>
#include <cstdio>
#include <string_view>

SSEClient::SSEClient(std::string url, std::optional<std::string> record_path)
    : url_(std::move(url)), record_path_(std::move(record_path)) {}

void SSEClient::stop() noexcept {
    stop_.store(true, std::memory_order_relaxed);
}

bool SSEClient::run(EventCallback cb) {
    cb_ = std::move(cb);
    stop_.store(false, std::memory_order_relaxed);

    if (record_path_) {
        record_file_.emplace(*record_path_);
        if (!record_file_->is_open()) {
            std::fprintf(stderr, "record: cannot open '%s' for writing\n",
                         record_path_->c_str());
            return false;
        }
        *record_file_ << "# rt-wiki-pipeline capture v1\n"
                       << "# format: <unix_timestamp_us>\\t<json_payload>\\n\n";
    }

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

#ifdef CURLSSLOPT_NATIVE_CA
    curl_easy_setopt(curl, CURLOPT_SSL_OPTIONS, CURLSSLOPT_NATIVE_CA);
#endif

    CURLcode res = curl_easy_perform(curl);

    // CURLE_WRITE_ERROR is expected when stop() was called; treat as clean exit.
    const bool intentional = (res == CURLE_WRITE_ERROR && stop_.load());
    if (res != CURLE_OK && !intentional) {
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        std::fprintf(stderr, "curl: HTTP %ld  %s\n", http_code, curl_easy_strerror(res));
        if (!buffer_.empty())
            std::fprintf(stderr, "response body: %.300s\n", buffer_.c_str());
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    record_file_.reset();
    return res == CURLE_OK || intentional;
}

std::size_t SSEClient::write_cb(char* ptr, std::size_t size,
                                std::size_t nmemb, void* userdata) {
    auto* self = static_cast<SSEClient*>(userdata);
    if (self->stop_.load(std::memory_order_relaxed))
        return 0;  // returning != size*nmemb signals abort to libcurl
    self->buffer_.append(ptr, size * nmemb);
    self->process_buffer();
    return size * nmemb;
}

void SSEClient::process_buffer() {
    std::size_t pos;
    while ((pos = buffer_.find("\n\n")) != std::string::npos) {
        std::string block = buffer_.substr(0, pos);
        buffer_.erase(0, pos + 2);

        std::size_t line_start = 0;
        while (line_start < block.size()) {
            std::size_t line_end = block.find('\n', line_start);
            if (line_end == std::string::npos) line_end = block.size();

            std::string_view line{block.data() + line_start, line_end - line_start};
            if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

            if (line.starts_with("data:")) {
                std::string_view payload = line.substr(5);
                if (!payload.empty() && payload.front() == ' ')
                    payload.remove_prefix(1);

                if (!payload.empty()) {
                    if (record_file_) {
                        using namespace std::chrono;
                        auto ts = duration_cast<microseconds>(
                            system_clock::now().time_since_epoch()).count();
                        (*record_file_) << ts << '\t' << payload << '\n';
                    }
                    WikiEvent ev{};
                    if (parse_wiki_event(payload, ev))
                        cb_(ev);
                }
                break;
            }
            line_start = line_end + 1;
        }
    }
}
