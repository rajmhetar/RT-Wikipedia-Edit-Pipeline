#pragma once
#include "wiki_event.hpp"
#include <functional>

using EventCallback = std::function<void(const WikiEvent&)>;

// Abstract event source — live SSE and file replay both implement this.
// The producer thread calls run(cb) and stays there until the source is
// exhausted or stop() is called (from the signal handler or another thread).
class EventSource {
public:
    virtual ~EventSource() = default;
    virtual bool run(EventCallback cb) = 0;
    virtual void stop() noexcept = 0;
};
