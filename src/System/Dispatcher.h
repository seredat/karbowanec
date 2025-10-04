#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <queue>
#include <boost/asio.hpp>
#include <boost/fiber/all.hpp>
#include <mutex>
#include <condition_variable>

namespace System {

  struct NativeContext;
  struct NativeContextGroup {
    NativeContext* firstContext{ nullptr };
    NativeContext* lastContext{ nullptr };
    NativeContext* firstWaiter{ nullptr };
    NativeContext* lastWaiter{ nullptr };
  };

  struct NativeContext {
    boost::fibers::fiber fiber;
    bool interrupted{ false };
    bool inExecutionQueue{ false };
    NativeContext* next{ nullptr };
    NativeContextGroup* group{ nullptr };
    NativeContext* groupPrev{ nullptr };
    NativeContext* groupNext{ nullptr };
    std::function<void()> procedure;
    std::function<void()> interruptProcedure;
  };

  class Dispatcher {
  public:
    Dispatcher();
    ~Dispatcher();

    Dispatcher(const Dispatcher&) = delete;
    Dispatcher& operator=(const Dispatcher&) = delete;

    void clear();
    void dispatch();
    NativeContext* getCurrentContext() const;
    void interrupt();
    void interrupt(NativeContext* context);
    bool interrupted();
    void pushContext(NativeContext* context);
    void remoteSpawn(std::function<void()>&& procedure);
    void yield();

    // Timer support
    void addTimer(uint64_t timeMs, NativeContext* context);
    void interruptTimer(uint64_t timeMs, NativeContext* context);

    NativeContext& getReusableContext();
    void pushReusableContext(NativeContext&);

    boost::asio::io_context& getIoContext() { return ioContext; }

  private:
    void spawn(std::function<void()>&& procedure);
    void contextProcedure(NativeContext* context);

    boost::asio::io_context ioContext;
    std::mutex mtx;
    std::condition_variable cv;

    std::queue<std::function<void()>> remoteSpawningProcedures;
    NativeContext mainContext;
    NativeContextGroup contextGroup;
    NativeContext* currentContext{ nullptr };
    NativeContext* firstResumingContext{ nullptr };
    NativeContext* lastResumingContext{ nullptr };
    NativeContext* firstReusableContext{ nullptr };
    size_t runningContextCount{ 0 };

    // timers: key = expire ms, value = context
    std::multimap<uint64_t, NativeContext*> timers;
  };

} // namespace System
