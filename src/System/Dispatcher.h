#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <queue>
#include <boost/asio.hpp>
#include <boost/coroutine/symmetric_coroutine.hpp>

namespace System {

  using coro_t = boost::coroutines::symmetric_coroutine<void>;

  struct NativeContextGroup;

  struct NativeContext {
    coro_t::call_type* coro{ nullptr };
    coro_t::yield_type* yield{ nullptr };
    bool interrupted{ false };
    bool inExecutionQueue{ false };
    NativeContext* next{ nullptr };
    NativeContextGroup* group{ nullptr };
    NativeContext* groupPrev{ nullptr };
    NativeContext* groupNext{ nullptr };
    std::function<void()> procedure;
    std::function<void()> interruptProcedure;
  };

  struct NativeContextGroup {
    NativeContext* firstContext{ nullptr };
    NativeContext* lastContext{ nullptr };
    NativeContext* firstWaiter{ nullptr };
    NativeContext* lastWaiter{ nullptr };
  };

  class Dispatcher {
  public:
    Dispatcher();
    Dispatcher(const Dispatcher&) = delete;
    ~Dispatcher();
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

    // API retained for compatibility
    void addTimer(uint64_t time, NativeContext* context);
    void interruptTimer(uint64_t time, NativeContext* context);
    void* getCompletionPort() const { return nullptr; }

    NativeContext& getReusableContext();
    void pushReusableContext(NativeContext&);

    boost::asio::io_context& getIoContext() { return ioContext; }

  private:
    void spawn(std::function<void()>&& procedure);
    void contextProcedure(coro_t::yield_type& yield);

    boost::asio::io_context ioContext;
    NativeContext mainContext;
    NativeContextGroup contextGroup;
    NativeContext* currentContext;
    NativeContext* firstResumingContext;
    NativeContext* lastResumingContext;
    NativeContext* firstReusableContext;
    size_t runningContextCount;
    std::multimap<uint64_t, NativeContext*> timers;
  };

} // namespace System
