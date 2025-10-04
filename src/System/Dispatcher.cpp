#include "Dispatcher.h"
#include <cassert>
#include <chrono>

namespace System {

  Dispatcher::Dispatcher() {
    // Ensure mainContext is the current context
    mainContext.interrupted = false;
    mainContext.inExecutionQueue = false;
    mainContext.group = &contextGroup;
    mainContext.groupPrev = nullptr;
    mainContext.groupNext = nullptr;
    mainContext.procedure = nullptr;
    mainContext.interruptProcedure = nullptr;

    contextGroup.firstContext = nullptr;
    contextGroup.lastContext = nullptr;
    contextGroup.firstWaiter = nullptr;
    contextGroup.lastWaiter = nullptr;

    currentContext = &mainContext;
  }

  Dispatcher::~Dispatcher() {
    // interrupt all contexts, yield until cleared
    std::unique_lock<std::mutex> lock(mtx);
    for (NativeContext* ctx = contextGroup.firstContext; ctx != nullptr; ctx = ctx->groupNext) {
      ctx->interrupted = true;
      if (ctx->interruptProcedure) ctx->interruptProcedure();
    }
    lock.unlock();

    // let remaining things finish
    dispatch();

    // cleanup fibers/contexts
    if (firstReusableContext) {
      // detach/Delete remaining fibers - use joinable checks
      // boost::fibers::fiber::joinable not always meaningful for detached fibers, but we try to join if joinable.
      NativeContext* it = firstReusableContext;
      while (it) {
        if (it->fiber.joinable()) {
          try { it->fiber.join(); }
          catch (...) {}
        }
        it = it->next;
      }
    }
  }

  void Dispatcher::clear() {
    std::unique_lock<std::mutex> lock(mtx);
    while (!remoteSpawningProcedures.empty()) remoteSpawningProcedures.pop();
    timers.clear();
    // Do not delete fibers here; reuse pool kept manageable by user code
  }

  NativeContext* Dispatcher::getCurrentContext() const {
    return currentContext;
  }

  bool Dispatcher::interrupted() {
    return currentContext && currentContext->interrupted;
  }

  void Dispatcher::interrupt() {
    interrupt(currentContext);
  }

  void Dispatcher::interrupt(NativeContext* context) {
    if (!context) return;
    if (!context->interrupted) {
      if (context->interruptProcedure) {
        // run interrupt procedure immediately (like original)
        auto fn = context->interruptProcedure;
        context->interruptProcedure = nullptr;
        fn();
      }
      else {
        context->interrupted = true;
      }
    }
  }

  void Dispatcher::pushContext(NativeContext* context) {
    assert(context != nullptr);
    std::unique_lock<std::mutex> lock(mtx);
    if (context->inExecutionQueue) return;
    context->next = nullptr;
    context->inExecutionQueue = true;
    if (firstResumingContext) {
      lastResumingContext->next = context;
    }
    else {
      firstResumingContext = context;
    }
    lastResumingContext = context;
    // wake up dispatch loop if sleeping
    cv.notify_one();
  }

  void Dispatcher::remoteSpawn(std::function<void()>&& procedure) {
    std::unique_lock<std::mutex> lock(mtx);
    remoteSpawningProcedures.push(std::move(procedure));
    cv.notify_one();
  }

  void Dispatcher::yield() {
    boost::this_fiber::yield();
  }

  NativeContext& Dispatcher::getReusableContext() {
    std::unique_lock<std::mutex> lock(mtx);
    if (firstReusableContext == nullptr) {
      // create a new reusable NativeContext (fiber will be launched on demand)
      firstReusableContext = new NativeContext();
      firstReusableContext->interrupted = false;
      firstReusableContext->inExecutionQueue = false;
      firstReusableContext->next = nullptr;
      firstReusableContext->group = nullptr;
      firstReusableContext->groupPrev = nullptr;
      firstReusableContext->groupNext = nullptr;
      firstReusableContext->procedure = nullptr;
      firstReusableContext->interruptProcedure = nullptr;
    }
    NativeContext* ctx = firstReusableContext;
    firstReusableContext = ctx->next;
    ctx->next = nullptr;
    return *ctx;
  }

  void Dispatcher::pushReusableContext(NativeContext& ctx) {
    std::unique_lock<std::mutex> lock(mtx);
    ctx.next = firstReusableContext;
    firstReusableContext = &ctx;
    if (runningContextCount > 0) --runningContextCount;
  }

  // spawn: create a new context and push into execution queue
  void Dispatcher::spawn(std::function<void()>&& procedure) {
    NativeContext& ctx = getReusableContext();
    ctx.procedure = std::move(procedure);
    ctx.interrupted = false;
    ctx.inExecutionQueue = false;
    ctx.group = &contextGroup;
    ctx.groupPrev = nullptr;
    ctx.groupNext = nullptr;

    // create a fiber that runs the contextProcedure for this NativeContext
    ctx.fiber = boost::fibers::fiber([this, &ctx]() {
      contextProcedure(&ctx);
      });
    ctx.fiber.detach();

    pushContext(&ctx);
  }

  void Dispatcher::contextProcedure(NativeContext* ctx) {
    // This function executes inside a fiber
    for (;;) {
      ++runningContextCount;
      try {
        if (ctx->procedure) ctx->procedure();
      }
      catch (...) {
        // swallow exceptions to mimic original
      }

      // When a context's procedure is done, remove it from its group and reschedule waiters if any
      if (ctx->group != nullptr) {
        if (ctx->groupPrev != nullptr) {
          // unlink from middle
          ctx->groupPrev->groupNext = ctx->groupNext;
          if (ctx->groupNext != nullptr) {
            ctx->groupNext->groupPrev = ctx->groupPrev;
          }
          else {
            ctx->group->lastContext = ctx->groupPrev;
          }
        }
        else {
          // first in group
          ctx->group->firstContext = ctx->groupNext;
          if (ctx->groupNext != nullptr) {
            ctx->groupNext->groupPrev = nullptr;
          }
          else {
            // group empty; wake up waiters
            if (ctx->group->firstWaiter != nullptr) {
              if (firstResumingContext != nullptr) {
                lastResumingContext->next = ctx->group->firstWaiter;
              }
              else {
                firstResumingContext = ctx->group->firstWaiter;
              }
              lastResumingContext = ctx->group->lastWaiter;
              ctx->group->firstWaiter = nullptr;
              ctx->group->lastWaiter = nullptr;
            }
          }
        }
        // push back to reusable pool
        pushReusableContext(*ctx);
      }
      else {
        // no group (shouldn't normally happen for spawned contexts), but still reuse
        pushReusableContext(*ctx);
      }

      // yield and let dispatch pick next
      boost::this_fiber::yield();
    }
  }

  void Dispatcher::dispatch() {
    std::unique_lock<std::mutex> lock(mtx);

    for (;;) {
      // 1) Handle remote spawns
      while (!remoteSpawningProcedures.empty()) {
        auto proc = std::move(remoteSpawningProcedures.front());
        remoteSpawningProcedures.pop();
        // spawn will create context & fiber and push context
        lock.unlock();
        spawn(std::move(proc));
        lock.lock();
      }

      // 2) run ready contexts (one by one)
      if (firstResumingContext) {
        NativeContext* ctx = firstResumingContext;
        firstResumingContext = ctx->next;
        if (!firstResumingContext) lastResumingContext = nullptr;

        currentContext = ctx;
        ctx->next = nullptr;
        ctx->inExecutionQueue = false;

        // If fiber is not started, create it; else, resume it by joining it
        if (!ctx->fiber.joinable()) {
          // fiber was detached on spawn; to run it we must create a new fiber to call the procedure.
          // Create a temporary fiber that runs contextProcedure and join it.
          // Note: to avoid complex reentrancy, we spawn a temporary fiber and join it outside lock.
          lock.unlock();
          boost::fibers::fiber f([this, ctx] { contextProcedure(ctx); });
          f.join(); // runs until it yields or completes
          lock.lock();
        }
        else {
          // The fiber exists - resume by unlocking and letting scheduler run it.
          lock.unlock();
          try {
            // join the fiber if joinable; otherwise yield
            if (ctx->fiber.joinable()) ctx->fiber.join();
            else boost::this_fiber::yield();
          }
          catch (...) {
            // swallow
          }
          lock.lock();
        }
        // after a context run, break to let caller continue (mimic original which ran one context)
        return;
      }

      // 3) process expired timers
      if (!timers.empty()) {
        uint64_t nowMs = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
          ).count()
          );

        auto it = timers.begin();
        while (it != timers.end() && it->first <= nowMs) {
          NativeContext* ctx = it->second;
          it = timers.erase(it);
          pushContext(ctx);
        }
        if (firstResumingContext) continue; // loop to run contexts
      }

      // 4) Poll io_context to run async completion handlers (non-blocking)
      lock.unlock();
      try {
        ioContext.poll();
      }
      catch (...) {
        // swallow any asio exceptions
      }
      // yield so other fibers may run
      boost::this_fiber::yield();
      lock.lock();

      // 5) nothing to run -> wait briefly
      if (!firstResumingContext && remoteSpawningProcedures.empty() && timers.empty()) {
        cv.wait_for(lock, std::chrono::milliseconds(1));
        // after wake, loop and recheck
      }
    }
  }

  void Dispatcher::addTimer(uint64_t timeMs, NativeContext* context) {
    std::unique_lock<std::mutex> lock(mtx);
    timers.emplace(timeMs, context);
    cv.notify_one();
  }

  void Dispatcher::interruptTimer(uint64_t timeMs, NativeContext* context) {
    std::unique_lock<std::mutex> lock(mtx);
    auto range = timers.equal_range(timeMs);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second == context) {
        timers.erase(it);
        break;
      }
    }
  }

} // namespace System
