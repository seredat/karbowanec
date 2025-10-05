#include "Dispatcher.h"
#include <cassert>
#include <chrono>
#include <stdexcept>

namespace System {

  Dispatcher::Dispatcher()
    : ioContext(),
    currentContext(&mainContext),
    firstResumingContext(nullptr),
    lastResumingContext(nullptr),
    firstReusableContext(nullptr),
    runningContextCount(0) {
    // Init main context
    mainContext.coro = nullptr;
    mainContext.yield = nullptr;
    mainContext.interrupted = false;
    mainContext.inExecutionQueue = false;
    mainContext.group = &contextGroup;
    mainContext.groupPrev = nullptr;
    mainContext.groupNext = nullptr;

    contextGroup.firstContext = nullptr;
    contextGroup.lastContext = nullptr;
    contextGroup.firstWaiter = nullptr;
    contextGroup.lastWaiter = nullptr;
  }

  Dispatcher::~Dispatcher() {
    // Interrupt all live contexts
    for (NativeContext* ctx = contextGroup.firstContext; ctx != nullptr; ctx = ctx->groupNext) {
      interrupt(ctx);
    }

    // Let them drain
    yield();

    assert(contextGroup.firstContext == nullptr);
    assert(firstResumingContext == nullptr);
    assert(runningContextCount == 0);

    // Delete all reusable coroutines
    while (firstReusableContext != nullptr) {
      auto* pcoro = firstReusableContext->coro;
      firstReusableContext->coro = nullptr;
      firstReusableContext->yield = nullptr;
      firstReusableContext = firstReusableContext->next;
      delete pcoro;
    }

    // Drain any leftover posted asio tasks
    for (;;) {
      std::size_t ran = ioContext.poll();
      if (ran == 0) break;
    }
  }

  void Dispatcher::clear() {
    while (firstReusableContext != nullptr) {
      auto* pcoro = firstReusableContext->coro;
      firstReusableContext->coro = nullptr;
      firstReusableContext->yield = nullptr;
      firstReusableContext = firstReusableContext->next;
      delete pcoro;
    }
    timers.clear();
  }

  static inline uint64_t now_ms() {
    return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
  }

  void Dispatcher::dispatch() {
    NativeContext* context = nullptr;

    // 1) Release expired timers → push waiting contexts
    const uint64_t now = now_ms();
    auto it = timers.begin();
    while (it != timers.end() && it->first <= now) {
      it->second->interruptProcedure = nullptr; // parity with originals
      pushContext(it->second);
      it = timers.erase(it);
    }

    // 2) If we already have something to run, pop it
    if (firstResumingContext) {
      context = firstResumingContext;
      firstResumingContext = context->next;
      if (!firstResumingContext) lastResumingContext = nullptr;

      assert(context->inExecutionQueue);
      context->inExecutionQueue = false;
    }
    else {
      // 3) Otherwise, process one round of IO
      try {
        ioContext.poll();
      }
      catch (...) {
        // swallow
      }

      if (!firstResumingContext) return;

      context = firstResumingContext;
      firstResumingContext = context->next;
      if (!firstResumingContext) lastResumingContext = nullptr;

      assert(context->inExecutionQueue);
      context->inExecutionQueue = false;
    }

    // 4) Context switch (coroutine swap)
    if (context != currentContext) {
      auto* pyield = currentContext->yield;
      currentContext = context;

      if (pyield != nullptr) {
        if (context->coro != nullptr)
          (*pyield)(*context->coro);
        else
          (*pyield)();
      }
      else if (context->coro) {
        (*context->coro)();
      }
    }
  }

  NativeContext* Dispatcher::getCurrentContext() const {
    return currentContext;
  }

  void Dispatcher::interrupt() {
    interrupt(currentContext);
  }

  void Dispatcher::interrupt(NativeContext* context) {
    assert(context != nullptr);
    if (!context->interrupted) {
      if (context->interruptProcedure) {
        context->interruptProcedure();
        context->interruptProcedure = nullptr;
      }
      else {
        context->interrupted = true;
      }
    }
  }

  bool Dispatcher::interrupted() {
    if (currentContext->interrupted) {
      currentContext->interrupted = false;
      return true;
    }
    return false;
  }

  void Dispatcher::pushContext(NativeContext* context) {
    assert(context != nullptr);

    if (context->inExecutionQueue) return;

    context->next = nullptr;
    context->inExecutionQueue = true;
    if (firstResumingContext) {
      assert(lastResumingContext->next == nullptr);
      lastResumingContext->next = context;
    }
    else {
      firstResumingContext = context;
    }
    lastResumingContext = context;
  }

  void Dispatcher::remoteSpawn(std::function<void()>&& procedure) {
    // Thread-safe: post to io_context so the spawn happens on the dispatcher thread
    auto f = std::make_shared<std::function<void()>>(std::move(procedure));
    ioContext.post([this, f]() mutable {
      this->spawn(std::move(*f));
      });
  }

  void Dispatcher::spawn(std::function<void()>&& procedure) {
    NativeContext& context = getReusableContext();

    if (contextGroup.firstContext != nullptr) {
      context.groupPrev = contextGroup.lastContext;
      assert(contextGroup.lastContext->groupNext == nullptr);
      contextGroup.lastContext->groupNext = &context;
    }
    else {
      context.groupPrev = nullptr;
      contextGroup.firstContext = &context;
      contextGroup.firstWaiter = nullptr; // parity with originals
    }

    context.interrupted = false;
    context.group = &contextGroup;
    context.groupNext = nullptr;
    context.procedure = std::move(procedure);
    contextGroup.lastContext = &context;
    pushContext(&context);
  }

  void Dispatcher::yield() {
    // Process whatever is ready
    try {
      ioContext.poll();
    }
    catch (...) {
      // swallow
    }

    if (firstResumingContext) {
      pushContext(currentContext);
      dispatch();
    }
  }

  NativeContext& Dispatcher::getReusableContext() {
    if (!firstReusableContext) {
      auto* pCoro = new coro_t::call_type([&, this](coro_t::yield_type& yield) {
        this->contextProcedure(yield);
        });
      // Prime coroutine: it will set firstReusableContext to its local NativeContext
      (*pCoro)();
      assert(firstReusableContext != nullptr);
      // Attach coroutine handle to the primed reusable context
      firstReusableContext->coro = pCoro;
    }

    NativeContext* ctx = firstReusableContext;
    firstReusableContext = ctx->next;
    return *ctx;
  }

  void Dispatcher::pushReusableContext(NativeContext& context) {
    context.next = firstReusableContext;
    firstReusableContext = &context;
    --runningContextCount;
  }

  void Dispatcher::addTimer(uint64_t time, NativeContext* context) {
    timers.insert({ time, context });
  }

  void Dispatcher::interruptTimer(uint64_t time, NativeContext* context) {
    auto range = timers.equal_range(time);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second == context) {
        pushContext(context);
        timers.erase(it);
        break;
      }
    }
  }

  void Dispatcher::contextProcedure(coro_t::yield_type& yield) {
    // Create the reusable context object that lives with this coroutine
    NativeContext context;
    context.yield = &yield;
    context.coro = nullptr; // set by getReusableContext() after priming
    context.inExecutionQueue = false;
    context.interrupted = false;
    context.next = nullptr;

    // Make this context available to getReusableContext()
    firstReusableContext = &context;

    // Return to the creator (priming step)
    yield();

    for (;;) {
      ++runningContextCount;
      try {
        context.procedure();
      }
      catch (...) {}

      // Remove from its group and possibly wake waiters (match original semantics)
      if (context.group != nullptr) {
        if (context.groupPrev != nullptr) {
          assert(context.groupPrev->groupNext == &context);
          context.groupPrev->groupNext = context.groupNext;
          if (context.groupNext != nullptr) {
            assert(context.groupNext->groupPrev == &context);
            context.groupNext->groupPrev = context.groupPrev;
          }
          else {
            assert(context.group->lastContext == &context);
            context.group->lastContext = context.groupPrev;
          }
        }
        else {
          assert(context.group->firstContext == &context);
          context.group->firstContext = context.groupNext;
          if (context.groupNext != nullptr) {
            assert(context.groupNext->groupPrev == &context);
            context.groupNext->groupPrev = nullptr;
          }
          else {
            // Was last in group; if group has waiters, queue them
            assert(context.group->lastContext == &context);
            if (context.group->firstWaiter != nullptr) {
              if (firstResumingContext != nullptr) {
                assert(lastResumingContext->next == nullptr);
                lastResumingContext->next = context.group->firstWaiter;
              }
              else {
                firstResumingContext = context.group->firstWaiter;
              }
              lastResumingContext = context.group->lastWaiter;
              context.group->firstWaiter = nullptr;
            }
          }
        }
        pushReusableContext(context);
      }

      // Hand control back to dispatcher to pick the next runnable
      dispatch();
    }
  }

} // namespace System
