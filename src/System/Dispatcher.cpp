#include "Dispatcher.h"
#include <cassert>
#include <chrono>
#include <stdexcept>
#include <algorithm>

namespace System {

  Dispatcher::Dispatcher()
    : ioContext(),
    currentContext(&mainContext),
    firstResumingContext(nullptr),
    lastResumingContext(nullptr),
    firstReusableContext(nullptr),
    runningContextCount(0)
  {
    mainContext.coro = nullptr;
    mainContext.yield = nullptr;
    mainContext.interrupted = false;
    mainContext.group = &contextGroup;
    mainContext.groupPrev = nullptr;
    mainContext.groupNext = nullptr;
    mainContext.inExecutionQueue = false;

    contextGroup.firstContext = nullptr;
    contextGroup.lastContext = nullptr;
    contextGroup.firstWaiter = nullptr;
    contextGroup.lastWaiter = nullptr;
  }

  Dispatcher::~Dispatcher() {
    for (NativeContext* ctx = contextGroup.firstContext; ctx != nullptr; ctx = ctx->groupNext)
      interrupt(ctx);

    yield();

    assert(contextGroup.firstContext == nullptr);
    assert(firstResumingContext == nullptr);
    assert(runningContextCount == 0);

    while (firstReusableContext != nullptr) {
      auto* pcoro = firstReusableContext->coro;
      firstReusableContext->coro = nullptr;
      firstReusableContext->yield = nullptr;
      firstReusableContext = firstReusableContext->next;
      delete pcoro;
    }

    // Flush Asio tasks before destruction
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

  void Dispatcher::dispatch() {
    NativeContext* context = nullptr;

    // Timer processing
    uint64_t now = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());

    auto it = timers.begin();
    while (it != timers.end() && it->first <= now) {
      pushContext(it->second);
      it = timers.erase(it);
    }

    // Pick context if available
    if (firstResumingContext) {
      context = firstResumingContext;
      firstResumingContext = context->next;
      if (!firstResumingContext)
        lastResumingContext = nullptr;
    }
    else {
      try {
        ioContext.poll();
      }
      catch (...) {
        // ignore transient errors
      }

      if (!firstResumingContext)
        return;

      context = firstResumingContext;
      firstResumingContext = context->next;
      if (!firstResumingContext)
        lastResumingContext = nullptr;
    }

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

  void Dispatcher::interrupt() { interrupt(currentContext); }

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
    context->next = nullptr;

    if (firstResumingContext) {
      lastResumingContext->next = context;
    }
    else {
      firstResumingContext = context;
    }
    lastResumingContext = context;
    context->inExecutionQueue = true;
  }

  void Dispatcher::remoteSpawn(std::function<void()>&& procedure) {
    spawn(std::move(procedure));
  }

  void Dispatcher::spawn(std::function<void()>&& procedure) {
    NativeContext& context = getReusableContext();

    if (contextGroup.firstContext != nullptr) {
      context.groupPrev = contextGroup.lastContext;
      contextGroup.lastContext->groupNext = &context;
    }
    else {
      context.groupPrev = nullptr;
      contextGroup.firstContext = &context;
    }

    context.interrupted = false;
    context.group = &contextGroup;
    context.groupNext = nullptr;
    context.procedure = std::move(procedure);
    contextGroup.lastContext = &context;

    pushContext(&context);
  }

  void Dispatcher::yield() {
    try {
      ioContext.poll();
    }
    catch (...) {
      // swallow exceptions
    }

    if (firstResumingContext) {
      pushContext(currentContext);
      dispatch();
    }
  }

  NativeContext& Dispatcher::getReusableContext() {
    if (!firstReusableContext) {
      auto* pCoro = new coro_t::call_type([&, this](coro_t::yield_type& yield) {
        contextProcedure(yield);
        });

      NativeContext* ctx = new NativeContext();
      ctx->coro = pCoro;
      ctx->yield = nullptr;
      ctx->next = nullptr;
      firstReusableContext = ctx;

      (*pCoro)();
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
    NativeContext context;
    context.yield = &yield;
    context.inExecutionQueue = false;
    context.interrupted = false;
    context.next = nullptr;
    firstReusableContext = &context;

    yield();

    for (;;) {
      ++runningContextCount;
      try {
        context.procedure();
      }
      catch (...) {}

      if (context.group != nullptr) {
        if (context.groupPrev != nullptr) {
          context.groupPrev->groupNext = context.groupNext;
          if (context.groupNext != nullptr)
            context.groupNext->groupPrev = context.groupPrev;
          else
            context.group->lastContext = context.groupPrev;
        }
        else {
          context.group->firstContext = context.groupNext;
          if (context.groupNext)
            context.groupNext->groupPrev = nullptr;
        }
        pushReusableContext(context);
      }

      dispatch();
    }
  }

} // namespace System
