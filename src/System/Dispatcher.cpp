#include "Dispatcher.h"

#include <cassert>
#include <chrono>
#include <stdexcept>
#include <utility>

#include <boost/asio.hpp>
#include <boost/coroutine/symmetric_coroutine.hpp>

namespace System {

  using coro_t = boost::coroutines::symmetric_coroutine<void>;

  Dispatcher::Dispatcher()
    : ioContext(),
    currentContext(&mainContext),
    firstResumingContext(nullptr),
    lastResumingContext(nullptr),
    firstReusableContext(nullptr),
    runningContextCount(0) {
    // Initialize main context (replicates the structure from platform impls)
    mainContext.coro = nullptr;
    mainContext.yield = nullptr;
    mainContext.interrupted = false;
    mainContext.inExecutionQueue = false;
    mainContext.group = &contextGroup;
    mainContext.groupPrev = nullptr;
    mainContext.groupNext = nullptr;
    mainContext.next = nullptr;
    mainContext.procedure = nullptr;
    mainContext.interruptProcedure = nullptr;

    contextGroup.firstContext = nullptr;
    contextGroup.lastContext = nullptr;
    contextGroup.firstWaiter = nullptr;
    contextGroup.lastWaiter = nullptr;
  }

  Dispatcher::~Dispatcher() {
    // Interrupt all contexts that are still registered in the group
    for (NativeContext* ctx = contextGroup.firstContext; ctx != nullptr; ctx = ctx->groupNext) {
      interrupt(ctx);
    }

    // Give them a chance to run their interrupt handlers and exit
    yield();

    // Drain any remaining asio work safely (no work_guard)
    for (;;) {
      std::size_t ran = ioContext.poll();
      if (ran == 0) break;
    }

    // Invariants as per original implementations
    assert(contextGroup.firstContext == nullptr);
    assert(contextGroup.firstWaiter == nullptr);
    assert(firstResumingContext == nullptr);
    assert(runningContextCount == 0);

    // Free all reusable coroutine objects
    while (firstReusableContext != nullptr) {
      auto* pcoro = firstReusableContext->coro;
      firstReusableContext->coro = nullptr;
      firstReusableContext->yield = nullptr;
      firstReusableContext = firstReusableContext->next;
      delete pcoro;
    }
  }

  void Dispatcher::clear() {
    // Free all reusable coroutine objects
    while (firstReusableContext != nullptr) {
      auto* pcoro = firstReusableContext->coro;
      firstReusableContext->coro = nullptr;
      firstReusableContext->yield = nullptr;
      firstReusableContext = firstReusableContext->next;
      delete pcoro;
    }

    // No persistent OS timers to close (we use a multimap of deadlines)
    timers.clear();
  }

  static inline uint64_t now_ms() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
      duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
  }

  void Dispatcher::dispatch() {
    // Try to pop a ready context
    NativeContext* context = nullptr;

    // 1) If something is already queued to resume, pop it
    if (firstResumingContext != nullptr) {
      context = firstResumingContext;
      firstResumingContext = context->next;
      if (firstResumingContext == nullptr) {
        lastResumingContext = nullptr;
      }
      // Mirror original behavior: clear flags on dequeue
      context->inExecutionQueue = false;
      context->interruptProcedure = nullptr;
    }
    else {
      // 2) Wake timers that have expired
      const uint64_t now = now_ms();
      for (auto it = timers.begin(); it != timers.end();) {
        if (it->first <= now) {
          pushContext(it->second);
          it = timers.erase(it);
        }
        else {
          break; // multimap is ordered
        }
      }

      // 3) Run one asio handler (if any) – may enqueue contexts
      try {
        ioContext.poll_one();
      }
      catch (...) {
        // keep scheduler alive
      }

      // 4) Try again to pop a ready context
      if (firstResumingContext != nullptr) {
        context = firstResumingContext;
        firstResumingContext = context->next;
        if (firstResumingContext == nullptr) {
          lastResumingContext = nullptr;
        }
        context->inExecutionQueue = false;
        context->interruptProcedure = nullptr;
      }
      else {
        // Nothing to do right now; match the non-blocking behavior path
        return;
      }
    }

    // Switch to the picked context if it isn't the current one
    if (context != currentContext) {
      auto* prevYield = currentContext->yield; // current context's yield handle
      currentContext = context;

      if (prevYield != nullptr) {
        if (context->coro != nullptr) {
          // Switch into the target coroutine
          (*prevYield)(*context->coro);
        }
        else {
          // Target is main or not fully initialized: yield without target
          (*prevYield)();
        }
      }
      else {
        // From main into a coroutine the first time
        if (context->coro) {
          (*context->coro)();
        }
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
      if (context->interruptProcedure != nullptr) {
        // Run the registered cancellation/cleanup
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

    // Prevent double-enqueue
    if (context->inExecutionQueue) {
      return;
    }

    context->next = nullptr;
    context->inExecutionQueue = true;

    if (firstResumingContext != nullptr) {
      assert(lastResumingContext->next == nullptr);
      lastResumingContext->next = context;
    }
    else {
      firstResumingContext = context;
    }
    lastResumingContext = context;
  }

  void Dispatcher::remoteSpawn(std::function<void()>&& procedure) {
    // Post to asio to ensure spawn runs on the dispatcher's thread
    ioContext.post([this, p = std::move(procedure)]() mutable {
      this->spawn(std::move(p));
      });
  }

  void Dispatcher::spawn(std::function<void()>&& procedure) {
    NativeContext& context = getReusableContext();

    // Insert into the active context group
    if (contextGroup.firstContext != nullptr) {
      context.groupPrev = contextGroup.lastContext;
      assert(contextGroup.lastContext->groupNext == nullptr);
      contextGroup.lastContext->groupNext = &context;
    }
    else {
      context.groupPrev = nullptr;
      contextGroup.firstContext = &context;
      contextGroup.firstWaiter = nullptr;
    }

    context.interrupted = false;
    context.group = &contextGroup;
    context.groupNext = nullptr;
    context.procedure = std::move(procedure);
    context.inExecutionQueue = false;
    context.interruptProcedure = nullptr;

    contextGroup.lastContext = &context;
    pushContext(&context);
  }

  void Dispatcher::yield() {
    // Expire timers and process all ready asio handlers (non-blocking)
    const uint64_t now = now_ms();
    for (auto it = timers.begin(); it != timers.end();) {
      if (it->first <= now) {
        it->second->interruptProcedure = nullptr;
        pushContext(it->second);
        it = timers.erase(it);
      }
      else {
        break;
      }
    }

    try {
      ioContext.poll();
    }
    catch (...) {
      // swallow
    }

    if (firstResumingContext != nullptr) {
      // Yield the current fiber and let the scheduler pick next
      pushContext(currentContext);
      dispatch();
    }
  }

  void Dispatcher::addTimer(uint64_t timeMs, NativeContext* context) {
    timers.emplace(timeMs, context);
  }

  void Dispatcher::interruptTimer(uint64_t timeMs, NativeContext* context) {
    // If already scheduled to run, nothing to do
    if (context->inExecutionQueue) {
      return;
    }
    auto range = timers.equal_range(timeMs);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second == context) {
        // Wake it immediately and erase the timer
        pushContext(context);
        timers.erase(it);
        break;
      }
    }
  }

  NativeContext& Dispatcher::getReusableContext() {
    if (firstReusableContext == nullptr) {
      // Create a coroutine whose first action is to publish a NativeContext
      // that lives on the coroutine's own stack, then yield back here.
      auto* pCoro = new coro_t::call_type([this](coro_t::yield_type& yield) {
        this->contextProcedure(yield);
        });

      // Run until the coroutine yields after publishing its NativeContext
      (*pCoro)();

      assert(firstReusableContext != nullptr);
      // Associate the coroutine object with the published NativeContext
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

  void Dispatcher::contextProcedure(coro_t::yield_type& yield) {
    // This NativeContext lives on the coroutine's own stack; safe for its lifetime
    NativeContext context{};
    context.coro = nullptr;                 // set by getReusableContext() after first yield
    context.yield = &yield;
    context.interrupted = false;
    context.inExecutionQueue = false;
    context.next = nullptr;
    context.group = nullptr;
    context.groupPrev = nullptr;
    context.groupNext = nullptr;
    context.procedure = nullptr;
    context.interruptProcedure = nullptr;

    // Publish into the free list and yield back to creator
    assert(firstReusableContext == nullptr);
    firstReusableContext = &context;
    yield();

    for (;;) {
      ++runningContextCount;
      try {
        if (context.procedure) {
          context.procedure();
        }
      }
      catch (...) {
        // keep scheduler alive
      }

      // Remove from its context group, mirroring originals
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
            // No more contexts in this group; wake waiters if any
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

      // Continue scheduling
      dispatch();
    }
  }

} // namespace System
