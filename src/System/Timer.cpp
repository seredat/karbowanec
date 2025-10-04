#include "Timer.h"
#include <cassert>
#include "Dispatcher.h"
#include "InterruptedException.h"

namespace System {

  Timer::Timer() : dispatcher(nullptr) {}

  Timer::Timer(Dispatcher& disp) : dispatcher(&disp) {}

  Timer::Timer(Timer&& other) noexcept : dispatcher(other.dispatcher) {
    other.dispatcher = nullptr;
  }

  Timer::~Timer() = default;

  Timer& Timer::operator=(Timer&& other) noexcept {
    if (this != &other) {
      dispatcher = other.dispatcher;
      other.dispatcher = nullptr;
    }
    return *this;
  }

  void Timer::sleep(std::chrono::nanoseconds duration) {
    assert(dispatcher != nullptr);

    if (dispatcher->interrupted()) {
      throw InterruptedException();
    }

    // Expire time in ms
    uint64_t now = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
      ).count()
      );
    uint64_t expireTime = now + duration.count() / 1000000;

    // Get current fiber context
    auto* context = dispatcher->getCurrentContext();

    bool interrupted = false;
    context->interruptProcedure = [&]() {
      if (!interrupted) {
        dispatcher->interruptTimer(expireTime, context);
        interrupted = true;
      }
      };

    // Register timer
    dispatcher->addTimer(expireTime, context);

    // Suspend this fiber until resumed by timer/interrupt
    dispatcher->dispatch();

    context->interruptProcedure = nullptr;

    if (interrupted) {
      throw InterruptedException();
    }
  }

} // namespace System
