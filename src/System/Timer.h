// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2025, The Karbo developers

#pragma once

#include <chrono>

namespace System {

  class Dispatcher;

  class Timer {
  public:
    Timer();
    explicit Timer(Dispatcher& dispatcher);
    Timer(const Timer&) = delete;
    Timer(Timer&& other) noexcept;
    ~Timer();
    Timer& operator=(const Timer&) = delete;
    Timer& operator=(Timer&& other) noexcept;

    void sleep(std::chrono::nanoseconds duration);

  private:
    Dispatcher* dispatcher{ nullptr };
  };

} // namespace System
