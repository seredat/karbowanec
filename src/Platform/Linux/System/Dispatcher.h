// Copyright (c) 2012-2017, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2019, The Karbo developers
//
// This file is part of Karbo.
//
// Karbo is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Karbo is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Karbo.  If not, see <http://www.gnu.org/licenses/>.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <queue>
#include <stack>
#include <stdexcept>
#include <mutex>
#include <condition_variable>
#include <memory>
#ifndef __GLIBC__
#include <bits/reg.h>
#endif

namespace System {

struct NativeContextGroup;

struct NativeContext {
  void* ucontext;
  void* stackPtr{nullptr};
  bool interrupted;
  bool inExecutionQueue;
  NativeContext* next{nullptr};
  NativeContextGroup* group;
  NativeContext* groupPrev;
  NativeContext* groupNext;
  std::function<void()> procedure;
  std::function<void()> interruptProcedure;

  NativeContext() = default;
  ~NativeContext() = default;
};

struct NativeContextGroup {
  NativeContext* firstContext{nullptr};
  NativeContext* lastContext{nullptr};
  NativeContext* firstWaiter{nullptr};
  NativeContext* lastWaiter{nullptr};

  NativeContextGroup() = default;
  ~NativeContextGroup() = default;
};

struct OperationContext {
  NativeContext *context;
  bool interrupted;
  uint32_t events;

  OperationContext() = default;
  ~OperationContext() = default;
};

struct ContextPair {
  OperationContext *readContext{nullptr};
  OperationContext *writeContext{nullptr};

  ContextPair() = default;
  ~ContextPair() = default;
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

  // system-dependent
  int getEpoll() const;
  NativeContext& getReusableContext();
  void pushReusableContext(NativeContext&);
  int getTimer();
  void pushTimer(int timer);

#ifdef __x86_64__
# if __WORDSIZE == 64
  static const int SIZEOF_PTHREAD_MUTEX_T = 40;
# else
  static const int SIZEOF_PTHREAD_MUTEX_T = 32;
# endif
#elif defined(__aarch64__)
  static const int SIZEOF_PTHREAD_MUTEX_T = 48;
#else
  static const int SIZEOF_PTHREAD_MUTEX_T = 24;
#endif

private:
  void spawn(std::function<void()>&& procedure);
  int epoll;
  alignas(void*) uint8_t mutex[SIZEOF_PTHREAD_MUTEX_T];
  int remoteSpawnEvent;
  ContextPair remoteSpawnEventContext;
  std::queue<std::function<void()>> remoteSpawningProcedures;
  std::stack<int> timers;

  NativeContext mainContext;
  NativeContextGroup contextGroup;
  NativeContext* currentContext{nullptr};
  NativeContext* firstResumingContext{nullptr};
  NativeContext* lastResumingContext{nullptr};
  NativeContext* firstReusableContext{nullptr};
  size_t runningContextCount{0};

  std::mutex dispatcherMutex;
  std::condition_variable_any remoteSpawnedCv;
  std::atomic<bool> remoteSpawned;

  void contextProcedure(void* ucontext);
  static void contextProcedureStatic(void* context);
};

}