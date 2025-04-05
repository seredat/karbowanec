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

#include "Dispatcher.h"
#include "System/ErrorMessage.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>

#include <windows.h>
#include <winsock2.h>

namespace System {

  namespace {

    const size_t STACK_SIZE = 16384;
    const size_t RESERVE_STACK_SIZE = 2097152;

  } // anonymous namespace

  Dispatcher::Dispatcher()
    : currentContext(nullptr),
    firstResumingContext(nullptr),
    lastResumingContext(nullptr),
    firstReusableContext(nullptr),
    runningContextCount(0) {
    std::string message;
    if (ConvertThreadToFiberEx(NULL, 0) == NULL) {
      message = "ConvertThreadToFiberEx failed, " + lastErrorMessage();
    }
    else {
      completionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
      if (completionPort == NULL) {
        message = "CreateIoCompletionPort failed, " + lastErrorMessage();
      }
      else {
        WSADATA wsaData;
        int wsaResult = WSAStartup(0x0202, &wsaData);
        if (wsaResult != 0) {
          message = "WSAStartup failed, " + errorMessage(wsaResult);
        }
        else {
          remoteNotificationSent = false;
          remoteSpawnOverlapped.hEvent = NULL;
          threadId = GetCurrentThreadId();

          mainContext.fiber = GetCurrentFiber();
          mainContext.interrupted = false;
          mainContext.group = &contextGroup;
          mainContext.groupPrev = nullptr;
          mainContext.groupNext = nullptr;
          mainContext.inExecutionQueue = false;
          contextGroup.firstContext = nullptr;
          contextGroup.lastContext = nullptr;
          contextGroup.firstWaiter = nullptr;
          contextGroup.lastWaiter = nullptr;
          currentContext = &mainContext;
          firstResumingContext = nullptr;
          firstReusableContext = nullptr;
          runningContextCount = 0;
          return;
        }

        CloseHandle(completionPort);
      }

      ConvertFiberToThread();
    }

    throw std::runtime_error("Dispatcher::Dispatcher, " + message);
  }

  Dispatcher::~Dispatcher() {
    for (NativeContext* context = contextGroup.firstContext; context != nullptr; context = context->groupNext) {
      interrupt(context);
    }

    yield();
    assert(timers.empty());
    assert(contextGroup.firstContext == nullptr);
    assert(contextGroup.firstWaiter == nullptr);
    assert(firstResumingContext == nullptr);
    assert(runningContextCount == 0);
    while (firstReusableContext != nullptr) {
      void* fiber = firstReusableContext->fiber;
      firstReusableContext = firstReusableContext->next;
      DeleteFiber(fiber);
    }

    WSACleanup();
    CloseHandle(completionPort);
    ConvertFiberToThread();
  }

  void Dispatcher::clear() {
    while (firstReusableContext != nullptr) {
      void* fiber = firstReusableContext->fiber;
      firstReusableContext = firstReusableContext->next;
      DeleteFiber(fiber);
    }
  }

  void Dispatcher::dispatch() {
    NativeContext* context;
    for (;;) {
      if (firstResumingContext != nullptr) {
        context = firstResumingContext;
        firstResumingContext = context->next;

        assert(context->inExecutionQueue);
        context->inExecutionQueue = false;

        break;
      }

      LARGE_INTEGER frequency;
      LARGE_INTEGER ticks;
      QueryPerformanceCounter(&ticks);
      QueryPerformanceFrequency(&frequency);
      uint64_t currentTime = ticks.QuadPart / (frequency.QuadPart / 1000);
      auto timerContextPair = timers.begin();
      auto end = timers.end();
      while (timerContextPair != end && timerContextPair->first <= currentTime) {
        pushContext(timerContextPair->second);
        timerContextPair = timers.erase(timerContextPair);
      }

      if (firstResumingContext != nullptr) {
        context = firstResumingContext;
        firstResumingContext = context->next;

        assert(context->inExecutionQueue);
        context->inExecutionQueue = false;

        break;
      }

      DWORD timeout = timers.empty() ? INFINITE : static_cast<DWORD>(std::min(timers.begin()->first - currentTime, static_cast<uint64_t>(INFINITE - 1)));
      OVERLAPPED_ENTRY entry;
      ULONG actual = 0;
      if (GetQueuedCompletionStatusEx(completionPort, &entry, 1, &actual, timeout, TRUE) == TRUE) {
        if (entry.lpOverlapped == &remoteSpawnOverlapped) {
          std::lock_guard<std::mutex> lock(dispatcherMutex);
          assert(remoteNotificationSent);
          assert(!remoteSpawningProcedures.empty());
          do {
            spawn(std::move(remoteSpawningProcedures.front()));
            remoteSpawningProcedures.pop();
          } while (!remoteSpawningProcedures.empty());

          remoteNotificationSent = false;
          continue;
        }

        context = reinterpret_cast<DispatcherContext*>(entry.lpOverlapped)->context;
        break;
      }

      DWORD lastError = GetLastError();
      if (lastError == WAIT_TIMEOUT) {
        continue;
      }

      if (lastError != WAIT_IO_COMPLETION) {
        throw std::runtime_error("Dispatcher::dispatch, GetQueuedCompletionStatusEx failed, " + errorMessage(lastError));
      }
    }

    if (context != currentContext) {
      currentContext = context;
      SwitchToFiber(context->fiber);
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
    std::lock_guard<std::mutex> lock(dispatcherMutex);
    remoteSpawningProcedures.push(std::move(procedure));
    if (!remoteNotificationSent) {
      remoteNotificationSent = true;
      if (PostQueuedCompletionStatus(completionPort, 0, 0, &remoteSpawnOverlapped) == NULL) {
        throw std::runtime_error("Dispatcher::remoteSpawn, PostQueuedCompletionStatus failed, " + lastErrorMessage());
      };
    }
  }

  void Dispatcher::spawn(std::function<void()>&& procedure) {
    NativeContext* context = &getReusableContext();
    if (contextGroup.firstContext != nullptr) {
      context->groupPrev = contextGroup.lastContext;
      assert(contextGroup.lastContext->groupNext == nullptr);
      contextGroup.lastContext->groupNext = context;
    }
    else {
      context->groupPrev = nullptr;
      contextGroup.firstContext = context;
      contextGroup.firstWaiter = nullptr;
    }

    context->interrupted = false;
    context->group = &contextGroup;
    context->groupNext = nullptr;
    context->procedure = std::move(procedure);
    contextGroup.lastContext = context;
    pushContext(context);
  }

  void Dispatcher::yield() {
    for (;;) {
      LARGE_INTEGER frequency;
      LARGE_INTEGER ticks;
      QueryPerformanceCounter(&ticks);
      QueryPerformanceFrequency(&frequency);
      uint64_t currentTime = ticks.QuadPart / (frequency.QuadPart / 1000);
      auto timerContextPair = timers.begin();
      auto end = timers.end();
      while (timerContextPair != end && timerContextPair->first <= currentTime) {
        timerContextPair->second->interruptProcedure = nullptr;
        pushContext(timerContextPair->second);
        timerContextPair = timers.erase(timerContextPair);
      }

      OVERLAPPED_ENTRY entries[16];
      ULONG actual = 0;
      if (GetQueuedCompletionStatusEx(completionPort, entries, 16, &actual, 0, TRUE) == TRUE) {
        assert(actual > 0);
        for (ULONG i = 0; i < actual; ++i) {
          if (entries[i].lpOverlapped == &remoteSpawnOverlapped) {
            std::lock_guard<std::mutex> lock(dispatcherMutex);
            assert(remoteNotificationSent);
            assert(!remoteSpawningProcedures.empty());
            do {
              spawn(std::move(remoteSpawningProcedures.front()));
              remoteSpawningProcedures.pop();
            } while (!remoteSpawningProcedures.empty());

            remoteNotificationSent = false;
            continue;
          }

          NativeContext* context = reinterpret_cast<DispatcherContext*>(entries[i].lpOverlapped)->context;
          context->interruptProcedure = nullptr;
          pushContext(context);
        }
      }
      else {
        DWORD lastError = GetLastError();
        if (lastError == WAIT_TIMEOUT) {
          break;
        }
        else if (lastError != WAIT_IO_COMPLETION) {
          throw std::runtime_error("Dispatcher::yield, GetQueuedCompletionStatusEx failed, " + errorMessage(lastError));
        }
      }
    }

    if (firstResumingContext != nullptr) {
      pushContext(currentContext);
      dispatch();
    }
  }

  void Dispatcher::addTimer(uint64_t time, NativeContext* context) {
    std::lock_guard<std::mutex> lock(timersMutex);
    timers.insert(std::make_pair(time, context));
  }

  HANDLE Dispatcher::getCompletionPort() const {
    return completionPort;
  }

  NativeContext& Dispatcher::getReusableContext() {
    if (firstReusableContext == nullptr) {
      void* fiber = CreateFiberEx(STACK_SIZE, RESERVE_STACK_SIZE, 0, contextProcedureStatic, this);
      if (fiber == NULL) {
        throw std::runtime_error("Dispatcher::getReusableContext, CreateFiberEx failed, " + lastErrorMessage());
      }

      SwitchToFiber(fiber);
      assert(firstReusableContext != nullptr);
      firstReusableContext->fiber = fiber;
    }

    NativeContext* context = firstReusableContext;
    firstReusableContext = context->next;
    return *context;
  }

  void Dispatcher::pushReusableContext(NativeContext& context) {
    context.next = firstReusableContext;
    firstReusableContext = &context;
    --runningContextCount;
  }

  void Dispatcher::interruptTimer(uint64_t time, NativeContext* context) {
    if (context->inExecutionQueue) {
      return;
    }

    auto range = timers.equal_range(time);
    for (auto it = range.first;; ++it) {
      assert(it != range.second);
      if (it->second == context) {
        pushContext(context);
        timers.erase(it);
        break;
      }
    }
  }

  void Dispatcher::contextProcedure() {
    assert(firstReusableContext == nullptr);
    NativeContext context;
    context.interrupted = false;
    context.next = nullptr;
    context.inExecutionQueue = false;
    firstReusableContext = &context;
    SwitchToFiber(currentContext->fiber);
    for (;;) {
      ++runningContextCount;
      try {
        context.procedure();
      }
      catch (...) {
      }

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

      dispatch();
    }
  }

  void Dispatcher::contextProcedureStatic(void* context) {
    static_cast<Dispatcher*>(context)->contextProcedure();
  }

} // namespace System