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

#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <vector>
#include <map>
#include <chrono>

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <windows.h>
#undef ERROR

namespace System {

  struct NativeContextGroup;

  struct NativeContext {
    void* fiber{ nullptr };
    bool interrupted;
    bool inExecutionQueue;
    NativeContext* next{ nullptr };
    NativeContextGroup* group;
    NativeContext* groupPrev;
    NativeContext* groupNext;
    std::function<void()> procedure;
    std::function<void()> interruptProcedure;
  };

  struct NativeContextGroup {
    NativeContext* firstContext{ nullptr };
    NativeContext* lastContext{ nullptr };
    NativeContext* firstWaiter{ nullptr };
    NativeContext* lastWaiter{ nullptr };
  };

  struct DispatcherContext : public OVERLAPPED {
    NativeContext* context;
  };

  class Dispatcher {
  public:
    Dispatcher();
    ~Dispatcher();

    void clear();
    void dispatch();
    NativeContext* getCurrentContext() const;
    void interrupt();
    void interrupt(NativeContext* context);
    bool interrupted();
    void pushContext(NativeContext* context);
    void remoteSpawn(std::function<void()>&& procedure);
    void yield();
    NativeContext& getReusableContext();
    void pushReusableContext(NativeContext& context);

    // Platform-specific
    void addTimer(uint64_t time, NativeContext* context);
    void interruptTimer(uint64_t time, NativeContext* context);
    HANDLE getCompletionPort() const;

  private:
    void spawn(std::function<void()>&& procedure);

    HANDLE completionPort;
    std::map<HANDLE, NativeContext*> timerHandles;
    std::mutex dispatcherMutex;
    bool remoteNotificationSent;
    std::queue<std::function<void()>> remoteSpawningProcedures;
    OVERLAPPED remoteSpawnOverlapped;
    uint32_t threadId;

    std::multimap<uint64_t, NativeContext*> timers;
    std::mutex timersMutex;

    // Context management variables
    NativeContext mainContext;
    NativeContextGroup contextGroup;
    NativeContext* currentContext{ nullptr };
    NativeContext* firstResumingContext{ nullptr };
    NativeContext* lastResumingContext{ nullptr };
    NativeContext* firstReusableContext{ nullptr };
    size_t runningContextCount{ 0 };

    void contextProcedure();
    static void contextProcedureStatic(void* context);
  };

} // namespace System