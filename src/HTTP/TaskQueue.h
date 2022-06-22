// Copyright (c) 2016-2022, The Karbo developers
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

#include <thread>
#include <functional>
#include <unordered_map>

#include "HTTP/httplib.h"

class RpcTaskQueue : public httplib::TaskQueue {
public:
  RpcTaskQueue() = default;
  virtual ~RpcTaskQueue() = default;

  virtual void enqueue(std::function<void()> fn) = 0;
  virtual void shutdown() = 0;

  virtual size_t connecions_count() = 0;

  virtual void on_idle() {}
};

class RpcThreadPool : public RpcTaskQueue {
public:
  explicit RpcThreadPool(size_t n) : shutdown_(false) {
    while (n) {
      threads_.emplace_back(worker(*this));
      n--;
    }
  }

  RpcThreadPool(const RpcThreadPool&) = delete;
  ~RpcThreadPool() override = default;

  void enqueue(std::function<void()> fn) override {
    std::unique_lock<std::mutex> lock(mutex_);
    jobs_.push_back(std::move(fn));
    cond_.notify_one();
    count_++;
  }

  void shutdown() override {
    // Stop all worker threads...
    {
      std::unique_lock<std::mutex> lock(mutex_);
      shutdown_ = true;
    }

    cond_.notify_all();

    // Join...
    for (auto& t : threads_) {
      t.join();
    }
  }

  size_t connecions_count() override {
    return count_;
  }

private:
  struct worker {
    explicit worker(RpcThreadPool& pool) : pool_(pool) {}

    void operator()() {
      for (;;) {
        std::function<void()> fn;
        {
          std::unique_lock<std::mutex> lock(pool_.mutex_);

          pool_.cond_.wait(
            lock, [&] { return !pool_.jobs_.empty() || pool_.shutdown_; });

          if (pool_.shutdown_ && pool_.jobs_.empty()) { break; }

          fn = pool_.jobs_.front();
          pool_.jobs_.pop_front();

        }

        assert(true == static_cast<bool>(fn));
        fn();

        {
          std::unique_lock<std::mutex> lock(pool_.mutex_);
          pool_.count_--;
        }
      }
    }

    RpcThreadPool& pool_;
  };
  friend struct worker;

  std::vector<std::thread> threads_;
  std::list<std::function<void()>> jobs_;

  size_t count_ = 0;

  bool shutdown_;

  std::condition_variable cond_;
  std::mutex mutex_;
};

