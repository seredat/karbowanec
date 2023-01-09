// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
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
#include <list>
#include <thread>

#include <System/Dispatcher.h>
#include <System/Event.h>
#include <System/RemoteContext.h>

#include "CryptoNote.h"
#include "CryptoNoteCore/Difficulty.h"

#include <System/ContextGroup.h>

#include "Logging/LoggerRef.h"

namespace CryptoNote {

struct BlockMiningParameters {
  Block blockTemplate;
  difficulty_type difficulty;
  std::vector<BinaryArray> blobs;
};

class Miner {
public:
  Miner(System::Dispatcher& dispatcher, Logging::ILogger& logger);
  ~Miner();

  Block mine(const BlockMiningParameters& blockMiningParameters, size_t threadCount);

  //NOTE! this is blocking method
  void stop();

  void merge_hr(bool do_log = false);
  void waitHashrateUpdate();
  void waitHashrateLog();

private:
  System::Dispatcher& m_dispatcher;
  System::Event m_miningStopped;

  enum class MiningState : uint8_t { MINING_STOPPED, BLOCK_FOUND, MINING_IN_PROGRESS};
  std::atomic<MiningState> m_state;

  std::atomic<uint64_t> m_last_hr_merge_time;
  std::atomic<uint64_t> m_hashes;
  std::atomic<uint64_t> m_current_hash_rate;
  std::mutex m_last_hash_rates_lock;
  std::list<uint64_t> m_last_hash_rates;
  System::ContextGroup m_hashRateUpdateContext;
  System::ContextGroup m_hashRateLogContext;
  bool m_stopped;

  std::vector<std::unique_ptr<System::RemoteContext<void>>>  m_workers;

  Block m_block;

  std::vector<BinaryArray> m_blobs;

  std::recursive_mutex m_blobs_lock;

  Logging::LoggerRef m_logger;

  void runWorkers(BlockMiningParameters blockMiningParameters, size_t threadCount);
  void workerFunc(const Block& blockTemplate, difficulty_type difficulty, uint32_t nonceStep);
  bool setStateBlockFound();

  void setBlobs(const std::vector<BinaryArray>& blobs);

  void stopHashrateUpdate();

};

} //namespace CryptoNote
