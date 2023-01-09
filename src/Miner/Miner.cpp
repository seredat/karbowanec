// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2019, The Karbowanec developers
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

#include "Miner.h"

#include <functional>
#include <numeric>

#include "crypto/crypto.h"
#include "crypto/random.h"
#include "CryptoNoteCore/CryptoNoteFormatUtils.h"
#include "CryptoNoteConfig.h"

#include <System/InterruptedException.h>
#include <System/Timer.h>

namespace CryptoNote {

Miner::Miner(System::Dispatcher& dispatcher, Logging::ILogger& logger) :
  m_dispatcher(dispatcher),
  m_miningStopped(dispatcher),
  m_state(MiningState::MINING_STOPPED),
  m_logger(logger, "Miner"),
  m_last_hr_merge_time(0),
  m_hashes(0),
  m_current_hash_rate(0),
  m_sleepingContext(dispatcher)
{
}

Miner::~Miner() {
  assert(m_state != MiningState::MINING_IN_PROGRESS);
}

void Miner::setBlobs(const std::vector<BinaryArray>& blobs) {
  std::cout << "set_blobs: " << blobs.size() << ENDL;
  std::lock_guard<decltype(m_blobs_lock)> lk(m_blobs_lock);
  m_blobs = blobs;
}

Block Miner::mine(const BlockMiningParameters& blockMiningParameters, size_t threadCount) {
  if (threadCount == 0) {
    throw std::runtime_error("Miner requires at least one thread");
  }

  if (m_state == MiningState::MINING_IN_PROGRESS) {
    throw std::runtime_error("Mining is already in progress");
  }

  setBlobs(blockMiningParameters.blobs);

  m_state = MiningState::MINING_IN_PROGRESS;
  m_miningStopped.clear();

  runWorkers(blockMiningParameters, threadCount);

  assert(m_state != MiningState::MINING_IN_PROGRESS);
  if (m_state == MiningState::MINING_STOPPED) {
    m_logger(Logging::DEBUGGING) << "Mining has been stopped";
    throw System::InterruptedException();
  }

  assert(m_state == MiningState::BLOCK_FOUND);
  return m_block;
}

void Miner::stop() {
  MiningState state = MiningState::MINING_IN_PROGRESS;

  if (m_state.compare_exchange_weak(state, MiningState::MINING_STOPPED)) {
    m_miningStopped.wait();
    m_miningStopped.clear();
  }

  stopHashrateUpdate();
}

void Miner::runWorkers(BlockMiningParameters blockMiningParameters, size_t threadCount) {
  assert(threadCount > 0);

  m_logger(Logging::INFO) << "Starting mining for difficulty " << blockMiningParameters.difficulty;

  try {
    blockMiningParameters.blockTemplate.nonce = Random::randomValue<uint32_t>();

    for (size_t i = 0; i < threadCount; ++i) {
      m_workers.emplace_back(std::unique_ptr<System::RemoteContext<void>> (
        new System::RemoteContext<void>(m_dispatcher, std::bind(&Miner::workerFunc, this, blockMiningParameters.blockTemplate, blockMiningParameters.difficulty, (uint32_t)threadCount)))
      );

      blockMiningParameters.blockTemplate.nonce++;
    }

    m_workers.clear();

  } catch (std::exception& e) {
    m_logger(Logging::ERROR) << "Error occured during mining: " << e.what();
    m_state = MiningState::MINING_STOPPED;
  }

  m_miningStopped.set();
}

void Miner::workerFunc(const Block& blockTemplate, difficulty_type difficulty, uint32_t nonceStep) {
  try {
    Block block = blockTemplate;
    Crypto::cn_context cryptoContext;

    while (m_state == MiningState::MINING_IN_PROGRESS) {
      Crypto::Hash hash;
      if (blockTemplate.majorVersion < CryptoNote::BLOCK_MAJOR_VERSION_5) {
        if (!get_block_longhash(cryptoContext, block, hash)) {
          //error occured
          m_logger(Logging::DEBUGGING) << "calculating long hash error occured";
          m_state = MiningState::MINING_STOPPED;
          return;
        }
      }
      else {
        BinaryArray pot;
        if (!get_signed_block_hashing_blob(blockTemplate, pot)) {
          m_state = MiningState::MINING_STOPPED;
          return;
        }

        Crypto::Hash hash_1, hash_2;
        uint32_t currentHeight = boost::get<BaseInput>(blockTemplate.baseTransaction.inputs[0]).blockIndex;
        uint32_t maxHeight = currentHeight - 1 - 10;

#define ITER 128
        for (uint32_t i = 0; i < ITER; i++) {
          cn_fast_hash(pot.data(), pot.size(), hash_1);

          for (uint8_t j = 1; j <= 8; j++) {
            uint8_t chunk[4] = {
              hash_1.data[j * 4 - 4],
              hash_1.data[j * 4 - 3],
              hash_1.data[j * 4 - 2],
              hash_1.data[j * 4 - 1]
            };

            uint32_t n = (chunk[0] << 24) |
              (chunk[1] << 16) |
              (chunk[2] << 8) |
              (chunk[3]);

            uint32_t height_j = n % maxHeight;
            std::lock_guard<decltype(m_blobs_lock)> lk(m_blobs_lock);
            BinaryArray& ba = m_blobs[height_j];
            pot.insert(std::end(pot), std::begin(ba), std::end(ba));
          }
        }

        if (!Crypto::y_slow_hash(pot.data(), pot.size(), hash_1, hash_2)) {
          m_state = MiningState::MINING_STOPPED;
          return;
        }

        hash = hash_2;
      }

      if (check_hash(hash, difficulty)) {
        m_logger(Logging::INFO) << "Found block for difficulty " << difficulty;

        if (!setStateBlockFound()) {
          m_logger(Logging::DEBUGGING) << "block is already found or mining stopped";
          return;
        }

        m_block = block;
        return;
      }

      block.nonce += nonceStep;
      ++m_hashes;
    }
  } catch (std::exception& e) {
    m_logger(Logging::ERROR) << "Miner got error: " << e.what();
    m_state = MiningState::MINING_STOPPED;
  }
}

bool Miner::setStateBlockFound() {
  auto state = m_state.load();

  for (;;) {
    switch (state) {
      case MiningState::BLOCK_FOUND:
        return false;

      case MiningState::MINING_IN_PROGRESS:
        if (m_state.compare_exchange_weak(state, MiningState::BLOCK_FOUND)) {
          return true;
        }
        break;

      case MiningState::MINING_STOPPED:
        return false;

      default:
        assert(false);
        return false;
    }
  }
}

uint64_t millisecondsSinceEpoch() {
  auto now = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
}

void Miner::merge_hr()
{
  if (m_last_hr_merge_time && m_state.load() == MiningState::MINING_IN_PROGRESS) {
    m_current_hash_rate = m_hashes * 1000 / (millisecondsSinceEpoch() - m_last_hr_merge_time + 1);
    std::lock_guard<std::mutex> lk(m_last_hash_rates_lock);
    m_last_hash_rates.push_back(m_current_hash_rate);
    if (m_last_hash_rates.size() > 19)
      m_last_hash_rates.pop_front();

    uint64_t total_hr = std::accumulate(m_last_hash_rates.begin(), m_last_hash_rates.end(), (uint64_t)0);
    float hr = static_cast<float>(total_hr) / static_cast<float>(m_last_hash_rates.size());

    m_logger(Logging::INFO, Logging::BRIGHT_WHITE) << "Hashrate: " << std::setprecision(2) << std::fixed << hr << " H/s";
  }

  m_last_hr_merge_time = millisecondsSinceEpoch();
  m_hashes = 0;
}

void Miner::waitHashrateUpdate() {
  m_stopped = false;

  while (!m_stopped) {
    m_sleepingContext.spawn([this]() {
      System::Timer timer(m_dispatcher);
    timer.sleep(std::chrono::seconds(10));
      });

    m_sleepingContext.wait();

    merge_hr();
  }
}

void Miner::stopHashrateUpdate() {
  m_stopped = true;

  m_sleepingContext.interrupt();
  m_sleepingContext.wait();
}

} //namespace CryptoNote
