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

#include "MinerManager.h"

#include <System/EventLock.h>
#include <System/InterruptedException.h>
#include <System/Timer.h>

#include "Common/StringTools.h"
#include "CryptoNoteConfig.h"
#include "CryptoNoteCore/CryptoNoteTools.h"
#include "CryptoNoteCore/CryptoNoteFormatUtils.h"
#include "CryptoNoteCore/TransactionExtra.h"
#include "HTTP/httplib.h"
#include "Rpc/CoreRpcServerCommandsDefinitions.h"
#include "Rpc/JsonRpc.h"

using namespace CryptoNote;

#if defined(WIN32)
#undef ERROR
#endif

namespace Miner {

namespace {

MinerEvent BlockMinedEvent() {
  MinerEvent event;
  event.type = MinerEventType::BLOCK_MINED;
  return event;
}

MinerEvent BlockchainUpdatedEvent() {
  MinerEvent event;
  event.type = MinerEventType::BLOCKCHAIN_UPDATED;
  return event;
}

void adjustMergeMiningTag(Block& blockTemplate) {
  if (blockTemplate.majorVersion == BLOCK_MAJOR_VERSION_2 || blockTemplate.majorVersion >= BLOCK_MAJOR_VERSION_3) {
    CryptoNote::TransactionExtraMergeMiningTag mmTag;
    mmTag.depth = 0;
    if (!CryptoNote::get_aux_block_header_hash(blockTemplate, mmTag.merkleRoot)) {
      throw std::runtime_error("Couldn't get block header hash");
    }

    blockTemplate.parentBlock.baseTransaction.extra.clear();
    if (!CryptoNote::appendMergeMiningTagToExtra(blockTemplate.parentBlock.baseTransaction.extra, mmTag)) {
      throw std::runtime_error("Couldn't append merge mining tag");
    }
  }
}

}

MinerManager::MinerManager(System::Dispatcher& dispatcher, const CryptoNote::MiningConfig& config, Logging::ILogger& logger) :
  m_dispatcher(dispatcher),
  m_logger(logger, "MinerManager"),
  m_contextGroup(dispatcher),
  m_config(config),
  m_miner(dispatcher, logger),
  m_blockchainMonitor(dispatcher, m_config.daemonHost, m_config.daemonPort, m_config.scanPeriod, logger),
  m_eventOccurred(dispatcher),
  m_httpEvent(dispatcher),
  m_lastBlockTimestamp(0) {

  m_httpEvent.set();
}

MinerManager::~MinerManager() {
}

void MinerManager::start() {
  m_logger(Logging::DEBUGGING) << "starting";

  BlockMiningParameters params;
  for (;;) {
    m_logger(Logging::INFO) << "requesting mining parameters";

    try {
      params = requestMiningParameters(m_dispatcher, m_config.daemonHost, m_config.daemonPort, m_config.miningSpendKey, m_config.miningViewKey);
    } catch (const std::exception& e) {
      m_logger(Logging::WARNING) << "Couldn't connect to daemon: " << e.what();
      System::Timer timer(m_dispatcher);
      timer.sleep(std::chrono::seconds(m_config.scanPeriod));
      continue;
    }

    adjustBlockTemplate(params.blockTemplate);
    break;
  }

  startBlockchainMonitoring();
  startMining(params);

  eventLoop();
}

void MinerManager::eventLoop() {
  size_t blocksMined = 0;

  for (;;) {
    m_logger(Logging::DEBUGGING) << "waiting for event";
    MinerEvent event = waitEvent();

    switch (event.type) {
      case MinerEventType::BLOCK_MINED: {
        m_logger(Logging::DEBUGGING) << "got BLOCK_MINED event";
        stopBlockchainMonitoring();

        if (submitBlock(m_minedBlock, m_config.daemonHost, m_config.daemonPort)) {
          m_lastBlockTimestamp = m_minedBlock.timestamp;

          if (m_config.blocksLimit != 0 && ++blocksMined == m_config.blocksLimit) {
            m_logger(Logging::INFO) << "Miner mined requested " << m_config.blocksLimit << " blocks. Quitting";
            return;
          }
        }

        BlockMiningParameters params = requestMiningParameters(m_dispatcher, m_config.daemonHost, m_config.daemonPort, m_config.miningSpendKey, m_config.miningViewKey);
        adjustBlockTemplate(params.blockTemplate);

        startBlockchainMonitoring();
        startMining(params);
        break;
      }

      case MinerEventType::BLOCKCHAIN_UPDATED: {
        m_logger(Logging::DEBUGGING) << "got BLOCKCHAIN_UPDATED event";
        stopMining();
        stopBlockchainMonitoring();
        BlockMiningParameters params = requestMiningParameters(m_dispatcher, m_config.daemonHost, m_config.daemonPort, m_config.miningSpendKey, m_config.miningViewKey);
        adjustBlockTemplate(params.blockTemplate);

        startBlockchainMonitoring();
        startMining(params);
        break;
      }

      default:
        assert(false);
        return;
    }
  }
}

MinerEvent MinerManager::waitEvent() {
  while(m_events.empty()) {
    m_eventOccurred.wait();
    m_eventOccurred.clear();
  }

  MinerEvent event = std::move(m_events.front());
  m_events.pop();

  return event;
}

void MinerManager::pushEvent(MinerEvent&& event) {
  m_events.push(std::move(event));
  m_eventOccurred.set();
}

void MinerManager::startMining(const CryptoNote::BlockMiningParameters& params) {
  m_contextGroup.spawn([this, params] () {
    try {
      m_minedBlock = m_miner.mine(params, m_config.threadCount);
      pushEvent(BlockMinedEvent());
    } catch (System::InterruptedException&) {
    } catch (std::exception& e) {
      m_logger(Logging::ERROR) << "Miner context unexpectedly finished: " << e.what();
    }
  });

  m_contextGroup.spawn([this]() {
    m_miner.waitHashrateUpdate();
  });
  
  m_contextGroup.spawn([this]() {
    m_miner.waitHashrateLog();
  });
}

void MinerManager::stopMining() {
  m_miner.stop();
}

void MinerManager::startBlockchainMonitoring() {
  m_contextGroup.spawn([this] () {
    try {
      m_blockchainMonitor.waitBlockchainUpdate();
      pushEvent(BlockchainUpdatedEvent());
    } catch (System::InterruptedException&) {
    } catch (std::exception& e) {
      m_logger(Logging::ERROR) << "BlockchainMonitor context unexpectedly finished: " << e.what();
    }
  });
}

void MinerManager::stopBlockchainMonitoring() {
  m_blockchainMonitor.stop();
}

bool MinerManager::submitBlock(const Block& minedBlock, const std::string& daemonHost, uint16_t daemonPort) {
  try {
    httplib::Client client(daemonHost, daemonPort);

    COMMAND_RPC_SUBMITBLOCK::request request;
    request.emplace_back(Common::toHex(toBinaryArray(minedBlock)));

    COMMAND_RPC_SUBMITBLOCK::response response;

    System::EventLock lk(m_httpEvent);
    JsonRpc::invokeJsonRpcCommand(client, "submitblock", request, response);

    m_logger(Logging::INFO) << "Block has been successfully submitted. Block hash: " << Common::podToHex(get_block_hash(minedBlock));
    return true;
  } catch (std::exception& e) {
    m_logger(Logging::WARNING) << "Couldn't submit block: " << Common::podToHex(get_block_hash(minedBlock)) << ", reason: " << e.what();
    return false;
  }
}

BlockMiningParameters MinerManager::requestMiningParameters(System::Dispatcher& dispatcher, const std::string& daemonHost, uint16_t daemonPort, const std::string& miningSpendKey, const std::string& miningViewKey) {
  std::string daemon_url = "http://" + daemonHost + ":" + std::to_string(daemonPort);

  httplib::Client client(daemon_url);

  BlockMiningParameters params;

  try {

    COMMAND_RPC_GETBLOCKTEMPLATE::request request;
    request.miner_spend_key = miningSpendKey;
    request.miner_view_key = miningViewKey;
    request.reserve_size = 0;

    COMMAND_RPC_GETBLOCKTEMPLATE::response response;

    System::EventLock lk(m_httpEvent);

    JsonRpc::invokeJsonRpcCommand(client, "getblocktemplate", request, response);

    if (response.status != CORE_RPC_STATUS_OK) {
      throw std::runtime_error("Core responded with wrong status: " + response.status);
    }

    params.difficulty = response.difficulty;

    if(!fromBinaryArray(params.blockTemplate, Common::fromHex(response.blocktemplate_blob))) {
      throw std::runtime_error("Couldn't deserialize block template");
    }

    m_logger(Logging::DEBUGGING) << "Requested block template with previous block hash: " << Common::podToHex(params.blockTemplate.previousBlockHash);

  } catch (std::exception& e) {
    m_logger(Logging::WARNING) << "Couldn't get block template: " << e.what();
    throw;
  }
  try {
    COMMAND_RPC_GET_HASHING_BLOBS::request request;
    COMMAND_RPC_GET_HASHING_BLOBS::response response;

    System::EventLock lk(m_httpEvent);

    const auto rsp = client.Post("/get_hashing_blobs.bin", storeToBinaryKeyValue(request), "application/octet-stream");
    if (rsp) {
      if (rsp->status == 200) {
        if (!loadFromBinaryKeyValue(response, rsp->body)) {
          throw std::runtime_error("Failed to parse binary response");
        }

        if (response.blobs.size() == 0) {
          throw std::runtime_error("No hashing blobs");
        }

        for (const auto& b : response.blobs) {
          params.blobs.emplace_back(Common::asBinaryArray(b));
        }
      }
    }
  }
  catch (std::exception& e) {
    m_logger(Logging::WARNING) << "Couldn't get hashing blobs: " << e.what();
    throw;
  }

  return params;
}


void MinerManager::adjustBlockTemplate(CryptoNote::Block& blockTemplate) const {
  adjustMergeMiningTag(blockTemplate);

  if (m_config.firstBlockTimestamp == 0) {
    //no need to fix timestamp
    return;
  }

  if (m_lastBlockTimestamp == 0) {
    blockTemplate.timestamp = m_config.firstBlockTimestamp;
  } else if (m_lastBlockTimestamp != 0 && m_config.blockTimestampInterval != 0) {
    blockTemplate.timestamp = m_lastBlockTimestamp + m_config.blockTimestampInterval;
  }
}

} //namespace Miner
