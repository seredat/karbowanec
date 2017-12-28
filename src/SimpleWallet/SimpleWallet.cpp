// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers, The Karbovanets developers
// Copyright (c) 2014-2016, XDN developers
// Copyright (c) 2014-2017, The Forknote developers
// Copyright (c) 2016-2018, Karbo developers
//
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without modification, are
// permitted provided that the following conditions are met:
// 
// 1. Redistributions of source code must retain the above copyright notice, this list of
//    conditions and the following disclaimer.
// 
// 2. Redistributions in binary form must reproduce the above copyright notice, this list
//    of conditions and the following disclaimer in the documentation and/or other
//    materials provided with the distribution.
// 
// 3. Neither the name of the copyright holder nor the names of its contributors may be
//    used to endorse or promote products derived from this software without specific
//    prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
// THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
// THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "SimpleWallet.h"
//#include "vld.h"

#include <ctime>
#include <fstream>
#include <future>
#include <iomanip>
#include <thread>
#include <set>
#include <sstream>
#include <locale>

#include <functional>
#include <iostream>
#include <cstring>
#include <string>
#include <map>

#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/utility/value_init.hpp>

#include "Common/CommandLine.h"
#include "Common/SignalHandler.h"
#include "Common/StringTools.h"
#include <Common/Base58.h>
#include "Common/PathTools.h"
#include "Common/Util.h"
#include "CryptoNoteCore/CryptoNoteFormatUtils.h"
#include "CryptoNoteProtocol/CryptoNoteProtocolHandler.h"
#include "NodeRpcProxy/NodeRpcProxy.h"
#include "Rpc/CoreRpcServerCommandsDefinitions.h"
#include "Rpc/HttpClient.h"

#include "Wallet/WalletRpcServer.h"
#include "WalletLegacy/WalletLegacy.h"
#include "Wallet/LegacyKeysImporter.h"
#include "WalletLegacy/WalletHelper.h"

#include "version.h"
#include "mnemonics/electrum-words.h"

#include <Logging/LoggerManager.h>

#if defined(WIN32)
#include <Windows.h>
#include <crtdbg.h>
#include <winsock2.h>
#include <windns.h>
#include <Rpc.h>
# else 
#include <arpa/nameser.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <resolv.h>
#include <netdb.h>
#endif

using namespace CryptoNote;
using namespace Logging;
using Common::JsonValue;

namespace po = boost::program_options;

#define EXTENDED_LOGS_FILE "wallet_details.log"
#undef ERROR

namespace {

const command_line::arg_descriptor<std::string> arg_wallet_file = { "wallet-file", "Use wallet <arg>", "" };
const command_line::arg_descriptor<std::string> arg_generate_new_wallet = { "generate-new-wallet", "Generate new wallet and save it to <arg>", "" };
const command_line::arg_descriptor<std::string> arg_daemon_address = { "daemon-address", "Use daemon instance at <host>:<port>", "" };
const command_line::arg_descriptor<std::string> arg_daemon_host = { "daemon-host", "Use daemon instance at host <arg> instead of localhost", "" };
const command_line::arg_descriptor<std::string> arg_password = { "password", "Wallet password", "", true };
const command_line::arg_descriptor<std::string> arg_mnemonic_seed = { "mnemonic-seed", "Specify mnemonic seed for wallet recovery/creation", "" };
const command_line::arg_descriptor<bool> arg_restore_deterministic_wallet = { "restore-deterministic-wallet", "Recover wallet using electrum-style mnemonic", false };
const command_line::arg_descriptor<bool> arg_non_deterministic = { "non-deterministic", "Creates non-deterministic (classic) view and spend keys", false };
const command_line::arg_descriptor<uint16_t> arg_daemon_port = { "daemon-port", "Use daemon instance at port <arg> instead of 32348", 0 };
const command_line::arg_descriptor<std::string> arg_log_file = {"log-file", "Set the log file location", ""};
const command_line::arg_descriptor<uint32_t> arg_log_level = { "set_log", "", INFO, true };
const command_line::arg_descriptor<bool> arg_testnet = { "testnet", "Used to deploy test nets. The daemon must be launched with --testnet flag", false };
const command_line::arg_descriptor< std::vector<std::string> > arg_command = { "command", "" };


bool parseUrlAddress(const std::string& url, std::string& address, uint16_t& port) {
  auto pos = url.find("://");
  size_t addrStart = 0;

  if (pos != std::string::npos) {
    addrStart = pos + 3;
  }

  auto addrEnd = url.find(':', addrStart);

  if (addrEnd != std::string::npos) {
    auto portEnd = url.find('/', addrEnd);
    port = Common::fromString<uint16_t>(url.substr(
      addrEnd + 1, portEnd == std::string::npos ? std::string::npos : portEnd - addrEnd - 1));
  } else {
    addrEnd = url.find('/');
    port = 80;
  }

  address = url.substr(addrStart, addrEnd - addrStart);
  return true;
}


inline std::string interpret_rpc_response(bool ok, const std::string& status) {
  std::string err;
  if (ok) {
    if (status == CORE_RPC_STATUS_BUSY) {
      err = "daemon is busy. Please try later";
    } else if (status != CORE_RPC_STATUS_OK) {
      err = status;
    }
  } else {
    err = "possible lost connection to daemon";
  }
  return err;
}

template <typename IterT, typename ValueT = typename IterT::value_type>
class ArgumentReader {
public:

  ArgumentReader(IterT begin, IterT end) :
    m_begin(begin), m_end(end), m_cur(begin) {
  }

  bool eof() const {
    return m_cur == m_end;
  }

  ValueT next() {
    if (eof()) {
      throw std::runtime_error("unexpected end of arguments");
    }

    return *m_cur++;
  }

private:

  IterT m_cur;
  IterT m_begin;
  IterT m_end;
};

struct TransferCommand {
  const CryptoNote::Currency& m_currency;
  size_t fake_outs_count;
  std::vector<CryptoNote::WalletLegacyTransfer> dsts;
  std::vector<uint8_t> extra;
  uint64_t fee;
#ifndef __ANDROID__
  std::map<std::string, std::vector<WalletLegacyTransfer>> aliases;
#endif

  TransferCommand(const CryptoNote::Currency& currency) :
    m_currency(currency), fake_outs_count(0), fee(currency.minimumFee()) {
  }

  bool parseArguments(LoggerRef& logger, const std::vector<std::string> &args) {

    ArgumentReader<std::vector<std::string>::const_iterator> ar(args.begin(), args.end());

    try {

      auto mixin_str = ar.next();

      if (!Common::fromString(mixin_str, fake_outs_count)) {
        logger(ERROR, BRIGHT_RED) << "mixin_count should be non-negative integer, got " << mixin_str;
        return false;
      }

      while (!ar.eof()) {

        auto arg = ar.next();

        if (arg.size() && arg[0] == '-') {

          const auto& value = ar.next();

          if (arg == "-p") {
            if (!createTxExtraWithPaymentId(value, extra)) {
              logger(ERROR, BRIGHT_RED) << "payment ID has invalid format: \"" << value << "\", expected 64-character string";
              return false;
            }
          } else if (arg == "-f") {
            bool ok = m_currency.parseAmount(value, fee);
            if (!ok) {
              logger(ERROR, BRIGHT_RED) << "Fee value is invalid: " << value;
              return false;
            }

            if (fee < m_currency.minimumFee()) {
              logger(ERROR, BRIGHT_RED) << "Fee value is less than minimum: " << m_currency.minimumFee();
              return false;
            }
          }
        } else {
          WalletLegacyTransfer destination;
          CryptoNote::TransactionDestinationEntry de;
#ifndef __ANDROID__		  
	  std::string aliasUrl;
#endif

          if (!m_currency.parseAccountAddressString(arg, de.addr)) {
            Crypto::Hash paymentId;
            if (CryptoNote::parsePaymentId(arg, paymentId)) {
              logger(ERROR, BRIGHT_RED) << "Invalid payment ID usage. Please, use -p <payment_id>. See help for details.";
            } else {
#ifndef __ANDROID__
			  // if string doesn't contain a dot, we won't consider it a url for now.
			  if (strchr(arg.c_str(), '.') == NULL) {
				logger(ERROR, BRIGHT_RED) << "Wrong address or alias: " << arg;
				return false;
			  }             
			  aliasUrl = arg;
#endif
            }
          }

		  auto value = ar.next();
		  bool ok = m_currency.parseAmount(value, de.amount);
		  if (!ok || 0 == de.amount) {
#if defined(WIN32)
#undef max
#undef min
#endif 
			  logger(ERROR, BRIGHT_RED) << "amount is wrong: " << arg << ' ' << value <<
				  ", expected number from 0 to " << m_currency.formatAmount(std::numeric_limits<uint64_t>::max());
			  return false;
		  }

#ifndef __ANDROID__
		  if (aliasUrl.empty()) {
#endif
			  destination.address = arg;
			  destination.amount = de.amount;
			  dsts.push_back(destination);
#ifndef __ANDROID__
		  }
		  else {
			  aliases[aliasUrl].emplace_back(WalletLegacyTransfer{ "", static_cast<int64_t>(de.amount) });
		  }
#endif
          
          if (!remote_fee_address.empty()) {
            destination.address = remote_fee_address;
            int64_t remote_node_fee = static_cast<int64_t>(de.amount * 0.0025);
            if (remote_node_fee > 10000000000000)
                remote_node_fee = 10000000000000;
            destination.amount = remote_node_fee;
            dsts.push_back(destination);
          }
          
        }
      }

	  if (dsts.empty()
#ifndef __ANDROID__
		&& aliases.empty()
#endif
){
        logger(ERROR, BRIGHT_RED) << "At least one destination address is required";
        return false;
      }
    } catch (const std::exception& e) {
      logger(ERROR, BRIGHT_RED) << e.what();
      return false;
    }

    return true;
  }
};

JsonValue buildLoggerConfiguration(Level level, const std::string& logfile) {
  JsonValue loggerConfiguration(JsonValue::OBJECT);
  loggerConfiguration.insert("globalLevel", static_cast<int64_t>(level));

  JsonValue& cfgLoggers = loggerConfiguration.insert("loggers", JsonValue::ARRAY);

  JsonValue& consoleLogger = cfgLoggers.pushBack(JsonValue::OBJECT);
  consoleLogger.insert("type", "console");
  consoleLogger.insert("level", static_cast<int64_t>(TRACE));
  consoleLogger.insert("pattern", "%D %T %L ");

  JsonValue& fileLogger = cfgLoggers.pushBack(JsonValue::OBJECT);
  fileLogger.insert("type", "file");
  fileLogger.insert("filename", logfile);
  fileLogger.insert("level", static_cast<int64_t>(TRACE));

  return loggerConfiguration;
}

std::error_code initAndLoadWallet(IWalletLegacy& wallet, std::istream& walletFile, const std::string& password) {
  WalletHelper::InitWalletResultObserver initObserver;
  std::future<std::error_code> f_initError = initObserver.initResult.get_future();

  WalletHelper::IWalletRemoveObserverGuard removeGuard(wallet, initObserver);
  wallet.initAndLoad(walletFile, password);
  auto initError = f_initError.get();

  return initError;
}

std::string tryToOpenWalletOrLoadKeysOrThrow(LoggerRef& logger, std::unique_ptr<IWalletLegacy>& wallet, const std::string& walletFile, const std::string& password) {
  std::string keys_file, walletFileName;
  WalletHelper::prepareFileNames(walletFile, keys_file, walletFileName);

  boost::system::error_code ignore;
  bool keysExists = boost::filesystem::exists(keys_file, ignore);
  bool walletExists = boost::filesystem::exists(walletFileName, ignore);
  if (!walletExists && !keysExists && boost::filesystem::exists(walletFile, ignore)) {
    boost::system::error_code renameEc;
    boost::filesystem::rename(walletFile, walletFileName, renameEc);
    if (renameEc) {
      throw std::runtime_error("failed to rename file '" + walletFile + "' to '" + walletFileName + "': " + renameEc.message());
    }

    walletExists = true;
  }

  if (walletExists) {
    logger(INFO) << "Loading wallet...";
    std::ifstream walletFile;
    walletFile.open(walletFileName, std::ios_base::binary | std::ios_base::in);
    if (walletFile.fail()) {
      throw std::runtime_error("error opening wallet file '" + walletFileName + "'");
    }

    auto initError = initAndLoadWallet(*wallet, walletFile, password);

    walletFile.close();
    if (initError) { //bad password, or legacy format
      if (keysExists) {
        std::stringstream ss;
        CryptoNote::importLegacyKeys(keys_file, password, ss);
        boost::filesystem::rename(keys_file, keys_file + ".back");
        boost::filesystem::rename(walletFileName, walletFileName + ".back");

        initError = initAndLoadWallet(*wallet, ss, password);
        if (initError) {
          throw std::runtime_error("failed to load wallet: " + initError.message());
        }

        logger(INFO) << "Storing wallet...";

        try {
          CryptoNote::WalletHelper::storeWallet(*wallet, walletFileName);
        } catch (std::exception& e) {
          logger(ERROR, BRIGHT_RED) << "Failed to store wallet: " << e.what();
          throw std::runtime_error("error saving wallet file '" + walletFileName + "'");
        }

        logger(INFO, BRIGHT_GREEN) << "Stored ok";
        return walletFileName;
      } else { // no keys, wallet error loading
        throw std::runtime_error("can't load wallet file '" + walletFileName + "', check password");
      }
    } else { //new wallet ok 
      return walletFileName;
    }
  } else if (keysExists) { //wallet not exists but keys presented
    std::stringstream ss;
    CryptoNote::importLegacyKeys(keys_file, password, ss);
    boost::filesystem::rename(keys_file, keys_file + ".back");

    WalletHelper::InitWalletResultObserver initObserver;
    std::future<std::error_code> f_initError = initObserver.initResult.get_future();

    WalletHelper::IWalletRemoveObserverGuard removeGuard(*wallet, initObserver);
    wallet->initAndLoad(ss, password);
    auto initError = f_initError.get();

    removeGuard.removeObserver();
    if (initError) {
      throw std::runtime_error("failed to load wallet: " + initError.message());
    }

    logger(INFO) << "Storing wallet...";

    try {
      CryptoNote::WalletHelper::storeWallet(*wallet, walletFileName);
    } catch(std::exception& e) {
      logger(ERROR, BRIGHT_RED) << "Failed to store wallet: " << e.what();
      throw std::runtime_error("error saving wallet file '" + walletFileName + "'");
    }

    logger(INFO, BRIGHT_GREEN) << "Stored ok";
    return walletFileName;
  } else { //no wallet no keys
    throw std::runtime_error("wallet file '" + walletFileName + "' is not found");
  }
}

std::string makeCenteredString(size_t width, const std::string& text) {
  if (text.size() >= width) {
    return text;
  }

  size_t offset = (width - text.size() + 1) / 2;
  return std::string(offset, ' ') + text + std::string(width - text.size() - offset, ' ');
}

const size_t TIMESTAMP_MAX_WIDTH = 19;
const size_t HASH_MAX_WIDTH = 64;
const size_t TOTAL_AMOUNT_MAX_WIDTH = 20;
const size_t FEE_MAX_WIDTH = 14;
const size_t BLOCK_MAX_WIDTH = 7;
const size_t UNLOCK_TIME_MAX_WIDTH = 11;

void printListTransfersHeader(LoggerRef& logger) {
  std::string header = makeCenteredString(TIMESTAMP_MAX_WIDTH, "timestamp (UTC)") + "  ";
  header += makeCenteredString(HASH_MAX_WIDTH, "hash") + "  ";
  header += makeCenteredString(TOTAL_AMOUNT_MAX_WIDTH, "total amount") + "  ";
  header += makeCenteredString(FEE_MAX_WIDTH, "fee") + "  ";
  header += makeCenteredString(BLOCK_MAX_WIDTH, "block") + "  ";
  header += makeCenteredString(UNLOCK_TIME_MAX_WIDTH, "unlock time");

  logger(INFO) << header;
  logger(INFO) << std::string(header.size(), '-');
}

void printListTransfersItem(LoggerRef& logger, const WalletLegacyTransaction& txInfo, IWalletLegacy& wallet, const Currency& currency) {
  std::vector<uint8_t> extraVec = Common::asBinaryArray(txInfo.extra);

  Crypto::Hash paymentId;
  std::string paymentIdStr = (getPaymentIdFromTxExtra(extraVec, paymentId) && paymentId != NULL_HASH ? Common::podToHex(paymentId) : "");

  char timeString[TIMESTAMP_MAX_WIDTH + 1];
  time_t timestamp = static_cast<time_t>(txInfo.timestamp);
  if (std::strftime(timeString, sizeof(timeString), "%Y-%m-%d %H:%M:%S", std::gmtime(&timestamp)) == 0) {
    throw std::runtime_error("time buffer is too small");
  }

  std::string rowColor = txInfo.totalAmount < 0 ? MAGENTA : GREEN;
  logger(INFO, rowColor)
    << std::setw(TIMESTAMP_MAX_WIDTH) << timeString
    << "  " << std::setw(HASH_MAX_WIDTH) << Common::podToHex(txInfo.hash)
    << "  " << std::setw(TOTAL_AMOUNT_MAX_WIDTH) << currency.formatAmount(txInfo.totalAmount)
    << "  " << std::setw(FEE_MAX_WIDTH) << currency.formatAmount(txInfo.fee)
    << "  " << std::setw(BLOCK_MAX_WIDTH) << txInfo.blockHeight
    << "  " << std::setw(UNLOCK_TIME_MAX_WIDTH) << txInfo.unlockTime;

  if (!paymentIdStr.empty()) {
    logger(INFO, rowColor) << "payment ID: " << paymentIdStr;
  }

  if (txInfo.totalAmount < 0) {
    if (txInfo.transferCount > 0) {
      logger(INFO, rowColor) << "transfers:";
      for (TransferId id = txInfo.firstTransferId; id < txInfo.firstTransferId + txInfo.transferCount; ++id) {
        WalletLegacyTransfer tr;
        wallet.getTransfer(id, tr);
        logger(INFO, rowColor) << tr.address << "  " << std::setw(TOTAL_AMOUNT_MAX_WIDTH) << currency.formatAmount(tr.amount);
      }
    }
  }

  logger(INFO, rowColor) << " "; //just to make logger print one endline
}

std::string prepareWalletAddressFilename(const std::string& walletBaseName) {
  return walletBaseName + ".address";
}

bool writeAddressFile(const std::string& addressFilename, const std::string& address) {
  std::ofstream addressFile(addressFilename, std::ios::out | std::ios::trunc | std::ios::binary);
  if (!addressFile.good()) {
    return false;
  }

  addressFile << address;

  return true;
}

#ifndef __ANDROID__
bool processServerAliasResponse(const std::string& s, std::string& address) {
	try {

		// Courtesy of Monero Project
		// make sure the txt record has "oa1:krb" and find it
		auto pos = s.find("oa1:krb");
		if (pos == std::string::npos)
			return false;
		// search from there to find "recipient_address="
		pos = s.find("recipient_address=", pos);
		if (pos == std::string::npos)
			return false;
		pos += 18; // move past "recipient_address="
		// find the next semicolon
		auto pos2 = s.find(";", pos);
		if (pos2 != std::string::npos)
		{
			// length of address == 95, we can at least validate that much here
			if (pos2 - pos == 95)
			{
				address = s.substr(pos, 95);
			} else {
				return false;
			}
		}
	}
	catch (std::exception&) {
		return false;
	}

	return true;
}

bool askAliasesTransfersConfirmation(const std::map<std::string, std::vector<WalletLegacyTransfer>>& aliases, const Currency& currency) {
	std::cout << "Would you like to send money to the following addresses?" << std::endl;

	for (const auto& kv : aliases) {
		for (const auto& transfer : kv.second) {
			std::cout << transfer.address << " " << std::setw(21) << currency.formatAmount(transfer.amount) << "  " << kv.first << std::endl;
		}
	}

	std::string answer;
	do {
		std::cout << "y/n: ";
		std::getline(std::cin, answer);
	} while (answer != "y" && answer != "Y" && answer != "n" && answer != "N");

	return answer == "y" || answer == "Y";
}
#endif

bool processServerFeeAddressResponse(const std::string& response, std::string& fee_address) {
    try {
        std::stringstream stream(response);
        JsonValue json;
        stream >> json;

        auto rootIt = json.getObject().find("fee_address");
        if (rootIt == json.getObject().end()) {
            return false;
        }

        fee_address = rootIt->second.getString();
    }
    catch (std::exception&) {
        return false;
    }

    return true;
}

}

std::string simple_wallet::get_commands_str() {
  std::stringstream ss;
  ss << "Commands: " << ENDL;
  std::string usage = m_consoleHandler.getUsage();
  boost::replace_all(usage, "\n", "\n  ");
  usage.insert(0, "  ");
  ss << usage << ENDL;
  return ss.str();
}

bool simple_wallet::help(const std::vector<std::string> &args/* = std::vector<std::string>()*/) {
  success_msg_writer() << get_commands_str();
  return true;
}

bool simple_wallet::seed(const std::vector<std::string> &args/* = std::vector<std::string>()*/) {
  std::string electrum_words;
  bool success = m_wallet->getSeed(electrum_words);

  if (success)
  {
    std::cout << "\nPLEASE NOTE: the following 25 words can be used to recover access to your wallet. Please write them down and store them somewhere safe and secure. Please do not store them in your email or on file storage services outside of your immediate control.\n";
    std::cout << electrum_words << std::endl;
  }
  else
  {
    fail_msg_writer() << "The wallet is non-deterministic and doesn't have mnemonic seed.";
  }
  return true;
}

bool simple_wallet::exit(const std::vector<std::string> &args) {
  m_consoleHandler.requestStop();
  return true;
}

simple_wallet::simple_wallet(System::Dispatcher& dispatcher, const CryptoNote::Currency& currency, Logging::LoggerManager& log) :
  m_dispatcher(dispatcher),
  m_daemon_port(0), 
  m_currency(currency), 
  m_logManager(log),
  logger(log, "simplewallet"),
  m_refresh_progress_reporter(*this), 
  m_initResultPromise(nullptr),
  m_walletSynchronized(false),
  m_trackingWallet(false){
  m_consoleHandler.setHandler("start_mining", boost::bind(&simple_wallet::start_mining, this, _1), "start_mining [<number_of_threads>] - Start mining in daemon");
  m_consoleHandler.setHandler("stop_mining", boost::bind(&simple_wallet::stop_mining, this, _1), "Stop mining in daemon");
  //m_consoleHandler.setHandler("refresh", boost::bind(&simple_wallet::refresh, this, _1), "Resynchronize transactions and balance");
  m_consoleHandler.setHandler("export_keys", boost::bind(&simple_wallet::export_keys, this, _1), "Show the secret keys of the opened wallet");
  m_consoleHandler.setHandler("tracking_key", boost::bind(&simple_wallet::export_tracking_key, this, _1), "Show the tracking key of the opened wallet");
  m_consoleHandler.setHandler("balance", boost::bind(&simple_wallet::show_balance, this, _1), "Show current wallet balance");
  m_consoleHandler.setHandler("incoming_transfers", boost::bind(&simple_wallet::show_incoming_transfers, this, _1), "Show incoming transfers");
  m_consoleHandler.setHandler("list_transfers", boost::bind(&simple_wallet::listTransfers, this, _1), "Show all known transfers");
  m_consoleHandler.setHandler("payments", boost::bind(&simple_wallet::show_payments, this, _1), "payments <payment_id_1> [<payment_id_2> ... <payment_id_N>] - Show payments <payment_id_1>, ... <payment_id_N>");
  m_consoleHandler.setHandler("bc_height", boost::bind(&simple_wallet::show_blockchain_height, this, _1), "Show blockchain height");
  m_consoleHandler.setHandler("transfer", boost::bind(&simple_wallet::transfer, this, _1),
    "transfer <mixin_count> <addr_1> <amount_1> [<addr_2> <amount_2> ... <addr_N> <amount_N>] [-p payment_id] [-f fee]"
    " - Transfer <amount_1>,... <amount_N> to <address_1>,... <address_N>, respectively. "
    "<mixin_count> is the number of transactions yours is indistinguishable from (from 0 to maximum available)");
  m_consoleHandler.setHandler("set_log", boost::bind(&simple_wallet::set_log, this, _1), "set_log <level> - Change current log level, <level> is a number 0-4");
  m_consoleHandler.setHandler("address", boost::bind(&simple_wallet::print_address, this, _1), "Show current wallet public address");
  m_consoleHandler.setHandler("save", boost::bind(&simple_wallet::save, this, _1), "Save wallet synchronized data");
  m_consoleHandler.setHandler("reset", boost::bind(&simple_wallet::reset, this, _1), "Discard cache data and start synchronizing from the start");
  m_consoleHandler.setHandler("show_seed", boost::bind(&simple_wallet::seed, this, _1), "Get wallet recovery phrase (deterministic seed)");
  m_consoleHandler.setHandler("help", boost::bind(&simple_wallet::help, this, _1), "Show this help");
  m_consoleHandler.setHandler("exit", boost::bind(&simple_wallet::exit, this, _1), "Close wallet");
}
//----------------------------------------------------------------------------------------------------

bool simple_wallet::set_log(const std::vector<std::string> &args)
{
	if (args.size() != 1)
	{
		fail_msg_writer() << "use: set_log <log_level_number_0-4>";
		return true;
	}

	uint16_t l = 0;
	if (!Common::fromString(args[0], l))
	{
		fail_msg_writer() << "wrong number format, use: set_log <log_level_number_0-4>";
		return true;
	}
 
	if (l > Logging::TRACE)
	{
		fail_msg_writer() << "wrong number range, use: set_log <log_level_number_0-4>";
		return true;
	}

	m_logManager.setMaxLevel(static_cast<Logging::Level>(l));
	return true;
}

//----------------------------------------------------------------------------------------------------

bool simple_wallet::init(const boost::program_options::variables_map& vm)
{
	handle_command_line(vm);

	if (!m_daemon_address.empty() && (!m_daemon_host.empty() || 0 != m_daemon_port))
	{
		fail_msg_writer() << "you can't specify daemon host or port several times";
		return false;
	}

	if (m_generate_new.empty() && m_wallet_file_arg.empty())
	{
		std::cout << "Nor 'generate-new-wallet' neither 'wallet-file' argument was specified.\nWhat do you want to do?\n";
		std::cout << "O - open wallet\n";
		std::cout << "G - generate new wallet\n";
		std::cout << "P - change password\n";
		std::cout << "I - import wallet from keys\n";
		std::cout << "R - restore backup/paperwallet\n";
		std::cout << "T - import tracking wallet\n";
		std::cout << "E - exit\n";
		
		char c;
		do
		{
			std::string answer;
			std::getline(std::cin, answer);
			c = answer[0];
			if (!(c == 'O' || c == 'G' || c == 'E' || c == 'I' || c == 'R' || c == 'T' || c == 'P' || c == 'o' || c == 'g' || c == 'e' || c == 'i' || c == 'r' || c == 't' || c == 'p' ))
				std::cout << "Unknown command: " << c <<std::endl;
			else
				break;
		}
		while (true);

		if (c == 'E' || c == 'e')
			return false;

		std::cout << "Specify wallet file name (e.g., wallet.bin).\n";
		std::string userInput;
		do
		{
			std::cout << "Wallet file name: ";
			std::getline(std::cin, userInput);
			boost::algorithm::trim(userInput);
		}
		while (userInput.empty());

		if (c == 'i' || c == 'I')
			m_import_new = userInput;
		else if (c == 'r' || c == 'R')
			m_restore_new = userInput;
		else if (c == 'g' || c == 'G')
			m_generate_new = userInput;
		else if (c == 't' || c == 'T')
			m_track_new = userInput;
		else if (c == 'p' || c == 'P')
			m_change_password = userInput;
		else
			m_wallet_file_arg = userInput;
	}

	if (!m_generate_new.empty() && !m_wallet_file_arg.empty())
	{
		fail_msg_writer() << "You can't specify 'generate-new-wallet' and 'wallet-file' arguments simultaneously";
		return false;
	}

	if (!m_generate_new.empty() && m_restore_deterministic_wallet)
	{
		fail_msg_writer() << "You can't generate new and restore wallet simultaneously.";
		return false;
	}

	std::string walletFileName;
	if (!m_generate_new.empty() || !m_import_new.empty() || !m_restore_new.empty() || !m_track_new.empty() || !m_change_password.empty())
	{
		std::string ignoredString;
		if (!m_generate_new.empty())
			WalletHelper::prepareFileNames(m_generate_new, ignoredString, walletFileName);
		else if (!m_import_new.empty())
			WalletHelper::prepareFileNames(m_import_new, ignoredString, walletFileName);
		else if (!m_restore_new.empty())
			WalletHelper::prepareFileNames(m_restore_new, ignoredString, walletFileName);
		else if (!m_track_new.empty())
			WalletHelper::prepareFileNames(m_track_new, ignoredString, walletFileName);
		else if (!m_change_password.empty())
			m_wallet_file_arg = m_change_password;

		boost::system::error_code ignore;
		if (boost::filesystem::exists(walletFileName, ignore))
		{
			fail_msg_writer() << walletFileName << " already exists";
			return false;
		}
	}

	if (m_daemon_host.empty())
		m_daemon_host = "localhost";
	if (!m_daemon_port)
		m_daemon_port = RPC_DEFAULT_PORT;
  
	if (!m_daemon_address.empty())
	{
		if (!parseUrlAddress(m_daemon_address, m_daemon_host, m_daemon_port))
		{
			fail_msg_writer() << "failed to parse daemon address: " << m_daemon_address;
			return false;
		}
		remote_fee_address = getFeeAddress();
	}
	else
	{
		if (!m_daemon_host.empty())
			remote_fee_address = getFeeAddress();
		m_daemon_address = std::string("http://") + m_daemon_host + ":" + std::to_string(m_daemon_port);
	}

	if (command_line::has_arg(vm, arg_password))
	{
		pwd_container.password(command_line::get_arg(vm, arg_password));
	}
	else if (!pwd_container.read_password())
	{
		fail_msg_writer() << "failed to read wallet password";
		return false;
	}

	this->m_node.reset(new NodeRpcProxy(m_daemon_host, m_daemon_port));

	std::promise<std::error_code> errorPromise;
	std::future<std::error_code> f_error = errorPromise.get_future();
	auto callback = [&errorPromise](std::error_code e) {errorPromise.set_value(e); };

	m_node->addObserver(static_cast<INodeRpcProxyObserver*>(this));
	m_node->init(callback);
	auto error = f_error.get();
	if (error)
	{
		fail_msg_writer() << "failed to init NodeRPCProxy: " << error.message();
		return false;
	}

	if (m_restore_deterministic_wallet && !m_wallet_file_arg.empty())
	{
		// check for recover flag. If present, require electrum word list (only recovery option for now).
		if (m_restore_deterministic_wallet)
		{
			if (m_non_deterministic)
			{
				fail_msg_writer() << "Cannot specify both --restore-deterministic-wallet and --non-deterministic";
				return false;
			}

			if (m_mnemonic_seed.empty())
			{
				std::cout << "Specify mnemonic seed: ";
				std::getline(std::cin, m_mnemonic_seed);

				if (m_mnemonic_seed.empty())
				{
					fail_msg_writer() << "Specify a recovery parameter with the --mnemonic-seed=\"words list here\"";
					return false;
				}
			}

			std::string languageName;
			if (!Crypto::ElectrumWords::words_to_bytes(m_mnemonic_seed, m_recovery_key, languageName))
			{
				fail_msg_writer() << "Electrum-style word list failed verification";
				return false;
			}
		}

		std::string walletAddressFile = prepareWalletAddressFilename(m_wallet_file_arg);
		boost::system::error_code ignore;
		if (boost::filesystem::exists(walletAddressFile, ignore))
		{
			logger(ERROR, BRIGHT_RED) << "Address file already exists: " + walletAddressFile;
			return false;
		}

		bool r = gen_wallet(m_wallet_file_arg, pwd_container.password(), m_recovery_key, 
			m_restore_deterministic_wallet, m_non_deterministic);
		if (!r)
		{
			logger(ERROR, BRIGHT_RED) << "Account creation failed";
			return false;
		}
	}

	if (!m_generate_new.empty())
	{
		std::string walletAddressFile = prepareWalletAddressFilename(m_generate_new);
		boost::system::error_code ignore;
		if (boost::filesystem::exists(walletAddressFile, ignore))
		{
			logger(ERROR, BRIGHT_RED) << "Address file already exists: " + walletAddressFile;
			return false;
		}

		if (!new_wallet(walletFileName, pwd_container.password()))
		{
			logger(ERROR, BRIGHT_RED) << "account creation failed";
			return false;
		}

		if (!writeAddressFile(walletAddressFile, m_wallet->getAddress()))
		{
			logger(WARNING, BRIGHT_RED) << "Couldn't write wallet address file: " + walletAddressFile;
		}
	}
	else if (!m_import_new.empty())
	{
		std::string walletAddressFile = prepareWalletAddressFilename(m_import_new);
		boost::system::error_code ignore;
		if (boost::filesystem::exists(walletAddressFile, ignore))
		{
			logger(ERROR, BRIGHT_RED) << "Address file already exists: " + walletAddressFile;
			return false;
		}

		std::string private_spend_key_string;
		std::string private_view_key_string;
		do
		{
			std::cout << "Private Spend Key: ";
			std::getline(std::cin, private_spend_key_string);
			boost::algorithm::trim(private_spend_key_string);
		}
		while (private_spend_key_string.empty());
		do
		{
			std::cout << "Private View Key: ";
			std::getline(std::cin, private_view_key_string);
			boost::algorithm::trim(private_view_key_string);
		}
		while (private_view_key_string.empty());

		Crypto::Hash private_spend_key_hash;
		Crypto::Hash private_view_key_hash;
		size_t size;
		if (!Common::fromHex(private_spend_key_string, &private_spend_key_hash, sizeof(private_spend_key_hash), size) 
			|| size != sizeof(private_spend_key_hash))
			return false;

		if (!Common::fromHex(private_view_key_string, &private_view_key_hash, sizeof(private_view_key_hash), size) 
			|| size != sizeof(private_spend_key_hash))
			return false;
		
		Crypto::SecretKey private_spend_key = *(struct Crypto::SecretKey *) &private_spend_key_hash;
		Crypto::SecretKey private_view_key = *(struct Crypto::SecretKey *) &private_view_key_hash;

		if (!new_wallet(private_spend_key, private_view_key, walletFileName, pwd_container.password()))
		{
			logger(ERROR, BRIGHT_RED) << "account creation failed";
			return false;
		}

		if (!writeAddressFile(walletAddressFile, m_wallet->getAddress()))
		{
			logger(WARNING, BRIGHT_RED) << "Couldn't write wallet address file: " + walletAddressFile;
		}
	}
	else if (!m_restore_new.empty())
	{
		std::string walletAddressFile = prepareWalletAddressFilename(m_restore_new);
		boost::system::error_code ignore;
		if (boost::filesystem::exists(walletAddressFile, ignore))
		{
			logger(ERROR, BRIGHT_RED) << "Address file already exists: " + walletAddressFile;
			return false;
		}

		std::string private_key_string;
      
		do
		{
			std::cout << "Private Key: ";
			std::getline(std::cin, private_key_string);
			boost::algorithm::trim(private_key_string);
		}
		while (private_key_string.empty());
      
		AccountKeys keys;
		uint64_t addressPrefix;
		std::string data;

		if (private_key_string.length() != 183)
		{
			logger(ERROR, BRIGHT_RED) << "Wrong Private key.";
			return false;
		}

		if (Tools::Base58::decode_addr(private_key_string, addressPrefix, data) 
			&& addressPrefix == parameters::CRYPTONOTE_PUBLIC_ADDRESS_BASE58_PREFIX 
			&& data.size() == sizeof(keys))
		{
			std::memcpy(&keys, data.data(), sizeof(keys));
		}

		if (!new_wallet(keys, walletFileName, pwd_container.password()))
		{
			logger(ERROR, BRIGHT_RED) << "account creation failed";
			return false;
		}

		if (!writeAddressFile(walletAddressFile, m_wallet->getAddress()))
		{
			logger(WARNING, BRIGHT_RED) << "Couldn't write wallet address file: " + walletAddressFile;
		}
	}
	else if (!m_track_new.empty())
	{
		std::string walletAddressFile = prepareWalletAddressFilename(m_restore_new);
		boost::system::error_code ignore;
		if (boost::filesystem::exists(walletAddressFile, ignore))
		{
			logger(ERROR, BRIGHT_RED) << "Address file already exists: " + walletAddressFile;
			return false;
		}

		std::string tracking_key_string;
      
		do
		{
			std::cout << "Tracking Key: ";
			std::getline(std::cin, tracking_key_string);
			boost::algorithm::trim(tracking_key_string);
		}
		while (tracking_key_string.empty());

		if (tracking_key_string.length() != 256)
		{
			logger(ERROR, BRIGHT_RED) << "Wrong Tracking key.";
			return false;
		}

		AccountKeys keys;

		std::string public_spend_key_string = tracking_key_string.substr(0, 64);
		std::string public_view_key_string = tracking_key_string.substr(64, 64);
		std::string private_spend_key_string = tracking_key_string.substr(128, 64);
		std::string private_view_key_string = tracking_key_string.substr(192, 64);

		Crypto::Hash public_spend_key_hash;
		Crypto::Hash public_view_key_hash;
		Crypto::Hash private_spend_key_hash;
		Crypto::Hash private_view_key_hash;

		size_t size;
		if (!Common::fromHex(public_spend_key_string, &public_spend_key_hash, sizeof(public_spend_key_hash), size) 
			|| size != sizeof(public_spend_key_hash))
			return false;
		if (!Common::fromHex(public_view_key_string, &public_view_key_hash, sizeof(public_view_key_hash), size) 
			|| size != sizeof(public_view_key_hash))
			return false;
		if (!Common::fromHex(private_spend_key_string, &private_spend_key_hash, sizeof(private_spend_key_hash), size) 
			|| size != sizeof(private_spend_key_hash))
			return false;
		if (!Common::fromHex(private_view_key_string, &private_view_key_hash, sizeof(private_view_key_hash), size) 
			|| size != sizeof(private_spend_key_hash))
			return false;

		Crypto::PublicKey public_spend_key  = *(struct Crypto::PublicKey*) &public_spend_key_hash;
		Crypto::PublicKey public_view_key   = *(struct Crypto::PublicKey*) &public_view_key_hash;
		Crypto::SecretKey private_spend_key = *(struct Crypto::SecretKey*) &private_spend_key_hash;
		Crypto::SecretKey private_view_key  = *(struct Crypto::SecretKey*) &private_view_key_hash;

		keys.address.spendPublicKey = public_spend_key;
		keys.address.viewPublicKey = public_view_key;
		keys.spendSecretKey = private_spend_key;
		keys.viewSecretKey = private_view_key;

		if (!new_tracking_wallet(keys, walletFileName, pwd_container.password()))
		{
			logger(ERROR, BRIGHT_RED) << "account creation failed";
			return false;
		}

		if (!writeAddressFile(walletAddressFile, m_wallet->getAddress()))
		{
			logger(WARNING, BRIGHT_RED) << "Couldn't write wallet address file: " + walletAddressFile;
		}
	}
	else
	{
		m_wallet.reset(new WalletLegacy(m_currency, *m_node, m_logManager));

		try
		{
			m_wallet_file = tryToOpenWalletOrLoadKeysOrThrow(logger, m_wallet, m_wallet_file_arg, pwd_container.password());
		}
		catch (const std::exception& e)
		{
			fail_msg_writer() << "failed to load wallet: " << e.what();
			return false;
		}

		m_wallet->addObserver(this);
		m_node->addObserver(static_cast<INodeObserver*>(this));

		logger(INFO, BRIGHT_WHITE) << "Opened wallet: " << m_wallet->getAddress();

		AccountKeys keys;
		m_wallet->getAccountKeys(keys);
		if (keys.spendSecretKey == boost::value_initialized<Crypto::SecretKey>())
		{
			m_trackingWallet = true;
			success_msg_writer() << "This is tracking wallet. Spending unavailable.\n";
		}

		if (!m_change_password.empty())
			change_password();

		success_msg_writer() <<
			"**********************************************************************\n" <<
			"Use \"help\" command to see the list of available commands.\n" <<
			"**********************************************************************";
	}

	return true;
}

//----------------------------------------------------------------------------------------------------

bool simple_wallet::deinit() {
  m_wallet->removeObserver(this);
  m_node->removeObserver(static_cast<INodeObserver*>(this));
  m_node->removeObserver(static_cast<INodeRpcProxyObserver*>(this));

  if (!m_wallet.get())
    return true;

  return close_wallet();
}
//----------------------------------------------------------------------------------------------------
void simple_wallet::handle_command_line(const boost::program_options::variables_map& vm)
{
	m_wallet_file_arg              = command_line::get_arg(vm, arg_wallet_file);
	m_generate_new                 = command_line::get_arg(vm, arg_generate_new_wallet);
	m_daemon_address               = command_line::get_arg(vm, arg_daemon_address);
	m_daemon_host                  = command_line::get_arg(vm, arg_daemon_host);
	m_daemon_port                  = command_line::get_arg(vm, arg_daemon_port);
	m_restore_deterministic_wallet = command_line::get_arg(vm, arg_restore_deterministic_wallet);
	m_non_deterministic            = command_line::get_arg(vm, arg_non_deterministic);
	m_mnemonic_seed                = command_line::get_arg(vm, arg_mnemonic_seed);
}

//----------------------------------------------------------------------------------------------------

bool simple_wallet::gen_wallet(const std::string &wallet_file, const std::string& password, 
	const Crypto::SecretKey& recovery_key, bool recover, bool two_random)
{
	m_wallet_file = wallet_file;

	m_wallet.reset(new WalletLegacy(m_currency, *m_node.get(), m_logManager));
	m_node->addObserver(static_cast<INodeObserver*>(this));
	m_wallet->addObserver(this);

	Crypto::SecretKey recovery_val;
	try
	{
		m_initResultPromise.reset(new std::promise<std::error_code>());
		std::future<std::error_code> f_initError = m_initResultPromise->get_future();

		recovery_val = m_wallet->generateKey(password, recovery_key, recover, two_random);
		auto initError = f_initError.get();
		m_initResultPromise.reset(nullptr);
		if (initError)
		{
			fail_msg_writer() << "failed to generate new wallet: " << initError.message();
			return false;
		}

		try
		{
			CryptoNote::WalletHelper::storeWallet(*m_wallet, m_wallet_file);
		}
		catch (std::exception& e)
		{
			fail_msg_writer() << "failed to save new wallet: " << e.what();
			throw;
		}

		AccountKeys keys;
		m_wallet->getAccountKeys(keys);

		logger(INFO, BRIGHT_WHITE) <<
			"Generated new wallet: " << m_wallet->getAddress() << std::endl <<
			"view key: " << Common::podToHex(keys.viewSecretKey);
	}
	catch (const std::exception& e)
	{
		fail_msg_writer() << "failed to generate new wallet: " << e.what();
		return false;
	}

	// convert rng value to electrum-style word list
	std::string electrum_words;
	Crypto::ElectrumWords::bytes_to_words(recovery_val, electrum_words, "English");
	std::string print_electrum = "";

	success_msg_writer() <<
		"**********************************************************************\n" <<
		"Your wallet has been generated.\n" <<
		"To start synchronizing with the daemon use \"refresh\" command.\n" <<
		"Use \"help\" command to see the list of available commands.\n" <<
		"Always use \"exit\" command when closing simplewallet to save\n" <<
		"current session's state. Otherwise, you will possibly need to synchronize \n" <<
		"your wallet again. Your wallet key is NOT under risk anyway.\n";

	if (!two_random)
	{
		std::cout << "\nPLEASE NOTE: the following 25 words can be used to recover access to your wallet. " <<
			"Please write them down and store them somewhere safe and secure. Please do not store them in your email or " <<
			"on file storage services outside of your immediate control.\n\n";
		std::cout << electrum_words << std::endl;
	}
	success_msg_writer() << "**********************************************************************";

	return true;
}

//----------------------------------------------------------------------------------------------------

bool simple_wallet::new_wallet(const std::string &wallet_file, const std::string& password)
{
	m_wallet_file = wallet_file;

	m_wallet.reset(new WalletLegacy(m_currency, *m_node.get(), m_logManager));
	m_node->addObserver(static_cast<INodeObserver*>(this));
	m_wallet->addObserver(this);

	try
	{
		m_initResultPromise.reset(new std::promise<std::error_code>());
		std::future<std::error_code> f_initError = m_initResultPromise->get_future();
		// m_wallet->initAndGenerate(password);
		// Create deterministic wallets by default
		m_wallet->initAndGenerateDeterministic(password);
		auto initError = f_initError.get();
		m_initResultPromise.reset(nullptr);
		if (initError)
		{
			fail_msg_writer() << "failed to generate new wallet: " << initError.message();
			return false;
		}

		try
		{
			CryptoNote::WalletHelper::storeWallet(*m_wallet, m_wallet_file);
			//create wallet backup file
			boost::filesystem::copy_file(m_wallet_file, boost::filesystem::change_extension(m_wallet_file, ".walletbak"));
		}
		catch (std::exception& e)
		{
			fail_msg_writer() << "failed to save new wallet: " << e.what();
			throw;
		}

		AccountKeys keys;
		m_wallet->getAccountKeys(keys);

		logger(INFO, BRIGHT_WHITE) <<
			"Generated new wallet: " << m_wallet->getAddress() << std::endl <<
			"view key: " << Common::podToHex(keys.viewSecretKey);
	}
	catch (const std::exception& e)
	{
		fail_msg_writer() << "failed to generate new wallet: " << e.what();
		return false;
	}

	AccountKeys keys;
	m_wallet->getAccountKeys(keys);
	// convert rng value to electrum-style word list
	std::string electrum_words;
	Crypto::ElectrumWords::bytes_to_words(keys.spendSecretKey, electrum_words, "English");
	std::string print_electrum = "";

	success_msg_writer() <<
		"**********************************************************************\n" <<
		"Your wallet has been generated.\n" <<
		"Use \"help\" command to see the list of available commands.\n" <<
		"Always use \"exit\" command when closing simplewallet to save\n" <<
		"current session's state. Otherwise, you will possibly need to synchronize \n" <<
		"your wallet again. Your wallet key is NOT under risk anyway.\n" <<
		"**********************************************************************";

	std::cout << "\nPLEASE NOTE: the following 25 words can be used to recover access to your wallet. " <<
		"Please write them down and store them somewhere safe and secure. Please do not store them in your email or " <<
		"on file storage services outside of your immediate control.\n\n";
	std::cout << electrum_words << std::endl;
	success_msg_writer() << "**********************************************************************";
	return true;
}

//----------------------------------------------------------------------------------------------------

bool simple_wallet::new_wallet(Crypto::SecretKey &secret_key, Crypto::SecretKey &view_key, const std::string &wallet_file, const std::string& password) {
  m_wallet_file = wallet_file;

  m_wallet.reset(new WalletLegacy(m_currency, *m_node.get(), m_logManager));
  m_node->addObserver(static_cast<INodeObserver*>(this));
  m_wallet->addObserver(this);
  try {
    m_initResultPromise.reset(new std::promise<std::error_code>());
    std::future<std::error_code> f_initError = m_initResultPromise->get_future();

    AccountKeys wallet_keys;
    wallet_keys.spendSecretKey = secret_key;
    wallet_keys.viewSecretKey = view_key;
    Crypto::secret_key_to_public_key(wallet_keys.spendSecretKey, wallet_keys.address.spendPublicKey);
    Crypto::secret_key_to_public_key(wallet_keys.viewSecretKey, wallet_keys.address.viewPublicKey);

    m_wallet->initWithKeys(wallet_keys, password);
    auto initError = f_initError.get();
    m_initResultPromise.reset(nullptr);
    if (initError) {
      fail_msg_writer() << "failed to generate new wallet: " << initError.message();
      return false;
    }

    try {
      CryptoNote::WalletHelper::storeWallet(*m_wallet, m_wallet_file);
    } catch (std::exception& e) {
      fail_msg_writer() << "failed to save new wallet: " << e.what();
      throw;
    }

    AccountKeys keys;
    m_wallet->getAccountKeys(keys);

    logger(INFO, BRIGHT_WHITE) <<
      "Imported wallet: " << m_wallet->getAddress() << std::endl;
  }
  catch (const std::exception& e) {
    fail_msg_writer() << "failed to import wallet: " << e.what();
    return false;
  }

  success_msg_writer() <<
    "**********************************************************************\n" <<
    "Your wallet has been imported.\n" <<
    "Use \"help\" command to see the list of available commands.\n" <<
    "Always use \"exit\" command when closing simplewallet to save\n" <<
    "current session's state. Otherwise, you will possibly need to synchronize \n" <<
    "your wallet again. Your wallet key is NOT under risk anyway.\n" <<
    "**********************************************************************";
  return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::new_wallet(AccountKeys &private_key, const std::string &wallet_file, const std::string& password) {
    m_wallet_file = wallet_file;

    m_wallet.reset(new WalletLegacy(m_currency, *m_node.get(), m_logManager));
    m_node->addObserver(static_cast<INodeObserver*>(this));
    m_wallet->addObserver(this);
    try {
        m_initResultPromise.reset(new std::promise<std::error_code>());
        std::future<std::error_code> f_initError = m_initResultPromise->get_future();

        m_wallet->initWithKeys(private_key, password);
        auto initError = f_initError.get();
        m_initResultPromise.reset(nullptr);
        if (initError) {
            fail_msg_writer() << "failed to generate new wallet: " << initError.message();
            return false;
        }

        try {
            CryptoNote::WalletHelper::storeWallet(*m_wallet, m_wallet_file);
        }
        catch (std::exception& e) {
            fail_msg_writer() << "failed to save new wallet: " << e.what();
            throw;
        }

        AccountKeys keys;
        m_wallet->getAccountKeys(keys);

        logger(INFO, BRIGHT_WHITE) <<
            "Imported wallet: " << m_wallet->getAddress() << std::endl;

        if (keys.spendSecretKey == boost::value_initialized<Crypto::SecretKey>()) {
           m_trackingWallet = true;
        }
    }
    catch (const std::exception& e) {
        fail_msg_writer() << "failed to import wallet: " << e.what();
        return false;
    }

    success_msg_writer() <<
        "**********************************************************************\n" <<
        "Your wallet has been imported.\n" <<
        "Use \"help\" command to see the list of available commands.\n" <<
        "Always use \"exit\" command when closing simplewallet to save\n" <<
        "current session's state. Otherwise, you will possibly need to synchronize \n" <<
        "your wallet again. Your wallet key is NOT under risk anyway.\n" <<
        "**********************************************************************";
    return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::new_tracking_wallet(AccountKeys &tracking_key, const std::string &wallet_file, const std::string& password) {
    m_wallet_file = wallet_file;

    m_wallet.reset(new WalletLegacy(m_currency, *m_node.get(), m_logManager));
    m_node->addObserver(static_cast<INodeObserver*>(this));
    m_wallet->addObserver(this);
    try {
        m_initResultPromise.reset(new std::promise<std::error_code>());
        std::future<std::error_code> f_initError = m_initResultPromise->get_future();

        m_wallet->initWithKeys(tracking_key, password);
        auto initError = f_initError.get();
        m_initResultPromise.reset(nullptr);
        if (initError) {
            fail_msg_writer() << "failed to generate new wallet: " << initError.message();
            return false;
        }

        try {
            CryptoNote::WalletHelper::storeWallet(*m_wallet, m_wallet_file);
        }
        catch (std::exception& e) {
            fail_msg_writer() << "failed to save new wallet: " << e.what();
            throw;
        }

        AccountKeys keys;
        m_wallet->getAccountKeys(keys);

        logger(INFO, BRIGHT_WHITE) <<
            "Imported wallet: " << m_wallet->getAddress() << std::endl;

        m_trackingWallet = true;
    }
    catch (const std::exception& e) {
        fail_msg_writer() << "failed to import wallet: " << e.what();
        return false;
    }

    success_msg_writer() <<
        "**********************************************************************\n" <<
        "Your tracking wallet has been imported. It doesn't allow spending funds.\n" <<
        "It allows to view incoming transactions but not outgoing ones. \n" <<
        "If there were spendings total balance will be inaccurate. \n" <<
        "Use \"help\" command to see the list of available commands.\n" <<
        "Always use \"exit\" command when closing simplewallet to save\n" <<
        "current session's state. Otherwise, you will possibly need to synchronize \n" <<
        "your wallet again. Your wallet key is NOT under risk anyway.\n" <<
        "**********************************************************************";
    return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::close_wallet()
{
  try {
    CryptoNote::WalletHelper::storeWallet(*m_wallet, m_wallet_file);
  } catch (const std::exception& e) {
    fail_msg_writer() << e.what();
    return false;
  }

  m_wallet->removeObserver(this);
  m_wallet->shutdown();

  return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::save(const std::vector<std::string> &args)
{
  try {
    CryptoNote::WalletHelper::storeWallet(*m_wallet, m_wallet_file);
    success_msg_writer() << "Wallet data saved";
  } catch (const std::exception& e) {
    fail_msg_writer() << e.what();
  }

  return true;
}

bool simple_wallet::reset(const std::vector<std::string> &args) {
  {
    std::unique_lock<std::mutex> lock(m_walletSynchronizedMutex);
    m_walletSynchronized = false;
  }

  m_wallet->reset();
  success_msg_writer(true) << "Reset completed successfully.";

  std::unique_lock<std::mutex> lock(m_walletSynchronizedMutex);
  while (!m_walletSynchronized) {
    m_walletSynchronizedCV.wait(lock);
  }

  std::cout << std::endl;

  return true;
}

std::string simple_wallet::get_password() {
	pwd_container.read_password();
	return pwd_container.password();
}

bool simple_wallet::change_password() {
  std::cout << std::endl;
  //std::cout << "Old ";
  //const auto oldpwd = get_password();
  const auto oldpwd = pwd_container.password();
  std::cout << "New ";
  const auto newpwd = get_password();
  std::cout << "Repeat new ";
  const auto newpwd2 = get_password();
  if (newpwd != newpwd2) {
	fail_msg_writer() << "Passwords doesn't match. Try again.";
	change_password();
	return false;
  } 
  try
	{
		m_wallet->changePassword(oldpwd, newpwd);
	}
	catch (const std::exception& e) {
		fail_msg_writer() << e.what();
		return false;
	}
	success_msg_writer(true) << "Password changed.";
	return true;
}

bool simple_wallet::start_mining(const std::vector<std::string>& args) {
  COMMAND_RPC_START_MINING::request req;
  req.miner_address = m_wallet->getAddress();

  bool ok = true;
  size_t max_mining_threads_count = (std::max)(std::thread::hardware_concurrency(), static_cast<unsigned>(2));
  if (0 == args.size()) {
    req.threads_count = 1;
  } else if (1 == args.size()) {
    uint16_t num = 1;
    ok = Common::fromString(args[0], num);
    ok = ok && (1 <= num && num <= max_mining_threads_count);
    req.threads_count = num;
  } else {
    ok = false;
  }

  if (!ok) {
    fail_msg_writer() << "invalid arguments. Please use start_mining [<number_of_threads>], " <<
      "<number_of_threads> should be from 1 to " << max_mining_threads_count;
    return true;
  }

  COMMAND_RPC_START_MINING::response res;

  try {
    HttpClient httpClient(m_dispatcher, m_daemon_host, m_daemon_port);

    invokeJsonCommand(httpClient, "/start_mining", req, res);

    std::string err = interpret_rpc_response(true, res.status);
    if (err.empty())
      success_msg_writer() << "Mining started in daemon";
    else
      fail_msg_writer() << "mining has NOT been started: " << err;

  } catch (const ConnectException&) {
    printConnectionError();
  } catch (const std::exception& e) {
    fail_msg_writer() << "Failed to invoke rpc method: " << e.what();
  }

  return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::stop_mining(const std::vector<std::string>& args)
{
  COMMAND_RPC_STOP_MINING::request req;
  COMMAND_RPC_STOP_MINING::response res;

  try {
    HttpClient httpClient(m_dispatcher, m_daemon_host, m_daemon_port);

    invokeJsonCommand(httpClient, "/stop_mining", req, res);
    std::string err = interpret_rpc_response(true, res.status);
    if (err.empty())
      success_msg_writer() << "Mining stopped in daemon";
    else
      fail_msg_writer() << "mining has NOT been stopped: " << err;
  } catch (const ConnectException&) {
    printConnectionError();
  } catch (const std::exception& e) {
    fail_msg_writer() << "Failed to invoke rpc method: " << e.what();
  }

  return true;
}
//----------------------------------------------------------------------------------------------------
void simple_wallet::initCompleted(std::error_code result) {
  if (m_initResultPromise.get() != nullptr) {
    m_initResultPromise->set_value(result);
  }
}
//----------------------------------------------------------------------------------------------------
void simple_wallet::connectionStatusUpdated(bool connected) {
  if (connected) {
    logger(INFO, GREEN) << "Wallet connected to daemon.";
  } else {
    printConnectionError();
  }
}
//----------------------------------------------------------------------------------------------------
void simple_wallet::externalTransactionCreated(CryptoNote::TransactionId transactionId)  {
  WalletLegacyTransaction txInfo;
  m_wallet->getTransaction(transactionId, txInfo);
  
  std::stringstream logPrefix;
  if (txInfo.blockHeight == WALLET_LEGACY_UNCONFIRMED_TRANSACTION_HEIGHT) {
    logPrefix << "Unconfirmed";
  } else {
    logPrefix << "Height " << txInfo.blockHeight << ',';
  }

  if (txInfo.totalAmount >= 0) {
    logger(INFO, GREEN) <<
      logPrefix.str() << " transaction " << Common::podToHex(txInfo.hash) <<
      ", received " << m_currency.formatAmount(txInfo.totalAmount);
  } else {
    logger(INFO, MAGENTA) <<
      logPrefix.str() << " transaction " << Common::podToHex(txInfo.hash) <<
      ", spent " << m_currency.formatAmount(static_cast<uint64_t>(-txInfo.totalAmount));
  }

  if (txInfo.blockHeight == WALLET_LEGACY_UNCONFIRMED_TRANSACTION_HEIGHT) {
    m_refresh_progress_reporter.update(m_node->getLastLocalBlockHeight(), true);
  } else {
    m_refresh_progress_reporter.update(txInfo.blockHeight, true);
  }
}
//----------------------------------------------------------------------------------------------------
void simple_wallet::synchronizationCompleted(std::error_code result) {
  std::unique_lock<std::mutex> lock(m_walletSynchronizedMutex);
  m_walletSynchronized = true;
  m_walletSynchronizedCV.notify_one();
}

void simple_wallet::synchronizationProgressUpdated(uint32_t current, uint32_t total) {
  std::unique_lock<std::mutex> lock(m_walletSynchronizedMutex);
  if (!m_walletSynchronized) {
    m_refresh_progress_reporter.update(current, false);
  }
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::export_keys(const std::vector<std::string>& args/* = std::vector<std::string>()*/) {
  AccountKeys keys;
  m_wallet->getAccountKeys(keys);
  std::cout << "Spend secret key: " << Common::podToHex(keys.spendSecretKey) << std::endl;
  std::cout << "View secret key: " << Common::podToHex(keys.viewSecretKey) << std::endl;
  std::cout << "Private keys: " << Tools::Base58::encode_addr(parameters::CRYPTONOTE_PUBLIC_ADDRESS_BASE58_PREFIX,
    std::string(reinterpret_cast<char*>(&keys), sizeof(keys))) << std::endl;
  
  return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::export_tracking_key(const std::vector<std::string>& args/* = std::vector<std::string>()*/) {
    AccountKeys keys;
    m_wallet->getAccountKeys(keys);
    std::string spend_public_key = Common::podToHex(keys.address.spendPublicKey);
    keys.spendSecretKey = boost::value_initialized<Crypto::SecretKey>();
    success_msg_writer(true) << "Tracking key: " << spend_public_key << Common::podToHex(keys.address.viewPublicKey) << Common::podToHex(keys.spendSecretKey) << Common::podToHex(keys.viewSecretKey);
    // This will show Tracking Key in style of Private Key Backup or Paperwallet, to prevent confusing we use above style of Bytecoin like tracking keys
    // success_msg_writer(true) << "Tracking key: " << Tools::Base58::encode_addr(parameters::CRYPTONOTE_PUBLIC_ADDRESS_BASE58_PREFIX, std::string(reinterpret_cast<char*>(&keys), sizeof(keys)));

    return true;
}
//---------------------------------------------------------------------------------------------------- 
bool simple_wallet::show_balance(const std::vector<std::string>& args/* = std::vector<std::string>()*/) {
  success_msg_writer() << "available balance: " << m_currency.formatAmount(m_wallet->actualBalance()) <<
    ", locked amount: " << m_currency.formatAmount(m_wallet->pendingBalance());

  return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::show_incoming_transfers(const std::vector<std::string>& args) {
  bool hasTransfers = false;
  size_t transactionsCount = m_wallet->getTransactionCount();
  for (size_t trantransactionNumber = 0; trantransactionNumber < transactionsCount; ++trantransactionNumber) {
    WalletLegacyTransaction txInfo;
    m_wallet->getTransaction(trantransactionNumber, txInfo);
    if (txInfo.totalAmount < 0) continue;
    hasTransfers = true;
    logger(INFO) << "        amount       \t                              tx id";
    logger(INFO, GREEN) <<  // spent - magenta
      std::setw(21) << m_currency.formatAmount(txInfo.totalAmount) << '\t' << Common::podToHex(txInfo.hash);
  }

  if (!hasTransfers) success_msg_writer() << "No incoming transfers";
  return true;
}

bool simple_wallet::listTransfers(const std::vector<std::string>& args) {
  bool haveTransfers = false;

  size_t transactionsCount = m_wallet->getTransactionCount();
  for (size_t trantransactionNumber = 0; trantransactionNumber < transactionsCount; ++trantransactionNumber) {
    WalletLegacyTransaction txInfo;
    m_wallet->getTransaction(trantransactionNumber, txInfo);
    if (txInfo.state != WalletLegacyTransactionState::Active || txInfo.blockHeight == WALLET_LEGACY_UNCONFIRMED_TRANSACTION_HEIGHT) {
      continue;
    }

    if (!haveTransfers) {
      printListTransfersHeader(logger);
      haveTransfers = true;
    }

    printListTransfersItem(logger, txInfo, *m_wallet, m_currency);
  }

  if (!haveTransfers) {
    success_msg_writer() << "No transfers";
  }

  return true;
}

bool simple_wallet::show_payments(const std::vector<std::string> &args) {
  if (args.empty()) {
    fail_msg_writer() << "expected at least one payment ID";
    return true;
  }

  logger(INFO) << "                            payment                             \t" <<
    "                          transaction                           \t" <<
    "  height\t       amount        ";

  bool payments_found = false;
  for (const std::string& arg: args) {
    Crypto::Hash expectedPaymentId;
    if (CryptoNote::parsePaymentId(arg, expectedPaymentId)) {
      size_t transactionsCount = m_wallet->getTransactionCount();
      for (size_t trantransactionNumber = 0; trantransactionNumber < transactionsCount; ++trantransactionNumber) {
        WalletLegacyTransaction txInfo;
        m_wallet->getTransaction(trantransactionNumber, txInfo);
        if (txInfo.totalAmount < 0) continue;
        std::vector<uint8_t> extraVec;
        extraVec.reserve(txInfo.extra.size());
        std::for_each(txInfo.extra.begin(), txInfo.extra.end(), [&extraVec](const char el) { extraVec.push_back(el); });

        Crypto::Hash paymentId;
        if (CryptoNote::getPaymentIdFromTxExtra(extraVec, paymentId) && paymentId == expectedPaymentId) {
          payments_found = true;
          success_msg_writer(true) <<
            paymentId << "\t\t" <<
            Common::podToHex(txInfo.hash) <<
            std::setw(8) << txInfo.blockHeight << '\t' <<
            std::setw(21) << m_currency.formatAmount(txInfo.totalAmount);// << '\t' <<
        }
      }

      if (!payments_found) {
        success_msg_writer() << "No payments with id " << expectedPaymentId;
        continue;
      }
    } else {
      fail_msg_writer() << "payment ID has invalid format: \"" << arg << "\", expected 64-character string";
    }
  }

  return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::show_blockchain_height(const std::vector<std::string>& args) {
  try {
    uint64_t bc_height = m_node->getLastLocalBlockHeight();
    success_msg_writer() << bc_height;
  } catch (std::exception &e) {
    fail_msg_writer() << "failed to get blockchain height: " << e.what();
  }

  return true;
}
#ifndef __ANDROID__
//----------------------------------------------------------------------------------------------------
std::string simple_wallet::resolveAlias(const std::string& aliasUrl) {
	std::string host;
	std::string uri;
	std::string record;
	std::string address;

	// DNS Lookup
	if (!fetch_dns_txt(aliasUrl, record)) {
		throw std::runtime_error("Failed to lookup DNS record");
	}

	if (!processServerAliasResponse(record, address)) {
		throw std::runtime_error("Failed to parse server response");
	}
	
	return address;
}

bool simple_wallet::fetch_dns_txt(const std::string domain, std::string &record) {

#ifdef WIN32
	using namespace std;

#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "Dnsapi.lib")

	PDNS_RECORD pDnsRecord;          //Pointer to DNS_RECORD structure.

	{
		WORD type = DNS_TYPE_TEXT;

		if (0 != DnsQuery_A(domain.c_str(), type, DNS_QUERY_BYPASS_CACHE, NULL, &pDnsRecord, NULL))
		{
			cerr << "Error querying: '" << domain << "'" << endl;
			return false;
		}
	}

	PDNS_RECORD it;
	map<WORD, function<void(void)>> callbacks;
	
	callbacks[DNS_TYPE_TEXT] = [&it,&record](void) -> void {
		std::stringstream stream;
		for (DWORD i = 0; i < it->Data.TXT.dwStringCount; i++) {
			stream << RPC_CSTR(it->Data.TXT.pStringArray[i]) << endl;;
		}
		record = stream.str();
	};

	for (it = pDnsRecord; it != NULL; it = it->pNext) {
		if (callbacks.count(it->wType)) {
			callbacks[it->wType]();
		}
	}
	DnsRecordListFree(pDnsRecord, DnsFreeRecordListDeep);
# else
	using namespace std;

	res_init();
	ns_msg nsMsg;
	int response;
	unsigned char query_buffer[1024];
	{
		ns_type type = ns_t_txt;

		const char * c_domain = (domain).c_str();
		response = res_query(c_domain, C_IN, type, query_buffer, sizeof(query_buffer));

		if (response < 0)
			return false;
	}

	ns_initparse(query_buffer, response, &nsMsg);

	map<ns_type, function<void(const ns_rr &rr)>> callbacks;

	callbacks[ns_t_txt] = [&nsMsg,&record](const ns_rr &rr) -> void {
		std::stringstream stream;
		stream << ns_rr_rdata(rr) + 1 << endl;
		record = stream.str();
	};

	for (int x = 0; x < ns_msg_count(nsMsg, ns_s_an); x++) {
		ns_rr rr;
		ns_parserr(&nsMsg, ns_s_an, x, &rr);
		ns_type type = ns_rr_type(rr);
		if (callbacks.count(type)) {
			callbacks[type](rr);
		}
	}

#endif
	if (record.empty())
		return false;

	return true;
}
#endif
//----------------------------------------------------------------------------------------------------
std::string simple_wallet::getFeeAddress() {
  
  HttpClient httpClient(m_dispatcher, m_daemon_host, m_daemon_port);

  HttpRequest req;
  HttpResponse res;

  req.setUrl("/feeaddress");
  httpClient.request(req, res);

  if (res.getStatus() != HttpResponse::STATUS_200) {
    throw std::runtime_error("Remote server returned code " + std::to_string(res.getStatus()));
  }

  std::string address;
  if (!processServerFeeAddressResponse(res.getBody(), address)) {
    throw std::runtime_error("Failed to parse server response");
  }

  return address;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::transfer(const std::vector<std::string> &args) {
  if (m_trackingWallet){
    fail_msg_writer() << "This is tracking wallet. Spending is impossible.";
    return true;
  }
  try {
    TransferCommand cmd(m_currency);

	if (!cmd.parseArguments(logger, args))
		return true;

#ifndef __ANDROID__
	for (auto& kv : cmd.aliases) {
		std::string address;

		try {
			address = resolveAlias(kv.first);

			AccountPublicAddress ignore;
			if (!m_currency.parseAccountAddressString(address, ignore)) {
				throw std::runtime_error("Address \"" + address + "\" is invalid");
			}
		}
		catch (std::exception& e) {
			fail_msg_writer() << "Couldn't resolve alias: " << e.what() << ", alias: " << kv.first;
			return true;
		}

		for (auto& transfer : kv.second) {
			transfer.address = address;
		}
	}

	if (!cmd.aliases.empty()) {
		if (!askAliasesTransfersConfirmation(cmd.aliases, m_currency)) {
			return true;
		}

		for (auto& kv : cmd.aliases) {
			std::copy(std::move_iterator<std::vector<WalletLegacyTransfer>::iterator>(kv.second.begin()),
				std::move_iterator<std::vector<WalletLegacyTransfer>::iterator>(kv.second.end()),
				std::back_inserter(cmd.dsts));
		}
	}
#endif

    CryptoNote::WalletHelper::SendCompleteResultObserver sent;

    std::string extraString;
    std::copy(cmd.extra.begin(), cmd.extra.end(), std::back_inserter(extraString));

    WalletHelper::IWalletRemoveObserverGuard removeGuard(*m_wallet, sent);

    CryptoNote::TransactionId tx = m_wallet->sendTransaction(cmd.dsts, cmd.fee, extraString, cmd.fake_outs_count, 0);
    if (tx == WALLET_LEGACY_INVALID_TRANSACTION_ID) {
      fail_msg_writer() << "Can't send money";
      return true;
    }

    std::error_code sendError = sent.wait(tx);
    removeGuard.removeObserver();

    if (sendError) {
      fail_msg_writer() << sendError.message();
      return true;
    }

    CryptoNote::WalletLegacyTransaction txInfo;
    m_wallet->getTransaction(tx, txInfo);
    success_msg_writer(true) << "Money successfully sent, transaction " << Common::podToHex(txInfo.hash);

    try {
      CryptoNote::WalletHelper::storeWallet(*m_wallet, m_wallet_file);
    } catch (const std::exception& e) {
      fail_msg_writer() << e.what();
      return true;
    }
  } catch (const std::system_error& e) {
    fail_msg_writer() << e.what();
  } catch (const std::exception& e) {
    fail_msg_writer() << e.what();
  } catch (...) {
    fail_msg_writer() << "unknown error";
  }

  return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::run() {
  {
    std::unique_lock<std::mutex> lock(m_walletSynchronizedMutex);
    while (!m_walletSynchronized) {
      m_walletSynchronizedCV.wait(lock);
    }
  }

  std::cout << std::endl;

  std::string addr_start = m_wallet->getAddress().substr(0, 6);
  m_consoleHandler.start(false, "[wallet " + addr_start + "]: ", Common::Console::Color::BrightYellow);
  return true;
}
//----------------------------------------------------------------------------------------------------
void simple_wallet::stop() {
  m_consoleHandler.requestStop();
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::print_address(const std::vector<std::string> &args/* = std::vector<std::string>()*/) {
  success_msg_writer() << m_wallet->getAddress();
  return true;
}
//----------------------------------------------------------------------------------------------------
bool simple_wallet::process_command(const std::vector<std::string> &args) {
  return m_consoleHandler.runCommand(args);
}

void simple_wallet::printConnectionError() const {
  fail_msg_writer() << "wallet failed to connect to daemon (" << m_daemon_address << ").";
}


int main(int argc, char* argv[]) {
#ifdef WIN32
   setlocale(LC_ALL, "");
   SetConsoleCP(1251);
   SetConsoleOutputCP(1251);
  _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif

  setbuf(stdout, NULL);

  po::options_description desc_general("General options");
  command_line::add_arg(desc_general, command_line::arg_help);
  command_line::add_arg(desc_general, command_line::arg_version);

  po::options_description desc_params("Wallet options");
  command_line::add_arg(desc_params, arg_wallet_file);
  command_line::add_arg(desc_params, arg_generate_new_wallet);
  command_line::add_arg(desc_params, arg_restore_deterministic_wallet);
  command_line::add_arg(desc_params, arg_non_deterministic);
  command_line::add_arg(desc_params, arg_mnemonic_seed);
  command_line::add_arg(desc_params, arg_password);
  command_line::add_arg(desc_params, arg_daemon_address);
  command_line::add_arg(desc_params, arg_daemon_host);
  command_line::add_arg(desc_params, arg_daemon_port);
  command_line::add_arg(desc_params, arg_command);
  command_line::add_arg(desc_params, arg_log_file);
  command_line::add_arg(desc_params, arg_log_level);
  command_line::add_arg(desc_params, arg_testnet);
  Tools::wallet_rpc_server::init_options(desc_params);

  po::positional_options_description positional_options;
  positional_options.add(arg_command.name, -1);

  po::options_description desc_all;
  desc_all.add(desc_general).add(desc_params);

  Logging::LoggerManager logManager;
  Logging::LoggerRef logger(logManager, "simplewallet");
  System::Dispatcher dispatcher;

  po::variables_map vm;

  bool r = command_line::handle_error_helper(desc_all, [&]() {
    po::store(command_line::parse_command_line(argc, argv, desc_general, true), vm);

    if (command_line::get_arg(vm, command_line::arg_help)) {
      CryptoNote::Currency tmp_currency = CryptoNote::CurrencyBuilder(logManager).currency();
      CryptoNote::simple_wallet tmp_wallet(dispatcher, tmp_currency, logManager);

      std::cout << CRYPTONOTE_NAME << " wallet v" << PROJECT_VERSION_LONG << std::endl;
      std::cout << "Usage: simplewallet [--wallet-file=<file>|--generate-new-wallet=<file>] [--daemon-address=<host>:<port>] [<COMMAND>]";
      std::cout << desc_all << '\n' << tmp_wallet.get_commands_str();
      return false;
    } else if (command_line::get_arg(vm, command_line::arg_version))  {
      std::cout << CRYPTONOTE_NAME << " wallet v" << PROJECT_VERSION_LONG;
      return false;
    }

    auto parser = po::command_line_parser(argc, argv).options(desc_params).positional(positional_options);
    po::store(parser.run(), vm);
    po::notify(vm);
    return true;
  });

  if (!r)
    return 1;
  
  auto modulePath = Common::NativePathToGeneric(argv[0]);
  auto cfgLogFile = Common::NativePathToGeneric(command_line::get_arg(vm, arg_log_file));

  if (cfgLogFile.empty()) {
    cfgLogFile = Common::ReplaceExtenstion(modulePath, ".log");
    } else {
    if (!Common::HasParentPath(cfgLogFile)) {
      cfgLogFile = Common::CombinePath(Common::GetPathDirectory(modulePath), cfgLogFile);
    }
  }

  //set up logging options
  Level logLevel = DEBUGGING;

  if (command_line::has_arg(vm, arg_log_level)) {
    logLevel = static_cast<Level>(command_line::get_arg(vm, arg_log_level));
  }

  logManager.configure(buildLoggerConfiguration(logLevel, cfgLogFile));

  logger(INFO, BRIGHT_WHITE) << CRYPTONOTE_NAME << " wallet v" << PROJECT_VERSION_LONG;

  CryptoNote::Currency currency = CryptoNote::CurrencyBuilder(logManager).
    testnet(command_line::get_arg(vm, arg_testnet)).currency();

  if (command_line::has_arg(vm, Tools::wallet_rpc_server::arg_rpc_bind_port)) {
    //runs wallet with rpc interface
    if (!command_line::has_arg(vm, arg_wallet_file)) {
      logger(ERROR, BRIGHT_RED) << "Wallet file not set.";
      return 1;
    }

    if (!command_line::has_arg(vm, arg_daemon_address)) {
      logger(ERROR, BRIGHT_RED) << "Daemon address not set.";
      return 1;
    }

    if (!command_line::has_arg(vm, arg_password)) {
      logger(ERROR, BRIGHT_RED) << "Wallet password not set.";
      return 1;
    }

    std::string wallet_file = command_line::get_arg(vm, arg_wallet_file);
    std::string wallet_password = command_line::get_arg(vm, arg_password);
    std::string daemon_address = command_line::get_arg(vm, arg_daemon_address);
    std::string daemon_host = command_line::get_arg(vm, arg_daemon_host);
    uint16_t daemon_port = command_line::get_arg(vm, arg_daemon_port);
    if (daemon_host.empty())
      daemon_host = "localhost";
    if (!daemon_port)
      daemon_port = RPC_DEFAULT_PORT;

    if (!daemon_address.empty()) {
      if (!parseUrlAddress(daemon_address, daemon_host, daemon_port)) {
        logger(ERROR, BRIGHT_RED) << "failed to parse daemon address: " << daemon_address;
        return 1;
      }
    }

    std::unique_ptr<INode> node(new NodeRpcProxy(daemon_host, daemon_port));

    std::promise<std::error_code> errorPromise;
    std::future<std::error_code> error = errorPromise.get_future();
    auto callback = [&errorPromise](std::error_code e) {errorPromise.set_value(e); };
    node->init(callback);
    if (error.get()) {
      logger(ERROR, BRIGHT_RED) << ("failed to init NodeRPCProxy");
      return 1;
    }

    std::unique_ptr<IWalletLegacy> wallet(new WalletLegacy(currency, *node.get(), logManager));

    std::string walletFileName;
    try  {
      walletFileName = ::tryToOpenWalletOrLoadKeysOrThrow(logger, wallet, wallet_file, wallet_password);

      logger(INFO) << "available balance: " << currency.formatAmount(wallet->actualBalance()) <<
      ", locked amount: " << currency.formatAmount(wallet->pendingBalance());

      logger(INFO, BRIGHT_GREEN) << "Loaded ok";
    } catch (const std::exception& e)  {
      logger(ERROR, BRIGHT_RED) << "Wallet initialize failed: " << e.what();
      return 1;
    }

    Tools::wallet_rpc_server wrpc(dispatcher, logManager, *wallet, *node, currency, walletFileName);

    if (!wrpc.init(vm)) {
      logger(ERROR, BRIGHT_RED) << "Failed to initialize wallet rpc server";
      return 1;
    }

    Tools::SignalHandler::install([&wrpc, &wallet] {
      wrpc.send_stop_signal();
    });

    logger(INFO) << "Starting wallet rpc server";
    wrpc.run();
    logger(INFO) << "Stopped wallet rpc server";
    
    try {
      logger(INFO) << "Storing wallet...";
      CryptoNote::WalletHelper::storeWallet(*wallet, walletFileName);
      logger(INFO, BRIGHT_GREEN) << "Stored ok";
    } catch (const std::exception& e) {
      logger(ERROR, BRIGHT_RED) << "Failed to store wallet: " << e.what();
      return 1;
    }
  } else {
    //runs wallet with console interface
    CryptoNote::simple_wallet wal(dispatcher, currency, logManager);
    
    if (!wal.init(vm)) {
      logger(ERROR, BRIGHT_RED) << "Failed to initialize wallet"; 
      return 1; 
    }

    std::vector<std::string> command = command_line::get_arg(vm, arg_command);
    if (!command.empty())
      wal.process_command(command);

    Tools::SignalHandler::install([&wal] {
      wal.stop();
    });
    
    wal.run();

    if (!wal.deinit()) {
      logger(ERROR, BRIGHT_RED) << "Failed to close wallet";
    } else {
      logger(INFO) << "Wallet closed";
    }
  }
  return 1;
  //CATCH_ENTRY_L0("main", 1);
}
