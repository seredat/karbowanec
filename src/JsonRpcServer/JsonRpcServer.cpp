// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright(c) 2014 - 2017 XDN - project developers
// Copyright(c) 2018 - 2023 The Karbo developers
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

#include "JsonRpcServer.h"

#include <fstream>
#include <future>
#include <system_error>
#include <memory>
#include <sstream>
#include "HTTP/HttpParserErrorCodes.h"

#include <System/TcpConnection.h>
#include <System/TcpListener.h>
#include <System/TcpStream.h>
#include <System/Ipv4Address.h>
#include "HTTP/HttpParser.h"
#include "HTTP/HttpResponse.h"

#include "Rpc/JsonRpc.h"
#include "Common/base64.hpp"
#include "Common/JsonValue.h"
#include "Common/StringTools.h"
#include "Serialization/JsonInputValueSerializer.h"
#include "Serialization/JsonOutputStreamSerializer.h"

namespace CryptoNote {

JsonRpcServer::JsonRpcServer(System::Dispatcher& sys, System::Event& stopEvent, Logging::ILogger& loggerGroup) :
  m_dispatcher(sys),
  stopEvent(stopEvent),
  logger(loggerGroup, "JsonRpcServer"),
  m_enable_ssl(false)
{
}

JsonRpcServer::~JsonRpcServer() {
  stop();
}

void JsonRpcServer::start(const std::string& bindAddress, uint16_t bindPort, uint16_t bindPortSSL) {
  if (m_enable_ssl) {
    m_workers.emplace_back(std::unique_ptr<System::RemoteContext<void>>(
      new System::RemoteContext<void>(m_dispatcher, std::bind(&JsonRpcServer::listen_ssl, this, bindAddress, bindPortSSL)))
    );
  }

  m_workers.emplace_back(std::unique_ptr<System::RemoteContext<void>>(
    new System::RemoteContext<void>(m_dispatcher, std::bind(&JsonRpcServer::listen, this, bindAddress, bindPort)))
  );

  stopEvent.wait();
}

void JsonRpcServer::stop() {
  if (m_enable_ssl) {
    https->stop();
  }

  http->stop();

  m_dispatcher.remoteSpawn([this]
  {
    stopEvent.set();
  });

  m_workers.clear();
}

void JsonRpcServer::init(const std::string& chain_file, const std::string& key_file, bool server_ssl_enable){
  m_chain_file = chain_file;
  m_key_file = key_file;
  m_enable_ssl = server_ssl_enable;

  http = new httplib::Server();

  http->Post(".*", [this](const httplib::Request& req, httplib::Response& res) {
    processRequest(req, res);
  });

  if (server_ssl_enable) {
    https = new httplib::SSLServer(m_chain_file.c_str(), m_key_file.c_str());

    https->Post(".*", [this](const httplib::Request& req, httplib::Response& res) {
      processRequest(req, res);
    });
  }
}

void JsonRpcServer::setAuth(const std::string& user, const std::string& password) {
  if (!user.empty() || !password.empty()) {
    m_credentials = base64::encode(Common::asBinaryArray(user + ":" + password));
  }
}

bool JsonRpcServer::authenticate(const httplib::Request& request) const {
  if (!m_credentials.empty()) {
    auto headerIt = request.headers.find("authorization");
    if (headerIt == request.headers.end()) {
      return false;
    }

    if (headerIt->second.substr(0, 6) != "Basic ") {
      return false;
    }

    if (headerIt->second.substr(6) != m_credentials) {
      return false;
    }
  }

  return true;
}

void JsonRpcServer::listen(const std::string address, const uint16_t port) {
  if (!http->listen(address.c_str(), port)) {
    logger(Logging::WARNING) << "Could not bind service to " << address << ":" << port
      << "\nIs another service using this address and port?\n";
  }
}

void JsonRpcServer::listen_ssl(const std::string address, const uint16_t port) {
  if (!https->listen(address.c_str(), port)) {
    logger(Logging::WARNING) << "Could not bind service to " << address << ":" << port
      << "\nIs another service using this address and port?\n";
  }
}

void JsonRpcServer::processRequest(const httplib::Request& req, httplib::Response& resp) {
  try {

    if (!authenticate(req)) {
      logger(Logging::WARNING) << "Authorization required";
      resp.status = 401;
      resp.set_header("WWW-Authenticate", "Basic realm=\"RPC\"");
      resp.set_content("Authorization required", "text/plain; charset=UTF-8");

      return;
    }

    if (req.path == "/json_rpc") {
      std::istringstream jsonInputStream(req.body);
      Common::JsonValue jsonRpcRequest;
      Common::JsonValue jsonRpcResponse(Common::JsonValue::OBJECT);

      try {
        jsonInputStream >> jsonRpcRequest;
      } catch (std::runtime_error&) {
        logger(Logging::DEBUGGING) << "Couldn't parse request: \"" << req.body << "\"";
        makeJsonParsingErrorResponse(jsonRpcResponse);
        
        resp.set_header("Access-Control-Allow-Origin", "*");
        resp.status = 200;
        resp.set_content(jsonRpcResponse.toString(), "application/json");

        return;
      }

      processJsonRpcRequest(jsonRpcRequest, jsonRpcResponse);

      std::ostringstream jsonOutputStream;
      jsonOutputStream << jsonRpcResponse;

      resp.status = 200;
      resp.set_content(jsonOutputStream.str(), "application/json");

    } else {
      logger(Logging::WARNING) << "Requested url \"" << req.path << "\" is not found";
      resp.status = 404;
      return;
    }
  } catch (std::exception& e) {
    logger(Logging::WARNING) << "Error while processing http request: " << e.what();
    resp.status = 500;
  }
}

void JsonRpcServer::prepareJsonResponse(const Common::JsonValue& req, Common::JsonValue& resp) {
  using Common::JsonValue;

  if (req.contains("id")) {
    resp.insert("id", req("id"));
  }
  
  resp.insert("jsonrpc", "2.0");
}

void JsonRpcServer::makeErrorResponse(const std::error_code& ec, Common::JsonValue& resp) {
  using Common::JsonValue;

  JsonValue error(JsonValue::OBJECT);

  JsonValue code;
  code = static_cast<int64_t>(CryptoNote::JsonRpc::errParseError); //Application specific error code

  JsonValue message;
  message = ec.message();

  JsonValue data(JsonValue::OBJECT);
  JsonValue appCode;
  appCode = static_cast<int64_t>(ec.value());
  data.insert("application_code", appCode);

  error.insert("code", code);
  error.insert("message", message);
  error.insert("data", data);

  resp.insert("error", error);
}

void JsonRpcServer::makeGenericErrorReponse(Common::JsonValue& resp, const char* what, int errorCode) {
  using Common::JsonValue;

  JsonValue error(JsonValue::OBJECT);

  JsonValue code;
  code = static_cast<int64_t>(errorCode);

  std::string msg;
  if (what) {
    msg = what;
  } else {
    msg = "Unknown application error";
  }

  JsonValue message;
  message = msg;

  error.insert("code", code);
  error.insert("message", message);

  resp.insert("error", error);

}

void JsonRpcServer::makeMethodNotFoundResponse(Common::JsonValue& resp) {
  using Common::JsonValue;

  JsonValue error(JsonValue::OBJECT);

  JsonValue code;
  code = static_cast<int64_t>(CryptoNote::JsonRpc::errMethodNotFound); //ambigous declaration of JsonValue::operator= (between int and JsonValue)

  JsonValue message;
  message = "Method not found";

  error.insert("code", code);
  error.insert("message", message);

  resp.insert("error", error);
}

void JsonRpcServer::fillJsonResponse(const Common::JsonValue& v, Common::JsonValue& resp) {
  resp.insert("result", v);
}

void JsonRpcServer::makeJsonParsingErrorResponse(Common::JsonValue& resp) {
  using Common::JsonValue;

  resp = JsonValue(JsonValue::OBJECT);
  resp.insert("jsonrpc", "2.0");
  resp.insert("id", nullptr);

  JsonValue error(JsonValue::OBJECT);
  JsonValue code;
  code = static_cast<int64_t>(CryptoNote::JsonRpc::errParseError); //ambigous declaration of JsonValue::operator= (between int and JsonValue)

  JsonValue message = "Parse error";

  error.insert("code", code);
  error.insert("message", message);

  resp.insert("error", error);
}

}
