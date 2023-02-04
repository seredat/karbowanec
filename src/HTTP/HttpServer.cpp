// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2014-2016 XDN developers
// Copyright (c) 2016-2018 Karbowanec developers
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

#include "HttpServer.h"
#include <boost/scope_exit.hpp>

#include <Common/base64.hpp>
#include <Common/StringTools.h>
#include <HTTP/HttpParser.h>
#include <System/InterruptedException.h>
#include <System/TcpStream.h>
#include <System/Ipv4Address.h>

using namespace Logging;

namespace {
	void fillUnauthorizedResponse(CryptoNote::HttpResponse& response) {
		response.setStatus(CryptoNote::HttpResponse::STATUS_401);
		response.addHeader("WWW-Authenticate", "Basic realm=\"RPC\"");
		response.addHeader("Content-Type", "text/plain");
		response.setBody("Authorization required");
	}
}

namespace CryptoNote {

HttpServer::HttpServer(System::Dispatcher& dispatcher, Logging::ILogger& log)
  : m_dispatcher(dispatcher), workingContextGroup(dispatcher), logger(log, "HttpServer") {

}

void HttpServer::start(const std::string& address, uint16_t port, const std::string& user, const std::string& password) {
  m_listener = System::TcpListener(m_dispatcher, System::Ipv4Address(address), port);
  workingContextGroup.spawn(std::bind(&HttpServer::acceptLoop, this));
  
  if (!user.empty() || !password.empty()) {
    m_credentials = base64::encode(Common::asBinaryArray(user + ":" + password));
  }
}

void HttpServer::stop() {
  workingContextGroup.interrupt();
  workingContextGroup.wait();
}

void HttpServer::acceptLoop() {
  try {
    System::TcpConnection connection; 
    bool accepted = false;

    while (!accepted) {
      try {
        connection = m_listener.accept();
        accepted = true;
      } catch (System::InterruptedException&) {
        throw;
      } catch (std::exception&) {
        // try again
      }
    }

    m_connections.insert(&connection);
    BOOST_SCOPE_EXIT_ALL(this, &connection) { 
      m_connections.erase(&connection); };

    workingContextGroup.spawn(std::bind(&HttpServer::acceptLoop, this));

    //auto addr = connection.getPeerAddressAndPort();
    auto addr = std::pair<System::Ipv4Address, uint16_t>(static_cast<System::Ipv4Address>(0), 0);
    try {
      addr = connection.getPeerAddressAndPort();
    }
    catch (std::runtime_error&) {
      logger(WARNING) << "Could not get IP of connection";
    }

    logger(DEBUGGING) << "Incoming connection from " << addr.first.toDottedDecimal() << ":" << addr.second;

    System::TcpStreambuf streambuf(connection);
    std::iostream stream(&streambuf);
    HttpParser parser;

    for (;;) {
      HttpRequest req;
      HttpResponse resp;
      resp.addHeader("Access-Control-Allow-Origin", "*");

      parser.receiveRequest(stream, req);
      if (authenticate(req)) {
        processRequest(req, resp);
      }
      else {
        logger(WARNING) << "Authorization required " << addr.first.toDottedDecimal() << ":" << addr.second;
        fillUnauthorizedResponse(resp);
      }

      stream << resp;
      stream.flush();

      if (stream.peek() == std::iostream::traits_type::eof()) {
        break;
      }
    }

    logger(DEBUGGING) << "Closing connection from " << addr.first.toDottedDecimal() << ":" << addr.second << " total=" << m_connections.size();

  } catch (System::InterruptedException&) {
  } catch (std::exception& e) {
    logger(DEBUGGING) << "Connection error: " << e.what();
  }
}

bool HttpServer::authenticate(const HttpRequest& request) const {
	if (!m_credentials.empty()) {
		auto headerIt = request.getHeaders().find("authorization");
		if (headerIt == request.getHeaders().end()) {
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

size_t HttpServer::get_connections_count() const {
	return m_connections.size();
}

}
