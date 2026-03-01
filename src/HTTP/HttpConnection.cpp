// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2026, The Karbo developers
//
// This file is part of Karbo.

#include "HttpConnection.h"
#include <System/InterruptedException.h>
#include <stdexcept>

namespace CryptoNote {

HttpConnection::HttpConnection() : dispatcher(nullptr), connType(Plain) {
}

HttpConnection::HttpConnection(System::Dispatcher& d, System::TcpConnection&& conn)
  : dispatcher(&d), tcpConnection(std::move(conn)), connType(Plain) {
}

HttpConnection::HttpConnection(System::Dispatcher& d,
                               System::TcpConnection&& conn,
                               boost::asio::ssl::context& sslCtx,
                               bool isServer)
  : dispatcher(&d), 
    tcpConnection(std::move(conn)),
    connType(SSL),
    sslContext(&sslCtx) {
  
  // Note: We can't directly wrap TcpConnection's socket in SSL stream
  // because TcpConnection uses shared_ptr to socket
  // This is a limitation - in production you'd need to refactor TcpConnection
  // or maintain the socket reference differently
  throw std::runtime_error("SSL connections require socket access - implementation pending");
}

HttpConnection::HttpConnection(HttpConnection&& other) noexcept
  : dispatcher(other.dispatcher),
    tcpConnection(std::move(other.tcpConnection)),
    connType(other.connType),
    sslStream(std::move(other.sslStream)),
    sslContext(other.sslContext) {
  
  other.dispatcher = nullptr;
  other.connType = Plain;
  other.sslContext = nullptr;
}

HttpConnection::~HttpConnection() {
}

HttpConnection& HttpConnection::operator=(HttpConnection&& other) noexcept {
  if (this != &other) {
    dispatcher = other.dispatcher;
    tcpConnection = std::move(other.tcpConnection);
    connType = other.connType;
    sslStream = std::move(other.sslStream);
    sslContext = other.sslContext;
    
    other.dispatcher = nullptr;
    other.connType = Plain;
    other.sslContext = nullptr;
  }
  return *this;
}

size_t HttpConnection::read(uint8_t* data, size_t size) {
  if (connType == SSL && sslStream) {
    // SSL read
    System::NativeContext* context = dispatcher->getCurrentContext();
    
    boost::system::error_code ec;
    size_t transferred = 0;
    
    sslStream->async_read_some(
      boost::asio::buffer(data, size),
      [context, &ec, &transferred, this](const boost::system::error_code& error, size_t bytes) {
        ec = error;
        transferred = bytes;
        dispatcher->pushContext(context);
      });
    
    dispatcher->dispatch();
    
    if (ec) {
      if (ec == boost::asio::error::interrupted) {
        throw System::InterruptedException();
      }
      throw std::runtime_error("SSL read failed: " + ec.message());
    }
    
    return transferred;
  } else {
    // Plain TCP read
    return tcpConnection.read(data, size);
  }
}

size_t HttpConnection::write(const uint8_t* data, size_t size) {
  if (connType == SSL && sslStream) {
    // SSL write
    System::NativeContext* context = dispatcher->getCurrentContext();
    
    boost::system::error_code ec;
    size_t transferred = 0;
    
    boost::asio::async_write(
      *sslStream,
      boost::asio::buffer(data, size),
      [context, &ec, &transferred, this](const boost::system::error_code& error, size_t bytes) {
        ec = error;
        transferred = bytes;
        dispatcher->pushContext(context);
      });
    
    dispatcher->dispatch();
    
    if (ec) {
      if (ec == boost::asio::error::interrupted) {
        throw System::InterruptedException();
      }
      throw std::runtime_error("SSL write failed: " + ec.message());
    }
    
    return transferred;
  } else {
    // Plain TCP write
    return tcpConnection.write(data, size);
  }
}

System::TcpConnection& HttpConnection::getPlainConnection() {
  return tcpConnection;
}

void HttpConnection::performSslHandshake(bool isServer) {
  if (!sslStream) {
    throw std::runtime_error("SSL stream not initialized");
  }
  
  System::NativeContext* context = dispatcher->getCurrentContext();
  boost::system::error_code ec;
  
  if (isServer) {
    sslStream->async_handshake(
      boost::asio::ssl::stream_base::server,
      [context, &ec, this](const boost::system::error_code& error) {
        ec = error;
        dispatcher->pushContext(context);
      });
  } else {
    sslStream->async_handshake(
      boost::asio::ssl::stream_base::client,
      [context, &ec, this](const boost::system::error_code& error) {
        ec = error;
        dispatcher->pushContext(context);
      });
  }
  
  dispatcher->dispatch();
  
  if (ec) {
    if (ec == boost::asio::error::interrupted) {
      throw System::InterruptedException();
    }
    throw std::runtime_error("SSL handshake failed: " + ec.message());
  }
}

} // namespace CryptoNote
