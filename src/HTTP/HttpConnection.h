// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2026, The Karbo developers
//
// This file is part of Karbo.

#pragma once

#include <memory>
#include <string>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <System/Dispatcher.h>
#include <System/TcpConnection.h>

namespace CryptoNote {

class HttpConnection {
public:
  enum ConnectionType {
    Plain,
    SSL
  };

  HttpConnection();
  HttpConnection(const HttpConnection&) = delete;
  HttpConnection(HttpConnection&& other) noexcept;
  ~HttpConnection();
  
  HttpConnection& operator=(const HttpConnection&) = delete;
  HttpConnection& operator=(HttpConnection&& other) noexcept;

  size_t read(uint8_t* data, size_t size);
  size_t write(const uint8_t* data, size_t size);
  
  System::TcpConnection& getPlainConnection();
  
private:
  friend class HttpListener;
  friend class HttpConnector;

  // For plain connections
  HttpConnection(System::Dispatcher& dispatcher, System::TcpConnection&& conn);
  
  // For SSL connections  
  HttpConnection(System::Dispatcher& dispatcher, 
                 System::TcpConnection&& conn,
                 boost::asio::ssl::context& sslContext,
                 bool isServer);

  void performSslHandshake(bool isServer);
  
  System::Dispatcher* dispatcher{nullptr};
  System::TcpConnection tcpConnection;
  
  ConnectionType connType{Plain};
  std::unique_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>> sslStream;
  boost::asio::ssl::context* sslContext{nullptr};
};

} // namespace CryptoNote
