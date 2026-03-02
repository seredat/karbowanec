// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2026, The Karbo developers
//
// This file is part of Karbo.

#pragma once

#include <iostream>
#include <boost/asio/ssl.hpp>
#include <System/TcpConnection.h>

namespace System {

class SslTcpStreambuf : public std::streambuf {
public:
  explicit SslTcpStreambuf(TcpConnection& connection, 
                           boost::asio::ssl::context& context, 
                           bool isServer = false);
  
  SslTcpStreambuf(const SslTcpStreambuf&) = delete;
  SslTcpStreambuf& operator=(const SslTcpStreambuf&) = delete;
  
  ~SslTcpStreambuf() override;

  void handshake();

private:
  int_type overflow(int_type ch) override;
  int_type underflow() override;
  int sync() override;

  TcpConnection& connection;
  boost::asio::ssl::stream<boost::asio::ip::tcp::socket&> sslStream;
  bool handshakeDone;
  bool isServer;
  
  char readBuf[4096];
  char writeBuf[4096];
};

} // namespace System
