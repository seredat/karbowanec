// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2016-2026, The Karbo developers
//
// This file is part of Karbo.

#include "SslTcpStreambuf.h"
#include <System/InterruptedException.h>
#include <stdexcept>

namespace System {

SslTcpStreambuf::SslTcpStreambuf(TcpConnection& conn, 
                                 boost::asio::ssl::context& context,
                                 bool server)
  : connection(conn)
  , sslStream(connection.getSocket(), context)
  , handshakeDone(false)
  , isServer(server)
{
  setg(readBuf, readBuf, readBuf);
  setp(writeBuf, writeBuf + sizeof(writeBuf));
}

SslTcpStreambuf::~SslTcpStreambuf() {
  try {
    sync();
    boost::system::error_code ec;
    sslStream.shutdown(ec);
  } catch (...) {
    // Suppress exceptions in destructor
  }
}

void SslTcpStreambuf::handshake() {
  if (handshakeDone) {
    return;
  }

  boost::system::error_code ec;
  
  if (isServer) {
    sslStream.handshake(boost::asio::ssl::stream_base::server, ec);
  } else {
    sslStream.handshake(boost::asio::ssl::stream_base::client, ec);
  }
  
  if (ec) {
    throw std::runtime_error("SSL handshake failed: " + ec.message());
  }
  
  handshakeDone = true;
}

SslTcpStreambuf::int_type SslTcpStreambuf::overflow(int_type ch) {
  if (ch == traits_type::eof()) {
    return traits_type::eof();
  }

  if (pptr() == epptr()) {
    // Buffer full, flush it
    if (sync() == -1) {
      return traits_type::eof();
    }
  }

  *pptr() = static_cast<char>(ch);
  pbump(1);
  return ch;
}

SslTcpStreambuf::int_type SslTcpStreambuf::underflow() {
  if (gptr() < egptr()) {
    return traits_type::to_int_type(*gptr());
  }

  if (!handshakeDone) {
    handshake();
  }

  boost::system::error_code ec;
  size_t bytesRead = sslStream.read_some(boost::asio::buffer(readBuf, sizeof(readBuf)), ec);
  
  if (ec) {
    if (ec == boost::asio::error::eof) {
      return traits_type::eof();
    }
    throw std::runtime_error("SSL read failed: " + ec.message());
  }

  if (bytesRead == 0) {
    return traits_type::eof();
  }

  setg(readBuf, readBuf, readBuf + bytesRead);
  return traits_type::to_int_type(*gptr());
}

int SslTcpStreambuf::sync() {
  if (pbase() == pptr()) {
    return 0; // Nothing to write
  }

  if (!handshakeDone) {
    handshake();
  }

  size_t bytesToWrite = pptr() - pbase();
  boost::system::error_code ec;
  
  size_t bytesWritten = boost::asio::write(sslStream, 
    boost::asio::buffer(writeBuf, bytesToWrite), ec);
  
  if (ec) {
    throw std::runtime_error("SSL write failed: " + ec.message());
  }

  if (bytesWritten != bytesToWrite) {
    throw std::runtime_error("SSL partial write");
  }

  setp(writeBuf, writeBuf + sizeof(writeBuf));
  return 0;
}

} // namespace System
