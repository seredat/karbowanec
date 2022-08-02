// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2014 The Boolberry
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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "Common/int-util.h"
#include "crypto/hash.h"
#include "CryptoNoteConfig.h"
#include "Difficulty.h"

namespace CryptoNote {

  using std::uint64_t;
  using std::vector;

#if defined(__SIZEOF_INT128__)

  static inline void mul(uint64_t a, uint64_t b, uint64_t& low, uint64_t& high) {
    typedef unsigned __int128 uint128_t;
    uint128_t res = (uint128_t)a * (uint128_t)b;
    low = (uint64_t)res;
    high = (uint64_t)(res >> 64);
  }

#else

  static inline void mul(uint64_t a, uint64_t b, uint64_t& low, uint64_t& high) {
    low = mul128(a, b, &high);
  }

#endif

  static inline bool cadd(uint64_t a, uint64_t b) {
    return a + b < a;
  }

  static inline bool cadc(uint64_t a, uint64_t b, bool c) {
    return a + b < a || (c && a + b == (uint64_t)-1);
  }

  bool check_hash_64(const Crypto::Hash& hash, uint64_t difficulty) {
    uint64_t low, high, top, cur;
    // First check the highest word, this will most likely fail for a random hash.
    mul(swap64le(((const uint64_t*)&hash)[3]), difficulty, top, high);
    if (high != 0) {
      return false;
    }
    mul(swap64le(((const uint64_t*)&hash)[0]), difficulty, low, cur);
    mul(swap64le(((const uint64_t*)&hash)[1]), difficulty, low, high);
    bool carry = cadd(cur, low);
    cur = high;
    mul(swap64le(((const uint64_t*)&hash)[2]), difficulty, low, high);
    carry = cadc(cur, low, carry);
    carry = cadc(high, top, carry);
    return !carry;
  }

#if defined(_MSC_VER)
#ifdef max
#undef max
#endif
#endif

  const CryptoNote::Difficulty max64bit(std::numeric_limits<std::uint64_t>::max());
  const boost::multiprecision::uint256_t max128bit(std::numeric_limits<boost::multiprecision::uint128_t>::max());
  const boost::multiprecision::uint512_t max256bit(std::numeric_limits<boost::multiprecision::uint256_t>::max());

#define FORCE_FULL_128_BITS

  bool check_hash_128(const Crypto::Hash& hash, CryptoNote::Difficulty difficulty) {
#ifndef FORCE_FULL_128_BITS
    // fast check
    if (difficulty >= max64bit && ((const uint64_t*)&hash)[3] > 0)
      return false;
#endif
    // usual slow check
    boost::multiprecision::uint512_t hashVal = 0;
#ifdef FORCE_FULL_128_BITS
    for (int i = 0; i < 4; i++) { // highest word is zero
#else
    for (int i = 1; i < 4; i++) { // highest word is zero
#endif
      hashVal <<= 64;
      hashVal |= swap64le(((const uint64_t*)&hash)[3 - i]);
    }
    return hashVal * difficulty <= max256bit;
  }

  bool check_hash(const Crypto::Hash & hash, CryptoNote::Difficulty difficulty) {
    if (difficulty <= max64bit) // if can convert to small difficulty - do it
      return check_hash_64(hash, difficulty.convert_to<std::uint64_t>());
    else
      return check_hash_128(hash, difficulty);
  }

}