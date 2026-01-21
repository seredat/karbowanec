// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2014 The Boolberry
// Copyright (c) 2014-2019, The Monero Project
// Copyright (c) 2016-2022, The Karbowanec developers
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

#pragma once

#include <cstdint>
#include <vector>

#include <boost/multiprecision/cpp_int.hpp>

#include "crypto/hash.h"

namespace CryptoNote
{
    typedef boost::multiprecision::uint128_t Difficulty;

    bool check_hash(const Crypto::Hash &hash, Difficulty difficulty);
    bool check_hash_64(const Crypto::Hash& hash, uint64_t difficulty);
    bool check_hash_128(const Crypto::Hash& hash, Difficulty difficulty);
}
