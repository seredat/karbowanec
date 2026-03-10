// Copyright (c) 2016-2026, The Karbo developers
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

#include "LMDBBlockchainDB.h"

#include <cassert>
#include <cstring>
#include <stdexcept>

#include "Common/FileSystemShim.h"

namespace CryptoNote {

// ─── Big-endian helpers ────────────────────────────────────────────────────

void LMDBBlockchainDB::encBE32(uint8_t* out, uint32_t v) {
  out[0] = (v >> 24) & 0xFF;
  out[1] = (v >> 16) & 0xFF;
  out[2] = (v >>  8) & 0xFF;
  out[3] =  v        & 0xFF;
}

void LMDBBlockchainDB::encBE64(uint8_t* out, uint64_t v) {
  out[0] = (v >> 56) & 0xFF;
  out[1] = (v >> 48) & 0xFF;
  out[2] = (v >> 40) & 0xFF;
  out[3] = (v >> 32) & 0xFF;
  out[4] = (v >> 24) & 0xFF;
  out[5] = (v >> 16) & 0xFF;
  out[6] = (v >>  8) & 0xFF;
  out[7] =  v        & 0xFF;
}

uint32_t LMDBBlockchainDB::decBE32(const uint8_t* b) {
  return (uint32_t(b[0]) << 24) | (uint32_t(b[1]) << 16) |
         (uint32_t(b[2]) <<  8) |  uint32_t(b[3]);
}

uint64_t LMDBBlockchainDB::decBE64(const uint8_t* b) {
  return (uint64_t(b[0]) << 56) | (uint64_t(b[1]) << 48) |
         (uint64_t(b[2]) << 40) | (uint64_t(b[3]) << 32) |
         (uint64_t(b[4]) << 24) | (uint64_t(b[5]) << 16) |
         (uint64_t(b[6]) <<  8) |  uint64_t(b[7]);
}

// ─── Internal helpers ──────────────────────────────────────────────────────

void LMDBBlockchainDB::checkRc(int rc, const char* op) const {
  if (rc == 0)            return;
  if (rc == MDB_MAP_FULL) throw LMDBMapFullException();
  throw std::runtime_error(std::string("LMDB ") + op + ": " + mdb_strerror(rc));
}

void LMDBBlockchainDB::openDb(MDB_txn* setupTxn, const char* name,
                               unsigned int flags, MDB_dbi& dbi) {
  int rc = mdb_dbi_open(setupTxn, name, flags | MDB_CREATE, &dbi);
  checkRc(rc, name);
}

MDB_txn* LMDBBlockchainDB::activeTxn() const {
  if (m_writeTxn) return m_writeTxn;
  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(m_env, nullptr, MDB_RDONLY, &txn);
  checkRc(rc, "activeTxn:begin");
  return txn;
}

void LMDBBlockchainDB::endReadTxn(MDB_txn* txn) {
  mdb_txn_abort(txn);
}

// ─── Constructor / Destructor ──────────────────────────────────────────────

LMDBBlockchainDB::LMDBBlockchainDB() = default;

LMDBBlockchainDB::~LMDBBlockchainDB() {
  close();
}

// ─── open / close / clear ─────────────────────────────────────────────────

bool LMDBBlockchainDB::open(const std::string& path) {
  int rc = mdb_env_create(&m_env);
  if (rc) return false;

  mdb_env_set_maxdbs(m_env, 16);

  // Start at 1 GB; grows as needed via resizeMap()
  mdb_env_set_mapsize(m_env, size_t(1) << 30);

  // MDB_WRITEMAP: use writable memory-map for fast writes
  // MDB_MAPASYNC: async flush (best-effort; still crash-safe at OS level)
  // MDB_NORDAHEAD: no read-ahead (saves RAM on large chains)
  unsigned int envFlags = MDB_WRITEMAP | MDB_MAPASYNC | MDB_NORDAHEAD;

  // Ensure the directory exists
  rc = mdb_env_open(m_env, path.c_str(), envFlags, 0664);
  if (rc) {
    mdb_env_close(m_env);
    m_env = nullptr;
    return false;
  }

  // Open all named databases in a setup transaction
  MDB_txn* setupTxn = nullptr;
  rc = mdb_txn_begin(m_env, nullptr, 0, &setupTxn);
  if (rc) {
    mdb_env_close(m_env);
    m_env = nullptr;
    return false;
  }

  try {
    openDb(setupTxn, "block_meta",        0,                         m_dbiBlockMeta);
    openDb(setupTxn, "block_data",        0,                         m_dbiBlockData);
    openDb(setupTxn, "tx_entries",        0,                         m_dbiTxEntries);
    openDb(setupTxn, "hash_to_height",    0,                         m_dbiHashToHeight);
    openDb(setupTxn, "hashing_blobs",     0,                         m_dbiHashingBlobs);
    openDb(setupTxn, "spent_keys",        0,                         m_dbiSpentKeys);
    openDb(setupTxn, "tx_indices",        0,                         m_dbiTxIndices);
    openDb(setupTxn, "key_outputs",       0,                         m_dbiKeyOutputs);
    openDb(setupTxn, "key_output_counts", 0,                         m_dbiKeyOutputCounts);
    openDb(setupTxn, "payment_id_idx",    MDB_DUPSORT | MDB_DUPFIXED, m_dbiPaymentIdIdx);
    openDb(setupTxn, "timestamp_idx",     0,                         m_dbiTimestampIdx);
    openDb(setupTxn, "gen_tx_idx",        0,                         m_dbiGenTxIdx);
  } catch (...) {
    mdb_txn_abort(setupTxn);
    mdb_env_close(m_env);
    m_env = nullptr;
    return false;
  }

  rc = mdb_txn_commit(setupTxn);
  if (rc) {
    mdb_env_close(m_env);
    m_env = nullptr;
    return false;
  }

  return true;
}

void LMDBBlockchainDB::close() {
  if (m_writeTxn) {
    mdb_txn_abort(m_writeTxn);
    m_writeTxn = nullptr;
  }
  if (m_env) {
    mdb_env_close(m_env);
    m_env = nullptr;
  }
}

void LMDBBlockchainDB::clear() {
  assert(!m_writeTxn && "clear() called with open write txn");

  MDB_txn* txn = nullptr;
  int rc = mdb_txn_begin(m_env, nullptr, 0, &txn);
  checkRc(rc, "clear:begin");

  auto dropDb = [&](MDB_dbi dbi) {
    int r = mdb_drop(txn, dbi, 0);  // 0 = empty, not delete
    checkRc(r, "clear:drop");
  };

  dropDb(m_dbiBlockMeta);
  dropDb(m_dbiBlockData);
  dropDb(m_dbiTxEntries);
  dropDb(m_dbiHashToHeight);
  dropDb(m_dbiHashingBlobs);
  dropDb(m_dbiSpentKeys);
  dropDb(m_dbiTxIndices);
  dropDb(m_dbiKeyOutputs);
  dropDb(m_dbiKeyOutputCounts);
  dropDb(m_dbiPaymentIdIdx);
  dropDb(m_dbiTimestampIdx);
  dropDb(m_dbiGenTxIdx);

  rc = mdb_txn_commit(txn);
  checkRc(rc, "clear:commit");
}

// ─── Transaction control ───────────────────────────────────────────────────

void LMDBBlockchainDB::beginWriteTxn() {
  assert(!m_writeTxn && "beginWriteTxn called with already-open txn");
  int rc = mdb_txn_begin(m_env, nullptr, 0, &m_writeTxn);
  checkRc(rc, "beginWriteTxn");
}

void LMDBBlockchainDB::commitTxn() {
  assert(m_writeTxn && "commitTxn called without active txn");
  int rc = mdb_txn_commit(m_writeTxn);
  m_writeTxn = nullptr;
  checkRc(rc, "commitTxn");
}

void LMDBBlockchainDB::abortTxn() {
  if (m_writeTxn) {
    mdb_txn_abort(m_writeTxn);
    m_writeTxn = nullptr;
  }
}

// ─── resizeMap ────────────────────────────────────────────────────────────

void LMDBBlockchainDB::resizeMap() {
  assert(!m_writeTxn && "resizeMap called with active write txn");
  MDB_envinfo info{};
  mdb_env_info(m_env, &info);
  size_t newSize = info.me_mapsize * 2;
  int rc = mdb_env_set_mapsize(m_env, newSize);
  checkRc(rc, "resizeMap");
}

// ─── getChainHeight ───────────────────────────────────────────────────────

uint32_t LMDBBlockchainDB::getChainHeight() const {
  MDB_txn* txn  = activeTxn();
  bool ownTxn   = !m_writeTxn;

  MDB_cursor* cur = nullptr;
  mdb_cursor_open(txn, m_dbiBlockMeta, &cur);

  MDB_val key{}, val{};
  int rc = mdb_cursor_get(cur, &key, &val, MDB_LAST);
  mdb_cursor_close(cur);
  if (ownTxn) endReadTxn(txn);

  if (rc == MDB_NOTFOUND) return 0;
  checkRc(rc, "getChainHeight");
  return decBE32(static_cast<const uint8_t*>(key.mv_data)) + 1;
}

// ─── block_meta ───────────────────────────────────────────────────────────

bool LMDBBlockchainDB::putBlockMeta(uint32_t height, const DbBlockMeta& meta) {
  assert(m_writeTxn);
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf};
  MDB_val v = {sizeof(DbBlockMeta), const_cast<DbBlockMeta*>(&meta)};
  int rc = mdb_put(m_writeTxn, m_dbiBlockMeta, &k, &v, 0);
  checkRc(rc, "putBlockMeta");
  return true;
}

bool LMDBBlockchainDB::getBlockMeta(uint32_t height, DbBlockMeta& meta) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf}, v{};
  int rc = mdb_get(txn, m_dbiBlockMeta, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getBlockMeta");
  std::memcpy(&meta, v.mv_data, sizeof(DbBlockMeta));
  return true;
}

bool LMDBBlockchainDB::removeLastBlockMeta() {
  assert(m_writeTxn);
  MDB_cursor* cur = nullptr;
  mdb_cursor_open(m_writeTxn, m_dbiBlockMeta, &cur);
  MDB_val k{}, v{};
  int rc = mdb_cursor_get(cur, &k, &v, MDB_LAST);
  if (rc == MDB_NOTFOUND) { mdb_cursor_close(cur); return false; }
  checkRc(rc, "removeLastBlockMeta:get");
  rc = mdb_cursor_del(cur, 0);
  mdb_cursor_close(cur);
  checkRc(rc, "removeLastBlockMeta:del");
  return true;
}

// ─── block_data ───────────────────────────────────────────────────────────

bool LMDBBlockchainDB::putBlockData(uint32_t height, const uint8_t* data, size_t size) {
  assert(m_writeTxn);
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf};
  MDB_val v = {size, const_cast<uint8_t*>(data)};
  int rc = mdb_put(m_writeTxn, m_dbiBlockData, &k, &v, 0);
  checkRc(rc, "putBlockData");
  return true;
}

bool LMDBBlockchainDB::getBlockData(uint32_t height, std::vector<uint8_t>& out) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf}, v{};
  int rc = mdb_get(txn, m_dbiBlockData, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getBlockData");
  out.assign(static_cast<const uint8_t*>(v.mv_data),
             static_cast<const uint8_t*>(v.mv_data) + v.mv_size);
  return true;
}

bool LMDBBlockchainDB::removeBlockData(uint32_t height) {
  assert(m_writeTxn);
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf};
  int rc = mdb_del(m_writeTxn, m_dbiBlockData, &k, nullptr);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removeBlockData");
  return true;
}

// ─── tx_entries ───────────────────────────────────────────────────────────

bool LMDBBlockchainDB::putTxEntry(uint32_t height, uint16_t txIdx,
                                   const uint8_t* data, size_t size) {
  assert(m_writeTxn);
  uint8_t keyBuf[6];
  encBE32(keyBuf, height);
  keyBuf[4] = (txIdx >> 8) & 0xFF;
  keyBuf[5] =  txIdx       & 0xFF;
  MDB_val k = {6, keyBuf};
  MDB_val v = {size, const_cast<uint8_t*>(data)};
  int rc = mdb_put(m_writeTxn, m_dbiTxEntries, &k, &v, 0);
  checkRc(rc, "putTxEntry");
  return true;
}

bool LMDBBlockchainDB::getTxEntry(uint32_t height, uint16_t txIdx,
                                   std::vector<uint8_t>& out) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  uint8_t keyBuf[6];
  encBE32(keyBuf, height);
  keyBuf[4] = (txIdx >> 8) & 0xFF;
  keyBuf[5] =  txIdx       & 0xFF;
  MDB_val k = {6, keyBuf}, v{};
  int rc = mdb_get(txn, m_dbiTxEntries, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getTxEntry");
  out.assign(static_cast<const uint8_t*>(v.mv_data),
             static_cast<const uint8_t*>(v.mv_data) + v.mv_size);
  return true;
}

bool LMDBBlockchainDB::removeTxEntriesForBlock(uint32_t height, uint16_t txCount) {
  assert(m_writeTxn);
  for (uint16_t i = 0; i < txCount; ++i) {
    uint8_t keyBuf[6];
    encBE32(keyBuf, height);
    keyBuf[4] = (i >> 8) & 0xFF;
    keyBuf[5] =  i       & 0xFF;
    MDB_val k = {6, keyBuf};
    int rc = mdb_del(m_writeTxn, m_dbiTxEntries, &k, nullptr);
    if (rc != 0 && rc != MDB_NOTFOUND) checkRc(rc, "removeTxEntriesForBlock");
  }
  return true;
}

// ─── hash_to_height ───────────────────────────────────────────────────────

bool LMDBBlockchainDB::putHashHeight(const Crypto::Hash& hash, uint32_t height) {
  assert(m_writeTxn);
  MDB_val k = {sizeof(hash), const_cast<Crypto::Hash*>(&hash)};
  uint8_t valBuf[4]; encBE32(valBuf, height);
  MDB_val v = {4, valBuf};
  int rc = mdb_put(m_writeTxn, m_dbiHashToHeight, &k, &v, 0);
  checkRc(rc, "putHashHeight");
  return true;
}

bool LMDBBlockchainDB::getHashHeight(const Crypto::Hash& hash, uint32_t& height) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  MDB_val k = {sizeof(hash), const_cast<Crypto::Hash*>(&hash)}, v{};
  int rc = mdb_get(txn, m_dbiHashToHeight, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getHashHeight");
  height = decBE32(static_cast<const uint8_t*>(v.mv_data));
  return true;
}

bool LMDBBlockchainDB::removeHashHeight(const Crypto::Hash& hash) {
  assert(m_writeTxn);
  MDB_val k = {sizeof(hash), const_cast<Crypto::Hash*>(&hash)};
  int rc = mdb_del(m_writeTxn, m_dbiHashToHeight, &k, nullptr);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removeHashHeight");
  return true;
}

// ─── hashing_blobs ────────────────────────────────────────────────────────

bool LMDBBlockchainDB::putHashingBlob(uint32_t height, const uint8_t* data, size_t size) {
  assert(m_writeTxn);
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf};
  MDB_val v = {size, const_cast<uint8_t*>(data)};
  int rc = mdb_put(m_writeTxn, m_dbiHashingBlobs, &k, &v, 0);
  checkRc(rc, "putHashingBlob");
  return true;
}

bool LMDBBlockchainDB::getHashingBlob(uint32_t height, std::vector<uint8_t>& out) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf}, v{};
  int rc = mdb_get(txn, m_dbiHashingBlobs, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getHashingBlob");
  out.assign(static_cast<const uint8_t*>(v.mv_data),
             static_cast<const uint8_t*>(v.mv_data) + v.mv_size);
  return true;
}

bool LMDBBlockchainDB::removeHashingBlob(uint32_t height) {
  assert(m_writeTxn);
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf};
  int rc = mdb_del(m_writeTxn, m_dbiHashingBlobs, &k, nullptr);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removeHashingBlob");
  return true;
}

// ─── spent_keys ───────────────────────────────────────────────────────────

bool LMDBBlockchainDB::putSpentKey(const Crypto::KeyImage& ki, uint32_t blockHeight) {
  assert(m_writeTxn);
  MDB_val k = {sizeof(ki), const_cast<Crypto::KeyImage*>(&ki)};
  uint8_t valBuf[4]; encBE32(valBuf, blockHeight);
  MDB_val v = {4, valBuf};
  int rc = mdb_put(m_writeTxn, m_dbiSpentKeys, &k, &v, 0);
  checkRc(rc, "putSpentKey");
  return true;
}

bool LMDBBlockchainDB::hasSpentKey(const Crypto::KeyImage& ki) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  MDB_val k = {sizeof(ki), const_cast<Crypto::KeyImage*>(&ki)}, v{};
  int rc = mdb_get(txn, m_dbiSpentKeys, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "hasSpentKey");
  return true;
}

bool LMDBBlockchainDB::getSpentKeyHeight(const Crypto::KeyImage& ki, uint32_t& height) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  MDB_val k = {sizeof(ki), const_cast<Crypto::KeyImage*>(&ki)}, v{};
  int rc = mdb_get(txn, m_dbiSpentKeys, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getSpentKeyHeight");
  height = decBE32(static_cast<const uint8_t*>(v.mv_data));
  return true;
}

bool LMDBBlockchainDB::removeSpentKey(const Crypto::KeyImage& ki) {
  assert(m_writeTxn);
  MDB_val k = {sizeof(ki), const_cast<Crypto::KeyImage*>(&ki)};
  int rc = mdb_del(m_writeTxn, m_dbiSpentKeys, &k, nullptr);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removeSpentKey");
  return true;
}

// ─── tx_indices ───────────────────────────────────────────────────────────

bool LMDBBlockchainDB::putTxIndex(const Crypto::Hash& txHash,
                                   uint32_t block, uint16_t txSlot) {
  assert(m_writeTxn);
  MDB_val k = {sizeof(txHash), const_cast<Crypto::Hash*>(&txHash)};
  // Value: [u32 block BE][u16 txSlot BE] = 6 bytes
  uint8_t valBuf[6];
  encBE32(valBuf,     block);
  valBuf[4] = (txSlot >> 8) & 0xFF;
  valBuf[5] =  txSlot       & 0xFF;
  MDB_val v = {6, valBuf};
  int rc = mdb_put(m_writeTxn, m_dbiTxIndices, &k, &v, 0);
  checkRc(rc, "putTxIndex");
  return true;
}

bool LMDBBlockchainDB::getTxIndex(const Crypto::Hash& txHash,
                                   uint32_t& block, uint16_t& txSlot) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  MDB_val k = {sizeof(txHash), const_cast<Crypto::Hash*>(&txHash)}, v{};
  int rc = mdb_get(txn, m_dbiTxIndices, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getTxIndex");
  const uint8_t* b = static_cast<const uint8_t*>(v.mv_data);
  block  = decBE32(b);
  txSlot = (uint16_t(b[4]) << 8) | b[5];
  return true;
}

bool LMDBBlockchainDB::removeTxIndex(const Crypto::Hash& txHash) {
  assert(m_writeTxn);
  MDB_val k = {sizeof(txHash), const_cast<Crypto::Hash*>(&txHash)};
  int rc = mdb_del(m_writeTxn, m_dbiTxIndices, &k, nullptr);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removeTxIndex");
  return true;
}

// ─── key_outputs ──────────────────────────────────────────────────────────
// Key:   {amount_BE(8), globalIdx_BE(4)} = 12 bytes
// Value: {block_BE(4), txSlot_BE(2), outIdx_BE(2)} = 8 bytes

bool LMDBBlockchainDB::putKeyOutput(uint64_t amount, uint32_t globalIdx,
                                     uint32_t block, uint16_t txSlot, uint16_t outIdx) {
  assert(m_writeTxn);
  uint8_t keyBuf[12];
  encBE64(keyBuf,   amount);
  encBE32(keyBuf+8, globalIdx);
  MDB_val k = {12, keyBuf};

  uint8_t valBuf[8];
  encBE32(valBuf,   block);
  valBuf[4] = (txSlot >> 8) & 0xFF;
  valBuf[5] =  txSlot       & 0xFF;
  valBuf[6] = (outIdx >> 8) & 0xFF;
  valBuf[7] =  outIdx       & 0xFF;
  MDB_val v = {8, valBuf};

  int rc = mdb_put(m_writeTxn, m_dbiKeyOutputs, &k, &v, 0);
  checkRc(rc, "putKeyOutput");

  // Update count
  uint8_t cntKeyBuf[8]; encBE64(cntKeyBuf, amount);
  MDB_val ck = {8, cntKeyBuf}, cv{};
  uint32_t oldCount = 0;
  if (mdb_get(m_writeTxn, m_dbiKeyOutputCounts, &ck, &cv) == 0) {
    oldCount = decBE32(static_cast<const uint8_t*>(cv.mv_data));
  }
  uint8_t newCntBuf[4]; encBE32(newCntBuf, oldCount + 1);
  MDB_val nv = {4, newCntBuf};
  rc = mdb_put(m_writeTxn, m_dbiKeyOutputCounts, &ck, &nv, 0);
  checkRc(rc, "putKeyOutput:count");
  return true;
}

bool LMDBBlockchainDB::getKeyOutput(uint64_t amount, uint32_t globalIdx,
                                     uint32_t& block, uint16_t& txSlot, uint16_t& outIdx) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  uint8_t keyBuf[12];
  encBE64(keyBuf,   amount);
  encBE32(keyBuf+8, globalIdx);
  MDB_val k = {12, keyBuf}, v{};
  int rc = mdb_get(txn, m_dbiKeyOutputs, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getKeyOutput");
  const uint8_t* b = static_cast<const uint8_t*>(v.mv_data);
  block  = decBE32(b);
  txSlot = (uint16_t(b[4]) << 8) | b[5];
  outIdx = (uint16_t(b[6]) << 8) | b[7];
  return true;
}

uint32_t LMDBBlockchainDB::getKeyOutputCount(uint64_t amount) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  uint8_t keyBuf[8]; encBE64(keyBuf, amount);
  MDB_val k = {8, keyBuf}, v{};
  int rc = mdb_get(txn, m_dbiKeyOutputCounts, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return 0;
  checkRc(rc, "getKeyOutputCount");
  return decBE32(static_cast<const uint8_t*>(v.mv_data));
}

bool LMDBBlockchainDB::removeLastKeyOutput(uint64_t amount) {
  assert(m_writeTxn);
  uint8_t cntKeyBuf[8]; encBE64(cntKeyBuf, amount);
  MDB_val ck = {8, cntKeyBuf}, cv{};
  int rc = mdb_get(m_writeTxn, m_dbiKeyOutputCounts, &ck, &cv);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removeLastKeyOutput:getCount");
  uint32_t count = decBE32(static_cast<const uint8_t*>(cv.mv_data));
  if (count == 0) return false;

  // Delete the last output entry
  uint8_t keyBuf[12];
  encBE64(keyBuf,   amount);
  encBE32(keyBuf+8, count - 1);
  MDB_val k = {12, keyBuf};
  rc = mdb_del(m_writeTxn, m_dbiKeyOutputs, &k, nullptr);
  checkRc(rc, "removeLastKeyOutput:del");

  // Update count
  if (count == 1) {
    mdb_del(m_writeTxn, m_dbiKeyOutputCounts, &ck, nullptr);
  } else {
    uint8_t newCntBuf[4]; encBE32(newCntBuf, count - 1);
    MDB_val nv = {4, newCntBuf};
    rc = mdb_put(m_writeTxn, m_dbiKeyOutputCounts, &ck, &nv, 0);
    checkRc(rc, "removeLastKeyOutput:updateCount");
  }
  return true;
}

// ─── payment_id_idx (DUPSORT) ─────────────────────────────────────────────

bool LMDBBlockchainDB::putPaymentId(const Crypto::Hash& paymentId, const Crypto::Hash& txHash) {
  assert(m_writeTxn);
  MDB_val k = {sizeof(paymentId), const_cast<Crypto::Hash*>(&paymentId)};
  MDB_val v = {sizeof(txHash),   const_cast<Crypto::Hash*>(&txHash)};
  int rc = mdb_put(m_writeTxn, m_dbiPaymentIdIdx, &k, &v, MDB_NODUPDATA);
  if (rc == MDB_KEYEXIST) return true; // already present
  checkRc(rc, "putPaymentId");
  return true;
}

bool LMDBBlockchainDB::getPaymentIdTxHashes(const Crypto::Hash& paymentId,
                                              std::vector<Crypto::Hash>& txHashes) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;

  MDB_cursor* cur = nullptr;
  mdb_cursor_open(txn, m_dbiPaymentIdIdx, &cur);

  MDB_val k = {sizeof(paymentId), const_cast<Crypto::Hash*>(&paymentId)}, v{};
  int rc = mdb_cursor_get(cur, &k, &v, MDB_SET);
  while (rc == 0) {
    Crypto::Hash h;
    std::memcpy(&h, v.mv_data, sizeof(h));
    txHashes.push_back(h);
    rc = mdb_cursor_get(cur, &k, &v, MDB_NEXT_DUP);
  }
  mdb_cursor_close(cur);
  if (ownTxn) endReadTxn(txn);
  return !txHashes.empty();
}

bool LMDBBlockchainDB::removePaymentId(const Crypto::Hash& paymentId, const Crypto::Hash& txHash) {
  assert(m_writeTxn);
  MDB_val k = {sizeof(paymentId), const_cast<Crypto::Hash*>(&paymentId)};
  MDB_val v = {sizeof(txHash),   const_cast<Crypto::Hash*>(&txHash)};
  int rc = mdb_del(m_writeTxn, m_dbiPaymentIdIdx, &k, &v);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removePaymentId");
  return true;
}

// ─── timestamp_idx ────────────────────────────────────────────────────────
// Key: {uint64_t timestamp BE (8), Crypto::Hash (32)} = 40 bytes
// Value: empty

bool LMDBBlockchainDB::putTimestamp(uint64_t timestamp, const Crypto::Hash& blockHash) {
  assert(m_writeTxn);
  uint8_t keyBuf[40];
  encBE64(keyBuf, timestamp);
  std::memcpy(keyBuf + 8, blockHash.data, 32);
  MDB_val k = {40, keyBuf};
  MDB_val v = {0, nullptr};
  int rc = mdb_put(m_writeTxn, m_dbiTimestampIdx, &k, &v, 0);
  checkRc(rc, "putTimestamp");
  return true;
}

bool LMDBBlockchainDB::getBlockHashesByTimestampRange(uint64_t tsBegin, uint64_t tsEnd,
                                                       uint32_t limit,
                                                       std::vector<Crypto::Hash>& hashes,
                                                       uint32_t& totalInRange) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;

  MDB_cursor* cur = nullptr;
  mdb_cursor_open(txn, m_dbiTimestampIdx, &cur);

  totalInRange = 0;
  uint8_t seekBuf[40];
  encBE64(seekBuf, tsBegin);
  std::memset(seekBuf + 8, 0, 32);  // smallest hash for this timestamp

  MDB_val k = {40, seekBuf}, v{};
  int rc = mdb_cursor_get(cur, &k, &v, MDB_SET_RANGE);
  while (rc == 0) {
    uint64_t ts = decBE64(static_cast<const uint8_t*>(k.mv_data));
    if (ts > tsEnd) break;
    totalInRange++;
    if (hashes.size() < limit) {
      Crypto::Hash h;
      std::memcpy(h.data, static_cast<const uint8_t*>(k.mv_data) + 8, 32);
      hashes.push_back(h);
    }
    rc = mdb_cursor_get(cur, &k, &v, MDB_NEXT);
  }
  mdb_cursor_close(cur);
  if (ownTxn) endReadTxn(txn);
  return true;
}

bool LMDBBlockchainDB::removeTimestamp(uint64_t timestamp, const Crypto::Hash& blockHash) {
  assert(m_writeTxn);
  uint8_t keyBuf[40];
  encBE64(keyBuf, timestamp);
  std::memcpy(keyBuf + 8, blockHash.data, 32);
  MDB_val k = {40, keyBuf};
  int rc = mdb_del(m_writeTxn, m_dbiTimestampIdx, &k, nullptr);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removeTimestamp");
  return true;
}

// ─── gen_tx_idx ───────────────────────────────────────────────────────────

bool LMDBBlockchainDB::putGeneratedTxCount(uint32_t height, uint64_t count) {
  assert(m_writeTxn);
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf};
  uint8_t valBuf[8]; encBE64(valBuf, count);
  MDB_val v = {8, valBuf};
  int rc = mdb_put(m_writeTxn, m_dbiGenTxIdx, &k, &v, 0);
  checkRc(rc, "putGeneratedTxCount");
  return true;
}

bool LMDBBlockchainDB::getGeneratedTxCount(uint32_t height, uint64_t& count) const {
  MDB_txn* txn = activeTxn();
  bool ownTxn  = !m_writeTxn;
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf}, v{};
  int rc = mdb_get(txn, m_dbiGenTxIdx, &k, &v);
  if (ownTxn) endReadTxn(txn);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "getGeneratedTxCount");
  count = decBE64(static_cast<const uint8_t*>(v.mv_data));
  return true;
}

bool LMDBBlockchainDB::removeGeneratedTxCount(uint32_t height) {
  assert(m_writeTxn);
  uint8_t keyBuf[4]; encBE32(keyBuf, height);
  MDB_val k = {4, keyBuf};
  int rc = mdb_del(m_writeTxn, m_dbiGenTxIdx, &k, nullptr);
  if (rc == MDB_NOTFOUND) return false;
  checkRc(rc, "removeGeneratedTxCount");
  return true;
}

} // namespace CryptoNote
