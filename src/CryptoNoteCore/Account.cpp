// Copyright (c) 2012-2016, The CryptoNote developers, The Bytecoin developers
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

#include "Account.h"
#include "CryptoNoteSerialization.h"
#include "crypto/crypto.h"
extern "C"
{
#include "crypto/keccak.h"
}

namespace CryptoNote {
//-----------------------------------------------------------------
AccountBase::AccountBase() {
  setNull();
}
//-----------------------------------------------------------------
void AccountBase::setNull() {
  m_keys = AccountKeys();
}
//-----------------------------------------------------------------
void AccountBase::generate() {

  Crypto::generate_keys(m_keys.address.spendPublicKey, m_keys.spendSecretKey);
  Crypto::generate_keys(m_keys.address.viewPublicKey, m_keys.viewSecretKey);

  m_creation_timestamp = time(NULL);

}

//-----------------------------------------------------------------
void AccountBase::generateDeterministic() {
  Crypto::generate_keys(m_keys.address.spendPublicKey, m_keys.spendSecretKey);

  // Derive viewSecretKey: sc_reduce32(keccak(spendSecretKey)). Controls viewPublicKey / address. UNCHANGED.
  Crypto::SecretKey viewKeySeed;
  keccak((uint8_t *)&m_keys.spendSecretKey, sizeof(Crypto::SecretKey),
         (uint8_t *)&viewKeySeed, sizeof(viewKeySeed));
  Crypto::generate_deterministic_keys(m_keys.address.viewPublicKey, m_keys.viewSecretKey, viewKeySeed);

  // Derive auditSecretKey: sc_reduce32(keccak("view_seed"||spendSecretKey)).
  // Domain-separated from viewSecretKey; independent capability for outgoing tx tracking.
  m_keys.auditSecretKey = computeAuditSecretKey(m_keys.spendSecretKey);

  m_creation_timestamp = time(NULL);
}

//-----------------------------------------------------------------
void AccountBase::generateViewFromSpend(const Crypto::SecretKey &spendSecret, Crypto::SecretKey &viewSecret, Crypto::PublicKey &viewPublic) {
  Crypto::SecretKey viewKeySeed;
  keccak((uint8_t *)&spendSecret, sizeof(spendSecret), (uint8_t *)&viewKeySeed, sizeof(viewKeySeed));

  Crypto::generate_deterministic_keys(viewPublic, viewSecret, viewKeySeed);
}

void AccountBase::generateViewFromSpend(const Crypto::SecretKey &spendSecret, Crypto::SecretKey &viewSecret) {
  /* If we don't need the pub key */
  Crypto::PublicKey unused_dummy_variable;
  generateViewFromSpend(spendSecret, viewSecret, unused_dummy_variable);
}

Crypto::SecretKey AccountBase::computeAuditSecretKey(const Crypto::SecretKey &spendSecretKey) {
  // auditSecretKey = sc_reduce32(keccak("view_seed" || spendSecretKey))
  // The "view_seed" domain separator ensures independence from viewSecretKey = sc_reduce32(keccak(spendSecretKey)).
  static const char domain[] = "view_seed";     // 9 chars, no null terminator included
  const size_t domainLen = sizeof(domain) - 1;
  uint8_t input[sizeof(domain) - 1 + sizeof(Crypto::SecretKey)];
  memcpy(input, domain, domainLen);
  memcpy(input + domainLen, &spendSecretKey, sizeof(spendSecretKey));

  Crypto::SecretKey rawHash;
  keccak(input, sizeof(input), (uint8_t *)&rawHash, sizeof(rawHash));

  // Apply sc_reduce32 via generate_deterministic_keys (discarding the dummy public key).
  Crypto::PublicKey dummyPub;
  Crypto::SecretKey auditKey;
  Crypto::generate_deterministic_keys(dummyPub, auditKey, rawHash);
  return auditKey;
}

//-----------------------------------------------------------------
const AccountKeys &AccountBase::getAccountKeys() const {
  return m_keys;
}

void AccountBase::setAccountKeys(const AccountKeys &keys) {
  m_keys = keys;
}
//-----------------------------------------------------------------

void AccountBase::serialize(ISerializer &s) {
  s(m_keys, "m_keys");
  s(m_creation_timestamp, "m_creation_timestamp");

  // Auto-upgrade: if auditSecretKey was not loaded from file (old wallet) but spend key is present,
  // try to derive auditSecretKey. Only set it if the wallet uses deterministic view key derivation.
  if (s.type() == ISerializer::INPUT && m_keys.auditSecretKey == NULL_SECRET_KEY
      && m_keys.spendSecretKey != NULL_SECRET_KEY) {
    // Verify this is a deterministic wallet: sc_reduce32(keccak(s))*G must equal viewPublicKey.
    Crypto::SecretKey viewKeySeed;
    keccak((uint8_t *)&m_keys.spendSecretKey, sizeof(Crypto::SecretKey),
           (uint8_t *)&viewKeySeed, sizeof(viewKeySeed));
    Crypto::PublicKey derivedViewPublic;
    Crypto::SecretKey derivedViewSecret;
    Crypto::generate_deterministic_keys(derivedViewPublic, derivedViewSecret, viewKeySeed);
    if (derivedViewPublic == m_keys.address.viewPublicKey) {
      m_keys.auditSecretKey = computeAuditSecretKey(m_keys.spendSecretKey);
    }
    // If viewPublicKey doesn't match, it's a non-deterministic wallet; auditSecretKey stays null.
  }
}
}
