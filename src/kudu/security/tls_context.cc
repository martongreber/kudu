// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/security/tls_context.h"

#include <openssl/crypto.h>
#ifndef OPENSSL_NO_ECDH
#include <openssl/ec.h> // IWYU pragma: keep
#endif
#include <openssl/err.h>
#include <openssl/obj_mac.h> // IWYU pragma: keep
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/init.h"
#include "kudu/security/security_flags.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"
#include "kudu/util/user.h"

// Hard code OpenSSL flag values from OpenSSL 1.0.1e[1][2] when compiling
// against OpenSSL 1.0.0 and below. We detect when running against a too-old
// version of OpenSSL using these definitions at runtime so that Kudu has full
// functionality when run against a new OpenSSL version, even if it's compiled
// against an older version.
//
// [1]: https://github.com/openssl/openssl/blob/OpenSSL_1_0_1e/ssl/ssl.h#L605-L609
// [2]: https://github.com/openssl/openssl/blob/OpenSSL_1_0_1e/ssl/tls1.h#L166-L172
#ifndef SSL_OP_NO_TLSv1
#define SSL_OP_NO_TLSv1 0x04000000U
#endif
#ifndef SSL_OP_NO_TLSv1_1
#define SSL_OP_NO_TLSv1_1 0x10000000U
#endif
#ifndef SSL_OP_NO_TLSv1_2
#define SSL_OP_NO_TLSv1_2 0x08000000U
#endif
#ifndef SSL_OP_NO_TLSv1_3
#define SSL_OP_NO_TLSv1_3 0x20000000U
#endif
#ifndef TLS1_1_VERSION
#define TLS1_1_VERSION 0x0302
#endif
#ifndef TLS1_2_VERSION
#define TLS1_2_VERSION 0x0303
#endif
#ifndef TLS1_3_VERSION
#define TLS1_3_VERSION 0x0304
#endif

using kudu::security::ca::CertRequestGenerator;
using std::nullopt;
using std::optional;
using std::shared_lock;
using std::string;
using std::vector;
using strings::Substitute;

DEFINE_int32(ipki_server_key_size, 2048,
             "the number of bits for server cert's private key. The server cert "
             "is used for TLS connections to and from clients and other servers.");
TAG_FLAG(ipki_server_key_size, experimental);

DEFINE_int32(openssl_security_level_override, -1,
             "if set to 0 or greater, overrides the security level for OpenSSL "
             "library of versions 1.1.0 and newer; for test purposes only");
TAG_FLAG(openssl_security_level_override, hidden);
TAG_FLAG(openssl_security_level_override, unsafe);

namespace kudu {
namespace security {

template<> struct SslTypeTraits<SSL> {
  static constexpr auto kFreeFunc = &SSL_free;
};
template<> struct SslTypeTraits<X509_STORE_CTX> {
  static constexpr auto kFreeFunc = &X509_STORE_CTX_free;
};

namespace {

constexpr const char* const kTLSv1 = "TLSv1";
constexpr const char* const kTLSv1_1 = "TLSv1.1";
constexpr const char* const kTLSv1_2 = "TLSv1.2";
constexpr const char* const kTLSv1_3 = "TLSv1.3";

Status CheckMaxSupportedTlsVersion(int tls_version, const char* tls_version_str) {
  // OpenSSL 1.1.1 and newer supports all of the TLS versions we care about, so
  // the below check is only necessary in older versions of OpenSSL.
#if OPENSSL_VERSION_NUMBER < 0x10101000L
  auto max_supported_tls_version = SSLv23_method()->version;
  DCHECK_GE(max_supported_tls_version, TLS1_VERSION);

  if (max_supported_tls_version < tls_version) {
    return Status::InvalidArgument(
        Substitute("invalid minimum TLS protocol version (--rpc_tls_min_protocol): "
                   "this platform does not support $0", tls_version_str));
  }
#endif
  return Status::OK();
}

} // anonymous namespace

TlsContext::TlsContext()
    : tls_ciphers_(SecurityDefaults::kDefaultTlsCiphers),
      tls_ciphersuites_(SecurityDefaults::kDefaultTlsCipherSuites),
      tls_min_protocol_(SecurityDefaults::kDefaultTlsMinVersion),
      lock_(RWMutex::Priority::PREFER_READING),
      trusted_cert_count_(0),
      has_cert_(false),
      is_external_cert_(false) {
  security::InitializeOpenSSL();
}

TlsContext::TlsContext(std::string tls_ciphers,
                       std::string tls_ciphersuites,
                       std::string tls_min_protocol,
                       std::vector<std::string> tls_excluded_protocols)
    : tls_ciphers_(std::move(tls_ciphers)),
      tls_ciphersuites_(std::move(tls_ciphersuites)),
      tls_min_protocol_(std::move(tls_min_protocol)),
      tls_excluded_protocols_(std::move(tls_excluded_protocols)),
      lock_(RWMutex::Priority::PREFER_READING),
      trusted_cert_count_(0),
      has_cert_(false),
      is_external_cert_(false) {
  security::InitializeOpenSSL();
}

Status TlsContext::Init() {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(!ctx_);

  // NOTE: 'SSLv23 method' sounds like it would enable only SSLv2 and SSLv3, but in fact
  // this is a sort of wildcard which enables all methods (including TLSv1 and later).
  // We explicitly disable SSLv2 and SSLv3 below so that only TLS methods remain.
  // See the discussion on https://trac.torproject.org/projects/tor/ticket/11598 for more
  // info.
  ctx_ = ssl_make_unique(SSL_CTX_new(SSLv23_method()));
  if (!ctx_) {
    return Status::RuntimeError("failed to create TLS context", GetOpenSSLErrors());
  }
  auto* ctx = ctx_.get();
  SSL_CTX_set_mode(ctx,
                   SSL_MODE_AUTO_RETRY |
                   SSL_MODE_ENABLE_PARTIAL_WRITE |
                   SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

  // Disable SSLv2 and SSLv3 which are vulnerable to various issues such as POODLE.
  // We support versions back to TLSv1.0 since OpenSSL on RHEL 6.4 and earlier does not
  // not support TLSv1.1 or later.
  //
  // Disable SSL/TLS compression to free up CPU resources and be less prone
  // to attacks exploiting the compression feature:
  //   https://tools.ietf.org/html/rfc7525#section-3.3
  auto options = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION;

  if (iequals(tls_min_protocol_, kTLSv1_3)) {
    RETURN_NOT_OK(CheckMaxSupportedTlsVersion(TLS1_3_VERSION, kTLSv1_3));
    options |= SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1_2;
  } else if (iequals(tls_min_protocol_, kTLSv1_2)) {
    RETURN_NOT_OK(CheckMaxSupportedTlsVersion(TLS1_2_VERSION, kTLSv1_2));
    options |= SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
  } else if (iequals(tls_min_protocol_, kTLSv1_1)) {
    RETURN_NOT_OK(CheckMaxSupportedTlsVersion(TLS1_1_VERSION, kTLSv1_1));
    options |= SSL_OP_NO_TLSv1;
  } else if (!iequals(tls_min_protocol_, kTLSv1)) {
    return Status::InvalidArgument("unknown TLS protocol", tls_min_protocol_);
  }

#if OPENSSL_VERSION_NUMBER > 0x1010007fL
  // KUDU-1926: disable TLS/SSL renegotiation.
  // See https://www.openssl.org/docs/man1.1.0/man3/SSL_set_options.html for
  // details. SSL_OP_NO_RENEGOTIATION option was back-ported from 1.1.1-dev to
  // 1.1.0h, so this is a best-effort approach if the binary compiled with
  // newer as per information in the CHANGES file for
  // 'Changes between 1.1.0g and 1.1.0h [27 Mar 2018]':
  //     Note that if an application built against 1.1.0h headers (or above) is
  //     run using an older version of 1.1.0 (prior to 1.1.0h) then the option
  //     will be accepted but nothing will happen, i.e. renegotiation will
  //     not be prevented.
  // The case of OpenSSL 1.0.2 and prior is handled by the InitiateHandshake()
  // method.
  options |= SSL_OP_NO_RENEGOTIATION;
#endif

  for (const auto& proto : tls_excluded_protocols_) {
    if (iequals(proto, kTLSv1_3)) {
      options |= SSL_OP_NO_TLSv1_3;
      continue;
    }
    if (iequals(proto, kTLSv1_2)) {
      options |= SSL_OP_NO_TLSv1_2;
      continue;
    }
    if (iequals(proto, kTLSv1_1)) {
      options |= SSL_OP_NO_TLSv1_1;
      continue;
    }
    if (iequals(proto, kTLSv1)) {
      options |= SSL_OP_NO_TLSv1;
      continue;
    }
    return Status::InvalidArgument("unknown TLS protocol", proto);
  }

  SSL_CTX_set_options(ctx, options);

  // Disable the TLS session cache on both the client and server sides. In Kudu
  // RPC, connections are not re-established based on TLS sessions anyway. Every
  // connection attempt from a client to a server results in a new connection
  // negotiation. Disabling the TLS session cache helps to avoid using extra
  // resources to store TLS session information and running the automatic check
  // for expired sessions every 255 connections, as mentioned at
  // https://www.openssl.org/docs/manmaster/man3/SSL_CTX_set_session_cache_mode.html
  SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);

  // The sequence of SSL_CTX_set_ciphersuites() and SSL_CTX_set_cipher_list()
  // calls below is essential to make sure the TLS engine ends up with usable,
  // non-empty set of ciphers in case of early 1.1.1 releases of OpenSSL
  // (like OpenSSL 1.1.1 shipped with Ubuntu 18).
  //
  // The SSL_CTX_set_ciphersuites() call cares only about TLSv1.3 ciphers, and
  // those might be none. From the other side, the implementation of
  // SSL_CTX_set_cipher_list() verifies that the overall result list of ciphers
  // is valid and usable, reporting an error otherwise.
  //
  // If the sequence is reversed, no error would be reported from
  // TlsContext::Init() in case of empty list of ciphers for some early-1.1.1
  // releases of OpenSSL. That's because SSL_CTX_set_cipher_list() would see
  // a non-empty list of default TLSv1.3 ciphers when given an empty list of
  // TLSv1.2 ciphers, and SSL_CTX_set_ciphersuites() would allow an empty set
  // of TLSv1.3 ciphers in a subsequent call.

#if OPENSSL_VERSION_NUMBER >= 0x10101000L
  // Set TLSv1.3 ciphers.
  OPENSSL_RET_NOT_OK(
      SSL_CTX_set_ciphersuites(ctx, tls_ciphersuites_.c_str()),
      Substitute("failed to set TLSv1.3 ciphers: $0", tls_ciphersuites_));
#endif

  // It's OK to configure pre-TLSv1.3 ciphers even if all pre-TLSv1.3 protocols
  // are disabled. At least, SSL_CTX_set_cipher_list() call doesn't report
  // any errors.
  OPENSSL_RET_NOT_OK(SSL_CTX_set_cipher_list(ctx, tls_ciphers_.c_str()),
                     Substitute("failed to set TLS ciphers: $0", tls_ciphers_));

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  // OpenSSL 1.1 and newer supports the 'security level' concept:
  //   https://www.openssl.org/docs/man1.1.0/man3/SSL_CTX_get_security_level.html
  //
  // For some Linux distros (e.g. RHEL/CentOS 8.1), the OpenSSL library is
  // configured to use security level 2 by default, which tightens requirements
  // on the number of bits used for various algorithms and ciphers (e.g., RSA
  // key should be at least 2048 bits long with security level 2). However,
  // in Kudu test environment we strive to use shorter keys because it saves
  // us time running our tests.
  auto level = SSL_CTX_get_security_level(ctx_.get());
  VLOG(1) << Substitute("OpenSSL security level is $0", level);
  if (FLAGS_openssl_security_level_override >= 0) {
    SSL_CTX_set_security_level(ctx_.get(), FLAGS_openssl_security_level_override);
    level = SSL_CTX_get_security_level(ctx_.get());
    VLOG(1) << Substitute("OpenSSL security level reset to $0", level);
  }
#endif

  // Enable ECDH curves. For OpenSSL 1.1.0 and up, this is done automatically.
#ifndef OPENSSL_NO_ECDH
#if OPENSSL_VERSION_NUMBER < 0x10002000L
  // OpenSSL 1.0.1 and below only support setting a single ECDH curve at once.
  // We choose prime256v1 because it's the first curve listed in the "modern
  // compatibility" section of the Mozilla Server Side TLS recommendations,
  // accessed Feb. 2017.
  c_unique_ptr<EC_KEY> ecdh { EC_KEY_new_by_curve_name(NID_X9_62_prime256v1), &EC_KEY_free };
  OPENSSL_RET_IF_NULL(ecdh, "failed to create prime256v1 curve");
  OPENSSL_RET_NOT_OK(SSL_CTX_set_tmp_ecdh(ctx_.get(), ecdh.get()),
                     "failed to set ECDH curve");
#elif OPENSSL_VERSION_NUMBER < 0x10100000L
  // OpenSSL 1.0.2 provides the set_ecdh_auto API which internally figures out
  // the best curve to use.
  OPENSSL_RET_NOT_OK(SSL_CTX_set_ecdh_auto(ctx_.get(), 1),
                     "failed to configure ECDH support");
#endif // #if OPENSSL_VERSION_NUMBER < 0x10002000L ... #elif ...
#endif // #ifndef OPENSSL_NO_ECDH ...

  return Status::OK();
}

Status TlsContext::VerifyCertChainUnlocked(const Cert& cert) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  X509_STORE* store = SSL_CTX_get_cert_store(ctx_.get());
  auto store_ctx = ssl_make_unique<X509_STORE_CTX>(X509_STORE_CTX_new());

  OPENSSL_RET_NOT_OK(X509_STORE_CTX_init(store_ctx.get(), store, cert.GetTopOfChainX509(),
                     cert.GetRawData()), "could not init X509_STORE_CTX");
  int rc = X509_verify_cert(store_ctx.get());
  if (rc != 1) {
    int err = X509_STORE_CTX_get_error(store_ctx.get());

    // This also clears the errors. It's important to do this before we call
    // X509NameToString(), as it expects the error stack to be empty and it
    // calls SCOPED_OPENSSL_NO_PENDING_ERRORS.
    const auto error_msg = GetOpenSSLErrors();
    if (err == X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT) {
      // It's OK to provide a self-signed cert.
      return Status::OK();
    }

    // Get the cert that failed to verify.
    X509* cur_cert = X509_STORE_CTX_get_current_cert(store_ctx.get());
    string cert_details;
    if (cur_cert) {
      cert_details = Substitute(" (error with cert: subject=$0, issuer=$1)",
                                X509NameToString(X509_get_subject_name(cur_cert)),
                                X509NameToString(X509_get_issuer_name(cur_cert)));
    }

    return Status::RuntimeError(
        Substitute("could not verify certificate chain$0: $1", cert_details, error_msg),
        X509_verify_cert_error_string(err));
  }
  return Status::OK();
}

Status TlsContext::UseCertificateAndKey(const Cert& cert, const PrivateKey& key) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  // Verify that the cert and key match.
  RETURN_NOT_OK(cert.CheckKeyMatch(key));

  std::lock_guard lock(lock_);

  // Verify that the appropriate CA certs have been loaded into the context
  // before we adopt a cert. Otherwise, client connections without the CA cert
  // available would fail.
  RETURN_NOT_OK(VerifyCertChainUnlocked(cert));

  CHECK(!has_cert_);

  OPENSSL_RET_NOT_OK(SSL_CTX_use_PrivateKey(ctx_.get(), key.GetRawData()),
                     "failed to use private key");
  OPENSSL_RET_NOT_OK(SSL_CTX_use_certificate(ctx_.get(), cert.GetTopOfChainX509()),
                     "failed to use certificate");
  has_cert_ = true;
  return Status::OK();
}

Status TlsContext::AddTrustedCertificate(const Cert& cert) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  VLOG(2) << "Trusting certificate " << cert.SubjectName();

  {
    // Workaround for a leak in OpenSSL <1.0.1:
    //
    // If we start trusting a cert, and its internal public-key field hasn't
    // yet been populated, then the first time it's used for verification will
    // populate it. In the case that two threads try to populate it at the same time,
    // one of the thread's copies will be leaked.
    //
    // To avoid triggering the race, we populate the internal public key cache
    // field up front before adding it to the trust store.
    //
    // See OpenSSL commit 33a688e80674aaecfac6d9484ec199daa0ee5b61.
    PublicKey k;
    CHECK_OK(cert.GetPublicKey(&k));
  }

  std::lock_guard lock(lock_);
  auto* cert_store = SSL_CTX_get_cert_store(ctx_.get());

  // Iterate through the certificate chain and add each individual certificate to the store.
  for (int i = 0; i < cert.chain_len(); ++i) {
    X509* inner_cert = sk_X509_value(cert.GetRawData(), i);
    int rc = X509_STORE_add_cert(cert_store, inner_cert);
    if (rc <= 0) {
      // Ignore the common case of re-adding a cert that is already in the
      // trust store.
      auto err = ERR_peek_error();
      if (ERR_GET_LIB(err) == ERR_LIB_X509 &&
          ERR_GET_REASON(err) == X509_R_CERT_ALREADY_IN_HASH_TABLE) {
        ERR_clear_error();
        return Status::OK();
      }
      OPENSSL_RET_NOT_OK(rc, "failed to add trusted certificate");
    }
  }
  trusted_cert_count_ += 1;
  return Status::OK();
}

Status TlsContext::DumpTrustedCerts(vector<string>* cert_ders) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  shared_lock lock(lock_);

  vector<string> ret;
  auto* cert_store = SSL_CTX_get_cert_store(ctx_.get());

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#define STORE_LOCK(CS) CRYPTO_w_lock(CRYPTO_LOCK_X509_STORE)
#define STORE_UNLOCK(CS) CRYPTO_w_unlock(CRYPTO_LOCK_X509_STORE)
#define STORE_GET_X509_OBJS(CS) (CS)->objs
#define X509_OBJ_GET_TYPE(X509_OBJ) (X509_OBJ)->type
#define X509_OBJ_GET_X509(X509_OBJ) (X509_OBJ)->data.x509
#else
#define STORE_LOCK(CS) CHECK_EQ(1, X509_STORE_lock(CS)) << "Could not lock certificate store"
#define STORE_UNLOCK(CS) CHECK_EQ(1, X509_STORE_unlock(CS)) << "Could not unlock certificate store"
#define STORE_GET_X509_OBJS(CS) X509_STORE_get0_objects(CS)
#define X509_OBJ_GET_TYPE(X509_OBJ) X509_OBJECT_get_type(X509_OBJ)
#define X509_OBJ_GET_X509(X509_OBJ) X509_OBJECT_get0_X509(X509_OBJ)
#endif

  STORE_LOCK(cert_store);
  auto unlock = MakeScopedCleanup([&]() {
      STORE_UNLOCK(cert_store);
    });
  auto* objects = STORE_GET_X509_OBJS(cert_store);
  int num_objects = sk_X509_OBJECT_num(objects);
  for (int i = 0; i < num_objects; i++) {
    auto* obj = sk_X509_OBJECT_value(objects, i);
    if (X509_OBJ_GET_TYPE(obj) != X509_LU_X509) continue;
    auto* x509 = X509_OBJ_GET_X509(obj);
    Cert c;
    c.AdoptAndAddRefX509(x509);
    string der;
    RETURN_NOT_OK(c.ToString(&der, DataFormat::DER));
    ret.emplace_back(std::move(der));
  }

  cert_ders->swap(ret);
  return Status::OK();
}

namespace {
Status SetCertAttributes(CertRequestGenerator::Config* config) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  RETURN_NOT_OK_PREPEND(GetFQDN(&config->hostname), "could not determine FQDN for CSR");

  // If the server has logged in from a keytab, then we have a 'real' identity,
  // and our desired CN should match the local username mapped from the Kerberos
  // principal name. Otherwise, we'll make up a common name based on the hostname.
  optional<string> principal = GetLoggedInPrincipalFromKeytab();
  if (!principal) {
    string uid;
    RETURN_NOT_OK_PREPEND(GetLoggedInUser(&uid),
                          "couldn't get local username");
    config->user_id = uid;
    return Status::OK();
  }
  string uid;
  RETURN_NOT_OK_PREPEND(security::MapPrincipalToLocalName(*principal, &uid),
                        "could not get local username for krb5 principal");
  config->user_id = uid;
  config->kerberos_principal = *principal;
  return Status::OK();
}
} // anonymous namespace

Status TlsContext::GenerateSelfSignedCertAndKey() {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  // Step 1: generate the private key to be self signed.
  PrivateKey key;
  RETURN_NOT_OK_PREPEND(GeneratePrivateKey(FLAGS_ipki_server_key_size,
                                           &key),
                                           "failed to generate private key");

  // Step 2: generate a CSR so that the self-signed cert can eventually be
  // replaced with a CA-signed cert.
  CertRequestGenerator::Config config;
  RETURN_NOT_OK(SetCertAttributes(&config));
  CertRequestGenerator gen(config);
  RETURN_NOT_OK_PREPEND(gen.Init(), "could not initialize CSR generator");
  CertSignRequest csr;
  RETURN_NOT_OK_PREPEND(gen.GenerateRequest(key, &csr), "could not generate CSR");

  // Step 3: generate a self-signed cert that we can use for terminating TLS
  // connections until we get the CA-signed cert.
  Cert cert;
  RETURN_NOT_OK_PREPEND(ca::CertSigner::SelfSignCert(key, config, &cert),
                        "failed to self-sign cert");

  // Workaround for an OpenSSL memory leak caused by a race in x509v3_cache_extensions.
  // Upon first use of each certificate, this function gets called to parse various
  // fields of the certificate. However, it's racey, so if multiple "first calls"
  // happen concurrently, one call overwrites the cached data from another, causing
  // a leak. Calling this nonsense X509_check_ca() forces the X509 extensions to
  // get cached, so we don't hit the race later. 'VerifyCertChain' also has the
  // effect of triggering the racy codepath.
  ignore_result(X509_check_ca(cert.GetTopOfChainX509()));
  ERR_clear_error(); // in case it left anything on the queue.

  // Step 4: Adopt the new key and cert.
  std::lock_guard lock(lock_);
  CHECK(!has_cert_);
  OPENSSL_RET_NOT_OK(SSL_CTX_use_PrivateKey(ctx_.get(), key.GetRawData()),
                     "failed to use private key");
  OPENSSL_RET_NOT_OK(SSL_CTX_use_certificate(ctx_.get(), cert.GetTopOfChainX509()),
                     "failed to use certificate");
  has_cert_ = true;
  csr_ = std::move(csr);
  return Status::OK();
}

optional<CertSignRequest> TlsContext::GetCsrIfNecessary() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  shared_lock lock(lock_);
  if (csr_) {
    return csr_->Clone();
  }
  return nullopt;
}

Status TlsContext::AdoptSignedCert(const Cert& cert) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  std::lock_guard lock(lock_);

  if (!csr_) {
    // A signed cert has already been adopted.
    return Status::OK();
  }

  // Verify that the appropriate CA certs have been loaded into the context
  // before we adopt a cert. Otherwise, client connections without the CA cert
  // available would fail.
  RETURN_NOT_OK(VerifyCertChainUnlocked(cert));

  PublicKey csr_key;
  RETURN_NOT_OK(csr_->GetPublicKey(&csr_key));
  PublicKey cert_key;
  RETURN_NOT_OK(cert.GetPublicKey(&cert_key));
  bool equals;
  RETURN_NOT_OK(csr_key.Equals(cert_key, &equals));
  if (!equals) {
    return Status::RuntimeError("certificate public key does not match the CSR public key");
  }

  OPENSSL_RET_NOT_OK(SSL_CTX_use_certificate(ctx_.get(), cert.GetTopOfChainX509()),
                     "failed to use certificate");

  // This should never fail since we already compared the cert's public key
  // against the CSR, but better safe than sorry. If this *does* fail, it
  // appears to remove the private key from the SSL_CTX, so we are left in a bad
  // state.
  OPENSSL_CHECK_OK(SSL_CTX_check_private_key(ctx_.get()))
    << "certificate does not match the private key";

  csr_.reset();

  return Status::OK();
}

Status TlsContext::LoadCertificateAndKey(const string& certificate_path,
                                         const string& key_path) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  Cert c;
  RETURN_NOT_OK(c.FromFile(certificate_path, DataFormat::PEM));
  PrivateKey k;
  RETURN_NOT_OK(k.FromFile(key_path, DataFormat::PEM));
  is_external_cert_ = true;
  return UseCertificateAndKey(c, k);
}

Status TlsContext::LoadCertificateAndPasswordProtectedKey(const string& certificate_path,
                                                          const string& key_path,
                                                          const PasswordCallback& password_cb) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  Cert c;
  RETURN_NOT_OK_PREPEND(c.FromFile(certificate_path, DataFormat::PEM),
                        "failed to load certificate");
  PrivateKey k;
  RETURN_NOT_OK_PREPEND(k.FromFile(key_path, DataFormat::PEM, password_cb),
                        "failed to load private key file");
  RETURN_NOT_OK(UseCertificateAndKey(c, k));
  is_external_cert_ = true;
  return Status::OK();
}

Status TlsContext::LoadCertificateAuthority(const string& certificate_path) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  if (has_cert_) DCHECK(is_external_cert_);
  Cert c;
  RETURN_NOT_OK(c.FromFile(certificate_path, DataFormat::PEM));
  return AddTrustedCertificate(c);
}

Status TlsContext::InitiateHandshake(TlsHandshake* handshake) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  DCHECK(handshake);
  CHECK(ctx_);
  c_unique_ptr<SSL> ssl;
  {
    // This lock is to protect against concurrent change of certificates
    // while calling SSL_new() here.
    shared_lock lock(lock_);
    ssl = ssl_make_unique(SSL_new(ctx_.get()));
  }
  if (!ssl) {
    return Status::RuntimeError("failed to create SSL handle", GetOpenSSLErrors());
  }

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  // KUDU-1926: disable TLS/SSL renegotiation. In version 1.0.2 and prior it's
  // possible to use the undocumented SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS flag.
  // TlsContext::Init() takes care of that for OpenSSL version 1.1.0h and newer.
  // For more context, see a note on the SSL_OP_NO_RENEGOTIATION option in the
  // $OPENSSL_ROOT/CHANGES and https://github.com/openssl/openssl/issues/4739.
  ssl->s3->flags |= SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS;
#endif
  return handshake->Init(std::move(ssl));
}

const char* TlsContext::GetEngineVersionInfo() const {
  CHECK(ctx_);
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  return CHECK_NOTNULL(SSLeay_version(SSLEAY_VERSION));
#else
  return CHECK_NOTNULL(OpenSSL_version(OPENSSL_VERSION));
#endif
}

} // namespace security
} // namespace kudu
