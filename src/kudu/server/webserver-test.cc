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

#include "kudu/server/webserver.h"

#include <openssl/crypto.h>

#include <cstdlib>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/security/test/test_pass.h"
#include "kudu/server/default_path_handlers.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/zlib.h"

using std::string;
using std::vector;
using std::unique_ptr;
using strings::Substitute;

DECLARE_int32(webserver_max_post_length_bytes);

DEFINE_bool(test_sensitive_flag, false, "a sensitive flag");
TAG_FLAG(test_sensitive_flag, sensitive);

namespace kudu {

namespace {
void SetSslOptions(WebserverOptions* opts) {
  string password;
  CHECK_OK(security::CreateTestSSLCertWithEncryptedKey(GetTestDataDirectory(),
                                                       &opts->certificate_file,
                                                       &opts->private_key_file,
                                                       &password));
  opts->private_key_password_cmd = Substitute("echo $0", password);
}

void SetHTPasswdOptions(WebserverOptions* opts) {
  CHECK_OK(security::CreateTestHTPasswd(GetTestDataDirectory(),
                                        &opts->password_file));
}

} // anonymous namespace

class WebserverTest : public KuduTest {
 public:
  WebserverTest() {
    static_dir_ = GetTestPath("webserver-docroot");
    CHECK_OK(env_->CreateDir(static_dir_));
  }

  void SetUp() override {
    KuduTest::SetUp();

    WebserverOptions opts;
    opts.port = 0;
    opts.doc_root = static_dir_;
    opts.enable_doc_root = enable_doc_root();
    if (use_ssl()) SetSslOptions(&opts);
    if (use_htpasswd()) SetHTPasswdOptions(&opts);
    MaybeSetupSpnego(&opts);
    server_.reset(new Webserver(opts));

    AddPreInitializedDefaultPathHandlers(server_.get());
    AddPostInitializedDefaultPathHandlers(server_.get());
    if (!use_htpasswd() || !FIPS_mode()) {
      ASSERT_OK(server_->Start());

      vector<Sockaddr> addrs;
      ASSERT_OK(server_->GetBoundAddresses(&addrs));
      ASSERT_EQ(addrs.size(), 1);
      ASSERT_TRUE(addrs[0].IsWildcard());
      ASSERT_OK(addr_.ParseString("127.0.0.1", addrs[0].port()));
      url_ = Substitute("http://$0", addr_.ToString());
      // For testing purposes, we assume the server has been initialized. Typically this
      // is set to true after the rpc server is started in the server startup process.
      server_->SetStartupComplete(true);
    }
  }

  void RunTestOptions() {
    curl_.set_custom_method("OPTIONS");
    curl_.set_return_headers(true);
    ASSERT_OK(curl_.FetchURL(url_, &buf_));
    ASSERT_STR_CONTAINS(buf_.ToString(),
                        "Allow: GET, POST, HEAD, OPTIONS");
  }

 protected:
  virtual void MaybeSetupSpnego(WebserverOptions* /*opts*/) {}

  // Overridden by subclasses.
  virtual bool enable_doc_root() const { return true; }
  virtual bool use_ssl() const { return false; }
  virtual bool use_htpasswd() const { return false; }

  EasyCurl curl_;
  faststring buf_;
  unique_ptr<Webserver> server_;
  Sockaddr addr_;
  string url_;
  string static_dir_;
};

class SslWebserverTest : public WebserverTest {
 protected:
  bool use_ssl() const override { return true; }
};

class PasswdWebserverTest : public WebserverTest {
 protected:
  bool use_htpasswd() const override { return true; }
};

// Send a HTTP request with no username and password. It should reject
// the request as the .htpasswd is presented to webserver.
TEST_F(PasswdWebserverTest, TestPasswdMissing) {
  if (FIPS_mode()) {
    return;
  }
  Status status = curl_.FetchURL(url_, &buf_);
  ASSERT_EQ("Remote error: HTTP 401", status.ToString());
}

TEST_F(PasswdWebserverTest, TestPasswdPresent) {
  if (FIPS_mode()) {
    return;
  }
  ASSERT_OK(curl_.set_auth(CurlAuthType::DIGEST, security::kTestAuthUsername,
                           security::kTestAuthPassword));
  ASSERT_OK(curl_.FetchURL(addr_.ToString(), &buf_));
}

TEST_F(PasswdWebserverTest, TestCrashInFIPSMode) {
  if (!FIPS_mode()) {
    return;
  }

  Status s = server_->Start();
  ASSERT_TRUE(s.IsIllegalState());
  ASSERT_STR_CONTAINS(s.ToString(), "Digest authentication in FIPS approved mode");
}

class SpnegoWebserverTest : public WebserverTest {
 protected:
  void MaybeSetupSpnego(WebserverOptions* opts) override {
    kdc_.reset(new MiniKdc(MiniKdcOptions{}));
    ASSERT_OK(kdc_->Start());
    kdc_->SetKrb5Environment();
    string kt_path;
    ASSERT_OK(kdc_->CreateServiceKeytab("HTTP/127.0.0.1", &kt_path));
    CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1));
    ASSERT_OK(kdc_->CreateUserPrincipal("alice"));

    opts->require_spnego = true;
    opts->spnego_post_authn_callback = [&](const string& spn) {
      last_authenticated_spn_ = spn;
    };
  }

  Status DoSpnegoCurl() {
    curl_.set_auth(CurlAuthType::SPNEGO);
    if (VLOG_IS_ON(1)) {
      curl_.set_verbose(true);
    }
    return curl_.FetchURL(url_, &buf_);
  }

  unique_ptr<MiniKdc> kdc_;
  const char* const kNotAuthn = "<none>";
  string last_authenticated_spn_ = kNotAuthn;

  // A SPNEGO token manually captured from a client exchange during some prior test run.
  // This is used as a basis for some fuzz tests.
  const char* kWellFormedTokenBase64 =
      "YIICVwYGKwYBBQUCoIICSzCCAkegDTALBgkqhkiG9xIBAgKiggI0BIICMGCCAiwGCSqGSIb3EgECAgEA"
      "boICGzCCAhegAwIBBaEDAgEOogcDBQAgAAAAo4IBP2GCATswggE3oAMCAQWhDRsLS1JCVEVTVC5DT02i"
      "HDAaoAMCAQOhEzARGwRIVFRQGwkxMjcuMC4wLjGjggEBMIH+oAMCARGhAwIBA6KB8QSB7pQrIA2cH2l4"
      "yfHpwhKz2HKYNxoxOw1j++ODByOfN3O/j9/Pp9PwJzQ7hjo5p5nK2OD+2S5YVuS92Ax/LiX8WaYxt9LC"
      "Hew8TkssFOiDffhag1taEcMG5KksPVxZejs+4NYiLj8dCwow3kShl/fpaLYXFFUgChaM7mVEDfMEIdos"
      "WB56k/KMJas7kuAkqDy8sEdPpgzbV7tPmkIFecXPKugZFTttkMREe19LcGO2KnOFflLj7s5F4euWzhrG"
      "v3oZXxDh0G6iyTouSEH+oh/LG97I0umcHrcEit6CjcjVewNhIUaP/Vn2Cu6X0FsF45qkgb4wgbugAwIB"
      "EaKBswSBsLrv38pBLMZo74lMEWHyOrbwrBG0kHfLHVSnxJJYikOwjAoNUm0/NUJc801TtbQZX/e6nRjS"
      "4spS2eU1xnPLcVBbtnonkG7xWSDv/Sl/k73oy7rObVWGQAtYkCJdcfWj1mxeojtrOPcKa9ivBiAuKcKl"
      "EdT2XD6lk161ygu306e7eH8pcuHv+bl9zP42rj85S0c3q0KXRXvsegAFUFk34+AC3fbmKLddEBUoYxms"
      "f+uj";

};

TEST_F(SpnegoWebserverTest, TestAuthenticated) {
  ASSERT_OK(kdc_->Kinit("alice"));
  ASSERT_OK(DoSpnegoCurl());
  EXPECT_EQ("alice@KRBTEST.COM", last_authenticated_spn_);
  EXPECT_STR_CONTAINS(buf_.ToString(), "Kudu");
}

TEST_F(SpnegoWebserverTest, TestUnauthenticatedBadKeytab) {
  ASSERT_OK(kdc_->Kinit("alice"));
  // Randomize the server's key in the KDC so that the key in the keytab doesn't match the
  // one for which the client will get a ticket. This is just an easy way to provoke an
  // error and make sure that our error handling works.
  ASSERT_OK(kdc_->RandomizePrincipalKey("HTTP/127.0.0.1"));

  Status s = DoSpnegoCurl();
  EXPECT_EQ(s.ToString(), "Remote error: HTTP 401");
  EXPECT_EQ(kNotAuthn, last_authenticated_spn_);
  // The essence here is to get HTTP 401 error status in the server's response.
  // There might be different messages returned from webserver because of
  //   * different messages from GSSAPI on Linux and macOS (actually depends
  //     on the version of SASL library, see kGssapiPattern in CleanSaslError())
  //   * different GSSAPI failure paths depending on libkrb5 and libcurl
  //     libraries: with the randomized keytab, the server fails with SASL step
  //     with Status::Incomplete() on macOS. On Linux, the server fails the
  //     SASL step with Status::NotAuthorized(). Instead of finding some
  //     universal way of screwing up the keytab to get the same behavior on
  //     both macOS and Linux, it's easier to rely on the fact that the required
  //     HTTP error code is received and acknowledge for various error messages.
  ASSERT_STR_MATCHES(buf_.ToString(),
                     "(Unspecified GSS failure|"
                     "GSSAPI Error: Miscellaneous failure|"
                     "Must authenticate with SPNEGO)");
}

TEST_F(SpnegoWebserverTest, TestUnauthenticatedNoClientAuth) {
  Status curl_status = DoSpnegoCurl();
  EXPECT_EQ("Remote error: HTTP 401", curl_status.ToString());
  EXPECT_EQ("Must authenticate with SPNEGO.", buf_.ToString());
  EXPECT_EQ(kNotAuthn, last_authenticated_spn_);
}

// Test some malformed authorization headers.
TEST_F(SpnegoWebserverTest, TestInvalidHeaders) {
  EXPECT_EQ(curl_.FetchURL(url_, &buf_, { "Authorization: blahblah" }).ToString(),
            "Remote error: HTTP 500");
  EXPECT_STR_CONTAINS(buf_.ToString(), "bad Negotiate header");
  EXPECT_EQ(curl_.FetchURL(url_, &buf_, { "Authorization: Negotiate aaa" }).ToString(),
            "Remote error: HTTP 401");
  EXPECT_STR_CONTAINS(buf_.ToString(), "Not authorized");
  // Error messages about an invalid token come from the Kerberos library, and
  // different versions of the library have different messages.
  ASSERT_STR_MATCHES(buf_.ToString(),
                     "(Invalid token was supplied|A token was invalid)");
}

// Test that if no authorization header at all is provided, the response
// contains an empty "WWW-Authenticate: Negotiate" header.
TEST_F(SpnegoWebserverTest, TestNoAuthHeader) {
  curl_.set_return_headers(true);
  ASSERT_EQ(curl_.FetchURL(url_, &buf_).ToString(), "Remote error: HTTP 401");
  ASSERT_STR_CONTAINS(buf_.ToString(), "WWW-Authenticate: Negotiate\r\n");
}

// Test all single-bit-flips of a well-formed token, to make sure we don't
// crash.
//
// NOTE: the original token is *well-formed* but not *valid* -- i.e. even if unmodified,
// it would not produce a successful authentication result, since it is a saved constant
// from some previous run of SPNEGO on a different KDC. This test is primarily concerned
// with defending against remote buffer overflows during token parsing, etc.
TEST_F(SpnegoWebserverTest, TestBitFlippedTokens) {
  string token;
  CHECK(strings::Base64Unescape(kWellFormedTokenBase64, &token));

  for (int i = 0; i < token.size(); i++) {
    SCOPED_TRACE(i);
    for (int bit = 0; bit < 8; bit++) {
      SCOPED_TRACE(bit);
      token[i] ^= 1 << bit;
      string b64_token;
      strings::Base64Escape(token, &b64_token);
      string header = Substitute("Authorization: Negotiate $0", b64_token);
      Status s = curl_.FetchURL(url_, &buf_, { header });
      EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
      token[i] ^= 1 << bit;
    }
  }
}

// Test all truncations of a well-formed token, to make sure we don't
// crash.
//
// NOTE: see above regarding "well-formed" vs "valid".
TEST_F(SpnegoWebserverTest, TestTruncatedTokens) {
  string token;
  CHECK(strings::Base64Unescape(kWellFormedTokenBase64, &token));

  do {
    token.resize(token.size() - 1);
    SCOPED_TRACE(token.size());
    string b64_token;
    strings::Base64Escape(token, &b64_token);
    string header = Substitute("Authorization: Negotiate $0", b64_token);
    Status s = curl_.FetchURL(url_, &buf_, { header });
    EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
  } while (!token.empty());
}

// Tests that even if we don't provide adequate authentication information in
// an OPTIONS request, the server still honors it.
TEST_F(SpnegoWebserverTest, TestAuthNotRequiredForOptions) {
  NO_FATALS(RunTestOptions());
}

TEST_F(WebserverTest, TestIndexPage) {
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  // Check expected header.
  ASSERT_STR_CONTAINS(buf_.ToString(), "X-Frame-Options: DENY");

  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(buf_.ToString(), "memz");
}

TEST_F(WebserverTest, TestHttpCompression) {
  std::ostringstream oss;
  string decoded_str;

  // Curl with gzip compression enabled.
  ASSERT_OK(curl_.FetchURL(url_, &buf_, {"Accept-Encoding: deflate, br, gzip"}));

  // If compressed successfully, we should be able to uncompress.
  ASSERT_OK(zlib::Uncompress(Slice(buf_.ToString()), &oss));
  decoded_str = oss.str();

  // Should have expected title.
  ASSERT_STR_CONTAINS(decoded_str, "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(decoded_str, "memz");

  // Should have expected header when compressed with headers returned.
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_,
                          {"Accept-Encoding: deflate, megaturbogzip,  gzip , br"}));
  ASSERT_STR_CONTAINS(buf_.ToString(), "Content-Encoding: gzip");


  // Curl with compression disabled.
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  // Check expected header.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Content-Type:");

  // Check unexpected header.
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "Content-Encoding: gzip");

  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(buf_.ToString(), "memz");


  // Curl with compression enabled but not accepted by Kudu.
  curl_.set_return_headers(true);
  ASSERT_OK(curl_.FetchURL(url_, &buf_, {"Accept-Encoding: megaturbogzip, deflate, xz"}));
  // Check expected header.
  ASSERT_STR_CONTAINS(buf_.ToString(), "HTTP/1.1 200 OK");

  // Check unexpected header.
  ASSERT_STR_NOT_CONTAINS(buf_.ToString(), "Content-Encoding: gzip");

  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");

  // Should have link to default path handlers (e.g memz)
  ASSERT_STR_CONTAINS(buf_.ToString(), "memz");
}

TEST_F(SslWebserverTest, TestSSL) {
  // We use a self-signed cert, so we need to disable cert verification in curl.
  curl_.set_verify_peer(false);

  ASSERT_OK(curl_.FetchURL(Substitute("https://$0/", addr_.ToString()), &buf_));
  // Should have expected title.
  ASSERT_STR_CONTAINS(buf_.ToString(), "Kudu");
}

TEST_F(WebserverTest, TestDefaultPaths) {
  // Test memz
  ASSERT_OK(curl_.FetchURL(Substitute("$0/memz?raw=1", url_), &buf_));
#ifdef TCMALLOC_ENABLED
  ASSERT_STR_CONTAINS(buf_.ToString(), "Bytes in use by application");
#else
  ASSERT_STR_CONTAINS(buf_.ToString(), "not available unless tcmalloc is enabled");
#endif

  // Test varz -- check for one of the built-in gflags flags.
  ASSERT_OK(curl_.FetchURL(Substitute("$0/varz?raw=1", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "--v=");

  // Test version -- check for version information
  ASSERT_OK(curl_.FetchURL(Substitute("$0/version", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "version_info");

  // Test healthz -- check for OK
  ASSERT_OK(curl_.FetchURL(Substitute("$0/healthz", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "OK");
}

TEST_F(WebserverTest, TestRedactFlagsDump) {
  kudu::g_should_redact = kudu::RedactContext::ALL;
  // Test varz -- check for the sensitive flag is redacted and HTML-escaped.
  ASSERT_OK(curl_.FetchURL(Substitute("$0/varz", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "--test_sensitive_flag=&lt;redacted&gt;");

  // Test varz?raw -- check for the sensitive flag is redacted and not HTML-escaped.
  ASSERT_OK(curl_.FetchURL(Substitute("$0/varz?raw=1", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), Substitute("--test_sensitive_flag=$0",
                                                  kRedactionMessage));
}

// Used in symbolization test below.
void SomeMethodForSymbolTest1() {}
// Used in symbolization test below.
void SomeMethodForSymbolTest2() {}

TEST_F(WebserverTest, TestPprofPaths) {
  // Test /pprof/cmdline GET
  ASSERT_OK(curl_.FetchURL(Substitute("$0/pprof/cmdline", url_), &buf_));
  ASSERT_STR_CONTAINS(buf_.ToString(), "webserver-test");
  ASSERT_TRUE(!HasSuffixString(buf_.ToString(), string("\x00", 1)))
    << "should not have trailing NULL: " << Slice(buf_).ToDebugString();

  // Test /pprof/symbol GET
  ASSERT_OK(curl_.FetchURL(Substitute("$0/pprof/symbol", url_), &buf_));
  ASSERT_EQ(buf_.ToString(), "num_symbols: 1");

  // Test /pprof/symbol POST
  {
    // Formulate a request with some valid symbol addresses.
    string req = StringPrintf("%p+%p",
                              &SomeMethodForSymbolTest1,
                              &SomeMethodForSymbolTest2);
    SCOPED_TRACE(req);
    ASSERT_OK(curl_.PostToURL(Substitute("$0/pprof/symbol", url_), req, &buf_));
    ASSERT_EQ(buf_.ToString(),
              StringPrintf("%p\tkudu::SomeMethodForSymbolTest1()\n"
                           "%p\tkudu::SomeMethodForSymbolTest2()\n",
                           &SomeMethodForSymbolTest1,
                           &SomeMethodForSymbolTest2));
  }
}

// Send a POST request with too much data. It should reject
// the request with the correct HTTP error code.
TEST_F(WebserverTest, TestPostTooBig) {
  FLAGS_webserver_max_post_length_bytes = 10;
  string req(10000, 'c');
  Status s = curl_.PostToURL(Substitute("$0/pprof/symbol", url_), req, &buf_);
  ASSERT_EQ("Remote error: HTTP 413", s.ToString());
}

// Test that static files are served and that directory listings are
// disabled.
TEST_F(WebserverTest, TestStaticFiles) {
  // Fetch a non-existent static file.
  Status s = curl_.FetchURL(Substitute("$0/foo.txt", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 404", s.ToString());

  // Create the file and fetch again. This time it should succeed.
  ASSERT_OK(WriteStringToFile(env_, "hello world",
                              Substitute("$0/foo.txt", static_dir_)));
  ASSERT_OK(curl_.FetchURL(Substitute("$0/foo.txt", url_), &buf_));
  ASSERT_EQ("hello world", buf_.ToString());

  // Create a directory and ensure that subdirectory listing is disabled.
  ASSERT_OK(env_->CreateDir(Substitute("$0/dir", static_dir_)));
  s = curl_.FetchURL(Substitute("$0/dir/", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 403", s.ToString());
}

class DisabledDocRootWebserverTest : public WebserverTest {
 protected:
  bool enable_doc_root() const override { return false; }
};

TEST_F(DisabledDocRootWebserverTest, TestHandlerNotFound) {
  Status s = curl_.FetchURL(Substitute("$0/foo", url_), &buf_);
  ASSERT_EQ("Remote error: HTTP 404", s.ToString());
  ASSERT_STR_CONTAINS(buf_.ToString(), "No handler for URI /foo");
}

// Test that HTTP OPTIONS requests are permitted.
TEST_F(WebserverTest, TestHttpOptions) {
  NO_FATALS(RunTestOptions());
}

// Test that we're able to reuse connections for subsequent fetches.
TEST_F(WebserverTest, TestConnectionReuse) {
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_EQ(1, curl_.num_connects());
  ASSERT_OK(curl_.FetchURL(url_, &buf_));
  ASSERT_EQ(0, curl_.num_connects());
}

class WebserverAdvertisedAddressesTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    WebserverOptions opts;
    opts.port = 0;
    string iface = use_webserver_interface();
    int32 port = use_webserver_port();
    string advertised = use_advertised_addresses();
    if (!iface.empty()) {
      opts.bind_interface = iface;
    }
    if (port != 0) {
      opts.port = port;
    }
    if (!advertised.empty()) {
      opts.webserver_advertised_addresses = advertised;
    }
    server_.reset(new Webserver(opts));

    ASSERT_OK(server_->Start());
  }

 protected:
  // Overridden by subclasses.
  virtual string use_webserver_interface() const { return ""; }
  virtual int32 use_webserver_port() const { return 0; }
  virtual string use_advertised_addresses() const { return ""; }

  void GetAddresses(vector<Sockaddr>* bound_addrs,
                    vector<Sockaddr>* advertised_addrs) {
    ASSERT_OK(server_->GetBoundAddresses(bound_addrs));
    ASSERT_OK(server_->GetAdvertisedAddresses(advertised_addrs));
  }

  unique_ptr<Webserver> server_;
};

class AdvertisedOnlyWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() const override { return "1.2.3.4:1234"; }
};

class BoundOnlyWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_webserver_interface() const override { return "127.0.0.1"; }
  int32 use_webserver_port() const override { return 9999; }
};

class BothBoundAndAdvertisedWebserverTest : public WebserverAdvertisedAddressesTest {
 protected:
  string use_advertised_addresses() const override { return "1.2.3.4:1234"; }
  string use_webserver_interface() const override { return "127.0.0.1"; }
  int32 use_webserver_port() const override { return 9999; }
};

TEST_F(AdvertisedOnlyWebserverTest, OnlyAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("1.2.3.4", advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
  ASSERT_NE(9999, bound_addrs[0].port());
}

TEST_F(BoundOnlyWebserverTest, OnlyBoundAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("127.0.0.1", advertised_addrs[0].host());
  ASSERT_EQ(9999, advertised_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_EQ(9999, bound_addrs[0].port());
}

TEST_F(BothBoundAndAdvertisedWebserverTest, BothBoundAndAdvertisedAddresses) {
  vector<Sockaddr> bound_addrs, advertised_addrs;
  NO_FATALS(GetAddresses(&bound_addrs, &advertised_addrs));

  ASSERT_EQ(1, advertised_addrs.size());
  ASSERT_EQ(1, bound_addrs.size());
  ASSERT_EQ("1.2.3.4", advertised_addrs[0].host());
  ASSERT_EQ(1234, advertised_addrs[0].port());
  ASSERT_EQ("127.0.0.1", bound_addrs[0].host());
  ASSERT_EQ(9999, bound_addrs[0].port());
}

// Various tests for failed webserver startup cases.
class WebserverNegativeTests : public KuduTest {
 protected:

  // Tries to start the webserver, expecting it to fail.
  // 'func' is used to set webserver options before starting it.
  template<class OptsFunc>
  void ExpectFailedStartup(const OptsFunc& func) {
    WebserverOptions opts;
    opts.port = 0;
    func(&opts);
    Webserver server(opts);
    Status s = server.Start();
    ASSERT_FALSE(s.ok()) << s.ToString();
  }
};

TEST_F(WebserverNegativeTests, BadCertFile) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->certificate_file = "/dev/null";
    });
}

TEST_F(WebserverNegativeTests, BadKeyFile) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_file = "/dev/null";
    });
}

TEST_F(WebserverNegativeTests, WrongPassword) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_password_cmd = "echo wrong_pass";
    });
}

TEST_F(WebserverNegativeTests, BadPasswordCommand) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      SetSslOptions(opts);
      opts->private_key_password_cmd = "/bin/false";
    });
}

TEST_F(WebserverNegativeTests, BadAdvertisedAddresses) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      opts->webserver_advertised_addresses = ";;;;;";
    });
}

TEST_F(WebserverNegativeTests, BadAdvertisedAddressesZeroPort) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      opts->webserver_advertised_addresses = "localhost:0";
    });
}

TEST_F(WebserverNegativeTests, SpnegoWithoutKeytab) {
  ExpectFailedStartup([](WebserverOptions* opts) {
      opts->require_spnego = true;
    });
}

} // namespace kudu
