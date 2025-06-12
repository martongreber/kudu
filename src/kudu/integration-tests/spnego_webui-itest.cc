#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/curl_util.h"          // EasyCurl
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/faststring.h"

using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::EasyCurl;
using kudu::HostPort;
using std::string;
using strings::Substitute;
using kudu::Status;


namespace kudu {
namespace master {

class SpnegoWebUiITest : public kudu::KuduTest { };

TEST_F(SpnegoWebUiITest, MasterWebUiRequiresKerberos) {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 0;
    opts.enable_kerberos = true;

    ExternalMiniCluster cluster(opts);
    ASSERT_OK(cluster.Start());
    ASSERT_OK(cluster.kdc()->Kinit("test-user"));

    EasyCurl c;
    faststring buf;
    Status s = c.FetchURL(
        Substitute("http://$0", cluster.master()->bound_http_hostport().ToString()),
        &buf);

    ASSERT_STR_CONTAINS(s.ToString(), "HTTP 401");

    c.set_auth(CurlAuthType::SPNEGO);
    s = c.FetchURL(
        Substitute("http://$0", cluster.master()->bound_http_hostport().ToString()),
        &buf);

    ASSERT_OK(s);





}
}
}