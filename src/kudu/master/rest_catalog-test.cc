#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/security/test/mini_kdc.h"

using kudu::KuduPartialRow;
using kudu::Status;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_bool(enable_rest_api);
DECLARE_bool(webserver_require_spnego);
DECLARE_string(spnego_keytab_file);

namespace kudu {
namespace master {

class RestCatalogTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    kdc_.reset(new MiniKdc(MiniKdcOptions{}));
    ASSERT_OK(kdc_->Start());
    kdc_->SetKrb5Environment();
    string kt_path;
    ASSERT_OK(kdc_->CreateServiceKeytabWithName("HTTP/127.0.0.1",
                                                "spnego.dedicated",
                                                &kt_path));
    ASSERT_OK(kdc_->CreateUserPrincipal("alice"));
    FLAGS_spnego_keytab_file = kt_path;
    FLAGS_webserver_require_spnego = true;


    // Set REST endpoint flag to true
    FLAGS_enable_rest_api = true;


    // Configure the mini-cluster
    auto opts = InternalMiniClusterOptions();
    opts.bind_mode = BindMode::LOOPBACK; 

    cluster_.reset(new InternalMiniCluster(env_, opts));

    // Start the cluster
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(KuduClientBuilder()
                  .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
                  .Build(&client_));
  }

  Status CreateTestTable() {
    string kTableName = "test_table";
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    KUDU_CHECK_OK(b.Build(&schema));
    vector<string> columnNames;
    columnNames.emplace_back("key");

    // Set the schema and range partition columns.
    KuduTableCreator* tableCreator = client_->NewTableCreator();
    tableCreator->table_name(kTableName).schema(&schema).set_range_partition_columns(columnNames);

    // Generate and add the range partition splits for the table.
    int32_t increment = 1000 / 10;
    for (int32_t i = 1; i < 10; i++) {
      KuduPartialRow* row = schema.NewRow();
      KUDU_CHECK_OK(row->SetInt32(0, i * increment));
      tableCreator->add_range_partition_split(row);
    }
    tableCreator->num_replicas(1);
    Status s = tableCreator->Create();
    delete tableCreator;
    return s;
  }

  string GetTableId(const string& table_name) {
    shared_ptr<KuduTable> table;
    Status s = client_->OpenTable(table_name, &table);
    if (!s.ok()) {
      LOG(ERROR) << "OpenTable failed: " << s.ToString();
      return "";
    }
    if (table->name() == table_name) {
      return table->id();
    }
    return "";
  }

  Status DoSpnegoCurl() {
    curl_.set_auth(CurlAuthType::SPNEGO);
    if (VLOG_IS_ON(1)) {
      curl_.set_verbose(true);
    }
    return curl_.FetchURL(url_, &buf_);
  }

 protected:
  string url_;
  unique_ptr<MiniKdc> kdc_;
  unique_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  EasyCurl curl_;
  faststring buf_;
  const char* columns =
      "{\"id\":10,\"name\":\"key\",\"type\":\"INT32\",\"is_key\":true,\"is_"
      "nullable\":false,\"encoding\":\"AUTO_ENCODING\",\"compression\":\"DEFAULT_COMPRESSION\","
      "\"cfile_block_size\":0,\"immutable\":false},{\"id\":11,\"name\":\"int_val\",\"type\":"
      "\"INT32\",\"is_key\":false,\"is_nullable\":false,\"encoding\":\"AUTO_ENCODING\","
      "\"compression\":\"DEFAULT_COMPRESSION\",\"cfile_block_size\":0,\"immutable\":false}";
  const char* new_column =
      ",{\"id\":12,\"name\":\"new_column\",\"type\":\"STRING\",\"is_key\":false,\"is_nullable\":"
      "true,\"encoding\":\"AUTO_ENCODING\",\"compression\":\"DEFAULT_COMPRESSION\",\"cfile_"
      "block_size\":0,\"immutable\":false}";
  const std::string kTableSchema = Substitute("{\"columns\":[$0]}", columns);
  const std::string kTableSchemaWithNewColumn =
      Substitute("{\"columns\":[$0$1]}", columns, new_column);
  const std::string kTablePartitionSchema = "{\"range_schema\":{\"columns\":[{\"id\":10}]}}";
};



TEST_F(RestCatalogTest, TestGetTablesOneTableWithKinit) {
  ASSERT_OK(CreateTestTable());

  ASSERT_OK(kdc_->Kinit("alice"));
  url_ =  Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString());
  ASSERT_OK(DoSpnegoCurl());

  string table_id = GetTableId("test_table");
  ASSERT_STR_CONTAINS(
      buf_.ToString(),
      Substitute("{\"tables\":[{\"table_id\":\"$0\",\"table_name\":\"test_table\"}]}", table_id));
}

TEST_F(RestCatalogTest, TestGetTablesOneTableNoKinit) {
  ASSERT_OK(CreateTestTable());

  // ASSERT_OK(kdc_->Kinit("alice"));
  url_ =  Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString());
  // ASSERT_OK(DoSpnegoCurl());
  curl_.FetchURL(url_, &buf_);

  ASSERT_STR_CONTAINS(
      buf_.ToString(),
      "Must authenticate with SPNEGO");
}


}  // namespace master
}  // namespace kudu
