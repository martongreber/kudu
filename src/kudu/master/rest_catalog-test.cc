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

namespace kudu {
namespace master {

class RestCatalogTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    // Set REST endpoint flag to true
    FLAGS_enable_rest_api = true;

    // Configure the mini-cluster
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));

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

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
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

TEST_F(RestCatalogTest, TestInvalidMethod) {
  EasyCurl c;
  c.set_custom_method("DELETE");
  faststring buf;
  Status s = c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 405");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Method not allowed\"}");
}

TEST_F(RestCatalogTest, TestInvalidMethodOnTableEndpoint) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  c.set_custom_method("CONNECT");
  faststring buf;
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 405");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Method not allowed\"}");
}

TEST_F(RestCatalogTest, TestGetTablesZeroTables) {
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf));
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"tables\":[]}");
}

TEST_F(RestCatalogTest, TestGetTablesOneTable) {
  ASSERT_OK(CreateTestTable());
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf));
  string table_id = GetTableId("test_table");
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      Substitute("{\"tables\":[{\"table_id\":\"$0\",\"table_name\":\"test_table\"}]}", table_id));
}

TEST_F(RestCatalogTest, TestGetTableEndpoint) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  faststring buf;
  ASSERT_OK(c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                  cluster_->mini_master()->bound_http_addr().ToString(),
                                  table_id),
                       &buf));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("test_table", &table));
  ASSERT_STR_CONTAINS(buf.ToString(),
                      Substitute("{\"name\":\"test_table\",\"id\":\"$0\",\"schema\":$1,"
                                 "\"partition_schema\":$2,\"owner\":\"$3\","
                                 "\"comment\":\"\",\"extra_config\":{}}",
                                 table_id,
                                 kTableSchema,
                                 kTablePartitionSchema,
                                 table->owner()));
}

TEST_F(RestCatalogTest, TestGetTableNotFound) {
  EasyCurl c;
  c.set_verbose(true);
  faststring buf;
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/05755b4c0c7640cd9f6673c2530a4e78",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 404");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Table not found\"}");
}

TEST_F(RestCatalogTest, TestGetTableMalformedId) {
  EasyCurl c;
  c.set_verbose(true);
  faststring buf;
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/123",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Invalid table ID\"}");
}

TEST_F(RestCatalogTest, TestDeleteTableNonExistent) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("DELETE");
  c.set_verbose(true);
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/05755b4c0c7640cd9f6673c2530a4e78",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 404");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Table not found\"}");
}

TEST_F(RestCatalogTest, TestDeleteTableEndpoint) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  faststring buf;
  c.set_custom_method("DELETE");
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "OK");
  ASSERT_TRUE(buf.size() == 0);
  shared_ptr<KuduTable> table;
  client_->OpenTable("test_table", &table);
  ASSERT_TRUE(table == nullptr);
}

TEST_F(RestCatalogTest, TestDeleteTableMalformedId) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("DELETE");
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/123",
                                   cluster_->mini_master()->bound_http_addr().ToString()),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Invalid table ID\"}");
}

TEST_F(RestCatalogTest, TestPostTableNoData) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
  Status s = c.FetchURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 411");
}

TEST_F(RestCatalogTest, TestPostTableMalformedData) {
  EasyCurl c;
  faststring buf;
  Status s = c.PostToURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      "{\"name\":\"test_table\"}",
      &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      "{\"error\":\"JSON table object is not correct: {\\\"name\\\":\\\"test_table\\\"}\"}");
}

TEST_F(RestCatalogTest, TestPostTableEndpoint) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("POST");
  Status s = c.PostToURL(
      Substitute("http://$0/api/v1/tables", cluster_->mini_master()->bound_http_addr().ToString()),
      R"({
        "name": "test_table",
        "schema": {
          "columns": [
            {"name": "key", "type": "INT32", "is_nullable": false, "is_key": true},
            {"name": "int_val", "type": "INT32", "is_nullable": false, "is_key": false}
          ]
        },
        "partition_schema": {
          "range_schema": {
            "columns": [{"name": "key"}]
          }
        },
        "num_replicas": 1,
        "owner": "root"
      })",
      &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "OK");
  string table_id = GetTableId("test_table");
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("test_table", &table));
  ASSERT_STR_CONTAINS(buf.ToString(),
                      Substitute("{\"name\":\"test_table\",\"id\":\"$0\",\"schema\":$1,\"partition_"
                                 "schema\":$2,"
                                 "\"owner\":\"$3\",\"comment\":\"\",\"extra_config\":{}}",
                                 table_id,
                                 kTableSchema,
                                 kTablePartitionSchema,
                                 table->owner()));
  ASSERT_TRUE(table != nullptr);
  ASSERT_EQ(table->name(), "test_table");
  ASSERT_EQ(table->num_replicas(), 1);
}

TEST_F(RestCatalogTest, TestPutTableMalformedId) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/123",
                                    cluster_->mini_master()->bound_http_addr().ToString()),
                         "{\"name\":\"test_table\"}",
                         &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Invalid table ID\"}");
}

TEST_F(RestCatalogTest, TestPutTableNoData) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.FetchURL(Substitute("http://$0/api/v1/tables/$1",
                                   cluster_->mini_master()->bound_http_addr().ToString(),
                                   table_id),
                        &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 411");
}

TEST_F(RestCatalogTest, TestPutTableMalformedData) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                    cluster_->mini_master()->bound_http_addr().ToString(),
                                    table_id),
                         "{\"name\":\"test_table\"}",
                         &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 400");
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      "{\"error\":\"JSON table object is not correct: {\\\"name\\\":\\\"test_table\\\"}\"}");
}

TEST_F(RestCatalogTest, TestPutTableNonExistent) {
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/05755b4c0c7640cd9f6673c2530a4e78",
                                    cluster_->mini_master()->bound_http_addr().ToString()),
                         R"({
                          "table": {
                            "table_name": "test_table"
                          },
                          "alter_schema_steps": [
                            {
                              "type": "ADD_COLUMN",
                              "add_column": {
                                "schema": {
                                  "name": "new_column",
                                  "type": "STRING",
                                  "is_nullable": true
                                }
                              }
                            }
                          ]
                        }
                        )",
                         &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "HTTP 404");
  ASSERT_STR_CONTAINS(buf.ToString(), "{\"error\":\"Table not found\"}");
}

TEST_F(RestCatalogTest, TestPutTableEndpointAddColumn) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                    cluster_->mini_master()->bound_http_addr().ToString(),
                                    table_id),
                         R"({
                          "table": {
                            "table_name": "test_table"
                          },
                          "alter_schema_steps": [
                            {
                              "type": "ADD_COLUMN",
                              "add_column": {
                                "schema": {
                                  "name": "new_column",
                                  "type": "STRING",
                                  "is_nullable": true
                                }
                              }
                            }
                          ]
                        }
                        )",
                         &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "OK");
  shared_ptr<KuduTable> table;
  s = client_->OpenTable("test_table", &table);
  ASSERT_STR_CONTAINS(
      buf.ToString(),
      Substitute("{\"name\":\"test_table\",\"id\":\"$0\",\"schema\":$1,\"partition_schema\":$2,"
                 "\"owner\":\"$3\",\"comment\":\"\",\"extra_config\":{}}",
                 table_id,
                 kTableSchemaWithNewColumn,
                 kTablePartitionSchema,
                 table->owner()));
  ASSERT_TRUE(s.ok());
  const KuduSchema& schema = table->schema();
  ASSERT_EQ(schema.num_columns(), 3);
  ASSERT_EQ(schema.Column(0).name(), "key");
  ASSERT_EQ(schema.Column(1).name(), "int_val");
  ASSERT_EQ(schema.Column(2).name(), "new_column");
}

TEST_F(RestCatalogTest, TestPutTableEndpointRenameColumn) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                    cluster_->mini_master()->bound_http_addr().ToString(),
                                    table_id),
                         R"({
                          "table": {
                            "table_name": "test_table"
                          },
                          "alter_schema_steps": [
                            {
                              "type": "RENAME_COLUMN",
                              "rename_column": {
                                "old_name": "int_val",
                                "new_name": "new_int_val"
                              }
                            }
                          ]
                        }
                        )",
                         &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "OK");
  shared_ptr<KuduTable> table;
  s = client_->OpenTable("test_table", &table);
  ASSERT_TRUE(s.ok());
  const KuduSchema& schema = table->schema();
  ASSERT_EQ(schema.num_columns(), 2);
  ASSERT_EQ(schema.Column(0).name(), "key");
  ASSERT_EQ(schema.Column(1).name(), "new_int_val");
}

TEST_F(RestCatalogTest, TestPutTableEndpointDropColumn) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                    cluster_->mini_master()->bound_http_addr().ToString(),
                                    table_id),
                          R"({
                            "table": {
                              "table_name": "test_table"
                            },
                            "alter_schema_steps": [
                              {
                                "type": "DROP_COLUMN",
                                "drop_column": {
                                  "name": "int_val"
                                }
                              }
                            ]
                          }
                          )", &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "OK");
  shared_ptr<KuduTable> table;
  s = client_->OpenTable("test_table", &table);
  ASSERT_TRUE(s.ok());
  const KuduSchema& schema = table->schema();
  ASSERT_EQ(schema.num_columns(), 1);
  ASSERT_EQ(schema.Column(0).name(), "key");
}

TEST_F(RestCatalogTest, TestPutTableEndpointChangeOwner) {
  ASSERT_OK(CreateTestTable());
  string table_id = GetTableId("test_table");
  EasyCurl c;
  faststring buf;
  c.set_custom_method("PUT");
  Status s = c.PostToURL(Substitute("http://$0/api/v1/tables/$1",
                                    cluster_->mini_master()->bound_http_addr().ToString(),
                                    table_id),
                         R"({
                          "table": {
                            "table_name": "test_table"
                          },
                          "new_table_owner": "new_owner"
                        }
                        )",
                         &buf);
  ASSERT_STR_CONTAINS(s.ToString(), "OK");
  shared_ptr<KuduTable> table;
  s = client_->OpenTable("test_table", &table);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(table->owner(), "new_owner");
}

}  // namespace master
}  // namespace kudu
