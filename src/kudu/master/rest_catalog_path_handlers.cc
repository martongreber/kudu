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

#include "kudu/master/rest_catalog_path_handlers.h"

#include <functional>
#include <optional>
#include <string>

#include "google/protobuf/util/json_util.h"
#include <google/protobuf/stubs/status.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/web_callback_registry.h"

#define RETURN_JSON_ERROR(jw, error_msg, status_code, error_code) \
  {                                                               \
    (jw).StartObject();                                           \
    (jw).String("error");                                         \
    (jw).String(error_msg);                                       \
    (jw).EndObject();                                             \
    (status_code) = (error_code);                                 \
    return;                                                       \
  }

using kudu::consensus::RaftPeerPB;
using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonStringToMessage;
using std::optional;
using std::ostringstream;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

RestCatalogPathHandlers::~RestCatalogPathHandlers() {}


// TODO: i've just copied this from master pathhandlers, need to think about code duplication.
Status RestCatalogPathHandlers::GetLeaderMasterHttpAddr(string* leader_http_addr) const {
  // TODO(aserbin): update this to work with proxied RPCs
  vector<ServerEntryPB> masters;
  RETURN_NOT_OK_PREPEND(master_->ListMasters(&masters,
                                             /*use_external_addr=*/false),
                        "unable to list masters");
  for (const auto& master : masters) {
    if (master.has_error()) {
      continue;
    }
    if (master.role() != RaftPeerPB::LEADER) {
      continue;
    }
    const ServerRegistrationPB& reg = master.registration();
    if (reg.http_addresses().empty()) {
      return Status::NotFound("leader master has no http address");
    }
    *leader_http_addr = Substitute("$0://$1:$2",
                                   reg.https_enabled() ? "https" : "http",
                                   reg.http_addresses(0).host(),
                                   reg.http_addresses(0).port());
    return Status::OK();
  }
  return Status::NotFound("no leader master known to this master");
}

void RestCatalogPathHandlers::HandleApiTableEndpoint(const Webserver::WebRequest& req,
                                                     Webserver::PrerenderedWebResponse* resp) {
  string table_id = req.path_params.at("table_id");
  ostringstream* output = &resp->output;
  JsonWriter jw(output, JsonWriter::COMPACT);

  if (table_id.length() != 32) {
    RETURN_JSON_ERROR(jw, "Invalid table ID", resp->status_code, HttpStatusCode::BadRequest);
  }
  CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
  scoped_refptr<TableInfo> table;
  Status status = master_->catalog_manager()->GetTableInfo(table_id, &table);

  if (!status.ok()) {
    RETURN_JSON_ERROR(jw, status.ToString(), resp->status_code, HttpStatusCode::ServiceUnavailable);
  }

  if (!table) {
    RETURN_JSON_ERROR(jw, "Table not found", resp->status_code, HttpStatusCode::NotFound);
  }

  if (req.request_method == "GET") {
    HandleGetTable(output, req, &resp->status_code);
  } else if (req.request_method == "PUT") {
    HandlePutTable(output, req, &resp->status_code);
  } else if (req.request_method == "DELETE") {
    HandleDeleteTable(output, req, &resp->status_code);
  } else {
    RETURN_JSON_ERROR(
        jw, "Method not allowed", resp->status_code, HttpStatusCode::MethodNotAllowed);
  }
}

void RestCatalogPathHandlers::HandleApiTablesEndpoint(const Webserver::WebRequest& req,
                                                      Webserver::PrerenderedWebResponse* resp) {
  ostringstream* output = &resp->output;


  if (req.request_method == "GET") {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());

    // TODO: just copy pasted it here.
    // Probably best to figure out a clever way to add this to all the places
    // sidenote, since we need the path here, and we know the branches of the implemented functions
    // we might also think about the swagger generation as we have all the info here.
    if (!l.leader_status().ok()) {
      // request came to a non-leader master
      // respond 307 reason: preserves request method
      resp->status_code = HttpStatusCode::TemporaryRedirect;
      // fill in 'Location' header with the same path but with the leader master host:port.
      string leader_http_addr;
      Status s = GetLeaderMasterHttpAddr(&leader_http_addr);
      resp->response_headers["Location"] = Substitute("$0/$1", leader_http_addr, "/api/v1/tables");

      return;
    }

    HandleGetTables(output, req, &resp->status_code);
  } else if (req.request_method == "POST") {
    CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
    HandlePostTables(output, req, &resp->status_code);
  } else {
    JsonWriter jw(output, JsonWriter::COMPACT);
    RETURN_JSON_ERROR(
        jw, "Method not allowed", resp->status_code, HttpStatusCode::MethodNotAllowed);
  }
}

void RestCatalogPathHandlers::HandleGetTables(std::ostringstream* output,
                                              const Webserver::WebRequest& req,
                                              HttpStatusCode* status_code) {
  ListTablesRequestPB request;
  ListTablesResponsePB response;
  std::optional<std::string> user = req.authn_principal.empty() ? "default" : req.authn_principal;
  Status status = master_->catalog_manager()->ListTables(&request, &response, user);
  JsonWriter jw(output, JsonWriter::COMPACT);

  if (!status.ok()) {
    RETURN_JSON_ERROR(jw, status.ToString(), *status_code, HttpStatusCode::ServiceUnavailable);
  }
  jw.StartObject();
  jw.String("tables");
  jw.StartArray();

  for (const auto& table : response.tables()) {
    jw.StartObject();
    jw.String("table_id");
    jw.String(table.id());
    jw.String("table_name");
    jw.String(table.name());
    jw.EndObject();
  }
  jw.EndArray();
  jw.EndObject();
  *status_code = HttpStatusCode::Ok;
}

void RestCatalogPathHandlers::HandlePostTables(std::ostringstream* output,
                                               const Webserver::WebRequest& req,
                                               HttpStatusCode* status_code) {
  CreateTableRequestPB request;
  CreateTableResponsePB response;

  const string& json_str = req.post_data;
  JsonParseOptions opts;
  opts.case_insensitive_enum_parsing = true;
  google::protobuf::util::Status google_status = JsonStringToMessage(json_str, &request, opts);

  if (!google_status.ok()) {
    JsonWriter jw(output, JsonWriter::COMPACT);
    RETURN_JSON_ERROR(jw,
                      Substitute("JSON table object is not correct: $0", json_str),
                      *status_code,
                      HttpStatusCode::BadRequest);
  }
  std::optional<std::string> user = req.authn_principal.empty() ? "default" : req.authn_principal;
  Status status =
      master_->catalog_manager()->CreateTableWithUser(&request, &response, user);

  if (status.ok()) {
    PrintTableObject(output, response.table_id());
    *status_code = HttpStatusCode::Created;
  } else {
    JsonWriter jw(output, JsonWriter::COMPACT);
    RETURN_JSON_ERROR(jw, status.ToString(), *status_code, HttpStatusCode::ServiceUnavailable);
  }
}

void RestCatalogPathHandlers::HandleGetTable(std::ostringstream* output,
                                             const Webserver::WebRequest& req,
                                             HttpStatusCode* status_code) {
  string table_id = req.path_params.at("table_id");
  PrintTableObject(output, table_id);
  *status_code = HttpStatusCode::Ok;
}

void RestCatalogPathHandlers::HandlePutTable(std::ostringstream* output,
                                             const Webserver::WebRequest& req,
                                             HttpStatusCode* status_code) {
  string table_id = req.path_params.at("table_id");
  AlterTableRequestPB request;
  AlterTableResponsePB response;
  request.mutable_table()->set_table_id(table_id);

  const string& json_str = req.post_data;
  JsonParseOptions opts;
  opts.case_insensitive_enum_parsing = true;
  google::protobuf::util::Status google_status = JsonStringToMessage(json_str, &request, opts);

  if (!google_status.ok()) {
    JsonWriter jw(output, JsonWriter::COMPACT);
    RETURN_JSON_ERROR(jw,
                      Substitute("JSON table object is not correct: $0", json_str),
                      *status_code,
                      HttpStatusCode::BadRequest);
  }

  std::optional<std::string> user = req.authn_principal.empty() ? "default" : req.authn_principal;
  Status status = master_->catalog_manager()->AlterTableWithUser(request, &response, user);

  if (!status.ok()) {
    JsonWriter jw(output, JsonWriter::COMPACT);
    RETURN_JSON_ERROR(jw, status.ToString(), *status_code, HttpStatusCode::ServiceUnavailable);
  }

  IsAlterTableDoneRequestPB check_req;
  IsAlterTableDoneResponsePB check_resp;
  check_req.mutable_table()->set_table_id(table_id);
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(60);

  while (MonoTime::Now() < deadline) {
    status = master_->catalog_manager()->IsAlterTableDone(&check_req, &check_resp, user);

    if (!status.ok()) {
      JsonWriter jw(output, JsonWriter::COMPACT);
      RETURN_JSON_ERROR(jw, status.ToString(), *status_code, HttpStatusCode::ServiceUnavailable);
    }

    if (check_resp.has_error()) {
      JsonWriter jw(output, JsonWriter::COMPACT);
      RETURN_JSON_ERROR(jw,
                        check_resp.error().ShortDebugString(),
                        *status_code,
                        HttpStatusCode::ServiceUnavailable);
    }

    if (check_resp.done()) {
      PrintTableObject(output, table_id);
      *status_code = HttpStatusCode::Ok;
      return;
    }
    SleepFor(MonoDelta::FromMilliseconds(200));
  }
  JsonWriter jw(output, JsonWriter::COMPACT);
  RETURN_JSON_ERROR(jw, "Alter table timed out", *status_code, HttpStatusCode::ServiceUnavailable);
}

void RestCatalogPathHandlers::HandleDeleteTable(std::ostringstream* output,
                                                const Webserver::WebRequest& req,
                                                HttpStatusCode* status_code) {
  string table_id = req.path_params.at("table_id");
  DeleteTableRequestPB request;
  DeleteTableResponsePB response;
  request.mutable_table()->set_table_id(table_id);
  JsonWriter jw(output, JsonWriter::COMPACT);
  std::optional<std::string> user = req.authn_principal.empty() ? "default" : req.authn_principal;
  Status status = master_->catalog_manager()->DeleteTableWithUser(request, &response, user);

  if (status.ok()) {
    *status_code = HttpStatusCode::NoContent;
  } else {
    RETURN_JSON_ERROR(jw, status.ToString(), *status_code, HttpStatusCode::ServiceUnavailable);
  }
}

void RestCatalogPathHandlers::PrintTableObject(std::ostringstream* output, const string& table_id) {
  scoped_refptr<TableInfo> table;
  Status status = master_->catalog_manager()->GetTableInfo(table_id, &table);
  JsonWriter jw(output, JsonWriter::COMPACT);

  jw.StartObject();
  {
    TableMetadataLock l(table.get(), LockMode::READ);
    jw.String("name");
    jw.String(l.data().name());
    jw.String("id");
    jw.String(table_id);
    jw.String("schema");
    jw.Protobuf(l.data().pb.schema());
    jw.String("partition_schema");
    jw.Protobuf(l.data().pb.partition_schema());
    jw.String("owner");
    jw.String(l.data().owner());
    jw.String("comment");
    jw.String(l.data().comment());
    jw.String("extra_config");
    jw.Protobuf(l.data().pb.extra_config());
  }
  jw.EndObject();
}

Status RestCatalogPathHandlers::Register(Webserver* server) {
  server->RegisterPrerenderedPathHandler(
      "/api/v1/tables/<table_id>",
      "",
      [this](const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
        this->HandleApiTableEndpoint(req, resp);
      },
      StyleMode::JSON,
      false);
  server->RegisterPrerenderedPathHandler(
      "/api/v1/tables",
      "",
      [this](const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
        this->HandleApiTablesEndpoint(req, resp);
      },
      StyleMode::JSON,
      false);
  return Status::OK();
}

}  // namespace master
}  // namespace kudu

#undef RETURN_JSON_ERROR
