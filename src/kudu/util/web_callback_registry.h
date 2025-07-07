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
#pragma once

#include <functional>
#include <sstream>
#include <string>
#include <unordered_map>

#include "kudu/util/easy_json.h"

namespace kudu {

enum class HttpStatusCode {
  Ok, // 200
  Created, // 201
  NoContent, // 204
  TemporaryRedirect, //307
  BadRequest, // 400
  AuthenticationRequired, // 401
  Forbidden, // 403
  NotFound, // 404
  MethodNotAllowed, // 405
  LengthRequired, // 411
  RequestEntityTooLarge, // 413
  InternalServerError, // 500
  ServiceUnavailable, // 503
  GatewayTimeout // 504
};

// StyleMode is an enumeration used to define the format of the server's response to the client.
// This format determines how the response data is presented and interpreted by the client.
enum class StyleMode {
  // This mode includes additional styling elements in the response,
  // such as CSS, navigation bar, etc.
  STYLED,
  // In this mode, the response is sent without any styling elements.
  UNSTYLED,
  // In rare cases when a binary data is sent as a response.
  BINARY,
  // This mode is used when the server response is in JSON format.
  JSON
};

// Interface for registering webserver callbacks.
//
// To register a webserver callback for /example/path:
//
// 1. Define a PathHandlerCallback that accepts an EasyJson
//    object and fills out its fields with relevant information.
// 2. Call RegisterPathHandler("/example/path", ...)
// 3. Create the file $KUDU_HOME/www/example/path.mustache.
class WebCallbackRegistry {
 public:
  typedef std::unordered_map<std::string, std::string> ArgumentMap;

  struct WebRequest {
    // The query string, parsed into key/value argument pairs.
    ArgumentMap parsed_args;

    // The HTTP request headers.
    ArgumentMap request_headers;

    // The authenticated principal, if any.
    std::string authn_principal;

    // The raw query string passed in the URL. May be empty.
    std::string query_string;

    // The method (POST/GET/etc).
    std::string request_method;

    // In the case of a POST, the posted data.
    std::string post_data;

    // Parameters extracted from the URL path.
    ArgumentMap path_params;
  };

  // A response to an HTTP request whose body is rendered by template.
  struct WebResponse {
    // Determines the status code of the HTTP response.
    HttpStatusCode status_code = HttpStatusCode::Ok;

    // Additional headers added to the HTTP response.
    ArgumentMap response_headers;

    // A JSON object to be rendered to HTML by a mustache template.
    EasyJson output;
  };

  // A response to an HTTP request.
  struct PrerenderedWebResponse {
    // Determines the status code of the HTTP response.
    HttpStatusCode status_code = HttpStatusCode::Ok;

    // Additional headers added to the HTTP response.
    ArgumentMap response_headers;

    // The fully-rendered HTML response body or a binary blob in case of
    // responses with 'application/octet-stream' Content-Type.
    std::ostringstream output;
  };

  // A function that handles an HTTP request where the response body will be rendered
  // with a mustache template from the JSON object held by 'resp'.
  typedef std::function<void (const WebRequest& args, WebResponse* resp)>
      PathHandlerCallback;

  // A function that handles an HTTP request, where the response body is the contents
  // of the 'output' member of 'resp'.
  typedef std::function<void (const WebRequest& args, PrerenderedWebResponse* resp)>
      PrerenderedPathHandlerCallback;

  virtual ~WebCallbackRegistry() = default;

  // Register a route 'path' to be rendered via template.
  // The appropriate template to use is determined by 'path'.
  // If 'style_mode' is StyleMode::STYLED, the page will be styled and include a header and footer.
  // If 'is_on_nav_bar' is true, a link to the page will be placed on the navbar
  // in the header of styled pages. The link text is given by 'alias'.
  // If 'skip_auth' is true, this endpoint will bypass SPNEGO authentication even when
  // --webserver_require_spnego is enabled.
  virtual void RegisterPathHandler(const std::string& path, const std::string& alias,
                                   const PathHandlerCallback& callback,
                                   StyleMode style_mode, bool is_on_nav_bar,
                                   bool skip_auth = false) = 0;

  // Register a route 'path'. See the RegisterPathHandler for details.
  virtual void RegisterPrerenderedPathHandler(const std::string& path, const std::string& alias,
                                              const PrerenderedPathHandlerCallback& callback,
                                              StyleMode style_mode,
                                              bool is_on_nav_bar,
                                              bool skip_auth = false) = 0;

  // Register route 'path' for application/octet-stream (binary data) responses.
  virtual void RegisterBinaryDataPathHandler(
      const std::string& path,
      const std::string& alias,
      const PrerenderedPathHandlerCallback& callback,
      bool skip_auth = false) = 0;

  // Register route 'path' for application/json responses.
  virtual void RegisterJsonPathHandler(
      const std::string& path,
      const std::string& alias,
      const PrerenderedPathHandlerCallback& callback,
      bool is_on_nav_bar,
      bool skip_auth = false) = 0;

  // Returns true if 'req' was proxied via Knox, false otherwise.
  static bool IsProxiedViaKnox(const WebRequest& req);
};

} // namespace kudu
