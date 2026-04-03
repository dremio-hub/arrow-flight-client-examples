/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dremio.examples.jdbc.oauth;

import com.beust.jcommander.Parameter;
import java.util.Properties;

final class OAuthParams {
  private static final String DEFAULT_SCOPE = "dremio.all";

  @Parameter(names = "--oauth-token-uri", description = "OAuth token endpoint", required = true)
  String tokenUri;

  @Parameter(names = "--oauth-scope", description = "OAuth scope")
  String scope = DEFAULT_SCOPE;

  @Parameter(names = "--oauth-resource", description = "RFC 8707 resource indicator")
  String resource;

  void applyTo(Properties properties) {
    properties.setProperty("oauth.tokenUri", tokenUri);
    properties.setProperty("oauth.scope", scope);
    ConnectionParams.setIfPresent(properties, "oauth.resource", resource);
  }
}
