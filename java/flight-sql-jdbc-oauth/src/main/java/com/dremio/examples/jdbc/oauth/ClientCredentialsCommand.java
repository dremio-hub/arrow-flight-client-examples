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
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import java.util.Properties;

@Parameters(commandNames = "client-credentials",
    commandDescription = "OAuth 2.0 Client Credentials flow")
final class ClientCredentialsCommand {
  @ParametersDelegate
  ConnectionParams connection = new ConnectionParams();

  @ParametersDelegate
  OAuthParams oauth = new OAuthParams();

  @Parameter(names = "--oauth-client-id", description = "OAuth client ID", required = true)
  String clientId;

  @Parameter(names = "--oauth-client-secret", description = "OAuth client secret", required = true)
  String clientSecret;

  Properties toProperties() {
    final Properties properties = connection.toProperties();
    properties.setProperty("oauth.flow", "client_credentials");
    oauth.applyTo(properties);
    properties.setProperty("oauth.clientId", clientId);
    properties.setProperty("oauth.clientSecret", clientSecret);
    return properties;
  }
}
