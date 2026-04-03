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

@Parameters(commandNames = "dremio-impersonation",
    commandDescription = "Dremio user impersonation via Token Exchange")
final class DremioImpersonationCommand {
  static final String DREMIO_PAT_TOKEN_TYPE =
      "urn:ietf:params:oauth:token-type:dremio:personal-access-token";
  static final String DREMIO_SUBJECT_TOKEN_TYPE =
      "urn:ietf:params:oauth:token-type:dremio:subject";

  @ParametersDelegate
  ConnectionParams connection = new ConnectionParams();

  @ParametersDelegate
  OAuthParams oauth = new OAuthParams();

  @Parameter(names = "--target-user", description = "Dremio user to impersonate", required = true)
  String targetUser;

  @Parameter(names = "--proxy-pat",
      description = "PAT of the proxy user authorized to impersonate", required = true)
  String proxyPat;

  @Parameter(names = "--oauth-client-id", description = "OAuth client ID")
  String clientId;

  @Parameter(names = "--oauth-client-secret", description = "OAuth client secret")
  String clientSecret;

  @Parameter(names = "--oauth-audience", description = "Token exchange audience")
  String audience;

  @Parameter(names = "--oauth-requested-token-type",
      description = "Requested token type URN")
  String requestedTokenType;

  Properties toProperties() {
    TokenExchangeCommand.requirePair(clientId, "--oauth-client-id",
        clientSecret, "--oauth-client-secret");

    final Properties properties = connection.toProperties();
    properties.setProperty("oauth.flow", "token_exchange");
    oauth.applyTo(properties);

    properties.setProperty("oauth.exchange.subjectToken", targetUser);
    properties.setProperty("oauth.exchange.subjectTokenType", DREMIO_SUBJECT_TOKEN_TYPE);
    properties.setProperty("oauth.exchange.actorToken", proxyPat);
    properties.setProperty("oauth.exchange.actorTokenType", DREMIO_PAT_TOKEN_TYPE);

    ConnectionParams.setIfPresent(properties, "oauth.clientId", clientId);
    ConnectionParams.setIfPresent(properties, "oauth.clientSecret", clientSecret);
    ConnectionParams.setIfPresent(properties, "oauth.exchange.aud", audience);
    ConnectionParams.setIfPresent(properties, "oauth.exchange.requestedTokenType",
        requestedTokenType);

    return properties;
  }
}
