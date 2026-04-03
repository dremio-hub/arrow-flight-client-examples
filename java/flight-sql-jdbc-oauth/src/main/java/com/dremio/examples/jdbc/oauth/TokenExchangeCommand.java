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
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import java.util.Properties;

@Parameters(commandNames = "token-exchange",
    commandDescription = "OAuth 2.0 Token Exchange flow")
final class TokenExchangeCommand {
  @ParametersDelegate
  ConnectionParams connection = new ConnectionParams();

  @ParametersDelegate
  OAuthParams oauth = new OAuthParams();

  @Parameter(names = "--oauth-subject-token", description = "Subject token", required = true)
  String subjectToken;

  @Parameter(names = "--oauth-subject-token-type", description = "Subject token type URN",
      required = true)
  String subjectTokenType;

  @Parameter(names = "--oauth-actor-token", description = "Actor token")
  String actorToken;

  @Parameter(names = "--oauth-actor-token-type", description = "Actor token type URN")
  String actorTokenType;

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
    requirePair(actorToken, "--oauth-actor-token", actorTokenType, "--oauth-actor-token-type");
    requirePair(clientId, "--oauth-client-id", clientSecret, "--oauth-client-secret");

    final Properties properties = connection.toProperties();
    properties.setProperty("oauth.flow", "token_exchange");
    oauth.applyTo(properties);

    properties.setProperty("oauth.exchange.subjectToken", subjectToken);
    properties.setProperty("oauth.exchange.subjectTokenType", subjectTokenType);

    ConnectionParams.setIfPresent(properties, "oauth.exchange.actorToken", actorToken);
    ConnectionParams.setIfPresent(properties, "oauth.exchange.actorTokenType", actorTokenType);
    ConnectionParams.setIfPresent(properties, "oauth.clientId", clientId);
    ConnectionParams.setIfPresent(properties, "oauth.clientSecret", clientSecret);
    ConnectionParams.setIfPresent(properties, "oauth.exchange.aud", audience);
    ConnectionParams.setIfPresent(properties, "oauth.exchange.requestedTokenType",
        requestedTokenType);

    return properties;
  }

  static void requirePair(String first, String firstName, String second, String secondName) {
    if ((first == null) != (second == null)) {
      throw new ParameterException(firstName + " and " + secondName
          + " must be provided together.");
    }
  }
}
