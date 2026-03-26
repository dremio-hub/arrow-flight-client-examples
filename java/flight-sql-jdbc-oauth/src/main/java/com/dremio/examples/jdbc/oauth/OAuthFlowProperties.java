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

import java.util.Optional;
import java.util.Properties;

final class OAuthFlowProperties {
  static final String DREMIO_PAT_TOKEN_TYPE =
      "urn:ietf:params:oauth:token-type:dremio:personal-access-token";
  static final String DREMIO_SUBJECT_TOKEN_TYPE =
      "urn:ietf:params:oauth:token-type:dremio:subject";
  private static final String DEFAULT_SCOPE = "dremio.all";

  private OAuthFlowProperties() {
  }

  static Properties clientCredentials(ExampleEnvironment environment) {
    final Properties properties =
        createBaseOAuthProperties(environment, "client_credentials");

    properties.setProperty("oauth.clientId",
        environment.require("DREMIO_OAUTH_CLIENT_ID"));
    properties.setProperty("oauth.clientSecret",
        environment.require("DREMIO_OAUTH_CLIENT_SECRET"));

    return properties;
  }

  static Properties tokenExchange(ExampleEnvironment environment) {
    final Properties properties =
        createBaseOAuthProperties(environment, "token_exchange");

    properties.setProperty("oauth.exchange.subjectToken",
        environment.require("DREMIO_OAUTH_SUBJECT_TOKEN"));
    properties.setProperty("oauth.exchange.subjectTokenType",
        environment.require("DREMIO_OAUTH_SUBJECT_TOKEN_TYPE"));

    addOptionalPair(properties, environment,
        "oauth.exchange.actorToken", "DREMIO_OAUTH_ACTOR_TOKEN",
        "oauth.exchange.actorTokenType", "DREMIO_OAUTH_ACTOR_TOKEN_TYPE");

    addTokenExchangeExtensions(properties, environment);

    return properties;
  }

  static Properties dremioUserImpersonation(ExampleEnvironment environment) {
    final Properties properties =
        createBaseOAuthProperties(environment, "token_exchange");

    properties.setProperty("oauth.exchange.subjectToken",
        environment.require("DREMIO_TARGET_USER"));
    properties.setProperty("oauth.exchange.subjectTokenType",
        DREMIO_SUBJECT_TOKEN_TYPE);
    properties.setProperty("oauth.exchange.actorToken",
        environment.require("DREMIO_PROXY_PAT"));
    properties.setProperty("oauth.exchange.actorTokenType",
        DREMIO_PAT_TOKEN_TYPE);

    addTokenExchangeExtensions(properties, environment);

    return properties;
  }

  private static Properties createBaseOAuthProperties(ExampleEnvironment environment,
      String flow) {
    final Properties properties =
        FlightSqlExampleSupport.baseConnectionProperties(environment);

    properties.setProperty("oauth.flow", flow);
    properties.setProperty("oauth.tokenUri",
        environment.require("DREMIO_OAUTH_TOKEN_URI"));
    properties.setProperty("oauth.scope",
        environment.optional("DREMIO_OAUTH_SCOPE").orElse(DEFAULT_SCOPE));
    FlightSqlExampleSupport.setIfPresent(properties, "oauth.resource",
        environment.optional("DREMIO_OAUTH_RESOURCE"));

    return properties;
  }

  private static void addTokenExchangeExtensions(Properties properties,
      ExampleEnvironment environment) {
    addOptionalPair(properties, environment,
        "oauth.clientId", "DREMIO_OAUTH_CLIENT_ID",
        "oauth.clientSecret", "DREMIO_OAUTH_CLIENT_SECRET");
    FlightSqlExampleSupport.setIfPresent(properties, "oauth.exchange.aud",
        environment.optional("DREMIO_OAUTH_AUDIENCE"));
    FlightSqlExampleSupport.setIfPresent(properties, "oauth.exchange.requestedTokenType",
        environment.optional("DREMIO_OAUTH_REQUESTED_TOKEN_TYPE"));
  }

  private static void addOptionalPair(Properties properties,
      ExampleEnvironment environment, String firstPropertyName, String firstEnvVar,
      String secondPropertyName, String secondEnvVar) {
    final Optional<String> firstValue = environment.optional(firstEnvVar);
    final Optional<String> secondValue = environment.optional(secondEnvVar);

    if (firstValue.isPresent() != secondValue.isPresent()) {
      throw new IllegalArgumentException(
          "Environment variables " + firstEnvVar + " and "
              + secondEnvVar + " must be provided together.");
    }

    if (firstValue.isPresent()) {
      properties.setProperty(firstPropertyName, firstValue.get());
      properties.setProperty(secondPropertyName, secondValue.get());
    }
  }
}
