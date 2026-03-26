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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

class OAuthFlowPropertiesTest {
  @Test
  void createsClientCredentialsProperties() {
    final Properties properties = OAuthFlowProperties.clientCredentials(environment(
        "DREMIO_OAUTH_TOKEN_URI", "https://coordinator.example.com/oauth/token",
        "DREMIO_OAUTH_CLIENT_ID", "service-client-id",
        "DREMIO_OAUTH_CLIENT_SECRET", "service-secret",
        "DREMIO_OAUTH_SCOPE", "dremio.all offline_access",
        "DREMIO_USE_ENCRYPTION", "true"));

    assertEquals("client_credentials", properties.getProperty("oauth.flow"));
    assertEquals("https://coordinator.example.com/oauth/token",
        properties.getProperty("oauth.tokenUri"));
    assertEquals("service-client-id", properties.getProperty("oauth.clientId"));
    assertEquals("service-secret", properties.getProperty("oauth.clientSecret"));
    assertEquals("dremio.all offline_access", properties.getProperty("oauth.scope"));
    assertEquals("true", properties.getProperty("useEncryption"));
  }

  @Test
  void createsTokenExchangeProperties() {
    final Properties properties = OAuthFlowProperties.tokenExchange(environment(
        "DREMIO_OAUTH_TOKEN_URI", "https://coordinator.example.com/oauth/token",
        "DREMIO_OAUTH_SUBJECT_TOKEN", "external-jwt",
        "DREMIO_OAUTH_SUBJECT_TOKEN_TYPE", "urn:ietf:params:oauth:token-type:jwt",
        "DREMIO_OAUTH_ACTOR_TOKEN", "proxy-token",
        "DREMIO_OAUTH_ACTOR_TOKEN_TYPE", "urn:ietf:params:oauth:token-type:access_token",
        "DREMIO_OAUTH_CLIENT_ID", "example-client-id",
        "DREMIO_OAUTH_CLIENT_SECRET", "example-client-secret",
        "DREMIO_OAUTH_AUDIENCE", "https://coordinator.example.com"));

    assertEquals("token_exchange", properties.getProperty("oauth.flow"));
    assertEquals("external-jwt", properties.getProperty("oauth.exchange.subjectToken"));
    assertEquals("urn:ietf:params:oauth:token-type:jwt",
        properties.getProperty("oauth.exchange.subjectTokenType"));
    assertEquals("proxy-token", properties.getProperty("oauth.exchange.actorToken"));
    assertEquals("urn:ietf:params:oauth:token-type:access_token",
        properties.getProperty("oauth.exchange.actorTokenType"));
    assertEquals("example-client-id", properties.getProperty("oauth.clientId"));
    assertEquals("example-client-secret", properties.getProperty("oauth.clientSecret"));
    assertEquals("https://coordinator.example.com",
        properties.getProperty("oauth.exchange.aud"));
  }

  @Test
  void createsDremioImpersonationProperties() {
    final Properties properties = OAuthFlowProperties.dremioUserImpersonation(environment(
        "DREMIO_OAUTH_TOKEN_URI", "https://coordinator.example.com/oauth/token",
        "DREMIO_TARGET_USER", "sharedaccessuser",
        "DREMIO_PROXY_PAT", "proxy-user-pat"));

    assertEquals("token_exchange", properties.getProperty("oauth.flow"));
    assertEquals("sharedaccessuser", properties.getProperty("oauth.exchange.subjectToken"));
    assertEquals(OAuthFlowProperties.DREMIO_SUBJECT_TOKEN_TYPE,
        properties.getProperty("oauth.exchange.subjectTokenType"));
    assertEquals("proxy-user-pat", properties.getProperty("oauth.exchange.actorToken"));
    assertEquals(OAuthFlowProperties.DREMIO_PAT_TOKEN_TYPE,
        properties.getProperty("oauth.exchange.actorTokenType"));
    assertEquals("dremio.all", properties.getProperty("oauth.scope"));
  }

  @Test
  void rejectsIncompleteTokenExchangeActorPair() {
    assertThrows(IllegalArgumentException.class,
        () -> OAuthFlowProperties.tokenExchange(environment(
            "DREMIO_OAUTH_TOKEN_URI", "https://coordinator.example.com/oauth/token",
            "DREMIO_OAUTH_SUBJECT_TOKEN", "external-jwt",
            "DREMIO_OAUTH_SUBJECT_TOKEN_TYPE", "urn:ietf:params:oauth:token-type:jwt",
            "DREMIO_OAUTH_ACTOR_TOKEN", "proxy-token")));
  }

  private static ExampleEnvironment environment(String... keyValues) {
    final Map<String, String> values = new HashMap<>();
    for (int index = 0; index < keyValues.length; index += 2) {
      values.put(keyValues[index], keyValues[index + 1]);
    }
    return ExampleEnvironment.of(values);
  }
}
