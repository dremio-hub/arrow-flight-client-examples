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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.beust.jcommander.ParameterException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

class CommandPropertiesTest {
  @Test
  void createsClientCredentialsProperties() {
    final ClientCredentialsCommand cmd = new ClientCredentialsCommand();
    cmd.oauth.tokenUri = "https://coordinator.example.com/oauth/token";
    cmd.clientId = "service-client-id";
    cmd.clientSecret = "service-secret";
    cmd.oauth.scope = "dremio.all offline_access";
    cmd.connection.useEncryption = true;

    final Properties properties = cmd.toProperties();

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
    final TokenExchangeCommand cmd = new TokenExchangeCommand();
    cmd.oauth.tokenUri = "https://coordinator.example.com/oauth/token";
    cmd.subjectToken = "external-jwt";
    cmd.subjectTokenType = "urn:ietf:params:oauth:token-type:jwt";
    cmd.actorToken = "proxy-token";
    cmd.actorTokenType = "urn:ietf:params:oauth:token-type:access_token";
    cmd.clientId = "example-client-id";
    cmd.clientSecret = "example-client-secret";
    cmd.audience = "https://coordinator.example.com";

    final Properties properties = cmd.toProperties();

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
    final DremioImpersonationCommand cmd = new DremioImpersonationCommand();
    cmd.oauth.tokenUri = "https://coordinator.example.com/oauth/token";
    cmd.targetUser = "sharedaccessuser";
    cmd.proxyPat = "proxy-user-pat";

    final Properties properties = cmd.toProperties();

    assertEquals("token_exchange", properties.getProperty("oauth.flow"));
    assertEquals("sharedaccessuser", properties.getProperty("oauth.exchange.subjectToken"));
    assertEquals(DremioImpersonationCommand.DREMIO_SUBJECT_TOKEN_TYPE,
        properties.getProperty("oauth.exchange.subjectTokenType"));
    assertEquals("proxy-user-pat", properties.getProperty("oauth.exchange.actorToken"));
    assertEquals(DremioImpersonationCommand.DREMIO_PAT_TOKEN_TYPE,
        properties.getProperty("oauth.exchange.actorTokenType"));
    assertEquals("dremio.all", properties.getProperty("oauth.scope"));
  }

  @Test
  void rejectsIncompleteTokenExchangeActorPair() {
    final TokenExchangeCommand cmd = new TokenExchangeCommand();
    cmd.oauth.tokenUri = "https://coordinator.example.com/oauth/token";
    cmd.subjectToken = "external-jwt";
    cmd.subjectTokenType = "urn:ietf:params:oauth:token-type:jwt";
    cmd.actorToken = "proxy-token";
    // actorTokenType intentionally null

    assertThrows(ParameterException.class, cmd::toProperties);
  }

  @Test
  void rejectsIncompleteClientCredentialsPair() {
    final TokenExchangeCommand cmd = new TokenExchangeCommand();
    cmd.oauth.tokenUri = "https://coordinator.example.com/oauth/token";
    cmd.subjectToken = "external-jwt";
    cmd.subjectTokenType = "urn:ietf:params:oauth:token-type:jwt";
    cmd.clientId = "only-id-no-secret";
    // clientSecret intentionally null

    assertThrows(ParameterException.class, cmd::toProperties);
  }

  @Test
  void omitsOptionalFieldsWhenNull() {
    final TokenExchangeCommand cmd = new TokenExchangeCommand();
    cmd.oauth.tokenUri = "https://coordinator.example.com/oauth/token";
    cmd.subjectToken = "external-jwt";
    cmd.subjectTokenType = "urn:ietf:params:oauth:token-type:jwt";

    final Properties properties = cmd.toProperties();

    assertNull(properties.getProperty("oauth.exchange.actorToken"));
    assertNull(properties.getProperty("oauth.clientId"));
    assertNull(properties.getProperty("oauth.exchange.aud"));
  }

  @Test
  void connectionUrlUsesHostAndPort() {
    final ConnectionParams connection = new ConnectionParams();
    connection.host = "flight.example.com";
    connection.port = 443;

    assertEquals("jdbc:arrow-flight-sql://flight.example.com:443", connection.connectionUrl());
  }
}
