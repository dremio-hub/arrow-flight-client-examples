/*
 * Copyright (C) 2017-2021 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.adhoc.flight;

import static com.adhoc.flight.QueryRunner.KEY_ROUTING_QUEUE;
import static com.adhoc.flight.QueryRunner.KEY_ROUTING_TAG;
import static com.adhoc.flight.QueryRunner.KEY_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.adhoc.flight.client.AdhocFlightClient;
import com.google.common.base.Strings;

/**
 * Test Adhoc Flight Client with a live Dremio instance.
 */
public class TestAdhocFlightClient {
  private static final String HOST = "localhost";
  private static final int PORT = 32010;
  private static final String USERNAME = "dremio";
  private static final String PASSWORD = "dremio123";
  public static final String SIMPLE_QUERY = "select * from (VALUES(1,2,3),(4,5,6))";
  public static final boolean DISABLE_SERVER_VERIFICATION = true;

  public static final String DEFAULT_SCHEMA_PATH = "$scratch";
  public static final String DEFAULT_ROUTING_TAG = "test-routing-tag";
  public static final String DEFAULT_ROUTING_QUEUE = "Low Cost User Queries";

  public static final String CREATE_TABLE = "create table $scratch.simple_table as " + SIMPLE_QUERY;
  public static final String CREATE_TABLE_NO_SCHEMA = "create table $scratch.simple_table as " + SIMPLE_QUERY;
  public static final String SIMPLE_QUERY_NO_SCHEMA = "SELECT * FROM simple_table";
  public static final String DROP_TABLE = "drop table $scratch.simple_table";
  public static final Map<String, String> EXPECTED_HEADERS = new HashMap<String, String>() {{
      put(KEY_ROUTING_QUEUE, DEFAULT_ROUTING_QUEUE);
      put("engine", "123");
    }
  };

  private AdhocFlightClient client;
  private BufferAllocator allocator;

  @Before
  public void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void shutdown() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(client, allocator);
    client = null;
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  /**
   * Creates a new FlightClient with no client properties set during authentication.
   *
   * @param host            the Dremio host.
   * @param port            the port Dremio Flight Server Endpoint is running on.
   * @param user            the Dremio username.
   * @param pass            the password corresponding to the Dremio username provided.
   * @param patOrAuthToken  the personal access token or OAuth2 token.
   */
  private void createBasicFlightClient(String host, int port, String user, String pass, String patOrAuthToken) {
    createBasicFlightClient(host, port, user, pass, patOrAuthToken, null);
  }

  /**
   * Creates a new FlightClient with client properties set during authentication.
   *
   * @param host             the Dremio host.
   * @param port             the port Dremio Flight Server Endpoint is running on.
   * @param user             the Dremio username.
   * @param pass             the password corresponding to the Dremio username provided.
   * @param patOrAuthToken   the personal access token or OAuth2 token.
   * @param clientProperties Dremio client properties to set during authentication.
   */
  private void createBasicFlightClient(String host, int port,
                                       String user, String pass,
                                       String patOrAuthToken,
                                       HeaderCallOption clientProperties) {
    client = AdhocFlightClient.getBasicClient(allocator, host, port, user, pass, patOrAuthToken,
        clientProperties, null);
  }

  /**
   * Creates a new FlightClient with client properties set during authentication.
   *
   * @param host             the Dremio host.
   * @param port             the port Dremio Flight Server Endpoint is running on.
   * @param user             the Dremio username.
   * @param pass             the password corresponding to the Dremio username provided.
   * @param patOrAuthToken   the personal access token or OAuth2 token.
   * @param clientProperties Dremio client properties to set during authentication.
   */
  private void createEncryptedFlightClientWithDisableServerVerification(String host, int port,
                                                                       String user, String pass,
                                                                       String patOrAuthToken,
                                                                       HeaderCallOption clientProperties)
      throws Exception {
    client = AdhocFlightClient.getEncryptedClient(allocator, host, port, user, pass, null, null,
      null, DISABLE_SERVER_VERIFICATION, clientProperties, null);
  }

  @Test
  public void testSimpleQuery() throws Exception {
    // Create FlightClient connecting to Dremio.
    createBasicFlightClient(HOST, PORT, USERNAME, PASSWORD, null);

    // Select
    client.runQuery(SIMPLE_QUERY, null, null, false);
  }

  @Test
  @Ignore("Need to run flight server in encrypted mode.")
  //TODO Enable encrypted flight server on actions.
  public void testSimpleQueryWithDisableServerVerification() throws Exception {
    // Create FlightClient connecting to Dremio.
    createEncryptedFlightClientWithDisableServerVerification(HOST, PORT, USERNAME, PASSWORD, null, null);

    // Select
    client.runQuery(SIMPLE_QUERY, null, null, false);
  }

  @Test
  public void testSimpleQueryWithClientPropertiesDuringAuth() throws Exception {
    // Create HeaderCallOption to transport Dremio client properties.
    final CallHeaders callHeaders = new FlightCallHeaders();
    callHeaders.insert(KEY_ROUTING_TAG, DEFAULT_ROUTING_TAG);
    callHeaders.insert(KEY_ROUTING_QUEUE, DEFAULT_ROUTING_QUEUE);
    final HeaderCallOption clientProperties = new HeaderCallOption(callHeaders);

    // Create FlightClient connecting to Dremio.
    createBasicFlightClient(HOST, PORT, USERNAME, PASSWORD, null, clientProperties);

    // Create table
    client.runQuery(CREATE_TABLE, null, null, false);

    // Select
    client.runQuery(SIMPLE_QUERY, null, null, false);

    // Drop table
    client.runQuery(DROP_TABLE, null, null, false);
  }

  @Test
  public void testSimpleQueryWithDefaultSchemaPath() throws Exception {
    final CallHeaders callHeaders = new FlightCallHeaders();
    callHeaders.insert(KEY_SCHEMA, DEFAULT_SCHEMA_PATH);
    final HeaderCallOption callOption = new HeaderCallOption(callHeaders);
    // Create FlightClient connecting to Dremio.
    createBasicFlightClient(HOST, PORT, USERNAME, PASSWORD, null);

    // Create table
    client.runQuery(CREATE_TABLE, callOption, null, false);

    // Select
    client.runQuery(SIMPLE_QUERY_NO_SCHEMA, null, null, false);

    // Drop table
    client.runQuery(DROP_TABLE, null, null, false);
  }

  @Test
  public void testBadHostname() {
    final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
        () -> createBasicFlightClient("1.1.1.1", PORT, USERNAME, PASSWORD, null));
    assertEquals(FlightStatusCode.UNAVAILABLE, fre.status().code());
  }

  @Test
  public void testBadPort() {
    final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
        () -> createBasicFlightClient(HOST, 1111, USERNAME, PASSWORD, null));
    assertEquals(FlightStatusCode.UNAVAILABLE, fre.status().code());
  }

  @Test
  public void testBadPassword() {
    final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
        () -> createBasicFlightClient(HOST, PORT, USERNAME, "BAD_PASSWORD", null));
    assertEquals(FlightStatusCode.UNAUTHENTICATED, fre.status().code());
  }

  @Test
  public void testNonExistentUser() {
    final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
        () -> createBasicFlightClient(HOST, PORT, "BAD_USER", PASSWORD, null));
    assertEquals(FlightStatusCode.UNAUTHENTICATED, fre.status().code());
  }

  @Test
  public void testNoAuthProvided() {
    final Exception exception = assertThrows(IllegalArgumentException.class,
        () -> createBasicFlightClient(HOST, PORT, USERNAME, null, null));

    assertTrue(exception.getMessage().contains("No authentication method chosen"));
  }

  @Test
  public void testMultipleAuthProvided() {
    final Exception exception = assertThrows(IllegalArgumentException.class,
        () -> createBasicFlightClient(HOST, PORT, USERNAME, PASSWORD, "SOME_TOKEN"));

    assertTrue(exception.getMessage().contains("Provide exactly one of"));
  }

  @Test
  public void testNoUsernameProvidedWithPassword() {
    final Exception exception = assertThrows(IllegalArgumentException.class,
        () -> createBasicFlightClient(HOST, PORT, null, PASSWORD, null));

    assertTrue(exception.getMessage().contains("Username must be defined for password authentication"));
  }

  @Test
  public void testHeaderPassDown() {
    final CallHeaders callHeaders = new FlightCallHeaders();
    callHeaders.insert(KEY_ROUTING_QUEUE, DEFAULT_ROUTING_QUEUE);
    callHeaders.insert("engine", "123");

    final HeaderCallOption callOption = new HeaderCallOption(callHeaders);

    final HeaderClientMiddlewareFactory clientFactory = new HeaderClientMiddlewareFactory();

    final List<FlightClientMiddleware.Factory> flightClientMiddlewareList = new ArrayList<>();

    flightClientMiddlewareList.add(clientFactory);

    client = AdhocFlightClient.getBasicClient(allocator, HOST, PORT, USERNAME, PASSWORD, null,
      callOption, flightClientMiddlewareList);

    EXPECTED_HEADERS.forEach( (key, value) -> {
      key = key.toLowerCase(Locale.ROOT);
      value = value.toLowerCase(Locale.ROOT);

      if (key.equalsIgnoreCase("authorization")) {
        final String[] authorizationHeaders = clientFactory.headers.get(key).split(" ");

        assertTrue(value.equalsIgnoreCase(authorizationHeaders[0]));
        assertTrue(!Strings.isNullOrEmpty(authorizationHeaders[1]));
      } else {
        assertTrue(value.equalsIgnoreCase(clientFactory.headers.get(key)));
      }
    });
  }

  static class HeaderClientMiddlewareFactory implements FlightClientMiddleware.Factory {
    private Map<String, String> headers;

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      headers = new HashMap<>();
      return new HeaderClientMiddleware(this);
    }
  }

  static class HeaderClientMiddleware implements FlightClientMiddleware {
    private final HeaderClientMiddlewareFactory factory;

    public HeaderClientMiddleware(HeaderClientMiddlewareFactory factory) {
      this.factory = factory;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      outgoingHeaders.keys().forEach( key ->
          factory.headers.put(key, outgoingHeaders.get(key)));
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {

    }

    @Override
    public void onCallCompleted(CallStatus status) {

    }
  }
}
