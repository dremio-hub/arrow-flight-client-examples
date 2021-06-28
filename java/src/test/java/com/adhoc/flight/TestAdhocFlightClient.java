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

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.adhoc.flight.client.AdhocFlightClient;

/**
 * Test Adhoc Flight Client.
 */
public class TestAdhocFlightClient {
  private static final String HOST = "localhost";
  private static final int PORT = 32010;
  private static final String USERNAME = "dremio";
  private static final String PASSWORD = "dremio123";
  public static final String SIMPLE_QUERY = "select * from (VALUES(1,2,3),(4,5,6))";

  public static final String DEFAULT_SCHEMA_PATH = "$scratch";
  public static final String DEFAULT_ROUTING_TAG = "test-routing-tag";
  public static final String DEFAULT_ROUTING_QUEUE = "Low Cost User Queries";

  public static final String CREATE_TABLE = "create table $scratch.simple_table as " + SIMPLE_QUERY;
  public static final String CREATE_TABLE_NO_SCHEMA = "create table $scratch.simple_table as " + SIMPLE_QUERY;
  public static final String SIMPLE_QUERY_NO_SCHEMA = "SELECT * FROM simple_table";
  public static final String DROP_TABLE = "drop table $scratch.simple_table";

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
   * @param host the Drmeio host.
   * @param port the port Dremio Flight Server Endpoint is running on.
   * @param user the Dremio username.
   * @param pass the password corresponding to the Dremio username provided.
   */
  private void createBasicFlightClient(String host, int port, String user, String pass) {
    createBasicFlightClient(host, port, user, pass, null);
  }

  /**
   * Creates a new FlightClient with client properties set during authentication.
   *
   * @param host             the Drmeio host.
   * @param port             the port Dremio Flight Server Endpoint is running on.
   * @param user             the Dremio username.
   * @param pass             the password corresponding to the Dremio username provided.
   * @param clientProperties Dremio client properties to set during authentication.
   */
  private void createBasicFlightClient(String host, int port,
                                       String user, String pass,
                                       HeaderCallOption clientProperties) {
    client = AdhocFlightClient.getBasicClient(allocator, host, port, user, pass, clientProperties);
  }

  @Test
  public void testSimpleQuery() throws Exception {
    // Create FlightClient connecting to Dremio.
    createBasicFlightClient(HOST, PORT, USERNAME, PASSWORD);

    // Select
    client.runQuery(SIMPLE_QUERY, true);
  }

  @Test
  public void testSimpleQueryWithClientPropertiesDuringAuth() throws Exception {
    // Create HeaderCallOption to transport Dremio client properties.
    final CallHeaders callHeaders = new FlightCallHeaders();
    callHeaders.insert("schema", DEFAULT_SCHEMA_PATH);
    callHeaders.insert("routing_tag", DEFAULT_ROUTING_TAG);
    callHeaders.insert("routing_queue", DEFAULT_ROUTING_QUEUE);
    final HeaderCallOption clientProperties = new HeaderCallOption(callHeaders);

    // Create FlightClient connecting to Dremio.
    createBasicFlightClient(HOST, PORT, USERNAME, PASSWORD, clientProperties);

    // Create table
    client.runQuery(CREATE_TABLE_NO_SCHEMA, true);

    // Select
    client.runQuery(SIMPLE_QUERY_NO_SCHEMA, true);

    // Drop table
    client.runQuery(DROP_TABLE, true);
  }

  @Test
  public void testSimpleQueryWithDefaultSchemaPath() throws Exception {
    // Create FlightClient connecting to Drmeio.
    createBasicFlightClient(HOST, PORT, USERNAME, PASSWORD);

    // Create table
    client.runQuery(CREATE_TABLE, true);

    // Select
    final CallHeaders callHeaders = new FlightCallHeaders();
    callHeaders.insert("schema", DEFAULT_SCHEMA_PATH);
    final HeaderCallOption callOption = new HeaderCallOption(callHeaders);
    client.runQuery(SIMPLE_QUERY_NO_SCHEMA, callOption, true);

    // Drop table
    client.runQuery(DROP_TABLE, true);
  }

  @Test
  public void testBadHostname() {
    final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
        () -> createBasicFlightClient("1.1.1.1", PORT, USERNAME, PASSWORD));
    assertEquals(FlightStatusCode.UNAVAILABLE, fre.status().code());
  }

  @Test
  public void testBadPort() {
    final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
        () -> createBasicFlightClient(HOST, 1111, USERNAME, PASSWORD));
    assertEquals(FlightStatusCode.UNAVAILABLE, fre.status().code());
  }

  @Test
  public void testBadPassword() {
    final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
        () -> createBasicFlightClient(HOST, PORT, USERNAME, "BAD_PASSWORD"));
    assertEquals(FlightStatusCode.UNAUTHENTICATED, fre.status().code());
  }

  @Test
  public void testNonExistentUser() {
    final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
        () -> createBasicFlightClient(HOST, PORT, "BAD_USER", PASSWORD));
    assertEquals(FlightStatusCode.UNAUTHENTICATED, fre.status().code());

  }
}
