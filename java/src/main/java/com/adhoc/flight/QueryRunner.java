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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.adhoc.flight.client.AdhocFlightClient;
import com.adhoc.flight.config.ConfigurationData;
import com.adhoc.flight.config.ConfigurationException;
import com.adhoc.flight.config.ConfigurationManager;
import com.adhoc.flight.utils.QueryUtils;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

/**
 * Java Flight sample application that runs the specified query.
 */
public class QueryRunner {
  private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator(Integer.MAX_VALUE);
  private static ConfigurationData CONFIGURATION;

  private static final String DEMO_TABLE = "dremio_flight_demo_table";
  private static final String DEMO_TABLE_SCHEMA = "$scratch";
  private static final String DEMO_CREATE_TABLE =
      String.format("CREATE TABLE %s.%s as select * from (VALUES(1,2,3),(4,5,6))",
        DEMO_TABLE_SCHEMA, DEMO_TABLE);
  private static final String DEMO_DROP_TABLE = String.format("DROP TABLE %s.%s",
      DEMO_TABLE_SCHEMA, DEMO_TABLE);
  private static final String DEMO_SELECT_TABLE = String.format("SELECT * FROM %s", DEMO_TABLE);
  private static final String DEMO_ROUTING_TAG = "test-routing-tag";
  private static final String DEMO_ROUTING_QUEUE = "Low Cost User Queries";
  private static final String DEMO_USERNAME = "dremio";
  private static final String DEMO_PASSWORD = "dremio123";

  public static final String KEY_SCHEMA = "SCHEMA";
  public static final String KEY_ROUTING_TAG = "ROUTING_TAG";
  public static final String KEY_ROUTING_QUEUE = "ROUTING_QUEUE";
  public static final String KEY_ROUTING_ENGINE = "ROUTING_ENGINE";



  /**
   * Runs a self contained demo to authenticate and query a Dremio Flight Server Endpoint.
   *
   * @throws Exception If there are issues running queries against the Dremio Arrow Flight
   *                   Server Endpoint.
   *                   - FlightRuntimeError with Flight status code:
   *                   - UNAUTHENTICATED: unable to authenticate against Dremio with given username and password.
   *                   - INVALID_ARGUMENT: issues parsing query input.
   *                   - UNAUTHORIZED: Dremio user is not authorized to access the dataset.
   *                   - UNAVAILABLE: Dremio resource is not available.
   *                   - TIMED_OUT: timed out trying to access Dremio resources.
   */
  public static void runDemo() throws Exception {
    System.out.println("\n[INFO] Running demo to query Dremio Flight Server Endpoint.");
    System.out.println("[INFO] Configured Dremio Flight Server Endpoint host: " + CONFIGURATION.host);
    System.out.println("[INFO] Configured Dremio Flight Server Endpoint port: " + CONFIGURATION.port);

    /**
     * Authentication
     */
    System.out.println("[INFO] [STEP 1]: Authenticating with the Dremio server using Arrow Flight " +
        "authorization header authentication.");
    System.out.println("[INFO] Initial UserSession client properties are set as well.");
    System.out.println(String.format("[INFO] Setting client property: %s => %s",
        KEY_ROUTING_TAG, DEMO_ROUTING_TAG));
    System.out.println(String.format("[INFO] Setting client property: %s => %s",
        KEY_ROUTING_QUEUE, DEMO_ROUTING_QUEUE));

    // Set routing-tag and routing-queue during initial authentication.
    final Map<String, String> properties = ImmutableMap.of(
        KEY_ROUTING_TAG, DEMO_ROUTING_TAG,
        KEY_ROUTING_QUEUE, DEMO_ROUTING_QUEUE);
    final HeaderCallOption routingCallOption = createClientProperties(properties);

    // Authenticates FlightClient with routing properties.
    try (final AdhocFlightClient client = createFlightClient(routingCallOption)) {
      QueryUtils.printAuthenticated(CONFIGURATION.host, CONFIGURATION.port);

      /**
       * Create demo table in $scratch
       */
      // Set default schema path to "$scratch" for the next and following FlightRPC requests.
      System.out.println(String.format("[INFO] Setting client property: %s => %s",
          KEY_SCHEMA, DEMO_TABLE_SCHEMA));
      final Map<String, String> schemaProperty = ImmutableMap.of(
          KEY_SCHEMA, DEMO_TABLE_SCHEMA);
      final HeaderCallOption schemaCallOption = createClientProperties(schemaProperty);
      System.out.println(String.format("[INFO] [STEP 2]: Create a test table in %s", DEMO_TABLE_SCHEMA));
      client.runQuery(DEMO_CREATE_TABLE, schemaCallOption, null, true);
      System.out.println(String.format("[INFO] Created %s.%s in %s successfully.",
          DEMO_TABLE_SCHEMA, DEMO_TABLE, DEMO_TABLE_SCHEMA));

      /**
       * Query demo table
       */
      System.out.println(String.format("[INFO] [STEP 3]: Query demo table %s.%s",
          DEMO_TABLE_SCHEMA, DEMO_TABLE));
      QueryUtils.printRunningQuery(DEMO_SELECT_TABLE);

      // Run query "select * from dremio_flight_demo_table" without schema path.
      client.runQuery(DEMO_SELECT_TABLE, schemaCallOption, null, true);
      System.out.println();

      /**
       * Drop Demo Table
       */
      System.out.println("[INFO] [STEP 5]: Drop demo table.");
      client.runQuery(DEMO_DROP_TABLE, schemaCallOption, null, true);
      System.out.println(String.format("[INFO] Dropped %s.%s successfully", KEY_SCHEMA, DEMO_TABLE_SCHEMA));
    } catch (Exception ex) {
      System.out.println("[ERROR] Exception: " + ex.getMessage());
      ex.printStackTrace();
    }
  }

  /**
   * An adhoc method to run a user query.
   * <p>
   * Note: This adhoc does not use any client properties.
   * Please See demo above for client properties usage.
   *
   * @param pathToSaveQueryResultsTo the file path to which the binary data for the
   *                                 {@link VectorSchemaRoot} with the query results.
   * @throws Exception If there are issues running queries against the Dremio Arrow Flight
   *                   Server Endpoint.
   *                   - FlightRuntimeError with Flight status code:
   *                   - UNAUTHENTICATED: unable to authenticate against Dremio with given credentials.
   *                   - INVALID_ARGUMENT: issues parsing query input.
   *                   - UNAUTHORIZED: Dremio user is not authorized to access the dataset.
   *                   - UNAVAILABLE: Dremio resource is not available.
   *                   - TIMED_OUT: timed out trying to access Dremio resources.
   */
  public static void runAdhoc(String pathToSaveQueryResultsTo) throws Exception {


    final Map<String, String> sessionPropertiesMap = new HashMap<>();

    CONFIGURATION.sessionProperties.forEach( sessionProperty -> {
      sessionPropertiesMap.put(sessionProperty.getKey(), sessionProperty.getValue());
    });

    if (!Strings.isNullOrEmpty(CONFIGURATION.engine)) {
      sessionPropertiesMap.put(KEY_ROUTING_ENGINE, CONFIGURATION.engine);
    }

    final HeaderCallOption clientProperties = createClientProperties(sessionPropertiesMap);

    try (final AdhocFlightClient client = createFlightClient(clientProperties)) {

      /**
       * Authentication
       */
      QueryUtils.printAuthenticated(CONFIGURATION.host, CONFIGURATION.port);

      /**
       * Run Query
       */
      QueryUtils.printRunningQuery(CONFIGURATION.query);

      if (pathToSaveQueryResultsTo != null) {
        client.runQuery(CONFIGURATION.query, clientProperties, new File(pathToSaveQueryResultsTo), true);
      } else {
        client.runQuery(CONFIGURATION.query, clientProperties, null, true);
      }
    } catch (Exception ex) {
      System.out.println("[ERROR] Exception: " + ex.getMessage());
      ex.printStackTrace();
    }
  }

  /**
   * An adhoc method to run a user query.
   * <p>
   * Note: This adhoc does not use any client properties.
   * Please See demo above for client properties usage.
   *
   * @throws Exception If there are issues running queries against the Dremio Arrow Flight
   *                   Server Endpoint.
   *                   - FlightRuntimeError with Flight status code:
   *                   - UNAUTHENTICATED: unable to authenticate against Dremio with given credentials.
   *                   - INVALID_ARGUMENT: issues parsing query input.
   *                   - UNAUTHORIZED: Dremio user is not authorized to access the dataset.
   *                   - UNAVAILABLE: Dremio resource is not available.
   *                   - TIMED_OUT: timed out trying to access Dremio resources.
   */
  public static void runAdhoc() throws Exception {

    runAdhoc(null);
  }

  public static void main(String[] args) throws Exception {
    try {
      // Parse configuration from all sources
      ConfigurationManager configManager = new ConfigurationManager();
      CONFIGURATION = configManager.parseConfiguration(args);

      // Validate configuration
      CONFIGURATION.validate();

      // Print configuration sources if verbose mode is enabled
      if (CONFIGURATION.verbose) {
        configManager.printConfigurationSources();
        CONFIGURATION.printConfiguration();
      }

      if (CONFIGURATION.help) {
        System.exit(1);
      } else if (CONFIGURATION.runDemo) {
        runDemo();
      } else {
        runAdhoc(CONFIGURATION.pathToSaveQueryResultsTo);
      }
    } catch (ConfigurationException e) {
      System.err.println("Configuration error: " + e.getMessage());
      System.exit(1);
    } finally {
      BUFFER_ALLOCATOR.close();
    }
  }

  /**
   * Creates a FlightClient instance based on command line arguments provided.
   *
   * @param clientProperties Dremio client properties.
   * @return an instance of AdhocFlightClient encapsulating the connected FlightClient instance
   *      and the CredentialCallOption with a bearer token to use in subsequent requests.
   * @throws Exception If there are issues running queries against the Dremio Arrow Flight
   *                   Server Endpoint.
   *                   - FlightRuntimeError with Flight status code:
   *                   - UNAUTHENTICATED: unable to authenticate against Dremio with given credentials.
   *                   - INVALID_ARGUMENT: issues parsing query input.
   *                   - UNAUTHORIZED: Dremio user is not authorized to access the dataset.
   *                   - UNAVAILABLE: Dremio resource is not available.
   *                   - TIMED_OUT: timed out trying to access Dremio resources.
   */
  private static AdhocFlightClient createFlightClient(HeaderCallOption clientProperties) throws Exception {
    // If no auth method provided, default to demo username/password
    if (Strings.isNullOrEmpty(CONFIGURATION.patOrAuthToken)) {
      if (Strings.isNullOrEmpty(CONFIGURATION.user)) {
        CONFIGURATION.user = DEMO_USERNAME;
      }

      if (Strings.isNullOrEmpty(CONFIGURATION.pass)) {
        CONFIGURATION.pass = DEMO_PASSWORD;
      }
    }

    if (CONFIGURATION.enableTls) {
      return AdhocFlightClient.getEncryptedClient(BUFFER_ALLOCATOR,
          CONFIGURATION.host, CONFIGURATION.port,
          CONFIGURATION.user, CONFIGURATION.pass,
          CONFIGURATION.patOrAuthToken,
          CONFIGURATION.keystorePath, CONFIGURATION.keystorePass,
          CONFIGURATION.disableServerVerification,
          CONFIGURATION.projectId,
          clientProperties,
          null);
    } else {
      return AdhocFlightClient.getBasicClient(BUFFER_ALLOCATOR,
          CONFIGURATION.host, CONFIGURATION.port,
          CONFIGURATION.user, CONFIGURATION.pass,
          CONFIGURATION.patOrAuthToken,
          CONFIGURATION.projectId,
          clientProperties,
          null);
    }
  }

  /**
   * Given a map of client properties strings, insert each entry into a Flight CallHeaders object.
   * Then return an instance of HeaderCallOption encapsulating the CallHeaders with Dremio client
   * properties.
   *
   * @param clientProperties Dremio client properties.
   * @return a HeaderCallOption encapsulating provided key, value property pairs.
   */
  private static HeaderCallOption createClientProperties(Map<String, String> clientProperties) {
    final CallHeaders callHeaders = new FlightCallHeaders();
    clientProperties.forEach(callHeaders::insert);
    return new HeaderCallOption(callHeaders);
  }


}
