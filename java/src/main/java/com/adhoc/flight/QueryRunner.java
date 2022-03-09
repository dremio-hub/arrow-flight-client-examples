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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.adhoc.flight.client.AdhocFlightClient;
import com.adhoc.flight.utils.QueryUtils;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

/**
 * Java Flight sample application that runs the specified query.
 */
public class QueryRunner {
  private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator(Integer.MAX_VALUE);
  private static final CommandLineArguments ARGUMENTS = new CommandLineArguments();

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
   * Class that holds all the command line arguments that can be used to run the
   * examples.
   */
  static class CommandLineArguments {
    @Parameter(names = {"-host", "--hostname"},
        description = "Dremio co-ordinator hostname. Defaults to \"localhost\".")
    public String host = "localhost";

    @Parameter(names = {"-port", "--flightport"},
        description = "Dremio flight server port. Defaults to 32010.")
    public int port = 32010;

    @Parameter(names = {"-user", "--username"},
        description = "Dremio username. Defaults to \"dremio\".")
    public String user;

    @Parameter(names = {"-pass", "--password"},
        description = "Dremio password. Defaults to \"dremio123\".")
    public String pass;

    @Parameter(names = {"-pat", "--personalAccessToken", "-authToken", "--authToken"},
        description = "Either a Personal Access Token or an OAuth2 Token.")
    public String patOrAuthToken;

    @Parameter(names = {"-query", "--sqlQuery"},
        description = "SQL query to test.")
    public String query = null;

    @Parameter(names = {"-binpath", "--saveBinaryPath"},
        description = "Path to save the SQL result binary to.")
    public String pathToSaveQueryResultsTo = null;

    @Parameter(names = {"-tls", "--tls"},
        description = "Enable encrypted connection. Defaults to false.")
    public boolean enableTls = false;

    @Parameter(names = {"-dsv", "--disableServerVerification"},
        description = "Disable TLS server verification. Defaults to false.")
    public boolean disableServerVerification = false;

    @Parameter(names = {"-kstpath", "--keyStorePath"},
        description = "Path to the jks keystore.")
    public String keystorePath = null;

    @Parameter(names = {"-kstpass", "--keyStorePassword"},
        description = "The jks keystore password.")
    public String keystorePass = null;

    @Parameter(names = {"-demo", "--runDemo"},
        description = "A flag to to run a demo of querying the Dremio Flight Server Endpoint. Defaults to false.")
    public boolean runDemo = false;

    @Parameter(names = {"-engine", "--engine"},
        description = "The specific engine to run against.")
    public String engine;

    @Parameter(names = {"-sp", "--sessionProperty"},
        description = "Key value pairs of SessionProperty, " +
          "example: -sp schema='Samples.\"samples.dremio.com\"' -sp key=value",
        listConverter = SessionPropertyConverter.class)
    public List<SessionProperty> sessionProperties = new ArrayList<>();

    @Parameter(names = {"-h", "--help"},
        description = "Show usage.", help = true)
    public boolean help = false;
  }

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
    System.out.println("[INFO] Configured Dremio Flight Server Endpoint host: " + ARGUMENTS.host);
    System.out.println("[INFO] Configured Dremio Flight Server Endpoint port: " + ARGUMENTS.port);

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
      QueryUtils.printAuthenticated(ARGUMENTS.host, ARGUMENTS.port);

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

    ARGUMENTS.sessionProperties.forEach( sessionProperty -> {
      sessionPropertiesMap.put(sessionProperty.getKey(), sessionProperty.getValue());
    });

    if (!Strings.isNullOrEmpty(ARGUMENTS.engine)) {
      sessionPropertiesMap.put(KEY_ROUTING_ENGINE, ARGUMENTS.engine);
    }

    final HeaderCallOption clientProperties = createClientProperties(sessionPropertiesMap);

    try (final AdhocFlightClient client = createFlightClient(clientProperties)) {

      /**
       * Authentication
       */
      QueryUtils.printAuthenticated(ARGUMENTS.host, ARGUMENTS.port);

      /**
       * Run Query
       */
      QueryUtils.printRunningQuery(ARGUMENTS.query);

      if (pathToSaveQueryResultsTo != null) {
        client.runQuery(ARGUMENTS.query, clientProperties, new File(pathToSaveQueryResultsTo), true);
      } else {
        client.runQuery(ARGUMENTS.query, clientProperties, null, true);
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
    parseCommandLineArgs(args);

    try {
      if (ARGUMENTS.help) {
        System.exit(1);
      } else if (ARGUMENTS.runDemo) {
        runDemo();
      } else {
        runAdhoc(ARGUMENTS.pathToSaveQueryResultsTo);
      }
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
    if (Strings.isNullOrEmpty(ARGUMENTS.patOrAuthToken)) {
      if (Strings.isNullOrEmpty(ARGUMENTS.user)) {
        ARGUMENTS.user = DEMO_USERNAME;
      }

      if (Strings.isNullOrEmpty(ARGUMENTS.pass)) {
        ARGUMENTS.pass = DEMO_PASSWORD;
      }
    }

    if (ARGUMENTS.enableTls) {
      Preconditions.checkNotNull(ARGUMENTS.keystorePath,
          "When TLS is enabled, path to the KeyStore is required.");
      Preconditions.checkNotNull(ARGUMENTS.keystorePass,
          "When TLS is enabled, the KeyStore password is required.");
      return AdhocFlightClient.getEncryptedClient(BUFFER_ALLOCATOR,
          ARGUMENTS.host, ARGUMENTS.port,
          ARGUMENTS.user, ARGUMENTS.pass,
          ARGUMENTS.patOrAuthToken,
          ARGUMENTS.keystorePath, ARGUMENTS.keystorePass,
          ARGUMENTS.disableServerVerification,
          clientProperties,
          null);
    } else {
      return AdhocFlightClient.getBasicClient(BUFFER_ALLOCATOR,
          ARGUMENTS.host, ARGUMENTS.port,
          ARGUMENTS.user, ARGUMENTS.pass,
          ARGUMENTS.patOrAuthToken,
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

  /**
   * Parses command line arguments.
   *
   * @param args command line arguments to parse.
   */
  private static void parseCommandLineArgs(String[] args) {
    JCommander jCommander = new JCommander(QueryRunner.ARGUMENTS);
    jCommander.setProgramName("Java Adhoc Client");
    try {
      jCommander.parse(args);
    } catch (ParameterException e) {
      System.out.println("\n" + e.getMessage() + "\n");
      jCommander.usage();
    }
    if (QueryRunner.ARGUMENTS.help) {
      jCommander.usage();
    }
  }
}
