package com.adhoc.flight;

import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;

import com.adhoc.flight.client.AdhocFlightClient;
import com.adhoc.flight.utils.PrintUtils;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableMap;

/**
 * Java Flight sample application that runs the specified query.
 */
public class QueryRunner {
    private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator(Integer.MAX_VALUE);
    private static final CommandLineArguments ARGUMENTS = new CommandLineArguments();

    private static final String CREATE_DEMO_TABLE =
            "CREATE TABLE $scratch.dremio_flight_demo_table as select * from (VALUES(1,2,3),(4,5,6))";
    private static final String DROP_DEMO_TABLE = "DROP TABLE $scratch.dremio_flight_demo_table";
    private static final String SELECT_DEMO_TABLE = "SELECT * FROM dremio_flight_demo_table";
    private static final String DEMO_TABLE_SCHEMA = "$scratch";

    static class CommandLineArguments {
        @Parameter(names = {"-host", "--hostname"},
                description = "Dremio co-ordinator hostname")
        public String host = "localhost";

        @Parameter(names = {"-port", "--flightport"},
                description = "Dremio flight server port")
        public int port = 32010;

        @Parameter(names = {"-user", "--username"},
                description = "Dremio username")
        public String user = "dremio";

        @Parameter(names = {"-pass", "--password"},
                description = "Dremio password")
        public String pass = "dremio123";

        @Parameter(names = {"-query", "--sqlQuery"},
                description = "SQL query to test")
        public String query = null;

        @Parameter(names = {"-tls", "--tls"},
                description = "Enable encrypted connection")
        public boolean enableTls = false;

        @Parameter(names = {"-kstpath", "--keyStorePath"},
                description = "Path to the jks keystore")
        public String keystorePath = null;

        @Parameter(names = {"-kstpass", "--keyStorePassword"},
                description = "The jks keystore password")
        public String keystorePass = null;

        @Parameter(names = {"-demo", "--runDemo"},
                description = "A flag to to run a demo of querying the Dremio Flight Server Endpoint.")
        public boolean runDemo = false;

        @Parameter(names = {"-h", "--help"},
                description = "show usage", help=true)
        public boolean help = false;
    }

    /**
     * Runs a self contained demo to authenticate and query a Dremio Flight Server Endpoint.
     *
     * @throws Exception
     */
    public static void runDemo() throws Exception {
        AdhocFlightClient client = null;

        try {
            System.out.println("\n[INFO] Running demo to query Dremio Flight Server Endpoint.");
            System.out.println("[INFO] Configured Dremio Flight Server Endpoint host: " + ARGUMENTS.host);
            System.out.println("[INFO] Configured Dremio Flight Server Endpoint port: " + ARGUMENTS.port);

            /**
             * Authentication
             */
            System.out.println("[INFO] [STEP 1]: Authenticating with the Dremio server using Arrow Flight " +
                    "authorization header authentication.");
            System.out.println("[INFO] Initial UserSession client properties are set as well.");
            System.out.println("[INFO] Setting client property: routing-tag => test-routing-tag");
            System.out.println("[INFO] Setting client property: routing-queue => Low Cost User Queries");

            // Set routing-tag and routing-queue during initial authentication.
            final Map<String, String> properties = ImmutableMap.of(
                    "routing-tag", "test-routing-tag",
                    "routing-queue", "Low Cost User Queries");
            final HeaderCallOption routingCallOption = createClientProperties(properties);
            // Authenticates FlightClient with routing properties.
            client = createFlightClient(routingCallOption);
            PrintUtils.prettyPrintAuthenticationSuccess(ARGUMENTS.host, ARGUMENTS.port);

            /**
             * Create demo table in $scratch
             */
            System.out.println("[INFO] [STEP 2]: Create a test table in $scratch.");
            client.runQuery(CREATE_DEMO_TABLE, null);
            System.out.println("[INFO] Created $scratch.dremio_flight_demo_table in $scratch successfully.");

            /**
             * Query demo table
             */
            System.out.println("[INFO] [STEP 3]: Query demo table $scrach.dremio_flight_demo_table");
            System.out.println("[INFO] Setting client property: schema => $scratch");
            PrintUtils.printRunQuery(SELECT_DEMO_TABLE);

            // Set default schema path to "$scratch" for the next FlightRPC request.
            final Map<String, String> schemaProperty = ImmutableMap.of(
                    "schema", DEMO_TABLE_SCHEMA);
            final HeaderCallOption schemaCallOption = createClientProperties(schemaProperty);
            // Run query "select * from dremio_flight_demo_table" without schema path.
            final List<Object[]> results = client.runQuery(SELECT_DEMO_TABLE, schemaCallOption);

            /**
             * Print Results
             */
            System.out.println("[INFO] [STEP 4]: Iterate through query results.");
            PrintUtils.prettyPrintRows(results);
            System.out.println();

            /**
             * Drop Demo Table
             */
            System.out.println("[INFO] [STEP 5]: Drop demo table.");
            client.runQuery(DROP_DEMO_TABLE, schemaCallOption);
            System.out.println("[INFO] Dropped $scratch.dremio_flight_demo_table successfully");
        } catch (Exception ex) {
            System.out.println("[ERROR] Exception: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            AutoCloseables.close(client);
        }
    }

    /**
     * An adhoc method to run a user query.
     *
     * Note: This adhoc does not use any client properties.
     *       Please See demo above for client properties usage.
     *
     * @throws Exception
     */
    public static void runAdhoc() throws Exception {
        AdhocFlightClient client = null;
        try {
            /**
             * Authentication
             */
            client = createFlightClient(null);
            PrintUtils.prettyPrintAuthenticationSuccess(ARGUMENTS.host, ARGUMENTS.port);

            /**
             * Run Query
             */
            final List<Object[]> results = client.runQuery(ARGUMENTS.query, null);

            /**
             * Print Results
             */
            PrintUtils.printRunQuery(ARGUMENTS.query);
            PrintUtils.prettyPrintRows(results);
        } catch (Exception ex) {
            System.out.println("[ERROR] Exception: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            AutoCloseables.close(client);
        }
    }

    public static void main(String[] args) throws Exception {
        parseCommandLineArgs(args);

        if (ARGUMENTS.help) {
            System.exit(1);
        } else if (ARGUMENTS.runDemo) {
            runDemo();
        } else {
            runAdhoc();
        }
    }

    /**
     * Creates a FlightClient instance based on command line arguments provided.
     *
     * @param clientProperties Dremio client properties.
     * @return an instance of AdhocFlightClient encapsulating the connected FlightClient instance
     *         and the CredentialCallOption with a bearer token to use in subsequent requests.
     * @throws Exception if there are issues connecting to the server.
     */
    private static AdhocFlightClient createFlightClient(HeaderCallOption clientProperties) throws Exception {
        if (ARGUMENTS.enableTls) {
            Preconditions.checkNotNull(ARGUMENTS.keystorePath,
                    "When TLS is enabled, path to the KeyStore is required.");
            Preconditions.checkNotNull(ARGUMENTS.keystorePass,
                    "When TLS is enabled, the KeyStore password is required.");
            return AdhocFlightClient.getEncryptedClient(BUFFER_ALLOCATOR,
                    ARGUMENTS.host, ARGUMENTS.port,
                    ARGUMENTS.user, ARGUMENTS.pass,
                    ARGUMENTS.keystorePath, ARGUMENTS.keystorePass,
                    clientProperties);
        } else {
            return AdhocFlightClient.getBasicClient(BUFFER_ALLOCATOR,
                    ARGUMENTS.host, ARGUMENTS.port,
                    ARGUMENTS.user, ARGUMENTS.pass,
                    clientProperties);
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
