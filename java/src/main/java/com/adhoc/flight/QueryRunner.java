package com.adhoc.flight;

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

/**
 * Java Adhoc Flight sample application that runs the specified query.
 */
public class QueryRunner {
    private static final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator(Integer.MAX_VALUE);
    private static final CommandLineArguments ARGUMENTS = new CommandLineArguments();

    private static AdhocFlightClient getFlightClient() throws Exception {
        if (ARGUMENTS.enableTls) {
            Preconditions.checkNotNull(ARGUMENTS.keystorePath,
                    "When TLS is enabled, path to the KeyStore is required.");
            Preconditions.checkNotNull(ARGUMENTS.keystorePass,
                    "When TLS is enabled, the KeyStore password is required.");
            return AdhocFlightClient.getEncryptedClient(
                    BUFFER_ALLOCATOR, ARGUMENTS.host, ARGUMENTS.port, ARGUMENTS.user, ARGUMENTS.pass,
                    ARGUMENTS.keystorePath, ARGUMENTS.keystorePass);
        } else {
            return AdhocFlightClient.getBasicClient(
                    BUFFER_ALLOCATOR, ARGUMENTS.host, ARGUMENTS.port, ARGUMENTS.user, ARGUMENTS.pass);
        }
    }

    public static void main(String[] args) throws Exception {
        parseCommandLineArgs(args);
        AdhocFlightClient client = null;
        try {
            client = getFlightClient();
            HeaderCallOption headerCallOption = getCallHeaders();
            PrintUtils.printRunQuery(ARGUMENTS.query);
            PrintUtils.prettyPrintRows(client.runQuery(ARGUMENTS.query, headerCallOption));
        } catch (Exception ex) {
            System.out.println("[ERROR] Exception: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            AutoCloseables.close(client);
        }
    }

    // TODO: Update the method as needed to pass the Call Headers.
    private static HeaderCallOption getCallHeaders() {
        final FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("schema", "Samples.\"samples.dremio.com\"");
        return new HeaderCallOption(headers);
    }

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
                description = "SQL query to test",
                required = true)
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

        @Parameter(names = {"-h", "--help"},
                description = "show usage", help=true)
        public boolean help = false;
    }
}
