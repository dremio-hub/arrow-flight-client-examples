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

package com.adhoc.flightsql;

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.example.FlightSqlClientDemoApp;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.adhoc.flight.QueryRunner;
import com.adhoc.flight.client.AdhocFlightClient;

/**
 * Dremio's Flight SQL Client Demo CLI Application.
 */
public class DremioFlightSqlClientDemoApp extends FlightSqlClientDemoApp {

    public DremioFlightSqlClientDemoApp(BufferAllocator bufferAllocator) {
        super(bufferAllocator);
    }

    public static void main(final String[] args) throws Exception {
        final Options options = new Options();

        options.addOption("host", "hostname", true, "Dremio co-ordinator hostname. Defaults to \"localhost\".");
        options.addOption("port", "flightport", true, "Dremio flight server port. Defaults to 32010.");
        options.addOption("user", "username", true, "Dremio username. Defaults to \"dremio\".");
        options.addOption("pass", "password", true, "Dremio password. Defaults to \"dremio123\".");
        options.addOption("pat", "personalAccessToken", true, "Personal Access Token");
        options.addOption("tls", "tls", true, "Enable encrypted connection. Defaults to false.");
        options.addOption("dsv", "disableServerVerification", true, "Disable TLS server verification. Defaults to false.");
        options.addOption("kstpath", "keyStorePath", true, "Path to the jks keystore.");
        options.addOption("kstpass", "keyStorePassword", true, "The jks keystore password.");
        options.addOption("sp", "sessionProperty", true, "Key value pairs of SessionProperty, " +
                "example: -sp schema='Samples.\"samples.dremio.com\"' -sp key=value");

        options.addRequiredOption("command", "command", true, "Method to run");

        options.addOption("query", "query", true, "Query");
        options.addOption("catalog", "catalog", true, "Catalog");
        options.addOption("schema", "schema", true, "Schema");
        options.addOption("table", "table", true, "Table");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            try (final DremioFlightSqlClientDemoApp thisApp = new DremioFlightSqlClientDemoApp(
                    new RootAllocator(Integer.MAX_VALUE))) {
                thisApp.executeApp(cmd);
            }

        } catch (final ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("DremioFlightSqlClientDemoApp -host localhost -port 32010 ...", options);
            throw e;
        }
    }

    /**
     * Adds a {@link CallOption} to the current {@code callOptions} array.
     */
    public void addCallOption(final CallOption optionToAdd) {
        callOptions.add(optionToAdd);
    }

    /**
     * Calls {@link DremioFlightSqlClientDemoApp#createFlightSqlClient}
     * in order to create a {@link FlightSqlClient} to be used in future calls,
     * and then calls {@link DremioFlightSqlClientDemoApp#executeCommand(CommandLine)}
     * to execute the command parsed at execution.
     *
     * @param cmd Parsed {@link CommandLine}; often the result of {@link DefaultParser#parse(Options, String[])}.
     */
    public void executeApp(CommandLine cmd) throws Exception {
        final String host = cmd.getOptionValue("hostname", "localhost").trim();
        final int port = Integer.parseInt(cmd.getOptionValue("flightport", "32010").trim());
        final String username = cmd.getOptionValue("username", "dremio").trim();
        final String password = cmd.getOptionValue("password", "dremio123").trim();
        final boolean useTls = Boolean.parseBoolean(cmd.getOptionValue("tls", "false").trim());
        final boolean disableServerVerification = Boolean.parseBoolean(cmd.getOptionValue("disableServerVerification", "false").trim());
        final String keyStorePath = cmd.getOptionValue("keyStorePath", null);
        final String keyStorePassword = cmd.getOptionValue("keyStorePassword", null);
        final String token = cmd.getOptionValue("personalAccessToken", null);
        final String[] sessionProperties = cmd.getOptionValues("sessionProperty");

        final Map<String, String> sessionPropertiesMap = new HashMap<>();

        if (sessionProperties != null) {
            for (String sessionProperty : sessionProperties) {
                String[] tokens = sessionProperty.split("=");
                sessionPropertiesMap.put(tokens[0], tokens[1]);
            }
        }

        final HeaderCallOption clientProperties = QueryRunner.createClientProperties(sessionPropertiesMap);

        AdhocFlightClient adhocFlightClient;
        if (useTls) {
            Preconditions.checkNotNull(keyStorePath,
                    "When TLS is enabled, path to the KeyStore is required.");
            Preconditions.checkNotNull(keyStorePassword,
                    "When TLS is enabled, the KeyStore password is required.");
            adhocFlightClient = AdhocFlightClient.getEncryptedClient(allocator,
                    host, port,
                    username, password,
                    token,
                    keyStorePath, keyStorePassword,
                    disableServerVerification,
                    clientProperties,
                    null);
        } else {
            adhocFlightClient = AdhocFlightClient.getBasicClient(allocator,
                    host, port,
                    username, password,
                    token,
                    clientProperties,
                    null);
        }

        FlightClient flightClient = adhocFlightClient.getFlightClient();

        flightSqlClient = new FlightSqlClient(flightClient);
        addCallOption(adhocFlightClient.getCredentialCallOption());

        executeCommand(cmd);
    }
}
