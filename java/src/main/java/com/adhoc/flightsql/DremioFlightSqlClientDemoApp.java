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

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.example.FlightSqlClientDemoApp;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Dremio's Flight SQL Client Demo CLI Application.
 */
public class DremioFlightSqlClientDemoApp extends FlightSqlClientDemoApp {

  public DremioFlightSqlClientDemoApp(BufferAllocator bufferAllocator) {
    super(bufferAllocator);
  }

  public static void main(final String[] args) throws Exception {
    final Options options = new Options();

    options.addOption("host", "host", true, "Host to connect to");
    options.addOption("port", "port", true, "Port to connect to");
    options.addOption("username", "username", true, "Auth username");
    options.addOption("password", "password", true, "Auth password");
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
   * Calls {@link DremioFlightSqlClientDemoApp#createFlightSqlClient(String, String, String, String)}
   * in order to create a {@link FlightSqlClient} to be used in future calls,
   * and then calls {@link DremioFlightSqlClientDemoApp#executeCommand(CommandLine)}
   * to execute the command parsed at execution.
   *
   * @param cmd Parsed {@link CommandLine}; often the result of {@link DefaultParser#parse(Options, String[])}.
   */
  public void executeApp(CommandLine cmd) throws Exception {
    createFlightSqlClient(
        cmd.getOptionValue("host", "localhost").trim(), cmd.getOptionValue("port", "32010").trim(),
        cmd.getOptionValue("username", "dremio").trim(), cmd.getOptionValue("password", "dremio123").trim());

    executeCommand(cmd);
  }

  /**
   * Creates a {@link FlightSqlClient} to be used with the example methods.
   *
   * @param host client's hostname.
   * @param port client's port.
   * @param user client's username auth.
   * @param pass client's password auth.
   */
  public void createFlightSqlClient(final String host, final String port, final String user, final String pass) {
    final ClientIncomingAuthHeaderMiddleware.Factory factory =
        new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    final FlightClient client = FlightClient.builder()
        .allocator(allocator)
        .location(Location.forGrpcInsecure(host, Integer.parseInt(port)))
        .intercept(factory)
        .build();
    client.handshake(new CredentialCallOption(new BasicAuthCredentialWriter(user, pass)));
    addCallOption(factory.getCredentialCallOption());
    flightSqlClient = new FlightSqlClient(client);
  }
}