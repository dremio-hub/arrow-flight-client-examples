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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.util.Properties;

public final class OAuthJdbcExamples {
  private OAuthJdbcExamples() {
  }

  public static void main(String[] args) throws Exception {
    final ClientCredentialsCommand clientCreds = new ClientCredentialsCommand();
    final TokenExchangeCommand tokenExchange = new TokenExchangeCommand();
    final DremioImpersonationCommand impersonation = new DremioImpersonationCommand();

    final JCommander jCommander = JCommander.newBuilder()
        .programName("java --add-opens=java.base/java.nio=ALL-UNNAMED -jar <jar>")
        .addCommand(clientCreds)
        .addCommand(tokenExchange)
        .addCommand(impersonation)
        .build();

    if (isTopLevelHelp(args)) {
      jCommander.usage();
      return;
    }

    try {
      jCommander.parse(args);
    } catch (ParameterException ex) {
      System.err.println(ex.getMessage());
      jCommander.usage();
      System.exit(1);
      return;
    }

    final String command = jCommander.getParsedCommand();
    if (command == null) {
      jCommander.usage();
      return;
    }

    if (isCommandHelpRequested(command, clientCreds, tokenExchange, impersonation)) {
      jCommander.getCommands().get(command).usage();
      return;
    }

    final Properties properties;
    final ConnectionParams connection;

    switch (command) {
      case "client-credentials":
        properties = clientCreds.toProperties();
        connection = clientCreds.connection;
        break;
      case "token-exchange":
        properties = tokenExchange.toProperties();
        connection = tokenExchange.connection;
        break;
      case "dremio-impersonation":
        properties = impersonation.toProperties();
        connection = impersonation.connection;
        break;
      default:
        jCommander.usage();
        System.exit(1);
        return;
    }

    FlightSqlExampleSupport.executeQuery(command, connection.connectionUrl(),
        connection.query, connection.maxRows, properties);
  }

  private static boolean isTopLevelHelp(String[] args) {
    return args.length == 1
        && ("-h".equals(args[0]) || "--help".equals(args[0]) || "help".equals(args[0]));
  }

  private static boolean isCommandHelpRequested(String command,
      ClientCredentialsCommand clientCreds, TokenExchangeCommand tokenExchange,
      DremioImpersonationCommand impersonation) {
    switch (command) {
      case "client-credentials":
        return clientCreds.connection.help;
      case "token-exchange":
        return tokenExchange.connection.help;
      case "dremio-impersonation":
        return impersonation.connection.help;
      default:
        return false;
    }
  }
}
