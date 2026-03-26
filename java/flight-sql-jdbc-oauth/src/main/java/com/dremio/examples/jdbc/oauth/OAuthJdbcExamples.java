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

import java.util.Locale;
import java.util.Properties;

public final class OAuthJdbcExamples {
  private OAuthJdbcExamples() {
  }

  public static void main(String[] args) throws Exception {
    try {
      run(args, ExampleEnvironment.system());
    } catch (IllegalArgumentException ex) {
      System.err.println("Configuration error: " + ex.getMessage());
      printUsage();
      System.exit(1);
    }
  }

  static void run(String[] args, ExampleEnvironment environment) throws Exception {
    if (args.length != 1 || isHelp(args[0])) {
      printUsage();
      return;
    }

    final String scenario = args[0].toLowerCase(Locale.ROOT);
    final Properties properties;

    switch (scenario) {
      case "client-credentials":
        properties = OAuthFlowProperties.clientCredentials(environment);
        break;
      case "token-exchange":
        properties = OAuthFlowProperties.tokenExchange(environment);
        break;
      case "dremio-impersonation":
        properties = OAuthFlowProperties.dremioUserImpersonation(environment);
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown scenario: " + args[0]);
    }

    FlightSqlExampleSupport.executeQuery(scenario, environment, properties);
  }

  private static boolean isHelp(String argument) {
    return "-h".equals(argument)
        || "--help".equals(argument)
        || "help".equals(argument);
  }

  private static void printUsage() {
    System.out.println("Usage: java -jar <jar> "
        + "<client-credentials|token-exchange|dremio-impersonation>");
    System.out.println("See java/flight-sql-jdbc-oauth/README.md for required environment variables.");
  }
}
