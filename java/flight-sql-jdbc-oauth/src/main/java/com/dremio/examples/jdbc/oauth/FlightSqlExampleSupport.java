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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;

final class FlightSqlExampleSupport {
  private static final String DEFAULT_QUERY = "SELECT 1 AS example_value";
  private static final int DEFAULT_PORT = 32010;
  private static final int DEFAULT_MAX_ROWS = 10;

  private FlightSqlExampleSupport() {
  }

  static Properties baseConnectionProperties(ExampleEnvironment environment) {
    final Properties properties = new Properties();

    properties.setProperty("useEncryption",
        Boolean.toString(environment.getBoolean("DREMIO_USE_ENCRYPTION", false)));
    properties.setProperty("disableCertificateVerification",
        Boolean.toString(environment.getBoolean(
            "DREMIO_DISABLE_CERTIFICATE_VERIFICATION", false)));

    setIfPresent(properties, "tlsRootCerts", environment.optional("DREMIO_TLS_ROOT_CERTS"));
    setIfPresent(properties, "trustStore", environment.optional("DREMIO_TRUST_STORE"));
    setIfPresent(properties, "trustStorePassword",
        environment.optional("DREMIO_TRUST_STORE_PASSWORD"));
    setIfPresent(properties, "clientCertificate",
        environment.optional("DREMIO_CLIENT_CERTIFICATE"));
    setIfPresent(properties, "clientKey", environment.optional("DREMIO_CLIENT_KEY"));
    setIfPresent(properties, "catalog", environment.optional("DREMIO_CATALOG"));

    return properties;
  }

  static void executeQuery(String scenario, ExampleEnvironment environment,
      Properties properties) throws Exception {
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

    final String url = connectionUrl(environment);
    final String query = environment.optional("DREMIO_SQL").orElse(DEFAULT_QUERY);
    final int maxRows = environment.getInt("DREMIO_MAX_ROWS", DEFAULT_MAX_ROWS);

    System.out.println("Scenario: " + scenario);
    System.out.println("Flight SQL URL: " + url);
    System.out.println("Query: " + query);

    try (Connection connection = DriverManager.getConnection(url, properties);
         Statement statement = connection.createStatement()) {
      final boolean hasResults = statement.execute(query);
      if (!hasResults) {
        System.out.println("Statement completed without a result set.");
        return;
      }

      try (ResultSet resultSet = statement.getResultSet()) {
        printRows(resultSet, maxRows);
      }
    }
  }

  private static String connectionUrl(ExampleEnvironment environment) {
    return String.format("jdbc:arrow-flight-sql://%s:%d",
        environment.require("DREMIO_HOST"),
        environment.getInt("DREMIO_FLIGHT_PORT", DEFAULT_PORT));
  }

  private static void printRows(ResultSet resultSet, int maxRows) throws SQLException {
    final ResultSetMetaData metadata = resultSet.getMetaData();
    final int columnCount = metadata.getColumnCount();
    int rowCount = 0;

    final StringBuilder header = new StringBuilder();
    for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
      if (columnIndex > 1) {
        header.append('\t');
      }
      header.append(metadata.getColumnLabel(columnIndex));
    }
    System.out.println(header);

    while (resultSet.next()) {
      if (rowCount == maxRows) {
        System.out.println("Stopped after " + maxRows + " rows.");
        return;
      }

      final StringBuilder row = new StringBuilder();
      for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
        if (columnIndex > 1) {
          row.append('\t');
        }
        row.append(String.valueOf(resultSet.getObject(columnIndex)));
      }
      System.out.println(row);
      rowCount++;
    }

    if (rowCount == 0) {
      System.out.println("Query returned no rows.");
    }
  }

  static void setIfPresent(Properties properties, String name,
      Optional<String> value) {
    if (value.isPresent()) {
      properties.setProperty(name, value.get());
    }
  }
}
