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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

final class FlightSqlExampleSupport {
  private FlightSqlExampleSupport() {
  }

  static void executeQuery(String scenario, String url, String query, int maxRows,
      Properties properties) throws Exception {
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

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

  static void executeQueryAndValidateImpersonation(String scenario, String url, String query,
      int maxRows, Properties properties, String targetUser) throws Exception {
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

    System.out.println("Scenario: " + scenario);
    System.out.println("Flight SQL URL: " + url);
    System.out.println("Query: " + query);

    try (Connection connection = DriverManager.getConnection(url, properties);
         Statement statement = connection.createStatement()) {
      final boolean hasResults = statement.execute(query);
      if (!hasResults) {
        throw new IllegalStateException("Impersonation validation failed: statement completed "
            + "without a result set.");
      }

      try (ResultSet resultSet = statement.getResultSet()) {
        validateImpersonationResult(printRows(resultSet, maxRows), targetUser);
      }
    }
  }

  private static List<Map<String, String>> printRows(ResultSet resultSet, int maxRows)
      throws SQLException {
    final ResultSetMetaData metadata = resultSet.getMetaData();
    final int columnCount = metadata.getColumnCount();
    int rowCount = 0;
    final List<Map<String, String>> rows = new ArrayList<>();

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
        return rows;
      }

      final StringBuilder row = new StringBuilder();
      final Map<String, String> values = new LinkedHashMap<>();
      for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
        if (columnIndex > 1) {
          row.append('\t');
        }
        final String value = String.valueOf(resultSet.getObject(columnIndex));
        row.append(value);
        values.put(metadata.getColumnLabel(columnIndex).toLowerCase(Locale.ROOT), value);
      }
      System.out.println(row);
      rows.add(values);
      rowCount++;
    }

    if (rowCount == 0) {
      System.out.println("Query returned no rows.");
    }
    return rows;
  }

  private static void validateImpersonationResult(List<Map<String, String>> rows,
      String targetUser) {
    if (rows.isEmpty()) {
      throw new IllegalStateException("Impersonation validation failed: query returned no rows.");
    }

    final Map<String, String> expectedColumns = new LinkedHashMap<>();
    expectedColumns.put("user_name", "USER()");
    expectedColumns.put("session_user_name", "SESSION_USER()");
    expectedColumns.put("system_user_name", "SYSTEM_USER()");

    for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
      final Map<String, String> row = rows.get(rowIndex);
      for (Map.Entry<String, String> expectedColumn : expectedColumns.entrySet()) {
        final String value = row.get(expectedColumn.getKey());
        if (value == null) {
          throw new IllegalStateException("Impersonation validation failed: query must return "
              + "column '" + expectedColumn.getKey() + "' for " + expectedColumn.getValue()
              + ".");
        }
        if (!targetUser.equals(value)) {
          throw new IllegalStateException("Impersonation validation failed: row "
              + (rowIndex + 1) + " " + expectedColumn.getValue() + " returned '" + value
              + "', expected '" + targetUser + "'.");
        }
      }
    }

    System.out.println("Impersonation validation passed: USER(), SESSION_USER(), and "
        + "SYSTEM_USER() all returned '" + targetUser + "'.");
  }
}
