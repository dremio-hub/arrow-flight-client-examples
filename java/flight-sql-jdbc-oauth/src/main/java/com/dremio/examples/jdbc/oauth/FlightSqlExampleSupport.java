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
}
