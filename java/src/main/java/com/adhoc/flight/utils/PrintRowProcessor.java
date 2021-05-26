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
package com.adhoc.flight.utils;

import com.adhoc.flight.client.RowProcessor;

/**
 * A RowProcessor implementation that will print out all of the data
 * received to STDOUT.
 * @deprecated
 * @see QueryUtils
 */
@Deprecated
public class PrintRowProcessor implements RowProcessor {
    private long rowCount = 0;

    /**
     * Prints a message out that the client has authenticated with the server.
     * @param host The server that the client connected to
     * @param port The port on the server that the client connected to
     */
    public void printAuthenticated(String host, int port) {
        System.out.println("[INFO] Authenticated with Dremio server at " + host + ":" + port
                + " successfully.");
    }

    /**
     * Prints out the header.
     */
    public void printPreamble() {
        System.out.println("[INFO] Printing query results from Dremio.");
    }

    @Override
    public void processRow(Object[] rowValues) {
        rowCount++;

        for (int i = 0; i < rowValues.length; i++) {
            System.out.print(rowValues[i]);
            if (i + 1 < rowValues.length) {
                System.out.print(",");
            }
        }
        System.out.println();
    }

    /**
     * Prints out the footer.
     */
    public void printFooter() {
        System.out.println("-----------------"
                + " Total number of records retrieved: " + rowCount
                + " -----------------");
    }

    /**
     * Prints out the query that was run.
     * @param query the query that was run
     */
    public void printRunQuery(String query) {
        System.out.println("[INFO] Running Query: " + query);
    }

    /**
     * Prints out a message that an exception was received.
     * @param ex the exception that was received.
     */
    public static void printExceptionOnClosed(Exception ex) {
        System.out.println("[ERROR] Encountered exception while closing client, exception: " + ex.getMessage());
    }
}
