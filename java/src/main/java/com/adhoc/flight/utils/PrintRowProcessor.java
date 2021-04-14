package com.adhoc.flight.utils;

import com.adhoc.flight.client.RowProcessor;

public class PrintRowProcessor implements RowProcessor {
    private long rowCount = 0;

    public void printAuthenticated(String host, int port) {
        System.out.println("[INFO] Authenticated with Dremio server at " + host + ":" + port
                + " successfully.");
    }

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

    public void printFooter() {
        System.out.println("-----------------"
                + " Total number of records retrieved: " + rowCount
                + " -----------------");
    }

    public void printRunQuery(String query) {
        System.out.println("[INFO] Running Query: " + query);
    }

    public static void printExceptionOnClosed(Exception ex) {
        System.out.println("[ERROR] Encountered exception while closing client, exception: " + ex.getMessage());
    }
}
