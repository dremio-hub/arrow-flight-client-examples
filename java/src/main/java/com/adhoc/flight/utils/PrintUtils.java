/*
 * Copyright (C) 2017-2020 Dremio Corporation
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

import java.util.List;

/**
 * Helper class to print output to console.
 */
public class PrintUtils {
    public static void prettyPrintRows(List<Object[]> rows) {
        System.out.println("-----------------"
                + " Total number of records: " + rows.size()
                + " -----------------");
        for (Object[] rowValues : rows) {
            for (int i = 0; i < rowValues.length; i++) {
                System.out.print(rowValues[i]);
                if (i + 1 < rowValues.length) {
                    System.out.print(",");
                }
            }
        }
        System.out.println("\n--------------------------------------------------------------");
    }

    public static void printRunQuery(String query) {
        System.out.println("Running Query: " + query);
    }

    public static void printExceptionOnClosed(Exception ex) {
        System.out.println("Encountered exception while closing client, exception: " + ex.getMessage());
    }
}
